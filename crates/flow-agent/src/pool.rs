//! 本地执行器绑定池（agent 侧）：BindExecutor → spawn + 本地握手 →
//! BindExecutorAck；取消/断线回收；Resume 事实（最后确认游标/固定结果）。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::{
    DRAIN_GRACE_MS, HEARTBEAT_TIMEOUT_MS, STARTUP_TIMEOUT_MS,
};
use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::spawn::spawn_executor_process;
use flow_engine::execution_protocol::transport::{master_handshake, FrameTransport};

use crate::relay::{Relay, PER_EXECUTOR_QUEUE};

/// 执行器声明的能力（与主进程必需能力对齐）。
pub const EXECUTOR_CAPABILITIES: &[&str] = &["execute", "js", "http", "transfer"];

/// 一个本地受控执行器会话句柄（runtime 持有）。
pub struct ExecutorHandle {
    pub dispatch_id: String,
    pub executor_id: String,
    pub executor_boot_id: String,
    /// master→executor 内层消息入口。
    pub inbox: mpsc::Sender<Message>,
    /// executor→上联出站（未包装内层；有界，满则背压）。
    pub out: mpsc::Receiver<Message>,
    pub cancel: CancellationToken,
    /// 主进程最后确认的审计序号（旁路观察，Resume 事实）。
    pub last_acked: Arc<AtomicU64>,
    /// 执行器已生成的固定结果（result_id, last_audit_seq）。
    pub result: Arc<Mutex<Option<(String, u64)>>>,
    /// Resume 事实：本端主动取消（drain/裁决/停机）杀掉了在飞执行器。
    pub forced_kill: Arc<AtomicBool>,
    /// Resume 事实：执行器在无固定结果时失联（未确认审计窗口可能丢失）。
    pub lost_data: Arc<AtomicBool>,
    /// executor_loop 走完回收（自然完成或被杀）；drain 宽限等待用。
    pub done: Arc<AtomicBool>,
    pub committed: Arc<AtomicBool>,
    /// 出站帧到达信号：executor_loop 每次 out.send 后 notify_one，
    /// fair_mux_bridge 空闲时等它而不是固定轮询（新增执行器由 50ms
    /// 安全网兜底——它尚不在桥的等待集内）。
    pub notify: Arc<Notify>,
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("bind failed: {0}")]
    Bind(String),
}

/// 绑定一个派发到新执行器进程并完成本地握手（确认后才转发 Execute）。
pub async fn bind_executor(
    executor: &flow_engine::execution_protocol::contract::ExecutorInvocation,
    journal_id: &str,
    master_epoch: u64,
    relay: &Arc<parking_lot::Mutex<Relay>>,
    executor_id: &str,
    dispatch_id: &str,
) -> Result<ExecutorHandle, PoolError> {
    // 模式标记（中继模式）由调用点显式追加，不做「预置参数列表」传递。
    let invocation = executor.clone();
    let (pair, mut child, child_side) = spawn_executor_process(
        &invocation,
        Some(&format!("agent:{executor_id}")),
        &["--remote-relay"],
    )
    .map_err(|e| PoolError::Bind(format!("spawn executor: {e}")))?;
    drop(child_side);
    let mut transport = FrameTransport::spawn(pair, 64);
    let hello = tokio::time::timeout(
        Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
        master_handshake(
            &mut transport,
            journal_id,
            master_epoch,
            executor_id,
            &format!("local-{dispatch_id}"),
            None,
        ),
    )
    .await
    .map_err(|_| {
        let _ = child.start_kill();
        PoolError::Bind("local handshake timeout".into())
    })?
    .map_err(|e| {
        let _ = child.start_kill();
        PoolError::Bind(format!("local handshake: {e}"))
    })?;
    let executor_boot_id = hello.boot_id;
    // 登记归属（R0：确认后才允许 Execute 转发）。
    relay
        .lock()
        .bind(dispatch_id, executor_id, &executor_boot_id);
    let (inbox, inbox_rx) = mpsc::channel::<Message>(64);
    let (out_tx, out) = mpsc::channel::<Message>(PER_EXECUTOR_QUEUE);
    let cancel = CancellationToken::new();
    let last_acked = Arc::new(AtomicU64::new(0));
    let result = Arc::new(Mutex::new(None));
    let forced_kill = Arc::new(AtomicBool::new(false));
    let lost_data = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(Notify::new());
    tokio::spawn(executor_loop(
        dispatch_id.to_string(),
        transport,
        child,
        inbox_rx,
        out_tx,
        cancel.clone(),
        last_acked.clone(),
        result.clone(),
        forced_kill.clone(),
        lost_data.clone(),
        done.clone(),
        committed.clone(),
        notify.clone(),
    ));
    Ok(ExecutorHandle {
        dispatch_id: dispatch_id.to_string(),
        executor_id: executor_id.to_string(),
        executor_boot_id,
        inbox,
        out,
        cancel,
        last_acked,
        result,
        forced_kill,
        lost_data,
        done,
        committed,
        notify,
    })
}

/// 执行器会话循环：本地转发 + 事实旁路记录 + 取消回收（kill/reap）。
/// 五个旁路事实句柄独立原子量（每执行器一份，取消/回收路径各自置位），
/// 打包成结构只会把返回值式访问换成 lock；调用点就一处，保持位置参数。
#[allow(clippy::too_many_arguments)]
async fn executor_loop(
    dispatch_id: String,
    mut transport: FrameTransport,
    mut child: tokio::process::Child,
    mut inbox: mpsc::Receiver<Message>,
    out: mpsc::Sender<Message>,
    cancel: CancellationToken,
    last_acked: Arc<AtomicU64>,
    result: Arc<Mutex<Option<(String, u64)>>>,
    forced_kill: Arc<AtomicBool>,
    lost_data: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    committed: Arc<AtomicBool>,
    notify: Arc<Notify>,
) {
    let mut last_seen = tokio::time::Instant::now();
    let heartbeat_timeout = Duration::from_millis(HEARTBEAT_TIMEOUT_MS);
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_timeout,
        heartbeat_timeout,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                forced_kill.store(true, Ordering::Relaxed);
                break;
            }
            _ = heartbeat.tick() => {
                // 执行器失联（心跳停）：与后端本地池同语义——宽限后 kill/reap。
                if last_seen.elapsed() > heartbeat_timeout {
                    if result.lock().await.is_none() {
                        lost_data.store(true, Ordering::Relaxed);
                    }
                    break;
                }
            }
            inbound = inbox.recv() => {
                let Some(message) = inbound else { break };
                if let Message::AuditAck { durable_audit_seq, .. } = &message {
                    last_acked.fetch_max(*durable_audit_seq, Ordering::Relaxed);
                }
                // The journal has committed the result; this per-dispatch process can be reaped.
                if matches!(message, Message::ResultCommitted { .. }) { committed.store(true, Ordering::Release); break; }
                if transport.send(message).await.is_err() { break; }
            }
            received = transport.recv() => {
                last_seen = tokio::time::Instant::now();
                match received {
                    Ok(message) => {
                        // 执行器心跳不带 dispatch_id（会话级）：补上本派发
                        // 归属，否则上联包装器按「无归属」丢弃，master 永远
                        // 收不到远程活跃证据（长任务会被误判空闲）。
                        let message = match message {
                            Message::Heartbeat { dispatch_id: None, phase } => Message::Heartbeat {
                                dispatch_id: Some(dispatch_id.clone()),
                                phase,
                            },
                            other => other,
                        };
                        match &message {
                            Message::AuditAck { durable_audit_seq, .. } => {
                                last_acked.fetch_max(*durable_audit_seq, Ordering::Relaxed);
                            }
                            Message::Result { result_id, last_audit_seq, .. } => {
                                *result.lock().await = Some((result_id.clone(), *last_audit_seq));
                            }
                            _ => {}
                        }
                        // 有界出站：满则停止读取（端到端背压，不提前确认）。
                        tokio::select! {
                            _ = cancel.cancelled() => { forced_kill.store(true, Ordering::Relaxed); break; },
                            sent = out.send(message) => { if sent.is_err() { break; } notify.notify_one(); },
                        }
                    }
                    Err(_) => {
                        // 失联且无固定结果：未确认审计窗口视同可能丢失。
                        if result.lock().await.is_none() {
                            lost_data.store(true, Ordering::Relaxed);
                        }
                        break;
                    }
                }
            }
        }
    }
    if !committed.load(Ordering::Acquire) {
        // 回收：尽力 Cancel → 宽限 → kill → wait/reap。
        let _ = transport
            .send(Message::Cancel {
                command_id: format!("agent-cancel-{dispatch_id}"),
                dispatch_id: dispatch_id.clone(),
            })
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(DRAIN_GRACE_MS);
        while let Ok(Ok(message)) = tokio::time::timeout_at(deadline, transport.recv()).await {
            if let Message::Result {
                result_id,
                last_audit_seq,
                ..
            } = &message
            {
                *result.lock().await = Some((result_id.clone(), *last_audit_seq));
            }
            let stopped = matches!(message, Message::Stopped { .. });
            if !matches!(
                tokio::time::timeout_at(deadline, out.send(message)).await,
                Ok(Ok(()))
            ) || stopped
            {
                break;
            }
            notify.notify_one();
        }
    }
    transport.close();
    let _ = child.start_kill();
    let _ = child.wait().await;
    done.store(true, Ordering::Relaxed);
}
