//! 本地执行器绑定池（agent 侧）：BindExecutor → spawn + 本地握手 →
//! BindExecutorAck；取消/断线回收；Resume 事实（最后确认游标/固定结果）。

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::{DRAIN_GRACE_MS, STARTUP_TIMEOUT_MS};
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
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("bind failed: {0}")]
    Bind(String),
}

/// 绑定一个派发到新执行器进程并完成本地握手（确认后才转发 Execute）。
pub async fn bind_executor(
    bin: &Path,
    journal_id: &str,
    master_epoch: u64,
    relay: &Arc<parking_lot::Mutex<Relay>>,
    executor_id: &str,
    dispatch_id: &str,
) -> Result<ExecutorHandle, PoolError> {
    let (pair, mut child, _child_side) =
        spawn_executor_process(bin, Some(&format!("agent:{executor_id}")))
            .map_err(|e| PoolError::Bind(format!("spawn executor: {e}")))?;
    let mut transport = FrameTransport::spawn(pair, 64);
    let hello = tokio::time::timeout(
        Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
        master_handshake(
            &mut transport,
            journal_id,
            master_epoch,
            executor_id,
            &format!("local-{dispatch_id}"),
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
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                forced_kill.store(true, Ordering::Relaxed);
                break;
            }
            inbound = inbox.recv() => {
                let Some(message) = inbound else { break };
                if transport.send(message).await.is_err() {
                    break;
                }
            }
            received = transport.recv() => {
                match received {
                    Ok((_channel, message)) => {
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
                        if out.send(message).await.is_err() {
                            break;
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
    // 回收：尽力 Cancel → 宽限 → kill → wait/reap。
    let _ = transport
        .send(Message::Cancel {
            command_id: format!("agent-cancel-{dispatch_id}"),
            dispatch_id: dispatch_id.clone(),
        })
        .await;
    let _ = tokio::time::timeout(Duration::from_millis(DRAIN_GRACE_MS), transport.recv()).await;
    transport.close();
    let _ = child.start_kill();
    let _ = child.wait().await;
    done.store(true, Ordering::Relaxed);
}
