//! 本地执行器绑定池（agent 侧）：BindExecutor → spawn + 本地握手 →
//! BindExecutorAck；取消/断线回收；Resume 事实（最后确认游标/固定结果）。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("bind failed: {0}")]
    Bind(String),
}

/// 绑定一个派发到新执行器进程并完成本地握手（确认后才转发 Execute）。
pub async fn bind_executor(
    bin: &PathBuf,
    journal_id: &str,
    master_epoch: u64,
    relay: &Arc<std::sync::Mutex<Relay>>,
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
        .unwrap()
        .bind(dispatch_id, executor_id, &executor_boot_id);
    let (inbox, inbox_rx) = mpsc::channel::<Message>(64);
    let (out_tx, out) = mpsc::channel::<Message>(PER_EXECUTOR_QUEUE);
    let cancel = CancellationToken::new();
    let last_acked = Arc::new(AtomicU64::new(0));
    let result = Arc::new(Mutex::new(None));
    tokio::spawn(executor_loop(
        dispatch_id.to_string(),
        transport,
        child,
        inbox_rx,
        out_tx,
        cancel.clone(),
        last_acked.clone(),
        result.clone(),
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
    })
}

/// 执行器会话循环：本地转发 + 事实旁路记录 + 取消回收（kill/reap）。
async fn executor_loop(
    dispatch_id: String,
    mut transport: FrameTransport,
    mut child: tokio::process::Child,
    mut inbox: mpsc::Receiver<Message>,
    out: mpsc::Sender<Message>,
    cancel: CancellationToken,
    last_acked: Arc<AtomicU64>,
    result: Arc<Mutex<Option<(String, u64)>>>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
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
                    Err(_) => break,
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
}
