//! 执行子进程池与可靠回收（二期 §3.1/§3.7/§3.9，I03）。
//!
//! 每进程同一时刻一个任务。通道承载按平台选择：unix 用单 socketpair
//! （argv 传 FD 原号，pre_exec 关闭其余继承 FD）；其余平台用 TCP loopback
//! 单连接回连。父关闭子端、子关闭父端，兄弟进程不继承连接。通道故障 →
//! 会话 Draining → 宽限后 kill → wait/reap；已取消/故障任务的进程统一
//! 回收重建，不直接复用。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::process::Child;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::{
    DRAIN_GRACE_MS, HEARTBEAT_TIMEOUT_MS, STARTUP_TIMEOUT_MS,
};
use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::transport::{master_handshake, FrameTransport};

use super::{FromSession, IpcOptions, ToSession};

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("executor spawn failed: {0}")]
    Spawn(String),
    #[error("executor handshake failed: {0}")]
    Handshake(String),
    #[error("executor session unavailable: {0}")]
    Unavailable(String),
}

/// 裸会话（无信号量份额）：spawn/握手完成即可用。
pub struct RawSession {
    pub to_session: mpsc::Sender<ToSession>,
    pub events: mpsc::Receiver<FromSession>,
    pub executor_id: String,
    pub boot_id: String,
    pub session_id: String,
    pub child_pid: Option<u32>,
}

/// 一个可派发的会话租约：持有 X_max 信号量份额与会话命令通道。
pub struct SessionLease {
    pub to_session: mpsc::Sender<ToSession>,
    pub events: mpsc::Receiver<FromSession>,
    pub executor_id: String,
    pub boot_id: String,
    pub session_id: String,
    pub child_pid: Option<u32>,
    _permit: OwnedSemaphorePermit,
}

pub struct ExecutorPool {
    options: IpcOptions,
    journal_id: String,
    master_epoch: u64,
    slots: Arc<Semaphore>,
    idle: Arc<Mutex<Vec<RawSession>>>,
    executor_counter: AtomicU64,
    session_counter: AtomicU64,
    shutdown: CancellationToken,
}

impl ExecutorPool {
    pub fn new(options: IpcOptions, journal_id: String, master_epoch: u64) -> Arc<Self> {
        let x_max = options.x_max.clamp(1, 16);
        Arc::new(Self {
            options,
            journal_id,
            master_epoch,
            slots: Arc::new(Semaphore::new(x_max)),
            idle: Arc::new(Mutex::new(Vec::new())),
            executor_counter: AtomicU64::new(0),
            session_counter: AtomicU64::new(0),
            shutdown: CancellationToken::new(),
        })
    }

    /// 关停：回收全部会话（kill + wait/reap）。
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let mut idle = self.idle.lock().await;
        while let Some(session) = idle.pop() {
            let _ = session.to_session.send(ToSession::Recycle).await;
        }
    }

    /// 获取一个执行会话：优先复用空闲会话，否则 spawn 新进程并完成握手。
    pub async fn acquire(self: &Arc<Self>) -> Result<SessionLease, PoolError> {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PoolError::Unavailable("pool closed".into()))?;
        loop {
            let reused = self.idle.lock().await.pop();
            if let Some(session) = reused {
                if session.to_session.is_closed() {
                    continue;
                }
                return Ok(SessionLease {
                    to_session: session.to_session,
                    events: session.events,
                    executor_id: session.executor_id,
                    boot_id: session.boot_id,
                    session_id: session.session_id,
                    child_pid: session.child_pid,
                    _permit: permit,
                });
            }
            let session = self.spawn_session().await?;
            return Ok(SessionLease {
                to_session: session.to_session,
                events: session.events,
                executor_id: session.executor_id,
                boot_id: session.boot_id,
                session_id: session.session_id,
                child_pid: session.child_pid,
                _permit: permit,
            });
        }
    }

    /// 派发结束后归还会话（健康时复用；draining/故障时回收重建）。
    pub async fn release(self: &Arc<Self>, lease: SessionLease, healthy: bool) {
        let SessionLease {
            to_session,
            events,
            executor_id,
            boot_id,
            session_id,
            child_pid,
            _permit,
        } = lease;
        if healthy && !self.shutdown.is_cancelled() {
            self.idle.lock().await.push(RawSession {
                to_session,
                events,
                executor_id,
                boot_id,
                session_id,
                child_pid,
            });
        } else {
            let _ = to_session.send(ToSession::Recycle).await;
        }
    }

    async fn spawn_session(self: &Arc<Self>) -> Result<RawSession, PoolError> {
        let executor_id = format!(
            "exec-{}",
            self.executor_counter.fetch_add(1, Ordering::Relaxed)
        );
        let session_id = format!(
            "session-{}",
            self.session_counter.fetch_add(1, Ordering::Relaxed)
        );
        spawn_and_handshake(
            &self.options.executor,
            &self.journal_id,
            self.master_epoch,
            &executor_id,
            &session_id,
            self.options.tag.as_deref(),
        )
        .await
    }
}

/// 平台通道承载：unix socketpair（argv 传 FD 原号，父端关闭子端副本）；
/// 其余平台 TCP loopback 单连接回连。差异在此收口，下游只见 FrameTransport
/// 单流端点（帧内通道标签）。
async fn spawn_transport(
    executor: &flow_engine::execution_protocol::contract::ExecutorInvocation,
    tag: Option<&str>,
) -> Result<(FrameTransport, Child), PoolError> {
    #[cfg(unix)]
    {
        let (parent, child, child_pair) =
            flow_engine::execution_protocol::spawn::spawn_executor_process(executor, tag)
                .map_err(|e| PoolError::Spawn(format!("{}: {e}", executor.program.display())))?;
        // 父进程关闭子端副本（子进程内按 argv 原号持有）。
        drop(child_pair);
        Ok((FrameTransport::spawn_local(parent, 128), child))
    }
    #[cfg(not(unix))]
    {
        let (channel, child) =
            flow_engine::execution_protocol::loopback::spawn_executor_loopback(executor, tag)
                .await
                .map_err(|e| PoolError::Spawn(format!("{}: {e}", executor.program.display())))?;
        Ok((FrameTransport::spawn(channel, 128), child))
    }
}

/// spawn 执行器并完成握手；子进程由会话驱动任务持有并负责 kill/reap。
async fn spawn_and_handshake(
    executor: &flow_engine::execution_protocol::contract::ExecutorInvocation,
    journal_id: &str,
    master_epoch: u64,
    executor_id: &str,
    session_id: &str,
    tag: Option<&str>,
) -> Result<RawSession, PoolError> {
    let (mut transport, mut child) = spawn_transport(executor, tag).await?;
    let child_pid = child.id();
    let hello = tokio::time::timeout(
        Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
        master_handshake(
            &mut transport,
            journal_id,
            master_epoch,
            executor_id,
            session_id,
        ),
    )
    .await
    .map_err(|_| {
        let _ = child.start_kill();
        PoolError::Handshake("handshake timeout".into())
    })?
    .map_err(|error| {
        let _ = child.start_kill();
        PoolError::Handshake(error.to_string())
    })?;
    let boot_id = hello.boot_id.clone();
    let (to_session, command_rx) = mpsc::channel::<ToSession>(64);
    let (event_tx, events) = mpsc::channel::<FromSession>(256);
    tokio::spawn(session_driver(
        transport,
        child,
        command_rx,
        event_tx,
        executor_id.to_string(),
    ));
    Ok(RawSession {
        to_session,
        events,
        executor_id: executor_id.to_string(),
        boot_id,
        session_id: session_id.to_string(),
        child_pid,
    })
}

/// 会话驱动：独占 transport 与子进程句柄；命令发送、事件转发、死亡回收。
async fn session_driver(
    mut transport: FrameTransport,
    mut child: Child,
    mut command_rx: mpsc::Receiver<ToSession>,
    event_tx: mpsc::Sender<FromSession>,
    executor_id: String,
) {
    let mut draining = false;
    let mut last_seen = tokio::time::Instant::now();
    let timeout = Duration::from_millis(HEARTBEAT_TIMEOUT_MS);
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + timeout, timeout);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(ToSession::Send(message)) => {
                        if transport.send(*message).await.is_err() {
                            break;
                        }
                    }
                    Some(ToSession::CancelDispatch(dispatch_id)) => {
                        let _ = transport
                            .send(Message::Cancel {
                                command_id: format!("cancel-{dispatch_id}"),
                                dispatch_id,
                            })
                            .await;
                        draining = true;
                    }
                    Some(ToSession::Recycle) | None => {
                        transport.close();
                        break;
                    }
                }
            }
            received = transport.recv() => {
                match received {
                    Ok((_channel, message)) => {
                        last_seen = tokio::time::Instant::now();
                        if matches!(message, Message::Heartbeat { .. }) {
                            continue;
                        }
                        if event_tx
                            .send(FromSession::Incoming(Box::new(message)))
                            .await
                            .is_err()
                        {
                            // 派发端已离开：保留会话等待下一个租约或回收。
                        }
                    }
                    Err(error) => {
                        let _ = event_tx
                            .send(FromSession::Dead(format!("channel failed: {error}")))
                            .await;
                        break;
                    }
                }
            }
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > timeout && !draining {
                    let _ = event_tx
                        .send(FromSession::Dead("heartbeat timeout".into()))
                        .await;
                    break;
                }
            }
        }
    }
    // 回收：宽限期收尾（尽力 Stopped/迟到审计）→ kill → wait/reap。
    let grace = Duration::from_millis(DRAIN_GRACE_MS);
    let _ = tokio::time::timeout(grace, async {
        while let Ok((_channel, message)) = transport.recv().await {
            if event_tx
                .send(FromSession::Incoming(Box::new(message)))
                .await
                .is_err()
            {
                break;
            }
        }
    })
    .await;
    let _ = child.start_kill();
    let status = child.wait().await; // wait/reap：不留僵尸
    if let Ok(status) = status {
        tracing::debug!(%executor_id, %status, "executor reaped");
    }
    tracing::debug!(%executor_id, "executor session recycled");
    drop(transport);
}
