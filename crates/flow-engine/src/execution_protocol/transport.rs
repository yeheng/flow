//! 传输边界与会话校验（postcard 单流帧）。
//!
//! 唯一承载形态：`spawn` 在一条双向流上复用全部消息（帧 = `u32 body_len |
//! postcard(Message)`，见 [`super::frame`]）。单写任务保证帧按提交序串行
//! 发出——Execute 先于其 TransferChunk、RequestOperation 晚于其请求块，
//! 跨通道乱序在物理上不存在，双方无需重组暂存。执行器本地 IPC 与 agent
//! 上联同用本类型。
//!
//! 每条连接一个串行帧写入器（单写任务按序写帧，避免并发写导致帧交错），
//! 读任务独立于节点执行持续运行（主进程不能等节点执行返回才读取审计）。
//! 不绑定 `.sock` 路径、不起本地 listener、无服务发现。

use tokio::sync::mpsc;

use super::contract::{
    DRAIN_GRACE_MS, HEARTBEAT_INTERVAL_MS, IDENTITY_MAX_BYTES, MAX_AUDIT_RECORD_BYTES,
    PROTOCOL_VERSION, REQUIRED_CAPABILITIES, W_BYTES,
};
use super::frame::{encode_frame, FrameDecoder};
use super::message::{Message, ProtocolError};

/// 一条通道的读任务产物。`Frame` 装箱：`Message` 最大变体携带完整
/// `ExecuteTask`（数百字节），装箱后事件枚举与队列槽位收缩到指针大小——
/// I/O 事件量等于消息流量，不装箱等于每帧复制整个 payload。
#[allow(dead_code)]
enum IoEvent {
    Frame(Box<Message>),
    Failed(ProtocolError),
    Eof,
}

/// 消息级传输端点：业务层唯一的收/发/关闭边界。
pub struct FrameTransport {
    incoming: mpsc::Receiver<IoEvent>,
    pending_error: Option<ProtocolError>,
    ended: bool,
    outbound: mpsc::Sender<Message>,
    io: IoHandle,
}

struct IoHandle {
    readers: Vec<tokio::task::JoinHandle<()>>,
    writers: Vec<tokio::task::JoinHandle<()>>,
    /// 会话共享故障状态（T03）：任一 I/O 方向失败即整会话终止。
    state: std::sync::Arc<SessionState>,
}

/// 会话级故障状态。写任务失败没有自然的事件通道可走——读事件队列归读端
/// 消费，而读端可能正永久 pending 在一个永远不完成的 read 上。失败事实
/// 必须绕开这个死角直达 `recv`，并终止读写两个任务。
struct SessionState {
    /// 出错后置位：拒绝继续发送；读任务据此退出读循环。
    broken: std::sync::atomic::AtomicBool,
    /// 首个失败的真实错误（后续 `recv` 观察到它，而不是笼统的 Closed）。
    error: parking_lot::Mutex<Option<ProtocolError>>,
    /// `recv` 的失败唤醒。notify_one 在无等待者时留存许可，不依赖
    /// `recv` 恰好已经在等待。
    failed: tokio::sync::Notify,
    /// 会话终止信号：唤醒阻塞中的读任务与写任务（close 同样触发）。
    terminate: tokio_util::sync::CancellationToken,
}

impl SessionState {
    /// 会话停止但无错误事实（EOF / 主动 close）：只终止任务，不占用错误槽。
    fn stop(&self) {
        self.broken.store(true, std::sync::atomic::Ordering::SeqCst);
        self.terminate.cancel();
    }

    /// 记录会话失败（首个错误保留）：停止任务并唤醒 `recv`。
    fn fail(&self, error: ProtocolError) {
        self.stop();
        let mut slot = self.error.lock();
        if slot.is_none() {
            *slot = Some(error);
        }
        drop(slot);
        self.failed.notify_one();
    }
}

impl FrameTransport {
    /// 单流承载：`u32 body_len | postcard(Message)`；发送队列一条，
    /// 帧序即提交序。
    pub fn spawn<S>(stream: S, queue_depth: usize) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (event_tx, incoming) = mpsc::channel(queue_depth.max(8));
        let (outbound, outbound_rx) = mpsc::channel::<Message>(queue_depth.max(8));
        let state = std::sync::Arc::new(SessionState {
            broken: std::sync::atomic::AtomicBool::new(false),
            error: parking_lot::Mutex::new(None),
            failed: tokio::sync::Notify::new(),
            terminate: tokio_util::sync::CancellationToken::new(),
        });
        let (read_half, write_half) = tokio::io::split(stream);
        let read_half: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> = Box::pin(read_half);
        let reader = tokio::spawn(read_loop(read_half, event_tx.clone(), state.clone()));
        let writer = tokio::spawn(write_loop(
            Box::pin(write_half) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>,
            outbound_rx,
            state.clone(),
        ));
        drop(event_tx);
        Self {
            incoming,
            pending_error: None,
            ended: false,
            outbound,
            io: IoHandle {
                readers: vec![reader],
                writers: vec![writer],
                state,
            },
        }
    }

    /// 发送一条消息。会话故障（读写任一失败/close）后阻塞中的发送也会被
    /// 释放：写任务退出丢弃出站队列，等待中的 send 以 Closed 返回。
    pub async fn send(&self, message: Message) -> Result<(), ProtocolError> {
        if self
            .io
            .state
            .broken
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ProtocolError::Closed);
        }
        self.outbound
            .send(message)
            .await
            .map_err(|_| ProtocolError::Closed)
    }

    /// 接收下一条消息。EOF/错误时返回 [`ProtocolError::Closed`]；
    /// 协议帧错误（非法长度/损坏 postcard 体）先返回错误，随后通道关闭。
    /// 写任务失败时没有读事件可等：失败信号直达（已排队的帧先于错误交付）。
    pub async fn recv(&mut self) -> Result<Message, ProtocolError> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if self.ended {
            return Err(ProtocolError::Closed);
        }
        let event = tokio::select! {
            biased;
            event = self.incoming.recv() => event,
            _ = self.io.state.failed.notified() => {
                self.ended = true;
                return Err(self
                    .io
                    .state
                    .error
                    .lock()
                    .take()
                    .unwrap_or(ProtocolError::Closed));
            }
        };
        match event {
            Some(IoEvent::Frame(message)) => Ok(*message),
            Some(IoEvent::Failed(error)) => {
                self.ended = true;
                Err(error)
            }
            Some(IoEvent::Eof) | None => {
                self.ended = true;
                // 通道关闭（读/写任务退出）可能先于失败通知被 poll：
                // 已记录的真实错误优先于笼统的 Closed。
                Err(self
                    .io
                    .state
                    .error
                    .lock()
                    .take()
                    .unwrap_or(ProtocolError::Closed))
            }
        }
    }

    /// 非阻塞发送（队列满/已关即失败；agent 拒绝路径使用）。
    pub fn try_send(&self, message: Message) -> Result<(), ProtocolError> {
        if self
            .io
            .state
            .broken
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ProtocolError::Closed);
        }
        self.outbound
            .try_send(message)
            .map_err(|_| ProtocolError::Closed)
    }

    /// 非阻塞发送，队列满时原样退回消息（服务循环积压重试用）。
    /// 成功 `Ok(None)`；满 `Ok(Some(message))`（调用方保留重试，不得丢弃）；
    /// 会话已关/写任务已退 `Err(Closed)`。丢弃式调用方用 [`Self::try_send`]。
    pub fn try_send_requeue(&self, message: Message) -> Result<Option<Message>, ProtocolError> {
        if self
            .io
            .state
            .broken
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ProtocolError::Closed);
        }
        match self.outbound.try_send(message) {
            Ok(()) => Ok(None),
            Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => Ok(Some(message)),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(ProtocolError::Closed),
        }
    }

    /// 尽力先弹出已缓冲帧；无则 None（非阻塞窥视）。
    pub fn try_recv(&mut self) -> Option<Message> {
        if self.ended {
            return None;
        }
        match self.incoming.try_recv() {
            Ok(IoEvent::Frame(message)) => Some(*message),
            Ok(IoEvent::Failed(error)) => {
                self.ended = true;
                self.pending_error = Some(error);
                None
            }
            Ok(IoEvent::Eof) | Err(mpsc::error::TryRecvError::Disconnected) => {
                self.ended = true;
                self.pending_error = Some(ProtocolError::Closed);
                None
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
        }
    }

    /// Finish queued frames before closing. The deadline also bounds blocked writers.
    pub async fn flush_and_close(mut self, timeout: std::time::Duration) {
        self.outbound = dead_sender();
        let _ = tokio::time::timeout(timeout, drain_tasks(&mut self.io.writers)).await;
        self.close();
        for task in self.io.readers.iter().chain(self.io.writers.iter()) {
            task.abort();
        }
        for task in self.io.readers.drain(..).chain(self.io.writers.drain(..)) {
            task.abort();
            let _ = task.await;
        }
    }

    /// 关闭全部通道并停止 I/O 任务。幂等。终止信号唤醒阻塞中的读/写任务，
    /// 不依赖对端 EOF 或后续流量。
    pub fn close(&mut self) {
        self.ended = true;
        self.io.state.stop();
        // 丢弃发送端使写任务退出并 shutdown 流；用死通道占位保持类型。
        self.outbound = dead_sender();
    }

    /// 等待全部 I/O 任务退出（关闭后回收）。
    pub async fn join(mut self) {
        drain_tasks(&mut self.io.readers).await;
        drain_tasks(&mut self.io.writers).await;
    }
}

impl Drop for FrameTransport {
    fn drop(&mut self) {
        self.outbound = dead_sender();
        let readers = std::mem::take(&mut self.io.readers);
        for task in &readers {
            task.abort();
        }
        let mut writers = std::mem::take(&mut self.io.writers);
        let state = self.io.state.clone();
        if readers.is_empty() && writers.is_empty() {
            state.stop();
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(DRAIN_GRACE_MS),
                    drain_tasks(&mut writers),
                )
                .await;
                state.stop();
                for task in readers.into_iter().chain(writers) {
                    task.abort();
                    let _ = task.await;
                }
            });
        } else {
            state.stop();
            for task in writers {
                task.abort();
            }
        }
    }
}

/// Remove each completed handle before polling another one. If interrupted by a
/// deadline/cancellation, the owner still holds every handle that needs reaping.
async fn drain_tasks(tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    while let Some(task) = tasks.last_mut() {
        let _ = task.await;
        tasks.pop();
    }
}

async fn deliver(
    events: &mpsc::Sender<IoEvent>,
    event: IoEvent,
    state: &SessionState,
) -> Result<(), ()> {
    tokio::select! {
        result = events.send(event) => result.map_err(|_| ()),
        _ = state.terminate.cancelled() => Err(()),
    }
}

/// 已关闭的死发送通道（close() 后占位）。
fn dead_sender() -> mpsc::Sender<Message> {
    let (tx, mut rx) = mpsc::channel(1);
    rx.close();
    tx
}

/// 单流读任务：会话在别处失败（写任务/close）时由终止信号唤醒退出，
/// 不等对端 EOF。
async fn read_loop(
    stream: impl tokio::io::AsyncRead + Unpin + Send,
    events: mpsc::Sender<IoEvent>,
    state: std::sync::Arc<SessionState>,
) {
    use tokio::io::AsyncReadExt;
    let mut stream = stream;
    let mut decoder = FrameDecoder::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        // 会话已在别处终止：不再发起新的读。
        if state.broken.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let read = tokio::select! {
            biased;
            read = stream.read(&mut buffer) => read,
            _ = state.terminate.cancelled() => return,
        };
        match read {
            Ok(0) => {
                let _ = deliver(&events, IoEvent::Eof, &state).await;
                state.stop();
                return;
            }
            Ok(n) => {
                if let Err(error) = decoder.feed(&buffer[..n]) {
                    // 错误事实随事件送达（优先于后续排队帧）；会话停止。
                    let _ = deliver(&events, IoEvent::Failed(error), &state).await;
                    state.stop();
                    return;
                }
                loop {
                    let popped = match decoder.pop() {
                        Ok(popped) => popped,
                        Err(error) => {
                            let _ = deliver(&events, IoEvent::Failed(error), &state).await;
                            state.stop();
                            return;
                        }
                    };
                    let Some(message) = popped else {
                        break;
                    };
                    if deliver(&events, IoEvent::Frame(Box::new(message)), &state)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) => {
                let _ = deliver(&events, IoEvent::Failed(ProtocolError::Io(error)), &state).await;
                // 读失败的错误事实已随事件送达；这里只停会话，不覆盖错误槽。
                state.stop();
                return;
            }
        }
    }
}

/// 单流写任务：按提交序串行写帧；观测帧编码失败丢帧计数，其余断线。
async fn write_loop(
    stream: impl tokio::io::AsyncWrite + Unpin + Send,
    queue: mpsc::Receiver<Message>,
    state: std::sync::Arc<SessionState>,
) {
    use tokio::io::AsyncWriteExt;
    let mut stream = stream;
    let mut queue = queue;
    loop {
        let message = tokio::select! {
            biased;
            message = queue.recv() => {
                match message {
                    Some(message) => message,
                    None => break,
                }
            }
            // 会话在别处失败（读任务/close）：立即退出，释放阻塞中的发送方。
            _ = state.terminate.cancelled() => {
                tracing::warn!("writer exit: session broken");
                return;
            }
        };
        if state.broken.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!(
                type_name = message.type_name(),
                "writer exit: session broken"
            );
            return;
        }
        let frame = match encode_frame(&message) {
            Ok(frame) => frame,
            Err(error) => {
                // 观测是可丢弃数据：编码失败丢帧计数，绝不为它拆除
                // golden source 会话（审计/结果/传输帧失败仍然断线——
                // 那是正确性路径，该炸就炸）。
                if !matches!(message, Message::ObservabilityBatch { .. }) {
                    tracing::error!(type_name = message.type_name(), %error, "frame encode failed");
                    state.fail(error);
                    return;
                }
                tracing::warn!(type_name = message.type_name(), %error, "observability frame dropped");
                continue;
            }
        };
        let written = tokio::select! {
            result = stream.write_all(&frame) => result,
            _ = state.terminate.cancelled() => return,
        };
        if let Err(error) = written {
            tracing::error!(type_name = message.type_name(), %error, "frame write failed");
            state.fail(ProtocolError::Io(error));
            return;
        }
        let flushed = tokio::select! {
            result = stream.flush() => result,
            _ = state.terminate.cancelled() => return,
        };
        if let Err(error) = flushed {
            tracing::error!(type_name = message.type_name(), %error, "frame flush failed");
            state.fail(ProtocolError::Io(error));
            return;
        }
    }
    tokio::select! {
        _ = stream.shutdown() => {},
        _ = state.terminate.cancelled() => {},
    }
}

/// 有界出站积压（T03）：服务循环在传输队列满时按 FIFO 暂存重试，避免
/// 阻塞中的 `send` 卡住整个 select——入站 Cancel/ACK/心跳始终可被处理。
/// 积压占用期间调用方应停止拉取新出站消息（自然背压到生产方）；容量
/// 超限视为会话失效（不用无界队列化解背压）。序保持：积压非空时新消息
/// 一律排在积压之后。
pub struct SendBacklog {
    queue: std::collections::VecDeque<Message>,
    cap: usize,
}

impl SendBacklog {
    /// `cap` 为积压上限（建议与队列深度同量级的小常数）。
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            queue: std::collections::VecDeque::new(),
            cap: cap.max(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// 入队一条消息（积压空则直发）。`Err` 表示会话不可用（写任务退出或
    /// 积压超限），调用方应终止会话路径。
    pub fn push(
        &mut self,
        transport: &FrameTransport,
        message: Message,
    ) -> Result<(), ProtocolError> {
        if self.queue.is_empty() {
            match transport.try_send_requeue(message) {
                Ok(None) => Ok(()),
                Ok(Some(message)) => {
                    self.queue.push_back(message);
                    Ok(())
                }
                Err(error) => Err(error),
            }
        } else if self.queue.len() < self.cap {
            self.queue.push_back(message);
            Ok(())
        } else {
            Err(ProtocolError::Closed)
        }
    }

    /// 重试冲刷积压（队列满则保留剩余）。`Err` 表示会话不可用。
    pub fn flush(&mut self, transport: &FrameTransport) -> Result<(), ProtocolError> {
        while let Some(message) = self.queue.pop_front() {
            match transport.try_send_requeue(message) {
                Ok(None) => continue,
                Ok(Some(message)) => {
                    self.queue.push_front(message);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

/// 会话身份（二期 §3.2 表）。保存在会话上下文；任务帧只带 dispatch_id。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    pub journal_id: String,
    pub master_epoch: u64,
    pub executor_id: String,
    pub boot_id: String,
    pub session_id: String,
}

/// 握手结果（主进程视角）。
#[derive(Debug, Clone)]
pub struct ExecutorHello {
    pub boot_id: String,
    pub build: String,
    pub capabilities: Vec<String>,
}

fn validate_identity(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > IDENTITY_MAX_BYTES {
        return Err(ProtocolError::Malformed("invalid identity length".into()));
    }
    Ok(())
}

/// 校验 Hello 的会话令牌与主进程期望值一致。
///
/// loopback 回连承载（非 unix 本地执行）在 spawn 时经 env 注入随机令牌：
/// 回连端口只在本机可见，但毫秒窗口内任何本机进程都可能抢连——抢连者
/// 拿不到注入给执行器的令牌。unix socketpair 走 FD 能力（exec 前已关闭
/// 其余继承 FD），不需要令牌，双方都是 `None`。
fn validate_session_token(
    presented: Option<&str>,
    expected: Option<&str>,
) -> Result<(), ProtocolError> {
    match (presented, expected) {
        (Some(presented), Some(expected)) if presented == expected => Ok(()),
        (None, None) => Ok(()),
        _ => Err(ProtocolError::Malformed(
            "session token mismatch on loopback channel".into(),
        )),
    }
}

/// 主进程侧握手：接收 Hello（校验版本/能力/boot 唯一性/会话令牌），回
/// Welcome，等待 Ready。任一步失败即明确拒绝，不降级。版本门集中在
/// 此处（帧不再自带版本号）。
pub async fn master_handshake(
    transport: &mut FrameTransport,
    journal_id: &str,
    master_epoch: u64,
    executor_id: &str,
    session_id: &str,
    expected_session_token: Option<&str>,
) -> Result<ExecutorHello, ProtocolError> {
    let message = tokio::time::timeout(
        std::time::Duration::from_millis(super::contract::STARTUP_TIMEOUT_MS),
        transport.recv(),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("handshake timeout".into()))??;
    let Message::Hello {
        version,
        boot_id,
        build,
        capabilities,
        session_token,
    } = message
    else {
        return Err(ProtocolError::Malformed(format!(
            "expected Hello as first message, got {}",
            message.type_name()
        )));
    };
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::Malformed(format!(
            "protocol version {version} not supported (expected {PROTOCOL_VERSION})"
        )));
    }
    validate_identity(&boot_id)?;
    validate_identity(&build)?;
    validate_session_token(session_token.as_deref(), expected_session_token)?;
    for capability in REQUIRED_CAPABILITIES {
        if !capabilities.iter().any(|c| c == capability) {
            return Err(ProtocolError::Malformed(format!(
                "executor missing required capability: {capability}"
            )));
        }
    }
    transport
        .send(Message::Welcome {
            version: PROTOCOL_VERSION,
            journal_id: journal_id.into(),
            master_epoch,
            session_id: session_id.into(),
            executor_id: executor_id.into(),
            window_bytes: W_BYTES,
            max_record_bytes: MAX_AUDIT_RECORD_BYTES,
            heartbeat_ms: HEARTBEAT_INTERVAL_MS,
            drain_grace_ms: DRAIN_GRACE_MS,
        })
        .await?;
    let message = tokio::time::timeout(
        std::time::Duration::from_millis(super::contract::STARTUP_TIMEOUT_MS),
        transport.recv(),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("Ready timeout".into()))??;
    match message {
        Message::Ready => Ok(ExecutorHello {
            boot_id,
            build,
            capabilities,
        }),
        other => Err(ProtocolError::Malformed(format!(
            "expected Ready, got {}",
            other.type_name()
        ))),
    }
}

/// 执行器侧握手：发 Hello（携带本端协议版本与会话令牌），校验 Welcome
/// 版本与限额 ≤ 本端硬上限，回 Ready。
pub async fn executor_handshake(
    transport: &mut FrameTransport,
    build: &str,
    capabilities: &[&str],
    session_token: Option<String>,
) -> Result<(SessionIdentity, NegotiatedLimits), ProtocolError> {
    let boot_id = fresh_boot_id();
    transport
        .send(Message::hello(
            boot_id.clone(),
            build,
            capabilities.iter().map(|s| s.to_string()).collect(),
            session_token,
        ))
        .await?;
    let message = tokio::time::timeout(
        std::time::Duration::from_millis(super::contract::STARTUP_TIMEOUT_MS),
        transport.recv(),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("handshake timeout".into()))??;
    let Message::Welcome {
        version,
        journal_id,
        master_epoch,
        session_id,
        executor_id,
        window_bytes,
        max_record_bytes,
        heartbeat_ms,
        drain_grace_ms,
    } = message
    else {
        return Err(ProtocolError::Malformed("expected Welcome".into()));
    };
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::Malformed(format!(
            "master protocol version {version} not supported (expected {PROTOCOL_VERSION})"
        )));
    }
    for identity in [&journal_id, &session_id, &executor_id] {
        validate_identity(identity)?;
    }
    if window_bytes > W_BYTES
        || max_record_bytes > MAX_AUDIT_RECORD_BYTES
        || max_record_bytes > window_bytes
    {
        return Err(ProtocolError::Malformed(
            "welcome limits exceed local hard caps".into(),
        ));
    }
    transport.send(Message::Ready).await?;
    Ok((
        SessionIdentity {
            journal_id,
            master_epoch,
            executor_id,
            boot_id,
            session_id,
        },
        NegotiatedLimits {
            window_bytes,
            max_record_bytes,
            heartbeat_ms,
            drain_grace_ms,
        },
    ))
}

/// Welcome 协商的限额快照。
#[derive(Debug, Clone, Copy)]
pub struct NegotiatedLimits {
    pub window_bytes: u64,
    pub max_record_bytes: u64,
    pub heartbeat_ms: u64,
    pub drain_grace_ms: u64,
}

/// 协议版本常量重导出（握手层比对用）。
pub const VERSION: u32 = PROTOCOL_VERSION;

/// 生成 16 字节随机 boot_id（hex）。getrandom 全平台统一（含 Windows），
/// 失败即 OS 熵系统故障——立即暴露，不做时间+计数退化：boot_id 的唯一性
/// 是会话身份与失联判定的事实来源，弱身份比崩溃更糟。
pub fn fresh_boot_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("os entropy unavailable");
    hex::encode(bytes)
}

#[cfg(all(test, unix))]
mod tests {
    use super::super::contract::{MAX_AUDIT_RECORD_BYTES, REQUIRED_CAPABILITIES, W_BYTES};
    use super::super::unix::socketpair_channel;
    use super::*;
    use std::os::fd::FromRawFd;

    async fn pair() -> (FrameTransport, FrameTransport) {
        let (parent, child) = socketpair_channel().unwrap();
        (
            FrameTransport::spawn(parent, 16),
            FrameTransport::spawn(child, 16),
        )
    }

    #[tokio::test]
    async fn handshake_roundtrip_and_context_identity() {
        let (mut master, mut worker) = pair().await;
        let worker_task = tokio::spawn(async move {
            let (identity, limits) =
                executor_handshake(&mut worker, "0.1.0", REQUIRED_CAPABILITIES, None)
                    .await
                    .expect("executor handshake");
            (identity, limits)
        });
        let hello = master_handshake(&mut master, "journal-1", 7, "exec-1", "session-1", None)
            .await
            .expect("master handshake");
        assert_eq!(hello.build, "0.1.0");
        let (identity, limits) = worker_task.await.unwrap();
        assert_eq!(identity.master_epoch, 7);
        assert_eq!(identity.session_id, "session-1");
        assert_eq!(identity.executor_id, "exec-1");
        assert_eq!(identity.boot_id, hello.boot_id);
        assert_eq!(limits.window_bytes, W_BYTES);
        assert_eq!(limits.max_record_bytes, MAX_AUDIT_RECORD_BYTES);
    }

    #[tokio::test]
    async fn missing_capability_rejected() {
        let (mut master, mut worker) = pair().await;
        let worker_task = tokio::spawn(async move {
            // 只声明 execute，缺 js/http/transfer
            executor_handshake(&mut worker, "0.1.0", &["execute"], None).await
        });
        let error = master_handshake(&mut master, "journal-1", 1, "e", "s", None)
            .await
            .expect_err("must reject missing capability");
        assert!(error.to_string().contains("required capability"));
        let _ = worker_task.await;
    }

    #[tokio::test]
    async fn first_message_not_hello_rejected() {
        let (mut a, mut _worker) = pair().await;
        _worker.send(Message::Ready).await.unwrap();
        let error = master_handshake(&mut a, "j", 1, "e", "s", None)
            .await
            .expect_err("non-hello rejected");
        assert!(error.to_string().contains("expected Hello"));
    }

    /// loopback 会话令牌：正确令牌通过；缺失/错值一律拒绝（抢连防御）。
    #[tokio::test]
    async fn session_token_roundtrip_and_mismatch_rejected() {
        let (mut master, mut worker) = pair().await;
        let worker_task = tokio::spawn(async move {
            executor_handshake(
                &mut worker,
                "0.1.0",
                REQUIRED_CAPABILITIES,
                Some("tok".into()),
            )
            .await
        });
        master_handshake(&mut master, "j", 1, "e", "s", Some("tok"))
            .await
            .expect("matching token accepted");
        worker_task.await.unwrap().expect("executor handshake");

        let (mut master, mut worker) = pair().await;
        let worker_task = tokio::spawn(async move {
            executor_handshake(&mut worker, "0.1.0", REQUIRED_CAPABILITIES, None).await
        });
        let error = master_handshake(&mut master, "j", 1, "e", "s", Some("tok"))
            .await
            .expect_err("missing token rejected");
        assert!(error.to_string().contains("session token"), "{error}");
        let _ = worker_task.await;

        let (mut master, mut worker) = pair().await;
        let worker_task = tokio::spawn(async move {
            executor_handshake(
                &mut worker,
                "0.1.0",
                REQUIRED_CAPABILITIES,
                Some("wrong".into()),
            )
            .await
        });
        let error = master_handshake(&mut master, "j", 1, "e", "s", Some("tok"))
            .await
            .expect_err("wrong token rejected");
        assert!(error.to_string().contains("session token"), "{error}");
        let _ = worker_task.await;
    }

    #[tokio::test]
    async fn single_stream_preserves_submission_order() {
        // 单流承载的契约：发送序 = 到达序。Execute（control 语义）先于其
        // TransferChunk（data 语义）提交，接收端必须按此顺序看到。
        let (a, mut b) = pair().await;
        a.send(Message::Ready).await.unwrap();
        a.send(Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "t".into(),
            offset: 0,
            bytes: b"hi".to_vec(),
            digest: "d".into(),
        })
        .await
        .unwrap();
        let message = b.recv().await.unwrap();
        assert_eq!(message.type_name(), "Ready");
        let message = b.recv().await.unwrap();
        assert_eq!(message.type_name(), "TransferChunk");
    }

    #[tokio::test]
    async fn unknown_variant_frame_fails_session() {
        // 注入变体索引越界的帧体（postcard 目录只有 28 个变体）：读端必须
        // 拒绝并关闭会话。
        let (parent, child) = socketpair_channel().unwrap();
        use tokio::io::AsyncWriteExt;
        let raw = {
            let fd = std::os::fd::AsRawFd::as_raw_fd(&child);
            // SAFETY: dup 只复制句柄，原 stream 生命周期不受影响。
            let dup = unsafe { libc::dup(fd) };
            unsafe { std::os::unix::net::UnixStream::from_raw_fd(dup) }
        };
        let mut _peer = FrameTransport::spawn(child, 8);
        let mut a = FrameTransport::spawn(parent, 8);
        raw.set_nonblocking(true).unwrap();
        let mut raw = tokio::net::UnixStream::from_std(raw).unwrap();
        // body_len=1，变体索引 200（越界）。
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.push(200u8);
        raw.write_all(&frame).await.unwrap();
        let error = a.recv().await.expect_err("unknown variant must fail");
        assert!(error.to_string().contains("postcard"), "{error}");
    }

    #[tokio::test]
    async fn peer_close_surfaces_as_closed() {
        let (mut a, b) = pair().await;
        drop(b);
        // 写端随后失效：send 或 recv 报 Closed。
        let outcome = a.recv().await;
        assert!(matches!(outcome, Err(ProtocolError::Closed)));
    }

    #[tokio::test]
    async fn malformed_frame_fails_reader_explicitly() {
        let (parent, child) = socketpair_channel().unwrap();
        // 用 dup 的独立句柄从对端注入非法长度帧。
        use std::os::fd::AsRawFd;
        let dup_fd = unsafe { libc::dup(child.as_raw_fd()) };
        let evil = unsafe { std::os::unix::net::UnixStream::from_raw_fd(dup_fd) };
        let mut a = FrameTransport::spawn(parent, 8);
        let mut _peer = FrameTransport::spawn(child, 8);
        use tokio::io::AsyncWriteExt;
        evil.set_nonblocking(true).unwrap();
        let mut evil = tokio::net::UnixStream::from_std(evil).unwrap();
        evil.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        let error = a.recv().await.expect_err("illegal length must fail");
        assert!(matches!(error, ProtocolError::FrameTooLarge(_, _)));
    }

    /// 读端永久 pending、写端必错的流：验证写任务失败不再留下永久阻塞的
    /// recv（T03 故障传播）。
    struct DeadWritePendingRead;

    impl tokio::io::AsyncRead for DeadWritePendingRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    impl tokio::io::AsyncWrite for DeadWritePendingRead {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write side dead",
            )))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write side dead",
            )))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write side dead",
            )))
        }
    }

    #[tokio::test]
    async fn writer_failure_wakes_blocked_recv_with_real_error() {
        // 写失败必须唤醒阻塞中的 recv 并携带真实 I/O 错误；后续 recv 稳定
        // Closed，阻塞中的发送方也被释放（写任务退出丢弃出站队列）。
        let mut transport = FrameTransport::spawn(DeadWritePendingRead, 8);
        transport
            .try_send(Message::Ready)
            .expect("queue before writer failure");
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), transport.recv())
            .await
            .expect("recv must wake on writer failure");
        assert!(
            matches!(outcome, Err(ProtocolError::Io(_))),
            "real io error expected: {outcome:?}"
        );
        assert!(matches!(transport.recv().await, Err(ProtocolError::Closed)));
        assert!(matches!(
            transport.try_send(Message::Ready),
            Err(ProtocolError::Closed)
        ));
    }

    #[tokio::test]
    async fn close_terminates_pending_reader_and_recv() {
        // close() 必须让读端永久 pending 的会话稳定收尾：recv 立即 Closed，
        // 读任务由终止信号唤醒退出，不依赖对端 EOF。
        let mut transport = FrameTransport::spawn(DeadWritePendingRead, 8);
        transport.close();
        assert!(matches!(transport.recv().await, Err(ProtocolError::Closed)));
        let reader_finished = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !transport.io.readers[0].is_finished() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(reader_finished.is_ok(), "reader task must exit after close");
    }
}

#[cfg(test)]
mod terminal_event_tests {
    use super::*;
    #[tokio::test]
    async fn close_interrupts_blocked_writer() {
        let (stream, _peer) = tokio::io::duplex(1);
        let mut transport = FrameTransport::spawn(stream, 8);
        transport.send(Message::Ready).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        transport.close();
        tokio::time::timeout(std::time::Duration::from_millis(200), transport.join())
            .await
            .expect("blocked writer must stop");
    }

    #[tokio::test]
    async fn close_interrupts_full_incoming_queue() {
        let (stream, peer) = tokio::io::duplex(4096);
        let mut transport = FrameTransport::spawn(stream, 8);
        let sender = FrameTransport::spawn(peer, 32);
        for _ in 0..24 {
            sender.send(Message::Ready).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        transport.close();
        tokio::time::timeout(std::time::Duration::from_millis(200), transport.join())
            .await
            .expect("full incoming queue must stop");
    }

    #[tokio::test]
    async fn partial_drain_never_repolls_completed_handles() {
        let blocked = tokio::spawn(std::future::pending::<()>());
        let completed = tokio::spawn(async {});
        let mut tasks = vec![blocked, completed];
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            drain_tasks(&mut tasks)
        )
        .await
        .is_err());
        assert_eq!(
            tasks.len(),
            1,
            "completed handles must leave the owner immediately"
        );
        tasks[0].abort();
        drain_tasks(&mut tasks).await;
        assert!(tasks.is_empty());
        drain_tasks(&mut tasks).await;
    }

    #[tokio::test]
    async fn graceful_close_writes_queued_control_frame() {
        use tokio::io::AsyncReadExt;
        let (ca, mut cb) = tokio::io::duplex(4096);
        let transport = FrameTransport::spawn(ca, 8);
        let message = Message::DrainComplete { drained: 3 };
        let expected = super::super::frame::encode_frame(&message).unwrap();
        transport.send(message).await.unwrap();
        transport
            .flush_and_close(std::time::Duration::from_secs(1))
            .await;
        let mut actual = vec![0; expected.len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            cb.read_exact(&mut actual),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn polling_preserves_terminal_errors_and_recv_stays_closed() {
        for error in [
            ProtocolError::Closed,
            ProtocolError::Malformed("bad frame".into()),
        ] {
            let (stream, _peer) = tokio::io::duplex(1024);
            let mut transport = FrameTransport::spawn(stream, 4);
            // Inject the read task event: no scheduling sleep or second EOF can mask the failure.
            let (events, incoming) = mpsc::channel(4);
            transport.incoming = incoming;
            let expected = error.to_string();
            events.send(IoEvent::Failed(error)).await.unwrap();
            assert!(transport.try_recv().is_none());
            assert!(transport.try_recv().is_none());
            assert_eq!(transport.recv().await.unwrap_err().to_string(), expected);
            assert!(matches!(transport.recv().await, Err(ProtocolError::Closed)));
        }
    }
}
