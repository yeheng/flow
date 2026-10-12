//! 传输边界与会话校验（二期 §3.1/§3.2；v3 单流收敛）。
//!
//! 两种承载共享本类型：
//!
//! - **单流（执行器本地 IPC，v3）**：`spawn` 在一条双向流上复用 control 与
//!   data（帧内 1 字节通道标签，见 [`super::frame`]）。单写任务保证帧按
//!   提交序串行发出——Execute 先于其 TransferChunk、RequestOperation 晚于
//!   其请求块，跨通道乱序在物理上不存在，双方无需重组暂存。
//! - **双流（二期遗留，已无生产调用方）**：`spawn_streams` 在两条独立流上
//!   承载 control（握手、派发、取消、确认、心跳，禁止大载荷）与 data（输入
//!   分块、审计批次、观测，独立预算类别）。v3 起 agent 上联与本地 IPC 都用
//!   单流 [`FrameTransport::spawn`]，本方法保留供测试与外部兼容。
//!
//! 每条连接一个串行帧写入器（单写任务按序写帧，避免并发写导致帧交错），
//! 读任务独立于节点执行持续运行（主进程不能等节点执行返回才读取审计）。
//! 不绑定 `.sock` 路径、不起本地 listener、无服务发现。

use tokio::sync::mpsc;

use super::contract::{
    CONTROL_MAX_FRAME, DATA_MAX_FRAME, DRAIN_GRACE_MS, HEARTBEAT_INTERVAL_MS, IDENTITY_MAX_BYTES,
    MAX_AUDIT_RECORD_BYTES, PROTOCOL_VERSION, REQUIRED_CAPABILITIES, W_BYTES,
};
use super::frame::{encode_frame, encode_tagged_frame, FrameDecoder};
use super::message::{Channel, Message, ProtocolError};

/// 一条通道的读任务产物。`Frame` 装箱：`Message` 最大变体携带完整
/// `ExecuteTask`（数百字节），装箱后事件枚举与队列槽位收缩到指针大小——
/// I/O 事件量等于消息流量，不装箱等于每帧复制整个 payload。
/// `Failed`/`Eof` 只经 recv 消费，构造态字段本身不被读；allow 随装箱后
/// 变体差异收窄仍保留（错误码分支保持不变）。
#[allow(dead_code)]
enum IoEvent {
    Frame(Channel, Box<Message>),
    Failed(Channel, ProtocolError),
    Eof(Channel),
}

/// 消息级传输端点：业务层唯一的收/发/关闭边界。
pub struct FrameTransport {
    incoming: mpsc::Receiver<IoEvent>,
    pending_error: Option<ProtocolError>,
    ended: bool,
    control_out: mpsc::Sender<Message>,
    data_out: mpsc::Sender<Message>,
    io: IoHandle,
}

struct IoHandle {
    readers: Vec<tokio::task::JoinHandle<()>>,
    writers: Vec<tokio::task::JoinHandle<()>>,
    /// 出错后置位：拒绝继续发送。
    broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl FrameTransport {
    /// 单流承载（v3 执行器本地 IPC）：标签帧 + 单队列串行写。
    /// 控制与数据共用一条发送队列，帧序即提交序。
    pub fn spawn<S>(stream: S, queue_depth: usize) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (event_tx, incoming) = mpsc::channel(queue_depth.max(8));
        let (outbound_tx, outbound_rx) = mpsc::channel::<Message>(queue_depth.max(8));
        let broken = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (read_half, write_half) = tokio::io::split(stream);
        let read_half: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> = Box::pin(read_half);
        let reader = tokio::spawn(read_loop_tagged(
            read_half,
            event_tx.clone(),
            broken.clone(),
        ));
        let writer = tokio::spawn(write_loop_tagged(
            Box::pin(write_half) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>,
            outbound_rx,
            broken.clone(),
        ));
        drop(event_tx);
        Self {
            incoming,
            pending_error: None,
            ended: false,
            control_out: outbound_tx.clone(),
            data_out: outbound_tx,
            io: IoHandle {
                readers: vec![reader],
                writers: vec![writer],
                broken,
            },
        }
    }

    /// 双流承载（二期遗留，已无生产调用方）：control 与 data 各一条流，
    /// 每通道独立串行写。v3 起 agent 上联与本地 IPC 都走单流 [`Self::spawn`]；
    /// 传输层不依赖 Unix，本方法保留供测试与外部兼容。
    pub fn spawn_streams<S>(control: S, data: S, queue_depth: usize) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (event_tx, incoming) = mpsc::channel(queue_depth.max(8));
        let (control_tx, control_rx) = mpsc::channel::<Message>(queue_depth.max(8));
        let (data_tx, data_rx) = mpsc::channel::<Message>(queue_depth.max(8));
        let broken = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // 读写半拆分：读任务与串行写任务各自持有独立句柄。
        let (control_read, control_write) = tokio::io::split(control);
        let (data_read, data_write) = tokio::io::split(data);
        let control_read: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> =
            Box::pin(control_read);
        let reader_control = tokio::spawn(read_loop(
            control_read,
            Channel::Control,
            CONTROL_MAX_FRAME,
            event_tx.clone(),
            broken.clone(),
        ));
        let data_read: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> = Box::pin(data_read);
        let reader_data = tokio::spawn(read_loop(
            data_read,
            Channel::Data,
            DATA_MAX_FRAME,
            event_tx.clone(),
            broken.clone(),
        ));
        let writer_control = tokio::spawn(write_loop(control_write, control_rx, broken.clone()));
        let writer_data = tokio::spawn(write_loop(
            Box::pin(data_write) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>,
            data_rx,
            broken.clone(),
        ));
        drop(event_tx);
        Self {
            incoming,
            pending_error: None,
            ended: false,
            control_out: control_tx,
            data_out: data_tx,
            io: IoHandle {
                readers: vec![reader_control, reader_data],
                writers: vec![writer_control, writer_data],
                broken,
            },
        }
    }

    /// 发送一条消息（自动路由到其声明通道；通道不符即协议错误）。
    pub async fn send(&self, message: Message) -> Result<(), ProtocolError> {
        if self.io.broken.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ProtocolError::Closed);
        }
        match message.channel() {
            Channel::Control => self
                .control_out
                .send(message)
                .await
                .map_err(|_| ProtocolError::Closed),
            Channel::Data => self
                .data_out
                .send(message)
                .await
                .map_err(|_| ProtocolError::Closed),
        }
    }

    /// 接收下一条消息。任一通道 EOF/错误时返回 [`ProtocolError::Closed`]；
    /// 协议帧错误（非法长度/未知类型/畸形信封）先返回错误，随后通道关闭。
    pub async fn recv(&mut self) -> Result<(Channel, Message), ProtocolError> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if self.ended {
            return Err(ProtocolError::Closed);
        }
        match self.incoming.recv().await {
            Some(IoEvent::Frame(channel, message)) => Ok((channel, *message)),
            Some(IoEvent::Failed(_, error)) => {
                self.ended = true;
                Err(error)
            }
            Some(IoEvent::Eof(_)) | None => {
                self.ended = true;
                Err(ProtocolError::Closed)
            }
        }
    }

    /// 非阻塞发送（队列满/已关即失败；agent 拒绝路径使用）。
    pub fn try_send(&self, message: Message) -> Result<(), ProtocolError> {
        if self.io.broken.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ProtocolError::Closed);
        }
        let sender = match message.channel() {
            Channel::Control => &self.control_out,
            Channel::Data => &self.data_out,
        };
        sender.try_send(message).map_err(|_| ProtocolError::Closed)
    }

    /// 尽力先弹出已缓冲帧；无则 None（非阻塞窥视）。
    pub fn try_recv(&mut self) -> Option<(Channel, Message)> {
        if self.ended {
            return None;
        }
        match self.incoming.try_recv() {
            Ok(IoEvent::Frame(channel, message)) => Some((channel, *message)),
            Ok(IoEvent::Failed(_, error)) => {
                self.ended = true;
                self.pending_error = Some(error);
                None
            }
            Ok(IoEvent::Eof(_)) | Err(mpsc::error::TryRecvError::Disconnected) => {
                self.ended = true;
                self.pending_error = Some(ProtocolError::Closed);
                None
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
        }
    }

    /// Finish queued frames before closing. The deadline also bounds blocked writers.
    pub async fn flush_and_close(mut self, timeout: std::time::Duration) {
        self.control_out = dead_sender();
        self.data_out = dead_sender();
        let _ = tokio::time::timeout(timeout, async {
            for writer in &mut self.io.writers {
                let _ = writer.await;
            }
        })
        .await;
        self.close();
        for task in self.io.readers.iter_mut().chain(self.io.writers.iter_mut()) {
            task.abort();
        }
    }

    /// 关闭全部通道并停止 I/O 任务。幂等。
    pub fn close(&mut self) {
        self.ended = true;
        self.io
            .broken
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // 丢弃发送端使写任务退出并 shutdown 流；用死通道占位保持类型。
        self.control_out = dead_sender();
        self.data_out = dead_sender();
    }

    /// 等待全部 I/O 任务退出（关闭后回收）。
    pub async fn join(self) {
        let io = self.io;
        for reader in io.readers {
            let _ = reader.await;
        }
        for writer in io.writers {
            let _ = writer.await;
        }
    }
}

async fn read_loop(
    stream: impl tokio::io::AsyncRead + Unpin + Send,
    channel: Channel,
    limit: usize,
    events: mpsc::Sender<IoEvent>,
    broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use tokio::io::AsyncReadExt;
    let mut stream = stream;
    let mut decoder = FrameDecoder::new(limit);
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                let _ = events.send(IoEvent::Eof(channel)).await;
                broken.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
            Ok(n) => {
                if let Err(error) = decoder.feed(&buffer[..n]) {
                    let _ = events.send(IoEvent::Failed(channel, error)).await;
                    broken.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                while let Ok(Some(json)) = decoder.pop_frame() {
                    let message = match serde_json::from_slice::<serde_json::Value>(&json)
                        .map_err(|e| ProtocolError::Malformed(format!("envelope: {e}")))
                        .and_then(Message::from_envelope)
                    {
                        Ok(message) => message,
                        Err(error) => {
                            let _ = events.send(IoEvent::Failed(channel, error)).await;
                            broken.store(true, std::sync::atomic::Ordering::SeqCst);
                            return;
                        }
                    };
                    if events
                        .send(IoEvent::Frame(channel, Box::new(message)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) => {
                let _ = events
                    .send(IoEvent::Failed(channel, ProtocolError::Io(error)))
                    .await;
                broken.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        }
    }
}

async fn write_loop(
    stream: impl tokio::io::AsyncWrite + Unpin + Send,
    queue: mpsc::Receiver<Message>,
    broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use tokio::io::AsyncWriteExt;
    let mut stream = stream;
    let mut queue = queue;
    while let Some(message) = queue.recv().await {
        if broken.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!(
                type_name = message.type_name(),
                "writer exit: session broken"
            );
            return;
        }
        let frame = match encode_frame(&message) {
            Ok(frame) => frame,
            Err(error) => {
                eprintln!(
                    "flow-transport: frame encode failed type={} error={error}",
                    message.type_name()
                );
                // 观测是可丢弃数据：编码失败丢帧计数，绝不为它拆除
                // golden source 会话（审计/结果/传输帧失败仍然断线——
                // 那是正确性路径，该炸就炸）。
                if !matches!(message, Message::ObservabilityBatch { .. }) {
                    tracing::error!(type_name = message.type_name(), %error, "frame encode failed");
                    broken.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                tracing::warn!(type_name = message.type_name(), %error, "observability frame dropped");
                continue;
            }
        };
        // 大载荷分多次 write；写不动时天然背压到发送队列。
        if let Err(error) = stream.write_all(&frame).await {
            eprintln!(
                "flow-transport: frame write failed type={} error={error}",
                message.type_name()
            );
            broken.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        if let Err(error) = stream.flush().await {
            eprintln!(
                "flow-transport: frame flush failed type={} error={error}",
                message.type_name()
            );
            broken.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    }
    let _ = stream.shutdown().await;
}

/// 已关闭的死发送通道（close() 后占位）。
fn dead_sender() -> mpsc::Sender<Message> {
    let (tx, mut rx) = mpsc::channel(1);
    rx.close();
    tx
}

/// 单流读任务：标签声明通道，且必须与消息类型声明的通道一致（双重校验）。
async fn read_loop_tagged(
    stream: impl tokio::io::AsyncRead + Unpin + Send,
    events: mpsc::Sender<IoEvent>,
    broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use tokio::io::AsyncReadExt;
    let mut stream = stream;
    let mut decoder = FrameDecoder::new_tagged();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                let _ = events.send(IoEvent::Eof(Channel::Control)).await;
                broken.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
            Ok(n) => {
                if let Err(error) = decoder.feed(&buffer[..n]) {
                    let _ = events.send(IoEvent::Failed(Channel::Control, error)).await;
                    broken.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                while let Ok(Some((channel, json))) = decoder.pop_tagged_frame() {
                    let message = match serde_json::from_slice::<serde_json::Value>(&json)
                        .map_err(|e| ProtocolError::Malformed(format!("envelope: {e}")))
                        .and_then(Message::from_envelope)
                    {
                        Ok(message) => message,
                        Err(error) => {
                            let _ = events.send(IoEvent::Failed(channel, error)).await;
                            broken.store(true, std::sync::atomic::Ordering::SeqCst);
                            return;
                        }
                    };
                    if message.channel() != channel {
                        let _ = events.send(IoEvent::Failed(
                            channel,
                            ProtocolError::Malformed(format!(
                                "channel tag {:?} mismatches message {}",
                                channel,
                                message.type_name()
                            )),
                        ))
                        .await;
                        broken.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                    if events
                        .send(IoEvent::Frame(channel, Box::new(message)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) => {
                let _ = events
                    .send(IoEvent::Failed(
                        Channel::Control,
                        ProtocolError::Io(error),
                    ))
                    .await;
                broken.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        }
    }
}

/// 单流写任务：与 [`write_loop`] 同一丢弃策略，帧格式带通道标签。
async fn write_loop_tagged(
    stream: impl tokio::io::AsyncWrite + Unpin + Send,
    queue: mpsc::Receiver<Message>,
    broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use tokio::io::AsyncWriteExt;
    let mut stream = stream;
    let mut queue = queue;
    while let Some(message) = queue.recv().await {
        if broken.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!(
                type_name = message.type_name(),
                "writer exit: session broken"
            );
            return;
        }
        let frame = match encode_tagged_frame(&message) {
            Ok(frame) => frame,
            Err(error) => {
                eprintln!(
                    "flow-transport: frame encode failed type={} error={error}",
                    message.type_name()
                );
                // 观测是可丢弃数据：编码失败丢帧计数，绝不为它拆除
                // golden source 会话（审计/结果/传输帧失败仍然断线——
                // 那是正确性路径，该炸就炸）。
                if !matches!(message, Message::ObservabilityBatch { .. }) {
                    tracing::error!(type_name = message.type_name(), %error, "frame encode failed");
                    broken.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                tracing::warn!(type_name = message.type_name(), %error, "observability frame dropped");
                continue;
            }
        };
        if let Err(error) = stream.write_all(&frame).await {
            eprintln!(
                "flow-transport: frame write failed type={} error={error}",
                message.type_name()
            );
            broken.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        if let Err(error) = stream.flush().await {
            eprintln!(
                "flow-transport: frame flush failed type={} error={error}",
                message.type_name()
            );
            broken.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    }
    let _ = stream.shutdown().await;
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

/// 主进程侧握手：接收 Hello（校验版本/能力/boot 唯一性），回 Welcome，
/// 等待 Ready。任一步失败即明确拒绝，不降级。
pub async fn master_handshake(
    transport: &mut FrameTransport,
    journal_id: &str,
    master_epoch: u64,
    executor_id: &str,
    session_id: &str,
) -> Result<ExecutorHello, ProtocolError> {
    let (channel, message) = tokio::time::timeout(
        std::time::Duration::from_millis(super::contract::STARTUP_TIMEOUT_MS),
        transport.recv(),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("handshake timeout".into()))??;
    let Message::Hello {
        boot_id,
        build,
        capabilities,
    } = message
    else {
        return Err(ProtocolError::Malformed(format!(
            "expected Hello on control, got {channel:?} first message"
        )));
    };
    validate_identity(&boot_id)?;
    validate_identity(&build)?;
    for capability in REQUIRED_CAPABILITIES {
        if !capabilities.iter().any(|c| c == capability) {
            return Err(ProtocolError::Malformed(format!(
                "executor missing required capability: {capability}"
            )));
        }
    }
    transport
        .send(Message::Welcome {
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
    let (_, message) = tokio::time::timeout(
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

/// 执行器侧握手：发 Hello，校验 Welcome 限额 ≤ 本端硬上限，回 Ready。
pub async fn executor_handshake(
    transport: &mut FrameTransport,
    build: &str,
    capabilities: &[&str],
) -> Result<(SessionIdentity, NegotiatedLimits), ProtocolError> {
    let boot_id = fresh_boot_id();
    transport
        .send(Message::Hello {
            boot_id: boot_id.clone(),
            build: build.into(),
            capabilities: capabilities.iter().map(|s| s.to_string()).collect(),
        })
        .await?;
    let (_, message) = tokio::time::timeout(
        std::time::Duration::from_millis(super::contract::STARTUP_TIMEOUT_MS),
        transport.recv(),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("handshake timeout".into()))??;
    let Message::Welcome {
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

thread_local! {
    /// 退化的 boot_id 兜底（未用）。
    static _BOOT_ID: String = fresh_boot_id();
}

/// 生成 16 字节随机 boot_id（hex）。从 getrandom 读；失败时退化为时间+计数
/// 组合，仍然保证本进程内唯一。
pub fn fresh_boot_id() -> String {
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut bytes = [0u8; 16];
    #[cfg(unix)]
    let filled = unsafe {
        // macOS: getentropy; Linux: getrandom。
        #[cfg(target_os = "macos")]
        {
            libc::getentropy(bytes.as_mut_ptr() as *mut libc::c_void, bytes.len()) == 0
        }
        #[cfg(not(target_os = "macos"))]
        {
            libc::getrandom(bytes.as_mut_ptr() as *mut libc::c_void, bytes.len(), 0)
                == bytes.len() as isize
        }
    };
    #[cfg(not(unix))]
    let filled = false;
    if filled {
        hex_encode(&bytes)
    } else {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        hex_encode(&nanos.to_be_bytes()) + &hex_encode(&n.to_be_bytes())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
                executor_handshake(&mut worker, "0.1.0", REQUIRED_CAPABILITIES)
                    .await
                    .expect("executor handshake");
            (identity, limits)
        });
        let hello = master_handshake(&mut master, "journal-1", 7, "exec-1", "session-1")
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
            executor_handshake(&mut worker, "0.1.0", &["execute"]).await
        });
        let error = master_handshake(&mut master, "journal-1", 1, "e", "s")
            .await
            .expect_err("must reject missing capability");
        assert!(error.to_string().contains("required capability"));
        let _ = worker_task.await;
    }

    #[tokio::test]
    async fn first_message_not_hello_rejected() {
        let (mut a, mut _worker) = pair().await;
        _worker.send(Message::Ready).await.unwrap();
        let error = master_handshake(&mut a, "j", 1, "e", "s")
            .await
            .expect_err("non-hello rejected");
        assert!(error.to_string().contains("expected Hello"));
    }

    #[tokio::test]
    async fn single_stream_preserves_submission_order_across_channels() {
        // 单流承载的契约：跨通道的发送序 = 到达序。Execute（control）先于
        // 其 TransferChunk（data）提交，接收端必须按此顺序看到。
        let (a, mut b) = pair().await;
        a.send(Message::Ready).await.unwrap();
        a.send(Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "t".into(),
            offset: 0,
            bytes: "aGk=".into(),
            digest: "d".into(),
        })
        .await
        .unwrap();
        let (channel, message) = b.recv().await.unwrap();
        assert_eq!(channel, Channel::Control);
        assert_eq!(message.type_name(), "Ready");
        let (channel, message) = b.recv().await.unwrap();
        assert_eq!(channel, Channel::Data);
        assert_eq!(message.type_name(), "TransferChunk");
    }

    #[tokio::test]
    async fn channel_tag_mismatch_fails_session() {
        // 手工构造「control 标签 + data 消息」的帧：读端必须拒绝。
        let (parent, child) = socketpair_channel().unwrap();
        use tokio::io::AsyncWriteExt;
        let raw = {
            // 在包进 transport 前取原始 FD 的独立副本注入坏帧。
            let fd = std::os::fd::AsRawFd::as_raw_fd(&child);
            // SAFETY: dup 只复制句柄，原 stream 生命周期不受影响。
            let dup = unsafe { libc::dup(fd) };
            unsafe { std::os::unix::net::UnixStream::from_raw_fd(dup) }
        };
        let mut _peer = FrameTransport::spawn(child, 8);
        let mut a = FrameTransport::spawn(parent, 8);
        raw.set_nonblocking(true).unwrap();
        let mut raw = tokio::net::UnixStream::from_std(raw).unwrap();
        let message = Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "t".into(),
            offset: 0,
            bytes: "aGk=".into(),
            digest: "d".into(),
        };
        // 用 control 标签发 data 消息。
        let json = serde_json::to_vec(&message.to_envelope()).unwrap();
        let mut frame = Vec::with_capacity(json.len() + 5);
        frame.extend_from_slice(&((json.len() + 1) as u32).to_be_bytes());
        frame.push(Channel::Control.tag());
        frame.extend_from_slice(&json);
        raw.write_all(&frame).await.unwrap();
        let error = a.recv().await.expect_err("tag mismatch must fail");
        assert!(error.to_string().contains("mismatches"), "{error}");
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
}

#[cfg(test)]
mod terminal_event_tests {
    use super::*;
    #[tokio::test]
    async fn graceful_close_writes_queued_control_frame() {
        use tokio::io::AsyncReadExt;
        let (ca, mut cb) = tokio::io::duplex(4096);
        let (da, _db) = tokio::io::duplex(4096);
        let transport = FrameTransport::spawn_streams(ca, da, 8);
        let message = Message::DrainComplete { drained: 3 };
        let expected = encode_frame(&message).unwrap();
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
            let (control, _control_peer) = tokio::io::duplex(1024);
            let (data, _data_peer) = tokio::io::duplex(1024);
            let mut transport = FrameTransport::spawn_streams(control, data, 4);
            // Inject the read task event: no scheduling sleep or second EOF can mask the failure.
            let (events, incoming) = mpsc::channel(4);
            transport.incoming = incoming;
            let expected = error.to_string();
            events
                .send(IoEvent::Failed(Channel::Control, error))
                .await
                .unwrap();
            assert!(transport.try_recv().is_none());
            assert!(transport.try_recv().is_none());
            assert_eq!(transport.recv().await.unwrap_err().to_string(), expected);
            assert!(matches!(transport.recv().await, Err(ProtocolError::Closed)));
        }
    }
}
