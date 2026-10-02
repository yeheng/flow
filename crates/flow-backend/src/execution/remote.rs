//! 远程 agent 管理器（三期 §1.2/§1.3/§1.4）：mTLS 上联、路由包络校验、
//! BindExecutor 归属、Resume 裁决与有界公平转发。
//!
//! 主进程保持唯一提交者：AuditAck/OperationPermit/ResultCommitted 只能由
//! 主进程权威产生后经 agent 转发；agent 中继不生成确认。所有持久事实仍
//! 走一期/二期的 journal 单行事务。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, Semaphore};
use tokio_rustls::TlsAcceptor;

use flow_engine::execution_protocol::contract::{A_MAX, STARTUP_TIMEOUT_MS, W_BYTES};
use flow_engine::execution_protocol::message::{Message, ProtocolError};
use flow_engine::execution_protocol::remote::{
    capability, validate_route_identity, ResumeAction, ResumeDecision, ResumeItem, RouteEnvelope,
};
use flow_engine::execution_protocol::transport::FrameTransport;

use crate::execution::dispatch::DispatchLink;
use crate::execution::pool::ToSession;
use crate::journal::JournalBackend;

type Result<T> = std::result::Result<T, flow_journal::Error>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

type BoxedStream = Pin<Box<dyn AsyncReadWrite + Send>>;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin {}
impl<T: AsyncRead + AsyncWrite + Unpin> AsyncReadWrite for T {}

// 链路死亡标记（dispatch.rs 在 Dead/关闭路径写入 "link-dead: " 前缀）。
pub(crate) fn is_link_dead(error: &crate::journal::JournalError) -> bool {
    matches!(
        error,
        crate::journal::JournalError::Journal(flow_journal::Error::Invalid(message))
            if message.starts_with("link-dead: ")
    )
}

// 远程模式选项。
#[derive(Debug, Clone)]
pub struct RemoteOptions {
    pub control_addr: String,
    pub data_addr: String,
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    // 断链后等待 agent 重连对账的总时限（超时按派发失败处理）。
    pub attach_timeout_ms: u64,
}

impl RemoteOptions {
    pub fn attach_timeout(&self) -> Duration {
        Duration::from_millis(self.attach_timeout_ms.max(1_000))
    }
}

// TLS 服务器配置（mTLS：验证 agent 客户端证书）。
pub struct TlsServer {
    pub acceptor: TlsAcceptor,
}

// 安装进程级 CryptoProvider（多 crate 启用不同 provider 时必须显式选择；
// 幂等，重复安装忽略）。
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl TlsServer {
    pub fn from_files(
        ca: &std::path::Path,
        cert: &std::path::Path,
        key: &std::path::Path,
    ) -> std::io::Result<Self> {
        Self::from_pem(
            &std::fs::read(ca)?,
            &std::fs::read(cert)?,
            &std::fs::read(key)?,
        )
    }

    pub fn from_pem(ca: &[u8], cert: &[u8], key: &[u8]) -> std::io::Result<Self> {
        use rustls_pemfile::{certs, private_key};
        use std::io::Cursor;
        let mut roots = rustls::RootCertStore::empty();
        for der in certs(&mut Cursor::new(ca))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(bad_data)?
        {
            let _ = roots.add(rustls::pki_types::CertificateDer::from(der.to_vec()));
        }
        if roots.is_empty() {
            return Err(bad_data("CA PEM contains no certificates"));
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(bad_data)?;
        let server_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            certs(&mut Cursor::new(cert))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(bad_data)?
                .into_iter()
                .collect();
        let server_key = private_key(&mut Cursor::new(key))
            .map_err(bad_data)?
            .ok_or_else(|| bad_data("no private key in PEM"))?;
        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(server_certs, server_key)
            .map_err(bad_data)?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

fn bad_data<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}

// 提取证书 CN（TLS 身份映射 agent_id；不信客户端自报字段）。
pub fn cert_common_name(der: &[u8]) -> Option<String> {
    use x509_parser::prelude::FromDer;
    let (rest, cert) = x509_parser::certificate::X509Certificate::from_der(der).ok()?;
    if !rest.is_empty() {
        return None;
    }
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_owned);
    cn
}

// attach 等待的裁决结果。
pub(crate) enum AttachOutcome {
    // agent 会话活跃：给出可驱动链路。
    Link(DispatchLink),
    // 结果已在权威日志（AlreadyCommitted）。
    Committed,
    // 取消/废止（CancelAndDrain）。
    Cancelled(String),
    // 执行器丢失/boot 变化/缺数据（ReconcileRequired 或 agent 未上报）。
    Lost(String),
}

// 会话侧持有的路由（dispatch → 事件通道）。
struct SessionRouting {
    envelope: RouteEnvelope,
    events: mpsc::Sender<crate::execution::pool::FromSession>,
}

// 跨会话存活的派发绑定。
struct BoundDispatch {
    agent_id: String,
    // 容量许可（drop 即释放槽位）。
    slot_permit: tokio::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    executor_boot_id: Mutex<Option<String>>,
    run_id: String,
    node_id: String,
    // 活跃链路（等待被 dispatcher 取走驱动）。
    pending_link: Mutex<Option<DispatchLink>>,
    verdict: Mutex<Option<AttachOutcome>>,
    notify: Notify,
}

struct AgentRegistration {
    agent_id: String,
    agent_boot_id: String,
    link_session_id: String,
    outbound: mpsc::Sender<ToSession>,
    slots_total: AtomicU64,
    slots_used: AtomicU64,
    // 槽位信号量：等待空位而非立即失败（容量准入，三期 §1.6）。
    slots_sem: Arc<Semaphore>,
    bind_acks: Mutex<BTreeMap<String, oneshot::Sender<Result<String>>>>,
}

// Agent 管理器：上联会话注册表 + 全局 A_max 准入 + Resume 裁决。
pub struct AgentManager {
    backend: Arc<JournalBackend>,
    journal_id: String,
    master_epoch: u64,
    global: Arc<Semaphore>,
    agents: Mutex<BTreeMap<String, Arc<AgentRegistration>>>,
    bindings: Mutex<BTreeMap<String, Arc<BoundDispatch>>>,
    pending_data: Mutex<BTreeMap<String, Arc<PendingData>>>,
    pending_binds: Mutex<BTreeMap<String, PendingBind>>,
    session_counter: AtomicU64,
    agent_counter: AtomicU64,
    shutdown: tokio_util::sync::CancellationToken,
}

// 等待 data 流汇入的槽（control 先握手，data 后合并——避免双方互等的
// 协议死锁：agent 收到 Welcome 才发起 data 连接）。
struct PendingData {
    data: tokio::sync::Mutex<Option<BoxedStream>>,
    notify: Notify,
}

struct PendingBind {
    agent_id: String,
    executor_id: String,
    events: mpsc::Sender<crate::execution::pool::FromSession>,
    ack: oneshot::Sender<Result<String>>,
}

impl AgentManager {
    pub fn new(backend: Arc<JournalBackend>, journal_id: String, master_epoch: u64) -> Arc<Self> {
        Arc::new(Self {
            backend,
            journal_id,
            master_epoch,
            global: Arc::new(Semaphore::new(A_MAX)),
            agents: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            pending_data: Mutex::new(BTreeMap::new()),
            pending_binds: Mutex::new(BTreeMap::new()),
            session_counter: AtomicU64::new(0),
            agent_counter: AtomicU64::new(0),
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    // 全局活跃派发准入（A_max）。
    pub(crate) fn global_slots(&self) -> &Arc<Semaphore> {
        &self.global
    }

    // TLS 监听入口（真实网络，三期 §1.2）。
    pub async fn listen_tls(self: &Arc<Self>, options: RemoteOptions) -> Result<()> {
        install_crypto_provider();
        let tls =
            TlsServer::from_files(&options.ca_cert, &options.server_cert, &options.server_key)
                .map_err(|e| invalid(format!("agent TLS config: {e}")))?;
        let control = TcpListener::bind(&options.control_addr)
            .await
            .map_err(|e| invalid(format!("agent control listen: {e}")))?;
        let data = TcpListener::bind(&options.data_addr)
            .await
            .map_err(|e| invalid(format!("agent data listen: {e}")))?;
        tracing::info!(
            control = %control.local_addr().map_err(|e| invalid(e.to_string()))?,
            data = %data.local_addr().map_err(|e| invalid(e.to_string()))?,
            "agent TLS listeners ready"
        );
        for (listener, is_control, acceptor) in [
            (control, true, tls.acceptor.clone()),
            (data, false, tls.acceptor.clone()),
        ] {
            let manager = self.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((socket, _)) = listener.accept().await else {
                        return;
                    };
                    let acceptor = acceptor.clone();
                    let manager = manager.clone();
                    tokio::spawn(async move {
                        let Ok(stream) = acceptor.accept(socket).await else {
                            return;
                        };
                        let agent_id = stream
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|certs| certs.first())
                            .and_then(|c| cert_common_name(c.as_ref()));
                        let Some(agent_id) = agent_id else {
                            tracing::warn!("agent TLS connection without client cert");
                            return;
                        };
                        let side = if is_control {
                            Side::Control
                        } else {
                            Side::Data
                        };
                        manager
                            .inject_stream(agent_id, side, Box::pin(stream))
                            .await;
                    });
                }
            });
        }
        Ok(())
    }

    // R0 模拟中继入口：直接注入双流（无 TLS；身份由注入方保证）。
    pub async fn attach_inprocess(
        self: &Arc<Self>,
        agent_id: String,
        control: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        data: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    ) {
        self.inject_stream(agent_id.clone(), Side::Control, Box::pin(control))
            .await;
        self.inject_stream(agent_id, Side::Data, Box::pin(data))
            .await;
    }

    async fn inject_stream(self: &Arc<Self>, agent_id: String, side: Side, stream: BoxedStream) {
        match side {
            Side::Control => {
                // control 到达即启动会话：先裸帧握手（Hello/Welcome），
                // 再等 data 流汇入完成 DataBind。
                let manager = self.clone();
                let agent_id = agent_id.clone();
                tokio::spawn(async move {
                    if let Err(error) = manager.run_session(agent_id, stream).await {
                        tracing::warn!(%error, "agent session ended");
                    }
                });
            }
            Side::Data => {
                // 统一在注册表登记（与会话任务使用同一槽位，避免注入先于
                // 会话注册时凭空创建无人等待的槽）。
                let mut slots = self.pending_data.lock().await;
                let slot = slots
                    .entry(agent_id.clone())
                    .or_insert_with(|| {
                        Arc::new(PendingData {
                            data: tokio::sync::Mutex::new(None),
                            notify: Notify::new(),
                        })
                    })
                    .clone();
                *slot.data.lock().await = Some(stream);
                slot.notify.notify_one();
            }
        }
    }

    // 单个 agent 会话：control 先裸帧握手（Hello/Welcome），随后等 data
    // 流汇入并完成 DataBind → 转发。
    async fn run_session(
        self: &Arc<Self>,
        agent_id: String,
        control: BoxedStream,
    ) -> std::result::Result<(), String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let control = tokio::io::BufStream::new(control);
        let (mut control_read, mut control_write) = tokio::io::split(control);
        // 1) control 裸帧握手。
        let (agent_boot_id, _build, capabilities, slots) = {
            let mut decoder = flow_engine::execution_protocol::frame::FrameDecoder::new(
                flow_engine::execution_protocol::CONTROL_MAX_FRAME,
            );
            let hello = loop {
                let mut chunk = [0u8; 16 * 1024];
                let n = tokio::time::timeout(
                    Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
                    control_read.read(&mut chunk),
                )
                .await
                .map_err(|_| "agent hello timeout".to_string())?
                .map_err(|e| format!("read hello: {e}"))?;
                if n == 0 {
                    return Err("control closed before hello".into());
                }
                decoder
                    .feed(&chunk[..n])
                    .map_err(|e| format!("hello frame: {e}"))?;
                if let Some(json) = decoder.pop_frame().map_err(|e| e.to_string())? {
                    let value: serde_json::Value =
                        serde_json::from_slice(&json).map_err(|e| e.to_string())?;
                    break Message::from_envelope(value).map_err(|e| e.to_string())?;
                }
            };
            let Message::AgentHello {
                agent_boot_id,
                build,
                capabilities,
                slots,
            } = hello
            else {
                return Err("expected AgentHello".into());
            };
            let _ = build;
            (agent_boot_id, build, capabilities, slots)
        };
        for required in capability::REQUIRED {
            if !capabilities.iter().any(|c| c == required) {
                return Err(format!("agent missing capability {required}"));
            }
        }
        if !validate_route_identity(&agent_id) || !validate_route_identity(&agent_boot_id) {
            return Err("invalid agent identity".into());
        }
        let link_session_id = format!(
            "link-{}",
            self.session_counter.fetch_add(1, Ordering::Relaxed)
        );
        let data_credential = flow_engine::execution_protocol::transport::fresh_boot_id();
        // 裸写 Welcome（agent 在 data 连接前等待它）。
        {
            let welcome = Message::AgentWelcome {
                journal_id: self.journal_id.clone(),
                master_epoch: self.master_epoch,
                link_session_id: link_session_id.clone(),
                agent_id: agent_id.clone(),
                data_credential: data_credential.clone(),
                heartbeat_ms: flow_engine::execution_protocol::HEARTBEAT_INTERVAL_MS,
                agent_window_bytes: W_BYTES * A_MAX as u64,
            };
            let frame = flow_engine::execution_protocol::frame::encode_frame(&welcome)
                .map_err(|e| format!("encode welcome: {e}"))?;
            control_write
                .write_all(&frame)
                .await
                .map_err(|e| format!("write welcome: {e}"))?;
            control_write
                .flush()
                .await
                .map_err(|e| format!("flush welcome: {e}"))?;
        }
        // 2) 等 data 流汇入（agent 收到 Welcome 后发起）。
        let data_slot = {
            let mut slots = self.pending_data.lock().await;
            slots
                .entry(agent_id.clone())
                .or_insert_with(|| {
                    Arc::new(PendingData {
                        data: tokio::sync::Mutex::new(None),
                        notify: Notify::new(),
                    })
                })
                .clone()
        };
        let data = tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS * 4), async {
            loop {
                if let Some(data) = data_slot.data.lock().await.take() {
                    return Ok::<BoxedStream, ()>(data);
                }
                data_slot.notify.notified().await;
            }
        })
        .await
        .map_err(|_| "data connection timeout".to_string())?
        .map_err(|_| "data connection cancelled".to_string())?;
        // 重组握手后的流交给统一 transport（BufStream 缓冲已由裸读消化）。
        let control = control_read.unsplit(control_write);
        let data = tokio::io::BufStream::new(data);
        let mut transport = FrameTransport::spawn_streams(control, data, 128);
        // 3) data 首帧 DataBind（防连接串配：同主体、boot、会话、一次性凭据）。
        tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS * 2), async {
            loop {
                let (channel, message) = transport.recv().await?;
                if channel == flow_engine::execution_protocol::Channel::Data {
                    if let Message::DataBind {
                        agent_boot_id: bind_boot,
                        link_session_id: bind_session,
                        credential,
                    } = message
                    {
                        if bind_boot != agent_boot_id
                            || bind_session != link_session_id
                            || credential != data_credential
                        {
                            return Err(ProtocolError::Malformed("data bind mismatch".into()));
                        }
                        return Ok(());
                    }
                    return Err(ProtocolError::Malformed(
                        "expected DataBind on data channel".into(),
                    ));
                }
            }
        })
        .await
        .map_err(|_| "data bind timeout".to_string())?
        .map_err(|e: ProtocolError| e.to_string())?;
        ();

        // 3) 注册会话（替换旧会话：旧 outbound 关闭 → 相关 runner link-dead）。
        let (outbound, mut commands) = mpsc::channel::<ToSession>(256);
        let registration = Arc::new(AgentRegistration {
            agent_id: agent_id.clone(),
            agent_boot_id: agent_boot_id.clone(),
            link_session_id: link_session_id.clone(),
            outbound: outbound.clone(),
            slots_total: AtomicU64::new(slots as u64),
            slots_used: AtomicU64::new(0),
            slots_sem: Arc::new(Semaphore::new(slots.max(1) as usize)),
            bind_acks: Mutex::new(BTreeMap::new()),
        });
        let previous = self
            .agents
            .lock()
            .await
            .insert(agent_id.clone(), registration.clone());
        if let Some(previous) = previous {
            // 迁移在飞占用（计数与许可都延续，不重置准入水位）。
            registration.slots_used.store(
                previous.slots_used.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            for _ in 0..previous.slots_used.load(Ordering::Relaxed) {
                let _ = registration.slots_sem.clone().acquire_owned().await;
            }
            tracing::info!(%agent_id, "agent reconnected; old uplink replaced");
        }
        tracing::info!(%agent_id, slots, "agent session registered");
        let mut routings: BTreeMap<String, SessionRouting> = BTreeMap::new();
        let result: std::result::Result<(), String> = async {
            loop {
                tokio::select! {
                    command = commands.recv() => {
                        let Some(command) = command else { return Ok(()); };
                        match command {
                            ToSession::Send(message) => {
                                // 业务帧按已确认路由包装；管理帧
                                // （BindExecutor/ResumeReply/Drain…）直接发送。
                                let frame = if
                                    flow_engine::execution_protocol::remote::routable(&message)
                                {
                                    match message
                                        .dispatch_id()
                                        .and_then(|id| routings.get(id))
                                    {
                                        Some(routing) => Message::Routed {
                                            envelope: routing.envelope.clone(),
                                            inner: Box::new(message),
                                        },
                                        None => continue, // 未绑定：不发送
                                    }
                                } else {
                                    message
                                };
                                transport.send(frame).await.map_err(|e| format!("send: {e}"))?;
                            }
                            ToSession::CancelDispatch(dispatch_id) => {
                                let Some(routing) = routings.get(&dispatch_id) else { continue };
                                let envelope = routing.envelope.clone();
                                transport.send(Message::Routed {
                                    envelope,
                                    inner: Box::new(Message::Cancel {
                                        command_id: format!("cancel-{dispatch_id}"),
                                        dispatch_id: dispatch_id.clone(),
                                    }),
                                })
                                .await
                                .map_err(|e| format!("cancel: {e}"))?;
                                routings.remove(&dispatch_id);
                            }
                            ToSession::Recycle => {
                                transport.close();
                                return Ok(());
                            }
                        }
                    }
                    received = transport.recv() => {
                        let (channel, message) = received.map_err(|e| format!("recv: {e}"))?;
                        match message {
                            Message::Routed { envelope, inner } => {
                                if channel == flow_engine::execution_protocol::Channel::Control
                                    && matches!(&*inner, Message::DataBind { .. })
                                {
                                    return Err("nested DataBind".into());
                                }
                                // 包络校验：会话身份 + 绑定归属 + boot 一致。
                                if envelope.agent_id != agent_id
                                    || envelope.agent_boot_id != agent_boot_id
                                    || envelope.link_session_id != link_session_id
                                {
                                    return Err(format!("route envelope mismatch from agent {agent_id}"));
                                }
                                let Some(routing) = routings.get(&envelope.dispatch_id.clone().unwrap_or_default()) else {
                                    // 未绑定/错路由：明确拒绝（协议错误关闭会话），
                                    // 不进入权威日志。
                                    return Err(format!(
                                        "routed frame for unbound dispatch rejected: {agent_id}"
                                    ));
                                };
                                if envelope.executor_boot_id != routing.envelope.executor_boot_id {
                                    return Err("executor boot mismatch in envelope".into());
                                }
                                let _ = routing
                                    .events
                                    .send(crate::execution::pool::FromSession::Incoming(*inner))
                                    .await;
                            }
                            Message::BindExecutorAck {
                                executor_id,
                                executor_boot_id,
                                dispatch_id,
                                ok,
                                error,
                            } => {
                                let pending = self.pending_binds.lock().await.remove(&dispatch_id);
                                if let Some(bind) = pending {
                                    if ok {
                                        // 注册路由 + 绑定记录，唤醒 binder。
                                        let envelope = RouteEnvelope {
                                            agent_id: agent_id.clone(),
                                            agent_boot_id: agent_boot_id.clone(),
                                            link_session_id: link_session_id.clone(),
                                            executor_id: executor_id.clone(),
                                            executor_boot_id: executor_boot_id.clone(),
                                            dispatch_id: Some(dispatch_id.clone()),
                                        };
                                        routings.insert(dispatch_id.clone(), SessionRouting {
                                            envelope,
                                            events: bind.events,
                                        });
                                        let _ = bind.ack.send(Ok(executor_boot_id.clone()));
                                        self.note_bound(&dispatch_id, executor_boot_id).await;
                                    } else {
                                        let _ = bind.ack.send(Err(invalid(format!(
                                            "agent bind failed: {}",
                                            error.unwrap_or_else(|| "unknown".into())
                                        ))));
                                    }
                                }
                            }
                            Message::CapacityReport { capacity } => {
                                registration
                                    .slots_total
                                    .store(capacity.slots as u64, Ordering::Relaxed);
                                registration
                                    .slots_used
                                    .store(capacity.in_flight as u64, Ordering::Relaxed);
                            }
                            Message::Resume { items, page, more } => {
                                let decisions = self
                                    .adjudicate_resume(&agent_id, &items)
                                    .await;
                                // 应用裁决：非继续项立即通知 agent 收尾并唤醒 waiter。
                                for decision in &decisions {
                                    let dispatch_id = decision.dispatch_id.clone();
                                    match &decision.action {
                                        ResumeAction::UploadOnly { .. }
                                        | ResumeAction::SubmitExistingResult => {
                                            routings.remove(&dispatch_id);
                                        }
                                        ResumeAction::AlreadyCommitted => {
                                            // 返回原提交确认：执行器收到后正常退出。
                                            if let Some(item) =
                                                items.iter().find(|i| i.dispatch_id == dispatch_id)
                                            {
                                                if let Some(result_id) = &item.result_id {
                                                    let _ = transport
                                                        .send(Message::Routed {
                                                            envelope: resume_envelope(
                                                                &agent_id,
                                                                &agent_boot_id,
                                                                &link_session_id,
                                                                &dispatch_id,
                                                            ),
                                                            inner: Box::new(Message::ResultCommitted {
                                                                dispatch_id: dispatch_id.clone(),
                                                                result_id: result_id.clone(),
                                                            }),
                                                        })
                                                        .await;
                                                }
                                            }
                                            routings.remove(&dispatch_id);
                                            self.resolve_binding(&dispatch_id, AttachOutcome::Committed, true)
                                                .await;
                                        }
                                        ResumeAction::CancelAndDrain => {
                                            routings.remove(&dispatch_id);
                                            let _ = transport
                                                .send(Message::Routed {
                                                    envelope: resume_envelope(
                                                        &agent_id,
                                                        &agent_boot_id,
                                                        &link_session_id,
                                                        &dispatch_id,
                                                    ),
                                                    inner: Box::new(Message::Cancel {
                                                        command_id: format!("cancel-{dispatch_id}"),
                                                        dispatch_id: dispatch_id.clone(),
                                                    }),
                                                })
                                                .await;
                                            self.resolve_binding(
                                                &dispatch_id,
                                                AttachOutcome::Cancelled("resume: cancel and drain".into()),
                                                true,
                                            )
                                            .await;
                                        }
                                        ResumeAction::ReconcileRequired => {
                                            routings.remove(&dispatch_id);
                                            let _ = transport
                                                .send(Message::Routed {
                                                    envelope: resume_envelope(
                                                        &agent_id,
                                                        &agent_boot_id,
                                                        &link_session_id,
                                                        &dispatch_id,
                                                    ),
                                                    inner: Box::new(Message::Cancel {
                                                        command_id: format!("cancel-{dispatch_id}"),
                                                        dispatch_id: dispatch_id.clone(),
                                                    }),
                                                })
                                                .await;
                                            self.resolve_binding(
                                                &dispatch_id,
                                                AttachOutcome::Lost("resume: reconcile required".into()),
                                                true,
                                            )
                                            .await;
                                        }
                                    }
                                }
                                transport
                                    .send(Message::ResumeReply { decisions, page })
                                    .await
                                    .map_err(|e| format!("resume reply: {e}"))?;
                                if more {
                                    continue;
                                }
                                // 清单末页：未上报的该 agent 绑定判 Lost（执行器已失联）。
                                self.mark_unreported_lost(&agent_id, &items).await;
                                // 为继续项建链路并唤醒 waiter。
                                self.attach_resumed(&agent_id, &items, &outbound, &mut routings, &link_session_id, &agent_boot_id)
                                    .await;
                            }
                            Message::DrainComplete { drained } => {
                                tracing::info!(%agent_id, drained, "agent drained");
                            }
                            Message::Heartbeat { .. } => {}
                            Message::Reject { reason } => {
                                tracing::warn!(%agent_id, %reason, "agent rejected");
                            }
                            other => {
                                return Err(format!(
                                    "unexpected agent message {}",
                                    other.type_name()
                                ));
                            }
                        }
                    }
                }
            }
        }
        .await;
        // 会话结束：注销（仅当仍是当前注册）并让绑定等待重连裁决。
        let mut agents = self.agents.lock().await;
        let still_current = agents
            .get(&agent_id)
            .is_some_and(|current| Arc::ptr_eq(current, &registration));
        if still_current {
            agents.remove(&agent_id);
        }
        drop(agents);
        result
    }

    async fn note_bound(&self, dispatch_id: &str, executor_boot_id: String) {
        if let Some(bound) = self.bindings.lock().await.get(dispatch_id) {
            *bound.executor_boot_id.lock().await = Some(executor_boot_id);
        }
    }

    // Resume 裁决（三期 §1.4 表）：以主日志为准，不信 agent 自报游标。
    async fn adjudicate_resume(&self, agent_id: &str, items: &[ResumeItem]) -> Vec<ResumeDecision> {
        let _ = agent_id;
        let state = self.backend.state().await;
        let bindings = self.bindings.lock().await;
        items
            .iter()
            .map(|item| {
                let action = match bindings.get(&item.dispatch_id) {
                    // master 无记录（master 重启/epoch 变化后）：只允许取消回收。
                    None => ResumeAction::ReconcileRequired,
                    Some(binding) => adjudicate_against_journal(&state, binding, item),
                };
                ResumeDecision {
                    dispatch_id: item.dispatch_id.clone(),
                    action,
                }
            })
            .collect()
    }

    // 对绑定写入终局裁决并移除（唤醒等待方）。
    async fn resolve_binding(&self, dispatch_id: &str, outcome: AttachOutcome, remove: bool) {
        let binding = if remove {
            self.bindings.lock().await.remove(dispatch_id)
        } else {
            self.bindings.lock().await.get(dispatch_id).cloned()
        };
        if let Some(binding) = binding {
            let mut verdict = binding.verdict.lock().await;
            if verdict.is_none() {
                *verdict = Some(outcome);
                drop(verdict);
                binding.notify.notify_one();
            }
        }
    }

    // resume 清单未上报的该 agent 绑定 → Lost（执行器已死/失联）。
    async fn mark_unreported_lost(&self, agent_id: &str, items: &[ResumeItem]) {
        let reported: std::collections::BTreeSet<&str> =
            items.iter().map(|item| item.dispatch_id.as_str()).collect();
        let mut bindings = self.bindings.lock().await;
        let lost: Vec<_> = bindings
            .iter()
            .filter(|(id, bound)| bound.agent_id == agent_id && !reported.contains(id.as_str()))
            .map(|(id, _)| id.clone())
            .collect();
        for id in lost {
            if let Some(bound) = bindings.remove(&id) {
                let mut verdict = bound.verdict.lock().await;
                if verdict.is_none() {
                    *verdict = Some(AttachOutcome::Lost(format!(
                        "agent {agent_id} did not report dispatch {id} on resume"
                    )));
                    bound.notify.notify_one();
                }
            }
        }
    }

    // 为 resume 中可继续的项建立新链路并唤醒等待的 dispatcher。
    #[allow(clippy::too_many_arguments)]
    async fn attach_resumed(
        &self,
        agent_id: &str,
        items: &[ResumeItem],
        outbound: &mpsc::Sender<ToSession>,
        routings: &mut BTreeMap<String, SessionRouting>,
        link_session_id: &str,
        agent_boot_id: &str,
    ) {
        for item in items {
            let binding = self.bindings.lock().await.get(&item.dispatch_id).cloned();
            let Some(binding) = binding else { continue };
            let bound_boot = binding.executor_boot_id.lock().await.clone();
            let Some(executor_boot_id) = bound_boot else {
                continue;
            };
            let envelope = RouteEnvelope {
                agent_id: agent_id.to_string(),
                agent_boot_id: agent_boot_id.to_string(),
                link_session_id: link_session_id.to_string(),
                executor_id: format!("{}:{}", agent_id, executor_boot_id),
                executor_boot_id: executor_boot_id.clone(),
                dispatch_id: Some(item.dispatch_id.clone()),
            };
            let (events, rx) = mpsc::channel(256);
            routings.insert(
                item.dispatch_id.clone(),
                SessionRouting {
                    envelope: envelope.clone(),
                    events,
                },
            );
            let link = DispatchLink {
                commands: outbound.clone(),
                events: rx,
            };
            let mut pending = binding.pending_link.lock().await;
            if pending.is_none() {
                *pending = Some(link);
                binding.notify.notify_one();
            }
        }
    }

    // 绑定一个派发到某 agent 的本地执行器（BindExecutor → 确认）。
    pub(crate) async fn bind_executor(
        self: &Arc<Self>,
        dispatch_id: String,
        run_id: String,
        node_id: String,
    ) -> Result<mpsc::Receiver<crate::execution::pool::FromSession>> {
        // 选择：占用最少的已连接 agent；空位等待由槽位信号量承担
        //（不立即失败，三期 §1.6）。
        let pick = {
            let agents = self.agents.lock().await;
            let mut best: Option<(Arc<AgentRegistration>, u64, u64)> = None;
            for registration in agents.values() {
                let total = registration.slots_total.load(Ordering::Relaxed);
                let used = registration.slots_used.load(Ordering::Relaxed);
                if best
                    .as_ref()
                    .is_none_or(|(_, _, used_so_far)| used < *used_so_far)
                {
                    best = Some((registration.clone(), total, used));
                }
            }
            best
        };
        let Some((registration, _total, _used)) = pick else {
            return Err(invalid("no agent connected"));
        };
        // 等待空位（容量准入不立即失败）。
        let slot_permit = registration
            .slots_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid("agent session closed while waiting for slot"))?;
        registration.slots_used.fetch_add(1, Ordering::Relaxed);
        let executor_id = format!(
            "exec-{}@{}",
            self.agent_counter.fetch_add(1, Ordering::Relaxed),
            registration.agent_id
        );
        let (events, rx) = mpsc::channel(256);
        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_binds.lock().await.insert(
            dispatch_id.clone(),
            PendingBind {
                agent_id: registration.agent_id.clone(),
                executor_id: executor_id.clone(),
                events,
                ack: ack_tx,
            },
        );
        self.bindings.lock().await.insert(
            dispatch_id.clone(),
            Arc::new(BoundDispatch {
                agent_id: registration.agent_id.clone(),
                slot_permit: tokio::sync::Mutex::new(Some(slot_permit)),
                executor_boot_id: Mutex::new(None),
                run_id,
                node_id,
                pending_link: Mutex::new(None),
                verdict: Mutex::new(None),
                notify: Notify::new(),
            }),
        );
        registration
            .outbound
            .send(ToSession::Send(Message::BindExecutor {
                executor_id,
                dispatch_id: dispatch_id.clone(),
            }))
            .await
            .map_err(|_| invalid("agent session closed before bind"))?;
        match tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS * 4), ack_rx).await {
            Ok(Ok(Ok(_boot))) => Ok(rx),
            Ok(Ok(Err(error))) => {
                self.bindings.lock().await.remove(&dispatch_id);
                registration.slots_used.fetch_sub(1, Ordering::Relaxed);
                Err(error)
            }
            Ok(Err(_)) | Err(_) => {
                self.bindings.lock().await.remove(&dispatch_id);
                registration.slots_used.fetch_sub(1, Ordering::Relaxed);
                Err(invalid("agent bind ack lost"))
            }
        }
    }

    // 取走绑定的活跃链路；无则等待重连裁决（Resume）或超时。
    pub(crate) async fn wait_attach(
        self: &Arc<Self>,
        dispatch_id: &str,
        timeout: Duration,
    ) -> AttachOutcome {
        let binding = self.bindings.lock().await.get(dispatch_id).cloned();
        let Some(binding) = binding else {
            return AttachOutcome::Lost("dispatch not bound".into());
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(link) = binding.pending_link.lock().await.take() {
                return AttachOutcome::Link(link);
            }
            if let Some(verdict) = binding.verdict.lock().await.take() {
                return verdict;
            }
            if tokio::time::Instant::now() >= deadline {
                return AttachOutcome::Lost("agent reconnect timeout".into());
            }
            let wait = binding.notify.notified();
            if tokio::time::timeout_at(deadline, wait).await.is_err() {
                return AttachOutcome::Lost("agent reconnect timeout".into());
            }
        }
    }

    // 派发结束：解除绑定并通知 agent 取消残留执行（若非正常完成）。
    pub(crate) async fn unbind(self: &Arc<Self>, dispatch_id: &str, healthy: bool) {
        let binding = self.bindings.lock().await.remove(dispatch_id);
        if let Some(binding) = binding {
            // 释放容量许可（drop permit → 槽位归还）。
            *binding.slot_permit.lock().await = None;
            if let Some(registration) = self.agents.lock().await.get(&binding.agent_id).cloned() {
                // 安全递减（会话替换后的计数以当前水位为准，不下溢）。
                let _ = registration.slots_used.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |used| used.checked_sub(1),
                );
                if !healthy {
                    let _ = registration
                        .outbound
                        .send(ToSession::CancelDispatch(dispatch_id.to_string()))
                        .await;
                }
            }
        }
    }

    // 取绑定 agent 当前会话的 outbound（会话已死时返回死通道：发送立即
    // 失败 → runner 走 link-dead → 等待重连裁决）。
    pub(crate) async fn active_outbound(&self, dispatch_id: &str) -> mpsc::Sender<ToSession> {
        let binding = self.bindings.lock().await.get(dispatch_id).cloned();
        if let Some(binding) = binding {
            if let Some(registration) = self.agents.lock().await.get(&binding.agent_id).cloned() {
                return registration.outbound.clone();
            }
        }
        dead_to_session()
    }

    // 发送 drain：agent 停止接新派发、取消在飞、转发最后确认后退出
    // （三期 §1.6）。
    pub async fn drain(&self, agent_id: &str, grace_ms: u64) -> Result<()> {
        let agents = self.agents.lock().await;
        let registration = agents
            .get(agent_id)
            .ok_or_else(|| invalid(format!("agent {agent_id} not connected")))?;
        registration
            .outbound
            .send(ToSession::Send(Message::Drain { grace_ms }))
            .await
            .map_err(|_| invalid("agent session closed"))?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

fn dead_to_session() -> mpsc::Sender<ToSession> {
    let (tx, mut rx) = mpsc::channel(1);
    rx.close();
    tx
}

enum Side {
    Control,
    Data,
}

// 依主日志裁决单项 resume（三期 §1.4 表）：run 终态/派发被替代 →
// CancelAndDrain；结果已提交 → AlreadyCommitted；boot 不符或缺口且无
// 封口结果 → ReconcileRequired；否则续跑（UploadOnly/SubmitExistingResult，
// 权威游标由重挂后的 ack 流传达）。
fn adjudicate_against_journal(
    state: &flow_engine::journal_state::State,
    binding: &BoundDispatch,
    item: &ResumeItem,
) -> ResumeAction {
    let Some(run) = state.runs.get(&binding.run_id) else {
        return ResumeAction::CancelAndDrain;
    };
    if run.terminal() {
        return ResumeAction::CancelAndDrain;
    }
    let Some(node) = run.nodes.get(&binding.node_id) else {
        return ResumeAction::CancelAndDrain;
    };
    if node.dispatch_id != item.dispatch_id {
        return ResumeAction::CancelAndDrain;
    }
    let Some(attempt) = node.attempts.get(&item.dispatch_id) else {
        return ResumeAction::CancelAndDrain;
    };
    if attempt.result.is_some() {
        return ResumeAction::AlreadyCommitted;
    }
    let bound_boot = binding
        .executor_boot_id
        .try_lock()
        .ok()
        .and_then(|guard| guard.clone());
    if bound_boot.as_deref() != Some(item.executor_boot_id.as_str()) {
        return ResumeAction::ReconcileRequired;
    }
    if item.lost_data && item.result_id.is_none() {
        // 数据缺口且无封口结果：缺口阻止成功，进入显式核对，不透明重跑。
        return ResumeAction::ReconcileRequired;
    }
    if item.result_id.is_some() {
        return ResumeAction::SubmitExistingResult;
    }
    ResumeAction::UploadOnly {
        durable_audit_seq: attempt.audit_seq,
    }
}

// resume 场景的包络（executor_id 以绑定记录为准由 attach_resumed 重建）。
fn resume_envelope(
    agent_id: &str,
    agent_boot_id: &str,
    link_session_id: &str,
    dispatch_id: &str,
) -> RouteEnvelope {
    RouteEnvelope {
        agent_id: agent_id.into(),
        agent_boot_id: agent_boot_id.into(),
        link_session_id: link_session_id.into(),
        executor_id: String::new(),
        executor_boot_id: String::new(),
        dispatch_id: Some(dispatch_id.into()),
    }
}

// 远程派发器：journal_driver 的第三执行端口（三期 §1.1）。
//
// 断链时派发挂起等待 agent 重连对账（不立即失败、不重新执行）；重挂后
// 继续驱动同一 runner（账本/屏障状态保留），结果/审计按既有序列幂等推进。
#[derive(Clone)]
pub struct RemoteDispatcher {
    manager: Arc<AgentManager>,
    options: RemoteOptions,
}

impl RemoteDispatcher {
    pub fn new(manager: Arc<AgentManager>, options: RemoteOptions) -> Self {
        Self { manager, options }
    }

    pub fn manager(&self) -> &Arc<AgentManager> {
        &self.manager
    }

    pub(crate) async fn execute(
        &self,
        attempt: &crate::journal_execution::Attempt,
        node: &flow_engine::Node,
        predecessors: std::collections::BTreeMap<String, flow_journal::StoredValue>,
        observations: Option<flow_engine::observation::ObservationStore>,
    ) -> std::result::Result<(), crate::journal::JournalError> {
        use crate::execution::dispatch::DispatchRunner;
        let _permit = self
            .manager
            .global_slots()
            .acquire()
            .await
            .map_err(|_| invalid("remote slots closed"))
            .map_err(crate::journal::JournalError::from)?;
        // 绑定（BindExecutor → agent 本地 spawn+握手 → Ack）。
        let mut events = self
            .manager
            .bind_executor(
                attempt.dispatch_id.clone(),
                attempt.run_id.clone(),
                attempt.node_id.clone(),
            )
            .await
            .map_err(crate::journal::JournalError::from)?;
        let mut runner =
            DispatchRunner::runner(attempt.clone(), node.clone(), predecessors, observations);
        let dispatch_id = attempt.dispatch_id.clone();
        let mut resume = false;
        loop {
            let outcome = if !resume {
                // 首挂：绑定 ack 后链路已就绪。
                AttachOutcome::Link(crate::execution::dispatch::DispatchLink {
                    commands: self.manager.active_outbound(&dispatch_id).await,
                    events: std::mem::replace(&mut events, mpsc::channel(1).1),
                })
            } else {
                self.manager
                    .wait_attach(&dispatch_id, self.options.attach_timeout())
                    .await
            };
            match outcome {
                AttachOutcome::Link(mut link) => {
                    let result = runner.drive_with_resume(&mut link, resume).await;
                    resume = true;
                    match result {
                        Ok(()) => {
                            self.manager.unbind(&dispatch_id, true).await;
                            return Ok(());
                        }
                        Err(error) if is_link_dead(&error) => {
                            // 上联断线：挂起等 Resume 裁决（三期 §1.4）。
                            tracing::warn!(%dispatch_id, "agent link lost; awaiting reconciliation");
                            continue;
                        }
                        Err(error) => {
                            self.manager.unbind(&dispatch_id, false).await;
                            return Err(error);
                        }
                    }
                }
                AttachOutcome::Committed => {
                    self.manager.unbind(&dispatch_id, true).await;
                    return Ok(());
                }
                AttachOutcome::Cancelled(reason) => {
                    self.manager.unbind(&dispatch_id, false).await;
                    return Err(invalid(format!("remote dispatch cancelled: {reason}")).into());
                }
                AttachOutcome::Lost(reason) => {
                    self.manager.unbind(&dispatch_id, false).await;
                    return Err(invalid(format!("remote dispatch lost: {reason}")).into());
                }
            }
        }
    }
}
