//! 远程 agent 管理器（三期 §1.2/§1.3/§1.4）：mTLS 上联、路由包络校验、
//! BindExecutor 归属、Resume 裁决与有界公平转发。
//!
//! 主进程保持唯一提交者：AuditAck/OperationPermit/ResultCommitted 只能由
//! 主进程权威产生后经 agent 转发；agent 中继不生成确认。所有持久事实仍
//! 走一期/二期的 journal 单行事务。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, Semaphore};
use tokio_rustls::TlsAcceptor;

use flow_engine::execution_protocol::contract::{
    bounded_control_text, A_MAX, PROTOCOL_VERSION, STARTUP_TIMEOUT_MS, W_BYTES,
};
use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::remote::{
    capability, validate_route_identity, ResumeAction, ResumeDecision, ResumeItem, RouteEnvelope,
};
use flow_engine::execution_protocol::transport::{FrameTransport, SendBacklog};

use crate::execution::dispatch::DispatchLink;
use crate::execution::ToSession;
use crate::journal::JournalBackend;

type Result<T> = std::result::Result<T, flow_journal::Error>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

type BoxedStream = Pin<Box<dyn AsyncReadWrite + Send>>;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin {}
impl<T: AsyncRead + AsyncWrite + Unpin> AsyncReadWrite for T {}

pub(crate) fn is_link_dead(error: &crate::journal::JournalError) -> bool {
    matches!(error, crate::journal::JournalError::LinkClosed(_))
}

/// WSS 接入的 agent 凭据：token → agent_id 映射表的一项。
///
/// 身份的信任来源跟承载走：mTLS 有证书 CN，WSS 只有 token——那就让
/// token 直接决定身份，而不是让客户端填一张没人验的表。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCredential {
    pub id: String,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct RemoteOptions {
    pub control_addr: String,
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    // 断链后等待 agent 重连对账的总时限（超时按派发失败处理）。
    pub attach_timeout_ms: u64,
    // WSS 接入监听地址（T13，代理终止 TLS 场景的附加承载）；缺省关闭。
    pub wss_addr: Option<String>,
    // WSS 凭据映射表（token → agent_id）：身份由凭证反查，客户端自报头
    // 仅作一致性断言（见 [`authenticate_wss`]）。与 wss_addr 成对配置。
    pub wss_agents: Vec<AgentCredential>,
}

impl RemoteOptions {
    pub fn attach_timeout(&self) -> Duration {
        Duration::from_millis(self.attach_timeout_ms.max(1_000))
    }
}

/// 监听侧并发 TLS/WSS 握手上限（防连接洪泛无界 spawn task/做握手）。
/// TLS/WSS 各自使用独立握手 permit 和启动期限；已认证 agent 注册表
/// 另受同一数量上限约束，同身份重连可替换旧会话。超限关闭后由 agent 退避。
pub const MAX_AGENT_SESSIONS: usize = 256;

// mTLS：验证 agent 客户端证书。
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
            roots
                .add(rustls::pki_types::CertificateDer::from(der.to_vec()))
                .map_err(bad_data)?;
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

/// WSS 认证失败的 401 响应（`ErrorResponse = http::Response<Option<String>>`，
/// `new()` 默认 200，这里显式 401）。
fn unauthorized() -> tokio_tungstenite::tungstenite::handshake::server::ErrorResponse {
    let mut response = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some(
        "unauthorized agent".to_string(),
    ));
    *response.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
    response
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
    events: mpsc::Sender<crate::execution::FromSession>,
}

// 跨会话存活的派发绑定。
struct BoundDispatch {
    agent_id: String,
    // The binding itself reserves one slot, including while disconnected.
    link_session_id: String,
    executor_id: String,
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
    link_session_id: String,
    outbound: mpsc::Sender<ToSession>,
    slots_total: AtomicU64,
    /// Drain 在 master 侧同步打标：候选选择只看本标志，不依赖 agent 回 ack
    /// 的时序——否则 drain 中（slots 已回收、used=0）的 agent 反而最优先被
    /// 选中，bind 被 agent 拒绝后整节点失败。
    draining: AtomicBool,
    cancel: tokio_util::sync::CancellationToken,
}

// Agent 管理器：上联会话注册表 + 全局 A_max 准入 + Resume 裁决。
//
// 锁序（禁止逆序获取）：agents → bindings → BoundDispatch 内部锁
// （executor_boot_id / pending_link / verdict）。pending_binds 独立、不参与嵌套。
// 现有代码在持 bindings 时会 await 单个绑定自己的内部锁——新增代码不得
// 引入相反顺序（内部锁 → bindings/agents，或跨绑定取锁），否则死锁。
pub struct AgentManager {
    backend: Arc<JournalBackend>,
    journal_id: String,
    master_epoch: u64,
    global: Arc<Semaphore>,
    agents: Mutex<BTreeMap<String, Arc<AgentRegistration>>>,
    bindings: Mutex<BTreeMap<String, Arc<BoundDispatch>>>,
    capacity_changed: Notify,
    pending_binds: Mutex<BTreeMap<String, PendingBind>>,
    session_counter: AtomicU64,
    agent_counter: AtomicU64,
    shutdown: tokio_util::sync::CancellationToken,
    tasks: tokio_util::task::TaskTracker,
}

// Bind acknowledgement identity must match the current agent session.
struct PendingBind {
    agent_id: String,
    link_session_id: String,
    executor_id: String,
    events: mpsc::Sender<crate::execution::FromSession>,
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
            capacity_changed: Notify::new(),
            pending_binds: Mutex::new(BTreeMap::new()),
            session_counter: AtomicU64::new(0),
            agent_counter: AtomicU64::new(0),
            shutdown: tokio_util::sync::CancellationToken::new(),
            tasks: tokio_util::task::TaskTracker::new(),
        })
    }

    // 全局活跃派发准入（A_max）。
    pub(crate) fn global_slots(&self) -> &Arc<Semaphore> {
        &self.global
    }

    // TLS 监听入口（真实网络）。单条 mTLS 连接承载整条上联（单流帧：
    // 一帧一形态，变体索引即帧类型），不再有第二条 data 连接。
    pub async fn listen_tls(self: &Arc<Self>, options: RemoteOptions) -> Result<()> {
        install_crypto_provider();
        let tls =
            TlsServer::from_files(&options.ca_cert, &options.server_cert, &options.server_key)
                .map_err(|e| invalid(format!("agent TLS config: {e}")))?;
        let listener = TcpListener::bind(&options.control_addr)
            .await
            .map_err(|e| invalid(format!("agent listen: {e}")))?;
        tracing::info!(
            listen = %listener.local_addr().map_err(|e| invalid(e.to_string()))?,
            "agent TLS listener ready"
        );
        let acceptor = tls.acceptor.clone();
        // 绑定即返回：accept 循环作为后台任务（与 v2 时代语义一致）。
        // 直接 await 会把 start_execution_remote（含生产 remote 模式启动）
        // 永久阻塞在 accept 循环上。
        let manager = self.clone();
        let handshakes = Arc::new(Semaphore::new(MAX_AGENT_SESSIONS));
        self.tasks.spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = manager.shutdown.cancelled() => return,
                    result = listener.accept() => result,
                };
                let Ok((socket, _)) = accepted else {
                    return;
                };
                let Ok(permit) = handshakes.clone().try_acquire_owned() else { continue; };
                let acceptor = acceptor.clone();
                let manager = manager.clone();
                manager.tasks.clone().spawn(async move {
                    let _permit = permit;
                    let handshake = tokio::select! {
                        _ = manager.shutdown.cancelled() => return,
                        result = tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS), acceptor.accept(socket)) => result,
                    };
                    let Ok(Ok(stream)) = handshake else {
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
                    manager.inject_stream(agent_id, Box::pin(stream)).await;
                });
            }
        });
        Ok(())
    }

    /// WSS 接入（T13）：反向代理/统一入口场景的**附加**承载。明文 TCP，
    /// 由代理终止 TLS；身份由 token→agent_id 映射表决定（凭证反查），
    /// 客户端自报的 `x-flow-agent-id` 只做一致性断言（不匹配即 401）。
    ///
    /// 直连 mTLS 路径（[`AgentManager::listen_tls`]）不变。
    ///
    /// 协议层面：一条完整帧对应一条（或分片后多条）WS Binary 消息；Text/超限消息拒绝；
    /// 协商上限由 [`MAX_WS_MESSAGE_BYTES`] 硬约束。
    #[allow(clippy::result_large_err)] // accept_hdr 回调签名固定，ErrorResponse 为大类型
    pub async fn listen_wss(
        self: &Arc<Self>,
        addr: String,
        agents: Vec<AgentCredential>,
    ) -> Result<()> {
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
        use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| invalid(format!("agent wss listen: {e}")))?;
        tracing::info!(
            listen = %listener.local_addr().map_err(|e| invalid(e.to_string()))?,
            "agent WSS listener ready"
        );
        let manager = self.clone();
        let limit = Some(flow_engine::execution_protocol::ws::MAX_WS_MESSAGE_BYTES);
        let config = Some(
            WebSocketConfig::default()
                .max_message_size(limit)
                .max_frame_size(limit),
        );
        let handshakes = Arc::new(Semaphore::new(MAX_AGENT_SESSIONS));
        self.tasks.spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = manager.shutdown.cancelled() => return,
                    result = listener.accept() => result,
                };
                let Ok((socket, _)) = accepted else {
                    return;
                };
                let Ok(permit) = handshakes.clone().try_acquire_owned() else { continue; };
                let manager = manager.clone();
                let agents = agents.clone();
                manager.tasks.clone().spawn(async move {
                    let _permit = permit;
                    let agent_slot: std::sync::Arc<std::sync::Mutex<Option<String>>> =
                        std::sync::Arc::new(std::sync::Mutex::new(None));
                    let slot = agent_slot.clone();
                    let handshake = tokio_tungstenite::accept_hdr_async_with_config(
                        socket,
                        move |request: &Request, response: Response| match authenticate_wss(
                            request, &agents,
                        ) {
                            Some(agent_id) => {
                                *slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(agent_id);
                                Ok(response)
                            }
                            None => Err(unauthorized()),
                        },
                        config,
                    );
                    let handshake = tokio::select! {
                        _ = manager.shutdown.cancelled() => return,
                        result = tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS), handshake) => result,
                    };
                    let Ok(Ok(ws)) = handshake else {
                        return;
                    };
                    let agent_id = agent_slot.lock().unwrap_or_else(|p| p.into_inner()).take();
                    let Some(agent_id) = agent_id else {
                        return;
                    };
                    manager
                        .inject_stream(
                            agent_id,
                            Box::pin(flow_engine::execution_protocol::ws::WsByteStream::new(ws)),
                        )
                        .await;
                });
            }
        });
        Ok(())
    }

    // R0 模拟中继入口：直接注入单流（无 TLS；身份由注入方保证）。
    pub async fn attach_inprocess(
        self: &Arc<Self>,
        agent_id: String,
        stream: impl AsyncReadWrite + Send + 'static,
    ) {
        self.inject_stream(agent_id, Box::pin(stream)).await;
    }

    async fn inject_stream(self: &Arc<Self>, agent_id: String, stream: BoxedStream) {
        let manager = self.clone();
        let agent_id = agent_id.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                _ = manager.shutdown.cancelled() => {},
                result = manager.run_session(agent_id, stream) => {
                    if let Err(error) = result { tracing::warn!(%error, "agent session ended"); }
                }
            }
        });
    }

    // agent 上联握手（T02）：AgentHello（版本门）→ 校验 → AgentWelcome。
    // 与执行器握手同样的「明确拒绝，不降级」：postcard 变体索引是位置
    // 编码，跨版本配对会静默错解成另一个消息类型，版本不匹配在握手处
    // 显式拒绝并断链（错误信息含双方版本号）。
    async fn session_handshake(
        self: &Arc<Self>,
        transport: &mut FrameTransport,
        agent_id: &str,
    ) -> std::result::Result<(String, u32, String), String> {
        let message = tokio::time::timeout(
            Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
            transport.recv(),
        )
        .await
        .map_err(|_| "agent hello timeout".to_string())?
        .map_err(|e| format!("read hello: {e}"))?;
        let Message::AgentHello {
            version,
            agent_boot_id,
            build: _,
            capabilities,
            slots,
        } = message
        else {
            return Err("expected AgentHello".into());
        };
        if version != PROTOCOL_VERSION {
            let _ = transport
                .send(Message::Reject {
                    reason: bounded_control_text(format!(
                        "agent protocol version {version} not supported \
                         (master speaks {PROTOCOL_VERSION})"
                    )),
                })
                .await;
            return Err(format!(
                "agent protocol version mismatch: agent={version} master={PROTOCOL_VERSION}"
            ));
        }
        for required in capability::REQUIRED {
            if !capabilities.iter().any(|c| c == required) {
                return Err(format!("agent missing capability {required}"));
            }
        }
        if !validate_route_identity(agent_id) || !validate_route_identity(&agent_boot_id) {
            return Err("invalid agent identity".into());
        }
        let link_session_id = format!(
            "link-{}",
            self.session_counter.fetch_add(1, Ordering::Relaxed)
        );
        transport
            .send(Message::AgentWelcome {
                version: PROTOCOL_VERSION,
                journal_id: self.journal_id.clone(),
                master_epoch: self.master_epoch,
                link_session_id: link_session_id.clone(),
                agent_id: agent_id.to_string(),
                heartbeat_ms: flow_engine::execution_protocol::HEARTBEAT_INTERVAL_MS,
                agent_window_bytes: W_BYTES * A_MAX as u64,
            })
            .await
            .map_err(|e| format!("write welcome: {e}"))?;
        Ok((agent_boot_id, slots, link_session_id))
    }

    // 单个 agent 会话（T02）：握手与业务帧共用同一条 transport
    //（单流：无需第二条连接与 DataBind）。
    async fn run_session(
        self: &Arc<Self>,
        agent_id: String,
        stream: BoxedStream,
    ) -> std::result::Result<(), String> {
        // 1) 握手全程走同一条 transport（T02）：先建传输再收发 AgentHello/
        // AgentWelcome。不再用临时 decoder 取首帧后丢弃——同一次 read 里
        // Hello 之后的尾随帧不会在交接时丢失。
        let mut transport = FrameTransport::spawn(stream, 128);
        let (agent_boot_id, slots, link_session_id) = tokio::select! {
            _ = self.shutdown.cancelled() => return Ok(()),
            result = tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS), self.session_handshake(&mut transport, &agent_id)) =>
                result.map_err(|_| "agent handshake deadline exceeded".to_string())??,
        };
        // 3) 注册会话（替换旧会话：旧 outbound 关闭 → 相关 runner link-dead）。
        let (outbound, mut commands) = mpsc::channel::<ToSession>(256);
        let registration = Arc::new(AgentRegistration {
            agent_id: agent_id.clone(),
            link_session_id: link_session_id.clone(),
            outbound: outbound.clone(),
            slots_total: AtomicU64::new(slots as u64),
            draining: AtomicBool::new(false),
            cancel: tokio_util::sync::CancellationToken::new(),
        });
        let previous = {
            let mut agents = self.agents.lock().await;
            if agents.len() >= MAX_AGENT_SESSIONS && !agents.contains_key(&agent_id) {
                return Err("authenticated agent session limit reached".into());
            }
            agents.insert(agent_id.clone(), registration.clone())
        };
        if let Some(previous) = previous {
            previous.cancel.cancel();
            tracing::info!(%agent_id, "agent reconnected; old uplink replaced");
        }
        self.capacity_changed.notify_waiters();
        tracing::info!(%agent_id, slots, "agent session registered");
        let mut routings: BTreeMap<String, SessionRouting> = BTreeMap::new();
        // 上联活跃看门狗（§3.7 Suspect）：agent 周期性发心跳，任何入站帧
        // 都算活跃证据；彻底静默超过 HEARTBEAT_TIMEOUT_MS 即判失联，
        // 结束会话（绑定转入 Resume 对账，agent 会退避重连）。没有它，
        // 静默网络分区会留下僵尸 registration：bind 每次烧满超时才失败。
        let mut liveness = tokio::time::Instant::now();
        let mut liveness_tick = tokio::time::interval(Duration::from_millis(
            flow_engine::execution_protocol::HEARTBEAT_TIMEOUT_MS,
        ));
        liveness_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Bounded receipts absorb in-flight duplicates after the live route is released.
        let mut completed: std::collections::VecDeque<(RouteEnvelope, Option<String>)> =
            Default::default();
        let mut resume_items = Vec::new();
        // 出站积压（T03）：handler 内只 push、循环顶 flush，不 await
        // `transport.send`——出站阻塞（队列满 + 对端停读）不能卡住整个
        // select，否则入站心跳/Cancel/Resume 与 liveness 看门狗全部停摆，
        // 双向饱和时两侧环路互相等死。FIFO 积压保住帧提交序；积压超限
        // 视为会话失效，显式终止（不用无界队列化解背压）。
        let mut send_backlog = SendBacklog::with_capacity(16);
        let result: std::result::Result<(), String> = async {
            loop {
                if let Err(error) = send_backlog.flush(&transport) {
                    return Err(format!("agent {agent_id} uplink send failed: {error}"));
                }
                tokio::select! {
                    _ = registration.cancel.cancelled() => return Ok(()),
                    _ = self.shutdown.cancelled() => return Ok(()),
                    _ = liveness_tick.tick() => {
                        if liveness.elapsed()
                            > Duration::from_millis(
                                flow_engine::execution_protocol::HEARTBEAT_TIMEOUT_MS,
                            )
                        {
                            return Err(format!(
                                "agent {agent_id} uplink silent for over {}ms",
                                flow_engine::execution_protocol::HEARTBEAT_TIMEOUT_MS
                            ));
                        }
                    }
                    // 积压占用期间不拉取新出站消息（自然背压到生产方），
                    // 也保住帧提交序：积压非空时新帧必须排在其后。
                    command = commands.recv(), if send_backlog.is_empty() => {
                        let Some(command) = command else { return Ok(()); };
                        match command {
                            ToSession::Send(message) => {
                                // 业务帧按已确认路由包装；管理帧
                                // （BindExecutor/ResumeReply/Drain…）直接发送。
                                if let Message::ResultCommitted { dispatch_id, result_id } = &*message {
                                    if let Some(route) = routings.remove(dispatch_id) {
                                        completed.push_back((route.envelope, Some(result_id.clone())));
                                        if completed.len() > 256 { completed.pop_front(); }
                                    }
                                }
                                let frame = if flow_engine::execution_protocol::remote::routable(
                                    &message,
                                ) {
                                    match message
                                        .dispatch_id()
                                        .and_then(|id| routings.get(id).map(|route| &route.envelope)
                                            .or_else(|| completed.iter().rev().find(|(envelope, _)| envelope.dispatch_id.as_deref() == Some(id)).map(|(envelope, _)| envelope)))
                                    {
                                        Some(routing) => Message::Routed {
                                            envelope: routing.clone(),
                                            inner: message,
                                        },
                                        None => continue, // 未绑定：不发送
                                    }
                                } else {
                                    *message
                                };
                                // 出站拥塞不阻塞环路（T03）：进 FIFO 积压，
                                // 循环顶统一冲刷；积压超限 = 会话失效。
                                if send_backlog.push(&transport, frame).is_err() {
                                    return Err(format!(
                                        "agent {agent_id} uplink send backlog overflow"
                                    ));
                                }
                            }
                            ToSession::CancelDispatch(dispatch_id) => {
                                let Some(routing) = routings.get(&dispatch_id) else { continue };
                                let envelope = routing.envelope.clone();
                                let cancel_frame = Message::Routed {
                                    envelope,
                                    inner: Box::new(Message::Cancel {
                                        command_id: format!("cancel-{dispatch_id}"),
                                        dispatch_id: dispatch_id.clone(),
                                    }),
                                };
                                if send_backlog.push(&transport, cancel_frame).is_err() {
                                    return Err(format!(
                                        "agent {agent_id} uplink send backlog overflow"
                                    ));
                                }
                                if let Some(route) = routings.remove(&dispatch_id) {
                                    completed.push_back((route.envelope, None));
                                    if completed.len() > 256 { completed.pop_front(); }
                                }
                            }
                            ToSession::Recycle => {
                                transport.close();
                                return Ok(());
                            }
                        }
                    }
                    received = transport.recv() => {
                        liveness = tokio::time::Instant::now();
                        let message = received.map_err(|e| format!("recv: {e}"))?;
                        match message {
                            Message::Routed { envelope, inner } => {
                                tracing::trace!(kind = inner.type_name(), dispatch = ?envelope.dispatch_id, "agent routed message");
                                // 包络校验：会话身份 + 绑定归属 + boot 一致。
                                if envelope.agent_id != agent_id
                                    || envelope.agent_boot_id != agent_boot_id
                                    || envelope.link_session_id != link_session_id
                                {
                                    return Err(format!("route envelope mismatch from agent {agent_id}"));
                                }
                                // 内层必须是可路由业务帧（出站 wrap 侧的同一规则，
                                // 在入站方向补齐）：嵌套 Routed / 上联管理帧 / 握手帧
                                // 是协议违规，关闭会话而不是往派发队列里转发。
                                if !flow_engine::execution_protocol::remote::routable(&inner) {
                                    return Err(format!(
                                        "non-routable inner message from agent {agent_id} rejected"
                                    ));
                                }
                                // Routed 帧必须携带派发身份：无 dispatch_id 既无法路由
                                // 也无法与完成记录比对——按协议违规关闭会话，而不是 unwrap。
                                let Some(dispatch_id) = envelope.dispatch_id.clone() else {
                                    return Err(format!(
                                        "routed frame without dispatch id from agent {agent_id} rejected"
                                    ));
                                };
                                let Some(routing) = routings.get(&dispatch_id) else {
                                    if let Some((previous, result_id)) = completed.iter().find(|(previous, _)| previous == &envelope) {
                                        if let (Some(result_id), Message::Result { result_id: id, .. }) = (result_id, &*inner) {
                                            if id == result_id
                                                && send_backlog.push(&transport, Message::Routed { envelope: previous.clone(), inner: Box::new(Message::ResultCommitted {
                                                        dispatch_id, result_id: result_id.clone(),
                                                    })}).is_err() {
                                                    return Err(format!(
                                                        "agent {agent_id} uplink send backlog overflow"
                                                    ));
                                                }
                                        }
                                        continue;
                                    }
                                    // 未绑定/错路由：明确拒绝（协议错误关闭会话），
                                    // 不进入权威日志。
                                    return Err(format!(
                                        "routed frame for unbound dispatch rejected: {agent_id}"
                                    ));
                                };
                                if envelope.executor_boot_id != routing.envelope.executor_boot_id {
                                    return Err("executor boot mismatch in envelope".into());
                                }
                                // Correctness frames cannot be dropped, and a slow runner
                                // must not hold the shared uplink select loop indefinitely.
                                // A full per-dispatch queue invalidates this session; journal
                                // facts and Resume adjudication decide recovery.
                                routing.events.try_send(crate::execution::FromSession::Incoming(inner))
                                    .map_err(|_| "dispatch inbound queue full or closed; resume required".to_string())?;
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
                                    if bind.agent_id != agent_id || bind.link_session_id != link_session_id || bind.executor_id != executor_id {
                                        let _ = bind.ack.send(Err(invalid("bind acknowledgement identity mismatch")));
                                        return Err("bind acknowledgement identity mismatch".into());
                                    }
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
                                        self.note_bound(&dispatch_id, executor_boot_id.clone()).await;
                                        let _ = bind.ack.send(Ok(executor_boot_id));
                                    } else {
                                        let _ = bind.ack.send(Err(invalid(format!(
                                            "agent bind failed: {}",
                                            error.unwrap_or_else(|| "unknown".into())
                                        ))));
                                    }
                                } else if ok {
                                    // The bind timed out or its dispatcher was cancelled while
                                    // the executor was starting. Reap that late successful bind.
                                    let envelope = RouteEnvelope {
                                        agent_id: agent_id.clone(), agent_boot_id: agent_boot_id.clone(),
                                        link_session_id: link_session_id.clone(), executor_id,
                                        executor_boot_id, dispatch_id: Some(dispatch_id.clone()),
                                    };
                                    completed.push_back((envelope.clone(), None));
                                    if completed.len() > 256 { completed.pop_front(); }
                                    if send_backlog.push(&transport, Message::Routed {
                                        envelope,
                                        inner: Box::new(Message::Cancel {
                                            command_id: format!("late-bind-{dispatch_id}"), dispatch_id,
                                        }),
                                    }).is_err() {
                                        return Err(format!(
                                            "agent {agent_id} uplink send backlog overflow"
                                        ));
                                    }
                                }
                            }
                            Message::CapacityReport { capacity } => {
                                registration
                                    .slots_total
                                    .store(capacity.slots as u64, Ordering::Relaxed);
                                // in_flight is telemetry, not an admission counter.
                                self.capacity_changed.notify_waiters();
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
                                                    if send_backlog.push(&transport, Message::Routed {
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
                                                        }).is_err() {
                                                        return Err(format!(
                                                            "agent {agent_id} uplink send backlog overflow"
                                                        ));
                                                    }
                                                }
                                            }
                                            routings.remove(&dispatch_id);
                                            self.resolve_binding(&dispatch_id, AttachOutcome::Committed)
                                                .await;
                                        }
                                        ResumeAction::CancelAndDrain => {
                                            routings.remove(&dispatch_id);
                                            if send_backlog.push(&transport, Message::Routed {
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
                                                }).is_err() {
                                                return Err(format!(
                                                    "agent {agent_id} uplink send backlog overflow"
                                                ));
                                            }
                                            self.resolve_binding(
                                                &dispatch_id,
                                                AttachOutcome::Cancelled("resume: cancel and drain".into()),
                                            )
                                            .await;
                                        }
                                        ResumeAction::ReconcileRequired => {
                                            routings.remove(&dispatch_id);
                                            if send_backlog.push(&transport, Message::Routed {
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
                                                }).is_err() {
                                                return Err(format!(
                                                    "agent {agent_id} uplink send backlog overflow"
                                                ));
                                            }
                                            self.resolve_binding(
                                                &dispatch_id,
                                                AttachOutcome::Lost("resume: reconcile required".into()),
                                            )
                                            .await;
                                        }
                                    }
                                }
                                if send_backlog.push(&transport, Message::ResumeReply { decisions: decisions.clone(), page }).is_err() {
                                    return Err(format!(
                                        "agent {agent_id} uplink send backlog overflow"
                                    ));
                                }
                                resume_items.extend(items.iter().cloned());
                                // Every page establishes routes before the next page arrives.
                                let continuing: Vec<_> = items.iter().filter(|item| decisions.iter().any(|decision|
                                    decision.dispatch_id == item.dispatch_id && matches!(decision.action, ResumeAction::UploadOnly { .. } | ResumeAction::SubmitExistingResult))).cloned().collect();
                                self.attach_resumed(&agent_id, &continuing, &outbound, &mut routings, &link_session_id, &agent_boot_id).await;
                                if more { continue; }
                                // 清单末页：未上报的该 agent 绑定判 Lost（执行器已失联）。
                                self.mark_unreported_lost(&agent_id, &link_session_id, &resume_items).await;
                                resume_items.clear();

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
        registration.cancel.cancel();
        self.pending_binds
            .lock()
            .await
            .retain(|_, bind| bind.link_session_id != link_session_id);
        self.capacity_changed.notify_waiters();
        result
    }

    async fn note_bound(&self, dispatch_id: &str, executor_boot_id: String) {
        if let Some(bound) = self.bindings.lock().await.get(dispatch_id) {
            *bound.executor_boot_id.lock().await = Some(executor_boot_id);
        }
    }

    // Resume 裁决（三期 §1.4 表）：以主日志为准，不信 agent 自报游标。
    async fn adjudicate_resume(&self, agent_id: &str, items: &[ResumeItem]) -> Vec<ResumeDecision> {
        let bindings = self.bindings.lock().await.clone();
        let mut decisions = Vec::new();
        for item in items {
            let action = match bindings.get(&item.dispatch_id) {
                Some(binding) if binding.agent_id == agent_id => {
                    let boot = binding.executor_boot_id.lock().await.clone();
                    self.backend
                        .inspect(|state| {
                            adjudicate_against_journal(state, binding, item, boot.as_deref())
                        })
                        .await
                }
                _ => ResumeAction::ReconcileRequired,
            };
            decisions.push(ResumeDecision {
                dispatch_id: item.dispatch_id.clone(),
                action,
            });
        }
        decisions
    }

    // 写入终局裁决，保留到 dispatcher 取走并释放绑定。
    async fn resolve_binding(&self, dispatch_id: &str, outcome: AttachOutcome) {
        let binding = self.bindings.lock().await.get(dispatch_id).cloned();
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
    async fn mark_unreported_lost(
        &self,
        agent_id: &str,
        link_session_id: &str,
        items: &[ResumeItem],
    ) {
        let reported: std::collections::BTreeSet<&str> =
            items.iter().map(|item| item.dispatch_id.as_str()).collect();
        let bindings = self.bindings.lock().await;
        let lost: Vec<_> = bindings
            .iter()
            .filter(|(id, bound)| {
                bound.agent_id == agent_id
                    && bound.link_session_id != link_session_id
                    && !reported.contains(id.as_str())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in lost {
            if let Some(bound) = bindings.get(&id) {
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
                executor_id: binding.executor_id.clone(),
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
    ) -> Result<mpsc::Receiver<crate::execution::FromSession>> {
        let registration = loop {
            let changed = self.capacity_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let agents = self.agents.lock().await;
            if agents.is_empty() {
                return Err(invalid("no agent connected"));
            }
            let mut bindings = self.bindings.lock().await;
            let pick = agents
                .values()
                .filter_map(|agent| {
                    if agent.draining.load(Ordering::Acquire) {
                        return None;
                    }
                    let used = bindings
                        .values()
                        .filter(|bound| bound.agent_id == agent.agent_id)
                        .count();
                    (used < agent.slots_total.load(Ordering::Relaxed) as usize)
                        .then_some((used, agent.clone()))
                })
                .min_by_key(|(used, _)| *used);
            if let Some((_, agent)) = pick {
                let executor_id = format!(
                    "exec-{}@{}",
                    self.agent_counter.fetch_add(1, Ordering::Relaxed),
                    agent.agent_id
                );
                bindings.insert(
                    dispatch_id.clone(),
                    Arc::new(BoundDispatch {
                        agent_id: agent.agent_id.clone(),
                        link_session_id: agent.link_session_id.clone(),
                        executor_id: executor_id.clone(),
                        executor_boot_id: Mutex::new(None),
                        run_id,
                        node_id,
                        pending_link: Mutex::new(None),
                        verdict: Mutex::new(None),
                        notify: Notify::new(),
                    }),
                );
                // 绑定与 executor_id 同临界区产出：不回读 bindings，
                // 消除插入后到读取前被并发 unbind 摘除的 panic 窗口。
                break (agent, executor_id);
            }
            drop(bindings);
            drop(agents);
            tokio::select! {
                _ = changed => {},
                _ = self.shutdown.cancelled() => return Err(invalid("agent manager shut down")),
            }
        };
        let (registration, executor_id) = registration;
        let (events, rx) = mpsc::channel(256);
        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_binds.lock().await.insert(
            dispatch_id.clone(),
            PendingBind {
                agent_id: registration.agent_id.clone(),
                link_session_id: registration.link_session_id.clone(),
                executor_id: executor_id.clone(),
                events,
                ack: ack_tx,
            },
        );
        let result = async {
            tokio::select! {
                result = registration.outbound.send(ToSession::Send(Box::new(Message::BindExecutor { executor_id, dispatch_id: dispatch_id.clone() }))) => {
                    result.map_err(|_| invalid("agent session closed before bind"))?;
                }
                _ = registration.cancel.cancelled() => return Err(invalid("agent session replaced before bind")),
            }
            tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS * 4), ack_rx).await
                .map_err(|_| invalid("agent bind acknowledgement timeout"))?
                .map_err(|_| invalid("agent bind acknowledgement lost"))??;
            Ok(rx)
        }.await;
        if result.is_err() {
            self.unbind(&dispatch_id, false).await;
        }
        result
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
        self.pending_binds.lock().await.remove(dispatch_id);
        let binding = self.bindings.lock().await.remove(dispatch_id);
        self.capacity_changed.notify_waiters();
        if let Some(binding) = binding {
            if !healthy {
                if let Some(registration) = self.agents.lock().await.get(&binding.agent_id).cloned()
                {
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
        registration.draining.store(true, Ordering::Release);
        registration
            .outbound
            .send(ToSession::Send(Box::new(Message::Drain { grace_ms })))
            .await
            .map_err(|_| invalid("agent session closed"))?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        self.agents.lock().await.clear();
        self.pending_binds.lock().await.clear();
    }
}

fn dead_to_session() -> mpsc::Sender<ToSession> {
    let (tx, mut rx) = mpsc::channel(1);
    rx.close();
    tx
}

// 依主日志裁决单项 resume（三期 §1.4 表）：run 终态/派发被替代 →
// CancelAndDrain；结果已提交 → AlreadyCommitted；boot 不符或缺口且无
// 封口结果 → ReconcileRequired；否则续跑（UploadOnly/SubmitExistingResult，
// 权威游标由重挂后的 ack 流传达）。
fn adjudicate_against_journal(
    state: &flow_engine::journal_state::State,
    binding: &BoundDispatch,
    item: &ResumeItem,
    bound_boot: Option<&str>,
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
    if bound_boot != Some(item.executor_boot_id.as_str()) {
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
        // Cancellation/panic must release pending binds as well as established ones.
        let _binding_guard = BindingGuard {
            manager: self.manager.clone(),
            dispatch_id: attempt.dispatch_id.clone(),
        };
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

struct BindingGuard {
    manager: Arc<AgentManager>,
    dispatch_id: String,
}
impl Drop for BindingGuard {
    fn drop(&mut self) {
        let manager = self.manager.clone();
        let dispatch_id = self.dispatch_id.clone();
        tokio::spawn(async move {
            manager.unbind(&dispatch_id, false).await;
        });
    }
}

/// WSS 认证（T13）：`Authorization: Bearer <token>` 对映射表逐项常量时间
/// 比较，**由表反查 agent_id**——身份来源是凭证，不是客户端填的表单。
///
/// 客户端自报的 `x-flow-agent-id` 降级为一致性断言：声明了就必须与凭证
/// 反查结果一致，否则 401；未声明则直接采用反查结果。任何单一凭证只能
/// 取得一个 agent 身份，冒充他 agent 在这里就进不了门（会话替换也因此
/// 天然要求持有同一身份的凭证）。
fn authenticate_wss(
    request: &tokio_tungstenite::tungstenite::handshake::server::Request,
    agents: &[AgentCredential],
) -> Option<String> {
    let provided = request
        .headers()
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    // 不提前退出：用比较次数泄露 token 位置等于泄露半个凭证。
    let mut matched: Option<&AgentCredential> = None;
    for agent in agents {
        if constant_time_eq(provided.as_bytes(), agent.token.as_bytes()) {
            matched = Some(agent);
        }
    }
    let agent = matched?;
    if !validate_route_identity(&agent.id) {
        return None;
    }
    if let Some(claimed) = request.headers().get("x-flow-agent-id") {
        let claimed = claimed.to_str().ok()?;
        if claimed != agent.id {
            return None;
        }
    }
    Some(agent.id.clone())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod capacity_tests {
    use super::*;

    #[tokio::test]
    async fn replacement_session_keeps_outstanding_capacity_reserved() {
        let root = std::env::temp_dir().join(format!("flow-capacity-{}", uuid::Uuid::now_v7()));
        let backend = JournalBackend::open(&root, flow_journal::JournalOptions::default())
            .await
            .unwrap();
        let manager = AgentManager::new(backend.clone(), backend.journal.id().into(), 1);
        let (outbound, mut messages) = mpsc::channel(8);
        let register = |session: &str| {
            Arc::new(AgentRegistration {
                agent_id: "agent".into(),
                link_session_id: session.into(),
                outbound: outbound.clone(),
                slots_total: AtomicU64::new(1),
                draining: AtomicBool::new(false),
                cancel: tokio_util::sync::CancellationToken::new(),
            })
        };
        manager
            .agents
            .lock()
            .await
            .insert("agent".into(), register("old"));
        let first = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .bind_executor("first".into(), "run".into(), "node".into())
                    .await
            })
        };
        assert!(matches!(messages.recv().await, Some(ToSession::Send(_))));
        manager
            .pending_binds
            .lock()
            .await
            .remove("first")
            .unwrap()
            .ack
            .send(Ok("boot-first".into()))
            .unwrap();
        let _first_events = first.await.unwrap().unwrap();
        manager
            .agents
            .lock()
            .await
            .insert("agent".into(), register("new"));
        manager.capacity_changed.notify_waiters();
        let second = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .bind_executor("second".into(), "run".into(), "node".into())
                    .await
            })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), messages.recv())
                .await
                .is_err(),
            "replacement must not reset used capacity"
        );
        assert_eq!(manager.bindings.lock().await.len(), 1);
        manager.unbind("first", true).await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), messages.recv())
                .await
                .unwrap(),
            Some(ToSession::Send(_))
        ));
        manager
            .pending_binds
            .lock()
            .await
            .remove("second")
            .unwrap()
            .ack
            .send(Ok("boot-second".into()))
            .unwrap();
        let _second_events = second.await.unwrap().unwrap();
        manager.unbind("second", true).await;
        assert!(manager.pending_binds.lock().await.is_empty());
        assert!(manager.bindings.lock().await.is_empty());
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// W1：WSS 身份由凭证映射表决定；自报 header 只做一致性断言。
#[cfg(test)]
mod auth_tests {
    use super::*;

    fn request(
        token: Option<&str>,
        claimed: Option<&str>,
    ) -> tokio_tungstenite::tungstenite::handshake::server::Request {
        let mut builder = tokio_tungstenite::tungstenite::http::Request::builder();
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        if let Some(claimed) = claimed {
            builder = builder.header("x-flow-agent-id", claimed);
        }
        builder.body(()).unwrap()
    }

    fn table() -> Vec<AgentCredential> {
        vec![
            AgentCredential {
                id: "agent-a".into(),
                token: "token-a".into(),
            },
            AgentCredential {
                id: "agent-b".into(),
                token: "token-b".into(),
            },
        ]
    }

    #[test]
    fn token_decides_identity() {
        // 正确凭证 + 一致声明 / 不声明：身份都是凭证反查结果。
        assert_eq!(
            authenticate_wss(&request(Some("token-a"), Some("agent-a")), &table()),
            Some("agent-a".into())
        );
        assert_eq!(
            authenticate_wss(&request(Some("token-b"), None), &table()),
            Some("agent-b".into())
        );
    }

    #[test]
    fn claimed_header_cannot_impersonate_another_agent() {
        // 持 A 凭证声明 B 身份 / 伪造 header 冒充他 agent / 错缺凭证：一律拒绝。
        assert_eq!(
            authenticate_wss(&request(Some("token-a"), Some("agent-b")), &table()),
            None
        );
        assert_eq!(
            authenticate_wss(&request(Some("token-b"), Some("agent-a")), &table()),
            None
        );
        assert_eq!(
            authenticate_wss(&request(Some("wrong"), Some("agent-a")), &table()),
            None
        );
        assert_eq!(
            authenticate_wss(&request(None, Some("agent-a")), &table()),
            None
        );
        assert_eq!(
            authenticate_wss(&request(Some("token-a"), Some("")), &table()),
            None
        );
    }

    #[test]
    fn unknown_token_matches_no_identity() {
        assert_eq!(
            authenticate_wss(&request(Some("token-c"), None), &table()),
            None
        );
    }
}

/// W2/W3：上联握手版本门 + 会话环路出站阻塞不再卡死（T03）。
#[cfg(test)]
mod session_tests {
    use super::*;

    async fn manager_with_data() -> (Arc<AgentManager>, Arc<JournalBackend>, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("flow-session-{}", uuid::Uuid::now_v7()));
        let backend = JournalBackend::open(&root, flow_journal::JournalOptions::default())
            .await
            .unwrap();
        let manager = AgentManager::new(backend.clone(), "journal-test".into(), 1);
        (manager, backend, root)
    }

    #[tokio::test]
    async fn shutdown_reclaims_listener_and_silent_handshake() {
        let (manager, backend, root) = manager_with_data().await;
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        manager
            .listen_wss(
                address.to_string(),
                vec![AgentCredential {
                    id: "agent".into(),
                    token: "secret".into(),
                }],
            )
            .await
            .unwrap();
        let _silent = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("shutdown must interrupt handshake");
        let _rebound = TcpListener::bind(address)
            .await
            .expect("listener port must be released");
        assert!(manager.tasks.is_empty());
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// 建立一对单流会话：master 侧走 attach_inprocess，测试侧持 transport。
    /// 返回 (测试侧 transport, link_session_id)。
    async fn session_pair(manager: &Arc<AgentManager>, agent_id: &str) -> (FrameTransport, String) {
        let (a, b) = tokio::io::duplex(256);
        manager.attach_inprocess(agent_id.to_string(), a).await;
        let mut transport = FrameTransport::spawn(b, 8);
        transport
            .send(Message::agent_hello(
                "boot-1",
                "test",
                capability::REQUIRED.iter().map(|s| s.to_string()).collect(),
                1,
            ))
            .await
            .unwrap();
        let welcome = transport.recv().await.unwrap();
        let Message::AgentWelcome {
            link_session_id, ..
        } = welcome
        else {
            panic!("expected AgentWelcome, got {}", welcome.type_name());
        };
        (transport, link_session_id)
    }

    /// W3：对端停读 + 出站饱和时入站帧仍被处理（旧版卡死在
    /// `transport.send().await`，心跳/Cancel/Resume 全部停摆）。
    #[tokio::test]
    async fn inbound_flows_while_outbound_is_saturated() {
        let (manager, backend, root) = manager_with_data().await;
        let (transport, link_session_id) = session_pair(&manager, "agent-1").await;
        // 一个待应答的 bind：inbound BindExecutorAck 是「入站仍被处理」的可观察点。
        let (events, _rx) = mpsc::channel(8);
        let (ack_tx, ack_rx) = oneshot::channel();
        manager.pending_binds.lock().await.insert(
            "d1".into(),
            PendingBind {
                agent_id: "agent-1".into(),
                link_session_id,
                executor_id: "e1".into(),
                events,
                ack: ack_tx,
            },
        );
        // 灌满出站（对端停读）：通道满 = transport 队列 + 底层缓冲已饱和、
        // 环路不再消费出站命令。
        let registration = manager.agents.lock().await.get("agent-1").cloned().unwrap();
        let mut sent = 0usize;
        loop {
            match registration
                .outbound
                .try_send(ToSession::Send(Box::new(Message::Drain { grace_ms: 0 })))
            {
                Ok(()) => sent += 1,
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    panic!("session died during saturation after {sent} frames")
                }
            }
            assert!(sent < 100_000, "outbound never saturated");
        }
        // 入站 ack 必须仍被处理并唤醒 binder。
        transport
            .send(Message::BindExecutorAck {
                executor_id: "e1".into(),
                executor_boot_id: "boot-e1".into(),
                dispatch_id: "d1".into(),
                ok: true,
                error: None,
            })
            .await
            .unwrap();
        let ack = tokio::time::timeout(Duration::from_secs(5), ack_rx)
            .await
            .expect("inbound bind ack must be processed while outbound is saturated")
            .expect("ack channel")
            .expect("bind acknowledged");
        assert_eq!(ack, "boot-e1");
        manager.shutdown().await;
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// W3：出站积压超限 = 会话失效，显式终止（不是永久悬挂）。
    #[tokio::test]
    async fn outbound_backlog_overflow_terminates_session() {
        let (manager, backend, root) = manager_with_data().await;
        let (transport, _link_session_id) = session_pair(&manager, "agent-1").await;
        let registration = manager.agents.lock().await.get("agent-1").cloned().unwrap();
        // 对端停读 + 入站洪水：每个 Resume 触发一个 ResumeReply 出站帧，
        // 积压超限后 push 报错 → 会话终止。
        let done = tokio::time::timeout(Duration::from_secs(20), async {
            for page in 0..20_000u32 {
                if registration.cancel.is_cancelled() {
                    return;
                }
                if transport
                    .send(Message::Resume {
                        items: vec![],
                        page,
                        more: true,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        })
        .await;
        assert!(done.is_ok(), "会话环路被出站阻塞卡死");
        assert!(
            registration.cancel.is_cancelled(),
            "出站积压超限必须显式终止会话，而不是永久悬挂"
        );
        manager.shutdown().await;
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// W2：跨版本 AgentHello 在握手处显式拒绝（错误信息含双方版本号），
    /// 不静默错解成另一个消息类型。
    #[tokio::test]
    async fn agent_hello_version_mismatch_is_rejected() {
        let (manager, backend, root) = manager_with_data().await;
        let (a, b) = tokio::io::duplex(4096);
        manager.attach_inprocess("agent-1".to_string(), a).await;
        let mut transport = FrameTransport::spawn(b, 8);
        transport
            .send(Message::AgentHello {
                version: PROTOCOL_VERSION - 1,
                agent_boot_id: "boot-1".into(),
                build: "test".into(),
                capabilities: capability::REQUIRED.iter().map(|s| s.to_string()).collect(),
                slots: 1,
            })
            .await
            .unwrap();
        let reason = match tokio::time::timeout(Duration::from_secs(5), transport.recv()).await {
            Ok(Ok(Message::Reject { reason })) => reason,
            other => panic!("版本不匹配必须显式 Reject 并断链: {other:?}"),
        };
        assert!(
            reason.contains(&(PROTOCOL_VERSION - 1).to_string())
                && reason.contains(&PROTOCOL_VERSION.to_string()),
            "拒绝原因必须含双方版本号：{reason}"
        );
        assert!(
            manager.agents.lock().await.is_empty(),
            "握手失败不得注册会话"
        );
        manager.shutdown().await;
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
