//! 远程 agent 管理器（三期 §1.2/§1.3/§1.4）：mTLS 上联、路由包络校验、
//! BindExecutor 归属、Resume 裁决与有界公平转发。
//!
//! 远程 agent 管理器（三期 §1.2/§1.3/§1.4）：mTLS 上联、路由包络校验、
//! BindExecutor 归属、Resume 裁决与有界公平转发。
//!
//! 主进程保持唯一提交者：AuditAck/OperationPermit/ResultCommitted 只能由
//! 主进程权威产生后经 agent 转发；agent 中继不生成确认。所有持久事实仍
//! 走一期/二期的 journal 单行事务。
//!
//! 职责边界：本模块管的是「主进程 ↔ agent 的上联链路」——谁能连上来、
//! 一条派发归属哪个执行器、断线后如何对账、以及链路上的流量如何被
//! 有界地转发。节点语义与审计/结果编排仍在 dispatch。
//!
//! 守护的核心不变量：
//! 1. 路由权只来自已确认的绑定关系——执行器与 agent 都不能自报机器身份
//!    来获得路由权（包络三重身份逐项校验）。
//! 2. 断线不重跑——重连后只有通过 Resume 对账的派发才允许继续；boot
//!    变化、数据缺口、结果未知一律进人工核对（ReconcileRequired）。
//! 3. 链路上的流量必须有界——出站积压、入站队列、并发会话数、窗口
//!    预算都有上限，任何一端停读都不能把主进程拖死。
//!
//! 为什么需要它：算力可以横向扩展（agent），但权威与提交点只有一份
//! （C5）。这条链路因此必须把「远端算力」接进「本地权威日志」，而接法
//! 决定了崩溃恢复时能不能判定外部副作用到底有没有发生。

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
/// 把一条诊断信息包装成 journal 层错误。
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

type BoxedStream = Pin<Box<dyn AsyncReadWrite + Send>>;

/// 上下行字节流（TLS / WSS / 内存管道）的统一抽象。
///
/// 远程会话只要求「可读写 + 可移动 + 可跨线程」；把这条要求抽成 trait
/// 是为了让 TLS 流、WS 流和测试用内存流走同一条会话代码路径。
pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin {}
impl<T: AsyncRead + AsyncWrite + Unpin> AsyncReadWrite for T {}

/// 判断一个派发错误是否属于「链路死亡」（可由远程 Resume 恢复）。
///
/// 只有这一类错误允许挂起等待重连：其余错误（协议违规、数据非法、
/// 落盘失败）重连后同样会失败，挂起等待只会把失败拖成超时。
pub(crate) fn is_link_dead(error: &crate::journal::JournalError) -> bool {
    matches!(error, crate::journal::JournalError::LinkClosed(_))
}

/// WSS 接入的 agent 凭据：token → agent_id 映射表的一项。
///
/// 身份的信任来源跟承载走：mTLS 有证书 CN，WSS 只有 token——那就让
/// token 直接决定身份，而不是让客户端填一张没人验的表。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCredential {
    /// agent 标识（与 mTLS 路径的证书 CN 同名空间）。
    pub id: String,
    /// 接入令牌：持有它即取得该 agent 身份。
    pub token: String,
}

/// 远程执行模式的连接参数。
#[derive(Debug, Clone)]
pub struct RemoteOptions {
    /// mTLS 控制监听地址（host:port）。
    pub control_addr: String,
    /// 客户端证书校验用的 CA PEM 路径。
    pub ca_cert: PathBuf,
    /// 服务端证书 PEM 路径。
    pub server_cert: PathBuf,
    /// 服务端私钥 PEM 路径。
    pub server_key: PathBuf,
    /// 断链后等待 agent 重连对账的总时限（超时按派发失败处理）。
    pub attach_timeout_ms: u64,
    /// WSS 接入监听地址（T13，代理终止 TLS 场景的附加承载）；缺省关闭。
    pub wss_addr: Option<String>,
    /// WSS 凭据映射表（token → agent_id）：身份由凭证反查，客户端自报头
    /// 仅作一致性断言（见 [`authenticate_wss`]）。与 wss_addr 成对配置。
    pub wss_agents: Vec<AgentCredential>,
}

impl RemoteOptions {
    /// 对账等待时长（下限 1s）。
    ///
    /// 下限存在的原因：过短的超时会让「agent 刚重启、马上回来」也变成
    /// 派发失败，用户会看到毫无规律的间歇性失败。
    pub fn attach_timeout(&self) -> Duration {
        Duration::from_millis(self.attach_timeout_ms.max(1_000))
    }
}

/// 监听侧并发 TLS/WSS 握手上限（防连接洪泛无界 spawn task/做握手）。
/// TLS/WSS 各自使用独立握手 permit 和启动期限；已认证 agent 注册表
/// 另受同一数量上限约束，同身份重连可替换旧会话。超限关闭后由 agent 退避。
pub const MAX_AGENT_SESSIONS: usize = 256;

/// mTLS 服务端：验证 agent 客户端证书。
pub struct TlsServer {
    /// 已配置好客户端证书校验器的 acceptor。
    pub acceptor: TlsAcceptor,
}

/// 安装进程级 CryptoProvider（多 crate 启用不同 provider 时必须显式选择；
/// 幂等，重复安装忽略）。
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl TlsServer {
    /// 从 PEM 文件构造 mTLS 服务端配置。
    ///
    /// 证书/私钥/CA 全部来自文件路径——它们是部署产物，不该被编译进二进制
    /// 或塞进配置字符串。
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

    /// 从 PEM 内容构造 mTLS 服务端配置。
    ///
    /// 客户端证书校验是**强制**的：agent 的身份来自证书 CN，不接受「无证书
    /// 但自报身份」的连接——否则路由信任链的第一环就断了。
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
            // 空 CA 根库 = 任何客户端证书都验不过；早失败比运行时逐个连接
            // 被拒更容易定位。
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

/// 把任意可显示错误包装成「配置/数据非法」类 IO 错误。
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

/// 提取证书 CN（TLS 身份映射 agent_id；不信客户端自报字段）。
///
/// 只取 subject 里的第一个 CN 且要求整段 DER 恰好是一张证书（`rest` 必须
/// 为空）——不完整的解析结果不能用来映射身份。
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

/// attach 等待的裁决结果。
pub(crate) enum AttachOutcome {
    /// agent 会话活跃：给出可驱动链路。
    Link(DispatchLink),
    /// 结果已在权威日志（AlreadyCommitted）。
    Committed,
    /// 取消/废止（CancelAndDrain）。
    Cancelled(String),
    /// 执行器丢失/boot 变化/缺数据（ReconcileRequired 或 agent 未上报）。
    Lost(String),
}

/// 会话侧持有的路由（dispatch → 事件通道）。
struct SessionRouting {
    /// 该派发的路由包络（出站时用它包裹业务帧，入站时用它校验来源）。
    envelope: RouteEnvelope,
    /// 派发侧事件通道的发送端（入站帧按派发分发到这里）。
    events: mpsc::Sender<crate::execution::FromSession>,
}

/// 跨会话存活的派发绑定。
///
/// 绑定在「链路断开」期间依然存在：它占着 agent 的一枚槽位、记录着该
/// 派发归属哪个执行器，是断线后对账（Resume）与恢复驱动的锚点。
struct BoundDispatch {
    /// 归属 agent 标识。
    agent_id: String,
    /// 绑定建立时所在的会话标识。
    /// The binding itself reserves one slot, including while disconnected.
    link_session_id: String,
    /// 绑定到该派发的执行器标识。
    executor_id: String,
    /// 执行器本次启动标识（BindExecutorAck 到达后才有值）。
    executor_boot_id: Mutex<Option<String>>,
    /// 所属 run 标识（对账时定位 journal 事实）。
    run_id: String,
    /// 所属节点标识（对账时定位节点事实）。
    node_id: String,
    /// 活跃链路（等待被 dispatcher 取走驱动）。
    pending_link: Mutex<Option<DispatchLink>>,
    /// 终局裁决（Committed / Cancelled / Lost）。
    verdict: Mutex<Option<AttachOutcome>>,
    /// 唤醒等待者（链路就绪或裁决产生时）。
    notify: Notify,
}

/// 一个已认证 agent 会话的注册记录。
struct AgentRegistration {
    /// agent 标识。
    agent_id: String,
    /// 本次上联会话标识（重连会换新值，旧会话因此可被识别为过期）。
    link_session_id: String,
    /// 派发方 → agent 的命令通道。
    outbound: mpsc::Sender<ToSession>,
    /// agent 报告的受控执行槽位总数（候选选择的上限依据）。
    slots_total: AtomicU64,
    /// Drain 在 master 侧同步打标：候选选择只看本标志，不依赖 agent 回 ack
    /// 的时序——否则 drain 中（slots 已回收、used=0）的 agent 反而最优先被
    /// 选中，bind 被 agent 拒绝后整节点失败。
    draining: AtomicBool,
    /// 该会话的取消令牌：会话替换或关停时触发。
    cancel: tokio_util::sync::CancellationToken,
}

/// Agent 管理器：上联会话注册表 + 全局 A_max 准入 + Resume 裁决。
///
/// 锁序（禁止逆序获取）：agents → bindings → BoundDispatch 内部锁
/// （executor_boot_id / pending_link / verdict）。pending_binds 独立、不参与嵌套。
/// 注册表内先 clone Arc，再释放注册表锁、访问绑定内部锁或发送消息。
/// agents → bindings 仅用于原子分配槽位；不持注册表锁等待绑定或网络。
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

/// Bind acknowledgement identity must match the current agent session.
///
/// 绑定的在途状态：binder 等待确认期间，ack 通过 oneshot 回传；会话身份
/// 三元组（agent_id / link_session_id / executor_id）必须与发起时一致，
/// 否则说明确认来自另一个会话或已被替换的绑定。
struct PendingBind {
    agent_id: String,
    link_session_id: String,
    executor_id: String,
    events: mpsc::Sender<crate::execution::FromSession>,
    ack: oneshot::Sender<Result<String>>,
}

impl AgentManager {
    /// 新建 agent 管理器。
    ///
    /// 全局准入信号量按 A_max 建立：它是「同时在飞的活跃派发」上界，与
    /// 本机的 X_max（执行进程数）是两个不同的量，不可互相代用。
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

    /// 全局活跃派发准入（A_max）。
    pub(crate) fn global_slots(&self) -> &Arc<Semaphore> {
        &self.global
    }

    /// TLS 监听入口（真实网络）。单条 mTLS 连接承载整条上联（单流帧：
    /// 一帧一形态，变体索引即帧类型），不再有第二条 data 连接。
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

    /// R0 模拟中继入口：直接注入单流（无 TLS；身份由注入方保证）。
    ///
    /// 仅供进程内模拟与测试使用——它绕过 mTLS/WSS 的身份验证，因此绝不能
    /// 出现在对外可达的部署路径上。
    pub async fn attach_inprocess(
        self: &Arc<Self>,
        agent_id: String,
        stream: impl AsyncReadWrite + Send + 'static,
    ) {
        self.inject_stream(agent_id, Box::pin(stream)).await;
    }

    /// 把一条已认证的字节流交给会话任务处理。
    ///
    /// 会话在独立任务里跑：一条 agent 上联不能阻塞监听循环，否则一条慢
    /// 连接就能拖住所有 agent 的接入。
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

    /// agent 上联握手（T02）：AgentHello（版本门）→ 校验 → AgentWelcome。
    /// 与执行器握手同样的「明确拒绝，不降级」：postcard 变体索引是位置
    /// 编码，跨版本配对会静默错解成另一个消息类型，版本不匹配在握手处
    /// 显式拒绝并断链（错误信息含双方版本号）。
    ///
    /// 返回 `(agent_boot_id, slots, link_session_id)`：前者用于包络校验，
    /// 后者分别是 agent 的槽位数与本次上联会话标识。
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

    /// 单个 agent 会话（T02）：握手与业务帧共用同一条 transport
    /// （单流：无需第二条连接与 DataBind）。
    ///
    /// 会话是「一条上联」的全部生命：注册 → 收发 → 注销。断链时它不负责
    /// 恢复——恢复由 Resume 对账在重连后的新会话里完成。
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
        // 出站命令队列 256：够吸收一次派发的控制帧与 Resume 批次；有界，
        // 背压以「发送方 await 阻塞」的形式暴露，而不是丢帧。
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
            // 上限只对「新身份」生效：同身份重连是替换而不是新增，否则
            // agent 重启一次就会撞上会话数上限。
            if agents.len() >= MAX_AGENT_SESSIONS && !agents.contains_key(&agent_id) {
                return Err("authenticated agent session limit reached".into());
            }
            agents.insert(agent_id.clone(), registration.clone())
        };
        if let Some(previous) = previous {
            // 替换旧会话：取消旧会话令牌 → 旧会话退出 → 挂在旧链路上的
            // runner 收到 link-dead，转入 Resume 对账而不是直接失败。
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
        // 保留已释放路由的回执：链路释放后仍可能收到在途的重复结果帧，
        // 需要据此补发原提交确认（而不是当成错路由拒绝掉）。
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
                                        // 有界回执表上限 256：超出丢最旧的一条——迟到的重复
                                        // 回执价值最低，而无界表会随派发量无限增长。
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
                                    // 路由已释放但派发确实完成过：补发原提交确认，
                                    // 让执行器知道结果已被权威日志接纳（幂等重放）。
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
                                    // 执行器换了任期：这一份执行事实与之前不是
                                    // 同一份，必须拒绝而不是当成重传。
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

    /// 记录某个派发已绑定到的执行器启动标识。
    ///
    /// 该标识是 Resume 对账的判据：boot 不同就意味着「不是同一份执行
    /// 事实」，必须走人工核对而不是续跑。
    async fn note_bound(&self, dispatch_id: &str, executor_boot_id: String) {
        let bound = self.bindings.lock().await.get(dispatch_id).cloned();
        if let Some(bound) = bound {
            *bound.executor_boot_id.lock().await = Some(executor_boot_id);
        }
    }

    /// Resume 裁决（三期 §1.4 表）：以主日志为准，不信 agent 自报游标。
    ///
    /// 逐项裁决而不是整体裁决：agent 一次可能上报多个派发，其中有的可
    /// 续跑、有的必须取消——整体裁决会把可恢复的一起拖死。
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

    /// 写入终局裁决，保留到 dispatcher 取走并释放绑定。
    ///
    /// 只写第一次：后来的裁决不得覆盖已定的结论，否则「先 Lost 后
    /// Committed」这类竞态会让派发结果取决于谁最后说话。
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

    /// resume 清单未上报的该 agent 绑定 → Lost（执行器已死/失联）。
    ///
    /// 「没上报」本身就是证据：清单是 agent 的全量未决任务声明，缺项
    /// 意味着该派发在 agent 侧已不存在——不能默认它还在跑。
    async fn mark_unreported_lost(
        &self,
        agent_id: &str,
        link_session_id: &str,
        items: &[ResumeItem],
    ) {
        let reported: std::collections::BTreeSet<&str> =
            items.iter().map(|item| item.dispatch_id.as_str()).collect();
        let bindings = self.bindings.lock().await.clone();
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

    /// 为 resume 中可继续的项建立新链路并唤醒等待的 dispatcher。
    ///
    /// 新链路的包络用**绑定记录里的**执行器身份重建（不采信 agent 上报
    /// 的身份）：路由权只来自已确认的绑定关系。
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

    /// 绑定一个派发到某 agent 的本地执行器（BindExecutor → 确认）。
    ///
    /// 返回该派发的事件接收端；绑定失败（agent 拒绝、超时、会话被替换）
    /// 时返回错误，调用方据此放弃这次派发。
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
                    // 正在 drain 的 agent 不再接新派发。
                    if agent.draining.load(Ordering::Acquire) {
                        return None;
                    }
                    // 已用槽位 = 绑定数（含断线期间仍占用的绑定）：断线的
                    // 绑定仍占槽，否则重连风暴会瞬间超配 agent 的执行器数。
                    let used = bindings
                        .values()
                        .filter(|bound| bound.agent_id == agent.agent_id)
                        .count();
                    (used < agent.slots_total.load(Ordering::Relaxed) as usize)
                        .then_some((used, agent.clone()))
                })
                // 选当前最空闲的 agent：先少后多，把负载摊平，避免一个
                // agent 吃满而其他闲置。
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
        // 在途绑定登记：会话侧收到 BindExecutorAck 时按 dispatch_id 找回
        // 这条记录（含事件通道与 ack 回传口）。
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
                // 会话被替换（同身份重连）时本次绑定作废——旧会话的确认
                // 不再能代表当前会话的执行器。
                _ = registration.cancel.cancelled() => return Err(invalid("agent session replaced before bind")),
            }
            // 确认超时 = 4 × STARTUP_TIMEOUT_MS：agent 侧还要 spawn 执行器
            // 并完成它自己的握手，这段耗时叠加在链路往返之上。
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

    /// 取走绑定的活跃链路；无则等待重连裁决（Resume）或超时。
    ///
    /// 三种出口互斥：拿到链路（继续驱动）、拿到裁决（终局）、超时
    /// （失联）。等待期间不持任何注册表锁——否则一个慢 agent 会把整个
    /// 注册表堵死。
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

    /// 派发结束：解除绑定并通知 agent 取消残留执行（若非正常完成）。
    ///
    /// healthy=false 时才发 Cancel：正常完成说明结果已被权威日志接纳，
    /// 再发取消只会让 agent 去杀一个已经完成的任务。
    pub(crate) async fn unbind(self: &Arc<Self>, dispatch_id: &str, healthy: bool) {
        self.pending_binds.lock().await.remove(dispatch_id);
        let binding = self.bindings.lock().await.remove(dispatch_id);
        self.capacity_changed.notify_waiters();
        if let Some(binding) = binding {
            if !healthy {
                let registration = self.agents.lock().await.get(&binding.agent_id).cloned();
                if let Some(registration) = registration {
                    let _ = registration
                        .outbound
                        .send(ToSession::CancelDispatch(dispatch_id.to_string()))
                        .await;
                }
            }
        }
    }

    /// 取绑定 agent 当前会话的 outbound（会话已死时返回死通道：发送立即
    /// 失败 → runner 走 link-dead → 等待重连裁决）。
    pub(crate) async fn active_outbound(&self, dispatch_id: &str) -> mpsc::Sender<ToSession> {
        let binding = self.bindings.lock().await.get(dispatch_id).cloned();
        if let Some(binding) = binding {
            if let Some(registration) = self.agents.lock().await.get(&binding.agent_id).cloned() {
                return registration.outbound.clone();
            }
        }
        dead_to_session()
    }

    /// 发送 drain：agent 停止接新派发、取消在飞、转发最后确认后退出
    /// （三期 §1.6）。
    ///
    /// draining 标志在 master 侧先置位再发命令：候选选择只看这个标志，
    /// 不等 agent 回 ack——否则 drain 中的 agent 反而会被优先选中。
    pub async fn drain(&self, agent_id: &str, grace_ms: u64) -> Result<()> {
        let registration = {
            let agents = self.agents.lock().await;
            let registration = agents
                .get(agent_id)
                .ok_or_else(|| invalid(format!("agent {agent_id} not connected")))?
                .clone();
            registration.draining.store(true, Ordering::Release);
            registration
        };
        registration
            .outbound
            .send(ToSession::Send(Box::new(Message::Drain { grace_ms })))
            .await
            .map_err(|_| invalid("agent session closed"))?;
        Ok(())
    }

    /// 关停 agent 管理器：停止监听与全部会话任务，清空注册表与在途绑定。
    ///
    /// 等待任务集合收敛后再清表：会话任务可能正在写裁决，先清表会让
    /// 裁决丢失。
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        self.agents.lock().await.clear();
        self.pending_binds.lock().await.clear();
    }
}

/// 构造一条「已死」的命令通道：发送立即失败。
///
/// 用于表达「这个派发当前没有可用链路」——让上层走既有的 link-dead
/// 路径（挂起等重连），而不是新增一条特例分支。
fn dead_to_session() -> mpsc::Sender<ToSession> {
    let (tx, mut rx) = mpsc::channel(1);
    rx.close();
    tx
}

/// 依主日志裁决单项 resume（三期 §1.4 表）：run 终态/派发被替代 →
/// CancelAndDrain；结果已提交 → AlreadyCommitted；boot 不符或缺口且无
/// 封口结果 → ReconcileRequired；否则续跑（UploadOnly/SubmitExistingResult，
/// 权威游标由重挂后的 ack 流传达）。
///
/// 裁决顺序即保守程度递减：先排除「肯定不该再跑」的情况，再考虑续跑。
/// 判错方向的代价不对称——把该跑的判成取消会丢工作，把该取消的判成续跑
/// 会重复外部副作用。
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

/// resume 场景的包络（executor_id 以绑定记录为准由 attach_resumed 重建）。
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

/// 远程派发器：journal_driver 的第三执行端口（三期 §1.1）。
///
/// 断链时派发挂起等待 agent 重连对账（不立即失败、不重新执行）；重挂后
/// 继续驱动同一 runner（账本/屏障状态保留），结果/审计按既有序列幂等推进。
#[derive(Clone)]
pub struct RemoteDispatcher {
    /// agent 管理器（注册表、绑定、准入）。
    manager: Arc<AgentManager>,
    /// 远程连接参数（对账超时等）。
    options: RemoteOptions,
}

impl RemoteDispatcher {
    /// 构造远程派发器。
    pub fn new(manager: Arc<AgentManager>, options: RemoteOptions) -> Self {
        Self { manager, options }
    }

    /// 取得底层 agent 管理器（关停、容量查询等由调用方使用）。
    pub fn manager(&self) -> &Arc<AgentManager> {
        &self.manager
    }

    /// 执行一次远程节点派发。
    ///
    /// 与本地路径的关键差异：链路死亡**不**直接失败，而是挂起等待 agent
    /// 重连后的 Resume 裁决；只有裁决明确「已提交 / 取消 / 丢失」或
    /// 对账超时，才结束这次派发。
    pub(crate) async fn execute(
        &self,
        attempt: &crate::journal_execution::Attempt,
        node: &flow_engine::Node,
        predecessors: std::collections::BTreeMap<String, flow_journal::StoredValue>,
        observations: Option<flow_engine::observation::ObservationStore>,
    ) -> std::result::Result<(), crate::journal::JournalError> {
        use crate::execution::dispatch::DispatchRunner;
        // 全局 A_max 准入：同时在飞的活跃派发数上限（与 agent 侧的执行器
        // 槽位数、窗口预算是不同的量）。
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
                // 重挂：等 Resume 对账给出裁决（链路就绪 / 已提交 / 取消 /
                // 丢失）。超时按丢失处理——宁可显式失败，也不在状态不明
                // 时继续跑。
                self.manager
                    .wait_attach(&dispatch_id, self.options.attach_timeout())
                    .await
            };
            match outcome {
                AttachOutcome::Link(mut link) => {
                    let result = runner.drive_with_resume(&mut link, resume).await;
                    // 之后一律按 resume 驱动：重连后不得重发 Execute，否则
                    // 节点会被执行两遍。
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

/// 派发绑定的清理守卫：无论派发正常结束、报错还是任务被取消，
/// 绑定都必须被解除——否则 agent 侧那一枚槽位会永久泄漏。
struct BindingGuard {
    /// agent 管理器。
    manager: Arc<AgentManager>,
    /// 待解除的派发标识。
    dispatch_id: String,
}
impl Drop for BindingGuard {
    /// 兜底解除绑定：在独立任务里做，避免 drop 中 await。
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

/// 常量时间字节比较（长度不等时直接判否——长度本身不是秘密）。
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

    /// 会话替换后，仍在飞的绑定必须继续占用槽位——新会话不能借「重连」
    /// 抹掉旧会话的在飞计数。
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

#[cfg(test)]
mod lock_tests {
    use super::*;

    /// 等待绑定时不得持有注册表锁：否则一个慢绑定会冻结整个注册表。
    #[tokio::test]
    async fn waiting_for_binding_does_not_lock_registry() {
        let dir = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(dir.path(), Default::default())
            .await
            .unwrap();
        let manager = AgentManager::new(backend.clone(), backend.journal.id().into(), 1);
        let binding = Arc::new(BoundDispatch {
            agent_id: "agent".into(),
            link_session_id: "session".into(),
            executor_id: "exec".into(),
            executor_boot_id: Mutex::new(None),
            run_id: "run".into(),
            node_id: "node".into(),
            pending_link: Mutex::new(None),
            verdict: Mutex::new(None),
            notify: Notify::new(),
        });
        manager
            .bindings
            .lock()
            .await
            .insert("dispatch".into(), binding.clone());
        let boot = binding.executor_boot_id.lock().await;
        let note = manager.note_bound("dispatch", "boot".into());
        tokio::pin!(note);
        assert!(futures::poll!(&mut note).is_pending());
        assert!(
            manager.bindings.try_lock().is_ok(),
            "registry cannot be held while waiting on a binding"
        );
        drop(boot);
        note.await;
        assert_eq!(
            binding.executor_boot_id.lock().await.as_deref(),
            Some("boot")
        );
        backend.close().await.unwrap();
    }

    /// 网络背压不得持有 agent 注册表锁：drain 的发送会等待通道容量。
    #[tokio::test]
    async fn drain_backpressure_does_not_lock_agent_registry() {
        let dir = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(dir.path(), Default::default())
            .await
            .unwrap();
        let manager = AgentManager::new(backend.clone(), backend.journal.id().into(), 1);
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(ToSession::CancelDispatch("occupied".into()))
            .await
            .unwrap();
        let registration = Arc::new(AgentRegistration {
            agent_id: "agent".into(),
            link_session_id: "session".into(),
            outbound: tx,
            slots_total: AtomicU64::new(1),
            draining: AtomicBool::new(false),
            cancel: tokio_util::sync::CancellationToken::new(),
        });
        manager
            .agents
            .lock()
            .await
            .insert("agent".into(), registration.clone());
        let drain = manager.drain("agent", 100);
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        assert!(
            manager.agents.try_lock().is_ok(),
            "network backpressure cannot hold the registry"
        );
        assert!(registration.draining.load(Ordering::Acquire));
        rx.recv().await.unwrap();
        drain.await.unwrap();
        assert!(
            matches!(rx.recv().await, Some(ToSession::Send(message)) if matches!(*message, Message::Drain { .. }))
        );
        backend.close().await.unwrap();
    }
}

/// W1：WSS 身份由凭证映射表决定；自报 header 只做一致性断言。
#[cfg(test)]
mod auth_tests {
    use super::*;

    /// 构造一个 WSS 握手请求（token 与自报身份可分别省略）。
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

    /// 两个 agent 的凭据表（token → agent_id）。
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

    /// 身份由 token 反查决定；自报 header 只做一致性断言。
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

    /// 自报 header 不能冒充他人：凭证与声明不一致时必须拒绝。
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

    /// 未知 token 不得匹配任何身份。
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

    /// 构造一个带临时数据目录的 agent 管理器。
    async fn manager_with_data() -> (Arc<AgentManager>, Arc<JournalBackend>, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("flow-session-{}", uuid::Uuid::now_v7()));
        let backend = JournalBackend::open(&root, flow_journal::JournalOptions::default())
            .await
            .unwrap();
        let manager = AgentManager::new(backend.clone(), "journal-test".into(), 1);
        (manager, backend, root)
    }

    /// 关停必须打断沉默中的握手并释放监听端口。
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
