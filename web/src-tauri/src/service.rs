//! 内嵌式 Journal 产品面：复用同一套方法模块，但**不开 WebSocket 监听**。
//! 专用 runtime 拥有执行器、订阅、调度与停机。
//!
//! 桌面端与服务端的差别只有「入口」：服务端把方法模块挂到 WebSocket 上，
//! 桌面端在进程内直接调用它。两边共用同一个 `flow_rpc::journal_v2`
//! 模块，意味着**产品面只有一份实现**，不会出现「桌面能做的服务端做不了」。
//!
//! 守护的不变量：
//!
//! - **渲染层不持有凭据**：`_token` 由本模块注入，请求头里的 token 一律
//!   来自服务端生成的进程内令牌，渲染层自带的 token 会被覆盖。
//! - **停机先关闸再收尾**：`closing` 置位后拒绝新请求，已接受的调用通过
//!   `gate` 排空后才关闭 SQLite 句柄——顺序反了会让在途调用撞上已关闭的库。
//! - **会话与窗口绑定**：订阅挂在会话上，窗口销毁时连带中止订阅任务，
//!   否则前端会收到一个指向已消失窗口的推送流。
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use flow_backend::journal::JournalBackend;
use jsonrpsee::core::server::Methods;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{oneshot, RwLock};

/// 本 crate 统一的错误类型：直接是 `String`。
///
/// 桌面端的错误会原样跨 IPC 传给渲染层，因此保持「人可读的字符串」，
/// 不暴露内部错误类型。
pub type Result<T> = std::result::Result<T, String>;

/// 订阅推送通道的接收端。
type Notifications = tokio::sync::mpsc::Receiver<Box<serde_json::value::RawValue>>;

/// 要调用的产品面。
///
/// 目前 `Flow` 与 `Journal` 指向同一套方法模块；保留两个名字是为了让
/// 渲染层能显式表达意图（而非依赖一个默认面）。
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Service {
    /// 产品面（工作流/运行/触发器等）。
    Flow,
    /// Journal 面。
    Journal,
}

/// 已装配好的桌面服务集合。
pub struct Services {
    /// 与服务端共用的产品面方法模块（进程内调用，不经网络）。
    journal_methods: Methods,
    /// journal 后端句柄（权威库）。
    journal: Arc<JournalBackend>,
    /// 进程内令牌：渲染层永远拿不到它。
    token: String,
    /// 附件下载路由（value 下载）。
    pub downloads: axum::Router,
    /// webhook HTTP 入口地址。
    pub http_url: String,
    /// 后台任务句柄；停机时统一 abort。
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// 停机标志：置位后拒绝新请求。
    pub closing: AtomicBool,
    /// 读锁用于排空在途请求，写锁用于停机时独占。
    pub gate: RwLock<()>,
}

impl Services {
    /// 打开（或创建）桌面端的 journal 后端并装配全部服务。
    async fn open(root: PathBuf) -> Result<Arc<Self>> {
        // 统一配置：`<root>/flow.toml` 可选（root 即 flow 主目录 ~/.flow）。
        // 文件缺省时保持布局 root/journal + root/data（与服务端缺省一致），
        // 已有安装数据不受影响；给了文件则 storage.data_dir 相对 root 解析。
        let config_path = root.join("flow.toml");
        let has_config_file = config_path.exists();
        let loaded =
            flow_config::Config::load_or_default(&config_path).map_err(|e| e.to_string())?;
        let mut config = loaded.config;
        if !has_config_file {
            config.storage.data_dir = "data".into();
            // 桌面默认执行槽位（T6）：交互负载为主，8 并发节点够用且
            // 内存可控（executor 常驻 ≈11 MiB/进程）。
            config.execution.x_max = 8;
            // 桌面 app 启动即预热一个执行器：首次运行省掉进程启动 + 握手
            // 延迟（交互延迟敏感）；预热失败降级为首次派发懒加载。
            config.execution.prewarm = true;
        }
        // 密钥与历史 v1 数据仍住在 data 目录（密钥是用户资产，不随权威
        // 切换搬家；v1 数据按「历史舍弃」原则保留只读）。工作流/run 的
        // 权威是 root/flow.sqlite，主服务与 /journal 工作区同源。
        let data = root.join(&config.storage.data_dir);
        let journal = JournalBackend::open(&root.join("journal"), Default::default())
            .await
            .map_err(|e| e.to_string())?;

        let built = Self::assemble(journal.clone(), config, config_path, data).await;
        if built.is_err() {
            let _ = journal.close().await;
        }
        built
    }

    /// 装配服务：方法模块、下载路由、执行器、触发器调度与 HTTP 入口。
    ///
    /// 这一步做的是「把服务端该起的东西在桌面端也起起来」：执行器与
    /// 触发器调度与部署形态无关，只有 RPC 入口形态不同。
    async fn assemble(
        journal: Arc<JournalBackend>,
        config: flow_config::Config,
        config_path: PathBuf,
        data_dir: PathBuf,
    ) -> Result<Arc<Self>> {
        let token = uuid::Uuid::new_v4().to_string();
        // config.update 的默认落点（path 为 None 即启动时无配置文件时生效）：
        // 与服务端同一来源——FLOW_CONFIG 声明 → flow 主目录 flow.toml。
        let default_path = flow_config::Config::default_write_path();
        let config_state = flow_rpc::ConfigState {
            config: std::sync::RwLock::new(config.clone()),
            path: std::sync::RwLock::new(Some(config_path)),
            default_path,
            env_overrides: Vec::new(),
        };
        let (state, _secrets) =
            flow_rpc::AppState::for_production(journal.clone(), config_state, &data_dir).await;
        // 主服务与 /journal 工作区同一 v2 模块（产品面全量；v1 RPC 面退役）
        let journal_methods: Methods = flow_rpc::journal_v2::module_product(
            journal.clone(),
            token.clone(),
            Some(state.clone()),
        )
        .map_err(|e| e.to_string())?
        .into();
        let downloads = flow_rpc::journal_download::router(journal.clone(), token.clone())
            .map_err(|e| e.to_string())?;
        // Only incoming webhooks need HTTP. RPC and downloads stay inside the process.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let http_url = format!(
            "http://{}",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let harness_agent = flow_backend::harness_agent_run_config(&config.harness.agent);
        flow_backend::start_execution(&config.execution, harness_agent, &journal)
            .await
            .map_err(|e| e.to_string())?;
        // 主后端 = journal：触发器走 journal_triggers（命令身份去重），
        // 不再跑 v1 调度器（schedule_fires 表语义）
        let journal_scheduler = if config.server.scheduler_enabled {
            flow_rpc::journal_triggers::start(journal.clone(), config.journal_trigger_tick())
        } else {
            tokio::spawn(async {})
        };
        let journal_http = journal.clone();
        let http_token = token.clone();
        let http = tokio::spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                flow_rpc::journal_triggers::router(journal_http, http_token),
            )
            .await
            {
                eprintln!("desktop webhook server stopped: {error}");
            }
        });
        Ok(Arc::new(Self {
            journal_methods,
            journal,
            token,
            downloads,
            http_url,
            tasks: Mutex::new(vec![journal_scheduler, http]),
            closing: AtomicBool::new(false),
            gate: RwLock::new(()),
        }))
    }

    /// 在进程内执行一次 RPC 调用，返回结果与订阅推送通道。
    ///
    /// 走的是与 WebSocket 完全相同的方法模块，因此桌面端与服务端的
    /// 参数校验、错误码、幂等语义天然一致。
    pub async fn request(
        &self,
        service: Service,
        method: String,
        mut params: Value,
    ) -> Result<(Value, Notifications)> {
        let _gate = self.gate.read().await;
        if self.closing.load(Ordering::SeqCst) {
            return Err("local service is stopping".into());
        }
        let module = match service {
            Service::Flow | Service::Journal => {
                let object = params
                    .as_object_mut()
                    .ok_or("parameters must be an object")?;
                // Never accept a network token from the desktop renderer.
                object.insert("_token".into(), json!(self.token));
                // v2 协议：写命令必须有幂等键。桌面调用方（渲染层）不感知
                // 协议细节——这里为缺失 request_id 的写命令补一个（一次
                // 调用一个键 = 一次交付语义）。
                let is_write = flow_rpc::journal_v2::is_write_method(&method);
                if is_write
                    && !object
                        .get("request_id")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty())
                {
                    object.insert("request_id".into(), json!(uuid::Uuid::new_v4().to_string()));
                }
                &self.journal_methods
            }
        };
        let request =
            json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}).to_string();
        let (reply, events) = module
            .raw_json_request(&request, 64)
            .await
            .map_err(|e| e.to_string())?;
        Ok((
            serde_json::from_str(reply.get()).map_err(|e| e.to_string())?,
            events,
        ))
    }

    /// 构造一次附件下载请求（内部调用下载路由，不经网络）。
    ///
    /// 走内部构造而不是让渲染层直接拼 URL：渲染层提供的 `run`/`output`
    /// 必须先做标识符校验，否则就成了路径穿越的入口。
    pub fn download_request(
        &self,
        run: &str,
        output: &str,
    ) -> Result<axum::http::Request<axum::body::Body>> {
        // Identifiers are one path segment, never a file path or URL supplied by the renderer.
        if [run, output].iter().any(|id| {
            id.is_empty()
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        }) {
            return Err("invalid run/output identifier".into());
        }
        axum::http::Request::builder()
            .uri(format!("/runs/{run}/values/{output}"))
            .header("authorization", format!("Bearer {}", self.token))
            .body(axum::body::Body::empty())
            .map_err(|e| e.to_string())
    }

    /// 停机：先关闸、排空在途调用，再关闭 SQLite 句柄。
    ///
    /// 顺序是有意为之：先 abort 后台任务（调度器、HTTP 服务），
    /// 再取写锁把在途调用排空，最后才关库。少了排空这一步，
    /// 正在写的调用会撞上已关闭的写者。
    async fn shutdown(&self) -> Result<()> {
        self.closing.store(true, Ordering::SeqCst);
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        // Drain accepted calls before closing the SQLite writer and query handles.
        let _gate = self.gate.write().await;
        let journal = self.journal.close().await.map_err(|e| e.to_string());
        journal
    }
}

/// 一个渲染层会话：归属哪个窗口、持有哪些订阅。
struct Session {
    /// 窗口标签；会话与窗口一一绑定。
    window: String,
    /// 订阅 id → 推送任务句柄（关窗时统一 abort）。
    subscriptions: HashMap<String, tokio::task::AbortHandle>,
}

/// 全部渲染层会话。
#[derive(Default)]
pub struct Sessions(Mutex<HashMap<String, Session>>);
impl Sessions {
    /// 为某个窗口开启会话，返回会话 id。
    pub fn open(&self, window: String) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.0.lock().unwrap().insert(
            id.clone(),
            Session {
                window,
                subscriptions: HashMap::new(),
            },
        );
        id
    }

    /// 校验会话 id 是否属于该窗口。
    ///
    /// 窗口绑定是安全边界：渲染层只能操作自己窗口的会话。
    pub fn check(&self, id: &str, window: &str) -> Result<()> {
        match self.0.lock().unwrap().get(id) {
            Some(session) if session.window == window => Ok(()),
            _ => Err("RPC client closed".into()),
        }
    }

    /// 登记一个订阅；会话不存在或订阅 id 重复时报错。
    pub fn insert(
        &self,
        id: &str,
        window: &str,
        sub: String,
        task: tokio::task::AbortHandle,
    ) -> Result<()> {
        let mut sessions = self.0.lock().unwrap();
        let session = sessions
            .get_mut(id)
            .filter(|s| s.window == window)
            .ok_or("RPC client closed")?;
        if session.subscriptions.contains_key(&sub) {
            return Err("duplicate subscription".into());
        }
        session.subscriptions.insert(sub, task);
        Ok(())
    }
    /// 移除一个订阅并中止其推送任务。
    pub fn remove(&self, id: &str, sub: &str) {
        if let Some(session) = self.0.lock().unwrap().get_mut(id) {
            if let Some(task) = session.subscriptions.remove(sub) {
                task.abort();
            }
        }
    }

    /// 关闭一个会话，连带中止它的全部订阅。
    pub fn close(&self, id: &str) {
        if let Some(session) = self.0.lock().unwrap().remove(id) {
            for task in session.subscriptions.into_values() {
                task.abort();
            }
        }
    }

    /// 关闭某个窗口下的全部会话（页面刷新/窗口销毁时调用）。
    ///
    /// 必须中止订阅任务：否则推送会继续往一个已经不存在的窗口写，
    /// 表现为「前端莫名收到重复事件」。
    pub fn close_window(&self, window: &str) {
        let mut sessions = self.0.lock().unwrap();
        sessions.retain(|_, session| {
            if session.window != window {
                return true;
            }
            for task in session.subscriptions.values() {
                task.abort();
            }
            false
        });
    }

    /// 关闭全部会话（应用退出时调用）。
    fn close_all(&self) {
        for session in self.0.lock().unwrap().drain().map(|(_, s)| s) {
            for task in session.subscriptions.into_values() {
                task.abort();
            }
        }
    }
}

/// 桌面端的宿主：拥有服务集合、专用 runtime 与会话表。
pub struct Host {
    /// 已装配的服务集合。
    pub services: Arc<Services>,
    /// 专用 runtime 的句柄（渲染层的异步命令借用它执行）。
    pub runtime: tokio::runtime::Handle,
    /// 渲染层会话表。
    pub sessions: Arc<Sessions>,
    /// 停机信号（oneshot 发送端）。
    stop: Mutex<Option<oneshot::Sender<()>>>,
    /// 承载服务的线程句柄。
    thread: Mutex<Option<std::thread::JoinHandle<Result<()>>>>,
}
impl Host {
    /// 启动专用 runtime 与后台服务，返回宿主句柄。
    ///
    /// 之所以另起一个线程与 runtime：journal 的写者、调度器与执行器需要
    /// 一个**独立的** tokio runtime，不能借用 Tauri 的主 runtime——
    /// 否则桌面端窗口关闭时会把后台任务一起带走。
    pub fn start(root: PathBuf) -> Result<Arc<Self>> {
        // Match `flow-journal-server`: dependencies enable both rustls providers.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let (stop, stopped) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("flow-journal".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                runtime.block_on(async {
                    let service = match Services::open(root).await {
                        Ok(service) => service,
                        Err(error) => {
                            let _ = ready.send(Err(error.clone()));
                            return Err(error);
                        }
                    };
                    let _ = ready.send(Ok((service.clone(), runtime.handle().clone())));
                    let _ = stopped.await;
                    service.shutdown().await
                })
                // Dropping this dedicated runtime stops remaining journal tasks.
            })
            .map_err(|e| e.to_string())?;
        match started.recv().map_err(|e| e.to_string())? {
            Ok((services, runtime)) => Ok(Arc::new(Self {
                services,
                runtime,
                sessions: Arc::default(),
                stop: Mutex::new(Some(stop)),
                thread: Mutex::new(Some(thread)),
            })),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }
    /// 停机：关闭全部会话、发出停机信号并等待服务线程退出。
    ///
    /// 等待线程退出（join）是必须的：只有线程真正结束，SQLite 写者才
    /// 一定已经关闭，进程退出时才不会留下 WAL 与锁。
    pub fn shutdown(&self) -> Result<()> {
        self.services.closing.store(true, Ordering::SeqCst);
        self.sessions.close_all();
        if let Some(stop) = self.stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.lock().unwrap().take() {
            return thread
                .join()
                .map_err(|_| "Journal service thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for Host {
    /// 兜底停机：即使调用方忘了显式 shutdown，Drop 也会尝试一次。
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
