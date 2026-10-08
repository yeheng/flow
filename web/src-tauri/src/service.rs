//! Embedded flow-server: same method modules, no WebSocket listener.
//! A dedicated runtime owns execution, subscriptions, scheduling and shutdown.
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use flow_backend::{journal::JournalBackend, AnyBackend, SqliteBackend};
use jsonrpsee::core::server::Methods;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{oneshot, RwLock};

pub type Result<T> = std::result::Result<T, String>;
type Notifications = tokio::sync::mpsc::Receiver<Box<serde_json::value::RawValue>>;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Service {
    Flow,
    Journal,
}

pub struct Services {
    flow: Methods,
    journal_methods: Methods,
    backend: AnyBackend,
    journal: Arc<JournalBackend>,
    token: String,
    pub downloads: axum::Router,
    pub http_url: String,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub closing: AtomicBool,
    pub gate: RwLock<()>,
}

impl Services {
    async fn open(root: PathBuf) -> Result<Arc<Self>> {
        // 统一配置：`<root>/flow.toml` 可选。文件缺省时保持历史桌面布局
        // （root/flow + root/journal），已有安装数据不受影响；给了文件则
        // storage.data_dir 相对 root 解析。
        let config_path = root.join("flow.toml");
        let has_config_file = config_path.exists();
        let loaded = flow_config::Config::load_or_default(&config_path)
            .map_err(|e| e.to_string())?;
        let config = loaded.config;
        let data = if has_config_file {
            root.join(&config.storage.data_dir)
        } else {
            root.join("flow")
        };
        let db_path = config
            .storage
            .database
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| data.join("flow.db"));
        let backend = AnyBackend::Sqlite(Arc::new(
            SqliteBackend::open(&data, db_path).await.map_err(|e| e.to_string())?,
        ));
        let journal = JournalBackend::open(&root.join("journal"), Default::default())
            .await
            .map_err(|e| e.to_string())?;
        let built = Self::assemble(
            backend.clone(),
            journal.clone(),
            config,
            config_path,
            data,
        )
        .await;
        if built.is_err() {
            let _ = journal.close().await;
            let _ = backend.shutdown().await;
        }
        built
    }

    async fn assemble(
        backend: AnyBackend,
        journal: Arc<JournalBackend>,
        config: flow_config::Config,
        config_path: PathBuf,
        data_dir: PathBuf,
    ) -> Result<Arc<Self>> {
        let token = uuid::Uuid::new_v4().to_string();
        let config_state = flow_rpc::ConfigState {
            config: std::sync::RwLock::new(config.clone()),
            path: Some(config_path),
            env_overrides: Vec::new(),
        };
        let (state, _secrets) =
            flow_rpc::AppState::for_production(backend.clone(), config_state, &data_dir).await;
        let flow = flow_rpc::build_module(state.clone())
            .map_err(|e| e.to_string())?
            .into();
        let journal_methods = flow_rpc::journal_v2::module(journal.clone(), token.clone())
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
        backend.start().await.map_err(|e| e.to_string())?;
        journal.start_execution().await.map_err(|e| e.to_string())?;
        let scheduler = if config.server.scheduler_enabled {
            let scheduler_backend = backend.clone();
            let tick = config.scheduler_tick();
            tokio::spawn(async move {
                flow_rpc::scheduler::run(scheduler_backend, tick).await;
            })
        } else {
            // 保持 tasks 通道形状：立即结束的占位任务
            tokio::spawn(async {})
        };
        let journal_scheduler =
            flow_rpc::journal_triggers::start(journal.clone(), config.journal_trigger_tick());
        let http = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, flow_rpc::webhook::router(state)).await {
                eprintln!("desktop webhook server stopped: {error}");
            }
        });
        Ok(Arc::new(Self {
            flow,
            journal_methods,
            backend,
            journal,
            token,
            downloads,
            http_url,
            tasks: Mutex::new(vec![scheduler, journal_scheduler, http]),
            closing: AtomicBool::new(false),
            gate: RwLock::new(()),
        }))
    }

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
            Service::Flow => &self.flow,
            Service::Journal => {
                let object = params
                    .as_object_mut()
                    .ok_or("journal parameters must be an object")?;
                // Never accept a network token from the desktop renderer.
                object.insert("_token".into(), json!(self.token));
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

    async fn shutdown(&self) -> Result<()> {
        self.closing.store(true, Ordering::SeqCst);
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        // Drain accepted calls before closing the journal/projector.
        let _gate = self.gate.write().await;
        let journal = self.journal.close().await.map_err(|e| e.to_string());
        let backend = self.backend.shutdown().await.map_err(|e| e.to_string());
        journal.and(backend)
    }
}

struct Session {
    window: String,
    subscriptions: HashMap<String, tokio::task::AbortHandle>,
}
#[derive(Default)]
pub struct Sessions(Mutex<HashMap<String, Session>>);
impl Sessions {
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
    pub fn check(&self, id: &str, window: &str) -> Result<()> {
        match self.0.lock().unwrap().get(id) {
            Some(session) if session.window == window => Ok(()),
            _ => Err("RPC client closed".into()),
        }
    }
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
    pub fn remove(&self, id: &str, sub: &str) {
        if let Some(session) = self.0.lock().unwrap().get_mut(id) {
            if let Some(task) = session.subscriptions.remove(sub) {
                task.abort();
            }
        }
    }
    pub fn close(&self, id: &str) {
        if let Some(session) = self.0.lock().unwrap().remove(id) {
            for task in session.subscriptions.into_values() {
                task.abort();
            }
        }
    }
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
    fn close_all(&self) {
        for session in self.0.lock().unwrap().drain().map(|(_, s)| s) {
            for task in session.subscriptions.into_values() {
                task.abort();
            }
        }
    }
}

pub struct Host {
    pub services: Arc<Services>,
    pub runtime: tokio::runtime::Handle,
    pub sessions: Arc<Sessions>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<std::thread::JoinHandle<Result<()>>>>,
}
impl Host {
    pub fn start(root: PathBuf) -> Result<Arc<Self>> {
        // Match `flow-server`: dependencies enable both rustls providers.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let (stop, stopped) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("flow-server".into())
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
                // Dropping this dedicated runtime stops the legacy engine's remaining tasks.
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
    pub fn shutdown(&self) -> Result<()> {
        self.services.closing.store(true, Ordering::SeqCst);
        self.sessions.close_all();
        if let Some(stop) = self.stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.lock().unwrap().take() {
            return thread
                .join()
                .map_err(|_| "flow-server thread panicked".to_string())?;
        }
        Ok(())
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
