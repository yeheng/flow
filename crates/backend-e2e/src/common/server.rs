//! 被测 `flow-server` 进程管理 + 双后端用例上下文。
//!
//! - `Kind::Sqlite`：独占临时目录 `flow.db`（DESIGN.md §13：只清理独占目录）；
//! - `Kind::Postgres`：共享容器上的独占测试库；
//! - `Ctx::restart` 换新端口重新拉起同一份存储——崩溃恢复用例的「重启」。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures::FutureExt;
use uuid::Uuid;

use crate::common::client;
use crate::common::{free_port, shared, TestDb, E2E_DB_PREFIX};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Sqlite,
    Postgres,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Sqlite => "sqlite",
            Kind::Postgres => "postgres",
        }
    }
}

/// 一个用例的后端上下文：服务进程 + 存储（临时目录或测试库）。
///
/// `bin` 是 `env!("CARGO_BIN_EXE_flow-server")`（测试 crate 编译期常量，
/// 只能从测试代码传进来——lib 编译时没有这个环境变量）。
pub struct Ctx {
    pub kind: Kind,
    bin: std::path::PathBuf,
    server: ServerProc,
    db: Option<TestDb>,
    sqlite_dir: Option<PathBuf>,
    /// 启动时透传给被测进程的额外环境变量（重启时保持一致）。
    extra_env: Vec<(String, String)>,
}

impl Ctx {
    pub async fn start(kind: Kind, bin: &str) -> Ctx {
        Self::start_with(kind, bin, &[]).await
    }

    /// extra_env 透传给被测进程（如 FLOW_MAX_RUNS：嵌套子 run 深度链需要
    /// 并发驱动数大于链长，否则 executor 容量许可会相互等待成环）。
    pub async fn start_with(kind: Kind, bin: &str, extra_env: &[(&str, &str)]) -> Ctx {
        match kind {
            Kind::Sqlite => {
                let dir = std::env::temp_dir().join(format!("flow-e2e-{}", Uuid::now_v7()));
                std::fs::create_dir_all(&dir).expect("创建 SQLite 临时目录失败");
                let server = ServerProc::spawn(
                    bin,
                    &Storage::Sqlite {
                        data_dir: dir.clone(),
                        db: dir.join("flow.db"),
                    },
                    "all",
                    5_000,
                    extra_env,
                );
                Ctx {
                    kind,
                    bin: bin.into(),
                    server,
                    db: None,
                    sqlite_dir: Some(dir),
                    extra_env: extra_env
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                }
            }
            Kind::Postgres => {
                let pg = shared().await;
                // schema 在这里初始化：TestDb 只负责「开出一个空库」
                let db = TestDb::create(&pg.url(), E2E_DB_PREFIX).await;
                flow_pg::schema::init(db.pool())
                    .await
                    .expect("初始化测试库 schema 失败");
                db.pool().close().await;
                let server = ServerProc::spawn(
                    bin,
                    &Storage::Postgres {
                        url: db.url.clone(),
                    },
                    "all",
                    5_000,
                    extra_env,
                );
                Ctx {
                    kind,
                    bin: bin.into(),
                    server,
                    db: Some(db),
                    sqlite_dir: None,
                    extra_env: extra_env
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                }
            }
        }
    }

    pub fn is_pg(&self) -> bool {
        matches!(self.kind, Kind::Postgres)
    }

    pub fn is_sqlite(&self) -> bool {
        matches!(self.kind, Kind::Sqlite)
    }

    pub fn addr(&self) -> SocketAddr {
        self.server.addr
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.server.http_addr
    }

    /// Postgres 用例的测试库连接串（SQLite 用例为 None；后端专属测试用）。
    pub fn pg_url(&self) -> Option<&str> {
        self.db.as_ref().map(|db| db.url.as_str())
    }

    pub async fn client(&self) -> jsonrpsee::ws_client::WsClient {
        client::connect(self.addr()).await
    }

    /// SIGKILL 当前服务进程并用同一份存储重新拉起（崩溃恢复的「重启」）。
    pub async fn restart(&mut self) {
        let storage = self.storage();
        self.server
            .restart(&self.bin.to_string_lossy(), &storage, &self.extra_env)
            .await;
    }

    fn storage(&self) -> Storage {
        match (&self.db, &self.sqlite_dir) {
            (Some(db), _) => Storage::Postgres {
                url: db.url.clone(),
            },
            (None, Some(dir)) => Storage::Sqlite {
                data_dir: dir.clone(),
                db: dir.join("flow.db"),
            },
            (None, None) => unreachable!("Ctx 必有存储"),
        }
    }

    /// 结束用例：杀进程 + DROP 测试库 / 删临时目录。正常与 panic 路径都会调用。
    pub async fn finish(self) {
        let Ctx {
            server,
            db,
            sqlite_dir,
            ..
        } = self;
        let mut server = server;
        server.kill();
        drop(server);
        if let Some(db) = db {
            db.cleanup().await;
        }
        if let Some(dir) = sqlite_dir {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// 同 [`run_case`]，但可向被测进程注入额外环境变量。
pub async fn run_case_with_env<F>(kind: Kind, bin: &str, extra_env: &[(&str, &str)], body: F)
where
    F: FnOnce(&mut Ctx) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
{
    let mut ctx = Ctx::start_with(kind, bin, extra_env).await;
    let outcome = std::panic::AssertUnwindSafe(body(&mut ctx))
        .catch_unwind()
        .await;
    let cleanup = std::panic::AssertUnwindSafe(ctx.finish())
        .catch_unwind()
        .await;
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
    if let Err(payload) = cleanup {
        std::panic::resume_unwind(payload);
    }
}

/// 跑一个用例体并保证清理（panic 也先清理再恢复 unwind，绝不漏进程/库/目录）。
///
/// 用例体签名：`|ctx: &Ctx| Box::pin(async move { ... })`——box 之后的 future
/// 生命周期与 `&Ctx` 借用绑定，两个后端变体（macro 展开）各自持一份闭包。
pub async fn run_case<F>(kind: Kind, bin: &str, body: F)
where
    F: FnOnce(&mut Ctx) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
{
    let mut ctx = Ctx::start(kind, bin).await;
    let outcome = std::panic::AssertUnwindSafe(body(&mut ctx))
        .catch_unwind()
        .await;
    let cleanup = std::panic::AssertUnwindSafe(ctx.finish())
        .catch_unwind()
        .await;
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
    if let Err(payload) = cleanup {
        std::panic::resume_unwind(payload);
    }
}

#[derive(Clone)]
enum Storage {
    Sqlite { data_dir: PathBuf, db: PathBuf },
    Postgres { url: String },
}

/// 一个被测 flow-server 进程。Drop 保证 SIGKILL + wait（不漏僵尸）。
pub struct ServerProc {
    child: Child,
    addr: SocketAddr,
    http_addr: SocketAddr,
}

impl ServerProc {
    fn spawn(
        bin: &str,
        storage: &Storage,
        role: &str,
        signal_wait_ms: u64,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let addr = socket_addr(free_port());
        let http_addr = socket_addr(free_port());
        let mut cmd = Command::new(bin);
        cmd.env("FLOW_ADDR", addr.to_string())
            .env("FLOW_HTTP_ADDR", http_addr.to_string())
            .env("FLOW_ROLE", role)
            // 时间参数全部压到最快：e2e 要的是行为证明，不是生产默认值
            .env("FLOW_LEASE_TTL_MS", "1500")
            .env("FLOW_SCAN_INTERVAL_MS", "50")
            .env("FLOW_INBOX_POLL_MS", "50")
            .env("FLOW_SIGNAL_POLL_MS", "50")
            .env("FLOW_SUBSCRIBE_POLL_MS", "50")
            .env("FLOW_SIGNAL_WAIT_MS", signal_wait_ms.to_string())
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdin(Stdio::null());
        // FLOW_E2E_SERVER_LOG=<文件>：被测进程的 stdout/stderr（tracing 默认写 stdout）
        // 重定向过去排障；默认 stdout 丢弃、stderr 继承
        if let Ok(path) = std::env::var("FLOW_E2E_SERVER_LOG") {
            let open = || {
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .ok()
            };
            match (open(), open()) {
                (Some(out), Some(err)) => {
                    cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
                }
                _ => {
                    cmd.stdout(Stdio::null()).stderr(Stdio::inherit());
                }
            }
        } else {
            cmd.stdout(Stdio::null()).stderr(Stdio::inherit());
        }
        match storage {
            Storage::Sqlite { data_dir, db } => {
                cmd.env("FLOW_DATA_DIR", data_dir).env("FLOW_DB", db);
            }
            Storage::Postgres { url } => {
                cmd.env("FLOW_BACKEND", "postgres")
                    .env("FLOW_DATABASE_URL", url);
            }
        }
        for (key, value) in extra_env {
            cmd.env(key, value);
        }
        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("启动 flow-server 失败：{e}"));
        let proc = ServerProc {
            child,
            addr,
            http_addr,
        };
        proc.wait_ready();
        proc
    }

    pub fn spawn_postgres(bin: &str, url: &str, role: &str, signal_wait_ms: u64) -> Self {
        Self::spawn(
            bin,
            &Storage::Postgres { url: url.into() },
            role,
            signal_wait_ms,
            &[],
        )
    }

    async fn restart(&mut self, bin: &str, storage: &Storage, extra_env: &[(String, String)]) {
        self.kill();
        let env: Vec<(&str, &str)> = extra_env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        *self = Self::spawn(bin, storage, "all", 5_000, &env);
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn kill(&mut self) {
        // SIGKILL：崩溃恢复必须是「没机会做任何清理」的死法
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect_timeout(&self.addr, Duration::from_millis(200)).is_ok()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("flow-server {} 在 30s 内未就绪", self.addr);
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        self.kill();
    }
}

/// gateway / executor 拆分场景的辅助入口（持久 inbox pending → delivered）。
pub async fn spawn_pg_server(bin: &str, url: &str, role: &str, signal_wait_ms: u64) -> ServerProc {
    ServerProc::spawn_postgres(bin, url, role, signal_wait_ms)
}

fn socket_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}
