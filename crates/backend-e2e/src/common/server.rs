//! 被测 `flow-journal-server` 进程管理 + 用例上下文。
//!
//! - journal 是唯一后端：每用例独占临时目录即 journal 根（全新数据集，
//!   历史不迁移）；v2 部署 token 每用例随机生成；
//! - `Ctx::restart` 换新端口重新拉起同一份 journal 目录——崩溃恢复用例的
//!   「重启」。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use futures::FutureExt;

use crate::common::client;
use flow_test_support::io::{wait_ready_child, Ready, PORT_RETRY_ATTEMPTS};

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定临时端口失败");
    listener.local_addr().expect("读取本地地址失败").port()
}

/// 一个用例的上下文：v2 服务进程 + journal 目录 + 部署 token。
///
/// `bin` 是 `env!("CARGO_BIN_EXE_flow-journal-server-e2e")`（测试 crate 编译期
/// 常量，只能从测试代码传进来——lib 编译时没有这个环境变量）。
pub struct Ctx {
    bin: PathBuf,
    server: ServerProc,
    journal_dir: PathBuf,
    token: String,
    /// 启动时透传给被测进程的额外环境变量（重启时保持一致）。
    extra_env: Vec<(String, String)>,
}

impl Ctx {
    pub async fn start(bin: &str) -> Ctx {
        Self::start_with(bin, &[]).await
    }

    /// extra_env 透传给被测进程（如 FLOW_EXECUTION_MODE / FLOW_X_MAX）。
    pub async fn start_with(bin: &str, extra_env: &[(&str, &str)]) -> Ctx {
        // v2 部署 token 随机（≥32 字节）
        let dir = std::env::temp_dir().join(format!("flow-e2e-j-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).expect("创建 journal 临时目录失败");
        let token = uuid::Uuid::now_v7().simple().to_string();
        let bin_path = PathBuf::from(bin);
        let server = ServerProc::spawn(&bin_path, &dir, &token, extra_env);
        Ctx {
            bin: bin_path,
            server,
            journal_dir: dir,
            token,
            extra_env: extra_env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.server.addr
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.server.http_addr
    }

    /// journal 根目录（崩溃恢复用例检查落盘事实用）。
    pub fn journal_dir(&self) -> &std::path::Path {
        &self.journal_dir
    }

    /// v2 部署 token（webhook HTTP 的 Bearer 凭据）。
    pub fn token(&self) -> &str {
        &self.token
    }

    pub async fn client(&self) -> client::Conn {
        client::connect(self.addr(), &self.token).await
    }

    /// SIGKILL 当前服务进程并用同一份 journal 目录重新拉起（崩溃恢复的
    /// 「重启」）。
    pub async fn restart(&mut self) {
        self.server
            .restart(&self.bin, &self.journal_dir, &self.token, &self.extra_env)
            .await;
    }

    /// 结束用例：杀进程 + 删临时目录。正常与 panic 路径都会调用。
    pub async fn finish(self) {
        let Ctx {
            mut server,
            journal_dir,
            ..
        } = self;
        server.kill();
        drop(server);
        let _ = std::fs::remove_dir_all(&journal_dir);
    }
}

/// 同 [`run_case`]，但可向被测进程注入额外环境变量。
pub async fn run_case_with_env<F>(bin: &str, extra_env: &[(&str, &str)], body: F)
where
    F: FnOnce(&mut Ctx) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
{
    let mut ctx = Ctx::start_with(bin, extra_env).await;
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

/// 跑一个用例体并保证清理（panic 也先清理再恢复 unwind，绝不漏进程/目录）。
///
/// 用例体签名：`|ctx: &Ctx| Box::pin(async move { ... })`——box 之后的 future
/// 生命周期与 `&Ctx` 借用绑定。
pub async fn run_case<F>(bin: &str, body: F)
where
    F: FnOnce(&mut Ctx) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
{
    let mut ctx = Ctx::start(bin).await;
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

/// 一个被测 flow-journal-server 进程。Drop 保证 SIGKILL + wait（不漏僵尸）。
pub struct ServerProc {
    child: Child,
    addr: SocketAddr,
    http_addr: SocketAddr,
}

impl ServerProc {
    fn spawn(
        bin: &std::path::Path,
        journal_dir: &std::path::Path,
        token: &str,
        extra_env: &[(&str, &str)],
    ) -> Self {
        // 端口是「建议」不是「预留」：free_port 放掉到被测进程 bind 之间会被
        // 并行用例抢走（TOCTOU，CI 多核上会真发生）。被抢时子进程因 AddrInUse
        // 立刻退出，换端口重来。
        let mut last = String::new();
        for _ in 1..=PORT_RETRY_ATTEMPTS {
            match Self::spawn_once(bin, journal_dir, token, extra_env) {
                Ok(proc) => return proc,
                Err(reason) => last = reason,
            }
        }
        panic!("{last}（已重试 {PORT_RETRY_ATTEMPTS} 次）");
    }

    /// 起一次并等就绪。`Err` 表示该端口不可用，换一个重试即可。
    fn spawn_once(
        bin: &std::path::Path,
        journal_dir: &std::path::Path,
        token: &str,
        extra_env: &[(&str, &str)],
    ) -> Result<Self, String> {
        let addr = socket_addr(free_port());
        let http_addr = socket_addr(free_port());
        let mut cmd = Command::new(bin);
        cmd.env("FLOW_JOURNAL_ADDR", addr.to_string())
            .env("FLOW_JOURNAL_HTTP_ADDR", http_addr.to_string())
            .env("FLOW_JOURNAL_DATA_DIR", journal_dir)
            .env("FLOW_JOURNAL_TOKEN", token)
            // 持久化密钥存储（storage.data_dir）也落在临时目录，别污染 CWD
            .env("FLOW_DATA_DIR", journal_dir)
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
        for (key, value) in extra_env {
            cmd.env(key, value);
        }
        let mut child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("启动 flow-journal-server 失败：{e}"));
        match wait_ready_child(addr, &mut child) {
            Ready::Ready => Ok(ServerProc {
                child,
                addr,
                http_addr,
            }),
            Ready::ChildExited(status) => Err(format!(
                "flow-journal-server 端口 {addr} 上启动即退出（{status}）——端口被并行用例占用"
            )),
            Ready::TimedOut => {
                let _ = child.kill();
                let _ = child.wait();
                Err(format!("flow-journal-server {addr} 未在就绪超时内起来"))
            }
        }
    }

    async fn restart(
        &mut self,
        bin: &std::path::Path,
        journal_dir: &std::path::Path,
        token: &str,
        extra_env: &[(String, String)],
    ) {
        self.kill();
        let env: Vec<(&str, &str)> = extra_env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        *self = Self::spawn(bin, journal_dir, token, &env);
    }

    pub fn kill(&mut self) {
        // SIGKILL：崩溃恢复必须是「没机会做任何清理」的死法
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        self.kill();
    }
}

fn socket_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}
