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
use flow_test_support::io::spawn_reporting_ports;

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
    /// 由已启动的子进程与报出的端口组装（自定义环境形态的 spawn 用，
    /// 如 wire 契约测试与远程执行用例自带 FLOW_* 环境变量起进程）。
    pub fn from_parts(child: Child, addr: SocketAddr, http_addr: SocketAddr) -> Self {
        ServerProc {
            child,
            addr,
            http_addr,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().expect("check server process")
    }

    /// wire 客户端（固定 TOKEN 的 v2 协议助手，见 [`crate::common::wire`]）。
    pub async fn client(&self) -> jsonrpsee::ws_client::WsClient {
        crate::common::wire::connect(self.addr).await
    }

    fn spawn(
        bin: &std::path::Path,
        journal_dir: &std::path::Path,
        token: &str,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let mut cmd = Command::new(bin);
        cmd.env("FLOW_JOURNAL_DATA_DIR", journal_dir)
            .env("FLOW_JOURNAL_TOKEN", token)
            // 持久化密钥存储（storage.data_dir）也落在临时目录，别污染 CWD
            .env("FLOW_DATA_DIR", journal_dir)
            .env(
                "RUST_LOG",
                format!(
                    "{},flow_rpc=info",
                    std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
                ),
            )
            .stdin(Stdio::null());
        let log_path = std::env::var_os("FLOW_E2E_SERVER_LOG").map(PathBuf::from);
        if let Some(path) = &log_path {
            cmd.stderr(Stdio::from(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .expect("open server log"),
            ));
        } else {
            cmd.stderr(Stdio::inherit());
        }
        // T6：默认执行模式已切 ipc。被测 server 的二进制位于 deps/ 目录，
        // locate() 找不到同目录兄弟 flow-executor——注入工作区构建产物路径
        // （workspace_bin 逻辑：优先同 profile 目录，回退 target/debug）。
        // 显式 extra_env 的 FLOW_EXECUTOR_BIN 优先，不覆盖。
        if !extra_env.iter().any(|(key, _)| *key == "FLOW_EXECUTOR_BIN")
            && std::env::var_os("FLOW_EXECUTOR_BIN").is_none()
        {
            cmd.env("FLOW_EXECUTOR_BIN", flow_test_support::io::executor_bin());
        }
        for (key, value) in extra_env {
            cmd.env(key, value);
        }
        let (child, ports) = spawn_reporting_ports(
            &mut cmd,
            "FLOW_JOURNAL_ADDR",
            "FLOW_JOURNAL_HTTP_ADDR",
            log_path,
        )
        .unwrap_or_else(|e| panic!("启动 flow-journal-server 失败：{e}"));
        Self {
            child,
            addr: ports.rpc,
            http_addr: ports.http,
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
