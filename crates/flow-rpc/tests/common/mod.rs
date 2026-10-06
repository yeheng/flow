//! flow-rpc 集成测试基建：真起 `flow-server` 进程（SQLite / Postgres 两种存储）、
//! JSON-RPC 客户端助手、定义构造器。
//!
//! 进程脚手架统一在这里：`ServerProc` / `free_port` / `wait_ready` / `connect` /
//! `named` / `call` / `call_err` / `line_def` 这些跨用例共用的东西只此一份
//! （DESIGN §13：共享夹具不逐文件复制——复制品会各自漂移）。
//! PG 测试库交给 `flow-test-support::pg`，flow-pg 的测试与 backend-e2e 都指着
//! 那一份。
//!
//! 就绪判定用 TCP connect 轮询（DESIGN.md §13 的约定）；`connect` 带重试，
//! 因为进程刚 bind 上端口时 RPC 路由可能还没装完。
//!
//! common 被多个测试二进制共享（ws_rpc 只要 SQLite 与进程脚手架，ws_pg 还要
//! PG 那套），各二进制只用到其中一部分辅助函数。
#![allow(dead_code)]

pub use flow_test_support::io::{flow_bin, spawn_reporting_ports};

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde_json::{json, Value};

/// Postgres 测试库名前缀（残留下一次启动时只扫这个前缀的库，全局共用一份）。
pub const DB_PREFIX: &str = "flow_test_";

/// 一个被测 flow-server 进程。Drop 保证 SIGKILL + wait（不漏僵尸）。
pub struct ServerProc {
    child: Child,
    addr: SocketAddr,
    http_addr: SocketAddr,
}

impl ServerProc {
    /// SQLite 后端：独占临时目录里的 `flow.db`（DESIGN.md §13：只清理独占目录）。
    ///
    /// 目录的清理归调用方（[`Workspace`] 持有一个 [`TempDir`]）：重启要复用同一份
    /// 目录，所有权留在 proc 身上会把目录在重启前删掉。
    pub fn spawn_sqlite(data_dir: std::path::PathBuf) -> ServerProc {
        let db = data_dir.join("flow.db");
        let mut cmd = Command::new(flow_bin());
        cmd.arg("server");
        cmd.env("FLOW_DATA_DIR", &data_dir)
            .env("FLOW_DB", &db)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        // 端口由被测进程自己向内核要（FLOW_ADDR/FLOW_HTTP_ADDR=127.0.0.1:0），
        // 实际端口从它的启动日志读回：父进程「挑端口再让子进程绑」存在 TOCTOU，
        // 并行用例会抢走端口（CI 多核上会真发生），子进程随即 AddrInUse 退出。
        let (child, ports) = spawn_reporting_ports(&mut cmd, "FLOW_ADDR", "FLOW_HTTP_ADDR", None)
            .unwrap_or_else(|e| panic!("{e}"));
        ServerProc {
            child,
            addr: ports.rpc,
            http_addr: ports.http,
        }
    }

    /// Postgres 后端：`role` 为 all / gateway / executor。时间参数全部压到最快，
    /// 行为测试等不起生产默认值。
    pub fn spawn_pg(url: &str, role: &str) -> ServerProc {
        Self::spawn_pg_with(url, role, 5_000, &[])
    }

    /// 同 [`ServerProc::spawn_pg`]，`extra_env` 覆盖默认环境变量（如拉长
    /// FLOW_SUBSCRIBE_POLL_MS 证明 NOTIFY 唤醒）。
    pub fn spawn_pg_with(
        url: &str,
        role: &str,
        signal_wait_ms: u64,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let mut cmd = Command::new(flow_bin());
        cmd.arg("server");
        cmd.env("FLOW_BACKEND", "postgres")
            .env("FLOW_DATABASE_URL", url)
            .env("FLOW_ROLE", role)
            .env("FLOW_LEASE_TTL_MS", "1500")
            .env("FLOW_SCAN_INTERVAL_MS", "50")
            .env("FLOW_INBOX_POLL_MS", "50")
            .env("FLOW_SUBSCRIBE_POLL_MS", "50")
            .env("FLOW_SIGNAL_WAIT_MS", signal_wait_ms.to_string())
            .env("RUST_LOG", "info,flow_pg=debug")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        for (key, value) in extra_env {
            cmd.env(key, value);
        }
        let (child, ports) = spawn_reporting_ports(&mut cmd, "FLOW_ADDR", "FLOW_HTTP_ADDR", None)
            .unwrap_or_else(|e| panic!("{e}"));
        ServerProc {
            child,
            addr: ports.rpc,
            http_addr: ports.http,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn client(&self) -> WsClient {
        connect(self.addr).await
    }

    /// SIGKILL：崩溃恢复必须是「没机会做任何清理」的死法。
    pub fn kill(&mut self) {
        self.child.kill().expect("kill 失败");
        self.child.wait().expect("wait 失败");
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 连上 WebSocket 服务端，失败重试到超时（进程刚 bind 上端口时路由可能还没装完）。
pub async fn connect(addr: SocketAddr) -> WsClient {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match WsClientBuilder::default()
            .request_timeout(Duration::from_secs(60))
            .build(format!("ws://{addr}"))
            .await
        {
            Ok(client) => return client,
            Err(err) => {
                if Instant::now() > deadline {
                    panic!("连接 {addr} 失败：{err}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// 按设计约定：所有方法使用具名参数（JSON-RPC 的 params 为对象）。
pub fn named(value: Value) -> ObjectParams {
    let mut params = ObjectParams::new();
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                params.insert(&key, item).unwrap();
            }
        }
        Value::Null => {}
        other => panic!("具名参数必须是对象：{other}"),
    }
    params
}

pub async fn call<T: serde::de::DeserializeOwned>(
    client: &WsClient,
    method: &str,
    params: Value,
) -> T {
    client
        .request(method, named(params))
        .await
        .unwrap_or_else(|e| panic!("调用 {method} 失败：{e}"))
}

pub async fn call_err(client: &WsClient, method: &str, params: Value) -> String {
    match client.request::<Value, _>(method, named(params)).await {
        Ok(value) => panic!("{method} 本应失败，实际返回 {value}"),
        Err(err) => err.to_string(),
    }
}

/// start → script → end。
pub fn line_def(code: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "n1", "type": "script", "name": "脚本", "params": {"code": code}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    })
}

/// 建 workflow → 写定义 → 发布，返回 (workflow_id, version)。
pub async fn publish(client: &WsClient, name: &str, def: Value) -> (String, i64) {
    let created: Value = call(client, "workflow.create", json!({"name": name})).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
    let updated: Value = call(
        client,
        "workflow.update",
        json!({"workflow_id": workflow_id, "definition": def}),
    )
    .await;
    let version = updated["version"].as_i64().unwrap();
    call::<Value>(
        client,
        "workflow.publish",
        json!({"workflow_id": workflow_id, "version": version}),
    )
    .await;
    (workflow_id, version)
}

pub async fn start_run(client: &WsClient, workflow_id: &str, input: Value) -> String {
    let started: Value = call(
        client,
        "run.start",
        json!({"workflow_id": workflow_id, "input": input}),
    )
    .await;
    started["run_id"].as_str().unwrap().to_string()
}

/// 轮询到 run 状态等于 `expected`，返回 run.get 的完整响应。
pub async fn wait_status(
    client: &WsClient,
    run_id: &str,
    expected: &str,
    timeout: Duration,
) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let run: Value = call(client, "run.get", json!({"run_id": run_id})).await;
        if run["run"]["status"] == json!(expected) {
            return run;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待 run {run_id} 到 {expected} 超时，当前 {}",
            run["run"]["status"]
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 轮询到事件流里出现满足谓词的事件为止。
pub async fn wait_event<F>(client: &WsClient, run_id: &str, predicate: F, timeout: Duration)
where
    F: Fn(&Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let events: Value = call(client, "run.events", json!({"run_id": run_id})).await;
        if events["events"]
            .as_array()
            .map(|list| list.iter().any(&predicate))
            .unwrap_or(false)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待事件超时：{events}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// PG 测试库（连接串 + 测试自己断言用的连接池）。
pub type PgTestDb = flow_test_support::pg::TestDb;

const DEFAULT_URL: &str = "postgres://flow:flow@127.0.0.1:54329/flow";

/// 连到 PG 测试库；未配置且默认端口不可达时返回 None（测试自动跳过）。
pub async fn test_db() -> Option<PgTestDb> {
    let base = std::env::var("FLOW_TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    if std::env::var("FLOW_TEST_DATABASE_URL").is_err() && !port_open().await {
        eprintln!("skip: 未设置 FLOW_TEST_DATABASE_URL 且 {DEFAULT_URL} 不可达");
        return None;
    }
    let db = PgTestDb::create(&base, DB_PREFIX).await;
    flow_pg::schema::init(db.pool())
        .await
        .expect("初始化 schema 失败");
    Some(db)
}

async fn port_open() -> bool {
    tokio::net::TcpStream::connect("127.0.0.1:54329")
        .await
        .is_ok()
}

/// 一个 SQLite 存储上的被测服务端 + 已连上的客户端。
///
/// `restart` 会 SIGKILL 后用同一份 `data_dir` / `db` 重新拉起——崩溃恢复用例
/// 的「重启」。Drop 时 ServerProc 连带删掉独占临时目录。
pub struct Workspace {
    pub proc: ServerProc,
    pub client: WsClient,
    /// 独占临时目录。ServerProc 只管进程，目录的所有权在这里、Drop 时整体删除。
    _dir: flow_test_support::io::TempDir,
}

impl Workspace {
    pub async fn start() -> Workspace {
        let dir = flow_test_support::io::TempDir::new("flow-rpc-test");
        let proc = ServerProc::spawn_sqlite(dir.path().to_path_buf());
        let client = proc.client().await;
        Workspace {
            proc,
            client,
            _dir: dir,
        }
    }

    /// 重启服务端进程，复用同一份 data_dir / db（模拟宕机后重启）。
    pub async fn restart(&mut self) {
        self.proc.kill();
        let dir = self._dir.path().to_path_buf();
        self.proc = ServerProc::spawn_sqlite(dir);
        self.client = self.proc.client().await;
    }

    pub async fn publish_workflow(&self, name: &str, definition: Value) -> (String, i64) {
        publish(&self.client, name, definition).await
    }

    pub async fn start_run(&self, workflow_id: &str, input: Value) -> String {
        start_run(&self.client, workflow_id, input).await
    }

    pub async fn wait_run_status(&self, run_id: &str, expected: &str) -> Value {
        wait_status(&self.client, run_id, expected, Duration::from_secs(30)).await
    }

    pub async fn wait_event<F>(&self, run_id: &str, predicate: F)
    where
        F: Fn(&Value) -> bool,
    {
        wait_event(&self.client, run_id, predicate, Duration::from_secs(20)).await
    }
}
