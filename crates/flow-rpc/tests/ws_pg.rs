//! Postgres 模式的 RPC 集成测试：真起 flow-server 进程（gateway/executor 角色
//! 拆分、SIGKILL 崩溃恢复、订阅轮询）。
//!
//! 需要 FLOW_TEST_DATABASE_URL（默认 127.0.0.1:54329 的本地测试库）；
//! 不可达时跳过。

use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use jsonrpsee::core::client::{ClientT, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://flow:flow@127.0.0.1:54329/flow";

fn base_url() -> String {
    std::env::var("FLOW_TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into())
}

struct TestDb {
    url: String,
    name: String,
    pool: PgPool,
}

async fn test_db() -> Option<TestDb> {
    let base = base_url();
    if std::env::var("FLOW_TEST_DATABASE_URL").is_err() {
        let host = base
            .split("//")
            .nth(1)
            .and_then(|s| s.split('/').next())
            .and_then(|s| {
                s.split('@')
                    .nth(1)
                    .map(|s| s.to_string())
                    .or(Some(s.to_string()))
            })
            .unwrap_or_default();
        let host_only = host.split(':').next().unwrap_or("");
        let port = host
            .split(':')
            .nth(1)
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(5432);
        if tokio::net::TcpStream::connect((host_only, port))
            .await
            .is_err()
        {
            eprintln!("skip: {host} 不可达且未设置 FLOW_TEST_DATABASE_URL");
            return None;
        }
    }
    let server_url = format!("{}/postgres", &base[..base.rfind('/').unwrap() + 1]);
    let admin = PgPool::connect(&server_url)
        .await
        .expect("连接 Postgres 失败");
    let name = format!(
        "flow_test_{}{}",
        chrono::Utc::now().format("%Y%m%d%H%M%S"),
        Uuid::now_v7().simple()
    );
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("创建测试数据库失败");
    admin.close().await;
    let url = format!("{}/{}", &base[..base.rfind('/').unwrap() + 1], name);
    let pool = PgPool::connect(&url).await.expect("连接测试数据库失败");
    flow_pg::schema::init(&pool)
        .await
        .expect("初始化 schema 失败");
    Some(TestDb { url, name, pool })
}

impl TestDb {
    async fn close(self) {
        self.pool.close().await;
        let server_url = format!("{}/postgres", &self.url[..self.url.rfind('/').unwrap() + 1]);
        if let Ok(admin) = PgPool::connect(&server_url).await {
            let _ = sqlx::query(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                self.name
            ))
            .execute(&admin)
            .await;
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", self.name))
                .execute(&admin)
                .await;
            admin.close().await;
        }
    }
}

struct ServerProc {
    child: Child,
    addr: SocketAddr,
}

impl ServerProc {
    /// role: all | gateway | executor
    fn spawn(db_url: &str, role: &str, signal_wait_ms: u64) -> ServerProc {
        let addr = free_port();
        let child = Command::new(env!("CARGO_BIN_EXE_flow-server"))
            .env("FLOW_BACKEND", "postgres")
            .env("FLOW_DATABASE_URL", db_url)
            .env("FLOW_ROLE", role)
            .env("FLOW_ADDR", addr.to_string())
            .env("FLOW_LEASE_TTL_MS", "1500")
            .env("FLOW_SCAN_INTERVAL_MS", "50")
            .env("FLOW_INBOX_POLL_MS", "50")
            .env("FLOW_SUBSCRIBE_POLL_MS", "50")
            .env("FLOW_SIGNAL_WAIT_MS", signal_wait_ms.to_string())
            .env("RUST_LOG", "info,flow_pg=debug")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("启动 flow-server (postgres) 失败");
        let proc = ServerProc { child, addr };
        wait_ready(addr);
        proc
    }

    fn kill(&mut self) {
        self.child.kill().expect("kill 失败");
        self.child.wait().expect("wait 失败");
    }

    async fn client(&self) -> WsClient {
        connect(self.addr).await
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn wait_ready(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "flow-server 未在 30s 内就绪"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

async fn connect(addr: SocketAddr) -> WsClient {
    WsClientBuilder::default()
        .connection_timeout(Duration::from_secs(10))
        .request_timeout(Duration::from_secs(20))
        .build(format!("ws://{addr}"))
        .await
        .expect("连接 WebSocket 失败")
}

fn named(value: Value) -> ObjectParams {
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

async fn call<T: serde::de::DeserializeOwned>(client: &WsClient, method: &str, params: Value) -> T {
    client
        .request(method, named(params))
        .await
        .unwrap_or_else(|e| panic!("调用 {method} 失败：{e}"))
}

async fn call_err(client: &WsClient, method: &str, params: Value) -> String {
    match client.request::<Value, _>(method, named(params)).await {
        Ok(value) => panic!("{method} 本应失败，实际返回 {value}"),
        Err(err) => err.to_string(),
    }
}

fn line_def(code: &str) -> Value {
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

async fn publish(client: &WsClient, name: &str, def: Value) -> (String, i64) {
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

async fn wait_status(client: &WsClient, run_id: &str, expected: &str, timeout: Duration) -> Value {
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

/// 集群冒烟：定义生命周期 → run.start 原子创建 → executor 驱动到终态 →
/// timeline / events / signal_status 可读。
#[tokio::test]
async fn postgres_cluster_smoke() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn(&db.url, "all", 5000);
    let client = server.client().await;

    // draft 拒绝执行
    let created: Value = call(&client, "workflow.create", json!({"name": "冒烟"})).await;
    let wf = created["workflow_id"].as_str().unwrap().to_string();
    let updated: Value = call(
        &client,
        "workflow.update",
        json!({"workflow_id": wf, "definition": line_def("return 1 + 1;")}),
    )
    .await;
    let draft_v = updated["version"].as_i64().unwrap();
    let err = call_err(
        &client,
        "run.start",
        json!({"workflow_id": wf, "version": draft_v}),
    )
    .await;
    assert!(err.contains("尚未发布"), "{err}");
    call::<Value>(
        &client,
        "workflow.publish",
        json!({"workflow_id": wf, "version": draft_v}),
    )
    .await;

    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": wf, "input": {"x": 5}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let run = wait_status(&client, &run_id, "succeeded", Duration::from_secs(15)).await;
    assert_eq!(run["run"]["output"], json!(2));

    // 时间线
    let timeline: Value = call(&client, "run.timeline", json!({"run_id": run_id})).await;
    assert_eq!(timeline["phase"], json!("succeeded"));
    assert_eq!(timeline["nodes"].as_array().unwrap().len(), 3);

    // 事件增量拉取（from_seq 闭区间语义）
    let evs: Value = call(
        &client,
        "run.events",
        json!({"run_id": run_id, "from_seq": 3}),
    )
    .await;
    let list = evs["events"].as_array().unwrap();
    assert!(list.len() >= 2);
    assert_eq!(list[0]["seq"], json!(3));

    // nodetypes 能力清单
    let nt: Value = call(&client, "nodetypes.list", json!({})).await;
    assert!(nt["node_types"].as_array().unwrap().len() >= 6);

    server.kill();
    db.close().await;
}

/// §6.1：human 信号经持久 inbox 交付；幂等与冲突语义在 RPC 层可见。
#[tokio::test]
async fn postgres_human_signal_delivery() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn(&db.url, "all", 5000);
    let client = server.client().await;

    let (wf, _) = publish(
        &client,
        "人工",
        json!({
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "approve", "type": "human_task", "name": "审批"},
                {"id": "end", "type": "end"}
            ],
            "edges": [
                {"from": "start", "to": "approve"},
                {"from": "approve", "to": "end"}
            ]
        }),
    )
    .await;
    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // 等人土节点进入等待（信号先校验再持久化，节点未开始时会被拒）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let evs: Value = call(&client, "run.events", json!({"run_id": run_id})).await;
        let hit = evs["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == json!("node_started") && e["node_id"] == json!("approve"));
        if hit {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "human 节点未开始");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Postgres 模式必须带 signal_id
    let err = call_err(
        &client,
        "run.signal",
        json!({"run_id": run_id, "node_id": "approve", "payload": {}}),
    )
    .await;
    assert!(err.contains("signal_id"), "{err}");

    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "sig-1", "node_id": "approve", "payload": {"ok": true}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(true), "{ack}");
    assert!(ack["event_seq"].as_u64().unwrap() > 1);

    // 重复同 id 同内容：幂等返回 delivered
    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "sig-1", "node_id": "approve", "payload": {"ok": true}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(true));

    // 同 id 不同内容：conflict
    let err = call_err(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "sig-1", "node_id": "approve", "payload": {"ok": false}}),
    )
    .await;
    assert!(
        err.contains("不同内容") || err.contains("conflict") || err.contains("-32012"),
        "{err}"
    );

    wait_status(&client, &run_id, "succeeded", Duration::from_secs(10)).await;

    // signal_status 可查已应用结果（run 终结后仍可查询原结果）
    let st: Value = call(
        &client,
        "run.signal_status",
        json!({"run_id": run_id, "signal_id": "sig-1"}),
    )
    .await;
    assert_eq!(st["status"], json!("applied"));
    assert_eq!(st["delivered"], json!(true));

    server.kill();
    db.close().await;
}

/// gateway/executor 角色拆分：无 executor 时信号 pending（不是 delivered），
/// executor 加入后落账，run.signal_status 可查。
#[tokio::test]
async fn signal_pending_without_executor_then_delivered() {
    let Some(db) = test_db().await else { return };
    let mut gateway = ServerProc::spawn(&db.url, "gateway", 600);
    let client = gateway.client().await;

    let (wf, _) = publish(&client, "拆分", line_def("return input.v;")).await;
    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": wf, "input": {"v": 1}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // human 信号进入 pending（还没有 executor 驱动该 run）
    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "s-1", "node_id": "end", "payload": {"x": 1}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(false), "{ack}");
    assert_eq!(ack["pending"], json!(true));
    assert_eq!(ack["signal_id"], json!("s-1"));

    let st: Value = call(
        &client,
        "run.signal_status",
        json!({"run_id": run_id, "signal_id": "s-1"}),
    )
    .await;
    assert_eq!(st["status"], json!("pending"));

    // executor 加入后接管并消费（该信号非法——end 不等待——最终 rejected）
    let mut executor = ServerProc::spawn(&db.url, "executor", 5000);
    let _ = &executor;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let st: Value = call(
            &client,
            "run.signal_status",
            json!({"run_id": run_id, "signal_id": "s-1"}),
        )
        .await;
        if st["status"] != json!("pending") {
            assert_eq!(st["status"], json!("rejected"), "{st}");
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "信号一直 pending");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    gateway.kill();
    executor.child.kill().expect("kill executor 失败");
    executor.child.wait().expect("wait executor 失败");
    db.close().await;
}

/// §11.3：SIGKILL 后同库重启，未完成 run 从共享日志恢复（纯节点重放）。
#[tokio::test]
async fn sigkill_recovery_from_shared_log() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn(&db.url, "all", 5000);
    let client = server.client().await;

    let def = json!({
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "wait", "type": "delay", "name": "等待", "params": {"ms": 900}},
            {"id": "n1", "type": "script", "name": "脚本", "params": {"code": "return 'done';"}},
            {"id": "end", "type": "end"}
        ],
        "edges": [
            {"from": "start", "to": "wait"},
            {"from": "wait", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    });
    let (wf, _) = publish(&client, "恢复", def).await;
    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // 等 delay 节点开始后 SIGKILL
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let evs: Value = call(&client, "run.events", json!({"run_id": run_id})).await;
        let hit = evs["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == json!("node_started") && e["node_id"] == json!("wait"));
        if hit {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "delay 未开始");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.kill();

    // 同库重启：run 必须被新进程接管并完成
    let mut server2 = ServerProc::spawn(&db.url, "all", 5000);
    let client2 = server2.client().await;
    let run = wait_status(&client2, &run_id, "succeeded", Duration::from_secs(20)).await;
    assert_eq!(run["run"]["output"], json!("done"));

    let evs: Value = call(&client2, "run.events", json!({"run_id": run_id})).await;
    let events = evs["events"].as_array().unwrap();
    let wait_starts = events
        .iter()
        .filter(|e| e["type"] == json!("node_started") && e["node_id"] == json!("wait"))
        .count();
    assert_eq!(
        wait_starts, 2,
        "delay 节点应重放一次（attempt 1/2）：{events:?}"
    );

    server2.kill();
    db.close().await;
}

/// §8：订阅按 run_id 维护游标轮询增量；seq 严格递增，终态事件可达。
#[tokio::test]
async fn subscription_streams_events_in_order() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn(&db.url, "all", 5000);
    let client = server.client().await;

    let (wf, _) = publish(&client, "订阅", line_def("return 'hi';")).await;

    let mut sub = client
        .subscribe::<Value, _>("run.subscribe", named(json!({})), "run.unsubscribe")
        .await
        .expect("订阅失败");

    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let mut last_seq = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "订阅未收到 run_completed"
        );
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("订阅超时")
            .expect("订阅流结束");
        let envelope = match msg {
            Ok(v) => v,
            Err(err) => panic!("订阅错误：{err}"),
        };
        assert_eq!(envelope["run_id"], json!(run_id));
        let seq = envelope["seq"].as_u64().unwrap();
        assert!(seq > last_seq, "seq 必须递增：{seq} after {last_seq}");
        last_seq = seq;
        if envelope["type"] == json!("run_completed") {
            break;
        }
    }

    server.kill();
    db.close().await;
}

/// Postgres 模式的 sub_workflow：子 run 经 gateway 单事务创建（确定性 id 幂等），
/// 父 run 轮询共享投影等待终态（父子可能落在不同 executor）。
#[tokio::test]
async fn postgres_sub_workflow_end_to_end() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn(&db.url, "all", 5000);
    let client = server.client().await;

    let (child_wf, _) = publish(&client, "子流程", line_def("return { got: input };")).await;
    let (parent_wf, _) = publish(
        &client,
        "父流程",
        json!({
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "sub", "type": "sub_workflow", "params": {"workflow_id": child_wf}},
                {"id": "end", "type": "end"}
            ],
            "edges": [{"from": "start", "to": "sub"}, {"from": "sub", "to": "end"}]
        }),
    )
    .await;

    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": parent_wf, "input": {"amount": 7}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let run = wait_status(&client, &run_id, "succeeded", Duration::from_secs(20)).await;
    assert_eq!(run["run"]["output"], json!({"got": {"amount": 7}}));

    // 父日志的时间线带确定性 child_run_id；子 run 的 run_started 记录深度 1
    let timeline: Value = call(&client, "run.timeline", json!({"run_id": run_id})).await;
    let sub = timeline["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == "sub")
        .unwrap();
    let child_run_id = sub["child_run_id"].as_str().unwrap().to_string();
    assert_eq!(child_run_id, format!("{run_id}:sub:1"));

    let child = wait_status(&client, &child_run_id, "succeeded", Duration::from_secs(10)).await;
    assert_eq!(child["run"]["output"], json!({"got": {"amount": 7}}));
    let events: Value = call(&client, "run.events", json!({"run_id": child_run_id})).await;
    assert_eq!(events["events"][0]["depth"], json!(1));

    server.kill();
    db.close().await;
}
