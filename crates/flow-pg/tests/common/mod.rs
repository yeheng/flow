//! PG 测试基建：每个测试使用独立数据库，彻底隔离扫描与容量状态。
//!
//! 容器与测试库生命周期的真身在 `flow-test-support::pg`——与 flow-rpc 的
//! ws_pg、backend-e2e 同指一份。测试库生命周期只能有一处实现：多份必然漂移，
//! 而漂移的代价是某份的残留清扫悄悄失效、测试库在服务器上堆到没人发现。
//! 这里只留 flow-pg 的
//! 测试自己需要的部分：连接配置、发布/起 run 助手、定义构造器、断言工具。
//!
//! 未配置 FLOW_TEST_DATABASE_URL 时返回 None，测试自动跳过。
//!
//! common 被多个测试二进制共享，各二进制只用到其中一部分辅助函数。
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use flow_test_support::pg::TestDb;
use sqlx::{PgConnection, PgPool, Row};

use flow_pg::{PgConfig, PgEngine};

/// PG 测试库名前缀：残留下一次启动时只扫这个前缀的库。
pub const DB_PREFIX: &str = "flow_test_";

pub const DEFAULT_URL: &str = "postgres://flow:flow@127.0.0.1:54329/flow";

/// 连到基础服务器，创建独立的测试数据库。
/// 数据库名带时间戳，启动时顺手清理超过 15 分钟的残留库。
pub async fn test_db() -> Option<TestDb> {
    let base = std::env::var("FLOW_TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    if std::env::var("FLOW_TEST_DATABASE_URL").is_err() && !port_open().await {
        // 未显式配置且默认端口不可达：跳过
        eprintln!("skip: 未设置 FLOW_TEST_DATABASE_URL 且 {DEFAULT_URL} 不可达");
        return None;
    }
    let db = TestDb::create(&base, DB_PREFIX).await;
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

/// 测试用引擎配置：快速轮询、短 TTL。
pub fn fast_config(role: flow_pg::Role) -> PgConfig {
    PgConfig {
        lease_ttl: Duration::from_millis(1500),
        scan_interval: Duration::from_millis(50),
        inbox_poll: Duration::from_millis(50),
        max_runs: 8,
        signal_wait: Duration::from_secs(5),
        signal_poll: Duration::from_millis(50),
        subscribe_poll: Duration::from_millis(50),
        statement_timeout: Duration::from_secs(15),
        lock_timeout: Duration::from_secs(15),
        idle_tx_timeout: Duration::from_secs(15),
        role,
    }
}

pub async fn engine(db: &TestDb, role: flow_pg::Role) -> Arc<PgEngine> {
    Arc::new(
        PgEngine::connect(&db.url, fast_config(role))
            .await
            .expect("连接引擎失败"),
    )
}

/// 发布一个工作流并返回 (workflow_id, version)。
pub async fn publish_definition(
    engine: &PgEngine,
    name: &str,
    definition: serde_json::Value,
) -> (String, i64) {
    let store = engine.store();
    let workflow_id = store.create_workflow(name).await.unwrap();
    let version = store
        .update_workflow(&workflow_id, &definition)
        .await
        .unwrap();
    store.publish(&workflow_id, version).await.unwrap();
    (workflow_id, version)
}

/// 原子创建 run（gateway 入口），返回 run_id。version 必须已发布
/// （published 解析与校验规则单点在 flow-backend，见 sqlite.rs 测试）。
pub async fn start_run(
    engine: &PgEngine,
    workflow_id: &str,
    version: i64,
    input: serde_json::Value,
) -> String {
    let created = engine
        .create_run(flow_pg::CreateRun {
            workflow_id: workflow_id.to_string(),
            version,
            input,
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    created.run_id
}

/// 轮询直到谓词为真。
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "等待 {what} 超时");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn run_status(pool: &PgPool, run_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM runs WHERE id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub async fn run_error(pool: &PgPool, run_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT error FROM runs WHERE id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub async fn run_output(pool: &PgPool, run_id: &str) -> Option<serde_json::Value> {
    sqlx::query_scalar("SELECT output FROM runs WHERE id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub type EventRow = (i64, String, Option<String>, Option<i64>, serde_json::Value);

/// 事件 (seq, kind, node_id, attempt, payload) 列表。
pub async fn events(pool: &PgPool, run_id: &str) -> Vec<EventRow> {
    let rows = sqlx::query(
        "SELECT seq, payload->>'type' AS kind, payload->>'node_id' AS node_id,
                payload->>'attempt' AS attempt, payload
         FROM run_events WHERE run_id = $1 ORDER BY seq",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|r| {
            (
                r.get::<i64, _>("seq"),
                r.get::<String, _>("kind"),
                r.get::<Option<String>, _>("node_id"),
                // attempt 在 SQL 里被 `->>` 取出成文本，这里转回数字
                r.get::<Option<String>, _>("attempt")
                    .and_then(|s| s.parse().ok()),
                r.get::<serde_json::Value, _>("payload"),
            )
        })
        .collect()
}

/// 统计某节点某类事件（按 attempt 区分重试）。
pub fn attempts_of(evs: &[EventRow], node: &str, kind: &str) -> Vec<i64> {
    evs.iter()
        .filter(|(_, k, n, _, _)| k == kind && n.as_deref() == Some(node))
        .map(|(s, _, _, _, _)| *s)
        .collect()
}

pub async fn lease_of(
    pool: &PgPool,
    run_id: &str,
) -> (Option<String>, i64, Option<chrono::DateTime<chrono::Utc>>) {
    let rec =
        sqlx::query("SELECT lease_owner, lease_epoch, lease_expires_at FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_one(pool)
            .await
            .unwrap();
    (
        rec.get("lease_owner"),
        rec.get("lease_epoch"),
        rec.get("lease_expires_at"),
    )
}

/// 直接以 SQL 插入一条 pending inbox 行（绕过 gateway 的终态检查，用于构造竞态窗口）。
pub async fn insert_pending_signal_raw(
    conn: &mut PgConnection,
    run_id: &str,
    signal_id: &str,
    node_id: &str,
    payload: serde_json::Value,
) {
    sqlx::query(
        "INSERT INTO run_signals (run_id, signal_id, kind, node_id, payload, created_at)
         VALUES ($1, $2, 'signal', $3, $4, clock_timestamp())",
    )
    .bind(run_id)
    .bind(signal_id)
    .bind(node_id)
    .bind(payload)
    .execute(conn)
    .await
    .unwrap();
}

pub fn def_line(script_code: &str) -> serde_json::Value {
    serde_json::json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "n1", "type": "script", "name": "脚本", "params": {"code": script_code}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    })
}

pub fn def_human() -> serde_json::Value {
    serde_json::json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "h", "type": "human_task", "name": "人工"},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "h"},
            {"from": "h", "to": "end"}
        ]
    })
}

pub fn def_http(retry: Option<u32>) -> serde_json::Value {
    let mut params = serde_json::json!({"url": "PLACEHOLDER", "method": "POST"});
    if let Some(max_attempts) = retry {
        params["retry"] = serde_json::json!({"max_attempts": max_attempts, "backoff_ms": 300});
    }
    serde_json::json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "h", "type": "http_call", "name": "HTTP", "params": params},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "h"},
            {"from": "h", "to": "end"}
        ]
    })
}

/// 本地 HTTP 计数服务：接受连接、读取请求、回复 200；返回 (地址, 计数 handle)。
pub async fn http_counter(status_line: &str) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_clone = count.clone();
    let status = status_line.to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let status = status.clone();
            let count = count_clone.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = "ok";
                let resp = format!(
                    "{status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (addr, count)
}
