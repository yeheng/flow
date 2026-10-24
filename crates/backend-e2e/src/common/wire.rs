//! wire 级进程 harness：真起 `flow-journal-server` 进程 + v2 JSON-RPC 客户端助手。
//!
//! 来源：原 `flow-rpc/tests/common/mod.rs`（测试随套件拆分迁入 backend-e2e，
//! 见 docs/e2e-coverage.md 的 E01 盘点）。单点维护原则不变（DESIGN.md §14）：
//! [`ServerProc`] 的进程管理真身在 [`crate::common::server`]（与 e2e_test! 宏
//! 的 Ctx 共用一份），这里只补 `from_parts` 装配口和客户端协议助手。
//!
//! 与 `client.rs` 的分工：那边的 `Conn` 绑定 e2e 随机 token（e2e_test! 用例）；
//! 这边是固定 TOKEN + 具名参数 + `.view` 物化 + request_id 注入的 v2 wire
//! 约定，供迁移过来的 wire 契约测试（journal_v2 / ws_journal / sigkill /
//! cluster）继续讲同一套协议。
//!
//! 被测二进制定位**不在这里**：`env!("CARGO_BIN_EXE_*")` 只在测试 target
//! 编译期注入，lib 里取不到。各测试自己解析：
//! - backend-e2e 内：`env!("CARGO_BIN_EXE_flow-journal-server-e2e")`
//! - 其他 crate（remote-e2e）：`flow_test_support::io::server_bin()`

pub use flow_test_support::io::spawn_reporting_ports;

pub use crate::common::server::ServerProc;

/// 固定部署 token（迁移前的 flow-rpc 进程测试沿用同一约定）。
pub const TOKEN: &str = "flow-rpc-process-test-token-32-bytes";

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde_json::{json, Value};

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

/// 发起一次 wire 级调用并反序列化结果。
pub async fn call<T: serde::de::DeserializeOwned>(
    client: &WsClient,
    method: &str,
    params: Value,
) -> T {
    let method = materialized(method);
    let reply: Value = client
        .request(method, named(enrich(method, params)))
        .await
        .unwrap_or_else(|e| panic!("调用 {method} 失败：{e}"));
    let result = if reply["committed"] == true {
        reply["result"].clone()
    } else {
        reply
    };
    serde_json::from_value(result).unwrap()
}

/// 把方法名映射到它「物化」后的方法名（读方法直接映射到视图方法）。
fn materialized(method: &str) -> &str {
    match method {
        "workflow.get" => "workflow.get.view",
        "workflow.list" => "workflow.list.view",
        "run.get" => "run.get.view",
        "run.list" => "run.list.view",
        "run.events" => "run.events.view",
        other => other,
    }
}
/// 给参数补上 wire 协议必需的字段（令牌、幂等键等）。
fn enrich(method: &str, mut params: Value) -> Value {
    params["_token"] = json!(TOKEN);
    if flow_rpc::journal_v2::is_write_method(method) && params.get("request_id").is_none() {
        params["request_id"] = json!(uuid::Uuid::now_v7().to_string());
    }
    params
}
/// 发起一次注定失败的调用，返回错误文本。
pub async fn call_err(client: &WsClient, method: &str, params: Value) -> String {
    let method = materialized(method);
    match client
        .request::<Value, _>(method, named(enrich(method, params)))
        .await
    {
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

/// 启动一次运行，返回 run_id。
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
