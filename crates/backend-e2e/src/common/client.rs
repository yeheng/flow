//! JSON-RPC over WebSocket 客户端助手：具名参数、错误码提取、状态轮询、订阅收集。
//!
//! 全部走真实 wire（进程外 flow-server），断言的是协议与语义，不是内部 API。

use std::time::{Duration, Instant};

use jsonrpsee::core::client::{ClientT, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::core::traits::ToRpcParams;
use jsonrpsee::core::ClientError;
use jsonrpsee::types::error::ErrorObjectOwned;
use jsonrpsee::ws_client::WsClient;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use serde_json::{json, Value};

/// 客户端类型别名（测试助手签名用）。
pub type Client = WsClient;

/// 具名参数（设计约定：所有方法 params 为对象）。
pub fn named(value: Value) -> ObjectParams {
    let mut params = ObjectParams::new();
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                params.insert(&key, item).expect("插入参数失败");
            }
        }
        Value::Null => {}
        other => panic!("具名参数必须是对象：{other}"),
    }
    params
}

/// 整体省略 params（JSON-RPC 允许）：线上到达的是 null，服务端归一为 {}。
pub struct NullParams;

impl ToRpcParams for NullParams {
    fn to_rpc_params(self) -> Result<Option<Box<RawValue>>, serde_json::Error> {
        serde_json::value::to_raw_value(&Value::Null).map(Some)
    }
}

pub async fn connect(addr: std::net::SocketAddr) -> WsClient {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match jsonrpsee::ws_client::WsClientBuilder::default()
            .connection_timeout(Duration::from_secs(5))
            .request_timeout(Duration::from_secs(30))
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

pub async fn call<T: DeserializeOwned>(client: &WsClient, method: &str, params: Value) -> T {
    client
        .request(method, named(params))
        .await
        .unwrap_or_else(|e| panic!("调用 {method} 失败：{e}"))
}

pub async fn call_json(client: &WsClient, method: &str, params: Value) -> Value {
    call::<Value>(client, method, params).await
}

/// 整体省略 params 的成功调用（服务端必须把 null 归一为 {}）。
pub async fn call_null_params<T: DeserializeOwned>(client: &WsClient, method: &str) -> T {
    client
        .request(method, NullParams)
        .await
        .unwrap_or_else(|e| panic!("调用 {method}（无 params）失败：{e}"))
}

/// 预期失败的调用：返回服务端业务错误对象（断言 code/message 用）。
pub async fn call_err(client: &WsClient, method: &str, params: Value) -> ErrorObjectOwned {
    match client.request::<Value, _>(method, named(params)).await {
        Ok(value) => panic!("{method} 本应失败，实际返回 {value}"),
        Err(ClientError::Call(err)) => err,
        Err(other) => panic!("{method} 预期业务错误，实际传输层错误：{other}"),
    }
}

pub fn err_code(err: &ClientError) -> i32 {
    match err {
        ClientError::Call(obj) => obj.code(),
        other => panic!("预期 JSON-RPC 业务错误（可提取 code），实际：{other}"),
    }
}

/// 建工作流 → 存定义 → 发布，返回 (workflow_id, version)。
pub async fn publish_workflow(client: &WsClient, name: &str, definition: Value) -> (String, i64) {
    let created: Value = call(client, "workflow.create", json!({ "name": name })).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
    let updated: Value = call(
        client,
        "workflow.update",
        json!({ "workflow_id": workflow_id, "definition": definition }),
    )
    .await;
    let version = updated["version"].as_i64().unwrap();
    call::<Value>(
        client,
        "workflow.publish",
        json!({ "workflow_id": workflow_id, "version": version }),
    )
    .await;
    (workflow_id, version)
}

/// run.start，返回 run_id（自动带上 workflow_version 断言）。
pub async fn start_run(client: &WsClient, workflow_id: &str, input: Value) -> String {
    let started: Value = call(
        client,
        "run.start",
        json!({ "workflow_id": workflow_id, "input": input }),
    )
    .await;
    assert!(
        started["workflow_version"].is_i64(),
        "run.start 必须回工作流版本：{started}"
    );
    started["run_id"].as_str().unwrap().to_string()
}

pub async fn get_run(client: &WsClient, run_id: &str) -> Value {
    call_json(client, "run.get", json!({ "run_id": run_id })).await
}

/// 轮询 run.get 直到状态符合预期（超时即 panic 带最后观测值）。
pub async fn wait_run_status(
    client: &WsClient,
    run_id: &str,
    expected: &str,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)]
    let mut last = Value::Null;
    loop {
        last = get_run(client, run_id).await;
        if last["run"]["status"] == json!(expected) {
            return last;
        }
        if Instant::now() > deadline {
            panic!("等待 run {run_id} 到 {expected} 超时：{last}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_run_terminal(client: &WsClient, run_id: &str, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)]
    let mut last = Value::Null;
    loop {
        last = get_run(client, run_id).await;
        let status = last["run"]["status"].as_str().unwrap_or_default();
        if matches!(status, "succeeded" | "failed" | "cancelled") {
            return last;
        }
        if Instant::now() > deadline {
            panic!("等待 run {run_id} 终态超时：{last}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 订阅 run.event。run_id 为 None 时是全局实时流。
pub async fn subscribe(
    client: &WsClient,
    run_id: Option<String>,
) -> jsonrpsee::core::client::Subscription<Value> {
    let params = match run_id {
        Some(run_id) => json!({ "run_id": run_id }),
        None => json!({}),
    };
    client
        .subscribe::<Value, _>("run.subscribe", named(params), "run.unsubscribe")
        .await
        .expect("订阅失败")
}

/// 收订阅事件直到某个 run 的终态事件出现（或超时）。返回该 run 的事件序列。
pub async fn collect_run_events(
    sub: &mut jsonrpsee::core::client::Subscription<Value>,
    run_id: &str,
    timeout: Duration,
) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut events = Vec::new();
    loop {
        let msg = tokio::time::timeout_at(deadline, sub.next())
            .await
            .unwrap_or_else(|_| panic!("订阅在 {timeout:?} 内没收到 run {run_id} 的终态事件"));
        let Some(msg) = msg else {
            panic!("订阅流意外结束（run {run_id} 未终结）");
        };
        let envelope = msg.expect("订阅消息错误");
        if envelope["run_id"] == json!(run_id) {
            let terminal = matches!(
                envelope["type"].as_str(),
                Some("run_completed") | Some("run_failed") | Some("run_cancelled")
            );
            events.push(envelope);
            if terminal {
                return events;
            }
        }
    }
}

/// webhook HTTP：POST /hook/<token>，返回 (状态码, 响应体文本)。
pub async fn http_post_hook(
    http_addr: std::net::SocketAddr,
    token: &str,
    body: Option<&str>,
) -> (reqwest::StatusCode, String) {
    let mut request = reqwest::Client::new().post(format!("http://{http_addr}/hook/{token}"));
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let response = request
        .send()
        .await
        .unwrap_or_else(|e| panic!("webhook 请求失败：{e}"));
    let status = response.status();
    let text = response
        .text()
        .await
        .unwrap_or_else(|e| panic!("读取 webhook 响应失败：{e}"));
    (status, text)
}

pub const TIMEOUT: Duration = Duration::from_secs(20);
pub const SHORT: Duration = Duration::from_secs(5);
