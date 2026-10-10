//! v2 产品面的 RPC/HTTP 客户端助手：token 注入、写命令幂等键与回执解包、
//! COMMITTED_NOT_VISIBLE(-32020) 恢复、订阅 v2 事件格式、webhook
//! `POST /hooks/<key>`（Bearer + Idempotency-Key）。
//!
//! 全部走真实 wire（进程外 flow-journal-server），断言的是 v2 协议与物化
//! 数据形状，不是内部 API。
//!
//! [`Conn`] 是 WsClient + 部署 token 的薄包装：`call` 自动补 `_token`；写命令
//! （`flow_rpc::journal_v2::is_write_method` 集合）自动带 `request_id` 并把
//! 回执 `{committed, result, ...}` 解包为 `result`（用例零感知）。

use std::time::{Duration, Instant};

use jsonrpsee::core::client::{ClientT, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::core::ClientError;
use jsonrpsee::types::error::ErrorObjectOwned;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

/// 连接超时（服务进程刚 bind 时路由可能还没装完，带重试）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// -32020 恢复的轮询参数（与 flow-cli journal call 同节奏）。
const STATUS_POLLS: usize = 20;
const STATUS_INTERVAL: Duration = Duration::from_millis(250);

/// v2 连接：WebSocket 客户端 + 部署 token。
pub struct Conn {
    ws: WsClient,
    token: String,
}

/// 连上 WebSocket 服务端，失败重试到超时。
pub async fn connect(addr: std::net::SocketAddr, token: &str) -> Conn {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    loop {
        match WsClientBuilder::default()
            .connection_timeout(Duration::from_secs(5))
            .request_timeout(Duration::from_secs(30))
            .build(format!("ws://{addr}"))
            .await
        {
            Ok(ws) => {
                return Conn {
                    ws,
                    token: token.to_string(),
                }
            }
            Err(err) => {
                if Instant::now() > deadline {
                    panic!("连接 {addr} 失败：{err}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

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

/// 一次原始调用（enrich 后的 params 直发），返回 result 或业务错误。
async fn raw_call(conn: &Conn, method: &str, params: Value) -> Result<Value, ClientError> {
    let mut enriched = match params {
        Value::Object(map) => Value::Object(map),
        Value::Null => json!({}),
        other => panic!("params 必须是对象：{other}"),
    };
    if let Some(obj) = enriched.as_object_mut() {
        obj.insert("_token".into(), json!(conn.token));
    }
    conn.ws.request::<Value, _>(method, named(enriched)).await
}

/// v2 语义调用：token 注入 + 写命令 request_id/回执解包 + -32020 恢复。
/// 写命令返回 `receipt.result`（v1 门面形状）。
pub async fn call<T: DeserializeOwned>(conn: &Conn, method: &str, params: Value) -> T {
    let write = flow_rpc::journal_v2::is_write_method(method);
    let request_id = if write {
        Some(
            params
                .get("request_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
        )
    } else {
        None
    };
    let mut enriched = match params {
        Value::Object(map) => Value::Object(map),
        Value::Null => json!({}),
        other => panic!("params 必须是对象：{other}"),
    };
    if let Some(obj) = enriched.as_object_mut() {
        obj.insert("_token".into(), json!(conn.token));
        if let Some(id) = request_id.as_deref() {
            obj.entry("request_id").or_insert(json!(id));
        }
    }
    let reply: Value = match conn
        .ws
        .request::<Value, _>(method, named(enriched.clone()))
        .await
    {
        Ok(reply) => reply,
        // COMMITTED_NOT_VISIBLE：命令已提交、投影尚未可见。绝不能重发写
        // 命令——按原 request_id 轮询 command.status 到可见为止。
        Err(ClientError::Call(err))
            if err.code() == flow_rpc::journal_v2::COMMITTED_NOT_VISIBLE =>
        {
            recover_committed(conn, method, &enriched, &err).await
        }
        Err(other) => panic!("调用 {method} 失败：{other}"),
    };
    if write {
        serde_json::from_value(reply["result"].clone())
            .unwrap_or_else(|e| panic!("{method} 回执 result 非法（{e}）：{reply}"))
    } else {
        serde_json::from_value(reply).unwrap_or_else(|e| panic!("{method} 响应非法（{e}）"))
    }
}

/// -32020 恢复：解析原始回执（error.data），按原 request_id 轮询
/// command.status 至 visible；超时则回退原始回执（提交事实已成立）。
async fn recover_committed(
    conn: &Conn,
    method: &str,
    original_params: &Value,
    err: &ErrorObjectOwned,
) -> Value {
    let original: Value = match err.data() {
        Some(data) => serde_json::from_str(data.get())
            .unwrap_or_else(|e| panic!("{method} 的 -32020 回执解析失败：{e}")),
        None => panic!("{method} 返回 -32020 但没有回执数据"),
    };
    let request_id = original["request_id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        original["committed"] == json!(true) && !request_id.is_empty(),
        "{method} 的 -32020 回执必须带 committed 与 request_id：{original}"
    );
    let scope = flow_rpc::journal_v2::command_scope(method, original_params);
    for _ in 0..STATUS_POLLS {
        let status: Value = raw_call(
            conn,
            "command.status",
            json!({"scope": scope, "request_id": request_id}),
        )
        .await
        .unwrap_or_else(|e| panic!("command.status 查询失败：{e}"));
        if status["visible"] == json!(true) {
            return status;
        }
        tokio::time::sleep(STATUS_INTERVAL).await;
    }
    original
}

pub async fn call_json(conn: &Conn, method: &str, params: Value) -> Value {
    call::<Value>(conn, method, params).await
}

/// 不 panic 的探测调用：实体可能尚未落库（initializing 窗口）时把业务错误
/// （如 -32011 not found）交给调用方当「未就绪」处理。
pub async fn try_call_json(
    conn: &Conn,
    method: &str,
    params: Value,
) -> Result<Value, ErrorObjectOwned> {
    let mut enriched = match params {
        Value::Object(map) => Value::Object(map),
        Value::Null => json!({}),
        other => panic!("params 必须是对象：{other}"),
    };
    if let Some(obj) = enriched.as_object_mut() {
        obj.insert("_token".into(), json!(conn.token));
        if flow_rpc::journal_v2::is_write_method(method) {
            obj.entry("request_id")
                .or_insert_with(|| json!(uuid::Uuid::now_v7().to_string()));
        }
    }
    match conn
        .ws
        .request::<Value, _>(method, named(enriched.clone()))
        .await
    {
        Ok(value) => Ok(value),
        Err(ClientError::Call(err))
            if err.code() == flow_rpc::journal_v2::COMMITTED_NOT_VISIBLE =>
        {
            Ok(recover_committed(conn, method, &enriched, &err).await)
        }
        Err(ClientError::Call(err)) => Err(err),
        Err(other) => panic!("{method} 探测调用失败（传输层）：{other}"),
    }
}

/// 预期失败的调用：返回服务端业务错误对象（断言 code/message 用）。
/// 注入逻辑与 [`call`] 一致（token / request_id），但不做回执解包。
pub async fn call_err(conn: &Conn, method: &str, params: Value) -> ErrorObjectOwned {
    let mut enriched = match params {
        Value::Object(map) => Value::Object(map),
        Value::Null => json!({}),
        other => panic!("params 必须是对象：{other}"),
    };
    if let Some(obj) = enriched.as_object_mut() {
        obj.insert("_token".into(), json!(conn.token));
        if flow_rpc::journal_v2::is_write_method(method) {
            obj.entry("request_id")
                .or_insert_with(|| json!(uuid::Uuid::now_v7().to_string()));
        }
    }
    match conn.ws.request::<Value, _>(method, named(enriched)).await {
        Ok(value) => panic!("{method} 本应失败，实际返回 {value}"),
        Err(ClientError::Call(err)) => err,
        Err(other) => panic!("{method} 预期业务错误，实际传输层错误：{other}"),
    }
}

/// 建工作流 → 存定义 → 发布，返回 (workflow_id, version)。
pub async fn publish_workflow(conn: &Conn, name: &str, definition: Value) -> (String, i64) {
    let created: Value = call(conn, "workflow.create", json!({ "name": name })).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
    let updated: Value = call(
        conn,
        "workflow.update",
        json!({ "workflow_id": workflow_id, "definition": definition }),
    )
    .await;
    let version = updated["version"].as_i64().unwrap();
    call::<Value>(
        conn,
        "workflow.publish",
        json!({ "workflow_id": workflow_id, "version": version }),
    )
    .await;
    (workflow_id, version)
}

/// run.start，返回 run_id（自动带上 workflow_version 断言）。
pub async fn start_run(conn: &Conn, workflow_id: &str, input: Value) -> String {
    let started: Value = call(
        conn,
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

pub async fn get_run(conn: &Conn, run_id: &str) -> Value {
    call_json(conn, "run.get.view", json!({ "run_id": run_id })).await
}

/// 轮询 run.get.view 直到状态符合预期（超时即 panic 带最后观测值）。
pub async fn wait_run_status(
    conn: &Conn,
    run_id: &str,
    expected: &str,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)]
    let mut last = Value::Null;
    loop {
        last = get_run(conn, run_id).await;
        if last["run"]["status"] == json!(expected) {
            return last;
        }
        if Instant::now() > deadline {
            panic!("等待 run {run_id} 到 {expected} 超时：{last}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_run_terminal(conn: &Conn, run_id: &str, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)]
    let mut last = Value::Null;
    loop {
        last = get_run(conn, run_id).await;
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

/// 订阅 run.event（journal v2 事件原形 + token）。
pub async fn subscribe(conn: &Conn, run_id: &str) -> jsonrpsee::core::client::Subscription<Value> {
    conn.ws
        .subscribe::<Value, _>(
            "run.subscribe",
            named(json!({
                "_token": conn.token,
                "run_id": run_id,
                "event_format": "v2",
                "from_seq": "0",
            })),
            "run.unsubscribe",
        )
        .await
        .expect("订阅失败")
}

/// 收订阅事件直到 run 的终态事件出现（或超时）。返回该 run 的事件序列。
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
        if envelope["event"]["run_id"] == json!(run_id) {
            let terminal = matches!(
                envelope["event"]["kind"].as_str(),
                Some("run_completed") | Some("run_failed") | Some("run_cancelled")
            );
            events.push(envelope);
            if terminal {
                return events;
            }
        }
    }
}

/// webhook HTTP：`POST /hooks/<key>`（Bearer 部署 token + Idempotency-Key），
/// 返回 (状态码, 响应体文本)。
pub async fn http_post_hook(
    http_addr: std::net::SocketAddr,
    api_token: &str,
    key: &str,
    body: Option<&str>,
) -> (reqwest::StatusCode, String) {
    let mut request = reqwest::Client::new()
        .post(format!("http://{http_addr}/hooks/{key}"))
        .header("authorization", format!("Bearer {api_token}"))
        .header("idempotency-key", uuid::Uuid::now_v7().to_string());
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
