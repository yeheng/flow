//! flow-rpc 集成测试基建：真起 `flow-journal-server` 进程（隔离 Journal 目录）、
//! JSON-RPC 客户端助手、定义构造器。
//!
//! 进程脚手架统一在这里：`ServerProc` / `free_port` / `wait_ready` / `connect` /
//! `named` / `call` / `call_err` / `line_def` 这些跨用例共用的东西只此一份
//! （DESIGN §13：共享夹具不逐文件复制——复制品会各自漂移）。
//! 就绪判定用 TCP connect 轮询（DESIGN.md §13 的约定）；`connect` 带重试，
//! 因为进程刚 bind 上端口时 RPC 路由可能还没装完。
//!
//! common 被多个 Journal 测试二进制共享，各自使用不同的辅助函数。
#![allow(dead_code)]

pub use flow_test_support::io::spawn_reporting_ports;
pub fn server_bin() -> std::path::PathBuf {
    env!("CARGO_BIN_EXE_flow-journal-server").into()
}
pub const TOKEN: &str = "flow-rpc-process-test-token-32-bytes";

use std::net::SocketAddr;
use std::process::Child;
use std::time::{Duration, Instant};

use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde_json::{json, Value};

/// 一个被测 flow-journal-server 进程。Drop 保证 SIGKILL + wait（不漏僵尸）。
pub struct ServerProc {
    child: Child,
    addr: SocketAddr,
    http_addr: SocketAddr,
}

impl ServerProc {
    /// 由已启动的子进程与报出的端口组装（自定义环境形态的 spawn 用）。
    pub fn from_parts(
        child: std::process::Child,
        addr: SocketAddr,
        http_addr: SocketAddr,
    ) -> ServerProc {
        ServerProc {
            child,
            addr,
            http_addr,
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
fn enrich(method: &str, mut params: Value) -> Value {
    params["_token"] = json!(TOKEN);
    if flow_rpc::journal_v2::is_write_method(method) && params.get("request_id").is_none() {
        params["request_id"] = json!(uuid::Uuid::now_v7().to_string());
    }
    params
}
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
