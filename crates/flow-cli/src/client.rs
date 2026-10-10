//! JSON-RPC WebSocket 客户端封装。
//!
//! CLI 与服务的**唯一**通道。这里刻意保持「薄」：只做连接、动态方法调用与
//! 错误归一化，不内置任何业务规则——「published 才能执行」「定义校验」等
//! 全部留在服务端 single source of truth（DESIGN.md §9），CLI 多一份判断
//! 就会多一处会过时的副本。
//!
//! 参数用 `serde_json::Map` 动态拼装（方法面是开放的字符串表，没有编译期
//! 类型可用）；jsonrpsee 的 `ToRpcParams` 对 Map 有现成实现。

use jsonrpsee::ws_client::WsClient;
use serde_json::{Map, Value};

use crate::error::CliError;

/// 连接服务。URL 归一化：`127.0.0.1:9802` 之类缺 scheme 的按 `ws://` 补全；
/// `http(s)://` 原样透传（jsonrpsee 的 ws-client 接受该 scheme 指代 ws）。
pub async fn connect(url: &str) -> Result<WsClient, CliError> {
    let normalized = if url.contains("://") {
        url.to_string()
    } else {
        format!("ws://{url}")
    };
    jsonrpsee::ws_client::WsClientBuilder::default()
        .build(&normalized)
        .await
        .map_err(|err| {
            CliError::local(format!(
                "连不上 flow-journal-server（{normalized}）：{err}\n提示：先 `cargo run --bin flow-journal-server`，或用 --url / FLOW_RPC 指定地址"
            ))
        })
}

/// 便捷构造 RPC 参数对象。方法面是动态字符串表，参数靠现场拼装；
/// 键值对按给定顺序插入（只影响调试可读性，不影响语义）。
pub fn object(pairs: Vec<(&str, Value)>) -> Map<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

/// 动态调用一个 RPC 方法。params 为空对象时方法仍会收到 `{}`——服务端解析层
/// 把整体省略（null）与 `{}` 同等对待（DESIGN.md §9），这里统一发对象更直观。
pub async fn call(
    client: &WsClient,
    method: &str,
    params: Map<String, Value>,
) -> Result<Value, CliError> {
    let token = std::env::var("FLOW_JOURNAL_TOKEN")
        .map_err(|_| CliError::local("FLOW_JOURNAL_TOKEN required"))?;
    let reply = crate::journal::call(client, &token, method, Value::Object(params)).await?;
    if is_write_method(method) && reply["committed"] == true {
        Ok(reply["result"].clone())
    } else {
        Ok(reply)
    }
}

pub(crate) fn is_write_method(method: &str) -> bool {
    matches!(
        method,
        "workflow.create"
            | "workflow.update"
            | "workflow.publish"
            | "workflow.delete"
            | "run.start"
            | "run.cancel"
            | "run.signal"
            | "run.adjudicate"
            | "schedule.change"
            | "schedule.create"
            | "schedule.update"
            | "schedule.delete"
            | "webhook.change"
            | "webhook.create"
            | "webhook.set_enabled"
            | "webhook.delete"
            | "template.create"
            | "template.update"
            | "template.delete"
            | "config.update"
            | "secrets.set"
            | "secrets.delete"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_error_keeps_server_code_for_scripts() {
        let err = CliError::Rpc {
            code: -32011,
            message: "工作流不存在：x".into(),
        };
        assert!(err.to_string().contains("-32011"), "{err}");
    }

    #[test]
    fn local_error_never_claims_a_server_code() {
        let err = CliError::local("文件不存在：x.json");
        assert!(!err.to_string().contains("code"), "{err}");
    }
}
