//! JSON-RPC WebSocket 客户端封装。
//!
//! CLI 与服务的**唯一**通道。这里刻意保持「薄」：只做连接、动态方法调用与
//! 错误归一化，不内置任何业务规则——「published 才能执行」「定义校验」等
//! 全部留在服务端 single source of truth（DESIGN.md §9），CLI 多一份判断
//! 就会多一处会过时的副本。
//!
//! 参数用 `serde_json::Map` 动态拼装（方法面是开放的字符串表，没有编译期
//! 类型可用）；jsonrpsee 的 `ToRpcParams` 对 Map 有现成实现。

use jsonrpsee::core::client::ClientT;
use jsonrpsee::ws_client::WsClient;
use serde_json::{Map, Value};

use crate::error::CliError;

/// 连接服务。URL 归一化：`127.0.0.1:9800` 之类缺 scheme 的按 `ws://` 补全；
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
                "连不上 flow-server（{normalized}）：{err}\n提示：先 `cargo run --bin flow-server`，或用 --url / FLOW_RPC 指定地址"
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
    client
        .request::<Value, _>(method, params)
        .await
        .map_err(|err| match err {
            // 服务端回了 JSON-RPC 错误对象：code/message 原样透出，不吞码
            jsonrpsee::core::ClientError::Call(object) => CliError::Rpc {
                code: object.code(),
                message: object.message().to_string(),
            },
            other => CliError::local(format!("RPC 调用 {method} 失败：{other}")),
        })
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
