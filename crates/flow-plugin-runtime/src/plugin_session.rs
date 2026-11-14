//! 通用插件会话层：spawn → initialize 握手 → invoke → 响应/通知/二进制帧。
//!
//! 从 agent_plugin 适配层抽取的公共底座（R4）：握手、版本门、令牌回验、
//! 请求-响应往返、通知转发、二进制块收集。**节点语义的适配层
//!（agent_plugin / llm 路径）建在这一层之上**，不各自手搓会话。
//!
//! 守护的不变量（与 agent_plugin 原实现逐条一致）：
//!
//! 1. **插件与执行器同进程组**（`new_session=false`）：master 按会话击杀
//!    执行器时，插件及其子进程一并回收。
//! 2. **环境是白名单快照 + RUST_LOG**：与执行器自身的收敛规则同源
//!    （单一来源在 flow-engine 的 environment 模块），凭据类变量不进入；
//!    秘密只经通道请求参数过线。
//! 3. **会话错误二分**：spawn/握手失败 = 环境问题（确定性）；invoke 期间
//!    的通道/协议失败 = 不确定（对端可能已在做副作用）。
//! 4. **每会话单次 invoke**：与派发生命周期 1:1；池化复用是未来的事，
//!    到时候 config 从 initialize 下发（设计 §3.4）。

use std::time::Duration;

use flow_engine::execution_protocol::environment::EXECUTOR_ENV_WHITELIST;
use flow_plugin_sdk::ipc::{Session, SpawnConfig};
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame, Frame, FrameKind};
use flow_plugin_sdk::protocol::handshake::{
    check_version, initialize_request, verify_session_token, InitializeParams, PROTOCOL_VERSION,
};
use flow_plugin_sdk::protocol::jsonrpc::{Error, Id, Message, Request};
use futures::future::BoxFuture;

use crate::plugin_registry::RegisteredPlugin;

/// 握手超时：覆盖插件 exec + 初始化 + 一次往返。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// 会话错误：二分语义见模块文档第 3 条。
#[derive(Debug)]
pub(super) enum PluginSessionError {
    /// 环境问题（二进制缺失/spawn/握手失败）：确定性失败，重试无意义。
    Setup(String),
    /// invoke 期间失败（通道/协议/超时）：不确定——副作用可能已发生。
    Invoke(String),
}

/// 一次 invoke 的完整产物：JSON-RPC 响应 + 先于响应到达的二进制块
///（按到达序拼接；长度/摘要校验由调用方按其方法层契约执行）。
pub(super) struct InvokeOutcome {
    /// 响应 id 对应的 result（None = error 响应）。
    pub result: Option<serde_json::Value>,
    /// error 响应（None = 成功）。
    pub error: Option<Error>,
    /// 响应前收到的二进制块拼接（llm egress 路径消费；agent 路径恒空）。
    #[allow(dead_code)] // R4 llm_node 落地前暂未读取
    pub binary: Vec<u8>,
}

/// 拉起插件并完成握手，返回就绪会话。
///
/// 握手含版本门 + 令牌回验（wire.md §3）与 `binary_blocks` 能力协商。
pub(super) async fn open(
    plugin: &RegisteredPlugin,
    require_binary_blocks: bool,
) -> Result<Session, PluginSessionError> {
    let root = match &plugin.source {
        crate::plugin_registry::RegistrationSource::Manifest(path) => path
            .parent()
            .and_then(|p| p.parent())
            .map(std::path::Path::to_path_buf),
        crate::plugin_registry::RegistrationSource::Default => None,
    };
    let publication_lease = if let Some(root) = root {
        Some(
            tokio::task::spawn_blocking(move || flow_config::plugin_fs::RootLease::shared(&root))
                .await
                .map_err(|e| PluginSessionError::Setup(e.to_string()))?
                .map_err(|e| {
                    PluginSessionError::Setup(format!("plugin publication lease failed: {e}"))
                })?,
        )
    } else {
        None
    };
    if !plugin.binary.is_file() {
        return Err(PluginSessionError::Setup(format!(
            "plugin binary missing at dispatch: {}",
            plugin.binary.display()
        )));
    }
    // I35：spawn 前复核内容哈希——注册与执行之间二进制/manifest 被换掉
    //（升级、篡改）时拒绝执行，而不是拿旧授予跑新代码。
    if let Err(reason) = plugin.recheck_content_hash() {
        return Err(PluginSessionError::Setup(reason));
    }
    let mut config = SpawnConfig::new(&plugin.binary);
    config.env = plugin_env();
    // 同进程组：master 按会话击杀执行器时连带回收插件子树，不留孤儿。
    config.new_session = false;
    let mut session = Session::spawn(config)
        .await
        .map_err(|error| PluginSessionError::Setup(format!("plugin spawn failed: {error}")))?;

    drop(publication_lease);

    let mut capabilities = vec![flow_plugin_sdk::protocol::handshake::Capability::MasterPermitV1];
    if require_binary_blocks {
        capabilities.push(flow_plugin_sdk::protocol::handshake::Capability::BinaryBlocks);
    }
    let params = InitializeParams {
        protocol_version: PROTOCOL_VERSION,
        capabilities,
        client_info: serde_json::json!({"name": "flow-executor"}),
    };
    let initialize = Message::Request(initialize_request(Id::Number(1), &params));
    write_rpc(&mut session, &initialize)
        .await
        .map_err(|error| {
            PluginSessionError::Setup(format!("plugin handshake io failed: {error}"))
        })?;
    let frame = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_frame(session.channel_mut()))
        .await
        .map_err(|_| PluginSessionError::Setup("plugin handshake timeout".into()))?
        .map_err(|error| PluginSessionError::Setup(format!("plugin handshake io failed: {error}")))?
        .as_rpc()
        .map_err(|error| {
            PluginSessionError::Setup(format!("plugin handshake protocol failed: {error}"))
        })?;
    let response = match frame {
        Message::Response(response) => response,
        _ => {
            return Err(PluginSessionError::Setup(
                "plugin handshake must get a response".into(),
            ))
        }
    };
    if let Some(error) = response.error {
        let _ = session.shutdown().await;
        return Err(PluginSessionError::Setup(format!(
            "plugin handshake rejected: {}",
            error.message
        )));
    }
    let result: flow_plugin_sdk::protocol::handshake::InitializeResult = serde_json::from_value(
        response
            .result
            .ok_or_else(|| PluginSessionError::Setup("handshake result missing".into()))?,
    )
    .map_err(|error| PluginSessionError::Setup(format!("handshake result malformed: {error}")))?;
    check_version(result.protocol_version).map_err(PluginSessionError::Setup)?;
    // unix fd 承载双方均无令牌；回连承载的令牌回验在这里把关。
    verify_session_token(None, &result)
        .map_err(|error| PluginSessionError::Setup(error.message))?;
    if require_binary_blocks
        && !result
            .capabilities
            .contains(&flow_plugin_sdk::protocol::handshake::Capability::BinaryBlocks)
    {
        let _ = session.shutdown().await;
        return Err(PluginSessionError::Setup(
            "plugin lacks binary_blocks capability required by this invoke".into(),
        ));
    }
    Ok(session)
}

/// 发起一次 invoke：请求 → 泵通知（逐条转发给 `on_notification`）与
/// 二进制块（收集拼接）→ 等 id 匹配的响应。
///
/// 通知泵不结束于响应：响应到达即返回，迟到的通知被丢弃（会话随派发
/// 关闭）。调用方对 binary 的校验（长度/摘要）按各自方法层契约执行。
pub(super) async fn invoke(
    session: &mut Session,
    method: &str,
    params: serde_json::Value,
    on_notification: impl FnMut(&flow_plugin_sdk::protocol::jsonrpc::Notification),
) -> Result<InvokeOutcome, PluginSessionError> {
    invoke_with_reverse(session, method, params, on_notification, None).await
}

/// 带反向请求处理的 invoke（09 §8 G5）：插件在回合内发起的
/// `permission/request` 等请求交给 `on_request` 裁决并回响应；未提供
/// 处理器时维持旧行为（忽略——旧插件不发请求）。
#[allow(clippy::type_complexity)]
pub(super) async fn invoke_with_reverse<'a>(
    session: &mut Session,
    method: &str,
    params: serde_json::Value,
    mut on_notification: impl FnMut(&flow_plugin_sdk::protocol::jsonrpc::Notification),
    mut on_request: Option<
        Box<
            dyn FnMut(flow_plugin_sdk::protocol::jsonrpc::Request) -> BoxFuture<'static, Message>
                + Send
                + 'a,
        >,
    >,
) -> Result<InvokeOutcome, PluginSessionError> {
    let request = Message::Request(Request {
        jsonrpc: "2.0",
        id: Id::Number(2),
        method: method.to_string(),
        params: Some(params),
    });
    write_rpc(session, &request)
        .await
        .map_err(|error| PluginSessionError::Invoke(format!("plugin channel failed: {error}")))?;
    let mut binary = Vec::new();
    loop {
        let frame = read_frame(session.channel_mut()).await.map_err(|error| {
            PluginSessionError::Invoke(format!("plugin channel failed: {error}"))
        })?;
        match frame.kind {
            FrameKind::Rpc => {
                let message = frame.as_rpc().map_err(|error| {
                    PluginSessionError::Invoke(format!("plugin protocol failed: {error}"))
                })?;
                match message {
                    Message::Notification(notification) => on_notification(&notification),
                    Message::Response(response) if matches!(response.id, Id::Number(2)) => {
                        return Ok(InvokeOutcome {
                            result: response.result,
                            error: response.error,
                            binary,
                        });
                    }
                    // 反向请求（permission/request 等）：交给宿主裁决并回响应。
                    Message::Request(plugin_request) => {
                        let Some(handler) = on_request.as_mut() else {
                            continue;
                        };
                        let id = plugin_request.id.clone();
                        let response = handler(plugin_request).await;
                        // 响应回写与帧读取串行（同一 channel_mut 借用）。
                        if write_rpc(session, &response).await.is_err() {
                            return Err(PluginSessionError::Invoke(
                                "plugin channel failed writing reverse response".into(),
                            ));
                        }
                        let _ = id;
                    }
                    // 其余消息（迟到响应）：忽略。
                    _ => {}
                }
            }
            FrameKind::Binary => binary.extend_from_slice(&frame.payload),
        }
    }
}

/// 白名单环境快照 + RUST_LOG（见模块文档第 2 条）。
fn plugin_env() -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut env = Vec::new();
    for name in EXECUTOR_ENV_WHITELIST {
        if let Some(value) = std::env::var_os(name) {
            env.push((std::ffi::OsString::from(*name), value));
        }
    }
    if let Some(value) = std::env::var_os("RUST_LOG") {
        env.push(("RUST_LOG".into(), value));
    }
    env
}

async fn write_rpc(session: &mut Session, message: &Message) -> Result<(), std::io::Error> {
    let frame = Frame {
        kind: FrameKind::Rpc,
        payload: message
            .to_vec()
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    };
    write_frame(session.channel_mut(), &frame).await
}
