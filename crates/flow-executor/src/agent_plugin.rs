//! harness agent 插件适配层：spawn `flow-agent-plugin` 子进程并驱动一次回合。
//!
//! 这是执行器与 agent 运行时的**唯一**边界：执行器不再链接任何 agent
//! 运行时代码（原进程内 headless 路径已删），改经 proc-ipc 字节通道 +
//! plugin-proto JSON-RPC 与插件进程对话（方法层见 agent-plugin crate）。
//!
//! 守护的不变量：
//! 1. **错误语义与原内联路径逐条对齐**：装配/环境类失败（二进制缺失、
//!    spawn/握手失败、插件回 `-32010`）= 确定性失败（Business）；回合
//!    类失败（`-32011`、通道中断）= 不确定失败（Uncertain）。
//! 2. **插件与执行器同进程组**（`new_session=false`）：master 按会话
//!    击杀执行器时，插件及其工具子进程一并回收，不留孤儿。
//! 3. **凭据只经通道进插件**：api_key 走请求参数；插件进程环境是白名单
//!    快照（与执行器自身的收敛规则同源），CONGA_*/FLOW_SECRET_* 不进入。
//! 4. **事件只带元信息**：`agent/event` 通知的投影在插件侧完成（隐私
//!    边界见 agent-plugin crate），本层只转发给进度投影器。

use std::path::PathBuf;
use std::time::Duration;

use agent_plugin::{RunTurnParams, TurnOutcome, TurnSpec, WireEvent, ASSEMBLY_ERROR};
use flow_engine::execution_protocol::environment::EXECUTOR_ENV_WHITELIST;
use plugin_proto::envelope::{read_frame, write_frame, Frame, FrameKind};
use plugin_proto::handshake::{
    check_version, initialize_request, verify_session_token, InitializeParams, PROTOCOL_VERSION,
};
use plugin_proto::jsonrpc::{Id, Message, Request};
use proc_ipc::{Session, SpawnConfig};

/// 插件二进制名（执行器同目录兄弟定位的缺省约定）。
const PLUGIN_BIN_NAME: &str = "flow-agent-plugin";
/// 握手超时：覆盖插件 exec + 初始化 + 一次往返。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// 适配层错误：两类失败的上游处置不同（见模块文档第 1 条）。
#[derive(Debug)]
pub(super) enum AgentPluginError {
    /// 确定性失败：环境问题（二进制缺失/权限/握手失败/装配失败）。
    /// 重试无意义，按 Business 收口。
    Assembly(String),
    /// 不确定失败：回合中断/失败。按 Uncertain 收口，恢复语义由上层决定。
    Turn(String),
}

/// 定位插件二进制：`FLOW_AGENT_PLUGIN_BIN` 显式指定，否则执行器同目录
/// 兄弟（部署形态把两个二进制放一起；e2e 共享同一 target 目录）。
fn locate_plugin() -> Result<PathBuf, AgentPluginError> {
    if let Some(path) = std::env::var_os("FLOW_AGENT_PLUGIN_BIN") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(AgentPluginError::Assembly(format!(
            "FLOW_AGENT_PLUGIN_BIN points to a missing file: {}",
            path.display()
        )));
    }
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(PLUGIN_BIN_NAME)))
        .filter(|path| path.is_file())
        .ok_or_else(|| {
            AgentPluginError::Assembly(format!(
                "harness agent plugin binary '{PLUGIN_BIN_NAME}' not found next to the executor; \
                 set FLOW_AGENT_PLUGIN_BIN to locate it"
            ))
        })?;
    Ok(sibling)
}

/// 白名单环境快照 + RUST_LOG：与执行器自身的环境收敛规则同源（单一
/// 来源在 flow-engine 的 environment 模块），插件与它的工具进程需要
/// 常规工具链环境（PATH/HOME/…），凭据类变量不进入。
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

/// 驱动一次 agent 回合：spawn 插件 → 握手 → `agent/runTurn` → 事件泵 →
/// 摘要。事件经 `on_event` 实时交给进度投影器。
pub(super) async fn run_agent_turn(
    config: &flow_engine::AgentRunConfig,
    prompt: &str,
    api_key: &str,
    workdir: &std::path::Path,
    roots: &std::path::Path,
    mut on_event: impl FnMut(WireEvent),
) -> Result<TurnOutcome, AgentPluginError> {
    let program = locate_plugin()?;
    let mut spawn_config = SpawnConfig::new(&program);
    spawn_config.env = plugin_env();
    // 同进程组：master 按会话击杀执行器时连带回收插件与工具子进程。
    spawn_config.new_session = false;
    let mut session = Session::spawn(spawn_config)
        .await
        .map_err(|error| AgentPluginError::Assembly(format!("plugin spawn failed: {error}")))?;

    // --- initialize 握手（超时按装配失败：环境问题，重试无意义）---
    let params = InitializeParams {
        protocol_version: PROTOCOL_VERSION,
        capabilities: vec![],
        client_info: serde_json::json!({"name": "flow-executor"}),
    };
    let initialize = Message::Request(initialize_request(Id::Number(1), &params));
    let handshake = async {
        write_rpc(&mut session, &initialize).await?;
        let frame = read_frame(session.channel_mut()).await?;
        let message = frame
            .as_rpc()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        match message {
            Message::Response(response) => Ok(response),
            _ => Err(std::io::Error::other("initialize must get a response")),
        }
    };
    let response = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake)
        .await
        .map_err(|_| AgentPluginError::Assembly("plugin handshake timeout".into()))?
        .map_err(assembly_io)?;
    let result = match (response.id.clone(), response.error) {
        (Id::Number(1), None) => response.result,
        (_, Some(error)) => {
            let _ = session.shutdown().await;
            return Err(AgentPluginError::Assembly(format!(
                "plugin handshake rejected: {}",
                error.message
            )));
        }
        _ => {
            let _ = session.shutdown().await;
            return Err(AgentPluginError::Assembly(
                "plugin handshake id mismatch".into(),
            ));
        }
    };
    let result: plugin_proto::handshake::InitializeResult = serde_json::from_value(
        result.ok_or_else(|| AgentPluginError::Assembly("handshake result missing".into()))?,
    )
    .map_err(|error| AgentPluginError::Assembly(format!("handshake result malformed: {error}")))?;
    check_version(result.protocol_version)
        .map_err(AgentPluginError::Assembly)?;
    // unix fd 承载双方均无令牌；回连承载的令牌回验在这里把关。
    verify_session_token(None, &result)
        .map_err(|error| AgentPluginError::Assembly(error.message))?;

    // --- runTurn：事件通知 → on_event；响应 → TurnOutcome ---
    let params = RunTurnParams {
        spec: TurnSpec {
            base_url: config.base_url.clone(),
            model: config.model.clone(),
            api: config.api.clone(),
            proxy: config.proxy.clone(),
            max_turns: config.max_turns.map(|value| value as usize),
            max_tool_calls_per_turn: config.max_tool_calls_per_turn.map(|value| value as usize),
            max_tokens: config.max_tokens.map(|value| value as usize),
            tool_timeout_secs: config.tool_timeout_secs,
        },
        api_key: api_key.to_string(),
        prompt: prompt.to_string(),
        workdir: workdir.display().to_string(),
        roots: roots.display().to_string(),
    };
    let request = Message::Request(Request {
        jsonrpc: "2.0",
        id: Id::Number(2),
        method: "agent/runTurn".into(),
        params: Some(serde_json::to_value(&params).expect("RunTurnParams serializable")),
    });
    write_rpc(&mut session, &request).await.map_err(turn_io)?;
    let outcome = loop {
        let frame = read_frame(session.channel_mut()).await.map_err(turn_io)?;
        let message = frame.as_rpc().map_err(turn_proto)?;
        match message {
            Message::Notification(notification) if notification.method == "agent/event" => {
                if let Some(event) = notification
                    .params
                    .and_then(|params| serde_json::from_value::<WireEvent>(params).ok())
                {
                    on_event(event);
                }
            }
            Message::Response(response) if matches!(response.id, Id::Number(2)) => {
                if let Some(error) = response.error {
                    let message = if error.code
                        == plugin_proto::jsonrpc::ErrorCode::Application(ASSEMBLY_ERROR)
                    {
                        AgentPluginError::Assembly(error.message)
                    } else {
                        AgentPluginError::Turn(error.message)
                    };
                    break Err(message);
                }
                let outcome = response
                    .result
                    .and_then(|result| serde_json::from_value::<TurnOutcome>(result).ok());
                match outcome {
                    Some(outcome) => break Ok(outcome),
                    None => {
                        break Err(AgentPluginError::Turn(
                            "plugin returned a malformed turn outcome".into(),
                        ))
                    }
                }
            }
            // 其余消息（迟到通知/响应对不上号）：忽略，回合以 id=2 的响应收口。
            _ => {}
        }
    };
    let _ = session.shutdown().await;
    outcome
}

fn assembly_io(error: std::io::Error) -> AgentPluginError {
    AgentPluginError::Assembly(format!("plugin handshake io failed: {error}"))
}

fn turn_io(error: std::io::Error) -> AgentPluginError {
    AgentPluginError::Turn(format!("plugin channel failed: {error}"))
}

fn turn_proto(error: plugin_proto::ProtoError) -> AgentPluginError {
    AgentPluginError::Turn(format!("plugin protocol failed: {error}"))
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
