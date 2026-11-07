//! harness agent 插件适配层：spawn `flow-agent-plugin` 子进程并驱动一次回合。
//!
//! 这是执行器与 agent 运行时的**唯一**边界：执行器不再链接任何 agent
//! 运行时代码（原进程内 headless 路径已删），改经 proc-ipc 字节通道 +
//! plugin-proto JSON-RPC 与插件进程对话（方法层见 agent-plugin crate）。
//! 会话底座（握手/往返/通知泵）在 [`crate::plugin_session`]，本层只做
//! agent 语义的参数组装与事件转发。
//!
//! 守护的不变量：
//! 1. **错误语义与原内联路径逐条对齐**：装配/环境类失败（二进制缺失、
//!    spawn/握手失败、插件回 `-32010`）= 确定性失败（Business）；回合
//!    类失败（`-32011`、通道中断）= 不确定失败（Uncertain）。
//! 2. 插件与执行器同进程组 / 环境白名单 / 凭据只走通道：见
//!    [`crate::plugin_session`] 模块文档（公共底座统一守护）。
//! 3. **事件只带元信息**：`agent/event` 通知的投影在插件侧完成（隐私
//!    边界见 agent-plugin crate），本层只转发给进度投影器。

use flow_harness_protocol::{RunTurnParams, TurnOutcome, TurnSpec, WireEvent, ASSEMBLY_ERROR};
use plugin_proto::jsonrpc::ErrorCode;

use crate::plugin_registry::RegisteredPlugin;
use crate::plugin_session::{self, PluginSessionError};

/// 适配层错误：两类失败的上游处置不同（见模块文档第 1 条）。
#[derive(Debug)]
pub(super) enum AgentPluginError {
    /// 确定性失败：环境问题（二进制缺失/权限/握手失败/装配失败）。
    /// 重试无意义，按 Business 收口。
    Assembly(String),
    /// 不确定失败：回合中断/失败。按 Uncertain 收口，恢复语义由上层决定。
    Turn(String),
}

impl From<PluginSessionError> for AgentPluginError {
    fn from(error: PluginSessionError) -> Self {
        match error {
            PluginSessionError::Setup(message) => Self::Assembly(message),
            PluginSessionError::Invoke(message) => Self::Turn(message),
        }
    }
}

/// 一次回合的上下文：调用方只组一个结构，不散参数。
pub(super) struct TurnRequest<'a> {
    pub(super) plugin: &'a RegisteredPlugin,
    pub(super) config: &'a flow_engine::AgentRunConfig,
    pub(super) prompt: &'a str,
    pub(super) api_key: &'a str,
    pub(super) workdir: &'a std::path::Path,
    pub(super) roots: &'a std::path::Path,
}

/// 驱动一次 agent 回合：spawn 插件 → 握手 → `agent/runTurn` → 事件泵 →
/// 摘要。事件经 `on_event` 实时交给进度投影器。
pub(super) async fn run_agent_turn(
    request: TurnRequest<'_>,
    mut on_event: impl FnMut(WireEvent),
) -> Result<TurnOutcome, AgentPluginError> {
    let TurnRequest {
        plugin,
        config,
        prompt,
        api_key,
        workdir,
        roots,
    } = request;
    let mut session = plugin_session::open(plugin, false).await?;

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
    let outcome = plugin_session::invoke(
        &mut session,
        &plugin.invoke_method,
        serde_json::to_value(&params).expect("RunTurnParams serializable"),
        |notification| {
            if notification.method == "agent/event" {
                if let Some(event) = notification
                    .params
                    .as_ref()
                    .and_then(|params| serde_json::from_value::<WireEvent>(params.clone()).ok())
                {
                    on_event(event);
                }
            }
        },
    )
    .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = session.shutdown().await;
            return Err(error.into());
        }
    };
    let _ = session.shutdown().await;
    match outcome.error {
        None => {}
        Some(error) => {
            return Err(match error.code {
                ErrorCode::Application(code) if code == ASSEMBLY_ERROR => {
                    AgentPluginError::Assembly(error.message)
                }
                _ => AgentPluginError::Turn(error.message),
            });
        }
    }
    outcome
        .result
        .and_then(|result| serde_json::from_value::<TurnOutcome>(result).ok())
        .map_or_else(
            || {
                Err(AgentPluginError::Turn(
                    "plugin returned a malformed turn outcome".into(),
                ))
            },
            Ok,
        )
}
