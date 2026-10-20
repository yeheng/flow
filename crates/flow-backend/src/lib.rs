//! V2 Journal coordinator, execution ports and product read models.
pub use flow_dto::{RunSource, RunStatus};
pub use flow_engine::{redact_value, secrets, Definition, NodeType, HTTP_METHODS};
use thiserror::Error;
pub mod execution;
pub mod journal;
#[cfg(feature = "journal-cluster")]
pub mod journal_cluster;
mod journal_commands;
pub use journal_commands::ProductCommand;
pub mod bundle;
mod execution_snapshot;
mod journal_driver;
mod journal_execution;
mod journal_history;
pub mod journal_import;
pub mod journal_views;
pub mod projection_check;
mod regression;
#[derive(Debug, Error)]
pub enum ViewError {
    #[error("工作流不存在：{0}")]
    WorkflowNotFound(String),
    #[error("版本不存在：{0} v{1}")]
    VersionNotFound(String, i64),
    #[error("版本尚未发布：{0} v{1}")]
    VersionNotPublished(String, i64),
    #[error("run 不存在：{0}")]
    RunNotFound(String),
    #[error("signal 不存在：{0}")]
    SignalNotFound(String),
    #[error("schedule 不存在：{0}")]
    ScheduleNotFound(String),
    #[error("webhook 不存在：{0}")]
    WebhookNotFound(String),
    #[error("节点模板不存在：{0}")]
    TemplateNotFound(String),
    #[error("模板名已存在：{0}")]
    TemplateNameTaken(String),
    #[error("参数非法：{0}")]
    Invalid(String),
    #[error("冲突：{0}")]
    Conflict(String),
    #[error("内部错误：{0}")]
    Internal(String),
}

/// `[harness.agent]`（用户输入 schema）→ 冻结快照用的运行期形态。唯一的
/// 转换点：base_url 与 model 都非空才启用（成对校验在 flow-config 已把关，
/// 这里防御半配形态）。
pub fn harness_agent_run_config(
    config: &flow_config::HarnessAgentConfig,
) -> Option<flow_dto::AgentRunConfig> {
    let base_url = config
        .base_url
        .as_deref()
        .filter(|value| !value.trim().is_empty())?;
    let model = config
        .model
        .as_deref()
        .filter(|value| !value.trim().is_empty())?;
    Some(flow_dto::AgentRunConfig {
        base_url: base_url.to_string(),
        model: model.to_string(),
        api: config
            .api
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("openai")
            .to_string(),
        proxy: config.proxy.clone(),
        max_turns: config.max_turns,
        max_tool_calls_per_turn: config.max_tool_calls_per_turn,
        max_tokens: config.max_tokens,
        tool_timeout_secs: config.tool_timeout_secs,
    })
}

pub async fn start_execution(
    execution: &flow_config::ExecutionConfig,
    harness_agent: Option<flow_dto::AgentRunConfig>,
    backend: &std::sync::Arc<crate::journal::JournalBackend>,
) -> Result<(), crate::journal::JournalError> {
    let invalid = |message: String| {
        crate::journal::JournalError::Journal(flow_journal::Error::Invalid(message))
    };
    // [harness.agent] 启动时安装一次：新 run 快照的唯一来源；派发时从
    // run.execution 读冻结值。配置在重启后修改只影响新 run。
    backend
        .install_harness_agent(harness_agent)
        .map_err(|message| invalid(message.to_string()))?;
    match execution.mode {
        // 进程内执行路径已全平台删除（节点执行语义只在 flow-executor）；
        // 配置期就明确拒绝，不静默改走别的模式。
        flow_config::ExecutionModeKind::InProcess => {
            Err(invalid(execution::IN_PROCESS_REMOVED.into()))
        }
        flow_config::ExecutionModeKind::Ipc => {
            let options = match execution::mode_from_config(execution) {
                Ok(execution::ExecutionMode::Ipc(options)) => options,
                Err(error) => return Err(invalid(format!("execution.mode=ipc: {error}"))),
            };
            backend
                .start_execution_ipc_prewarm(
                    execution::ExecutionMode::Ipc(options),
                    execution.prewarm,
                )
                .await
        }
        flow_config::ExecutionModeKind::Remote => {
            let options = execution::remote_options_from_config(execution).map_err(invalid)?;
            backend.start_execution_remote(options).await
        }
    }
}
