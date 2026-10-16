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
mod journal_driver;
mod journal_execution;
mod journal_history;
pub mod journal_import;
pub mod journal_views;
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

pub async fn start_execution(
    execution: &flow_config::ExecutionConfig,
    backend: &std::sync::Arc<crate::journal::JournalBackend>,
) -> Result<(), crate::journal::JournalError> {
    let invalid = |message: String| {
        crate::journal::JournalError::Journal(flow_journal::Error::Invalid(message))
    };
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
