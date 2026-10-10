//! V2 Journal coordinator, execution ports and product read models.
pub use flow_dto::{RunSource, RunStatus};
pub use flow_engine::{redact_value, secrets, Definition, NodeType, HTTP_METHODS};
use thiserror::Error;
pub mod execution;
pub mod journal;
mod journal_commands;
mod journal_driver;
mod journal_execution;
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
        flow_config::ExecutionModeKind::InProcess => backend.start_execution().await,
        flow_config::ExecutionModeKind::Ipc => {
            let options = match execution::mode_from_config(execution) {
                Ok(execution::ExecutionMode::Ipc(options)) => options,
                Ok(execution::ExecutionMode::InProcess) => {
                    return Err(invalid(
                        "execution.mode=ipc: internal parse mismatch".into(),
                    ))
                }
                Err(error) => return Err(invalid(format!("execution.mode=ipc: {error}"))),
            };
            backend
                .start_execution_ipc(execution::ExecutionMode::Ipc(options))
                .await
        }
        flow_config::ExecutionModeKind::Remote => {
            let options = execution::remote_options_from_config(execution).map_err(invalid)?;
            backend.start_execution_remote(options).await
        }
    }
}
