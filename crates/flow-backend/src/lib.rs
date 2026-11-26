//! V2 Journal coordinator, execution ports and product read models.
//!
//! 本 crate 是 master 侧的协调器：只做裁决与调度，绝不执行任何节点语义。
//! 它守护的核心不变量是「没有事实就没有决策」——每一次状态迁移都必须落成
//! 一条 journal 事件，并与当前状态、永久回执在同一个 SQLite 事务里提交。
//! 需要它，是因为节点语义必须待在受管理的执行器子进程里（DESIGN C2），
//! 而权威状态又必须只有一份（DESIGN C1）；协调器正是这两者之间唯一的裁决面，
//! 任何绕过它的写入都会撕裂「事实 → 状态」的对应关系。
pub use flow_domain::{Definition, NodeType, HTTP_METHODS};
pub use flow_dto::{RunSource, RunStatus};
pub use flow_engine::redact_value;
pub mod environments;
pub mod execution;
pub mod journal;
mod journal_commands;
pub mod observation;
pub mod plugin_config;
pub mod plugin_registry;
pub mod secrets;
pub mod secrets_store;
pub use journal_commands::{ProductCommand, WriteActor};
pub mod bundle;
mod execution_snapshot;
mod journal_driver;
mod journal_execution;
mod journal_history;
pub mod journal_import;
pub mod journal_permissions;
pub mod journal_views;
pub mod policy;
pub mod projection_check;
mod regression;
pub use flow_journal::query::Error as StorageError;
/// Read and write paths share the same typed error categories.
pub use flow_journal::Error as ViewError;

/// `[harness.agent]`（用户输入 schema）→ 冻结快照用的运行期形态。唯一的
/// 转换点：base_url 与 model 都非空才启用（成对校验在 flow-config 已把关，
/// 这里防御半配形态）。
///
/// 转换只在启动时发生一次：新 run 的快照由此生成，in-flight run 一律读
/// `run.execution.agent` 里冻结的值——重启后修改配置不得改变在飞 run 的
/// 执行身份，否则同一次运行的证据链会出现两套 agent 配置。
#[deprecated = "R3：走 plugin_config::harness_agent_plugin（manifest 三源合一 + 快照元数据）"]
pub fn harness_agent_run_config(
    config: &flow_config::HarnessAgentConfig,
) -> Option<flow_dto::AgentRunConfig> {
    crate::plugin_config::harness_agent_plugin(config)
        .expect("harness agent 配置解析失败")
        .map(|installed| installed.run_config)
}

/// A validated execution port whose coordinator has not yet started.
pub struct PreparedExecution {
    backend: std::sync::Arc<journal::JournalBackend>,
    port: PreparedPort,
}

enum PreparedPort {
    Ipc(execution::IpcOptions, bool),
    Remote(
        execution::remote::RemoteOptions,
        execution::remote::RemoteListeners,
    ),
}

impl PreparedExecution {
    /// Activate dispatch and recovery after the caller has accepted its work.
    pub async fn start(self) -> Result<(), journal::JournalError> {
        match self.port {
            PreparedPort::Ipc(options, prewarm) => {
                self.backend.activate_ipc(options, prewarm).await
            }
            PreparedPort::Remote(options, listeners) => {
                self.backend.activate_remote(options, listeners).await
            }
        }
    }
}

/// Validate the port and establish runtime configuration for run snapshots.
/// Remote sockets are bound here. Recovery, epoch allocation and accepting
/// remote agents begin only when the prepared port is started.
pub async fn prepare_execution(
    config: &flow_config::ExecutionConfig,
    harness_agent: Option<plugin_config::InstalledAgentPlugin>,
    backend: &std::sync::Arc<journal::JournalBackend>,
) -> Result<PreparedExecution, journal::JournalError> {
    let invalid = |message: String| flow_journal::Error::Invalid(message);
    let port = match config.mode {
        flow_config::ExecutionModeKind::InProcess => {
            return Err(invalid(execution::IN_PROCESS_REMOVED.into()).into());
        }
        flow_config::ExecutionModeKind::Ipc => {
            let execution::ExecutionMode::Ipc(options) = execution::mode_from_config(config)
                .map_err(|error| invalid(format!("execution.mode=ipc: {error}")))?;
            journal_driver::check_executor(&options)?;
            PreparedPort::Ipc(options, config.prewarm)
        }
        flow_config::ExecutionModeKind::Remote => {
            let options = execution::remote_options_from_config(config).map_err(invalid)?;
            let listeners = execution::remote::RemoteListeners::bind(&options).await?;
            PreparedPort::Remote(options, listeners)
        }
    };
    backend
        .install_harness_agent_plugin(harness_agent)
        .map_err(|error| invalid(error.to_owned()))?;
    let executor = match &port {
        PreparedPort::Ipc(options, _) => Some(options.executor.program.clone()),
        PreparedPort::Remote(..) => None,
    };
    *backend
        .plugin_executor_program
        .write()
        .unwrap_or_else(|e| e.into_inner()) = executor;
    Ok(PreparedExecution {
        backend: backend.clone(),
        port,
    })
}

/// Prepare and immediately start the execution port for service entry points.
pub async fn start_execution(
    execution: &flow_config::ExecutionConfig,
    harness_agent: Option<plugin_config::InstalledAgentPlugin>,
    backend: &std::sync::Arc<journal::JournalBackend>,
) -> Result<(), journal::JournalError> {
    prepare_execution(execution, harness_agent, backend)
        .await?
        .start()
        .await
}

pub mod resident_service;

pub mod service_host;

pub use flow_plugin_catalog as plugin_scan;

pub mod plugin_bundle;

pub mod plugin_operation;

pub mod plugin_lifecycle;
