//! V2 Journal coordinator, execution ports and product read models.
//!
//! 本 crate 是 master 侧的协调器：只做裁决与调度，绝不执行任何节点语义。
//! 它守护的核心不变量是「没有事实就没有决策」——每一次状态迁移都必须落成
//! 一条 journal 事件，并与当前状态、永久回执在同一个 SQLite 事务里提交。
//! 需要它，是因为节点语义必须待在受管理的执行器子进程里（DESIGN C2），
//! 而权威状态又必须只有一份（DESIGN C1）；协调器正是这两者之间唯一的裁决面，
//! 任何绕过它的写入都会撕裂「事实 → 状态」的对应关系。
pub use flow_dto::{RunSource, RunStatus};
pub use flow_engine::{redact_value, secrets, Definition, NodeType, HTTP_METHODS};
pub mod execution;
pub mod journal;
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
/// Read and write paths share the same typed error categories.
pub use flow_journal::Error as ViewError;
pub use flow_store::store::Error as StorageError;

/// `[harness.agent]`（用户输入 schema）→ 冻结快照用的运行期形态。唯一的
/// 转换点：base_url 与 model 都非空才启用（成对校验在 flow-config 已把关，
/// 这里防御半配形态）。
///
/// 转换只在启动时发生一次：新 run 的快照由此生成，in-flight run 一律读
/// `run.execution.agent` 里冻结的值——重启后修改配置不得改变在飞 run 的
/// 执行身份，否则同一次运行的证据链会出现两套 agent 配置。
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
    // api 缺省为 openai：供应商协议不在这里推断，写死默认值优于让请求
    // 因空 api 字段在派发时才失败。
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

/// 按配置选择执行端口（本地 IPC 子进程池或远程 agent）并启动协调循环。
///
/// 先安装 `[harness.agent]` 快照来源，再按 `execution.mode` 建立端口；
/// `InProcess` 模式在此显式拒绝——进程内执行路径已全平台删除，静默改走别的
/// 模式会让用户以为节点语义仍受进程隔离保护（C2）。
///
/// # Errors
/// 执行器二进制缺失、配置非法、端口监听失败或 harness agent 配置重复安装时返回错误。
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
