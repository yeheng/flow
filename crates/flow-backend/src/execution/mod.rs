//! 执行模式与 IPC 选项（二期 §5，I09）。
//!
//! 基础（进程内）与 IPC 执行模式显式配置；同一数据集/进程内一个 run 只
//! 绑定一种执行模式。启用 IPC 时二进制缺失/版本不兼容直接失败，不静默
//! 落回进程内执行。

pub mod dispatch;
pub mod pool;
pub mod remote;

use std::path::PathBuf;

use flow_engine::execution_protocol::contract::{
    ExecutorInvocation, EXECUTION_MODE_ENV, X_MAX_DEFAULT,
};

pub use dispatch::IpcDispatcher;
pub use pool::ExecutorPool;

#[derive(Debug, Clone)]
pub enum ExecutionMode {
    /// 一期进程内执行（默认）。
    InProcess,
    /// 二期本地 IPC 执行子进程。
    Ipc(IpcOptions),
}

#[derive(Debug, Clone)]
pub struct IpcOptions {
    /// 执行器召唤方式：同目录兄弟 flow-executor（缺省定位）或显式二进制。
    pub executor: ExecutorInvocation,
    /// 执行槽位上限 X_max（1..=16）。
    pub x_max: usize,
    /// 附加到执行器命令行的诊断标记（执行器忽略未知参数）；用于进程
    /// 归属诊断与测试隔离。
    pub tag: Option<String>,
}

/// 执行器定位（I09）：[`ExecutorInvocation::locate`]——FLOW_EXECUTOR_BIN
/// 显式路径优先，否则当前可执行文件同目录的兄弟 `flow-executor`。
/// 找不到即报错，不回退。
pub fn locate_executor() -> Result<ExecutorInvocation, String> {
    ExecutorInvocation::locate()
}

/// 从环境变量解析执行模式（测试可注入变量值）。
pub fn mode_from_env() -> Result<ExecutionMode, String> {
    let value = std::env::var(EXECUTION_MODE_ENV).unwrap_or_default();
    mode_from_env_with(&value)
}

/// 从统一配置解析执行模式（产品入口；env 已由 flow-config 合并进配置值）。
pub fn mode_from_config(config: &flow_config::ExecutionConfig) -> Result<ExecutionMode, String> {
    match config.mode {
        flow_config::ExecutionModeKind::InProcess => Ok(ExecutionMode::InProcess),
        flow_config::ExecutionModeKind::Ipc => {
            let executor = match &config.executor_bin {
                Some(bin) => Ok(ExecutorInvocation::explicit(bin.clone())),
                None => ExecutorInvocation::locate(),
            }?;
            Ok(ExecutionMode::Ipc(IpcOptions {
                executor,
                x_max: config.x_max as usize,
                tag: None,
            }))
        }
        flow_config::ExecutionModeKind::Remote => Err(
            "execution.mode=remote requires [execution.remote] (use remote_options_from_config)"
                .into(),
        ),
    }
}

/// 远程模式环境变量（R1/R3 运维入口）。
pub struct RemoteEnv {
    pub control_addr: String,
    pub data_addr: String,
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub attach_timeout_ms: u64,
}

/// 从统一配置解析远程模式选项（mode=remote 时 [execution.remote] 必填，
/// validate 已把关，这里是取值转换）。
pub fn remote_options_from_config(
    config: &flow_config::ExecutionConfig,
) -> Result<crate::execution::remote::RemoteOptions, String> {
    let remote = config
        .remote
        .as_ref()
        .ok_or("execution.mode=remote requires [execution.remote]")?;
    Ok(crate::execution::remote::RemoteOptions {
        control_addr: remote.control_addr.clone(),
        data_addr: remote.data_addr.clone(),
        ca_cert: remote.ca_cert.clone(),
        server_cert: remote.cert.clone(),
        server_key: remote.key.clone(),
        attach_timeout_ms: remote.attach_timeout_ms,
    })
}

pub fn mode_from_env_with(value: &str) -> Result<ExecutionMode, String> {
    match value {
        "" | "in_process" => Ok(ExecutionMode::InProcess),
        "ipc" => {
            let executor = locate_executor()?;
            Ok(ExecutionMode::Ipc(IpcOptions {
                executor,
                x_max: X_MAX_DEFAULT,
                tag: None,
            }))
        }
        other => Err(format!(
            "invalid {EXECUTION_MODE_ENV}={other:?}; expected in_process|ipc"
        )),
    }
}
