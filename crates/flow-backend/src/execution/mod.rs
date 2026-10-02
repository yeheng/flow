//! 执行模式与 IPC 选项（二期 §5，I09）。
//!
//! 基础（进程内）与 IPC 执行模式显式配置；同一数据集/进程内一个 run 只
//! 绑定一种执行模式。启用 IPC 时二进制缺失/版本不兼容直接失败，不静默
//! 落回进程内执行。

pub mod dispatch;
pub mod remote;
pub mod pool;

use std::path::{Path, PathBuf};

use flow_engine::execution_protocol::contract::{
    EXECUTION_MODE_ENV, EXECUTOR_BIN_ENV, EXECUTOR_BIN_NAME, X_MAX_DEFAULT,
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
    /// 执行器二进制路径。
    pub executor_bin: PathBuf,
    /// 执行槽位上限 X_max（1..=16）。
    pub x_max: usize,
    /// 附加到执行器命令行的诊断标记（执行器忽略未知参数）；用于进程
    /// 归属诊断与测试隔离。
    pub tag: Option<String>,
}

/// 执行器二进制定位（I09）：FLOW_EXECUTOR_BIN 显式路径 → 可执行文件
/// 同目录 → target 目录常规位置。找不到即报错，不回退。
pub fn locate_executor() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os(EXECUTOR_BIN_ENV) {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "{EXECUTOR_BIN_ENV}={path:?} does not point to an executable file"
        ));
    }
    let Ok(current) = std::env::current_exe() else {
        return Err("cannot locate executor binary: current_exe unavailable".into());
    };
    let sibling = current
        .parent()
        .map(|dir| dir.join(EXECUTOR_BIN_NAME))
        .filter(|path| path.is_file());
    if let Some(path) = sibling {
        return Ok(path);
    }
    // cargo 布局：test/deps 二进制向上两级找 target/{debug,release}。
    let mut dir = current.parent().map(Path::to_path_buf);
    for _ in 0..3 {
        let Some(parent) = dir else { break };
        for profile in ["debug", "release"] {
            let candidate = parent.join(profile).join(EXECUTOR_BIN_NAME);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
        dir = parent.parent().map(Path::to_path_buf);
    }
    Err(format!(
        "executor binary '{EXECUTOR_BIN_NAME}' not found; set {EXECUTOR_BIN_ENV}"
    ))
}

/// 从环境变量解析执行模式（测试可注入变量值）。
pub fn mode_from_env() -> Result<ExecutionMode, String> {
    let value = std::env::var(EXECUTION_MODE_ENV).unwrap_or_default();
    mode_from_env_with(&value)
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

/// 解析远程模式选项（FLOW_REMOTE_*）；缺失即报错（不静默回退）。
pub fn remote_options_from_env() -> Result<crate::execution::remote::RemoteOptions, String> {
    let read = |name: &str| -> Result<PathBuf, String> {
        std::env::var(name)
            .map(PathBuf::from)
            .map_err(|_| format!("{name} not set for remote mode"))
    };
    Ok(crate::execution::remote::RemoteOptions {
        control_addr: std::env::var("FLOW_REMOTE_CONTROL_ADDR")
            .map_err(|_| "FLOW_REMOTE_CONTROL_ADDR not set".to_string())?,
        data_addr: std::env::var("FLOW_REMOTE_DATA_ADDR")
            .map_err(|_| "FLOW_REMOTE_DATA_ADDR not set".to_string())?,
        ca_cert: read("FLOW_REMOTE_CA")?,
        server_cert: read("FLOW_REMOTE_CERT")?,
        server_key: read("FLOW_REMOTE_KEY")?,
        attach_timeout_ms: std::env::var("FLOW_REMOTE_ATTACH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120_000),
    })
}

/// 按给定环境值解析执行模式。
pub fn mode_from_env_with(value: &str) -> Result<ExecutionMode, String> {
    match value {
        "" | "in_process" => Ok(ExecutionMode::InProcess),
        "ipc" => {
            let executor_bin = locate_executor()?;
            Ok(ExecutionMode::Ipc(IpcOptions {
                executor_bin,
                x_max: X_MAX_DEFAULT,
                tag: None,
            }))
        }
        other => Err(format!(
            "invalid {EXECUTION_MODE_ENV}={other:?}; expected in_process|ipc"
        )),
    }
}
