//! 执行模式与 IPC 选项（二期 §5，I09）。
//!
//! 本地执行只有 IPC：节点执行语义只存在于 flow-executor 子进程（unix 走
//! socketpair，其余平台走 loopback 回连，见 execution_protocol）。启用 IPC
//! 时二进制缺失/版本不兼容直接失败，不静默降级。

pub mod dispatch;
pub mod pool;
pub mod remote;

use flow_engine::execution_protocol::message::Message;

/// 主进程 → 会话驱动命令。`Send` 装箱：`Message` 最大变体携带完整
/// `ExecuteTask`，装箱使枚举收缩到指针大小，命令队列每个槽位少搬数百字节。
/// 会话层协议类型（全平台）：本地池会话与远程 agent 链路共用。
pub enum ToSession {
    Send(Box<Message>),
    /// 持久取消已提交：发 Cancel 并等待排空（宽限后 kill）。
    CancelDispatch(String),
    /// 会话整体回收（派发失败/通道故障/关停）。
    Recycle,
}

/// 会话驱动 → 当前派发的事件。`Incoming` 装箱理由同 `ToSession::Send`：
/// 消息流经有界队列，槽位大小决定每帧的复制成本。
pub enum FromSession {
    Incoming(Box<Message>),
    /// 会话死亡（通道 EOF/错误/心跳超时或回收完成）；此后不再有事件。
    Dead(String),
}


use flow_engine::execution_protocol::contract::{
    ExecutorInvocation, EXECUTION_MODE_ENV, X_MAX_DEFAULT,
};

pub use dispatch::IpcDispatcher;
pub use pool::ExecutorPool;

#[derive(Debug, Clone)]
pub enum ExecutionMode {
    /// 本地 IPC 执行子进程（全平台；unix socketpair，其余 loopback 回连）。
    Ipc(IpcOptions),
}

/// `mode=in_process` 的明确拒绝文案（进程内执行路径已全平台删除；节点
/// 执行语义只在 flow-executor）。
pub const IN_PROCESS_REMOVED: &str =
    "execution.mode=in_process 已删除：本地执行只有 ipc（节点执行语义只在 flow-executor）";

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
        flow_config::ExecutionModeKind::InProcess => Err(IN_PROCESS_REMOVED.into()),
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
        ca_cert: remote.ca_cert.clone(),
        server_cert: remote.cert.clone(),
        server_key: remote.key.clone(),
        attach_timeout_ms: remote.attach_timeout_ms,
    })
}

pub fn mode_from_env_with(value: &str) -> Result<ExecutionMode, String> {
    // 空值 = 未设置，默认 ipc（全平台）。
    let value = if value.is_empty() { "ipc" } else { value };
    match value {
        // 进程内执行路径已全平台删除——显式拒绝，不静默改走 ipc。
        "in_process" => Err(IN_PROCESS_REMOVED.into()),
        "ipc" => {
            let executor = locate_executor()?;
            Ok(ExecutionMode::Ipc(IpcOptions {
                executor,
                x_max: X_MAX_DEFAULT,
                tag: None,
            }))
        }
        other => Err(format!(
            "invalid {EXECUTION_MODE_ENV}={other:?}; expected {}",
            expected_modes()
        )),
    }
}

/// 合法的 `FLOW_EXECUTION_MODE` 取值（in_process 已全平台删除）。
fn expected_modes() -> &'static str {
    "ipc"
}
