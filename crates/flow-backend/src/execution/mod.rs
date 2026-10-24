//! 执行模式与 IPC 选项（二期 §5，I09）。
//!
//! 本地执行只有 IPC：节点执行语义只存在于 flow-executor 子进程（unix 走
//! socketpair，其余平台走 loopback 回连，见 execution_protocol）。启用 IPC
//! 时二进制缺失/版本不兼容直接失败，不静默降级。
//!
//! 职责边界：本模块只做「用户意图 → 执行模式与执行器定位」的翻译，不含
//! 任何派发、进程回收或远端路由逻辑——那些分别在 dispatch / pool / remote
//! 三个子模块里。守护的不变量：执行模式只有一个合法取值（ipc），其余
//! 取值（已删除的 in_process、拼错的 remote）必须显式报错。
//!
//! 为什么需要这一层：env 注入（测试）与统一配置（产品）必须走同一条解析
//! 路径，否则同一份配置在两条路径下会得到不同的执行语义；把「解析」与
//! 「执行」分开，也让模式决策可以被单独断言。

pub mod dispatch;
pub mod pool;
pub mod remote;

use flow_engine::execution_protocol::message::Message;

/// 主进程 → 会话驱动命令。`Send` 装箱：`Message` 最大变体携带完整
/// `ExecuteTask`，装箱使枚举收缩到指针大小，命令队列每个槽位少搬数百字节。
/// 会话层协议类型（全平台）：本地池会话与远程 agent 链路共用。
pub enum ToSession {
    /// 一条待发往执行器（或经 agent 中继）的协议帧。
    Send(Box<Message>),
    /// 持久取消已提交：发 Cancel 并等待排空（宽限后 kill）。
    CancelDispatch(String),
    /// 会话整体回收（派发失败/通道故障/关停）。
    Recycle,
}

/// 会话驱动 → 当前派发的事件。`Incoming` 装箱理由同 `ToSession::Send`：
/// 消息流经有界队列，槽位大小决定每帧的复制成本。
pub enum FromSession {
    /// 一条来自执行器（或 agent 中继）的协议帧。
    Incoming(Box<Message>),
    /// 会话死亡（通道 EOF/错误/心跳超时或回收完成）；此后不再有事件。
    Dead(String),
}

use flow_engine::execution_protocol::contract::{
    ExecutorInvocation, EXECUTION_MODE_ENV, X_MAX_DEFAULT,
};

pub use dispatch::IpcDispatcher;
pub use pool::ExecutorPool;

/// 执行模式（§5）：当前只有 IPC 一种实现，`in_process` 已随进程内执行
/// 路径删除。保留枚举而非直接用 `IpcOptions`，是为了让「模式」在配置、
/// 日志与错误文案里都有稳定名字（`execution.mode=ipc`），未来若新增模式
/// 也不必改动所有匹配点。
#[derive(Debug, Clone)]
pub enum ExecutionMode {
    /// 本地 IPC 执行子进程（全平台；unix socketpair，其余 loopback 回连）。
    Ipc(IpcOptions),
}

/// `mode=in_process` 的明确拒绝文案（进程内执行路径已全平台删除；节点
/// 执行语义只在 flow-executor）。
pub const IN_PROCESS_REMOVED: &str =
    "execution.mode=in_process 已删除：本地执行只有 ipc（节点执行语义只在 flow-executor）";

/// 本地 IPC 执行模式的运行参数。
///
/// 三个字段共同决定「执行器是谁、能同时跑多少、怎么归因」：执行器定位
/// 决定进程从哪来，x_max 决定并发进程数上界，tag 只用于诊断归属。
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
        // in_process 已删除：显式报错而非退回 ipc，否则用户以为在进程内跑、
        // 实际换了故障域，C2 的隔离承诺就被无声打破。
        flow_config::ExecutionModeKind::InProcess => Err(IN_PROCESS_REMOVED.into()),
        flow_config::ExecutionModeKind::Ipc => {
            let executor = match &config.executor_bin {
                Some(bin) => Ok(ExecutorInvocation::explicit(bin.clone())),
                // 未显式指定时按契约定位同目录兄弟 flow-executor；找不到即报错。
                None => ExecutorInvocation::locate(),
            }?;
            Ok(ExecutionMode::Ipc(IpcOptions {
                executor,
                x_max: config.x_max as usize,
                tag: None,
            }))
        }
        // remote 的完整选项在 [execution.remote] 段，模式解析阶段不读它：
        // 缺段由 validate 报错，这里只负责把人引到正确的配置入口。
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
        wss_addr: remote.wss_addr.clone(),
        wss_agents: remote
            .agents
            .iter()
            .map(|agent| crate::execution::remote::AgentCredential {
                id: agent.id.clone(),
                token: agent.token.clone(),
            })
            .collect(),
    })
}

/// 从指定的 `FLOW_EXECUTION_MODE` 取值解析执行模式。
///
/// 与 [`mode_from_env`] 的区别只在于取值来源（参数 vs 环境变量）：抽出
/// 参数化版本是为了让测试能注入取值，而不必真的改进程环境（并行测试
/// 之间会互相污染环境变量）。
///
/// # Errors
/// 取值为 `in_process` 时返回 [`IN_PROCESS_REMOVED`]；取值非法时返回
/// 列出合法取值的错误。两条路径都**不**退回默认 ipc——静默改执行模式
/// 意味着用户以为在跑 A、实际在跑 B。
pub fn mode_from_env_with(value: &str) -> Result<ExecutionMode, String> {
    // 空值 = 未设置，默认 ipc（全平台）。
    let value = if value.is_empty() { "ipc" } else { value };
    match value {
        // 进程内执行路径已全平台删除——显式拒绝，不静默改走 ipc。
        "in_process" => Err(IN_PROCESS_REMOVED.into()),
        "ipc" => {
            // 定位失败即报错：执行器缺失时提前失败，比让每次派发都失败更早、
            // 错误信息也更可读。
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
