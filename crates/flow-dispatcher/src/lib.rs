//! Process supervision and point-to-point task dispatch over IPC.

pub mod execution_protocol;
pub mod pool;
pub mod process;

use execution_protocol::{ExecutorInvocation, Message};
pub use pool::ExecutorPool;
use tokio::sync::mpsc;

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

mod job_transfer;
mod jobs;
pub mod worker;
pub use jobs::{Dispatcher, Job, JobCompletion, JobError, MAX_JOB_BYTES, MAX_STREAMED_JOB_BYTES};

/// Validate the complete control frame before touching the worker session.
pub async fn send_execute(
    commands: &mpsc::Sender<ToSession>,
    message: Message,
) -> Result<(), JobError> {
    if !matches!(
        message,
        Message::Execute { .. }
            | Message::PrepareJob { .. }
            | Message::ExecuteJob { .. }
            | Message::ExecuteJobStream { .. }
    ) {
        return Err(JobError::Protocol("expected an execute command".into()));
    }
    let size = postcard::experimental::serialized_size(&message)
        .map_err(|error| JobError::Protocol(format!("execute encode probe: {error}")))?;
    let limit = execution_protocol::CONTROL_MAX_FRAME;
    if size > limit {
        return Err(JobError::Protocol(format!(
            "execute frame exceeds control budget: {size} bytes > {limit}"
        )));
    }
    commands
        .send(ToSession::Send(Box::new(message)))
        .await
        .map_err(|_| JobError::Lost("executor session closed before execute".into()))
}

/// Send only after the caller has committed the result to its authoritative store.
pub async fn send_committed(
    commands: &mpsc::Sender<ToSession>,
    dispatch_id: String,
    result_id: String,
) -> Result<(), JobError> {
    commands
        .send(ToSession::Send(Box::new(Message::ResultCommitted {
            dispatch_id,
            result_id,
        })))
        .await
        .map_err(|_| JobError::Lost("executor session closed before commit confirmation".into()))
}
