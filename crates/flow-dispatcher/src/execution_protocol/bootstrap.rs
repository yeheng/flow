//! Platform-specific executor startup shared by the backend and remote agent.
//!
//! 本模块只做一件事：按平台把「执行器二进制 + 通道」拉起来，产出一个
//! 统一的 [`FrameTransport`]。unix 走 socketpair + FD 传递（能力即凭据，
//! 无需会话令牌），非 unix 走回连 + 会话令牌（防本机抢连）。
//!
//! 守护的核心不变量：两种承载对上层完全等价——同一个 [`FrameTransport`]、
//! 同一套握手与限额；差异只体现在第三返回值（会话令牌是否存在）上。
//! 之所以要收敛到这一层：主进程与 agent 不应知道执行器跑在哪种承载上，
//! 否则每加一个平台就要改一遍业务逻辑。
use super::{contract::ExecutorInvocation, transport::FrameTransport};
use crate::process::ManagedChild;

/// 按平台启动执行器并返回 (传输端点, 子进程句柄, 会话令牌)。
///
/// 第三项在 unix 上恒为 `None`（FD 能力即凭据），在回连承载上为
/// `Some(token)`：调用方必须把它带到握手校验里，否则抢连者可以冒充
/// 执行器完成握手。
///
/// # Errors
/// 进程创建失败或回连超时/失败时返回 I/O 错误；调用方应终止该次
/// 启动，不做静默重试。
pub async fn spawn_executor_transport(
    executor: &ExecutorInvocation,
    tag: Option<&str>,
    extra_args: &[&str],
    queue_depth: usize,
) -> std::io::Result<(FrameTransport, ManagedChild, Option<String>)> {
    #[cfg(unix)]
    {
        let (parent, child, child_side) =
            super::spawn::spawn_executor_process(executor, tag, extra_args)?;
        // 父进程侧必须丢掉子端副本：留着它会让父进程与子进程同时持有
        // 同一端点，EOF 语义会失真。
        drop(child_side);
        Ok((
            FrameTransport::spawn_local(parent, queue_depth),
            child,
            None,
        ))
    }
    #[cfg(not(unix))]
    {
        // 回连承载没有 FD 能力：随机令牌是唯一的抢连防御。
        let token = super::transport::fresh_boot_id();
        let (stream, child) =
            super::loopback::spawn_executor_loopback(executor, tag, token.clone(), extra_args)
                .await?;
        Ok((
            FrameTransport::spawn(stream, queue_depth),
            child,
            Some(token),
        ))
    }
}
