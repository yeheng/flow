//! Platform-specific executor startup shared by the backend and remote agent.
use super::{contract::ExecutorInvocation, transport::FrameTransport};
use crate::process::ManagedChild;

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
        drop(child_side);
        Ok((
            FrameTransport::spawn_local(parent, queue_depth),
            child,
            None,
        ))
    }
    #[cfg(not(unix))]
    {
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
