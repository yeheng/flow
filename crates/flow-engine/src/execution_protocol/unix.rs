//! 本地执行 IPC 的 Unix 承载：单 socketpair 构造。
//!
//! 传输层（[`super::transport`]）不依赖 Unix；本模块是本地子进程执行
//! （spawn + pre_exec 关闭多余 FD）的 Unix 实现，仅 unix 目标编译。
//! v3 起执行器本地通道收敛为单流（帧内通道标签），一条 socketpair 即可。

use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream as StdStream;

use tokio::net::UnixStream;

use super::transport::FrameTransport;

/// Create CLOEXEC endpoints. spawn clears the child flag only in the intended
/// fork child, and channel_from_raw restores it before any user tools run.
pub fn socketpair_channel() -> std::io::Result<(UnixStream, UnixStream)> {
    let (parent, child) = StdStream::pair()?;
    set_cloexec(parent.as_raw_fd())?;
    set_cloexec(child.as_raw_fd())?;
    // tokio 要求非阻塞；socketpair 默认阻塞。
    parent.set_nonblocking(true)?;
    child.set_nonblocking(true)?;
    Ok((UnixStream::from_std(parent)?, UnixStream::from_std(child)?))
}

/// 执行器入口：按父进程 `--channel-fd` 指定的原号构建端点。
pub fn channel_from_raw(fd: i32) -> std::io::Result<UnixStream> {
    // SAFETY: FD 由父进程 spawn 链保证有效，且只移交一次。
    let stream = unsafe { StdStream::from_raw_fd(fd) };
    set_cloexec(stream.as_raw_fd())?;
    stream.set_nonblocking(true)?;
    UnixStream::from_std(stream)
}

impl FrameTransport {
    /// 在 socketpair 端点上启动 I/O 任务（本地执行便捷入口）。
    pub fn spawn_local(stream: UnixStream, queue_depth: usize) -> Self {
        Self::spawn(stream, queue_depth)
    }
}

fn set_cloexec(fd: i32) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::IntoRawFd;

    #[tokio::test]
    async fn channel_endpoints_and_adopted_descriptor_are_cloexec() {
        let (parent, child) = socketpair_channel().unwrap();
        for fd in [parent.as_raw_fd(), child.as_raw_fd()] {
            assert_ne!(
                unsafe { libc::fcntl(fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
        }
        let raw = child.into_std().unwrap().into_raw_fd();
        unsafe {
            libc::fcntl(raw, libc::F_SETFD, 0);
        }
        let adopted = channel_from_raw(raw).unwrap();
        assert_ne!(
            unsafe { libc::fcntl(adopted.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
    }
}
