//! 本地执行 IPC 的 Unix 承载：单 socketpair 构造。
//!
//! 传输层（[`super::transport`]）不依赖 Unix；本模块是本地子进程执行
//! （spawn + pre_exec 关闭多余 FD）的 Unix 实现，仅 unix 目标编译。
//! v3 起执行器本地通道收敛为单流（帧内通道标签），一条 socketpair 即可。
//!
//! 守护的核心不变量：通道端点默认 CLOEXEC（不泄漏给无关子进程），
//! 只有目标执行器在接管时清除该标志；FD 语义一旦泄漏，别的进程就能
//! 读到这条会话的协议帧。

use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream as StdStream;

use tokio::net::UnixStream;

use super::transport::FrameTransport;

/// Create CLOEXEC endpoints. spawn clears the child flag only in the intended
/// fork child, and channel_from_raw restores it before any user tools run.
pub fn socketpair_channel() -> std::io::Result<(UnixStream, UnixStream)> {
    let (parent, child) = StdStream::pair()?;
    // 两端都先置 CLOEXEC：父进程在 exec 别的程序时不会顺手把这条会话
    // 的 FD 带出去；执行器侧接管时会自己清一次标志。
    set_cloexec(parent.as_raw_fd())?;
    set_cloexec(child.as_raw_fd())?;
    // tokio 要求非阻塞；socketpair 默认阻塞。
    parent.set_nonblocking(true)?;
    child.set_nonblocking(true)?;
    Ok((UnixStream::from_std(parent)?, UnixStream::from_std(child)?))
}

/// 执行器入口：按父进程 `--channel-fd` 指定的原号构建端点。
///
/// 接管后立即恢复 CLOEXEC：执行器之后还会 fork 出用户工具进程，那
/// 些进程不应看到这条会话的 FD。
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

/// 给 FD 置上 CLOEXEC 标志（exec 时不继承）。
///
/// 错误一律上抛：标志设不上意味着这条会话 FD 可能泄漏给子进程，
/// 属于必须失败的安全边界，而不是可以忽略的尽力而为。
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
