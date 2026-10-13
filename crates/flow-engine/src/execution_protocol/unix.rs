//! 本地执行 IPC 的 Unix 承载：单 socketpair 构造。
//!
//! 传输层（[`super::transport`]）不依赖 Unix；本模块是本地子进程执行
//! （spawn + pre_exec 关闭多余 FD）的 Unix 实现，仅 unix 目标编译。
//! v3 起执行器本地通道收敛为单流（帧内通道标签），一条 socketpair 即可。

use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream as StdStream;

use tokio::net::UnixStream;

use super::transport::FrameTransport;

/// 创建一条 socketpair。返回 (父端, 子端)。FD 无 CLOEXEC——子端经
/// `--channel-fd <N>` 参数告知执行器原号使用（见 spawn），父端在 spawn 后
/// 关闭子端副本，保证除目标子进程外无人继承连接。
pub fn socketpair_channel() -> std::io::Result<(UnixStream, UnixStream)> {
    let mut fds = [0i32; 2];
    // SAFETY: 传入的是有效且尺寸正确的输出缓冲。
    unsafe {
        if libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    // SAFETY: FD 由本函数创建且只移交一次。
    let (parent, child) = unsafe { (StdStream::from_raw_fd(fds[0]), StdStream::from_raw_fd(fds[1])) };
    // tokio 要求非阻塞；socketpair 默认阻塞。
    parent.set_nonblocking(true)?;
    child.set_nonblocking(true)?;
    Ok((
        UnixStream::from_std(parent)?,
        UnixStream::from_std(child)?,
    ))
}

/// 执行器入口：按父进程 `--channel-fd` 指定的原号构建端点。
pub fn channel_from_raw(fd: i32) -> std::io::Result<UnixStream> {
    // SAFETY: FD 由父进程 spawn 链保证有效，且只移交一次。
    let stream = unsafe { StdStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    UnixStream::from_std(stream)
}

impl FrameTransport {
    /// 在 socketpair 端点上启动 I/O 任务（本地执行便捷入口）。
    pub fn spawn_local(stream: UnixStream, queue_depth: usize) -> Self {
        Self::spawn(stream, queue_depth)
    }
}
