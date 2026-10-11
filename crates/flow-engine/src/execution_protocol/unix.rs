//! 本地执行 IPC 的 Unix 承载：socketpair 构造与固定 FD 槽位接管。
//!
//! 传输层（[`super::transport`]）不依赖 Unix；本模块是本地子进程执行
//! （spawn + pre_exec FD 映射）的 Unix 实现，仅 unix 目标编译。

use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream as StdStream;

use tokio::net::UnixStream;

use super::transport::FrameTransport;

/// 两条 socketpair 的本地端点归属：父端与子端各持其一。
pub struct ChannelPair {
    pub control: UnixStream,
    pub data: UnixStream,
}

/// 创建一组双通道 socketpair。返回 (父端, 子端)。FD 无 CLOEXEC——子端经
/// pre_exec dup2 到固定槽位后关闭原 FD（见 spawn），父端在 spawn 后关闭
/// 子端副本，保证除目标子进程外无人继承连接。
pub fn socketpair_channels() -> std::io::Result<(ChannelPair, ChannelPair)> {
    let mut control = [0i32; 2];
    let mut data = [0i32; 2];
    // SAFETY: 传入的是有效且尺寸正确的输出缓冲。
    unsafe {
        if libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, control.as_mut_ptr()) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, data.as_mut_ptr()) != 0 {
            let err = std::io::Error::last_os_error();
            libc::close(control[0]);
            libc::close(control[1]);
            return Err(err);
        }
    }
    let take = |fds: &[i32; 2]| -> (StdStream, StdStream) {
        // SAFETY: FD 由本函数创建且只移交一次。
        unsafe {
            let a = StdStream::from_raw_fd(fds[0]);
            let b = StdStream::from_raw_fd(fds[1]);
            (a, b)
        }
    };
    let (control_parent, control_child) = take(&control);
    let (data_parent, data_child) = take(&data);
    // tokio 要求非阻塞；socketpair 默认阻塞。
    control_parent.set_nonblocking(true)?;
    control_child.set_nonblocking(true)?;
    data_parent.set_nonblocking(true)?;
    data_child.set_nonblocking(true)?;
    Ok((
        ChannelPair {
            control: UnixStream::from_std(control_parent)?,
            data: UnixStream::from_std(data_parent)?,
        },
        ChannelPair {
            control: UnixStream::from_std(control_child)?,
            data: UnixStream::from_std(data_child)?,
        },
    ))
}

/// 执行器入口：从固定 FD 槽位构建端点（FD 已由 pre_exec dup2 就位）。
pub fn channels_from_raw(control_fd: i32, data_fd: i32) -> std::io::Result<ChannelPair> {
    // SAFETY: FD 由父进程 pre_exec 保证有效，且只移交一次。
    let control = unsafe { StdStream::from_raw_fd(control_fd) };
    let data = unsafe { StdStream::from_raw_fd(data_fd) };
    control.set_nonblocking(true)?;
    data.set_nonblocking(true)?;
    Ok(ChannelPair {
        control: UnixStream::from_std(control)?,
        data: UnixStream::from_std(data)?,
    })
}

impl FrameTransport {
    /// 在 socketpair 端点上启动 I/O 任务（本地执行便捷入口）。
    pub fn spawn(pair: ChannelPair, queue_depth: usize) -> Self {
        Self::spawn_streams(pair.control, pair.data, queue_depth)
    }
}
