//! 字节通道：unix 用 socketpair，其余平台用本地 socket（named pipe /
//! 文件系统 socket）回连。
//!
//! 两种承载对上层完全等价：**同一种 `Channel` 类型、同一套 spawn/connect
//! 语义**。差异只有一处：unix 的通道端点随进程创建而诞生（socketpair），
//! 其余平台要先有监听方，子进程再回连（谁当服务端由 spawn 侧决定）。
//!
//! 「不占网络栈」是刻意的：本地 socket 按对象名/路径寻址，没有端口，
//! 因此没有「端口抢连窗口」，也没有「端口被占导致启动失败」这类问题。

use std::io;

/// 字节通道类型。
///
/// unix 是 socketpair 端点；其余平台是本地 socket 连接。两者都实现
/// `AsyncRead + AsyncWrite + Send + 'static`，可以直接喂给任何帧解码器。
#[cfg(unix)]
pub type Channel = tokio::net::UnixStream;

/// 字节通道类型（非 unix：本地 socket 连接）。
#[cfg(not(unix))]
pub type Channel = interprocess::local_socket::tokio::Stream;

/// 创建一对互连的通道端点（unix：socketpair）。
///
/// 两端都先置 CLOEXEC：父进程 exec 别的程序时不会顺手把这条会话的 fd
/// 带出去；子进程接管通道时自己清一次标志。
#[cfg(unix)]
pub fn pair() -> io::Result<(Channel, Channel)> {
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream as StdStream;

    let (parent, child) = StdStream::pair()?;
    set_cloexec(parent.as_raw_fd())?;
    set_cloexec(child.as_raw_fd())?;
    // tokio 要求非阻塞；socketpair 默认阻塞。
    parent.set_nonblocking(true)?;
    child.set_nonblocking(true)?;
    Ok((
        tokio::net::UnixStream::from_std(parent)?,
        tokio::net::UnixStream::from_std(child)?,
    ))
}

/// 接管继承来的通道 fd（unix 子进程侧）。
///
/// 接管后立即恢复 CLOEXEC：插件之后还会再 fork 出自己的子进程，那些
/// 进程不应看到这条会话的 fd。
#[cfg(unix)]
pub fn adopt_raw_fd(fd: i32) -> io::Result<Channel> {
    use std::os::fd::FromRawFd;
    use std::os::unix::net::UnixStream as StdStream;

    // SAFETY: fd 由宿主的 spawn 链保证有效，且只移交一次。
    let stream = unsafe { StdStream::from_raw_fd(fd) };
    set_cloexec(std::os::fd::AsRawFd::as_raw_fd(&stream))?;
    stream.set_nonblocking(true)?;
    tokio::net::UnixStream::from_std(stream)
}

/// 按环境变量取通道（子进程侧入口）。
///
/// unix：读 `PROC_IPC_FD` 接管继承来的 fd；其余平台：读 `PROC_IPC_ADDR`
/// 回连。两条路径的失败都是「启动失败」，由调用方显式处理，不做重试。
#[cfg(unix)]
pub fn from_env() -> io::Result<Channel> {
    let fd = crate::ipc::env::fd_from_env().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "PROC_IPC_FD is missing: not started as a plugin child",
        )
    })?;
    adopt_raw_fd(fd)
}

/// 按环境变量取通道（子进程侧入口，非 unix）。
#[cfg(not(unix))]
pub async fn from_env() -> io::Result<Channel> {
    let addr = crate::ipc::env::addr_from_env().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "PROC_IPC_ADDR is missing: not started as a plugin child",
        )
    })?;
    connect(&addr).await
}

/// 连接本地 socket（非 unix 子进程侧）。
///
/// 只连一次：连接失败由调用方按启动失败处理，不做重试循环 —— 重试会把
/// 「宿主已经放弃」变成子进程里的长时间悬挂。
#[cfg(not(unix))]
pub async fn connect(addr: &str) -> io::Result<Channel> {
    use interprocess::local_socket::tokio::Stream;
    use interprocess::local_socket::traits::tokio::Stream as _;
    use interprocess::local_socket::{GenericNamespaced, ToNsName};

    let name = addr.to_ns_name::<GenericNamespaced>()?;
    Stream::connect(name).await
}

/// 给 fd 置上 CLOEXEC 标志（exec 时不继承）。
///
/// 错误一律上抛：标志设不上意味着这条会话的 fd 可能泄漏给别的进程，
/// 属于必须失败的安全边界，而不是可以忽略的尽力而为。
#[cfg(unix)]
pub fn set_cloexec(fd: i32) -> io::Result<()> {
    // SAFETY: fcntl 只读取/修改 fd 标志，不触碰内存。
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;

    #[tokio::test]
    async fn pair_endpoints_are_cloexec_and_bidirectional() {
        let (parent, child) = pair().unwrap();
        for fd in [parent.as_raw_fd(), child.as_raw_fd()] {
            assert_ne!(
                unsafe { libc::fcntl(fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
                0,
                "通道端点必须默认 CLOEXEC，否则会泄漏给无关子进程"
            );
        }
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut parent, mut child) = (parent, child);
        child.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        parent.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }
}
