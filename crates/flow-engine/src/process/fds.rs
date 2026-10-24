//! Enumerate before fork; only async-signal-safe fcntl runs in pre_exec.
//!
//! 本模块负责**子进程 fd 收敛**：fork 前备好 fd 清单，exec 前用
//! async-signal-safe 的 fcntl 逐个打上 FD_CLOEXEC。
//! 不变量：fork 后只做 async-signal-safe 调用（否则可能死锁）；只打标记不
//! 立即关闭（fd 号可能被 Rust 复用于 exec-error 管道）；只保留调用方声明要传
//! 的那一个 fd。
//! 不这样做的代价：执行器会继承主进程的信道、数据库句柄与临时文件。
use tokio::process::Command;

/// 把 `command` 配置成：除 `keep` 外的继承 fd 全部在 exec 时关闭。
///
/// `keep` 是要显式传给子进程的那个 fd（如执行器通信通道）；为 `None`
/// 时表示一个都不留。
pub(crate) fn close_inherited_fds(command: &mut Command, keep: Option<i32>) {
    // 枚举在 fork 之前完成：fork 后只能碰 async-signal-safe 的东西。
    let inherited = open_fds();
    let fallback_limit = fd_limit();
    unsafe {
        command.pre_exec(move || {
            if let Some(fd) = keep {
                // 清掉 keep 的 CLOEXEC：它就是要传下去的那个。
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            if inherited.is_empty() {
                // 枚举失效时的兜底：盲扫到上限，逐个打标记（EBADF 跳过）。
                for fd in 3..fallback_limit {
                    if Some(fd) != keep {
                        mark_cloexec(fd)?;
                    }
                }
            } else {
                // 正常路径：只处理枚举到的 fd，不去盲扫。
                for fd in &inherited {
                    if Some(*fd) != keep {
                        mark_cloexec(*fd)?;
                    }
                }
            }
            Ok(())
        });
    }
}

/// Enumerate outside fork. Mark at exec rather than close immediately: an FD
/// number can have been reused by Rust for its private exec-error pipe.
///
/// Linux 读 /proc/self/fd，macOS 读 /dev/fd；两者都拿不到（极少数平台）
/// 返回空表，调用方回落盲扫。枚举与 fork 之间新开的 FD 由 Rust 默认的
/// close-on-exec 兜底——这道防线针对的是无 CLOEXEC 的继承句柄，它们在
/// 枚举时必然已打开。
///
/// 返回值只含 > 2 的 fd：0/1/2 是标准流，由 stdio 装配按需 dup2 处理。
fn open_fds() -> Vec<i32> {
    let dir = if std::path::Path::new("/proc/self/fd").exists() {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.parse::<i32>().ok())
                })
                // 标准流不在收敛范围内：它们要被 dup2 复用。
                .filter(|fd| *fd > 2)
                .collect()
        })
        // 读不到就返回空表：调用方会走盲扫兜底。
        .unwrap_or_default()
}

/// 盲扫兜底的上限（getrlimit 是 pre_exec 安全的低层调用）。
///
/// 封顶 65536：盲扫是 O(上限) 的 EBADF 系统调用流水，上限取 rlim_cur 的
/// 全量（1M+）会把 spawn 拖到不可用；兜底路径只在 FD 枚举完全失效时
/// 走到，覆盖主流范围的 65536 已优于「不关」。
fn fd_limit() -> i32 {
    unsafe {
        // 先给保守默认值：getrlimit 失败时也有上限可依。
        let mut limit = libc::rlimit {
            rlim_cur: 4096,
            rlim_max: 4096,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            // 无上限（RLIM_INFINITY）时用 65536 兜底，不做无穷盲扫。
            let cur = if limit.rlim_cur == libc::RLIM_INFINITY {
                65_536
            } else {
                limit.rlim_cur
            };
            cur.min(65_536) as i32
        } else {
            4096
        }
    }
}

/// 给单个 fd 打上 `FD_CLOEXEC` 标记。
///
/// 标记而不是关闭：exec 时内核会自动关掉它，避免 fd 编号被复用后误关
/// 别人的东西。
fn mark_cloexec(fd: i32) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        let error = std::io::Error::last_os_error();
        // The parent-side directory iterator or child stdio setup may have
        // already closed this descriptor. Other failures must reject spawn.
        // EBADF：这个 fd 在枚举之后已被关掉（枚举用的目录迭代器本身、
        // 或子进程 stdio 装配关掉了它）——跳过即可；其余错误必须让
        // spawn 失败，否则会漏掉一个本该关闭的 fd。
        return if error.raw_os_error() == Some(libc::EBADF) {
            Ok(())
        } else {
            Err(error)
        };
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
