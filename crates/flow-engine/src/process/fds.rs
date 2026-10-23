//! Enumerate before fork; only async-signal-safe fcntl runs in pre_exec.
use tokio::process::Command;

pub(crate) fn close_inherited_fds(command: &mut Command, keep: Option<i32>) {
    let inherited = open_fds();
    let fallback_limit = fd_limit();
    unsafe {
        command.pre_exec(move || {
            if let Some(fd) = keep {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            if inherited.is_empty() {
                for fd in 3..fallback_limit {
                    if Some(fd) != keep {
                        mark_cloexec(fd)?;
                    }
                }
            } else {
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
                .filter(|fd| *fd > 2)
                .collect()
        })
        .unwrap_or_default()
}

/// 盲扫兜底的上限（getrlimit 是 pre_exec 安全的低层调用）。
///
/// 封顶 65536：盲扫是 O(上限) 的 EBADF 系统调用流水，上限取 rlim_cur 的
/// 全量（1M+）会把 spawn 拖到不可用；兜底路径只在 FD 枚举完全失效时
/// 走到，覆盖主流范围的 65536 已优于「不关」。
fn fd_limit() -> i32 {
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 4096,
            rlim_max: 4096,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
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

fn mark_cloexec(fd: i32) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        let error = std::io::Error::last_os_error();
        // The parent-side directory iterator or child stdio setup may have
        // already closed this descriptor. Other failures must reject spawn.
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
