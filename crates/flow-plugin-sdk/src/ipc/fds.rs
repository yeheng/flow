//! 子进程 fd 收敛：fork 前备好 fd 清单，exec 前用 async-signal-safe 的
//! fcntl 逐个打上 FD_CLOEXEC；必要时把通道 dup2 落位到固定号。
//!
//! 不变量：
//! - fork 后只做 async-signal-safe 调用（否则可能死锁）；只打标记不
//!   立即关闭（fd 号可能被 Rust 复用于 exec-error 管道）。
//! - 只保留调用方声明要传的那一个 fd（通道），其余一律 CLOEXEC。
//! - 通道最终落在**固定号**上：通道的原始
//!   号取决于「fork 那一刻父进程的 fd 表布局」，是个脆弱契约——dup2 落位
//!   后，子进程看到的号不随宿主布局变化，也不受宿主后续改动（守护进程
//!   化、posix_spawn、多线程 fork）的牵连。
//!
//! 不这样做的代价：插件会继承宿主的数据库句柄、监听 socket 与临时文件，
//! 这是「宿主 + 插件」系统最经典的信息泄漏通道。

use std::io;

use tokio::process::Command;

/// 收敛 + 落位：除 `keep` 外全部 CLOEXEC，并把 `keep` dup2 到 `target`。
///
/// exec 后子进程的通道恒在 `target` 号上；原号（若不同于 target）重新
/// 打上 CLOEXEC —— 子进程只看到一条通道 fd。dup2 是 async-signal-safe
/// 的，可以在 pre_exec 里做。
pub(crate) fn close_inherited_fds_to(command: &mut Command, keep: i32, target: i32) {
    let keep = Some(keep);
    let target = Some(target);
    // 枚举在 fork 之前完成：fork 后只能碰 async-signal-safe 的东西。
    let inherited = open_fds();
    let fallback_limit = fd_limit();
    unsafe {
        command.pre_exec(move || {
            if let Some(fd) = keep {
                // 清掉 keep 的 CLOEXEC：它就是要传下去的那个。
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(io::Error::last_os_error());
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
            // 落位：dup2 会清掉目标号的 CLOEXEC；原号重新打上标记，
            // 子进程只看到 target 一条通道。dup2 是 async-signal-safe 的。
            if let (Some(keep), Some(target)) = (keep, target) {
                if keep != target {
                    if libc::dup2(keep, target) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    mark_cloexec(keep)?;
                }
            }
            Ok(())
        });
    }
}

/// 枚举当前进程打开的 fd（> 2）。
///
/// Linux 读 `/proc/self/fd`，macOS 读 `/dev/fd`；两者都拿不到（极少数
/// 平台）返回空表，调用方回落盲扫。枚举与 fork 之间新开的 fd 由 Rust
/// 默认的 close-on-exec 兜底 —— 这道防线针对的是**无 CLOEXEC 的继承
/// 句柄**，它们在枚举时必然已打开。
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
/// 封顶 65536：盲扫是 O(上限) 的 EBADF 系统调用流水，无上限的 rlimit
/// 会把 spawn 拖到不可用。
fn fd_limit() -> i32 {
    // SAFETY: getrlimit 只读取当前进程的限制，不触碰内存。
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

/// 给单个 fd 打上 `FD_CLOEXEC` 标记。
///
/// 标记而不是关闭：exec 时内核会自动关掉它，避免 fd 编号被复用后误关
/// 别人的东西。
fn mark_cloexec(fd: i32) -> io::Result<()> {
    // SAFETY: fcntl 只读取/修改 fd 标志。
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        let error = io::Error::last_os_error();
        // 枚举之后这个 fd 可能已被关掉（枚举用的目录迭代器本身、子进程
        // stdio 装配关掉了它）——跳过即可；其余错误必须让 spawn 失败，
        // 否则会漏掉一个本该关闭的 fd。
        return if error.raw_os_error() == Some(libc::EBADF) {
            Ok(())
        } else {
            Err(error)
        };
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
