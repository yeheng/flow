//! 执行器子进程 spawn（二期 §3.1 的 FD 映射，供本地池与远程 agent 复用）。
//!
//! 仅映射目标 socketpair FD（dup2 到固定槽位后关闭其余继承 FD），其余 FD
//! 默认 close-on-exec；父进程 spawn 后需自行关闭子端副本（本函数返回的
//! 父端持有前，子端 `ChannelPair` 由调用方 drop）。

use std::os::fd::AsRawFd;
use std::path::Path;

use tokio::process::{Child, Command};

use super::transport::{socketpair_channels, ChannelPair};

/// spawn 一个执行器进程并返回 (父端双通道, 子进程句柄)。
/// `tag` 为可选诊断参数（执行器忽略未知参数）。
pub fn spawn_executor_process(
    bin: &Path,
    tag: Option<&str>,
) -> std::io::Result<(ChannelPair, Child, ChannelPair)> {
    let (parent, child_pair) = socketpair_channels()?;
    let child_control = child_pair.control.as_raw_fd();
    let child_data = child_pair.data.as_raw_fd();
    let mut command = Command::new(bin);
    if let Some(tag) = tag {
        command.arg(format!("--tag {tag}"));
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    // pre_exec 只做允许的低层 FD 操作：dup2/close/getrlimit。禁止分配
    // 内存、加锁或调用普通异步代码（二期 §3.1）。
    unsafe {
        command.pre_exec(move || {
            use super::contract::{EXECUTOR_CONTROL_FD_SLOT, EXECUTOR_DATA_FD_SLOT};
            if libc::dup2(child_control, EXECUTOR_CONTROL_FD_SLOT) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::dup2(child_data, EXECUTOR_DATA_FD_SLOT) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if child_control != EXECUTOR_CONTROL_FD_SLOT {
                libc::close(child_control);
            }
            if child_data != EXECUTOR_DATA_FD_SLOT {
                libc::close(child_data);
            }
            // 关闭其余继承 FD（父端 + 兄弟 socketpair），防泄漏/串配。
            let limit = fd_limit();
            let mut fd: i32 = 3;
            while fd < limit {
                if fd != EXECUTOR_CONTROL_FD_SLOT && fd != EXECUTOR_DATA_FD_SLOT {
                    libc::close(fd);
                }
                fd += 1;
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    // 注意：子端副本由调用方 drop（本函数返回它以明确生命周期）。
    Ok((parent, child, child_pair))
}

/// 子进程 FD 上限（getrlimit 是 pre_exec 安全的低层调用）。
fn fd_limit() -> i32 {
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 4096,
            rlim_max: 4096,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            limit.rlim_cur.min(65_536) as i32
        } else {
            4096
        }
    }
}
