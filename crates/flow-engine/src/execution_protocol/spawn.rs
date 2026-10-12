//! 执行器子进程 spawn（供本地池与远程 agent 复用）。
//!
//! 通道 FD 以 `--channel-fd <N>` 随 argv 传递：子端 socketpair FD 原号
//! 存活过 exec（无 CLOEXEC），pre_exec 只需关闭其余继承 FD，不做任何
//! dup2 落位。其余 FD 默认 close-on-exec 由关闭循环兜底；父进程 spawn 后
//! 需自行关闭子端副本（本函数返回的子端流由调用方 drop）。

use std::os::fd::AsRawFd;

use tokio::net::UnixStream;
use tokio::process::{Child, Command};

use super::contract::ExecutorInvocation;
use super::unix::socketpair_channel;

/// spawn 一个执行器进程并返回 (父端通道, 子进程句柄, 子端通道副本)。
/// `tag` 为可选诊断参数（执行器忽略未知参数）。
pub fn spawn_executor_process(
    executor: &ExecutorInvocation,
    tag: Option<&str>,
) -> std::io::Result<(UnixStream, Child, UnixStream)> {
    let (parent, child) = socketpair_channel()?;
    let child_fd = child.as_raw_fd();
    // stdio 占用 0..=3 的 dup2 在 pre_exec 之前完成；通道 FD 必须在其上，
    // 否则会被 stdio 装配覆盖。socketpair 在父进程 stdio 存活时创建，
    // 分配号恒 >= 3，此守卫只固化契约。
    if child_fd < 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("channel fd {child_fd} below stdio range"),
        ));
    }
    let mut command = Command::new(&executor.program);
    command.args(&executor.prefix);
    // `--tag` 紧跟程序名：journal_ipc 的进程识别（pgrep -f
    // "flow-executor --tag <tag>"）依赖这段连续子串，通道 FD 参数放其后。
    if let Some(tag) = tag {
        command.arg("--tag").arg(tag);
    }
    command.arg("--channel-fd").arg(child_fd.to_string());
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    // 环境隔离（T6）：执行器只需要 argv 与继承的通道 FD，不继承主进程
    // 环境（凭据/路径/代理等一概不泄漏）；仅回填诊断日志级别。
    command.env_clear();
    if let Some(log) = std::env::var_os("RUST_LOG") {
        command.env("RUST_LOG", log);
    }
    // pre_exec 只做允许的低层 FD 操作：close/getrlimit。禁止分配内存、
    // 加锁或调用普通异步代码（二期 §3.1）。不 dup、不落位——FD 原号即通道。
    let keep_fd = child_fd;
    unsafe {
        command.pre_exec(move || {
            // 关闭其余继承 FD（父端），防泄漏/串配；保留 stdio 与通道 FD。
            let limit = fd_limit();
            let mut fd: i32 = 3;
            while fd < limit {
                if fd != keep_fd {
                    libc::close(fd);
                }
                fd += 1;
            }
            Ok(())
        });
    }
    let spawned = command.spawn()?;
    // 注意：子端副本由调用方 drop（本函数返回它以明确生命周期）。
    Ok((parent, spawned, child))
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

#[cfg(test)]
mod tests {
    // spawn 连通性（argv 传 FD + 关闭循环 + 握手）由 flow-backend 的
    // journal_ipc / journal_ipc_packaging 集成测试覆盖：它们经执行池召唤
    // 真实 flow-executor 二进制跑完整派发。本单元测试二进制的参数由
    // libtest 解析，无法扮演执行器接收 --channel-fd。
}
