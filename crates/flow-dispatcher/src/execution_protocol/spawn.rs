//! 执行器子进程 spawn（供本地池与远程 agent 复用）。
//!
//! 通道 FD 以 `--channel-fd <N>` 随 argv 传递：子端 socketpair FD 原号
//! 在目标 fork 子进程内清除 CLOEXEC，执行器接管后立即恢复；pre_exec 为其余
//! 继承 FD 设置 CLOEXEC，不做 dup2 落位。父进程 spawn 后
//! 需自行关闭子端副本（本函数返回的子端流由调用方 drop）。
//!
//! 守护的核心不变量：只有**目标执行器**能拿到通道 FD（其余继承 FD 一律
//! CLOEXEC），且子进程环境是白名单式的（凭据类变量不继承）——这两条合起来
//! 决定「谁能看到这条会话」与「执行器能看到哪些秘密」。

use std::os::fd::AsRawFd;

use crate::process::ManagedChild as Child;
use tokio::net::UnixStream;
use tokio::process::Command;

use super::contract::ExecutorInvocation;
use super::environment::apply_executor_env_whitelist;
use super::unix::socketpair_channel;

/// spawn 一个执行器进程并返回 (父端通道, 子进程句柄, 子端通道副本)。
/// `tag` 为可选诊断参数（执行器忽略未知参数）；`extra_args` 为调用点
/// 显式指定的模式/标记参数（如 agent 中继的 `--remote-relay`）——不做
/// 「预置参数列表」的隐式传递。
pub fn spawn_executor_process(
    executor: &ExecutorInvocation,
    tag: Option<&str>,
    extra_args: &[&str],
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
    command.args(extra_args);
    // `--tag` 紧跟程序名：journal_ipc 的进程识别（pgrep -f
    // "flow-executor --tag <tag>"）依赖这段连续子串，通道 FD 参数放其后。
    if let Some(tag) = tag {
        command.arg("--tag").arg(tag);
    }
    // 通道 FD 号随 argv 传给子进程：子进程据此从继承的 FD 表里接管
    // 对端；号必须与父进程看到的一致（同一个 socketpair 端）。
    command.arg("--channel-fd").arg(child_fd.to_string());
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    // 环境隔离（T6）：执行器只需要 argv、继承的通道 FD 与下述白名单环境，
    // 不继承主进程其余环境（凭据/代理等一概不泄漏）；RUST_LOG 仍单独回填。
    command.env_clear();
    if let Some(log) = std::env::var_os("RUST_LOG") {
        command.env("RUST_LOG", log);
    }
    apply_executor_env_whitelist(&mut command);
    // The channel is CLOEXEC in the parent. Only the intended executor clears
    // it in its fork child; unrelated concurrent spawns cannot inherit it.
    crate::process::close_inherited_fds(&mut command, Some(child_fd));
    let spawned = Child::spawn_executor(&mut command)?;
    // 注意：子端副本由调用方 drop（本函数返回它以明确生命周期）。
    Ok((parent, spawned, child))
}
