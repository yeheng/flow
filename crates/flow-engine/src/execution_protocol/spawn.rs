//! 执行器子进程 spawn（供本地池与远程 agent 复用）。
//!
//! 通道 FD 以 `--channel-fd <N>` 随 argv 传递：子端 socketpair FD 原号
//! 在目标 fork 子进程内清除 CLOEXEC，执行器接管后立即恢复；pre_exec 为其余
//! 继承 FD 设置 CLOEXEC，不做 dup2 落位。父进程 spawn 后
//! 需自行关闭子端副本（本函数返回的子端流由调用方 drop）。

use std::os::fd::AsRawFd;

use crate::process::ManagedChild as Child;
use tokio::net::UnixStream;
use tokio::process::Command;

use super::contract::ExecutorInvocation;
use super::unix::socketpair_channel;

/// 执行器运行时环境白名单（T6 修订）：执行器仍以 `env_clear` 起步，但
/// harness（command 模式的外部 CLI 与内联 agent 的工具）需要
/// 常规工具链环境——空 `PATH`/`HOME` 的 shell 里 cargo/git/node 大面积
/// 不可用。按显式白名单从父进程透传；`FLOW_*`（协议/凭据）、`CONGA_*`
/// （harness 配置，由 executor 派发时从冻结快照构造）、`RUST_LOG`
/// （单独注入）与任何密钥类变量都不透传。remote 模式下父进程是远端
/// 机器上的 flow-agent，透传的是那台机器的环境。
#[cfg(unix)]
pub(crate) const EXECUTOR_ENV_WHITELIST: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "TMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "USER",
    "LOGNAME",
    "SHELL",
    "RUSTUP_HOME",
    "CARGO_HOME",
    "XDG_DATA_HOME",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
];

#[cfg(windows)]
pub(crate) const EXECUTOR_ENV_WHITELIST: &[&str] = &[
    "PATH",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "SYSTEMROOT",
    "COMSPEC",
    "PATHEXT",
    "LANG",
];

/// 把白名单环境应用到执行器命令。
pub(crate) fn apply_executor_env_whitelist(command: &mut Command) {
    for name in EXECUTOR_ENV_WHITELIST {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    // spawn 连通性（argv 传 FD + 关闭循环 + 握手）由 flow-backend 的
    // journal_ipc / journal_ipc_packaging 集成测试覆盖：它们经执行池召唤
    // 真实 flow-executor 二进制跑完整派发。本单元测试二进制的参数由
    // libtest 解析，无法扮演执行器接收 --channel-fd。

    /// 白名单测试做进程 env 手术；锁保留以防未来新增同类测试并行践踏。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(unix)]
    #[test]
    fn executor_env_whitelist_passes_toolchain_and_blocks_protocol_vars() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut command = Command::new("true");
        // 测试进程环境里必然存在的工具链变量应进入白名单集
        unsafe {
            std::env::set_var("CONGA_LLM_KEY", "sk-leak");
            std::env::set_var("FLOW_SECRET_X", "leak");
        }
        apply_executor_env_whitelist(&mut command);
        let envs = command
            .as_std()
            .get_envs()
            .map(|(k, _)| k.to_string_lossy().into_owned())
            .collect::<std::collections::BTreeSet<_>>();
        // 白名单变量在设置时透传（PATH 几乎必然存在；HOME 同理）
        assert!(envs.contains("PATH"), "PATH 应在白名单集内：{envs:?}");
        // 协议/密钥/harness 配置变量不透传（env_clear 语义不回退）
        for banned in ["CONGA_LLM_KEY", "FLOW_SECRET_X", "RUST_LOG"] {
            assert!(!envs.contains(banned), "{banned} 不该透传：{envs:?}");
        }
        unsafe {
            std::env::remove_var("CONGA_LLM_KEY");
            std::env::remove_var("FLOW_SECRET_X");
        }
    }
}
