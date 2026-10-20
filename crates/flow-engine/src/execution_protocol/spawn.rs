//! 执行器子进程 spawn（供本地池与远程 agent 复用）。
//!
//! 通道 FD 以 `--channel-fd <N>` 随 argv 传递：子端 socketpair FD 原号
//! 存活过 exec（无 CLOEXEC），pre_exec 只需关闭其余继承 FD，不做任何
//! dup2 落位。其余 FD 默认 close-on-exec 由关闭循环兜底；父进程 spawn 后
//! 需自行关闭子端副本（本函数返回的子端流由调用方 drop）。

use std::os::fd::AsRawFd;

use crate::process::ManagedChild as Child;
use tokio::net::UnixStream;
use tokio::process::Command;

use super::contract::ExecutorInvocation;
use super::unix::socketpair_channel;

/// 执行器运行时环境白名单（T6 修订）：执行器仍以 `env_clear` 起步，但
/// harness 子进程（command 模式的外部 CLI 与 agent 模式的 runner）需要
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

/// 把白名单环境（加上部署变量 `FLOW_HARNESS_AGENT_BIN`）应用到执行器命令。
/// `FLOW_HARNESS_AGENT_BIN` 是部署事实（executor 本地解析 runner 路径），
/// 与 `RUST_LOG` 同类——显式透传，不进通用白名单（`FLOW_*` 默认全排除）。
pub(crate) fn apply_executor_env_whitelist(command: &mut Command) {
    for name in EXECUTOR_ENV_WHITELIST {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    if let Some(bin) = std::env::var_os("FLOW_HARNESS_AGENT_BIN") {
        command.env("FLOW_HARNESS_AGENT_BIN", bin);
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
    // pre_exec 只做允许的低层 FD 操作：close/getrlimit。禁止分配内存、
    // 加锁或调用普通异步代码（二期 §3.1）。不 dup、不落位——FD 原号即通道。
    //
    // 关闭清单在**父进程**枚举（/proc/self/fd 或 /dev/fd，O(打开数)、无
    // 信号安全约束），pre_exec 只对清单逐个 close——盲扫 rlim_cur（现代
    // 系统可达 1M+）会把每次 spawn 变成上百万次 EBADF 系统调用，并行
    // 派发时直接把整机 CPU 打满（实测拖死服务器启动）。清单枚举不到的
    // 兜底：盲扫但封顶 65536（原行为）。
    let keep_fd = child_fd;
    let inherited = open_fds();
    let fallback_limit = fd_limit();
    unsafe {
        command.pre_exec(move || {
            // 关闭其余继承 FD（父端），防泄漏/串配；保留 stdio 与通道 FD。
            if inherited.is_empty() {
                let mut fd: i32 = 3;
                while fd < fallback_limit {
                    if fd != keep_fd {
                        libc::close(fd);
                    }
                    fd += 1;
                }
            } else {
                for fd in &inherited {
                    if *fd != keep_fd {
                        libc::close(*fd);
                    }
                }
            }
            Ok(())
        });
    }
    let spawned = Child::spawn_executor(&mut command)?;
    // 注意：子端副本由调用方 drop（本函数返回它以明确生命周期）。
    Ok((parent, spawned, child))
}

/// 父进程侧枚举当前打开的 FD（stdio 除外）。
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

#[cfg(test)]
mod tests {
    use super::*;

    // spawn 连通性（argv 传 FD + 关闭循环 + 握手）由 flow-backend 的
    // journal_ipc / journal_ipc_packaging 集成测试覆盖：它们经执行池召唤
    // 真实 flow-executor 二进制跑完整派发。本单元测试二进制的参数由
    // libtest 解析，无法扮演执行器接收 --channel-fd。

    #[cfg(unix)]
    #[test]
    fn executor_env_whitelist_passes_toolchain_and_blocks_protocol_vars() {
        let mut command = Command::new("true");
        // 测试进程环境里必然存在的工具链变量应进入白名单集
        unsafe {
            std::env::set_var("CONGA_LLM_KEY", "sk-leak");
            std::env::set_var("FLOW_SECRET_X", "leak");
            std::env::remove_var("FLOW_HARNESS_AGENT_BIN");
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

    #[cfg(unix)]
    #[test]
    fn executor_env_whitelist_forwards_runner_bin_when_set() {
        let mut command = Command::new("true");
        unsafe {
            std::env::set_var("FLOW_HARNESS_AGENT_BIN", "/opt/flow-harness-agent");
        }
        apply_executor_env_whitelist(&mut command);
        let forwarded = command
            .as_std()
            .get_envs()
            .find(|(k, _)| *k == "FLOW_HARNESS_AGENT_BIN")
            .and_then(|(_, v)| v)
            .map(|v| v.to_string_lossy().into_owned());
        assert_eq!(forwarded.as_deref(), Some("/opt/flow-harness-agent"));
        unsafe {
            std::env::remove_var("FLOW_HARNESS_AGENT_BIN");
        }
    }
}
