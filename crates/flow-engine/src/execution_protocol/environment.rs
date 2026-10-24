//! 所有执行器启动方式共用的环境白名单，独立于 Unix FD 承载。

use tokio::process::Command;

/// 执行器运行时环境白名单（T6 修订）：执行器仍以 `env_clear` 起步，但
/// harness（command 模式的外部 CLI 与内联 agent 的工具）需要
/// 常规工具链环境——空 `PATH`/`HOME` 的 shell 里 cargo/git/node 大面积
/// 不可用。按显式白名单从父进程透传；`FLOW_*`（协议/凭据）、`CONGA_*`
/// （harness 配置，由 executor 派发时从冻结快照构造）、`RUST_LOG`
/// （单独注入）与任何密钥类变量都不透传。例外 `FLOW_HOME`：它是
/// harness 沙箱的目录定位（无凭据语义），必须与父进程一致（测试隔离
/// 与多实例部署都靠它在父子间对齐 `~/.flow`）。remote 模式下父进程是
/// 远端机器上的 flow-agent，透传的是那台机器的环境。
#[cfg(not(windows))]
pub(crate) const EXECUTOR_ENV_WHITELIST: &[&str] = &[
    "PATH",
    "HOME",
    "FLOW_HOME",
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
    "FLOW_HOME",
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
///
/// 只透传白名单里的变量，且父进程没设的变量不设（不补空串）——补空串
/// 会让子进程误以为「变量存在但为空」，行为与「未设置」不同。
pub(crate) fn apply_executor_env_whitelist(command: &mut Command) {
    for name in EXECUTOR_ENV_WHITELIST {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_environment_preserves_only_configured_allowlisted_values() {
        // Inspect command construction without spawning or mutating process-wide env.
        let mut command = Command::new("unused-executor");
        command.env_clear();
        apply_executor_env_whitelist(&mut command);
        let actual: std::collections::BTreeMap<_, _> = command
            .as_std()
            .get_envs()
            .map(|(name, value)| (name.to_owned(), value.unwrap().to_owned()))
            .collect();
        assert!(
            !actual.is_empty(),
            "toolchain environment must be preserved"
        );
        for name in EXECUTOR_ENV_WHITELIST {
            let inherited = std::env::var_os(name);
            assert_eq!(
                actual.get(std::ffi::OsStr::new(name)),
                inherited.as_ref(),
                "{name}"
            );
        }
        for name in actual.keys() {
            assert!(EXECUTOR_ENV_WHITELIST
                .iter()
                .any(|allowed| name == std::ffi::OsStr::new(allowed)));
        }
    }

    #[test]
    fn platform_allowlist_keeps_runtime_paths_without_credentials_or_protocol_config() {
        for name in ["PATH", "FLOW_HOME"] {
            assert!(
                EXECUTOR_ENV_WHITELIST.contains(&name),
                "missing runtime path {name}"
            );
        }
        #[cfg(windows)]
        for name in [
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
            "SYSTEMROOT",
            "COMSPEC",
            "TEMP",
            "TMP",
            "PATHEXT",
        ] {
            assert!(
                EXECUTOR_ENV_WHITELIST.contains(&name),
                "missing Windows runtime path {name}"
            );
        }
        #[cfg(unix)]
        for name in ["HOME", "CARGO_HOME", "RUSTUP_HOME", "TMPDIR"] {
            assert!(
                EXECUTOR_ENV_WHITELIST.contains(&name),
                "missing Unix runtime path {name}"
            );
        }
        for name in [
            "CONGA_LLM_KEY",
            "FLOW_SECRET_X",
            "FLOW_EXECUTOR_CONNECT_ADDR",
            "FLOW_EXECUTOR_SESSION_TOKEN",
            "RUST_LOG",
            "HTTP_PROXY",
            "HTTPS_PROXY",
        ] {
            assert!(
                !EXECUTOR_ENV_WHITELIST.contains(&name),
                "must not inherit {name}"
            );
        }
    }
}
