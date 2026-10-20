//! 回连承载：本地 socket（IPC7，替代旧版 TCP loopback）。
//!
//! socketpair + pre_exec FD 传递是 unix 专属（见 [`super::spawn`]）；其余
//! 平台（Windows named pipe）与本模块自身的测试路径用「父进程先建
//! listener → 经 env 告知子进程名 → 子进程单连接回连」承载单流通道。
//! 相比 TCP loopback：不占用网络栈、无本机端口抢连窗口（named pipe 按
//! 对象名寻址，fs socket 按路径寻址）。会话令牌仍保留（spawn 时随机生成
//! 经 env 注入，Hello 回验，见 [`SESSION_TOKEN_ENV`]）作纵深防御——同机
//! 其他进程理论上仍可抢连知名对象，令牌把门外汉挡在握手。
//!
//! 名字按平台选择：Windows 用 namespaced（named pipe 名）；unix 用文件
//! 系统 socket 路径（仅测试/回退路径——生产 unix 本地执行走 socketpair，
//! 不经过本模块）。承载类型与名字类型由 interprocess 统一。
//!
//! unix socket 文件的收尾：interprocess 的 Listener drop 已实现名字回收
//!（drop 时 `remove_file`，见其「Name reclamation」文档），进程不崩溃就
//! 不会在临时目录累积 `.sock`；崩溃残留需人工清（随机后缀防撞名）。
//! 本模块的测试断言 spawn 返回后路径已不存在。

use std::time::Duration;

use crate::process::ManagedChild as Child;
use interprocess::local_socket::tokio::Stream;
use interprocess::local_socket::traits::tokio::{Listener as _, Stream as _};
use interprocess::local_socket::ListenerOptions;
use tokio::process::Command;

use super::contract::{ExecutorInvocation, SESSION_TOKEN_ENV, STARTUP_TIMEOUT_MS};

/// 主进程告知执行器回连地址的环境变量（仅 spawn 时由父进程注入；执行器
/// env_clear 后仍可见）。值在 Windows 是 named pipe 名，unix 是 socket 路径。
pub const CONNECT_ADDR_ENV: &str = "FLOW_EXECUTOR_CONNECT_ADDR";

/// 一次性回连名：Windows named pipe（namespaced），unix 文件系统 socket
/// 路径。随机后缀防撞名/抢注。
fn fresh_local_name() -> String {
    let unique = super::transport::fresh_boot_id();
    if cfg!(windows) {
        format!("flow-executor-{unique}")
    } else {
        std::env::temp_dir()
            .join(format!("flow-executor-{unique}.sock"))
            .display()
            .to_string()
    }
}

/// 把回连地址（env 值）解析为本平台本地 socket 名。
fn parse_local_name(addr: &str) -> std::io::Result<interprocess::local_socket::Name<'_>> {
    #[cfg(windows)]
    {
        use interprocess::local_socket::{GenericNamespaced, ToNsName};
        addr.to_ns_name::<GenericNamespaced>()
    }
    #[cfg(unix)]
    {
        use interprocess::local_socket::{GenericFilePath, ToFsName};
        addr.to_fs_name::<GenericFilePath>()
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = addr;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no local socket backend on this platform",
        ))
    }
}

/// spawn 一个执行器进程并等它回连出单通道。返回 (通道, 子进程句柄)。
/// `extra_args` 为调用点显式指定的模式/标记参数（见
/// [`super::spawn::spawn_executor_process`]）。
///
/// 与 [`super::spawn::spawn_executor_process`] 同一套环境隔离规则：
/// env_clear 后只回填 RUST_LOG、回连名与会话令牌；stdout/stderr 不
/// 携带协议帧。
pub async fn spawn_executor_loopback(
    executor: &ExecutorInvocation,
    tag: Option<&str>,
    session_token: String,
    extra_args: &[&str],
) -> std::io::Result<(Stream, Child)> {
    let addr = fresh_local_name();
    spawn_executor_loopback_at(&addr, executor, tag, session_token, extra_args).await
}

/// 指定回连名的 spawn（测试用：名字已知才能断言 socket 文件收尾）。
async fn spawn_executor_loopback_at(
    addr: &str,
    executor: &ExecutorInvocation,
    tag: Option<&str>,
    session_token: String,
    extra_args: &[&str],
) -> std::io::Result<(Stream, Child)> {
    let listener = ListenerOptions::new()
        .name(parse_local_name(addr)?)
        .create_tokio()?;
    let mut command = Command::new(&executor.program);
    command.args(extra_args);
    if let Some(tag) = tag {
        command.arg("--tag").arg(tag);
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    command.env_clear();
    if let Some(log) = std::env::var_os("RUST_LOG") {
        command.env("RUST_LOG", log);
    }
    super::spawn::apply_executor_env_whitelist(&mut command);
    command.env(CONNECT_ADDR_ENV, addr);
    command.env(SESSION_TOKEN_ENV, &session_token);
    let mut child = Child::spawn_executor(&mut command)?;
    // 单连接即整条会话；执行器起不来（缺二进制/秒退）时靠超时收口。
    let accept = async { listener.accept().await };
    let channel = tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS), accept).await;
    match channel {
        Ok(Ok(stream)) => Ok((stream, child)),
        Ok(Err(error)) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(error)
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "executor loopback connect timeout",
            ))
        }
    }
}

/// 执行器侧：按 [`CONNECT_ADDR_ENV`] 指示的地址回连主进程（单连接）。
pub async fn connect_loopback(addr: &str) -> std::io::Result<Stream> {
    Stream::connect(parse_local_name(addr)?).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_spawn_establishes_single_channel() {
        // 子进程用例：spawn_executor_loopback 注入 CONNECT_ADDR_ENV——测试
        // 二进制看到它就扮演执行器（收 1 字节回显）后返回。
        if let Ok(addr) = std::env::var(CONNECT_ADDR_ENV) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("child runtime");
            runtime.block_on(async {
                let mut channel = connect_loopback(&addr).await.unwrap();
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut byte = [0u8; 1];
                channel.read_exact(&mut byte).await.unwrap();
                channel.write_all(&byte).await.unwrap();
            });
            return;
        }
        // 父进程：以自身测试二进制扮演执行器，验证 spawn→单连接回连→回显。
        let executor = super::super::contract::ExecutorInvocation::explicit(
            std::env::current_exe().expect("test binary"),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("parent runtime");
        runtime.block_on(async {
            let (mut channel, mut child) =
                spawn_executor_loopback(&executor, None, "test-session-token".into(), &[])
                    .await
                    .expect("spawn");
            assert!(child.id().is_some(), "child process handle");
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            channel.write_all(b"Z").await.unwrap();
            let mut echoed = [0u8; 1];
            tokio::time::timeout(Duration::from_secs(5), channel.read_exact(&mut echoed))
                .await
                .expect("echo within timeout")
                .unwrap();
            assert_eq!(echoed, *b"Z", "回显证明单连接双向可用");
            let _ = child.start_kill();
            let _ = child.wait().await;
        });
    }

    /// unix socket 文件不残留：interprocess 的 Listener drop 回收名字
    ///（drop 时 remove_file）。用已知名字的入口断言「listen 期间路径存在、
    /// spawn 返回后路径消失」——每次 spawn 留一个文件不是小事。
    #[cfg(unix)]
    #[test]
    fn loopback_socket_file_is_reclaimed_on_drop() {
        if std::env::var(CONNECT_ADDR_ENV).is_ok() {
            return;
        }
        let unique = super::super::transport::fresh_boot_id();
        let dir = std::env::temp_dir().join(format!("flow-loop-{unique}"));
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join("sock.txt");
        let exec = script(
            &dir,
            &format!(
                "test -S \"${env}\" && echo present > {marker}\n\
                 exec python3 -c 'import os,socket; a=os.environ[\"{env}\"]; s=socket.socket(socket.AF_UNIX); s.connect(a); s.close()'",
                env = CONNECT_ADDR_ENV,
                marker = marker.display(),
            ),
        );
        let addr = fresh_local_name();
        let invocation = super::super::contract::ExecutorInvocation::explicit(exec);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (_channel, mut child) =
                spawn_executor_loopback_at(&addr, &invocation, None, "tok".into(), &[])
                    .await
                    .expect("loopback session");
            let _ = child.start_kill();
            let _ = child.wait().await;
        });
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap_or_default().trim(),
            "present",
            "listen 期间 socket 路径必须存在"
        );
        assert!(
            !std::path::Path::new(&addr).exists(),
            "spawn 返回后 socket 路径必须被回收"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    fn script(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("fake-executor");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// T10：回连承载的 env_clear 隔离（父进程无关变量不泄漏；回连地址注入）。
    /// 子进程用 python 的 AF_UNIX socket 回连（回连名是 fs socket 路径）。
    #[cfg(unix)]
    #[test]
    fn loopback_child_env_is_cleared() {
        let dir = std::env::temp_dir().join(format!("flow-loop-{}", fresh_local_name()));
        std::fs::create_dir_all(&dir).unwrap();
        let env_dump = dir.join("env.txt");
        let exec = script(
            &dir,
            &format!(
                "env > {dump}\nexec python3 -c 'import os,socket; a=os.environ[\"{env}\"]; s=socket.socket(socket.AF_UNIX); s.connect(a); s.sendall(b\"ping\"); s.close()'",
                dump = env_dump.display(),
                env = CONNECT_ADDR_ENV,
            ),
        );
        std::env::set_var("FLOW_T10_LOOP_LEAK", "must-not-leak");
        let invocation = super::super::contract::ExecutorInvocation::explicit(exec);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            use tokio::io::AsyncReadExt;
            let (mut channel, mut child) =
                spawn_executor_loopback(&invocation, None, "tok".into(), &[])
                    .await
                    .expect("loopback session");
            let mut buf = [0u8; 4];
            tokio::time::timeout(Duration::from_secs(5), channel.read_exact(&mut buf))
                .await
                .expect("child must connect back")
                .unwrap();
            assert_eq!(&buf, b"ping");
            let _ = child.start_kill();
            let _ = child.wait().await;
        });
        std::env::remove_var("FLOW_T10_LOOP_LEAK");
        let dump = std::fs::read_to_string(&env_dump).unwrap_or_default();
        assert!(dump.contains(CONNECT_ADDR_ENV), "回连地址必须注入:\n{dump}");
        assert!(
            dump.contains(SESSION_TOKEN_ENV),
            "会话令牌必须注入（抢连防御）：\n{dump}"
        );
        assert!(
            !dump.contains("FLOW_T10_LOOP_LEAK"),
            "env_clear 必须丢弃父进程无关变量:\n{dump}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// T10：缺二进制/子进程秒退在回连侧必须有界失败（不永久挂起）。
    #[test]
    fn loopback_startup_failures_are_bounded() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let missing =
                super::super::contract::ExecutorInvocation::explicit("/nonexistent/flow-executor");
            assert!(spawn_executor_loopback(&missing, None, "tok".into(), &[])
                .await
                .is_err());
        });
        #[cfg(unix)]
        {
            let dir = std::env::temp_dir().join(format!("flow-loop-{}", fresh_local_name()));
            std::fs::create_dir_all(&dir).unwrap();
            let exec = script(&dir, "exit 1");
            let invocation = super::super::contract::ExecutorInvocation::explicit(exec);
            let start = std::time::Instant::now();
            let result = runtime.block_on(spawn_executor_loopback(
                &invocation,
                None,
                "tok".into(),
                &[],
            ));
            assert!(result.is_err(), "child exit must surface as error");
            assert!(
                start.elapsed() < Duration::from_secs(20),
                "connect timeout must be bounded"
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
