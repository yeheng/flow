//! 非 unix 平台的执行器 spawn 传输：本地 TCP loopback 单连接回连。
//!
//! socketpair + pre_exec FD 传递是 unix 专属（见 [`super::spawn`]）；其余
//! 平台用「父进程 bind 127.0.0.1:0 → 经 env 告知子进程 → 子进程单连接
//! 回连」承载 v3 单流通道（帧内通道标签，见 [`super::frame`]）。
//!
//! 信任域：临时端口只在本机可见，且单用户本机工具的 RPC 端点、journal
//! 目录与密钥存储本就同处一个信任域（loopback + token）；unix 的
//! close-on-exec 隔离目标是「不向无关子进程泄漏 FD」，不是抵御敌意本地
//! 进程——回连端口在毫秒窗口内被抢连的前提已经是本机有敌意代码。

use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};

use super::contract::{ExecutorInvocation, STARTUP_TIMEOUT_MS};

/// 主进程告知执行器回连地址的环境变量（仅 spawn 时由父进程注入；执行器
/// env_clear 后仍可见）。
pub const CONNECT_ADDR_ENV: &str = "FLOW_EXECUTOR_CONNECT_ADDR";

/// spawn 一个执行器进程并等它回连出单通道。返回 (通道, 子进程句柄)。
///
/// 与 [`super::spawn::spawn_executor_process`] 同一套环境隔离规则：
/// env_clear 后只回填 RUST_LOG 与回连地址；stdout/stderr 不携带协议帧。
pub async fn spawn_executor_loopback(
    executor: &ExecutorInvocation,
    tag: Option<&str>,
) -> std::io::Result<(TcpStream, Child)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut command = Command::new(&executor.program);
    command.args(&executor.prefix);
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
    command.env(CONNECT_ADDR_ENV, addr.to_string());
    let mut child = command.spawn()?;
    // 单连接即整条会话；执行器起不来（缺二进制/秒退）时靠超时收口。
    let accept = async { listener.accept().await.map(|(stream, _)| stream) };
    let channel = tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS), accept).await;
    match channel {
        Ok(Ok(stream)) => {
            stream.set_nodelay(true)?;
            Ok((stream, child))
        }
        Ok(Err(error)) => {
            let _ = child.start_kill();
            Err(error)
        }
        Err(_) => {
            let _ = child.start_kill();
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "executor loopback connect timeout",
            ))
        }
    }
}

/// 执行器侧：按 [`CONNECT_ADDR_ENV`] 指示的地址回连主进程（单连接）。
pub async fn connect_loopback(addr: &str) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
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
            let (mut channel, mut child) = spawn_executor_loopback(&executor, None)
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
}
