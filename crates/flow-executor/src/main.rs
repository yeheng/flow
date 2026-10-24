//! flow-executor：受管理执行子进程（bin: flow-executor）。
//!
//! 通道引导两种形态：unix 由主进程 argv 传入 `--channel-fd <N>`（socketpair
//! 子端原号，无 CLOEXEC，pre_exec 后其余 FD 已关闭）；其余平台经
//! `FLOW_EXECUTOR_CONNECT_ADDR` 单连接回连 loopback。stdout/stderr 不携带
//! 协议帧，只用于诊断输出；未知参数忽略（`--tag` 等诊断标记由主进程追加）。
//!
//! 定位契约：主进程（flow-server / flow-agent）经
//! `flow_engine::execution_protocol::contract::ExecutorInvocation::locate`
//! 在自身同目录召唤本二进制，或用 FLOW_EXECUTOR_BIN 显式指定。

use flow_executor::runtime::{ChannelBootstrap, ExecutorConfig};

/// 进程入口：装好诊断日志 → 解析通道引导 → 建 Tokio 运行时 → 跑会话。
///
/// 退出码语义由主进程按契约解读：`0` 干净结束，`71` 引导失败（无可用
/// 通道），`72`/`73` 握手被拒/超时，其余为会话期错误码（见
/// [`flow_executor::runtime::run_executor`]）。
fn main() -> std::process::ExitCode {
    // 主进程把 stdout 接到 null（协议帧不走 stdio）；诊断日志必须走 stderr，
    // 否则 tracing 默认的 stdout 会被丢进 /dev/null（挂死排查时不可见）。
    // 多执行器并存时用 PID 区分进程（测试的 nocapture 会汇合多个子进程 stderr）。
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    tracing::info!(pid = std::process::id(), "executor process starting");
    let Some(config) = bootstrap_config() else {
        return std::process::ExitCode::from(71);
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("executor tokio runtime");
    // 会话循环在多线程运行时上跑：节点语义虽在 spawn_blocking 里执行，
    // 但心跳、通道收发与看门狗交互都要独立推进，单线程运行时会被阻塞式
    // 用户代码饿死心跳（心跳一停，主进程即判失联）。
    let code = runtime.block_on(flow_executor::runtime::run_executor(config));
    // 退出前确保 runtime 干净停止（在途阻塞任务随进程终止）。
    runtime.shutdown_timeout(std::time::Duration::from_millis(300));
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}

/// 解析通道引导：回连地址 env 优先（非 unix 的唯一形态）；否则 unix
/// `--channel-fd <N>`（主进程 spawn 时注入 argv）。回连形态同时携带
/// 会话令牌（Hello 回验，防本机抢连）；socketpair 形态走 FD 能力，无令牌。
fn bootstrap_config() -> Option<ExecutorConfig> {
    // --remote-relay 由召唤方按调用点追加：只有 agent 上联形态需要应用层
    // 重放（结果帧/审计帧可能在断线中丢失），本地形态走幂等重传即可。
    let remote_replay = std::env::args().any(|arg| arg == "--remote-relay");
    // 会话令牌是 loopback 回连形态的认领凭据：防同机其他进程抢连这条
    // 通道。空串视同没有令牌（不参与回验）。
    let session_token = std::env::var(flow_engine::execution_protocol::SESSION_TOKEN_ENV)
        .ok()
        .filter(|token| !token.is_empty());
    if let Ok(addr) = std::env::var(flow_engine::execution_protocol::CONNECT_ADDR_ENV) {
        if !addr.is_empty() {
            return Some(ExecutorConfig {
                bootstrap: ChannelBootstrap::Loopback { addr },
                // 出站队列深度：控制帧与 data 帧共用一条通道，深度决定
                // 「传输满时能缓冲多少」；与本地形态取同值以免两套心智。
                queue_depth: 128,
                remote_replay,
                session_token,
            });
        }
    }
    #[cfg(unix)]
    {
        let args: Vec<String> = std::env::args().collect();
        // socketpair 子端的原号由主进程以 argv 传入：FD 本身就是能力，
        // 不需要额外令牌（本机其他进程拿不到这个 FD）。
        let channel_fd = args
            .windows(2)
            .find(|w| w[0] == "--channel-fd")
            .and_then(|w| w[1].parse::<i32>().ok())?;
        Some(ExecutorConfig {
            bootstrap: ChannelBootstrap::Fd { channel_fd },
            queue_depth: 128,
            remote_replay,
            session_token: None,
        })
    }
    #[cfg(not(unix))]
    {
        eprintln!(
            "flow-executor: {} 未设置（本平台本地执行只走 loopback 回连）",
            flow_engine::execution_protocol::CONNECT_ADDR_ENV
        );
        None
    }
}
