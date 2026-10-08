//! flow-executor：受管理执行子进程（bin: flow-executor）。
//!
//! FD 槽位由主进程 pre_exec 固定（control=100，data=101），或经环境变量
//! 显式覆盖；stdout/stderr 不携带协议帧，只用于诊断输出。未知参数忽略
//! （`--tag` 等诊断标记由主进程追加）。
//!
//! 定位契约：主进程（flow-server / flow-agent）经
//! `flow_engine::execution_protocol::contract::ExecutorInvocation::locate`
//! 在自身同目录召唤本二进制，或用 FLOW_EXECUTOR_BIN 显式指定。

fn main() -> std::process::ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let control_fd = std::env::var("FLOW_EXECUTOR_CONTROL_FD")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(flow_engine::execution_protocol::EXECUTOR_CONTROL_FD_SLOT);
    let data_fd = std::env::var("FLOW_EXECUTOR_DATA_FD")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(flow_engine::execution_protocol::EXECUTOR_DATA_FD_SLOT);
    let config = flow_executor::runtime::ExecutorConfig {
        control_fd,
        data_fd,
        queue_depth: 128,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("executor tokio runtime");
    let code = runtime.block_on(flow_executor::runtime::run_executor(config));
    // 退出前确保 runtime 干净停止（在途阻塞任务随进程终止）。
    runtime.shutdown_timeout(std::time::Duration::from_millis(300));
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}
