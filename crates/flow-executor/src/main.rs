//! flow-executor 二进制入口（I03/I09）。
//!
//! FD 槽位由主进程 pre_exec 固定（control=100，data=101），或经环境变量
//! 显式覆盖。stdin 不作为协议通道；stdout/stderr 不携带协议帧，只用于
//! 诊断输出。

use flow_executor::runtime::{run_executor, ExecutorConfig};

fn main() {
    // 未知参数（如 --tag 诊断标记）忽略。
    let _ = std::env::args();
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
    let config = ExecutorConfig {
        control_fd,
        data_fd,
        queue_depth: 128,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("executor tokio runtime");
    let code = runtime.block_on(run_executor(config));
    // 退出前确保 runtime 干净停止（在途阻塞任务随进程终止）。
    runtime.shutdown_timeout(std::time::Duration::from_millis(300));
    std::process::exit(code);
}
