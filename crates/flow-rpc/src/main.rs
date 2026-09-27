//! flow-server：JSON-RPC WebSocket 服务进程（薄壳）。
//!
//! 入口三件套（serve / cron 调度器 / webhook HTTP）实现在
//! [`flow_rpc::run_from_env`]，与 backend-e2e 的被测进程、backend-perf 的
//! 自举服务模式共用同一份代码——生产进程与被测进程逐字一致。

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    flow_rpc::run_from_env().await
}
