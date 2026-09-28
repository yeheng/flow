//! flow-server：JSON-RPC WebSocket 服务进程（薄壳）。
//!
//! 入口三件套（serve / cron 调度器 / webhook HTTP）实现在
//! [`flow_rpc::run_from_env`]，与 backend-e2e 的被测进程、backend-perf 的
//! 自举服务模式共用同一份代码——生产进程与被测进程逐字一致。
//!
//! runtime 形态随部署后端切换（[`flow_backend::prefer_current_thread_runtime`]）：
//! SQLite（缺省，canonical）是「单进程·单线程·单写者」模型——整个服务跑在
//! current_thread runtime 上，配合 data_dir 排他 flock 与单连接池，把单写者
//! 假设焊成构造保证；Postgres 对等模式保持多线程。形态选择只在薄壳 main
//! 发生一次，lib 内的请求处理路径依旧不感知 FLOW_BACKEND。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = if flow_backend::prefer_current_thread_runtime() {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    }
    .enable_all()
    .build()?;
    runtime.block_on(flow_rpc::run_from_env())
}
