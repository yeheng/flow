//! backend-e2e 的被测服务进程（薄壳）。
//!
//! 与 flow-rpc 的 `flow-server` 二进制是同一份实现（[`flow_rpc::run_from_env`]），
//! 存在的唯一理由是让 backend-e2e 的集成测试可以通过
//! `env!("CARGO_BIN_EXE_flow-server")` 拿到「本 crate 自己构建的」可执行文件——
//! 跨 package 拿不到别人的 bin，也不该假设 target 目录布局。
//!
//! runtime 形态与 flow-server 逐字一致（sqlite = current_thread 单线程 runtime）：
//! 被测进程必须就是生产形态，否则 e2e 钉的契约与线上跑的不是同一种并发模型。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = if flow_rpc::prefer_current_thread_runtime() {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    }
    .enable_all()
    .build()?;
    runtime.block_on(flow_rpc::run_from_env())
}
