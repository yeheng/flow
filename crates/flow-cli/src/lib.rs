//! flow-cli：flow 工作流引擎的命令行客户端（lib）。
//!
//! 定位与边界（与 docs/DESIGN.md §2 一致）：
//! - **纯 RPC 客户端**：只连 flow-server 的 JSON-RPC 2.0 over WebSocket，
//!   不依赖 flow-backend / flow-store / flow-pg，也不读 FLOW_BACKEND——
//!   后端差异在服务端吸收，CLI 对 SQLite / Postgres 后端行为一致；
//! - 不直连 SQLite 或读 data_dir：避免了「绕过 published 校验直接操纵
//!   事件日志」的第二条路径，CRUD 语义唯一来源仍是 RPC 层那一份实现。
//!
//! 命令定义与分发在 [`cli`]；bin 入口（src/main.rs）只做参数适配与
//! 退出码传递（[`run_from_args`] 是库形态的进程入口）。

pub mod cli;
pub mod client;
pub mod error;
pub mod journal;
pub mod output;
pub mod run;
pub mod workflow;

/// 进程入口：自带多线程 runtime（与原 `#[tokio::main]` 形态一致），
/// 解析 `flow cli` 之后的参数并分发，返回进程退出码（0/1/2/4 契约）。
pub fn run_from_args(argv: &[String]) -> i32 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("cli tokio runtime");
    runtime.block_on(cli::run_from_args(argv))
}
