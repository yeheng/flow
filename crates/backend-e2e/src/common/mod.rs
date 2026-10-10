//! journal v2 e2e 的公共 harness：被测服务进程、v2 RPC 客户端、fixtures。
//!
//! 设计约束（与仓库测试约定一致，DESIGN.md §13）：
//! - 真起 `flow-journal-server` 进程：崩溃恢复只能靠真的 SIGKILL 进程证明；
//! - 每个用例独占一个临时 journal 目录 + 随机 v2 部署 token，只清理该目录；
//! - 客户端走真实 wire 并讲 v2 协议（token / request_id / 回执 / -32020 恢复），
//!   断言的是产品契约，不是内部 API。
//!
//! 用例通过 [`run_case`] 运行：panic 也会先走到清理再恢复 unwind，
//! 不会把服务进程 / 临时目录漏在磁盘上。

pub mod client;
pub mod fixtures;
pub mod server;

pub use client::{
    call, call_err, call_json, collect_run_events, connect, http_post_hook, named,
    publish_workflow, start_run, subscribe, try_call_json, wait_run_status, wait_run_terminal,
    Conn, SHORT, TIMEOUT,
};
pub use server::{run_case, run_case_with_env, Ctx, ServerProc};
