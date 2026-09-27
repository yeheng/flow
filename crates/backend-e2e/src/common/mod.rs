//! 双后端 e2e 的公共 harness：docker 容器、测试库、服务进程、RPC 客户端。
//!
//! 设计约束（与仓库测试约定一致，DESIGN.md §13）：
//! - 真起 `flow-server` 进程：崩溃恢复只能靠真的 SIGKILL 进程证明；
//! - SQLite 用例用独占临时目录的 `flow.db`，只清理该目录，绝不碰系统临时目录本身；
//! - Postgres 用例每个用例一个独占数据库（`e2e_<ts>_<uuid>`），用完即 DROP；
//! - Postgres 服务器由本 crate 用 docker CLI 管理：进程退出（atexit）与
//!   下次启动（残留清扫）双保险，保证「每次运行后不遗留容器」。
//!
//! 用例通过 [`run_case`] 运行：panic 也会先走到清理再恢复 unwind，
//! 不会把服务进程 / 测试库 / 临时目录漏在磁盘上。

pub mod client;
pub mod container;
pub mod db;
pub mod fixtures;
pub mod server;

pub use client::{
    call, call_err, call_json, call_null_params, collect_run_events, connect, err_code,
    http_post_hook, named, publish_workflow, start_run, subscribe, wait_run_status,
    wait_run_terminal, Client, SHORT, TIMEOUT,
};
pub use container::PgContainer;
pub use db::TestDb;
pub use server::{run_case, run_case_with_env, spawn_pg_server, Ctx, Kind, ServerProc};
