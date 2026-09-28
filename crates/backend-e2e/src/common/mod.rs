//! 双后端 e2e 的公共 harness：被测服务进程、RPC 客户端、fixtures。
//!
//! docker 容器与测试库不在本 crate 里维护——它们在 `flow-test-support::pg`
//! （flow-pg / flow-rpc 的测试同样指着那一份），避免同一套生命周期被复制第
//! 三遍。这里只保留 e2e 专属的部分：真起 flow-server 进程、跑用例体并保证
//! 清理、以及定义构造器。
//!
//! 设计约束（与仓库测试约定一致，DESIGN.md §13）：
//! - 真起 `flow-server` 进程：崩溃恢复只能靠真的 SIGKILL 进程证明；
//! - SQLite 用例用独占临时目录的 `flow.db`，只清理该目录，绝不碰系统临时目录本身；
//! - Postgres 用例每个用例一个独占数据库（`e2e_<ts>_<uuid>`），用完即 DROP；
//! - Postgres 服务器由 `flow-test-support::pg` 用 docker CLI 管理：进程退出
//!   （atexit）与下次启动（残留清扫）双保险，容器数据目录挂 tmpfs、删除一律
//!   `rm -f -v`、启动时回收孤儿匿名 volume——保证「每次运行后不遗留容器、
//!   不留 volume 垃圾」。
//!
//! 用例通过 [`run_case`] 运行：panic 也会先走到清理再恢复 unwind，
//! 不会把服务进程 / 测试库 / 临时目录漏在磁盘上。

pub mod client;
pub mod fixtures;
pub mod server;

/// e2e 测试库名前缀：残留清扫只动这个前缀的库（`flow_test_%` 是 flow-pg /
/// flow-rpc 那一套的，两边各自认各自的名字，互不误伤）。
pub const E2E_DB_PREFIX: &str = "e2e_";

pub use client::{
    call, call_err, call_json, call_null_params, collect_run_events, connect, err_code,
    http_post_hook, named, publish_workflow, start_run, subscribe, wait_run_status,
    wait_run_terminal, Client, SHORT, TIMEOUT,
};
pub use flow_test_support::pg::{free_port, shared, PgContainer, TestDb};
pub use server::{run_case, run_case_with_env, spawn_pg_server, Ctx, Kind, ServerProc};
