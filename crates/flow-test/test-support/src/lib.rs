//! 测试基建：产品无关的临时目录 / 端口等待。
//!
//! 这些代码**不属于任何产品 crate**，却会被每个测试二进制各编一份
//! （`tests/` 下的模块在每个集成测试 target 里都要重新编译），散着写必然
//! 各自漂移。集中到这一个 publish = false 的 crate：单点维护，依赖方向保持
//! 「测试 → 基建 → 产品」，反过来不行。
//!
//! - [`io`]：临时目录守卫、空闲端口、就绪等待。
//!
//! （v1 Postgres 后端的 docker 测试库基建已随后端一并删除。）

pub mod io;

pub mod provider;

// ---------------------------------------------------------------------------
// e2e 公共 harness（自 backend-e2e/src/common 迁入，T2）
//
// 契约纪律：support 不依赖任何被测实现（backend/rpc/engine/executor）；
// 对服务端的唯一契约引用是 flow_dto::rpc::is_write_method（产品词汇表）。
// ---------------------------------------------------------------------------
pub mod client;
pub mod fixtures;
pub mod server;
pub mod wire;

pub use client::{
    call, call_err, call_json, collect_run_events, connect, http_post_hook, named,
    publish_workflow, start_run, subscribe, try_call_json, wait_run_status, wait_run_terminal,
    Conn, SHORT, TIMEOUT,
};
pub use server::{run_case, run_case_with_env, Ctx, ServerProc};
