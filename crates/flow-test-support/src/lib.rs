//! 测试基建：产品无关的临时目录 / 端口等待，以及（可选）Postgres 测试库。
//!
//! 这些代码**不属于任何产品 crate**，却一度散落在 `flow-pg/tests/common/`、
//! `flow-rpc/tests/ws_*.rs`、`backend-e2e/src/common/`、以及若干 `#[cfg(test)]`
//! 模块里，每个测试二进制各编一份、各自漂移。集中到这一个 publish = false 的
//! crate 之后：单点维护，依赖方向保持「测试 → 基建 → 产品」，反过来不行。
//!
//! - [`io`]：临时目录守卫、空闲端口、就绪等待（零依赖，默认就带上）；
//! - [`pg`]：docker 一次性容器 + 每用例独占测试库 + 孤儿 volume 回收
//!   （需要 docker CLI / sqlx，用 `pg` 特性关掉；见 Cargo.toml）。

pub mod io;
#[cfg(feature = "pg")]
pub mod pg;
