//! backend-e2e：v2 产品面的端到端测试项目。
//!
//! 组织方式（测试基建的真身集中在 `flow-test-support`，这里只放 e2e 专属的部分）：
//! - `common`：公共 harness（被测 flow-journal-server 进程、v2 RPC 客户端、
//!   fixtures）；
//! - `src/bin/flow-journal-server-e2e.rs`：被测服务进程（与 flow-rpc 的
//!   `flow-journal-server` 是同一份 `serve_journal_product` 实现，只是包了一层
//!   壳并改名，避免与产品 bin 同名撞 `target/debug/flow-journal-server`），
//!   让本 crate 的测试能用 `CARGO_BIN_EXE_flow-journal-server-e2e` 定位；
//! - `tests/*.rs`：用例，用 [`e2e_test!]` 对 journal v2 产品面跑一遍
//!   （v1 RPC 面（flow-server）与 Postgres 后端均已删除）。
//!
//! 运行：`cargo test -p backend-e2e`（无外部依赖；崩溃恢复用 SIGKILL 真杀）。

pub mod common;

/// 用例宏：journal v2 产品面（`flow-journal-server-e2e` 真起进程 + SIGKILL
/// 重启恢复 + 全量 wire 断言）。
#[macro_export]
macro_rules! e2e_test {
    ($name:ident, $body:expr) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn journal() {
                $crate::common::run_case(env!("CARGO_BIN_EXE_flow-journal-server-e2e"), $body)
                    .await;
            }
        }
    };
}
