//! backend-e2e：单机 E2E 套件（v2 产品面）。
//!
//! 组织方式（测试基建的真身集中在 `flow-test-support`，这里只放 e2e 专属的部分）：
//! - `common`：公共 harness（被测 flow-journal-server 进程、v2 RPC 客户端、
//!   fixtures）；`common::wire` 是原 flow-rpc 进程测试的 wire 助手（供迁移
//!   用例与 remote-e2e 套件共用）；
//! - `src/bin/flow-journal-server-e2e.rs`：被测服务进程（与 flow-rpc 的
//!   `flow-journal-server` 是同一份 `serve_journal_product` 实现，只是包了一层
//!   壳并改名，避免与产品 bin 同名撞 `target/debug/flow-journal-server`），
//!   让本 crate 的测试能用 `CARGO_BIN_EXE_flow-journal-server-e2e` 定位；
//!   注意：**薄壳只读环境变量、不解析 --config**（与产品 bin 的差异），
//!   需要 `--config` 的用例请改用 `flow_test_support::io::server_bin()`；
//! - `tests/*.rs`：两类用例——`e2e_test!` 宏的 journal v2 产品面进程测试；
//!   以及自产品 crate 迁入的单机集成面（`journal_*` 后端/IPC、`journal_v2` /
//!   `ws_journal` / `sigkill` / `webhook` / `contracts` wire 契约、`cli*`
//!   子进程往返）。分布式（agent/集群）用例在 remote-e2e 套件。
//!
//! 运行：`cargo test -p backend-e2e`（无外部依赖；崩溃恢复用 SIGKILL 真杀；
//! 覆盖矩阵见 docs/e2e-coverage.md）。

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
