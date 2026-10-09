//! backend-e2e：后端契约的端到端测试项目。
//!
//! 组织方式（测试基建的真身集中在 `flow-test-support`，这里只放 e2e 专属的部分）：
//! - `common`：公共 harness（被测服务进程、RPC 客户端、fixtures）；
//! - `docker` / 测试库：来自 [`flow_test_support::pg`]（容器 + 每用例独占库
//!   + 孤儿 volume 回收），与 flow-pg / flow-rpc 的测试同指一份；
//! - `src/bin/flow-server-e2e.rs`：被测服务进程（与 flow-rpc 的 `flow-server`
//!   是同一份实现，只是包了一层壳并改名，避免与产品 bin 同名撞
//!   `target/debug/flow-server`），让本 crate 的测试能用
//!   `CARGO_BIN_EXE_flow-server-e2e` 定位；
//! - `tests/*.rs`：用例，用 [`e2e_test!`] 生成的用例对 SQLite / Postgres /
//!   Journal 三个后端各跑一遍，钉死「后端同一套契约」（journal 的已知语义
//!   差异在用例体内按 `ctx.kind` 分支断言，差异清单见
//!   docs/SQLITE_V1_TO_V2_MIGRATION.md §3）。
//!
//! 运行：`cargo test -p backend-e2e`（需要 docker；Postgres 由容器提供，
//! 运行结束自动清理，见 flow-test-support/src/pg.rs 的三层清理策略）。

pub mod common;

/// v1 引擎语义专属用例（Postgres 臂）：钉 v1 引擎的外部操作自动重试
/// （5xx/拒连/截断重试）——v2 的安全立场是外部操作失败一律进 uncertain
/// 等人工裁决（差异清单见 docs/SQLITE_V1_TO_V2_MIGRATION.md §3）。
/// SQLite 臂已随 v1 后端删除；Postgres 共享同一 v1 引擎语义。
#[macro_export]
macro_rules! e2e_test_v1_only {
    ($name:ident, $body:expr) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn postgres() {
                $crate::common::run_case(
                    $crate::common::Kind::Postgres,
                    env!("CARGO_BIN_EXE_flow-server-e2e"),
                    $body,
                )
                .await;
            }
        }
    };
}

#[macro_export]
macro_rules! e2e_test {
    ($name:ident, $body:expr) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn postgres() {
                $crate::common::run_case(
                    $crate::common::Kind::Postgres,
                    env!("CARGO_BIN_EXE_flow-server-e2e"),
                    $body,
                )
                .await;
            }

            #[tokio::test]
            async fn journal() {
                $crate::common::run_case(
                    $crate::common::Kind::Journal,
                    env!("CARGO_BIN_EXE_flow-server-e2e"),
                    $body,
                )
                .await;
            }
        }
    };
}
