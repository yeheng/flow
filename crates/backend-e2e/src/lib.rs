//! backend-e2e：后端契约的端到端测试项目。
//!
//! 组织方式（测试基建的真身集中在 `flow-test-support`，这里只放 e2e 专属的部分）：
//! - `common`：公共 harness（被测服务进程、RPC 客户端、fixtures）；
//! - `docker` / 测试库：来自 [`flow_test_support::pg`]（容器 + 每用例独占库
//!   + 孤儿 volume 回收），与 flow-pg / flow-rpc 的测试同指一份；
//! - `src/bin/flow-server.rs`：被测服务进程（与 flow-rpc 的 bin 等价，
//!   让本 crate 的测试能用 `CARGO_BIN_EXE_flow-server` 定位）；
//! - `tests/*.rs`：用例，用 [`e2e_test!`] 生成的用例对 SQLite / Postgres
//!   两个后端各跑一遍，钉死「两个后端同一套契约」。
//!
//! 运行：`cargo test -p backend-e2e`（需要 docker；Postgres 由容器提供，
//! 运行结束自动清理，见 flow-test-support/src/pg.rs 的三层清理策略）。

pub mod common;

#[macro_export]
macro_rules! e2e_test {
    ($name:ident, $body:expr) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn sqlite() {
                $crate::common::run_case(
                    $crate::common::Kind::Sqlite,
                    env!("CARGO_BIN_EXE_flow-server"),
                    $body,
                )
                .await;
            }

            #[tokio::test]
            async fn postgres() {
                $crate::common::run_case(
                    $crate::common::Kind::Postgres,
                    env!("CARGO_BIN_EXE_flow-server"),
                    $body,
                )
                .await;
            }
        }
    };
}
