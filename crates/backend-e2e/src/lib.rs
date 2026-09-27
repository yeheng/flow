//! backend-e2e：后端契约的端到端测试项目。
//!
//! 组织方式：
//! - `common`：公共 harness（容器、测试库、服务进程、RPC 客户端、fixtures）；
//! - `src/bin/flow-server.rs`：被测服务进程（与 flow-rpc 的 bin 等价，
//!   让本 crate 的测试能用 `CARGO_BIN_EXE_flow-server` 定位）；
//! - `tests/*.rs`：用例，用 [`e2e_test!`] 生成的用例对 SQLite / Postgres
//!   两个后端各跑一遍，钉死「两个后端同一套契约」。
//!
//! 运行：`cargo test -p backend-e2e`（需要 docker；Postgres 由容器提供，
//! 运行结束自动清理，见 `common::container` 的三层清理策略）。

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
