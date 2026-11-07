//! backend-e2e：单机 E2E 套件（v2 产品面）。公共 harness 在
//! flow-test-support（T2 迁出）；本包只拥有用例与被测服务薄壳 bin。

/// 用例宏：journal v2 产品面（`flow-journal-server-e2e` 真起进程 + SIGKILL
/// 重启恢复 + 全量 wire 断言）。
#[macro_export]
macro_rules! e2e_test {
    ($name:ident, $body:expr) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn journal() {
                ::flow_test_support::run_case(env!("CARGO_BIN_EXE_flow-journal-server-e2e"), $body)
                    .await;
            }
        }
    };
}
