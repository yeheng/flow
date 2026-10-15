//! remote-e2e：分布式 E2E 套件。
//!
//! 组织方式与 backend-e2e（单机套件）对称：
//! - `tests/*`：用例。真 master + agent + executor 链路（部分用例以 in-process
//!   模拟 agent 注入身份/剪断上联，二进制进程用 `flow-test-support` 的
//!   workspace_bin 定位）；
//! - wire 级客户端助手与 backend-e2e 共用一份（`backend_e2e::common::wire`），
//!   单机/分布式两套讲同一套 v2 协议（e2e-review.md E06 的契约一致要求）。
//!
//! 运行：`cargo test -p remote-e2e`（无外部依赖；集群用例默认 cfg 门控 +
//! `--ignored`，需 PG 与 `--features journal-cluster`）。

pub mod placeholder {
    //! 占位：本 crate 只有 tests/，无库面。保留模块避免空 lib 警告。
}
