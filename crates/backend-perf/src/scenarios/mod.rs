//! 压测场景。每个场景独占一个被测进程上下文（临时目录 / 测试库即建即毁），
//! 场景之间的数据量互不污染——读路径延迟只跟本场景预置的数据量有关。
//!
//! - [`throughput`]：run 执行吞吐（chain / fanout 两种定义形态）；
//! - [`read`]：RPC 读路径延迟（run.get / list / stats / timeline / events）；
//! - [`subscribe`]：订阅推送延迟（落账 → 事件到达客户端）；
//! - [`recovery`]：崩溃恢复耗时（SIGKILL → 重启就绪 + 恢复后推进）。

pub mod read;
pub mod recovery;
pub mod subscribe;
pub mod throughput;

use backend_e2e::common::Ctx;

use crate::opts::{Opts, Scenario};
use crate::report::Report;

pub async fn run(scenario: Scenario, ctx: &mut Ctx, opts: &Opts) -> Vec<Report> {
    match scenario {
        Scenario::Throughput => throughput::run(ctx, opts).await,
        Scenario::Read => read::run(ctx, opts).await,
        Scenario::Subscribe => subscribe::run(ctx, opts).await,
        Scenario::Recovery => recovery::run(ctx, opts).await,
    }
}
