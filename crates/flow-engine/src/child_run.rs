//! sub_workflow 节点的子 run 启动边界。
//!
//! Driver 拿不到引擎句柄（DriverSpec 只有 run_id/definition/input），
//! 单机（flow-rpc）与 Postgres（flow-pg）各自实现本 trait：
//! 单机实现同进程复用 Engine，Postgres 实现经 gateway 事务创建 + 轮询共享日志。
//!
//! 幂等性：child_run_id 由 Driver 确定性派生（`{父run}:{节点}:{attempt}`）并随
//! node_started 落盘；崩溃重放沿用同一 id，`start` 撞 `RunExists` 时调用方附着等待。

use futures::future::BoxFuture;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::EngineError;

/// 嵌套深度上限：validate 检不了跨 definition 的循环引用，运行时深度兜底。
pub const MAX_SUB_WORKFLOW_DEPTH: u32 = 8;

/// 子 run 的终态结果。
#[derive(Debug, Clone, PartialEq)]
pub enum ChildRunOutcome {
    Succeeded(Value),
    Failed(String),
    Cancelled,
}

pub trait ChildRunLauncher: Send + Sync {
    /// 启动子 run。`depth` 是子 run 的深度（调用方已算好父深度 + 1）。
    /// child_run_id 已存在时返回 `EngineError::RunExists`，由执行层附着等待。
    fn start<'a>(
        &'a self,
        child_run_id: &'a str,
        workflow_id: &'a str,
        input: Value,
        depth: u32,
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    /// 等待子 run 到达终态；`cancel` 触发后应尽快返回 `ChildRunOutcome::Cancelled`。
    fn await_terminal<'a>(
        &'a self,
        child_run_id: &'a str,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<ChildRunOutcome, EngineError>>;

    /// best-effort 取消级联：父 run 取消时尽量取消子 run，失败由实现方记日志。
    fn cancel<'a>(&'a self, child_run_id: &'a str) -> BoxFuture<'a, ()>;
}
