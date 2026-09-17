//! Phase 0 后端边界：run 事件出口的最小接口。
//!
//! 这不是把文件 append 改名为 trait——接口必须表达（DISTRIBUTED.md §11 Phase 0）：
//! - **受保护提交**：`append` 在单机后端是逐事件 fsync，在 Postgres 后端是
//!   「锁 runs 行 → 校验租约代次 → 分配 seq → 插入事件」的同一事务，
//!   失败时以 `EngineError::LeaseLost` 表示所有权已转移；
//! - **终态原子性**：`append_terminal` 把终态事件、元数据投影、租约释放和
//!   剩余 pending 输入的拒绝放在一个提交点；
//! - **信号消费**：`commit_signal` / `reject_signal` / `consume_cancel` 对应
//!   持久 inbox 的原子消费协议（§6.2）。
//!
//! Driver 只依赖本 trait。单机后端见 `engine::FileSink`，Postgres 后端见 flow-pg。

use futures::future::BoxFuture;
use serde_json::Value;

use crate::engine::DbRunStatus;
use crate::error::EngineError;
use crate::event::{Envelope, Event};

/// 持久 inbox 里的一条待处理外部输入。
#[derive(Debug, Clone, PartialEq)]
pub struct PendingInput {
    /// 客户端提供的稳定幂等 id；消费与拒绝都按它落账。
    pub signal_id: String,
    pub kind: PendingInputKind,
    /// signal 必带，cancel 为空。
    pub node_id: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingInputKind {
    Signal,
    Cancel,
}

/// 原子消费结果。`Duplicate` 表示该输入已被处理过（按 signal_id 幂等），
/// 不写事件、不报错，Driver 视为无操作。
#[derive(Debug, Clone, PartialEq)]
pub enum CommitOutcome {
    Applied(Envelope),
    Duplicate,
}

/// run 事件出口。一个 run 的日志写入者是唯一的：持有租约的本实例 Driver。
pub trait RunEventSink: Send {
    /// 受保护追加（非终态事件）。LeaseLost 表示所有权已转移，调用方必须停止派发。
    fn append<'a>(&'a mut self, event: Event) -> BoxFuture<'a, Result<Envelope, EngineError>>;

    /// 终态追加：事件 + 元数据投影 + 释放租约 + 拒绝剩余 pending 输入，同一提交点。
    fn append_terminal<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, EngineError>>;

    /// 非终态状态投影（running / awaiting_resume）。
    /// Postgres 后端必须匹配 owner/epoch（§5.3），单机后端写 runs 表。
    fn project_status<'a>(
        &'a mut self,
        status: DbRunStatus,
        error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    /// 取当前未消费的外部输入（不锁定、不消费）。
    /// 单机后端信号走内存通道，这里恒为空。
    fn poll_inputs<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<PendingInput>, EngineError>>;

    /// 原子消费有效信号：SignalReceived 事件与 inbox applied 同事务（§6.2 步骤 4）。
    fn commit_signal<'a>(
        &'a mut self,
        input: &'a PendingInput,
        event: Event,
    ) -> BoxFuture<'a, Result<CommitOutcome, EngineError>>;

    /// 非法信号只标 rejected，不写事件、不改变 run 终态（§6.2 步骤 3）。
    fn reject_signal<'a>(
        &'a mut self,
        input: &'a PendingInput,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    /// 消费取消命令：RunCancelled + 终态投影 + 租约释放 + 拒绝其余 pending 输入，
    /// 同一事务（§6.2）。返回取消事件的提交结果；Duplicate 表示已被处理。
    fn consume_cancel<'a>(
        &'a mut self,
        input: &'a PendingInput,
    ) -> BoxFuture<'a, Result<CommitOutcome, EngineError>>;

    /// 优雅释放租约：校验 owner/epoch/有效期后清空（§5.2）。
    /// 仅用于计划内移交；失败不重试，等 TTL 到期由接管流程处理。
    fn release<'a>(&'a mut self) -> BoxFuture<'a, Result<(), EngineError>>;
}
