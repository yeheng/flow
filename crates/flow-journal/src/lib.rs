//! Bounded single-writer SQLite authority. Facts and current state commit together.
//!
//! 本 crate 是事件溯源的事实存储层：唯一写者把「已接受事实」「当前状态投影」
//! 与「永久回执」放进同一个 SQLite 事务提交，读面只读同一份库。
//! 核心不变量：一次提交要么三者同时生效、要么整体不生效；提交序号 LSN 由
//! 唯一写者分配，失败事务不占序号；已落盘事实读回时必须通过摘要与尺寸
//! 校验，否则判为损坏而不是「尽力还原」。
//! 需要它的原因：业务语义只允许存在一份权威副本，内存投影与磁盘事实一旦
//! 分叉就无法判定谁是真的；把两者绑进同一事务，恢复期才只需信任磁盘。
pub use flow_facts::codec;

#[cfg(feature = "storage")]
pub mod maintenance;
#[cfg(feature = "storage")]
pub mod page;
/// 同库只读查询层（原 flow-store 合入，T6）：Reader 私有只读连接池，
/// 与写入者共用 schema/摘要/格式规则，读写能力在 API 面分离。
#[cfg(feature = "query")]
pub mod query;
#[cfg(feature = "storage")]
pub mod sqlite;
#[cfg(feature = "storage")]
pub mod storage;
#[cfg(feature = "storage")]
pub mod tail;
pub mod value;
#[cfg(feature = "storage")]
pub mod writer;

pub use flow_facts::{
    decimal, Error, Event, EventKind, Result, Transaction, CHUNK_BYTES, INLINE_BYTES,
    MAX_LINE_BYTES, MAX_VALUE_BYTES, VALUE_BUDGET,
};
#[cfg(feature = "storage")]
pub use storage::{scan, Location, Recovery, ScanFault};
pub use value::{StoredValue, ValueRef};
#[cfg(feature = "storage")]
pub use writer::{Commit, Journal, JournalOptions, QueueClass};

#[cfg(feature = "storage")]
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
