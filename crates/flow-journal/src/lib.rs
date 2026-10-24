//! Bounded single-writer SQLite authority. Facts and current state commit together.
//!
//! 本 crate 是事件溯源的事实存储层：唯一写者把「已接受事实」「当前状态投影」
//! 与「永久回执」放进同一个 SQLite 事务提交，读面只读同一份库。
//! 核心不变量：一次提交要么三者同时生效、要么整体不生效；提交序号 LSN 由
//! 唯一写者分配，失败事务不占序号；已落盘事实读回时必须通过摘要与尺寸
//! 校验，否则判为损坏而不是「尽力还原」。
//! 需要它的原因：业务语义只允许存在一份权威副本，内存投影与磁盘事实一旦
//! 分叉就无法判定谁是真的；把两者绑进同一事务，恢复期才只需信任磁盘。
pub mod codec;
#[cfg(feature = "storage")]
pub mod maintenance;
#[cfg(feature = "storage")]
pub mod page;
#[cfg(feature = "storage")]
pub mod sqlite;
#[cfg(feature = "storage")]
pub mod storage;
#[cfg(feature = "storage")]
pub mod tail;
pub mod value;
#[cfg(feature = "storage")]
pub mod writer;

pub use codec::{Event, EventKind, Transaction};
#[cfg(feature = "storage")]
pub use storage::{scan, Location, Recovery, ScanFault};
pub use value::{StoredValue, ValueRef};
#[cfg(feature = "storage")]
pub use writer::{Commit, Journal, JournalOptions, QueueClass};

/// 单条事务记录（编码后）的字节上限。
///
/// 事实以「每条事务一行」存储：行过大既撑爆 SQLite 页缓存，也让读回路径
/// 需要一次性物化过多字节。写入端按此限额编码，读回端按同一限额复核。
pub const MAX_LINE_BYTES: usize = 1024 * 1024;
/// 内联业务值的字节上限。
///
/// 小于该值的值直接嵌进事件 JSON；超过则切块存入 `value_chunks`，避免大
/// 正文在事实 JSON 与 BLOB 表里重复保存（重复即两份真相）。
pub const INLINE_BYTES: usize = 64 * 1024;
/// 单个值分块的原始字节上限（base64 解码前）。
///
/// 与 schema 里 `value_chunks.bytes` 的 CHECK 约束一致，保证任何一次块
/// 读取的内存占用都有界。
pub const CHUNK_BYTES: usize = 256 * 1024;
/// 单个业务值的总字节上限。
///
/// 限制单值规模，避免一次流式写入把磁盘写满；超限时已写入的分块保留，
/// 调用方据此决定续传还是放弃。
pub const MAX_VALUE_BYTES: u64 = 256 * 1024 * 1024;

/// 存储层失败的分类。
///
/// 分类直接决定唯一写者是否冻结：`Invalid` / `Limit` / `Conflict` 是调用方
/// 输入或时序问题，只拒绝当前事务，写者继续服务；`Io` / `Unavailable` /
/// `Json` 意味着无法确认权威是否完整，必须冻结写者并让人工介入。把「坏输入」
/// 误判成「平台故障」会把一条错误请求升级成「权威不可用直到重启」。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("journal I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("journal JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid journal: {0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
    #[error("journal limit exceeded: {0}")]
    Limit(String),
    #[error("journal unavailable: {0}")]
    Unavailable(String),
    #[error("journal conflict: {0}")]
    Conflict(String),
}
/// 本 crate 的统一返回类型。
///
/// 调用方按 [`Error`] 的变体决定后续动作：输入类错误换输入重试，平台类
/// 错误停写并告警。
pub type Result<T> = std::result::Result<T, Error>;

/// 构造「输入非法」错误。
///
/// 只用于确实由外部输入引发的规则违反；这类错误不冻结唯一写者（见
/// [`Error`]），因此不能用来掩盖内部状态损坏。
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

/// Canonical decimal u64 on disk and in v2 DTOs, never a JSON number.
///
/// u64 一律以十进制字符串出现：JSON number 在 JS 客户端会丢精度（超过
/// 2^53），而 LSN、序号、字节数都可能越过该阈值。序列化写字符串，反序列化
/// 只接受规范写法（无前导零、无正号），保证同一数值只有一种线上表示——
/// 否则同一事实的摘要会因写法不同而分裂，幂等判定与篡改检测同时失效。
pub mod decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    /// 把 u64 序列化成十进制字符串，而不是 JSON number。
    pub fn serialize<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
    /// 解析十进制字符串为 u64，非规范写法一律拒绝。
    ///
    /// 拒绝的理由：`"01"` 与 `"1"` 数值相同但字节不同，若放行，同一个值会
    /// 产生两种摘要，写进事实账本后重放无法判定二者是否为同一事实。
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let s = String::deserialize(d)?;
        let value: u64 = s.parse().map_err(serde::de::Error::custom)?;
        // 回写比较而不是正则：等价于「必须是 value 的规范十进制表示」。
        if value.to_string() != s {
            return Err(serde::de::Error::custom("noncanonical decimal"));
        }
        Ok(value)
    }
}

/// Maximum JSON value materialized by product commands and read models.
///
/// 产品命令与读模型可物化的最大 JSON 值。物化（大值转成内存对象）必须有
/// 上限，否则一次 256 MiB 的值就能把读模型进程撑爆；写入侧的暂存配额也按
/// 该预算的两倍发放，覆盖「两个命令同时暂存」的场景。
pub const VALUE_BUDGET: usize = 8 * 1024 * 1024;
