//! 事实编解码层：把领域事件与事务编码成稳定的线上字节，并计算摘要。
//!
//! 本模块负责「怎么写」而不负责「写到哪里」：事件/事务的 JSON 形状、u64 的
//! 十进制规范、深度与字节上限、摘要算法都集中在这里，存储层只搬运这些字节。
//! 核心不变量：同一个语义事件必须编码成逐字节相同的 JSON——对象键必须排序，
//! 否则同一事实的摘要会随 map 实现或插入顺序漂移，幂等判定与篡改检测同时失效。
//! 需要它的原因：事实账本要能跨版本重放与校验，编码必须是确定性函数；同时
//! 所有写入都要有字节与深度上限，否则一条畸形载荷就能撑爆唯一写者。
use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{invalid, Error, Result};

/// 事实事件的封闭词汇表。
///
/// 新增事件类型属于协议变更：必须显式加进枚举，未知 kind 反序列化直接失败，
/// 好过「静默忽略」——静默忽略会让旧执行器把新事实当成从未发生，从而在
/// 缺失语义的情况下继续推进业务。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    SegmentStarted,
    SegmentSealed,
    Command,
    WorkflowCreated,
    WorkflowUpdated,
    WorkflowPublished,
    WorkflowDeleted,
    ScheduleChanged,
    WebhookChanged,
    TemplateChanged,
    RunStarted,
    RunCancelled,
    RunCompleted,
    RunFailed,
    DispatchStarted,
    InputPrepared,
    ValueChunk,
    ValuePublished,
    OperationIntent,
    OperationAuthorized,
    OperationOutcome,
    NodeCompleted,
    NodeFailed,
    NodeSkipped,
    WaitRegistered,
    WaitResolved,
    SignalReceived,
    Adjudicated,
    LateAudit,
    MasterEpochStarted,
    LegacyImport,
    /// Explicit machine retry authorization for a declared repeatable operation.
    OperationRetryAuthorized,
}

/// 一条已接受的事实。
///
/// `v` 是事件格式版本，只增不改；`run_seq` 是 run 内业务序号，`audit_seq` 是
/// dispatch 内审计序号，两者都为 0 表示该事实不属于业务/审计序列。
/// `deny_unknown_fields` 让多余字段直接失败：多出的字段通常意味着写入方与
/// 读取方版本不一致，静默丢弃会让两方对同一事实的理解分叉。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub v: u32,
    pub kind: EventKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(with = "crate::decimal")]
    pub run_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(with = "crate::decimal")]
    pub audit_seq: u64,
    pub payload: Value,
}

impl Event {
    /// 构造一条默认身份的事实：v1 格式、不绑定 run/dispatch、序号为 0。
    ///
    /// 需要绑定身份或序号的事件由调用方自行填字段；默认值只在「与身份无关」
    /// 的事实上才是正确的。
    pub fn new(kind: EventKind, payload: Value) -> Self {
        Self {
            v: 1,
            kind,
            run_id: None,
            run_seq: 0,
            dispatch_id: None,
            node_id: None,
            audit_seq: 0,
            payload,
        }
    }

    /// The single audit predicate. Storage flags, fact pages and subscriptions all
    /// derive from it; no caller hand-writes `ValueChunk | LateAudit` again.
    ///
    /// 审计谓词只此一处：存储层的 `audit_event` 列、事实分页的过滤以及订阅面
    /// 都从这里派生。规则一旦被复制成第二份「手写 matches!」，新增事件类型时
    /// 就会漏改其中一处，读面随之出现无法解释的分叉。
    pub fn is_audit(&self) -> bool {
        self.audit_seq > 0
    }

    /// 事件只动审计面（值分块/发布与迟到审计），不改任何业务实体状态。
    /// 折叠 undo 策略（engine）与 current_entities 投影（backend）共用这
    /// 一份清单——两处手写 `matches!` 会随新事件类型漂移成第二份语义。
    /// 它与 `is_audit` 语义不同：审计序列里也有会改业务状态的事件（如节点
    /// 终态），这里只判「不改业务实体」的那一类。
    pub fn is_audit_only(&self) -> bool {
        matches!(
            self.kind,
            EventKind::ValueChunk | EventKind::ValuePublished | EventKind::LateAudit
        )
    }
}

/// 一个已提交事务的完整线上形状。
///
/// `lsn` 是提交序号，由唯一写者分配；`tx_id` 是调用方给的幂等键，在库中
/// 唯一。`deny_unknown_fields` 保证新旧两端对同一事务的理解不会因为多余
/// 字段而分叉。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    pub v: u32,
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
    pub tx_id: String,
    pub events: Vec<Event>,
}

impl Transaction {
    /// 校验事务的版本、身份与事件内部一致性。
    ///
    /// 写侧与读侧共用同一套规则：写侧在落盘前拒绝畸形事务，读侧在读回时
    /// 复核；二者必须一致，否则「能写进去但读不回来」的事故会静默发生。
    ///
    /// # Errors
    /// 版本不符、身份缺失、序号与身份不匹配、JSON 过深或身份串过长时返回
    /// [`Error::Invalid`]（调用方输入问题，不冻结写者）。
    pub fn validate(&self) -> Result<()> {
        // lsn 必须非 0：0 是「还没有提交」的哨兵值。
        if self.v != 2
            || self.lsn == 0
            || self.journal_id.is_empty()
            || self.tx_id.is_empty()
            || self.events.is_empty()
        {
            return Err(invalid("invalid transaction version/identity/events"));
        }
        for event in &self.events {
            // 事件格式版本只认 1：更高版本需要显式适配，不能当作 1 读入。
            if event.v != 1 {
                return Err(invalid("unsupported required event version"));
            }
            // 审计序号只在 dispatch 内有意义，缺 dispatch 的审计事实无法归位。
            if event.audit_seq > 0 && event.dispatch_id.is_none() {
                return Err(invalid("audit sequence without dispatch"));
            }
            // 业务序号只在 run 内有意义，缺 run 的业务事实无法归位。
            if event.run_seq > 0 && event.run_id.is_none() {
                return Err(invalid("run sequence without run"));
            }
            // 迟到审计：有审计序号但没有业务序号，且必须属于审计序列。
            if event.kind == EventKind::LateAudit && (!event.is_audit() || event.run_seq != 0) {
                return Err(invalid(
                    "late audit requires audit sequence and no run sequence",
                ));
            }
            validate_depth(&event.payload, 0)?;
            // 身份串长度上限：身份会进索引列与查询条件，无界长度既是存储
            // 放大器也是查询开销来源。
            if event
                .run_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > 128)
                || event
                    .dispatch_id
                    .as_ref()
                    .is_some_and(|id| id.is_empty() || id.len() > 128)
            {
                return Err(invalid("invalid event identity length"));
            }
        }
        Ok(())
    }
}

/// 递归检查 JSON 嵌套深度。
///
/// 上限 64 层：深度不可控的载荷会撑爆递归栈，而栈溢出发生在唯一写者线程
/// 上等同于权威停摆。
///
/// # Errors
/// 超过深度上限时返回 [`Error::Limit`]。
pub fn validate_depth(value: &Value, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(Error::Limit("JSON depth exceeds 64".into()));
    }
    match value {
        Value::Array(v) => {
            for child in v {
                validate_depth(child, depth + 1)?;
            }
        }
        Value::Object(v) => {
            for child in v.values() {
                validate_depth(child, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// 计算字节的 SHA-256 十六进制摘要。
///
/// 摘要用于事务体与值块的完整性校验：读回时重算并比对，不一致即判定
/// 存储损坏，绝不返回「尽力还原」的内容。
pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Fingerprints use sorted object keys regardless of serde_json's map feature selection.
/// Hash while serializing so a large command input does not need a second encoded allocation.
///
/// 指纹是命令幂等的依据：同一逻辑请求必须得到同一指纹，否则重试会被当成
/// 新命令重复执行。排序键保证「键序不同但语义相同」的载荷指纹一致。
///
/// # Errors
/// 载荷超深或编码后超过字节上限时返回 [`Error::Limit`]。
pub fn fingerprint(value: &Value, limit: usize) -> Result<String> {
    struct Canonical<'a>(&'a Value);
    impl Serialize for Canonical<'_> {
        /// 按「键有序」的规范形状序列化，把指纹的确定性规则收在这一处。
        fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::{SerializeMap, SerializeSeq};
            match self.0 {
                Value::Object(map) => {
                    let mut keys = map.keys().collect::<Vec<_>>();
                    // 排序是确定性的唯一来源：serde_json 的 map 实现（是否保序、
                    // 是否 BTreeMap）不应影响指纹。
                    keys.sort_unstable();
                    let mut output = s.serialize_map(Some(keys.len()))?;
                    for key in keys {
                        output.serialize_entry(key, &Canonical(&map[key]))?;
                    }
                    output.end()
                }
                Value::Array(values) => {
                    let mut output = s.serialize_seq(Some(values.len()))?;
                    for value in values {
                        output.serialize_element(&Canonical(value))?;
                    }
                    output.end()
                }
                other => other.serialize(s),
            }
        }
    }
    struct HashWriter {
        hash: Sha256,
        bytes: usize,
        limit: usize,
    }
    impl Write for HashWriter {
        /// 边编码边哈希，并在写入过程中卡住字节上限。
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            // 编码过程中就卡上限：先编完再量长度等于先分配任意大的内存。
            if bytes.len() > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("fingerprint byte limit"));
            }
            self.hash.update(bytes);
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        /// 空实现：哈希写入器没有缓冲，不需要刷出。
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    validate_depth(value, 0)?;
    let mut writer = HashWriter {
        hash: Sha256::new(),
        bytes: 0,
        limit,
    };
    serde_json::to_writer(&mut writer, &Canonical(value))?;
    Ok(hex::encode(writer.hash.finalize()))
}

/// A serialization limit is enforced while encoding, not after allocating an arbitrary Vec.
///
/// 写入端专用的有界缓冲：把字节上限放在编码路径上，而不是编码完再检查
/// 长度，否则「超大载荷」会先以完整尺寸落进内存。
pub struct BoundedBuffer {
    pub bytes: Vec<u8>,
    pub limit: usize,
}
impl Write for BoundedBuffer {
    /// 追加字节并检查上限，超限立即失败而不是先收下再报错。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("encoded byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    /// 空实现：缓冲只写内存，无需刷出。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 把可序列化对象编码成不超过 `limit` 字节的 JSON。
///
/// # Errors
/// 编码后超过字节上限时返回 [`Error::Limit`]；序列化本身失败返回
/// [`Error::Json`]。
pub fn bounded_json(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut buffer = BoundedBuffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut buffer, value).map_err(|e| {
        // 有界缓冲把超限伪装成 io 错误，这里再还原成 Limit：超限是调用方
        // 输入过大，不是序列化缺陷。
        if e.is_io() {
            Error::Limit(format!("JSON exceeds {limit} bytes"))
        } else {
            Error::Json(e)
        }
    })?;
    Ok(buffer.bytes)
}
