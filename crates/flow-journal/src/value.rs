//! 业务值的存储形态：内联值、分块值与发布描述符。
//!
//! 本模块负责「值怎么表示、怎么校验、怎么读回」：小值内联进事件 JSON，
//! 大值切块存进 value_chunks，并用发布描述符引用。
//! 核心不变量：一个值要么完整可验证（分块连续且摘要相符）要么不可用；读回
//! 路径永不返回未发布或摘要不符的值。
//! 需要它的原因：事实事件要保持轻量（不能把大正文重复写进每一行），而业务
//! 又必须能证明某个引用指向的字节确实存在且未被篡改。
use std::collections::BTreeMap;
use std::io::{Read, Write};
#[cfg(feature = "storage")]
use std::path::Path;

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::ser::Formatter as _;
use serde_json::Value;
#[cfg(feature = "storage")]
use sha2::{Digest, Sha256};

use crate::codec::{bounded_json, Event, EventKind};
use crate::{invalid, Error, Result, CHUNK_BYTES, INLINE_BYTES, MAX_VALUE_BYTES};
#[cfg(feature = "storage")]
use crate::{Journal, QueueClass};

/// 一个业务值的两种存放形态。
///
/// `Inline` 直接嵌在事件 JSON 里；`Ref` 指向分块存储的值。二者在 DTO 上都
/// 用 `type`/`value` 标签区分，形状稳定，客户端可据此判别。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum StoredValue {
    Inline(Value),
    Ref(ValueRef),
}

impl<'de> Deserialize<'de> for StoredValue {
    /// 反序列化并顺带做语义校验：内联值查字节上限，引用值查描述符合法性。
    ///
    /// 校验放在反序列化里，可以保证任何来源的 DTO 都满足同一组不变量，而
    /// 不是只有本 crate 内部构造出来的才可信。
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(
            tag = "type",
            content = "value",
            rename_all = "snake_case",
            deny_unknown_fields
        )]
        enum Wire {
            Inline(Value),
            Ref(ValueRef),
        }
        match Wire::deserialize(d)? {
            Wire::Inline(v) => StoredValue::inline(v).map_err(serde::de::Error::custom),
            Wire::Ref(r) => {
                r.validate().map_err(serde::de::Error::custom)?;
                Ok(Self::Ref(r))
            }
        }
    }
}

/// 一个已发布值的引用描述符。
///
/// 它是「值存在」的凭据：journal_id 指向数据集，output_id 标识值，
/// chunk_count/total_bytes 描述形状，digest 是全部字节的摘要。
/// 引用本身不含正文，也不保证引用有效——有效性由发布事实与实际分块的一致
/// 性决定。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueRef {
    pub journal_id: String,
    pub output_id: String,
    pub codec: ValueCodec,
    pub version: u32,
    #[serde(with = "crate::decimal")]
    pub chunk_count: u64,
    #[serde(with = "crate::decimal")]
    pub total_bytes: u64,
    pub digest: String,
}

/// 值的编码方式，决定读回时如何解释字节。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueCodec {
    Json,
    Utf8,
    Bytes,
}

/// 一个值分块：元数据 + base64 编码的字节。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    pub output_id: String,
    #[serde(with = "crate::decimal")]
    pub chunk_index: u64,
    pub data: String,
}
impl Chunk {
    /// 解码分块字节并校验大小。
    ///
    /// 先按 base64 长度上限粗筛，再解 base64，最后校验非空与不超
    /// CHUNK_BYTES：过大或为空的块都会破坏「分块连续 + 摘要相符」的判定。
    ///
    /// # Errors
    /// 超过字节上限返回 [`Error::Limit`]；base64 非法或字节越界返回
    /// [`Error::Invalid`]。
    pub fn decode(&self) -> Result<Vec<u8>> {
        if self.data.len() > CHUNK_BYTES.div_ceil(3) * 4 {
            return Err(Error::Limit("chunk bytes".into()));
        }
        let bytes = STANDARD
            .decode(&self.data)
            .map_err(|e| invalid(e.to_string()))?;
        if bytes.is_empty() || bytes.len() > CHUNK_BYTES {
            return Err(invalid("empty/oversize chunk"));
        }
        Ok(bytes)
    }
}

impl StoredValue {
    /// 构造内联值：先确认编码后不超过内联上限。
    pub fn inline(value: Value) -> Result<Self> {
        bounded_json(&value, INLINE_BYTES)?;
        Ok(Self::Inline(value))
    }
    /// 值的字节数：内联值按编码后长度算，引用值直接取描述符里的总量。
    pub fn bytes(&self) -> Result<u64> {
        match self {
            Self::Inline(value) => Ok(bounded_json(value, INLINE_BYTES)?.len() as u64),
            Self::Ref(r) => Ok(r.total_bytes),
        }
    }
}

impl ValueRef {
    /// 校验描述符自身是否自洽。
    ///
    /// 不变量：digest 必须是 64 位十六进制；空值与非空值的块数必须互斥
    /// （0 字节的值不可能有分块）；块数不能超过总字节数（每块至少 1 字节）。
    ///
    /// # Errors
    /// 任一条件不满足返回 [`Error::Invalid`]。
    pub fn validate(&self) -> Result<()> {
        if self.journal_id.is_empty()
            || self.output_id.is_empty()
            || self.version != 1
            || self.total_bytes > MAX_VALUE_BYTES
            || self.digest.len() != 64
            || !self
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || (self.total_bytes == 0) != (self.chunk_count == 0)
            || self.chunk_count > self.total_bytes
        {
            return Err(invalid("invalid value descriptor"));
        }
        Ok(())
    }
}

/// Verified publication descriptors used by the reducer to validate business
/// references. Chunk bytes live in `value_chunks`; hashing and stream accounting
/// happen once in the journal writer. No hash implementation state is persisted.
///
/// 缓存的是「已发布描述符」，供 reducer 校验业务引用：引用必须在此缓存中逐字
/// 匹配才算有效。缓存只服务于验证，不是权威——权威在 current_entities 与
/// value_chunks 里。
#[derive(Debug, Clone, Default)]
pub struct Publications {
    published: BTreeMap<String, ValueRef>,
}
/// 发布描述符缓存上限；不是永久去重或引用有效性的保留期限。
///
/// 它是硬上限而非软缓存：超限后最旧的描述符被淘汰，此后针对这些 id 的
/// 引用会被 `check` 判为「未发布」而拒绝，因此业务侧不应长期攥着旧引用。
pub const PUBLISHED_TAIL: usize = 8192;

/// 发布缓存的撤销记录：id 及其原值（`None` 表示原本不存在）。
pub type ValueUndo = (String, Option<ValueRef>);
impl Publications {
    /// Seed a descriptor read from the verified committed-prefix publication row.
    ///
    /// 同一 output_id 已有不同描述符时拒绝而不是覆盖：output_id 被复用说明
    /// 写入方出了问题，静默覆盖会让已确认的引用指向别的内容。
    pub fn remember(&mut self, value: ValueRef) -> Result<()> {
        value.validate()?;
        if let Some(previous) = self.published.get(&value.output_id) {
            if previous != &value {
                return Err(invalid("conflicting indexed publication"));
            }
        }
        self.published.insert(value.output_id.clone(), value);
        Ok(())
    }
    /// 按插入顺序淘汰最旧的发布描述符，直到缓存不超过上限。
    pub fn trim_cache(&mut self) {
        while self.published.len() > PUBLISHED_TAIL {
            self.published.pop_first();
        }
    }
    /// 返回当前缓存的发布描述符数量（观测用）。
    pub fn cached_publications(&self) -> usize {
        self.published.len()
    }
    /// 记录某 id 的发布描述符原值，供撤销使用。
    pub fn save(&self, id: &str) -> ValueUndo {
        (id.into(), self.published.get(id).cloned())
    }
    /// 撤销时把发布描述符恢复原状。
    pub fn restore(&mut self, undo: ValueUndo) {
        let (id, published) = undo;
        match published {
            Some(value) => {
                self.published.insert(id, value);
            }
            None => {
                self.published.remove(&id);
            }
        }
    }
    /// Only publications mutate this cache. Chunk continuity and digest are
    /// enforced by the journal writer when the bytes are stored.
    ///
    /// 只有发布事实会改动这个缓存；分块连续性与摘要在写者存字节时校验，
    /// reducer 不重复做同一件事。
    pub fn apply(&mut self, event: &Event, journal_id: &str) -> Result<()> {
        if event.kind != EventKind::ValuePublished {
            return Ok(());
        }
        let value: ValueRef = serde_json::from_value(event.payload.clone())?;
        value.validate()?;
        if value.journal_id != journal_id {
            return Err(invalid("publication belongs to another journal"));
        }
        if self.published.contains_key(&value.output_id) {
            return Err(invalid("duplicate publication"));
        }
        self.published.insert(value.output_id.clone(), value);
        Ok(())
    }
    /// 校验业务值是否可用。
    ///
    /// 内联值查字节上限；引用值必须与缓存中的发布描述符逐字相同，否则视
    /// 为「未发布或描述符不符」而拒绝。
    pub fn check(&self, value: &StoredValue) -> Result<()> {
        match value {
            StoredValue::Inline(v) => {
                bounded_json(v, INLINE_BYTES)?;
            }
            StoredValue::Ref(r) => {
                if self.published.get(&r.output_id) != Some(r) {
                    return Err(invalid(
                        "unpublished or mismatched business value reference",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// A command-local value. Large JSON stays in an anonymous, delete-on-close
/// file until the writer commits it with the command in one SQLite transaction.
///
/// 已准备好但尚未发布的大值。值由匿名临时文件承载，直到随命令一起提交；
/// 命令失败时文件随对象一起消失，不会留下无主的值正文。
#[cfg(feature = "storage")]
pub struct PreparedValue {
    stored: StoredValue,
    staged: Option<StagedValue>,
}
/// 已暂存的大值与其占用的字节配额。
///
/// 配额（`_permit`）在对象被丢弃时自动归还，保证暂存内存总量始终有界。
#[cfg(feature = "storage")]
pub struct StagedValue {
    reference: ValueRef,
    file: std::fs::File,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
#[cfg(feature = "storage")]
impl PreparedValue {
    /// 把一个 JSON 值准备成「可内联或可暂存」的形式。
    ///
    /// 小于内联上限的值直接内联；更大的值写入匿名文件并生成描述符，字节
    /// 配额随对象一起持有。
    pub(crate) fn json(
        journal_id: &str,
        value: Value,
        limit: usize,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Self> {
        use std::io::{Seek, SeekFrom};
        let bytes = bounded_json(&value, limit)?;
        if bytes.len() <= INLINE_BYTES {
            return Ok(Self {
                stored: StoredValue::Inline(value),
                staged: None,
            });
        }
        let reference = ValueRef {
            journal_id: journal_id.into(),
            output_id: uuid::Uuid::now_v7().to_string(),
            codec: ValueCodec::Json,
            version: 1,
            chunk_count: bytes.len().div_ceil(CHUNK_BYTES) as u64,
            total_bytes: bytes.len() as u64,
            digest: crate::codec::digest(&bytes),
        };
        reference.validate()?;
        let mut file = tempfile::tempfile()?;
        file.write_all(&bytes)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            stored: StoredValue::Ref(reference.clone()),
            staged: Some(StagedValue {
                reference,
                file,
                _permit: permit,
            }),
        })
    }
    /// 取准备好的值形态（内联或引用）。
    pub fn stored(&self) -> &StoredValue {
        &self.stored
    }
    /// 取出可暂存的部分；内联值返回 `None`。
    pub fn into_staged(self) -> Option<StagedValue> {
        self.staged
    }
}
#[cfg(feature = "storage")]
impl StagedValue {
    /// 生成与该值对应的发布事实。
    ///
    /// 发布事实必须出现在最终事务里，否则值会成为没有事实引用的孤儿块。
    pub fn publication(&self) -> Result<Event> {
        Ok(Event::new(
            EventKind::ValuePublished,
            serde_json::to_value(&self.reference)?,
        ))
    }
    /// 该值会被切成多少块。
    pub fn chunk_count(&self) -> u64 {
        self.reference.chunk_count
    }
    /// Metadata is sufficient for reducer preview and apply: raw chunk bytes
    /// are validated by the writer and rehydrated only by journal readers.
    ///
    /// 返回每个分块事务的元数据（正文为 null）：reducer 预演只需要块的存在
    /// 与顺序，真正的字节由写者校验、由读者复原。
    pub fn prefix(&self, first_lsn: u64) -> Vec<crate::Transaction> {
        (0..self.reference.chunk_count).map(|index| crate::Transaction {
            v: 2, journal_id: self.reference.journal_id.clone(), lsn: first_lsn + index,
            tx_id: uuid::Uuid::now_v7().to_string(),
            events: vec![Event::new(EventKind::ValueChunk, serde_json::json!({
                "output_id":self.reference.output_id, "chunk_index":index.to_string(), "data":null,
            }))],
        }).collect()
    }
    /// 读出一个分块的实际字节（base64 编码后返回）。
    ///
    /// 期望长度由总字节数与块序号推出：最后一块是余数，其余都是满块。
    pub(crate) fn read_chunk(&mut self, index: u64) -> Result<Chunk> {
        let expected = (self.reference.total_bytes - index * CHUNK_BYTES as u64)
            .min(CHUNK_BYTES as u64) as usize;
        let mut bytes = vec![0; expected];
        self.file.read_exact(&mut bytes)?;
        Ok(Chunk {
            output_id: self.reference.output_id.clone(),
            chunk_index: index,
            data: STANDARD.encode(bytes),
        })
    }
}

/// Scan only this object's chunks. Memory is bounded by one transaction and one raw chunk.
/// A candidate may be checked before publication; consumers must set require_published=true.
/// Bytes can be delivered before the final checksum: callers must abort their stream on error.
///
/// 只扫描本对象的分块：内存占用限于一个事务与一块原始字节。
/// `require_published=false` 时可以在发布前先检查候选值（用于预演）；
/// 真正的消费方必须置 `true`。
/// 注意字节可能在最终摘要校验之前就被写出：调用方必须在出错时中止自己的
/// 流，否则会消费到一段后来被证明不完整的内容。
#[cfg(feature = "storage")]
pub fn read_value(
    root: &Path,
    upper_lsn: u64,
    value: &ValueRef,
    require_published: bool,
    out: &mut impl Write,
) -> Result<()> {
    value.validate()?;
    let conn = crate::sqlite::read_connection(root)?;
    // 整个读取在一个只读快照里：避免读到写者提交一半的中间状态。
    conn.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let (identity, durable) = crate::sqlite::metadata(&conn)?;
    if identity != value.journal_id || upper_lsn > durable {
        return Err(invalid("foreign value dataset/boundary"));
    }
    let publication = crate::sqlite::entity(&conn, "published", &value.output_id)?;
    let published = if let Some(record) = publication {
        let lsn = record["lsn"]
            .as_str()
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| invalid("invalid publication sequence"))?;
        if lsn <= upper_lsn {
            if serde_json::from_value::<ValueRef>(record["value"].clone())? != *value {
                return Err(invalid("mismatched publication"));
            }
            true
        } else {
            false
        }
    } else {
        false
    };
    // 要求已发布时，未发布（含「发布在快照之外」）一律拒绝。
    if require_published && !published {
        return Err(invalid("value is not published"));
    }
    let mut index = 0;
    let mut total = 0;
    let mut hash = Sha256::new();
    let mut statement=conn.prepare("SELECT chunk_index,bytes,digest FROM value_chunks WHERE output_id=? AND lsn<=? ORDER BY chunk_index").map_err(crate::sqlite::db)?;
    let mut rows = statement
        .query(rusqlite::params![
            value.output_id,
            format!("{upper_lsn:020}")
        ])
        .map_err(crate::sqlite::db)?;
    while let Some(row) = rows.next().map_err(crate::sqlite::db)? {
        let chunk_index: i64 = row.get(0).map_err(crate::sqlite::db)?;
        let bytes: Vec<u8> = row.get(1).map_err(crate::sqlite::db)?;
        let digest: String = row.get(2).map_err(crate::sqlite::db)?;
        // 分块必须连续、非空且不超限，并与自己记录的摘要相符。
        if chunk_index != index as i64
            || bytes.is_empty()
            || bytes.len() > CHUNK_BYTES
            || crate::codec::digest(&bytes) != digest
        {
            return Err(invalid("non-contiguous or corrupt chunk"));
        }
        total += bytes.len() as u64;
        if total > value.total_bytes {
            return Err(invalid("value length exceeds descriptor"));
        }
        hash.update(&bytes);
        out.write_all(&bytes)?;
        index += 1;
    }
    if index != value.chunk_count
        || total != value.total_bytes
        || hex::encode(hash.finalize()) != value.digest
        || (require_published && !published)
    {
        return Err(invalid("value incomplete, unpublished or digest mismatch"));
    }
    Ok(())
}

/// Bounded producer. Callers supply a real stream; no whole-body buffering before chunking.
/// Execution audit identity is attached at the execution facade, not manufactured here.
///
/// 有界生产者：调用方提供真实流，切块前不整体缓冲。执行审计身份由执行门面
/// 附加，这里不凭空制造。
#[cfg(feature = "storage")]
pub async fn store_stream(
    journal: &Journal,
    input: &mut (impl tokio::io::AsyncRead + Unpin),
    codec: ValueCodec,
) -> Result<ValueRef> {
    store_stream_using(journal.id(), input, codec, |event| async {
        let class = if event.kind == EventKind::ValueChunk {
            QueueClass::Audit(
                event.payload["output_id"]
                    .as_str()
                    .ok_or_else(|| invalid("value chunk missing output_id"))?
                    .into(),
            )
        } else {
            QueueClass::Control
        };
        journal
            .submit(uuid::Uuid::now_v7().to_string(), vec![event], class)
            .await?;
        Ok(())
    })
    .await
}

/// Share chunk encoding while letting the backend enforce its normal fact admission.
///
/// 复用同一套分块编码，但把「如何提交」交给调用方：backend 可以走自己的
/// 事实准入通道，而不是绕过它。
#[cfg(feature = "storage")]
pub async fn store_stream_using<F, Fut, E>(
    journal_id: &str,
    input: &mut (impl tokio::io::AsyncRead + Unpin),
    codec: ValueCodec,
    mut submit: F,
) -> std::result::Result<ValueRef, E>
where
    F: FnMut(Event) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), E>>,
    E: From<Error>,
{
    use tokio::io::AsyncReadExt;
    let output_id = uuid::Uuid::now_v7().to_string();
    let mut hash = Sha256::new();
    let mut total = 0;
    let mut index = 0;
    let mut buffer = vec![0; CHUNK_BYTES];
    loop {
        let n = input.read(&mut buffer).await.map_err(Error::from)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        // 超限时保留已捕获的前缀：调用方据此判断续传还是放弃，丢弃前缀等于
        // 白做。
        if total > MAX_VALUE_BYTES {
            return Err(Error::Limit("value bytes; captured prefix retained".into()).into());
        }
        hash.update(&buffer[..n]);
        let chunk = Chunk {
            output_id: output_id.clone(),
            chunk_index: index,
            data: STANDARD.encode(&buffer[..n]),
        };
        submit(Event::new(
            EventKind::ValueChunk,
            serde_json::to_value(chunk).map_err(Error::from)?,
        ))
        .await?;
        index += 1;
    }
    let value = ValueRef {
        journal_id: journal_id.into(),
        output_id,
        codec,
        version: 1,
        chunk_count: index,
        total_bytes: total,
        digest: hex::encode(hash.finalize()),
    };
    submit(Event::new(
        EventKind::ValuePublished,
        serde_json::to_value(&value).map_err(Error::from)?,
    ))
    .await?;
    Ok(value)
}

/// 存一个 JSON 值：小值内联，大值转为分块存储。
///
/// # Errors
/// 物化超限、流式写入失败或超出行上限时返回错误。
#[cfg(feature = "storage")]
pub async fn store_json(
    journal: &Journal,
    value: Value,
    materialization_limit: usize,
) -> Result<StoredValue> {
    let bytes = bounded_json(&value, materialization_limit)?;
    if bytes.len() <= INLINE_BYTES {
        return Ok(StoredValue::Inline(value));
    }
    let mut input = bytes.as_slice();
    Ok(StoredValue::Ref(
        store_stream(journal, &mut input, ValueCodec::Json).await?,
    ))
}

/// 把存储的值物化回内存 JSON。
///
/// 引用值只允许物化为 JSON 或 UTF-8；二进制值无法表示成 JSON，直接报错，
/// 而不是产出一个含义不明的值。
///
/// # Errors
/// 超出物化上限、值未发布或摘要不符时返回错误。
#[cfg(feature = "storage")]
pub fn materialize(
    root: &Path,
    upper_lsn: u64,
    value: &StoredValue,
    limit: usize,
) -> Result<Value> {
    match value {
        StoredValue::Inline(value) => {
            bounded_json(value, limit.min(INLINE_BYTES))?;
            Ok(value.clone())
        }
        StoredValue::Ref(reference) => {
            if reference.total_bytes > limit as u64 {
                return Err(Error::Limit("aggregate materialization".into()));
            }
            let mut out = crate::codec::BoundedBuffer {
                bytes: Vec::new(),
                limit,
            };
            read_value(root, upper_lsn, reference, true, &mut out)?;
            match reference.codec {
                ValueCodec::Json => Ok(serde_json::from_slice(&out.bytes)?),
                ValueCodec::Utf8 => Ok(Value::String(
                    String::from_utf8(out.bytes).map_err(|e| invalid(e.to_string()))?,
                )),
                ValueCodec::Bytes => Err(invalid("binary value cannot be materialized as JSON")),
            }
        }
    }
}

// Keep sync Read available for callers building streaming export adapters.
/// 有界拷贝：最多复制 `max` 字节，超出即报错。
///
/// 读 `max + 1` 字节用于判断「是否恰好超限」：只读 `max` 字节无法区分
/// 「正好等于上限」与「超过上限」。
pub fn copy_bounded(mut from: impl Read, to: &mut impl Write, max: u64) -> Result<u64> {
    let n = std::io::copy(&mut from.by_ref().take(max + 1), to)?;
    if n > max {
        return Err(Error::Limit("stream length".into()));
    }
    Ok(n)
}

/// Structured JSON object composer. serde_json's formatter owns every
/// structural byte (`{`, `,`, `:`, `}`) and every key escape; callers only
/// supply complete values. Hand-splicing these bytes is forbidden: one
/// misplaced comma produces a broken document that takes down every
/// downstream parser.
///
/// 结构化 JSON 对象合成器：结构字节与键转义全部由 serde_json 的 formatter
/// 产出，调用方只提供完整值。手写拼接这些字节是被禁止的：一个错位的逗号
/// 就会产出一份坏文档，并拖垮所有下游解析器。
pub struct ObjectWriter<W: Write> {
    writer: W,
    formatter: serde_json::ser::CompactFormatter,
    first: bool,
}

impl<W: Write> ObjectWriter<W> {
    /// Writes the opening brace; every structural byte after it comes from
    /// the serde_json formatter.
    ///
    /// 开括号之后的全部结构字节都由 formatter 产出。
    pub fn begin(writer: W) -> Result<Self> {
        let mut object = Self {
            writer,
            formatter: serde_json::ser::CompactFormatter,
            first: true,
        };
        object.formatter.begin_object(&mut object.writer)?;
        Ok(object)
    }
    /// Serialize one key and position the stream at its value slot.
    ///
    /// 键的引号与转义同样交给 serde_json。
    pub fn key(&mut self, key: &str) -> Result<()> {
        self.formatter
            .begin_object_key(&mut self.writer, self.first)?;
        serde_json::to_writer(&mut self.writer, key)?;
        self.formatter.begin_object_value(&mut self.writer)?;
        self.first = false;
        Ok(())
    }
    /// Serialize one value through serde.
    ///
    /// 值必须能被 serde 完整表示，序列化失败即放弃整个对象。
    pub fn value<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        serde_json::to_writer(&mut self.writer, value)?;
        Ok(())
    }
    /// Stream one complete JSON value's raw bytes (published-reference
    /// contents or caller-validated fragments).
    ///
    /// 写入的字节必须恰好构成一个完整 JSON 值；调用方需自行保证这一点，
    /// 因为这里不再做结构校验。
    pub fn raw_value_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
        Ok(())
    }
    /// Value slot for streaming writers ([`read_value`]): the bytes written
    /// here must form exactly one complete JSON value.
    ///
    /// 流式写入用的值槽位：写进去的字节必须恰好是一个完整 JSON 值。
    pub fn value_writer(&mut self) -> &mut W {
        &mut self.writer
    }
    /// Writes the closing brace and returns the wrapped writer.
    ///
    /// 收尾由 formatter 完成，保证成对的花括号与逗号都由它产出。
    pub fn finish(self) -> Result<W> {
        let mut object = self;
        object.formatter.end_object(&mut object.writer)?;
        Ok(object.writer)
    }
}

/// Compose one JSON object from stored entries without materializing whole
/// values: inline entries serialize through serde, references stream through
/// the fixed 64 KiB pipe. Fan-in does not materialize whole objects. The
/// returned task must be joined; dropping the reader cancels production.
///
/// 用存储条目合成一个 JSON 对象而不整体物化：内联条目经 serde 序列化，引用
/// 条目经固定 64 KiB 管道流式写出。扇入过程不物化整个对象。
/// 返回的任务必须被 join；丢弃读端会取消生产，合成结果也就不会完整。
#[cfg(feature = "storage")]
pub fn compose_object(
    root: std::path::PathBuf,
    upper: u64,
    entries: Vec<(String, StoredValue)>,
) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<Result<()>>) {
    let (reader, writer) = tokio::io::duplex(64 * 1024);
    let task = tokio::task::spawn_blocking(move || {
        let mut writer = tokio_util::io::SyncIoBridge::new(writer);
        let mut object = ObjectWriter::begin(&mut writer)?;
        for (key, value) in entries {
            object.key(&key)?;
            match value {
                StoredValue::Inline(value) => {
                    bounded_json(&value, INLINE_BYTES)?;
                    serde_json::to_writer(object.value_writer(), &value)?;
                }
                StoredValue::Ref(value) => {
                    if value.codec != ValueCodec::Json {
                        return Err(invalid("non-JSON reference in JSON composition"));
                    }
                    read_value(&root, upper, &value, true, object.value_writer())?;
                }
            }
        }
        object.finish()?;
        writer.flush()?;
        Ok(())
    });
    (reader, task)
}

#[cfg(test)]
mod object_writer_tests {
    use super::*;

    /// 钉住「结构与转义由 serde 拥有」：手写的结构字节一旦介入，坏掉的
    /// 对象会拖垮所有下游解析器。
    #[test]
    fn structure_and_escapes_are_owned_by_serde() {
        let mut bytes = Vec::new();
        {
            let mut object = ObjectWriter::begin(&mut bytes).unwrap();
            object.key("status").unwrap();
            object.value(&404u64).unwrap();
            object.key("weird \"key\"\n").unwrap();
            object
                .value(&serde_json::json!({"a": [1, 2, null]}))
                .unwrap();
            object.key("raw").unwrap();
            object.raw_value_bytes(b"{\"streamed\":true}").unwrap();
            object.finish().unwrap();
        }
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["status"], 404);
        assert_eq!(parsed["raw"], serde_json::json!({"streamed": true}));
        assert_eq!(parsed["weird \"key\"\n"]["a"][1], 2);
    }

    /// 钉住空对象仍是合法 JSON：零条目扇入不能产出半个对象。
    #[test]
    fn empty_object_is_valid_json() {
        let mut bytes = Vec::new();
        let object = ObjectWriter::begin(&mut bytes).unwrap();
        let _ = object.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            serde_json::json!({})
        );
    }
}
