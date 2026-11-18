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

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::ser::Formatter as _;
use serde_json::Value;

use crate::codec::{bounded_json, Event, EventKind};
use crate::{invalid, Error, Result, CHUNK_BYTES, INLINE_BYTES, MAX_VALUE_BYTES};

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
    /// Value slot for streaming adapters: the bytes written
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
