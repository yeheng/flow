use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::ser::Formatter as _;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::codec::{bounded_json, Event, EventKind};
use crate::{
    invalid, Error, Journal, QueueClass, Result, CHUNK_BYTES, INLINE_BYTES, MAX_VALUE_BYTES,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueCodec {
    Json,
    Utf8,
    Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    pub output_id: String,
    #[serde(with = "crate::decimal")]
    pub chunk_index: u64,
    pub data: String,
}
impl Chunk {
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
    pub fn inline(value: Value) -> Result<Self> {
        bounded_json(&value, INLINE_BYTES)?;
        Ok(Self::Inline(value))
    }
    pub fn bytes(&self) -> Result<u64> {
        match self {
            Self::Inline(value) => Ok(bounded_json(value, INLINE_BYTES)?.len() as u64),
            Self::Ref(r) => Ok(r.total_bytes),
        }
    }
}

impl ValueRef {
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

/// Incremental validation state reconstructed from journal facts, never from a client.
/// Unfinished streams keep only a hash and counters, not their bytes.
#[derive(Debug, Clone, Default)]
pub struct ValueCatalog {
    streams: BTreeMap<String, ValueProgress>,
    published: BTreeMap<String, ValueRef>,
}
#[derive(Debug, Clone, Default)]
pub struct ValueProgress {
    hash: Sha256,
    chunks: u64,
    bytes: u64,
}
/// 发布描述符缓存上限；不是永久去重或引用有效性的保留期限。
pub const PUBLISHED_TAIL: usize = 8192;

pub type ValueUndo = (String, Option<ValueProgress>, Option<ValueRef>);
impl ValueCatalog {
    /// 快照保留缓存中的 live 描述符和按键排序的尾部。
    /// 发布和引用可以位于不同事务，不能依赖键序推断在飞窗口；backend
    /// 在 reducer 检查前从 committed execution index 按原前缀恢复缺项。
    /// 永久 publication 身份留在该派生索引及权威 journal，不随缓存逐出。
    pub fn snapshot(&self, live: &std::collections::HashSet<String>) -> Result<Value> {
        use sha2::digest::common::hazmat::SerializableState;
        let tail_start = self.published.len().saturating_sub(PUBLISHED_TAIL);
        let published: BTreeMap<String, ValueRef> = self
            .published
            .iter()
            .enumerate()
            .filter(|(index, (id, _))| *index >= tail_start || live.contains(*id))
            .map(|(_, (id, value))| (id.clone(), value.clone()))
            .collect();
        // Bound derived-cache allocations before building the JSON tree.
        crate::codec::bounded_json(&published, 8 * 1024 * 1024)?;
        if self.streams.len() > 8192 {
            return Err(crate::Error::Limit("checkpoint stream budget".into()));
        }
        let streams=self.streams.iter().map(|(id,s)|serde_json::json!({"id":id,"chunks":s.chunks.to_string(),"bytes":s.bytes.to_string(),"sha256_state":STANDARD.encode(s.hash.serialize())})).collect::<Vec<_>>();
        Ok(serde_json::json!({"version":1,"streams":streams,"published":published}))
    }
    pub fn from_snapshot(value: Value) -> Result<Self> {
        use sha2::digest::common::hazmat::{SerializableState, SerializedState};
        if value["version"] != 1 {
            return Err(invalid("unsupported value checkpoint"));
        }
        let mut catalog = Self {
            streams: BTreeMap::new(),
            published: serde_json::from_value(value["published"].clone())?,
        };
        for s in value["streams"]
            .as_array()
            .ok_or_else(|| invalid("missing checkpoint streams"))?
        {
            let state = STANDARD
                .decode(
                    s["sha256_state"]
                        .as_str()
                        .ok_or_else(|| invalid("missing hash state"))?,
                )
                .map_err(|_| invalid("invalid hash state"))?;
            let state: SerializedState<Sha256> = state
                .as_slice()
                .try_into()
                .map_err(|_| invalid("invalid hash state length"))?;
            let hash = <Sha256 as SerializableState>::deserialize(&state)
                .map_err(|_| invalid("invalid hash checkpoint"))?;
            let parse = |key: &str| {
                s[key]
                    .as_str()
                    .and_then(|n| n.parse::<u64>().ok())
                    .ok_or_else(|| invalid("invalid stream counter"))
            };
            let id = s["id"]
                .as_str()
                .ok_or_else(|| invalid("missing stream id"))?;
            let bytes = parse("bytes")?;
            let chunks = parse("chunks")?;
            if bytes > MAX_VALUE_BYTES || chunks > bytes || catalog.published.contains_key(id) {
                return Err(invalid("invalid stream checkpoint"));
            }
            catalog.streams.insert(
                id.into(),
                ValueProgress {
                    hash,
                    chunks,
                    bytes,
                },
            );
        }
        for value in catalog.published.values() {
            value.validate()?;
        }
        Ok(catalog)
    }
    /// Seed only descriptors read from the verified committed-prefix disk index.
    /// The reducer still compares every business reference against this catalog.
    pub fn remember_verified(&mut self, value: ValueRef) -> Result<()> {
        value.validate()?;
        if let Some(previous) = self.published.get(&value.output_id) {
            if previous != &value {
                return Err(invalid("conflicting indexed publication"));
            }
        }
        self.published.insert(value.output_id.clone(), value);
        Ok(())
    }
    pub fn trim_cache(&mut self) {
        while self.published.len() > PUBLISHED_TAIL {
            self.published.pop_first();
        }
    }
    pub fn cached_publications(&self) -> usize {
        self.published.len()
    }
    pub fn open_streams(&self) -> usize {
        self.streams.len()
    }
    pub fn save(&self, id: &str) -> ValueUndo {
        (
            id.into(),
            self.streams.get(id).cloned(),
            self.published.get(id).cloned(),
        )
    }
    pub fn restore(&mut self, undo: ValueUndo) {
        let (id, stream, published) = undo;
        match stream {
            Some(v) => {
                self.streams.insert(id.clone(), v);
            }
            None => {
                self.streams.remove(&id);
            }
        }
        match published {
            Some(v) => {
                self.published.insert(id, v);
            }
            None => {
                self.published.remove(&id);
            }
        }
    }
    pub fn apply(&mut self, event: &Event, journal_id: &str) -> Result<()> {
        match event.kind {
            EventKind::ValueChunk => {
                let chunk: Chunk = serde_json::from_value(event.payload.clone())?;
                if self.published.contains_key(&chunk.output_id) {
                    return Err(invalid("chunk after publication"));
                }
                let bytes = chunk.decode()?;
                if !self.streams.contains_key(&chunk.output_id) && self.streams.len() >= 8192 {
                    return Err(Error::Limit("open value stream budget".into()));
                }
                let stream = self.streams.entry(chunk.output_id).or_default();
                if stream.chunks != chunk.chunk_index
                    || stream.bytes + bytes.len() as u64 > MAX_VALUE_BYTES
                {
                    return Err(invalid("value chunk gap/duplicate/limit"));
                }
                stream.hash.update(&bytes);
                stream.chunks += 1;
                stream.bytes += bytes.len() as u64;
            }
            EventKind::ValuePublished => {
                let value: ValueRef = serde_json::from_value(event.payload.clone())?;
                value.validate()?;
                let stream = self
                    .streams
                    .get(&value.output_id)
                    .cloned()
                    .unwrap_or_default();
                if value.journal_id != journal_id
                    || self.published.contains_key(&value.output_id)
                    || value.chunk_count != stream.chunks
                    || value.total_bytes != stream.bytes
                    || value.digest != hex::encode(stream.hash.finalize())
                {
                    return Err(invalid("publication requires complete matching chunks"));
                }
                self.streams.remove(&value.output_id);
                self.published.insert(value.output_id.clone(), value);
            }
            _ => {}
        }
        Ok(())
    }
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

/// Scan only this object's chunks. Memory is bounded by one transaction and one raw chunk.
/// A candidate may be checked before publication; consumers must set require_published=true.
/// Bytes can be delivered before the final checksum: callers must abort their stream on error.
pub fn read_value(
    root: &Path,
    upper_lsn: u64,
    value: &ValueRef,
    require_published: bool,
    out: &mut impl Write,
) -> Result<()> {
    value.validate()?;
    let mut index = 0;
    let mut total = 0;
    let mut hash = Sha256::new();
    let mut published = false;
    for location in crate::address::locations(root, upper_lsn, &value.journal_id, &value.output_id)?
    {
        let location = location?;
        let tx = crate::codec::decode(&crate::maintenance::read_location(root, &location)?)?;
        if tx.journal_id != value.journal_id {
            return Err(invalid("foreign value journal"));
        }
        for event in &tx.events {
            if event.kind == EventKind::ValueChunk
                && event.payload.get("output_id").and_then(Value::as_str)
                    == Some(value.output_id.as_str())
            {
                if published {
                    return Err(invalid("chunk after publication"));
                }
                let chunk: Chunk = serde_json::from_value(event.payload.clone())?;
                if chunk.chunk_index != index {
                    return Err(invalid("non-contiguous or duplicate chunk"));
                }
                let bytes = chunk.decode()?;
                total += bytes.len() as u64;
                if total > value.total_bytes {
                    return Err(invalid("value length exceeds descriptor"));
                }
                hash.update(&bytes);
                out.write_all(&bytes)?;
                index += 1;
            }
            if event.kind == EventKind::ValuePublished
                && event.payload.get("output_id").and_then(Value::as_str)
                    == Some(value.output_id.as_str())
            {
                let descriptor: ValueRef = serde_json::from_value(event.payload.clone())?;
                if descriptor != *value
                    || published
                    || index != value.chunk_count
                    || total != value.total_bytes
                    || hex::encode(hash.clone().finalize()) != value.digest
                {
                    return Err(invalid("invalid value publication"));
                }
                published = true;
            }
        }
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
pub async fn store_stream(
    journal: &Journal,
    input: &mut (impl tokio::io::AsyncRead + Unpin),
    codec: ValueCodec,
) -> Result<ValueRef> {
    use tokio::io::AsyncReadExt;
    let output_id = uuid::Uuid::now_v7().to_string();
    let mut hash = Sha256::new();
    let mut total = 0;
    let mut index = 0;
    let mut buffer = vec![0; CHUNK_BYTES];
    loop {
        let n = input.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        if total > MAX_VALUE_BYTES {
            return Err(Error::Limit("value bytes; captured prefix retained".into()));
        }
        hash.update(&buffer[..n]);
        let chunk = Chunk {
            output_id: output_id.clone(),
            chunk_index: index,
            data: STANDARD.encode(&buffer[..n]),
        };
        journal
            .submit(
                uuid::Uuid::now_v7().to_string(),
                vec![Event::new(
                    EventKind::ValueChunk,
                    serde_json::to_value(chunk)?,
                )],
                QueueClass::Audit(output_id.clone()),
            )
            .await?;
        index += 1;
    }
    let value = ValueRef {
        journal_id: journal.id().into(),
        output_id,
        codec,
        version: 1,
        chunk_count: index,
        total_bytes: total,
        digest: hex::encode(hash.finalize()),
    };
    journal
        .submit(
            uuid::Uuid::now_v7().to_string(),
            vec![Event::new(
                EventKind::ValuePublished,
                serde_json::to_value(&value)?,
            )],
            QueueClass::Control,
        )
        .await?;
    Ok(value)
}

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
pub struct ObjectWriter<W: Write> {
    writer: W,
    formatter: serde_json::ser::CompactFormatter,
    first: bool,
}

impl<W: Write> ObjectWriter<W> {
    /// Writes the opening brace; every structural byte after it comes from
    /// the serde_json formatter.
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
    pub fn key(&mut self, key: &str) -> Result<()> {
        self.formatter
            .begin_object_key(&mut self.writer, self.first)?;
        serde_json::to_writer(&mut self.writer, key)?;
        self.formatter.begin_object_value(&mut self.writer)?;
        self.first = false;
        Ok(())
    }
    /// Serialize one value through serde.
    pub fn value<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        serde_json::to_writer(&mut self.writer, value)?;
        Ok(())
    }
    /// Stream one complete JSON value's raw bytes (published-reference
    /// contents or caller-validated fragments).
    pub fn raw_value_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
        Ok(())
    }
    /// Value slot for streaming writers ([`read_value`]): the bytes written
    /// here must form exactly one complete JSON value.
    pub fn value_writer(&mut self) -> &mut W {
        &mut self.writer
    }
    /// Writes the closing brace and returns the wrapped writer.
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
