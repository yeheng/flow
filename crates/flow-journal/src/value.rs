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

/// Verified publication descriptors used by the reducer to validate business
/// references. Chunk bytes live in `value_chunks`; hashing and stream accounting
/// happen once in the journal writer. No hash implementation state is persisted.
#[derive(Debug, Clone, Default)]
pub struct Publications {
    published: BTreeMap<String, ValueRef>,
}
/// 发布描述符缓存上限；不是永久去重或引用有效性的保留期限。
pub const PUBLISHED_TAIL: usize = 8192;

pub type ValueUndo = (String, Option<ValueRef>);
impl Publications {
    /// Seed a descriptor read from the verified committed-prefix publication row.
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
    pub fn trim_cache(&mut self) {
        while self.published.len() > PUBLISHED_TAIL {
            self.published.pop_first();
        }
    }
    pub fn cached_publications(&self) -> usize {
        self.published.len()
    }
    pub fn save(&self, id: &str) -> ValueUndo {
        (id.into(), self.published.get(id).cloned())
    }
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
#[cfg(feature = "storage")]
pub async fn store_stream(
    journal: &Journal,
    input: &mut (impl tokio::io::AsyncRead + Unpin),
    codec: ValueCodec,
) -> Result<ValueRef> {
    store_stream_using(journal.id(), input, codec, |event| async {
        let class = if event.kind == EventKind::ValueChunk {
            QueueClass::Audit(event.payload["output_id"].as_str().unwrap().into())
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
