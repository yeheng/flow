use std::collections::HashSet;
use std::io::{BufRead, Write};

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use sha2::{Digest, Sha256};

use crate::{invalid, Error, Result, MAX_LINE_BYTES};

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
}

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
}

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
    pub fn validate(&self) -> Result<()> {
        if self.v != 2
            || self.lsn == 0
            || self.journal_id.is_empty()
            || self.tx_id.is_empty()
            || self.events.is_empty()
        {
            return Err(invalid("invalid transaction version/identity/events"));
        }
        for event in &self.events {
            if event.v != 1 {
                return Err(invalid("unsupported required event version"));
            }
            if event.audit_seq > 0 && event.dispatch_id.is_none() {
                return Err(invalid("audit sequence without dispatch"));
            }
            if event.run_seq > 0 && event.run_id.is_none() {
                return Err(invalid("run sequence without run"));
            }
            validate_depth(&event.payload, 0)?;
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<'a> {
    #[serde(borrow)]
    data: &'a RawValue,
    sha256: String,
}

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Fingerprints use sorted object keys regardless of serde_json's map feature selection.
/// Hash while serializing so a large command input does not need a second encoded allocation.
pub fn fingerprint(value: &Value, limit: usize) -> Result<String> {
    struct Canonical<'a>(&'a Value);
    impl Serialize for Canonical<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::{SerializeMap, SerializeSeq};
            match self.0 {
                Value::Object(map) => {
                    let mut keys = map.keys().collect::<Vec<_>>();
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
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("fingerprint byte limit"));
            }
            self.hash.update(bytes);
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
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
pub struct BoundedBuffer {
    pub bytes: Vec<u8>,
    pub limit: usize,
}
impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("encoded byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn bounded_json(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut buffer = BoundedBuffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut buffer, value).map_err(|e| {
        if e.is_io() {
            Error::Limit(format!("JSON exceeds {limit} bytes"))
        } else {
            Error::Json(e)
        }
    })?;
    Ok(buffer.bytes)
}

pub fn encode(tx: &Transaction) -> Result<Vec<u8>> {
    tx.validate()?;
    let data = bounded_json(tx, MAX_LINE_BYTES)?;
    let raw = RawValue::from_string(String::from_utf8(data).expect("JSON is UTF-8"))?;
    let envelope = Envelope {
        data: &raw,
        sha256: digest(raw.get().as_bytes()),
    };
    let mut line = bounded_json(&envelope, MAX_LINE_BYTES - 1)?;
    line.push(b'\n');
    Ok(line)
}

pub fn decode(line: &[u8]) -> Result<Transaction> {
    if line.len() > MAX_LINE_BYTES {
        return Err(Error::Limit("transaction line".into()));
    }
    if line.last() != Some(&b'\n') || line[..line.len().saturating_sub(1)].contains(&b'\n') {
        return Err(invalid("transaction requires exactly one terminating LF"));
    }
    let mut de = serde_json::Deserializer::from_slice(line);
    Unique::deserialize(&mut de)?;
    de.end()?;
    let envelope: Envelope<'_> = serde_json::from_slice(line)?;
    if digest(envelope.data.get().as_bytes()) != envelope.sha256 {
        return Err(invalid("data SHA-256 mismatch"));
    }
    let tx: Transaction = serde_json::from_str(envelope.data.get())?;
    tx.validate()?;
    Ok(tx)
}

/// Returns at most one bounded physical line. Oversize and ambiguous tail bytes are never skipped.
pub fn read_line(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(invalid("tail missing LF"))
            };
        }
        let n = buf
            .iter()
            .position(|b| *b == b'\n')
            .map_or(buf.len(), |i| i + 1);
        if n > MAX_LINE_BYTES.saturating_sub(bytes.len()) {
            return Err(Error::Limit("transaction line".into()));
        }
        bytes.extend_from_slice(&buf[..n]);
        reader.consume(n);
        if bytes.last() == Some(&b'\n') {
            return Ok(Some(bytes));
        }
    }
}

// Validate duplicate keys without converting or reserializing raw data. Per-object sets and
// the serde recursion limit bound work by the physical line size, independently of history.
struct Unique;
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        d.deserialize_any(UniqueVisitor)
    }
}
struct UniqueVisitor;
impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = Unique;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("JSON with unique keys")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut m: M) -> std::result::Result<Unique, M::Error> {
        let mut keys = HashSet::new();
        while let Some(key) = m.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate JSON key"));
            }
            m.next_value::<Unique>()?;
        }
        Ok(Unique)
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut s: S) -> std::result::Result<Unique, S::Error> {
        while s.next_element::<Unique>()?.is_some() {}
        Ok(Unique)
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<Unique, E> {
        Ok(Unique)
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<Unique, E> {
        Ok(Unique)
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<Unique, E> {
        Ok(Unique)
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Unique, E> {
        Ok(Unique)
    }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Unique, E> {
        Ok(Unique)
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
        Ok(Unique)
    }
}
