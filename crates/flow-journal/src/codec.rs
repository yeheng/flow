use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{invalid, Error, Result};

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

    /// The single audit predicate. Storage flags, fact pages and subscriptions all
    /// derive from it; no caller hand-writes `ValueChunk | LateAudit` again.
    pub fn is_audit(&self) -> bool {
        self.audit_seq > 0
    }

    /// 事件只动审计面（值分块/发布与迟到审计），不改任何业务实体状态。
    /// 折叠 undo 策略（engine）与 current_entities 投影（backend）共用这
    /// 一份清单——两处手写 `matches!` 会随新事件类型漂移成第二份语义。
    pub fn is_audit_only(&self) -> bool {
        matches!(
            self.kind,
            EventKind::ValueChunk | EventKind::ValuePublished | EventKind::LateAudit
        )
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
            if event.kind == EventKind::LateAudit && (!event.is_audit() || event.run_seq != 0) {
                return Err(invalid(
                    "late audit requires audit sequence and no run sequence",
                ));
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
