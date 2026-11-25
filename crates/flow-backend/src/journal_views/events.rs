use super::JournalBackend;
use crate::{StorageError, ViewError};
use flow_journal::{
    page::{self, Cursor, Filter, Page, PositionedEvent},
    tail::TailReader,
    EventKind, MAX_LINE_BYTES,
};
use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
pub enum EventReadError {
    #[error(transparent)]
    Projection(#[from] StorageError),
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Journal(#[from] ViewError),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
}

fn invalid(error: impl std::fmt::Display) -> EventReadError {
    EventReadError::Invalid(error.to_string())
}

pub enum EventStream {
    Control,
    Audit,
    Permission,
}

/// The data set, run and committed upper bound for a historical read.
pub struct RunEvents {
    run_id: String,
    journal_id: String,
    root: std::path::PathBuf,
    upper: u64,
}

impl RunEvents {
    fn at(b: &JournalBackend, run_id: String, upper: u64) -> Self {
        Self {
            run_id,
            journal_id: b.journal.id().to_owned(),
            root: b.journal.root().to_path_buf(),
            upper,
        }
    }

    pub async fn open(b: &JournalBackend, run_id: String) -> Result<Option<Self>, StorageError> {
        let (upper, row) = b.store.get_summary("run", &run_id).await?;
        Ok(row.map(|_| Self::at(b, run_id, upper)))
    }

    fn first(&self, filter: Filter) -> Cursor {
        Cursor::first(
            self.journal_id.clone(),
            self.run_id.clone(),
            filter,
            self.upper,
        )
    }

    async fn read(
        &self,
        filter: Filter,
        cursor: Cursor,
        limit: usize,
    ) -> Result<Page, EventReadError> {
        let root = self.root.clone();
        let journal_id = self.journal_id.clone();
        let run_id = self.run_id.clone();
        let upper = self.upper;
        Ok(tokio::task::spawn_blocking(move || {
            page::page(&root, &journal_id, &run_id, filter, cursor, upper, limit)
        })
        .await??)
    }

    pub async fn page(
        &self,
        stream: EventStream,
        cursor: Option<&Value>,
        limit: Option<&Value>,
    ) -> Result<Value, EventReadError> {
        let filter = match stream {
            EventStream::Control => Filter::Events,
            EventStream::Audit => Filter::Audit,
            EventStream::Permission => Filter::Permission,
        };
        let cursor = self.cursor(filter, cursor)?;
        let limit = limit
            .map(|v| v.as_u64().ok_or_else(|| invalid("invalid limit")))
            .transpose()?
            .unwrap_or(256)
            .min(256) as usize;
        serde_json::to_value(self.read(filter, cursor, limit).await?).map_err(invalid)
    }

    fn cursor(&self, filter: Filter, cursor: Option<&Value>) -> Result<Cursor, EventReadError> {
        match cursor.filter(|v| !v.is_null()) {
            Some(v) => serde_json::from_value(v.clone()).map_err(invalid),
            None => Ok(self.first(filter)),
        }
    }

    pub async fn from_sequence(
        &self,
        from: u64,
        cursor: Option<&Value>,
    ) -> Result<Value, EventReadError> {
        let filter = Filter::Events;
        let cursor = self.cursor(filter, cursor)?;
        let mut page = self.read(filter, cursor, 256).await?;
        page.events.retain(|e| e.event.run_seq >= from);
        serde_json::to_value(page).map_err(invalid)
    }
}
/// Read permissions.audit.page from the committed fact stream.
pub async fn permissions_audit_page(
    b: &JournalBackend,
    p: &Value,
) -> Result<Value, EventReadError> {
    let decision = p.get("decision").and_then(Value::as_str);
    if decision.is_some_and(|value| !matches!(value, "allow" | "deny")) {
        return Err(invalid("decision must be allow | deny"));
    }
    let run_id = p.get("run_id").and_then(Value::as_str);
    let limit = p
        .get("limit")
        .map(|v| v.as_u64().ok_or_else(|| invalid("invalid limit")))
        .transpose()?
        .unwrap_or(100)
        .min(256) as usize;
    let cursor = p.get("cursor").filter(|value| !value.is_null());
    let (lsn, event_index) = match cursor {
        None => (1, 0),
        Some(cursor) => {
            // Older clients send numeric LSNs; v2 positions use decimal strings.
            let lsn = match &cursor["lsn"] {
                Value::String(value) => value
                    .parse::<u64>()
                    .ok()
                    .filter(|lsn| lsn.to_string() == *value),
                value => value.as_u64(),
            }
            .ok_or_else(|| invalid("invalid audit cursor lsn"))?;
            let event_index = cursor
                .get("event_index")
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|index| u32::try_from(index).ok())
                        .ok_or_else(|| invalid("invalid audit cursor event_index"))
                })
                .transpose()?
                .unwrap_or(0);
            (lsn, event_index)
        }
    };
    let cursor_run = cursor.and_then(|value| value.get("run_id"));
    if let (Some(run), Some(cursor_run)) = (run_id, cursor_run.and_then(Value::as_str)) {
        if run != cursor_run {
            return Err(invalid("cursor binding/boundary mismatch"));
        }
    }
    // Explicit null preserves the dataset-only continuation of the old cursor.
    let dataset_only = run_id.is_none() && decision.is_none() && cursor_run == Some(&Value::Null);
    let root = b.journal.root().to_path_buf();
    let identity = b.journal.id().to_owned();
    let decision = decision.map(str::to_owned);
    let run_id = run_id.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        let scope = match run_id.as_deref() {
            Some(id) => page::PermissionScope::Run(id),
            None if dataset_only => page::PermissionScope::Dataset,
            None => page::PermissionScope::All,
        };
        let (events, next) = page::permission_events(
            &root,
            &identity,
            scope,
            decision.as_deref(),
            page::EventPosition { lsn, event_index },
            limit,
        )?;
        let complete = next.is_none();
        let mut next_cursor = serde_json::to_value(next).map_err(invalid)?;
        if !complete && (dataset_only || run_id.is_some()) {
            next_cursor["run_id"] = json!(run_id);
        }
        Ok(json!({"events": events, "complete": complete, "next_cursor": next_cursor}))
    })
    .await?
}

/// Business callers only see the public progress stream and a reduced payload.
pub async fn invocation_events(
    b: &JournalBackend,
    run_id: String,
    cursor: Option<String>,
    after_lsn: Option<String>,
) -> Result<Page, EventReadError> {
    let read = RunEvents::at(b, run_id, b.journal.durable_lsn());
    let cursor = match cursor {
        Some(raw) => serde_json::from_str(&raw).map_err(|_| invalid("invalid cursor"))?,
        None => {
            let mut cursor = read.first(Filter::Events);
            if let Some(raw) = after_lsn {
                let bad_offset = || invalid("invalid after_lsn");
                let after = raw.parse::<u64>().map_err(|_| bad_offset())?;
                if after.to_string() != raw || after > read.upper {
                    return Err(bad_offset());
                }
                cursor.next_lsn = after.checked_add(1).ok_or_else(bad_offset)?;
            }
            cursor
        }
    };
    let mut page = read.read(Filter::Events, cursor, 100).await?;
    // 白名单：只保留业务进展类事件，执行器内部的审计事实
    // （含更多细节）不属于调用方可观测范围。
    page.events.retain(|v| {
        matches!(
            v.event.kind,
            EventKind::RunStarted
                | EventKind::RunCompleted
                | EventKind::RunFailed
                | EventKind::RunCancelled
                | EventKind::DispatchStarted
                | EventKind::NodeCompleted
                | EventKind::NodeFailed
                | EventKind::NodeSkipped
                | EventKind::WaitRegistered
                | EventKind::WaitResolved
                | EventKind::SignalReceived
        )
    });
    for positioned in &mut page.events {
        let event = &mut positioned.event;
        // payload 裁剪：只留下业务需要的字段，失败统一成
        // EXECUTION_FAILED（不泄露内部错误细节）。
        event.payload = match event.kind {
            EventKind::RunStarted => {
                serde_json::json!({"workflow_version":event.payload["workflow_version"]})
            }
            EventKind::WaitRegistered => {
                serde_json::json!({"wait":{"kind":event.payload["wait"]["kind"],"wake_at":event.payload["wait"]["wake_at"]}})
            }
            EventKind::NodeSkipped => serde_json::json!({"reason":event.payload["reason"]}),
            EventKind::NodeFailed | EventKind::RunFailed => {
                serde_json::json!({"error":"EXECUTION_FAILED"})
            }
            _ => serde_json::json!({}),
        };
    }

    Ok(page)
}

/// A subscription's read position. The transport controls pacing and delivery.
pub struct RunEventTail {
    backend: std::sync::Arc<JournalBackend>,
    run_id: String,
    reader: Option<TailReader>,
    scanned: u64,
    from_seq: u64,
    from_lsn: u64,
    audit: bool,
}

pub struct RunEventBatch {
    pub records: Vec<PositionedEvent>,
    pub caught_up: bool,
    pub terminal: bool,
}

impl RunEventTail {
    pub fn new(
        backend: std::sync::Arc<JournalBackend>,
        run_id: String,
        from_seq: u64,
        from_lsn: u64,
        audit: bool,
    ) -> Self {
        let reader = TailReader::for_run(backend.journal.root(), backend.journal.id(), &run_id);
        Self {
            backend,
            run_id,
            reader: Some(reader),
            scanned: 0,
            from_seq,
            from_lsn,
            audit,
        }
    }

    /// Scan committed facts at the same upper bound used to read the run status.
    pub async fn next(&mut self) -> Result<Option<RunEventBatch>, EventReadError> {
        let (upper, row) = self.backend.store.get_summary("run", &self.run_id).await?;
        let Some(row) = row else { return Ok(None) };
        let terminal = matches!(
            row["status"].as_str(),
            Some("succeeded" | "failed" | "cancelled")
        );
        let Some(mut tail) = self.reader.take() else {
            return Err(EventReadError::Journal(ViewError::Unavailable(
                "event tail closed".into(),
            )));
        };
        let id = self.run_id.clone();
        let Self {
            scanned,
            audit,
            from_lsn,
            from_seq,
            ..
        } = *self;
        let (tail, records, scanned) =
            tokio::task::spawn_blocking(move || -> flow_journal::Result<_> {
                let mut records = Vec::new();
                let mut bytes = 0;
                let mut latest = scanned;
                // 单批最多累积 MAX_LINE_BYTES：再大就会被 jsonrpsee 的响应
                // 体积上限拒收。
                while bytes < MAX_LINE_BYTES {
                    let Some((tx, location)) = tail.next(upper)? else {
                        // A filtered reader may exhaust this snapshot without returning
                        // a transaction at upper. Progress is scan work, not event delivery.
                        latest = upper;
                        break;
                    };
                    latest = tx.lsn;
                    bytes += location.bytes as usize;
                    for (index, event) in tx.events.into_iter().enumerate() {
                        // 两种流各自的推进基准：control 按 run_seq（业务事件序），
                        // audit 按 lsn（写入序）。
                        if event.run_id.as_deref() == Some(&id)
                            && (if audit {
                                tx.lsn > from_lsn && event.is_audit()
                            } else {
                                event.run_seq > from_seq
                            })
                        {
                            records.push(PositionedEvent {
                                lsn: tx.lsn,
                                event_index: index,
                                event,
                            });
                        }
                    }
                }
                Ok((tail, records, latest))
            })
            .await
            .map_err(invalid)??;

        self.reader = Some(tail);
        self.scanned = scanned;
        Ok(Some(RunEventBatch {
            records,
            caught_up: scanned >= upper,
            terminal,
        }))
    }
}
