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
/// permissions.audit.page：数据集级权限审计分页。
///
/// 按 run_id 和 decision 过滤权限事实，事件保留裁决主体、时间和能力项。实现按 run 分页
/// （复用 run 级 Permission 过滤器）：无 run_id 时从在册 run 里逐个收集
/// 到页满；游标 = 下一批 (run_id, lsn)。
pub async fn permissions_audit_page(
    b: &JournalBackend,
    p: &Value,
) -> Result<Value, EventReadError> {
    let decision = p.get("decision").and_then(Value::as_str);
    if let Some(decision) = decision {
        if !matches!(decision, "allow" | "deny") {
            return Err(EventReadError::Invalid(
                "decision must be allow | deny".into(),
            ));
        }
    }
    let run_id = p.get("run_id").and_then(Value::as_str);
    let limit = p
        .get("limit")
        .map(|v| {
            v.as_u64()
                .ok_or_else(|| EventReadError::Invalid("invalid limit".into()))
        })
        .transpose()?
        .unwrap_or(100)
        .min(256) as usize;
    // 候选 run：指定 run 或全部在册 run（有权限事实的只占少数，逐 run 扫描
    // 由 Permission 过滤器短路）。
    let runs: Vec<String> = match run_id {
        Some(run_id) => vec![run_id.to_owned()],
        None => b.run_ids_view().await,
    };
    let identity = b.journal.id().to_owned();
    let root = b.journal.root().to_path_buf();
    let decision = decision.map(str::to_owned);
    // 数据集级权限事实（run 外）：游标 run_id 为 null 时只续读数据集段。
    let dataset_only = p
        .get("cursor")
        .is_some_and(|c| !c.is_null() && c["run_id"].is_null());
    if dataset_only && run_id.is_none() && decision.is_none() {
        let from_lsn = p["cursor"]["lsn"].as_u64().unwrap_or(1);
        let dataset = tokio::task::spawn_blocking({
            let root = root.clone();
            let identity = identity.clone();
            move || {
                flow_journal::page::dataset_permission_events(&root, &identity, from_lsn, limit + 1)
            }
        })
        .await
        .map_err(invalid)??;
        let complete = dataset.len() <= limit;
        let events: Vec<Value> = dataset
            .into_iter()
            .take(limit)
            .map(|positioned| serde_json::to_value(&positioned).map_err(invalid))
            .collect::<Result<_, _>>()?;
        return Ok(json!({"events": events, "complete": complete}));
    }
    let mut events: Vec<Value> = Vec::new();
    let mut exhausted = true;
    for run_id in runs {
        let Some((upper, summary)) = b.store.get_summary("run", &run_id).await.ok() else {
            continue;
        };
        if summary.is_none() {
            continue;
        }
        let mut cursor = match p.get("cursor").filter(|v| !v.is_null()) {
            // 游标只认 (run_id, lsn) 续读位置：跨 run 续读由 items 顺序保证。
            Some(v) => {
                let value = v.clone();
                let wanted_run = value["run_id"].as_str().unwrap_or(&run_id).to_owned();
                let from_lsn = value["lsn"].as_u64().unwrap_or(1);
                Cursor::first(identity.clone(), wanted_run, Filter::Permission, upper)
                    .with_next_lsn(from_lsn)
            }
            None => Cursor::first(identity.clone(), run_id.clone(), Filter::Permission, upper),
        };
        loop {
            let page = tokio::task::spawn_blocking({
                let root = root.clone();
                let identity = identity.clone();
                let run_id = run_id.clone();
                let cursor = cursor.clone();
                move || {
                    flow_journal::page::page(
                        &root,
                        &identity,
                        &run_id,
                        Filter::Permission,
                        cursor,
                        upper,
                        256,
                    )
                }
            })
            .await
            .map_err(invalid)??;
            for positioned in &page.events {
                // decision 过滤只对裁决事件有意义：给出过滤时只返回匹配的
                // permission_decided；未给过滤时返回全部权限事实。
                let record = &positioned.event.payload["record"];
                let matched = match &decision {
                    Some(decision) => {
                        matches!(positioned.event.kind, EventKind::PermissionDecided)
                            && record["decision"].as_str() == Some(decision.as_str())
                    }
                    None => true,
                };
                if matched {
                    events.push(serde_json::to_value(positioned).map_err(invalid)?);
                }
            }
            if events.len() >= limit {
                exhausted = false;
                break;
            }
            match page.next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }
        if !exhausted {
            break;
        }
    }
    events.truncate(limit);
    // 追加数据集级权限事实（无 run 范围、无 decision 过滤时）：审批审计之外
    // 的「谁改了权限 / 谁授予了插件」同属权限审计面（09 §10.3）。
    if run_id.is_none() && decision.is_none() && events.len() < limit {
        let remaining = limit - events.len();
        let dataset = tokio::task::spawn_blocking({
            let root = root.clone();
            let identity = identity.clone();
            move || {
                flow_journal::page::dataset_permission_events(&root, &identity, 1, remaining + 1)
            }
        })
        .await
        .map_err(invalid)??;
        // 数据集段截断时整页不算读完（续读游标由 CLI 从末条 run_id=null 事件推导）。
        exhausted &= dataset.len() <= remaining;
        for positioned in dataset.into_iter().take(remaining) {
            events.push(serde_json::to_value(&positioned).map_err(invalid)?);
        }
    }
    Ok(json!({
        "events": events,
        "complete": exhausted,
        // 导出提示：JSONL = 逐行 serde_json::to_string(events[i])（CLI 同款）。
    }))
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
