//! Explicit v2 coordinator foundation. Commands are journal decisions, SQLite is disposable.
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use flow_engine::journal_state::{command_key, CommandRecord, State};
use flow_journal::page::CommitCursor;
use flow_journal::tail::TailReader;
use flow_journal::{Event, EventKind, Journal, JournalOptions, Transaction};
use flow_store::projection::{ProjectedRow, Projector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub committed: bool,
    pub commit_cursor: CommitCursor,
    pub request_id: Option<String>,
    pub result: Value,
    pub visible: bool,
}
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("execution link closed: {0}")]
    LinkClosed(String),
    #[error("{0}")]
    Journal(#[from] flow_journal::Error),
    #[error("projection: {0}")]
    Projection(String),
    #[error("COMMITTED_NOT_VISIBLE")]
    CommittedNotVisible(CommandReceipt),
}
impl From<serde_json::Error> for JournalError {
    fn from(e: serde_json::Error) -> Self {
        Self::Journal(e.into())
    }
}
type Result<T> = std::result::Result<T, JournalError>;

struct CommittedState {
    state: State,
    reader: TailReader,
    pending: Option<tokio::task::JoinHandle<flow_journal::Result<flow_journal::Commit>>>,
    dirty_runs: BTreeSet<String>,
}
pub struct JournalBackend {
    pub journal: Journal,
    pub projection: Arc<Projector>,
    pub observations: Option<flow_engine::observation::ObservationStore>,
    state: Mutex<CommittedState>,
    projected: Arc<Notify>,
    changed: Arc<Notify>,
    paused: Arc<AtomicBool>,
    shutdown: CancellationToken,
    projector: Mutex<Option<tokio::task::JoinHandle<()>>>,
    projection_error: Arc<Mutex<Option<String>>>,
    waiters: Semaphore,
    wait_timeout: Duration,
    pub(crate) execution: Mutex<Option<tokio::task::JoinHandle<()>>>,
    execution_changed: Arc<Notify>,
    pub(crate) execution_peak: AtomicUsize,
}

impl JournalBackend {
    /// Offline rebuild into a new SQLite file. Holds the authority lock throughout;
    /// neither the existing projection nor any authoritative file is modified.
    pub async fn rebuild_projection(root: &Path, destination: &Path) -> Result<u64> {
        let _lock = flow_journal::maintenance::offline_lock(root)?;
        let report = flow_journal::maintenance::verify(root)?;
        if report.fault.is_some() {
            return Err(
                flow_journal::Error::Invalid("cannot rebuild a corrupt journal".into()).into(),
            );
        }
        let identity = report.journal_id.as_str();
        if identity.is_empty() {
            return Err(flow_journal::Error::Invalid("missing journal identity".into()).into());
        }
        // Exclusive creation prevents clobbering the old projection or another operator's work.
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(destination)
            .map_err(flow_journal::Error::Io)?
            .sync_all()
            .map_err(flow_journal::Error::Io)?;
        let projection = Projector::open(destination, identity)
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?;
        let mut reader = TailReader::new(root, identity);
        let mut state = State::default();
        while let Some((tx, _)) = reader.next(report.last_lsn)? {
            state.apply(&tx)?;
            projection
                .apply(identity, tx.lsn, project_rows(&state, &tx)?)
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
        }
        projection.close().await;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(flow_journal::Error::Io)?;
        Ok(report.last_lsn)
    }

    pub async fn open(root: &Path, options: JournalOptions) -> Result<Arc<Self>> {
        let journal = Journal::open(root, options).await?;
        let mut reader = TailReader::new(root, journal.id());
        let root_owned = root.to_path_buf();
        let upper = journal.durable_lsn();
        let committed =
            tokio::task::spawn_blocking(move || -> flow_journal::Result<CommittedState> {
                let mut state = State::default();
                if let Some(checkpoint) =
                    flow_journal::maintenance::load_checkpoint(&root_owned, 2)?
                {
                    let restored = (|| -> flow_journal::Result<State> {
                        let mut state: State =
                            serde_json::from_value(checkpoint.state["state"].clone())?;
                        state.values = flow_journal::value::ValueCatalog::from_snapshot(
                            checkpoint.state["values"].clone(),
                        )?;
                        if state.journal_id != checkpoint.journal_id
                            || state.applied_lsn != checkpoint.lsn
                            || state.applied_lsn > upper
                        {
                            return Err(flow_journal::Error::Invalid(
                                "state checkpoint boundary mismatch".into(),
                            ));
                        }
                        Ok(state)
                    })();
                    if let Err(error) = &restored {
                        tracing::warn!(%error, "invalid checkpoint; replaying journal");
                    }
                    if let Ok(restored) = restored {
                        reader = TailReader::after(
                            &root_owned,
                            &checkpoint.journal_id,
                            &checkpoint.boundary,
                        )?;
                        state = restored;
                    }
                }
                while let Some((tx, _)) = reader.next(upper)? {
                    state.apply(&tx)?;
                }
                Ok(CommittedState {
                    dirty_runs: state
                        .runs
                        .values()
                        .filter(|r| !r.terminal())
                        .map(|r| r.run_id.clone())
                        .collect(),
                    state,
                    reader,
                    pending: None,
                })
            })
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))??;
        let projection = Arc::new(
            Projector::open(&root.join("projection.sqlite"), journal.id())
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?,
        );
        if projection
            .applied_lsn()
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?
            > upper
        {
            return Err(JournalError::Projection(
                "projection cursor is ahead of authoritative journal".into(),
            ));
        }
        if projection
            .applied_lsn()
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?
            < upper
        {
            let mut rows = Vec::new();
            for (key, value) in &committed.state.workflows {
                rows.push(ProjectedRow {
                    kind: "workflow".into(),
                    key: key.clone(),
                    value: Some(serde_json::to_value(value)?),
                });
            }
            for (key, value) in &committed.state.runs {
                rows.push(ProjectedRow {
                    kind: "run".into(),
                    key: key.clone(),
                    value: Some(serde_json::to_value(value)?),
                });
            }
            for (kind, map) in [
                ("schedule", &committed.state.schedules),
                ("webhook", &committed.state.webhooks),
                ("template", &committed.state.templates),
                ("legacy", &committed.state.legacy),
            ] {
                for (key, value) in map {
                    rows.push(ProjectedRow {
                        kind: kind.into(),
                        key: key.clone(),
                        value: Some(value.clone()),
                    });
                }
            }
            projection
                .restore_snapshot(journal.id(), upper, rows)
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
        }
        let mut tail = committed.reader.fork();
        let mut state = committed.state.clone();
        let backend = Arc::new(Self {
            observations: flow_engine::observation::ObservationStore::open(
                root.join("observations"),
                Default::default(),
            )
            .ok(),
            journal,
            projection,
            state: Mutex::new(committed),
            projected: Arc::new(Notify::new()),
            changed: Arc::new(Notify::new()),
            paused: Arc::new(AtomicBool::new(false)),
            shutdown: CancellationToken::new(),
            projector: Mutex::new(None),
            projection_error: Arc::new(Mutex::new(None)),
            waiters: Semaphore::new(256),
            wait_timeout: Duration::from_secs(5),
            execution: Mutex::new(None),
            execution_changed: Arc::new(Notify::new()),
            execution_peak: AtomicUsize::new(0),
        });
        let weak = Arc::downgrade(&backend);
        let task = tokio::spawn(async move {
            loop {
                let Some(b) = weak.upgrade() else { return };
                if b.shutdown.is_cancelled() {
                    return;
                }
                let changed = b.changed.clone();
                let shutdown = b.shutdown.clone();
                if b.paused.load(Ordering::Relaxed) || state.applied_lsn >= b.journal.durable_lsn()
                {
                    drop(b);
                    tokio::select! {_=changed.notified()=>{},_=shutdown.cancelled()=>return,_=tokio::time::sleep(Duration::from_millis(20))=>{}}
                    continue;
                }
                let upper = b.journal.durable_lsn();
                let processed = tokio::task::spawn_blocking(move || -> flow_journal::Result<_> {
                    let mut batch = Vec::new();
                    let mut bytes = 0;
                    while bytes < 4 * 1024 * 1024 {
                        let Some((tx, location)) = tail.next(upper)? else {
                            break;
                        };
                        state.apply(&tx)?;
                        let rows = project_rows(&state, &tx)?;
                        bytes += location.bytes as usize;
                        batch.push((tx.lsn, rows));
                    }
                    Ok((tail, state, batch))
                })
                .await;
                let (next_tail, next_state, batch) = match processed {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => {
                        *b.projection_error.lock().await = Some(e.to_string());
                        b.projected.notify_waiters();
                        return;
                    }
                    Err(e) => {
                        *b.projection_error.lock().await = Some(e.to_string());
                        b.projected.notify_waiters();
                        return;
                    }
                };
                tail = next_tail;
                state = next_state;
                for (lsn, rows) in batch {
                    if let Err(e) = b.projection.apply(b.journal.id(), lsn, rows).await {
                        *b.projection_error.lock().await = Some(e.to_string());
                        b.projected.notify_waiters();
                        return;
                    }
                    b.projected.notify_waiters();
                }
            }
        });
        *backend.projector.lock().await = Some(task);
        let initial = CommandReceipt {
            committed: true,
            commit_cursor: CommitCursor {
                journal_id: backend.journal.id().into(),
                lsn: upper,
            },
            request_id: None,
            result: Value::Null,
            visible: false,
        };
        backend.visible(initial).await?;
        Ok(backend)
    }

    pub async fn state(&self) -> State {
        self.state.lock().await.state.clone()
    }
    pub fn execution_peak(&self) -> usize {
        self.execution_peak.load(Ordering::Relaxed)
    }
    pub(crate) async fn inspect<T>(&self, read: impl FnOnce(&State) -> T) -> T {
        read(&self.state.lock().await.state)
    }
    pub(crate) async fn internal<T>(
        &self,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        let (events, result) = decide(&committed.state)?;
        if !events.is_empty() {
            self.commit_locked(&mut committed, events).await?;
        }
        Ok(result)
    }

    /// The decision and conflicting command are serialized until durability and reducer apply.
    /// No successful public return bypasses the projection barrier, including deduplicated calls.
    pub async fn command(
        &self,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, Value)>,
    ) -> Result<CommandReceipt> {
        if scope.is_empty()
            || scope.len() > 256
            || request_id.is_some_and(|s| s.is_empty() || s.len() > 128)
        {
            return Err(flow_journal::Error::Invalid("invalid request identity".into()).into());
        }
        let fingerprint = flow_journal::codec::fingerprint(request, 9 * 1024 * 1024)?;
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        if let Some(id) = request_id {
            if let Some(original) = committed.state.commands.get(&command_key(scope, id)) {
                if original.fingerprint != fingerprint {
                    return Err(flow_journal::Error::Conflict(
                        "same request identity with different arguments".into(),
                    )
                    .into());
                }
                let receipt = self.receipt(original);
                drop(committed);
                return self.visible(receipt).await;
            }
        }
        let (mut events, result) = decide(&committed.state)?;
        flow_journal::codec::bounded_json(&result, flow_journal::INLINE_BYTES)?;
        let record = CommandRecord {
            scope: scope.into(),
            request_id: request_id.map(str::to_owned),
            fingerprint,
            result,
            lsn: 0,
        };
        events.push(Event::new(
            EventKind::Command,
            serde_json::to_value(&record)?,
        ));
        let commit = self.commit_locked(&mut committed, events).await?;
        let mut receipt = self.receipt(&record);
        receipt.commit_cursor.lsn = commit;
        drop(committed);
        self.visible(receipt).await
    }

    pub async fn append(&self, events: Vec<Event>) -> Result<u64> {
        let mut state = self.state.lock().await;
        self.refresh_locked(&mut state).await?;
        if let [event] = events.as_slice() {
            if event.audit_seq > 0 {
                if let Some(attempt) = event
                    .run_id
                    .as_ref()
                    .and_then(|id| state.state.runs.get(id))
                    .and_then(|r| event.node_id.as_ref().and_then(|id| r.nodes.get(id)))
                    .and_then(|n| event.dispatch_id.as_ref().and_then(|id| n.attempts.get(id)))
                {
                    if let Some(digest) = attempt.audit_digests.get(&event.audit_seq) {
                        let incoming =
                            flow_journal::codec::digest(&flow_journal::codec::bounded_json(
                                event,
                                flow_journal::MAX_LINE_BYTES,
                            )?);
                        if *digest != incoming {
                            return Err(flow_journal::Error::Conflict(
                                "same audit identity with different content".into(),
                            )
                            .into());
                        }
                        return attempt
                            .audit_lsns
                            .get(&event.audit_seq)
                            .copied()
                            .ok_or_else(|| {
                                JournalError::Projection("missing original audit receipt".into())
                            });
                    }
                }
            }
        }
        self.commit_locked(&mut state, events).await
    }
    async fn commit_locked(
        &self,
        committed: &mut CommittedState,
        events: Vec<Event>,
    ) -> Result<u64> {
        // Include master-produced chunks/structural transactions before checking the command.
        self.refresh_locked(committed).await?;
        let tx = Transaction {
            v: 2,
            journal_id: self.journal.id().into(),
            lsn: committed.state.applied_lsn + 1,
            tx_id: uuid::Uuid::now_v7().to_string(),
            events,
        };
        committed.state.check(&tx)?;
        let journal = self.journal.clone();
        committed.pending = Some(tokio::spawn(async move {
            journal
                .submit(tx.tx_id, tx.events, flow_journal::QueueClass::Control)
                .await
        }));
        self.refresh_locked(committed)
            .await?
            .ok_or_else(|| JournalError::Projection("missing pending commit receipt".into()))
    }
    async fn refresh_locked(&self, committed: &mut CommittedState) -> Result<Option<u64>> {
        let mut submitted = None;
        if let Some(pending) = committed.pending.as_mut() {
            // Await by reference: aborting the RPC leaves this pending decision owned by the
            // coordinator. A conflicting retry first joins it, then consults committed state.
            let result = pending
                .await
                .map_err(|e| JournalError::Projection(e.to_string()));
            committed.pending = None;
            submitted = Some(result??.transaction.lsn);
        }
        while let Some((tx, _)) = committed.reader.next(self.journal.durable_lsn())? {
            committed.state.apply(&tx)?;
            for event in &tx.events {
                if let Some(id) = &event.run_id {
                    committed.dirty_runs.insert(id.clone());
                    if event.kind == EventKind::RunCancelled {
                        let children = committed
                            .state
                            .runs
                            .values()
                            .filter(|r| {
                                !r.terminal() && r.parent.as_ref().is_some_and(|p| p.run_id == *id)
                            })
                            .map(|r| r.run_id.clone())
                            .collect::<Vec<_>>();
                        committed.dirty_runs.extend(children);
                    }
                }
            }
        }
        if !committed.dirty_runs.is_empty() {
            self.execution_changed.notify_one();
        }
        self.changed.notify_waiters();
        Ok(submitted)
    }
    fn receipt(&self, record: &CommandRecord) -> CommandReceipt {
        CommandReceipt {
            committed: true,
            commit_cursor: CommitCursor {
                journal_id: self.journal.id().into(),
                lsn: record.lsn,
            },
            request_id: record.request_id.clone(),
            result: record.result.clone(),
            visible: false,
        }
    }
    async fn visible(&self, mut receipt: CommandReceipt) -> Result<CommandReceipt> {
        let Ok(_permit) = self.waiters.try_acquire() else {
            return Err(JournalError::CommittedNotVisible(receipt));
        };
        let wait = async {
            loop {
                let notified = self.projected.notified();
                if self
                    .projection
                    .applied_lsn()
                    .await
                    .map_err(|e| JournalError::Projection(e.to_string()))?
                    >= receipt.commit_cursor.lsn
                {
                    return Ok::<(), JournalError>(());
                }
                if self.projection_error.lock().await.is_some() {
                    return Err(JournalError::Projection("projector unavailable".into()));
                }
                notified.await;
            }
        };
        if !matches!(
            tokio::time::timeout(self.wait_timeout, wait).await,
            Ok(Ok(()))
        ) {
            return Err(JournalError::CommittedNotVisible(receipt));
        }
        receipt.visible = true;
        Ok(receipt)
    }
    pub async fn command_status(
        &self,
        scope: &str,
        request_id: &str,
    ) -> Result<Option<CommandReceipt>> {
        let mut committed = self.state.lock().await;
        // A cancelled caller may have left a durable decision awaiting reducer apply.
        self.refresh_locked(&mut committed).await?;
        let receipt = committed
            .state
            .commands
            .get(&command_key(scope, request_id))
            .map(|r| self.receipt(r));
        drop(committed);
        let Some(mut receipt) = receipt else {
            return Ok(None);
        };
        receipt.visible = self
            .projection
            .applied_lsn()
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?
            >= receipt.commit_cursor.lsn;
        Ok(Some(receipt))
    }

    /// 幂等前置检查（含业务指纹校验）：命中已提交命令返回回执，调用方
    /// 据此跳过输入值的重复 store；同身份不同参数 → Conflict（与 command()
    /// 内部去重同一语义，重试路径不得绕过指纹校验）。
    pub async fn deduped_command(
        &self,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
    ) -> Result<Option<CommandReceipt>> {
        let Some(id) = request_id.filter(|id| !id.is_empty()) else {
            return Ok(None);
        };
        let fingerprint = flow_journal::codec::fingerprint(request, 9 * 1024 * 1024)?;
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        let Some(original) = committed
            .state
            .commands
            .get(&command_key(scope, id))
            .cloned()
        else {
            return Ok(None);
        };
        if original.fingerprint != fingerprint {
            return Err(flow_journal::Error::Conflict(
                "same request identity with different arguments".into(),
            )
            .into());
        }
        let receipt = self.receipt(&original);
        drop(committed);
        Ok(Some(self.visible(receipt).await?))
    }
    pub fn pause_projection(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        self.changed.notify_waiters();
    }
    pub async fn close(&self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(task) = self.execution.lock().await.take() {
            let _ = task.await;
        }
        if let Some(task) = self.projector.lock().await.take() {
            let _ = task.await;
        }
        if let Err(error) = self.checkpoint().await {
            tracing::warn!(%error,"checkpoint unavailable; next startup will replay journal");
        }
        self.projection.close().await;
        self.journal.close().await?;
        Ok(())
    }
    pub async fn checkpoint(&self) -> Result<()> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        let upper = committed.state.applied_lsn;
        // 已发布值快照只保留仍被状态引用的 Ref：终态 run 的历史值不进
        // checkpoint（见 ValueCatalog::snapshot 的安全依据）。
        let state = serde_json::to_value(&committed.state)?;
        let live = live_output_ids(&state);
        let snapshot =
            serde_json::json!({"state":state,"values":committed.state.values.snapshot(&live)?});
        let root = self.journal.root().to_path_buf();
        drop(committed);
        tokio::task::spawn_blocking(move || {
            flow_journal::maintenance::write_checkpoint(&root, upper, 2, snapshot)
        })
        .await
        .map_err(|e| JournalError::Projection(e.to_string()))??;
        Ok(())
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.shutdown.clone()
    }
    pub(crate) fn execution_notification(&self) -> Arc<Notify> {
        self.execution_changed.clone()
    }
    pub(crate) async fn take_dirty_runs(&self) -> Vec<String> {
        std::mem::take(&mut self.state.lock().await.dirty_runs)
            .into_iter()
            .collect()
    }
    pub(crate) async fn defer_run(&self, id: String) {
        self.state.lock().await.dirty_runs.insert(id);
    }
    pub(crate) async fn defer_runs(&self, ids: Vec<String>) {
        self.state.lock().await.dirty_runs.extend(ids);
    }
}

fn project_rows(state: &State, tx: &Transaction) -> flow_journal::Result<Vec<ProjectedRow>> {
    let mut keys = BTreeSet::new();
    for e in &tx.events {
        if let Some(id) = &e.run_id {
            keys.insert(("run", id.clone()));
        }
        match e.kind {
            EventKind::LegacyImport => {
                keys.insert((
                    "legacy",
                    flow_engine::journal_state::string(&e.payload, "key")?,
                ));
            }
            EventKind::WorkflowCreated
            | EventKind::WorkflowUpdated
            | EventKind::WorkflowPublished
            | EventKind::WorkflowDeleted => {
                if let Some(id) = e.payload["workflow_id"].as_str() {
                    keys.insert(("workflow", id.into()));
                }
            }
            EventKind::ScheduleChanged => {
                keys.insert((
                    "schedule",
                    flow_engine::journal_state::string(&e.payload, "id")?,
                ));
            }
            EventKind::WebhookChanged => {
                keys.insert((
                    "webhook",
                    flow_engine::journal_state::string(&e.payload, "token")?,
                ));
            }
            EventKind::TemplateChanged => {
                keys.insert((
                    "template",
                    flow_engine::journal_state::string(&e.payload, "id")?,
                ));
            }
            _ => {}
        }
    }
    keys.into_iter()
        .map(|(kind, key)| {
            let value = match kind {
                "run" => state.runs.get(&key).map(serde_json::to_value).transpose()?,
                "workflow" => state
                    .workflows
                    .get(&key)
                    .map(serde_json::to_value)
                    .transpose()?,
                "schedule" => state.schedules.get(&key).cloned(),
                "webhook" => state.webhooks.get(&key).cloned(),
                "template" => state.templates.get(&key).cloned(),
                "legacy" => state.legacy.get(&key).cloned(),
                _ => None,
            };
            Ok(ProjectedRow {
                kind: kind.into(),
                key,
                value,
            })
        })
        .collect()
}

/// 状态仍引用的已发布值 id 集：把整个状态序列化后递归扫 ValueRef 的
/// serde 形状（`journal_id`+`output_id`+`digest` 三键同现即命中）。
/// 任何被 runs/workflows/commands/attempts 持有的 StoredValue::Ref 都
/// 躲不过这层扫描——按字段枚举引用面则永远怕漏一处。
fn live_output_ids(state: &Value) -> std::collections::HashSet<String> {
    fn walk(value: &Value, live: &mut std::collections::HashSet<String>) {
        match value {
            Value::Object(map) => {
                if map.contains_key("journal_id")
                    && map.contains_key("digest")
                    && map.contains_key("chunk_count")
                {
                    if let Some(output_id) = map.get("output_id").and_then(Value::as_str) {
                        live.insert(output_id.to_string());
                    }
                }
                for child in map.values() {
                    walk(child, live);
                }
            }
            Value::Array(items) => {
                for child in items {
                    walk(child, live);
                }
            }
            _ => {}
        }
    }
    let mut live = std::collections::HashSet::new();
    walk(state, &mut live);
    live
}
