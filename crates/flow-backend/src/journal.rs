//! Explicit v2 coordinator foundation. Commands are journal decisions, SQLite is disposable.
use std::collections::BTreeSet;

/// 在飞（非终态）run 状态数上限：约束主进程 run 状态内存与调度表。
/// 这是后端状态内存约束，不是协议量——协议并发面（A_max/X_max）见
/// `flow_engine::execution_protocol::contract`，两者不得互相代用。
/// 执行点：`journal_commands::run_start` 与 `execution::dispatch::register_child_run`
/// （同一常量，不另写数字）。
pub const R_MAX: usize = 1000;

/// 定义缓存硬顶：正常路径靠 run 终态逐出，这里只兜底防泄漏
/// （超过即任意逐出——缓存只省磁盘读，正确性不依赖它）。
const DEFINITION_CACHE_MAX: usize = 128;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use flow_engine::journal_state::{CommandRecord, State};
use flow_journal::page::CommitCursor;
use flow_journal::tail::TailReader;
use flow_journal::{Event, EventKind, Journal, JournalOptions, Transaction};
use flow_store::projection::{ProjectedRow, Projector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
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
    reader: Option<TailReader>,
    reading:
        Option<tokio::task::JoinHandle<(TailReader, flow_journal::Result<Option<Transaction>>)>>,
    pending: Option<tokio::task::JoinHandle<flow_journal::Result<flow_journal::Commit>>>,
    dirty_runs: BTreeSet<String>,
    applying: Option<Transaction>,
}
pub struct JournalBackend {
    #[cfg(feature = "journal-cluster")]
    cluster: std::sync::OnceLock<Arc<crate::journal_cluster::Membership>>,
    pub journal: Journal,
    pub projection: Arc<Projector>,
    audit_index: Arc<Projector>,
    pub observations: Option<flow_engine::observation::ObservationStore>,
    state: Mutex<CommittedState>,
    projected: Arc<Notify>,
    changed: Arc<Notify>,
    paused: Arc<AtomicBool>,
    /// 协调器停止信号（`cancellation()` 返回它）。**不用于停投影**：协调器
    /// 退出前可能正卡在 `command()` 的可见性等待上，投影一停就永远等不到。
    shutdown: CancellationToken,
    /// 投影停止信号：由 `close()` 在协调器 join 之后才触发。
    projector_stop: CancellationToken,
    projector: Mutex<Option<tokio::task::JoinHandle<()>>>,
    projection_error: Arc<Mutex<Option<String>>>,
    /// 投影游标的内存镜像：projector 每批 apply 后 fetch_max 更新。等待者
    /// 用一次原子读判可见，不再按唤醒逐次查 SQLite。
    projected_lsn: AtomicU64,
    /// 写命令等到可见的上限（病理阀：健康路径毫秒级追平；超时才发
    /// -32020，意味着投影坏了而非投影慢）。测试可调小。
    wait_timeout_ms: AtomicU64,
    pub(crate) execution: Mutex<Option<tokio::task::JoinHandle<()>>>,
    execution_changed: Arc<Notify>,
    pub(crate) execution_peak: AtomicUsize,
    /// 诊断：协调循环心跳（每轮 select 醒来自增；挂死排查用）。
    pub(crate) coordinator_ticks: AtomicU64,
    pub(crate) coordinator_last_tick_ms: AtomicI64,
    /// 物化后的 run 定义缓存：definition 随 run 钉死版本、创建后不可变，
    /// 是天然缓存键——每个 run 只物化一次（从 journal 值目录拼 chunk），
    /// 终态即逐出。调度循环每个 tick 都要用定义，不该每趟重读磁盘。
    definitions: Mutex<std::collections::HashMap<String, Arc<flow_engine::Definition>>>,
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
        let temporary = tempfile::tempdir_in(destination.parent().unwrap_or(Path::new(".")))
            .map_err(flow_journal::Error::Io)?;
        let history = Projector::open(&temporary.path().join("execution.sqlite"), identity)
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?;
        let mut reader = TailReader::new(root, identity);
        let mut state = State {
            compact_audits: true,
            ..Default::default()
        };
        while let Some((tx, _)) = reader.next(report.last_lsn)? {
            crate::journal_history::prepare(&history, &mut state, &tx).await?;
            state.apply(&tx)?;
            history
                .apply(
                    identity,
                    tx.lsn,
                    crate::journal_history::index_rows(&state, &tx)?,
                )
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
            projection
                .apply(identity, tx.lsn, project_rows(&state, &tx)?)
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
            crate::journal_history::trim(&mut state);
        }
        history.close().await;
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
        let history_path = root.join("execution-index.sqlite");
        let audit_index = Arc::new(
            Projector::open(&history_path, journal.id())
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?,
        );
        let mut indexed = audit_index
            .applied_lsn()
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?;
        let upper = journal.durable_lsn();
        if indexed > upper {
            return Err(JournalError::Projection(
                "execution index ahead of journal".into(),
            ));
        }
        // Old derived indexes lack permanent receipts and unfinished stream state.
        // Rebuild from authority once; never reinterpret an old checkpoint as that index.
        if indexed > 0
            && audit_index
                .get("index_format", "single-v2")
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?
                .1
                .is_none()
        {
            audit_index
                .restore_snapshot(journal.id(), 0, vec![])
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
            indexed = 0;
        }
        let root_owned = root.to_path_buf();
        let identity = journal.id().to_owned();
        let (mut state, reader) =
            tokio::task::spawn_blocking(move || -> flow_journal::Result<_> {
                let mut state = State {
                    compact_audits: true,
                    ..Default::default()
                };
                let mut reader = TailReader::new(&root_owned, &identity);
                if let Some(cp) = flow_journal::maintenance::load_checkpoint(&root_owned, 5)? {
                    if cp.lsn <= indexed && cp.lsn <= upper && cp.journal_id == identity {
                        let restored = (|| -> flow_journal::Result<State> {
                            let mut s: State = serde_json::from_value(cp.state["state"].clone())?;
                            s.values = flow_journal::value::ValueCatalog::from_snapshot(
                                cp.state["values"].clone(),
                            )?;
                            if s.applied_lsn != cp.lsn
                                || s.journal_id != identity
                                || s.runs.values().any(|r| r.terminal())
                            {
                                return Err(flow_journal::Error::Invalid(
                                    "active checkpoint boundary mismatch".into(),
                                ));
                            }
                            s.compact_audits = true;
                            Ok(s)
                        })();
                        match restored {
                            Ok(s) => {
                                state = s;
                                reader = TailReader::after(&root_owned, &identity, &cp.boundary)?;
                            }
                            Err(e) => {
                                tracing::warn!(%e, "invalid active checkpoint; replaying journal")
                            }
                        }
                    }
                }
                Ok((state, reader))
            })
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))??;
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let scan = tokio::task::spawn_blocking(move || -> flow_journal::Result<TailReader> {
            let mut reader = reader;
            while let Some((tx, _)) = reader.next(upper)? {
                if send.blocking_send(tx).is_err() {
                    break;
                }
            }
            Ok(reader)
        });
        while let Some(tx) = receive.recv().await {
            crate::journal_history::prepare(&audit_index, &mut state, &tx).await?;
            state.apply(&tx)?;
            audit_index
                .apply(
                    journal.id(),
                    tx.lsn,
                    crate::journal_history::index_rows(&state, &tx)?,
                )
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
            crate::journal_history::trim(&mut state);
        }
        let reader = scan
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))??;
        let committed = CommittedState {
            dirty_runs: state.runs.keys().cloned().collect(),
            state,
            reader: Some(reader),
            reading: None,
            pending: None,
            applying: None,
        };
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
            || projection
                .requires_split_runs()
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?
        {
            projection
                .restore_history(&history_path, upper)
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
        }
        let projected_cursor = projection
            .applied_lsn()
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?;
        let backend = Arc::new(Self {
            #[cfg(feature = "journal-cluster")]
            cluster: std::sync::OnceLock::new(),
            observations: flow_engine::observation::ObservationStore::open(
                root.join("observations"),
                Default::default(),
            )
            .ok(),
            journal,
            projection,
            audit_index,
            state: Mutex::new(committed),
            projected: Arc::new(Notify::new()),
            changed: Arc::new(Notify::new()),
            paused: Arc::new(AtomicBool::new(false)),
            shutdown: CancellationToken::new(),
            projector_stop: CancellationToken::new(),
            projector: Mutex::new(None),
            projection_error: Arc::new(Mutex::new(None)),
            projected_lsn: AtomicU64::new(projected_cursor),
            wait_timeout_ms: AtomicU64::new(30_000),
            execution: Mutex::new(None),
            execution_changed: Arc::new(Notify::new()),
            execution_peak: AtomicUsize::new(0),
            coordinator_ticks: AtomicU64::new(0),
            coordinator_last_tick_ms: AtomicI64::new(0),
            definitions: Mutex::new(std::collections::HashMap::new()),
        });
        let weak = Arc::downgrade(&backend);
        let task = tokio::spawn(async move {
            loop {
                let Some(b) = weak.upgrade() else { return };
                if b.projector_stop.is_cancelled() {
                    return;
                }
                let changed = b.changed.clone();
                let shutdown = b.projector_stop.clone();
                let next = b.projected_lsn.load(Ordering::Acquire) + 1;
                let result = async {
                    let upper = b.audit_index.applied_lsn().await?;
                    if b.paused.load(Ordering::Relaxed) || next > upper {
                        return Ok::<_, sqlx::Error>(false);
                    }
                    let rows = b.audit_index.history_changes(next).await?;
                    b.projection.apply(b.journal.id(), next, rows).await?;
                    b.projected_lsn.store(next, Ordering::Release);
                    b.projected.notify_waiters();
                    Ok(true)
                }
                .await;
                match result {
                    Ok(true) => continue,
                    Err(e) => {
                        *b.projection_error.lock().await = Some(e.to_string());
                        b.projected.notify_waiters();
                        return;
                    }
                    Ok(false) => {}
                }
                drop(b);
                tokio::select! { _=changed.notified()=>{}, _=shutdown.cancelled()=>return,
                _=tokio::time::sleep(Duration::from_millis(20))=>{} }
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

    /// Explicit whole-history diagnostic export. Production reads use run_snapshot,
    /// metadata projections or journal pages instead of allocating this export.
    pub async fn state(&self) -> State {
        self.full_state()
            .await
            .expect("verified execution index unavailable")
    }
    pub async fn full_state(&self) -> Result<State> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        let mut state = committed.state.clone();
        let upper = state.applied_lsn;
        drop(committed);
        let mut after = String::new();
        loop {
            let keys = self
                .audit_index
                .history_keys("run", None, &after, upper, 256)
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
            if keys.is_empty() {
                break;
            }
            for id in &keys {
                if let Some(run) =
                    crate::journal_history::run_at(&self.audit_index, id, upper, true).await?
                {
                    state.runs.insert(id.clone(), run);
                }
            }
            after = keys.last().unwrap().clone();
        }
        Ok(state)
    }
    pub(crate) async fn run_status(
        &self,
        id: &str,
    ) -> Result<Option<flow_engine::journal_state::RunStatus>> {
        let committed = self.state.lock().await;
        if let Some(run) = committed.state.runs.get(id) {
            return Ok(Some(run.status));
        }
        let upper = committed.state.applied_lsn;
        drop(committed);
        self.audit_index
            .history_at("run", id, upper)
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?
            .map(|v| serde_json::from_value(v["status"].clone()).map_err(JournalError::from))
            .transpose()
    }
    pub async fn run_snapshot(&self, id: &str) -> Result<Option<flow_engine::journal_state::Run>> {
        Ok(self.run_snapshot_with_cursor(id).await?.1)
    }
    pub(crate) async fn run_snapshot_with_cursor(
        &self,
        id: &str,
    ) -> Result<(u64, Option<flow_engine::journal_state::Run>)> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        let upper = committed.state.applied_lsn;
        if let Some(run) = committed.state.runs.get(id) {
            return Ok((upper, Some(run.clone())));
        }
        drop(committed);
        Ok((
            upper,
            crate::journal_history::run_at(&self.audit_index, id, upper, false).await?,
        ))
    }
    pub(crate) async fn run_children(
        &self,
        id: &str,
        upper: u64,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        Ok(self
            .audit_index
            .history_children(id, upper)
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?
            .into_iter()
            .collect())
    }
    pub(crate) async fn dispatch_owner(
        &self,
        dispatch: &str,
    ) -> Result<Option<(String, String, u32)>> {
        let owner = self
            .audit_index
            .dispatch_owner(dispatch)
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?;
        owner
            .map(|owner| {
                let run = flow_engine::journal_state::string(&owner, "run_id")?;
                let node = flow_engine::journal_state::string(&owner, "node_id")?;
                let attempt = owner["attempt"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| JournalError::Projection("invalid indexed attempt".into()))?;
                Ok((run, node, attempt))
            })
            .transpose()
    }
    async fn load_selected(&self, state: &mut State, id: Option<&str>) -> Result<()> {
        if let Some(id) = id {
            if !state.runs.contains_key(id) {
                if let Some(run) =
                    crate::journal_history::run_at(&self.audit_index, id, state.applied_lsn, false)
                        .await?
                {
                    state.runs.insert(id.into(), run);
                }
            }
        }
        Ok(())
    }
    pub fn execution_peak(&self) -> usize {
        self.execution_peak.load(Ordering::Relaxed)
    }
    pub(crate) async fn inspect<T>(&self, read: impl FnOnce(&State) -> T) -> T {
        read(&self.state.lock().await.state)
    }

    /// 取（或首次物化并缓存）某 run 的已校验 definition。definition 随
    /// run 钉死版本、创建后不可变——每个 run 只物化一次；终态由调度循环
    /// 调 [`Self::evict_definition`] 逐出。物化/校验失败不缓存，瞬时故障
    /// （如值目录暂不可读）的重试路径照常工作。
    pub(crate) async fn cached_definition(
        &self,
        run_id: &str,
        definition: &flow_journal::StoredValue,
    ) -> Result<Arc<flow_engine::Definition>> {
        if let Some(hit) = self.definitions.lock().await.get(run_id) {
            return Ok(hit.clone());
        }
        let root = self.journal.root().to_path_buf();
        let upper = self.journal.durable_lsn();
        let stored = definition.clone();
        let materialized = tokio::task::spawn_blocking(move || {
            flow_journal::value::materialize(&root, upper, &stored, flow_journal::MAX_LINE_BYTES)
        })
        .await
        .map_err(|e| flow_journal::Error::Unavailable(e.to_string()))??;
        let parsed = Arc::new(flow_engine::journal_state::validate_definition(
            &materialized,
        )?);
        let mut cache = self.definitions.lock().await;
        cache.insert(run_id.to_owned(), parsed.clone());
        while cache.len() > DEFINITION_CACHE_MAX {
            if let Some(oldest) = cache.keys().next().cloned() {
                cache.remove(&oldest);
            } else {
                break;
            }
        }
        Ok(parsed)
    }

    /// run 终态后逐出其定义缓存条目。
    pub(crate) async fn evict_definition(&self, run_id: &str) {
        self.definitions.lock().await.remove(run_id);
    }
    pub(crate) async fn internal_for_run<T>(
        &self,
        id: &str,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        self.internal_selected(Some(id), decide).await
    }
    pub(crate) async fn internal<T>(
        &self,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        self.internal_selected(None, decide).await
    }
    async fn internal_selected<T>(
        &self,
        selected: Option<&str>,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        self.load_selected(&mut committed.state, selected).await?;
        let decision = decide(&committed.state);
        crate::journal_history::trim(&mut committed.state);
        let (events, result) = decision?;
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
        self.command_selected(scope, request_id, request, None, decide)
            .await
    }
    pub(crate) async fn command_for_run(
        &self,
        run_id: &str,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, Value)>,
    ) -> Result<CommandReceipt> {
        self.command_selected(scope, request_id, request, Some(run_id), decide)
            .await
    }
    async fn command_selected(
        &self,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        selected: Option<&str>,
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
            if let Some(original) = self.command_record(&committed.state, scope, id).await? {
                if original.fingerprint != fingerprint {
                    return Err(flow_journal::Error::Conflict(
                        "same request identity with different arguments".into(),
                    )
                    .into());
                }
                let receipt = self.receipt(&original);
                drop(committed);
                return self.visible(receipt).await;
            }
        }
        self.load_selected(&mut committed.state, selected).await?;
        let decision = decide(&committed.state);
        crate::journal_history::trim(&mut committed.state);
        let (mut events, result) = decision?;
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
                let (_, original) = self
                    .audit_index
                    .get("audit", &audit_key(event)?)
                    .await
                    .map_err(|e| JournalError::Projection(e.to_string()))?;
                if let Some(original) = original {
                    let incoming = flow_journal::codec::digest(&flow_journal::codec::bounded_json(
                        event,
                        flow_journal::MAX_LINE_BYTES,
                    )?);
                    if original["digest"].as_str() != Some(incoming.as_str()) {
                        return Err(flow_journal::Error::Conflict(
                            "same audit identity with different content".into(),
                        )
                        .into());
                    }
                    return original["lsn"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| {
                            JournalError::Projection("invalid original audit receipt".into())
                        });
                }
            }
        }
        self.commit_locked(&mut state, events).await
    }
    pub async fn store_stream(
        &self,
        input: &mut (impl tokio::io::AsyncRead + Unpin),
        codec: flow_journal::value::ValueCodec,
    ) -> Result<flow_journal::ValueRef> {
        flow_journal::value::store_stream_using(self.journal.id(), input, codec, |event| async {
            self.append(vec![event]).await.map(|_| ())
        })
        .await
    }
    pub async fn store_json(
        &self,
        value: Value,
        limit: usize,
    ) -> Result<flow_journal::StoredValue> {
        let bytes = flow_journal::codec::bounded_json(&value, limit)?;
        if bytes.len() <= flow_journal::INLINE_BYTES {
            return Ok(flow_journal::StoredValue::Inline(value));
        }
        Ok(flow_journal::StoredValue::Ref(
            self.store_stream(&mut bytes.as_slice(), flow_journal::value::ValueCodec::Json)
                .await?,
        ))
    }
    async fn command_record(
        &self,
        state: &State,
        scope: &str,
        id: &str,
    ) -> Result<Option<CommandRecord>> {
        if let Some(record) = state.command_record(scope, id) {
            return Ok(Some(record.clone()));
        }
        let value = self
            .audit_index
            .get(
                "command",
                &flow_engine::journal_state::command_key(scope, id),
            )
            .await
            .map_err(|e| JournalError::Projection(e.to_string()))?
            .1;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(JournalError::from)
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
        crate::journal_history::prepare(&self.audit_index, &mut committed.state, &tx).await?;
        let checked = committed.state.check(&tx);
        crate::journal_history::trim(&mut committed.state);
        checked?;
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
        loop {
            if committed.applying.is_none() {
                if committed.reading.is_none() {
                    let upper = self.journal.durable_lsn();
                    if committed.state.applied_lsn >= upper {
                        break;
                    }
                    let mut reader = committed.reader.take().expect("owned journal reader");
                    let applied = committed.state.applied_lsn;
                    committed.reading = Some(tokio::task::spawn_blocking(move || {
                        let next = (|| {
                            while let Some((tx, _)) = reader.next(upper)? {
                                if tx.lsn > applied {
                                    return Ok(Some(tx));
                                }
                            }
                            Ok(None)
                        })();
                        (reader, next)
                    }));
                }
                // Await by reference so cancelled RPCs cannot lose the reader or its position.
                let result = committed.reading.as_mut().unwrap().await;
                committed.reading = None;
                let (reader, next) = match result {
                    Ok(result) => result,
                    Err(error) => {
                        // A panicked worker cannot return its reader. Re-scan on the next
                        // attempt; already applied LSNs are skipped below.
                        committed.reader =
                            Some(TailReader::new(self.journal.root(), self.journal.id()));
                        return Err(JournalError::Projection(error.to_string()));
                    }
                };
                committed.reader = Some(reader);
                committed.applying = next?;
            }
            let Some(tx) = committed.applying.as_ref() else {
                break;
            };
            // Keep the transaction owned across cancellation; a retry can idempotently
            // finish the index before exposing reducer state or acknowledging the audit.
            if committed.state.applied_lsn < tx.lsn {
                crate::journal_history::prepare(&self.audit_index, &mut committed.state, tx)
                    .await?;
                committed.state.apply(tx)?;
            }
            self.audit_index
                .apply(
                    self.journal.id(),
                    tx.lsn,
                    crate::journal_history::index_rows(&committed.state, tx)?,
                )
                .await
                .map_err(|e| JournalError::Projection(e.to_string()))?;
            let parents: Vec<_> = committed
                .state
                .runs
                .values()
                .filter(|r| r.terminal())
                .filter_map(|r| r.parent.as_ref().map(|p| p.run_id.clone()))
                .collect();
            committed.dirty_runs.extend(parents);
            crate::journal_history::trim(&mut committed.state);
            let tx = committed.applying.take().expect("owned transaction");
            for event in &tx.events {
                if let Some(id) = &event.run_id {
                    if committed.state.runs.get(id).is_some_and(|r| !r.terminal()) {
                        committed.dirty_runs.insert(id.clone());
                    } else {
                        committed.dirty_runs.remove(id);
                    }
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
        // notify_one（非 notify_waiters）：投影循环在批次间隙重新注册
        // waited future 之前到达的通知会丢；notify_one 会留存一个许可，
        // 把视图滞后从「最长 20ms 轮询间隔」压到「批次处理时间」。
        self.changed.notify_one();
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
        let wait = async {
            loop {
                let notified = self.projected.notified();
                if self.projected_lsn.load(Ordering::Acquire) >= receipt.commit_cursor.lsn {
                    return Ok::<(), JournalError>(());
                }
                if self.projection_error.lock().await.is_some() {
                    return Err(JournalError::Projection("projector unavailable".into()));
                }
                notified.await;
            }
        };
        let timeout = Duration::from_millis(self.wait_timeout_ms.load(Ordering::Relaxed).max(1));
        if !matches!(tokio::time::timeout(timeout, wait).await, Ok(Ok(()))) {
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
        let receipt = self
            .command_record(&committed.state, scope, request_id)
            .await?
            .map(|r| self.receipt(&r));
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
        let Some(original) = self.command_record(&committed.state, scope, id).await? else {
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
        self.changed.notify_one();
    }
    /// 调整写命令等到投影可见的上限（毫秒）。测试注入用；产品默认 30s，
    /// 健康路径下投影在毫秒级追平，超时即投影病理。
    pub fn set_projection_wait(&self, ms: u64) {
        self.wait_timeout_ms.store(ms, Ordering::Relaxed);
    }
    pub async fn close(&self) -> Result<()> {
        // 顺序要紧：先让协调器停，**投影继续跑**——协调器在飞的
        // `run_cancel`/`command` 还要等 `applied_lsn >= commit_lsn`；投影先停
        // 会让每个等待吃满 wait_timeout（默认 30s），close 表现为挂死。
        self.shutdown.cancel();
        if let Some(task) = self.execution.lock().await.take() {
            let _ = task.await;
        }
        self.projector_stop.cancel();
        if let Some(task) = self.projector.lock().await.take() {
            let _ = task.await;
        }
        if let Err(error) = self.checkpoint().await {
            tracing::warn!(%error,"checkpoint unavailable; next startup will replay journal");
        }
        let closed = self.journal.close().await;
        #[cfg(feature = "journal-cluster")]
        if let Some(cluster) = self.cluster.get() {
            let snapshot = self.cluster_snapshot().await.ok();
            cluster.close(snapshot.as_ref()).await;
        }
        self.projection.close().await;
        self.audit_index.close().await;
        closed?;
        Ok(())
    }
    #[cfg(feature = "journal-cluster")]
    pub fn cluster(&self) -> Option<&Arc<crate::journal_cluster::Membership>> {
        self.cluster.get()
    }
    #[cfg(feature = "journal-cluster")]
    pub fn install_cluster(&self, cluster: Arc<crate::journal_cluster::Membership>) -> bool {
        self.cluster.set(cluster).is_ok()
    }
    #[cfg(feature = "journal-cluster")]
    pub(crate) async fn projection_failure(&self) -> Option<String> {
        self.projection_error.lock().await.clone()
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
            flow_journal::maintenance::write_checkpoint(&root, upper, 5, snapshot)
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
        let mut state = self.state.lock().await;
        if state.state.runs.get(&id).is_some_and(|r| !r.terminal()) {
            state.dirty_runs.insert(id);
        }
    }
    pub(crate) async fn defer_runs(&self, ids: Vec<String>) {
        let mut state = self.state.lock().await;
        for id in ids {
            if state.state.runs.get(&id).is_some_and(|r| !r.terminal()) {
                state.dirty_runs.insert(id);
            }
        }
    }

    /// 诊断快照：挂死排查用（零日志冻结时看内部游标、队列与等待者）。
    pub async fn diagnostics(&self) -> serde_json::Value {
        let committed = self.state.lock().await;
        let applied_lsn = committed.state.applied_lsn;
        let resident_runs = committed.state.runs.len();
        let resident_attempts: usize = committed
            .state
            .runs
            .values()
            .flat_map(|r| r.nodes.values())
            .map(|n| n.attempts.len())
            .sum();
        let cached_publications = committed.state.values.cached_publications();
        let pending = committed.pending.is_some();
        let dirty_count = committed.dirty_runs.len();
        let dirty: Vec<String> = committed.dirty_runs.iter().take(256).cloned().collect();
        drop(committed);
        let stats = self.journal.stats();
        serde_json::json!({
            "applied_lsn": applied_lsn,
            "resident_runs":resident_runs,"resident_attempts":resident_attempts,"cached_publications":cached_publications,
            "projected_lsn": self.projected_lsn.load(std::sync::atomic::Ordering::Acquire),
            "durable_lsn": stats.durable_lsn,
            "writer_failure": stats.failure,
            "commit_pending": pending,
            "peak_queued_requests": stats.peak_queued_requests,
            "dirty_runs": dirty,
            "dirty_run_count": dirty_count,
            "projection_error": self.projection_error.lock().await.clone(),
            "executor_peak": self.execution_peak(),
            "coordinator_ticks": self.coordinator_ticks.load(Ordering::Relaxed),
            "coordinator_last_tick_ms": self.coordinator_last_tick_ms.load(Ordering::Relaxed),
            "now_ms": chrono::Utc::now().timestamp_millis(),
        })
    }
}

fn audit_key(event: &Event) -> flow_journal::Result<String> {
    let run = event
        .run_id
        .as_deref()
        .ok_or_else(|| flow_journal::Error::Invalid("audit missing run".into()))?;
    let node = event
        .node_id
        .as_deref()
        .or_else(|| event.payload["node_id"].as_str())
        .ok_or_else(|| flow_journal::Error::Invalid("audit missing node".into()))?;
    let dispatch = event
        .dispatch_id
        .as_deref()
        .ok_or_else(|| flow_journal::Error::Invalid("audit missing dispatch".into()))?;
    Ok(serde_json::to_string(&(
        run,
        node,
        dispatch,
        event.audit_seq,
    ))?)
}
pub(crate) fn audit_rows(tx: &Transaction) -> flow_journal::Result<Vec<ProjectedRow>> {
    tx.events.iter().filter(|event| event.audit_seq > 0).map(|event| {
        Ok(ProjectedRow { kind: "audit".into(), key: audit_key(event)?, value: Some(serde_json::json!({
            "digest": flow_journal::codec::digest(&flow_journal::codec::bounded_json(event, flow_journal::MAX_LINE_BYTES)?),
            "lsn": tx.lsn.to_string(),
        })) })
    }).collect()
}

fn run_row(run: &flow_engine::journal_state::Run) -> flow_journal::Result<Value> {
    let mut value = serde_json::to_value(run.summary())?;
    value["_node_layout"] = Value::String("split-v1".into());
    Ok(value)
}

pub(crate) fn project_rows(
    state: &State,
    tx: &Transaction,
) -> flow_journal::Result<Vec<ProjectedRow>> {
    let mut keys = BTreeSet::new();
    for e in &tx.events {
        if let Some(id) = &e.run_id {
            // Audit payloads update the permanent receipt index, not the product run row.
            if !matches!(
                e.kind,
                EventKind::ValueChunk | EventKind::ValuePublished | EventKind::LateAudit
            ) {
                keys.insert(("run", id.clone()));
                if e.kind == EventKind::RunStarted {
                    if let Some(run) = state.runs.get(id) {
                        for node_id in run.nodes.keys() {
                            keys.insert(("run_node", serde_json::to_string(&(id, node_id))?));
                        }
                    }
                } else if let Some(node_id) = e
                    .node_id
                    .as_deref()
                    .or_else(|| e.payload["node_id"].as_str())
                {
                    keys.insert(("run_node", serde_json::to_string(&(id, node_id))?));
                }
            }
        }
        if let (Some(run), Some(node), Some(dispatch)) = (
            e.run_id.as_deref(),
            e.node_id
                .as_deref()
                .or_else(|| e.payload["node_id"].as_str()),
            e.dispatch_id.as_deref(),
        ) {
            keys.insert((
                "run_attempt",
                serde_json::to_string(&(run, node, dispatch))?,
            ));
        }
        if e.run_seq > 0 {
            if let Some(id) = &e.run_id {
                keys.insert(("run", id.clone()));
            }
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
                "run" => state.runs.get(&key).map(run_row).transpose()?,
                "run_node" => {
                    let (run_id, node_id): (String, String) = serde_json::from_str(&key)?;
                    state
                        .runs
                        .get(&run_id)
                        .and_then(|r| r.nodes.get(&node_id))
                        .map(|node| {
                            let mut value = serde_json::to_value(node.summary())?;
                            value["_attempt_layout"] = Value::String("split-v1".into());
                            Ok::<_, serde_json::Error>(value)
                        })
                        .transpose()?
                }
                "run_attempt" => {
                    let (run, node, dispatch): (String, String, String) =
                        serde_json::from_str(&key)?;
                    state
                        .runs
                        .get(&run)
                        .and_then(|r| r.nodes.get(&node))
                        .and_then(|n| n.attempts.get(&dispatch))
                        .map(serde_json::to_value)
                        .transpose()?
                }
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
