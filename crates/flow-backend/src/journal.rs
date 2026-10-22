//! Single-coordinator decisions. SQLite atomically owns facts and current execution state.
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
    atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use flow_engine::journal_state::{CommandRecord, State};
use flow_journal::page::CommitCursor;
use flow_journal::{Event, EventKind, Journal, JournalOptions, Transaction};
use flow_store::store::{EntityChange, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify, RwLock};
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
    #[error("storage: {0}")]
    Storage(String),
}
impl From<serde_json::Error> for JournalError {
    fn from(e: serde_json::Error) -> Self {
        Self::Journal(e.into())
    }
}
type Result<T> = std::result::Result<T, JournalError>;

fn validate_command_identity(scope: &str, request_id: Option<&str>) -> flow_journal::Result<()> {
    if scope.is_empty()
        || scope.len() > 256
        || request_id.is_some_and(|s| s.is_empty() || s.len() > 128)
    {
        return Err(flow_journal::Error::Invalid(
            "invalid request identity".into(),
        ));
    }
    Ok(())
}

struct CommittedState {
    state: State,
    pending: Option<tokio::task::JoinHandle<flow_journal::Result<flow_journal::Commit>>>,
    dirty_runs: BTreeSet<String>,
}
pub struct JournalBackend {
    #[cfg(feature = "journal-cluster")]
    cluster: std::sync::OnceLock<Arc<crate::journal_cluster::Membership>>,
    pub journal: Journal,
    pub store: Arc<Store>,
    pub observations: Option<flow_engine::observation::ObservationStore>,
    state: Mutex<CommittedState>,
    /// Workflow mutations take the write guard before the state lock. Dependency
    /// freezing holds a read guard, so it can copy only selected versions while
    /// retaining one consistent metadata view across asynchronous value reads.
    pub(crate) workflow_metadata: RwLock<()>,
    shutdown: CancellationToken,
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
    /// 启动时安装的 `[harness.agent]` 运行配置：**新 run 快照的唯一来源**。
    /// 派发时从 `run.execution.agent` 读冻结值，不从这里现取——配置在进程
    /// 重启后修改只影响新 run，in-flight run 的重试/恢复身份不变。
    harness_agent: std::sync::OnceLock<Option<flow_dto::AgentRunConfig>>,
}

impl JournalBackend {
    /// 安装启动时解析的 `[harness.agent]` 配置（`start_execution` 的唯一
    /// 调用点；重复调用是编程错误，fail loud）。`None` = 未启用。
    pub fn install_harness_agent(
        &self,
        config: Option<flow_dto::AgentRunConfig>,
    ) -> std::result::Result<(), &'static str> {
        self.harness_agent
            .set(config)
            .map(|_| ())
            .map_err(|_| "harness agent config installed twice; startup path must call this once")
    }

    /// 新 run 快照的 agent 配置来源：未安装（旧调用路径/测试）视为未启用。
    pub(crate) fn harness_agent_config(&self) -> Option<flow_dto::AgentRunConfig> {
        self.harness_agent.get().cloned().flatten()
    }

    pub async fn open(root: &Path, options: JournalOptions) -> Result<Arc<Self>> {
        let journal = Journal::open(root, options).await?;
        let store = Arc::new(
            Store::open(&root.join(flow_journal::sqlite::DATABASE), journal.id())
                .await
                .map_err(|e| JournalError::Storage(e.to_string()))?,
        );
        let state =
            crate::journal_history::load(&store, journal.id(), journal.durable_lsn()).await?;
        let backend = Arc::new(Self {
            #[cfg(feature = "journal-cluster")]
            cluster: std::sync::OnceLock::new(),
            state: Mutex::new(CommittedState {
                dirty_runs: state.runs.keys().cloned().collect(),
                state,
                pending: None,
            }),
            workflow_metadata: RwLock::new(()),
            observations: flow_engine::observation::ObservationStore::open(
                root.join("observations"),
                Default::default(),
            )
            .ok(),
            journal,
            store,
            shutdown: CancellationToken::new(),
            execution: Mutex::new(None),
            execution_changed: Arc::new(Notify::new()),
            execution_peak: AtomicUsize::new(0),
            coordinator_ticks: AtomicU64::new(0),
            coordinator_last_tick_ms: AtomicI64::new(0),
            definitions: Mutex::new(std::collections::HashMap::new()),
            harness_agent: std::sync::OnceLock::new(),
        });
        Ok(backend)
    }

    /// Explicit whole-history diagnostic export. Production reads use run_snapshot,
    /// metadata queries or fact pages instead of allocating this export.
    ///
    /// Dev/诊断便捷入口：存储故障以 Err 上抛而不是 panic——这是 pub API，
    /// 一旦被 RPC 层引入，expect 就成了远程可触发的进程崩溃。
    pub async fn state(&self) -> Result<State> {
        self.full_state().await
    }
    pub async fn full_state(&self) -> Result<State> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        let mut state = committed.state.clone();
        let upper = state.applied_lsn;
        let mut after = String::new();
        loop {
            let keys = self
                .store
                .current_keys("run", None, &after, upper, 256)
                .await
                .map_err(|e| JournalError::Storage(e.to_string()))?;
            if keys.is_empty() {
                break;
            }
            for id in &keys {
                if let Some(run) =
                    crate::journal_history::run_at(&self.store, id, upper, true).await?
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
        self.store
            .current_at("run", id, upper)
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?
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
        Ok((
            upper,
            crate::journal_history::run_at(&self.store, id, upper, false).await?,
        ))
    }
    pub(crate) async fn run_children(
        &self,
        id: &str,
        upper: u64,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        Ok(self
            .store
            .children(id, upper)
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?
            .into_iter()
            .collect())
    }
    pub(crate) async fn dispatch_owner(
        &self,
        dispatch: &str,
    ) -> Result<Option<(String, String, u32)>> {
        let owner = self
            .store
            .dispatch_owner(dispatch)
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?;
        owner
            .map(|owner| {
                let run = flow_engine::journal_state::string(&owner, "run_id")?;
                let node = flow_engine::journal_state::string(&owner, "node_id")?;
                let attempt = owner["attempt"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| JournalError::Storage("invalid indexed attempt".into()))?;
                Ok((run, node, attempt))
            })
            .transpose()
    }
    async fn load_selected(&self, state: &mut State, id: Option<&str>) -> Result<()> {
        if let Some(id) = id {
            if !state.runs.contains_key(id) {
                if let Some(run) =
                    crate::journal_history::run_at(&self.store, id, state.applied_lsn, false)
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

    /// Finish any commit whose caller was cancelled before its receipt arrived.
    /// Workflow preflight must see that committed version before storing values.
    pub(crate) async fn inspect_committed<T>(
        &self,
        read: impl FnOnce(&State) -> flow_journal::Result<T>,
    ) -> Result<T> {
        let mut committed = self.state.lock().await;
        self.refresh_locked(&mut committed).await?;
        Ok(read(&committed.state)?)
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
    /// Facts, current state and permanent receipts share one SQLite commit.
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
        validate_command_identity(scope, request_id)?;
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
        // Raw metadata facts (offline import/maintenance) obey the same ordering.
        let _metadata = if events.iter().any(|e| {
            matches!(
                e.kind,
                EventKind::WorkflowCreated
                    | EventKind::WorkflowUpdated
                    | EventKind::WorkflowPublished
                    | EventKind::WorkflowDeleted
            )
        }) {
            Some(self.workflow_metadata.write().await)
        } else {
            None
        };
        let mut state = self.state.lock().await;
        self.refresh_locked(&mut state).await?;
        if let [event] = events.as_slice() {
            if event.audit_seq > 0 {
                let (_, original) = self
                    .store
                    .get("audit", &audit_key(event)?)
                    .await
                    .map_err(|e| JournalError::Storage(e.to_string()))?;
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
                            JournalError::Storage("invalid original audit receipt".into())
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
            .store
            .get(
                "command",
                &flow_engine::journal_state::command_key(scope, id),
            )
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?
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
        crate::journal_history::prepare(&self.store, &mut committed.state, &tx).await?;
        let rows = committed
            .state
            .preview(&tx, |next| crate::journal_history::index_rows(next, &tx))?;
        let journal = self.journal.clone();
        let changed = self.execution_changed.clone();
        committed.pending = Some(tokio::spawn(async move {
            let result = journal.submit_prepared(tx, rows).await;
            changed.notify_one();
            result
        }));
        self.refresh_locked(committed)
            .await?
            .ok_or_else(|| JournalError::Storage("missing pending commit receipt".into()))
    }
    async fn refresh_locked(&self, committed: &mut CommittedState) -> Result<Option<u64>> {
        let Some(pending) = committed.pending.as_mut() else {
            return Ok(None);
        };
        // Await by reference: cancellation leaves the complete accepted decision owned here.
        let outcome = pending
            .await
            .map_err(|e| JournalError::Storage(e.to_string()));
        committed.pending = None;
        let commit = outcome??;
        let tx = &commit.transaction;
        committed.state.apply(tx)?;
        let parents: Vec<_> = committed
            .state
            .runs
            .values()
            .filter(|r| r.terminal())
            .filter_map(|r| r.parent.as_ref().map(|p| p.run_id.clone()))
            .collect();
        committed.dirty_runs.extend(parents);
        crate::journal_history::trim(&mut committed.state);
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
        if !committed.dirty_runs.is_empty() {
            self.execution_changed.notify_one();
        }
        Ok(Some(tx.lsn))
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
        // The current rows and receipt became visible at the same COMMIT as their facts.
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
            .store
            .applied_lsn()
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?
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
        validate_command_identity(scope, request_id)?;
        let Some(id) = request_id else {
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
    pub async fn close(&self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(task) = self.execution.lock().await.take() {
            let _ = task.await;
        }
        let pending = {
            let mut committed = self.state.lock().await;
            self.refresh_locked(&mut committed).await
        };
        let closed = self.journal.close().await;
        #[cfg(feature = "journal-cluster")]
        if let Some(cluster) = self.cluster.get() {
            let snapshot = self.cluster_snapshot().await.ok();
            cluster.close(snapshot.as_ref()).await;
        }
        self.store.close().await;
        pending?;
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
    pub async fn checkpoint(&self) -> Result<()> {
        let root = self.journal.root().to_path_buf();
        tokio::task::spawn_blocking(move || flow_journal::maintenance::checkpoint(&root))
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))??;
        Ok(())
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.shutdown.clone()
    }
    pub(crate) fn execution_notification(&self) -> Arc<Notify> {
        self.execution_changed.clone()
    }
    pub(crate) async fn take_dirty_runs(&self) -> Vec<String> {
        let mut committed = self.state.lock().await;
        if let Err(error) = self.refresh_locked(&mut committed).await {
            tracing::error!(%error,"cannot finish accepted SQLite transition");
            self.shutdown.cancel();
            return Vec::new();
        }
        std::mem::take(&mut committed.dirty_runs)
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
            "projected_lsn": stats.durable_lsn,
            "durable_lsn": stats.durable_lsn,
            "writer_failure": stats.failure,
            "commit_pending": pending,
            "peak_queued_requests": stats.peak_queued_requests,
            "dirty_runs": dirty,
            "dirty_run_count": dirty_count,
            "projection_error": Option::<String>::None,
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
pub(crate) fn audit_rows(tx: &Transaction) -> flow_journal::Result<Vec<EntityChange>> {
    tx.events.iter().filter(|event| event.audit_seq > 0).map(|event| {
        Ok(EntityChange { kind: "audit".into(), key: audit_key(event)?, value: Some(serde_json::json!({
            "digest": flow_journal::codec::digest(&flow_journal::codec::bounded_json(event, flow_journal::MAX_LINE_BYTES)?),
            "lsn": tx.lsn.to_string(),
        })) })
    }).collect()
}

fn run_row(run: &flow_engine::journal_state::Run) -> flow_journal::Result<Value> {
    Ok(serde_json::to_value(run.summary())?)
}

pub(crate) fn project_rows(
    state: &State,
    tx: &Transaction,
) -> flow_journal::Result<Vec<EntityChange>> {
    let mut keys = BTreeSet::new();
    for e in &tx.events {
        if let Some(id) = &e.run_id {
            // Audit payloads update the permanent receipt index, not the product run row.
            if !e.is_audit_only() {
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
                    let version = match e.kind {
                        EventKind::WorkflowUpdated => e.payload["version"]["version"].as_u64(),
                        EventKind::WorkflowPublished => {
                            Some(flow_engine::journal_state::number(&e.payload, "version")?)
                        }
                        _ => None,
                    };
                    if let Some(version) = version {
                        keys.insert((
                            "workflow_version",
                            serde_json::to_string(&(id, format!("{version:020}")))?,
                        ));
                    }
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
                            serde_json::to_value(node.summary())
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
                "workflow" => state.workflows.get(&key).map(|workflow| {
                    let mut summary=serde_json::json!({"workflow_id":workflow.workflow_id,"name":workflow.name,"created_at":workflow.created_at,"versions":{}});
                    summary["deleted"]=serde_json::json!(workflow.deleted);
                    summary["latest_version"]=serde_json::json!(workflow.versions.last_key_value().map_or(0,|(v,_)|*v));
                    summary["published_version"]=serde_json::json!(workflow.versions.iter().rev().find(|(_,v)|v.published).map(|(v,_)|*v));
                    summary
                }),
                "workflow_version" => {
                    let (id,version):(String,String)=serde_json::from_str(&key)?;
                    let version=version.parse::<u64>().map_err(|_|flow_journal::Error::Invalid("invalid version key".into()))?;
                    state.workflows.get(&id).and_then(|w|w.versions.get(&version)).map(serde_json::to_value).transpose()?
                },
                "schedule" => state.schedules.get(&key).cloned(),
                "webhook" => state.webhooks.get(&key).cloned(),
                "template" => state.templates.get(&key).cloned(),
                "legacy" => state.legacy.get(&key).cloned(),
                _ => None,
            };
            Ok(EntityChange {
                kind: kind.into(),
                key,
                value,
            })
        })
        .collect()
}
