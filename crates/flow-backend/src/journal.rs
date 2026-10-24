//! Single-coordinator decisions. SQLite atomically owns facts and current execution state.
//!
//! 本模块是 journal 的唯一权威后端：产品命令、协调循环与执行端口的记账都
//! 经过这里，把「事实、当前状态、永久回执、待处理动作」放进同一个 SQLite
//! 事务。它守护的核心不变量是「没有事实就没有决策」与「同一提交决定」——
//! 内存里的 `State` 只是已提交事实的缓存，崩溃后一律以 SQLite 为准。
//! 需要它，是因为 SQLite 单写者 + 排他锁是当前唯一的权威边界（C1/C5）：
//! 任何绕过本模块直写数据库的路径都会让状态与事实失去对应关系。
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

/// 命令提交后的永久回执：`scope` + `request_id` 命中同一条已提交命令时
/// 返回同一份内容，因此调用方可以安全重试而不会重复产生副作用。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub committed: bool,
    pub commit_cursor: CommitCursor,
    pub request_id: Option<String>,
    pub result: Value,
    pub visible: bool,
}

/// 后端统一错误面：`LinkClosed` 表示执行端口断开，`Journal` 透传 journal
/// 的类型化错误（Invalid/Conflict/Limit/NotFound…），`Storage` 覆盖读侧
/// 与提交后置失败的兜底——读面与写面共享同一套错误分类，RPC 层才能把
/// 存储故障与业务拒绝区分开。
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

/// 校验命令身份（幂等键）的形状：scope 与 request_id 的长度上限是
/// 索引键与回执存储的预算边界，越界直接拒绝而不是截断——截断会让两个
/// 不同请求撞上同一个幂等键。
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

/// 已提交状态的内存镜像：事实、当前状态与「需要重新调度」的 run 集合。
/// `pending` 表示有一个提交在途，`failure` 表示权威库已不可信——两者
/// 都会让后续决策 fail closed（见 [`JournalBackend::begin_decision`]）。
struct CommittedState {
    state: State,
    pending: bool,
    failure: Option<String>,
    dirty_runs: BTreeSet<String>,
}

/// journal 权威后端：持有 writer、只读 store 与内存状态投影。
///
/// 锁序固定为 `workflow_metadata → decision → state`：`decision` 把写决策、
/// 持久化与状态发布串成一段临界区，`state` 只保护同步内存访问，任何
/// 跨 SQLite 查询或落盘等待都不得持有 `state`。
pub struct JournalBackend {
    pub journal: Journal,
    pub store: Arc<Store>,
    pub observations: Option<flow_engine::observation::ObservationStore>,
    state: Arc<Mutex<CommittedState>>,
    // Lock order: workflow_metadata → decision → state. Never await IO with state held.
    decision: Arc<Mutex<()>>,
    /// Workflow mutations take the write guard before the state lock. Dependency
    /// freezing holds a read guard, so it can copy only selected versions while
    /// retaining one consistent metadata view across asynchronous value reads.
    pub(crate) workflow_metadata: RwLock<()>,
    shutdown: CancellationToken,
    pub(crate) execution: Mutex<Option<tokio::task::JoinHandle<()>>>,
    execution_changed: Arc<Notify>,
    commits: tokio::sync::watch::Sender<()>,
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
    ///
    /// 用 `OnceLock` 而不是可写字段：快照来源必须唯一，否则同一个 run
    /// 的两处 agent 配置会分叉，冻结语义随之失效。
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

    /// 打开（或创建）数据目录上的权威库，并从已提交的当前行恢复内存状态。
    ///
    /// 启动只加载当前实体，不全量回放历史（ADR 0002）：回放会让启动时间
    /// 与历史长度成正比，而当前行本来就是事实的物化结果。
    pub async fn open(root: &Path, options: JournalOptions) -> Result<Arc<Self>> {
        let journal = Journal::open(root, options).await?;
        let store = Arc::new(
            Store::open(&root.join(flow_journal::sqlite::DATABASE), journal.id())
                .await
                .map_err(|e| JournalError::Storage(e.to_string()))?,
        );
        // 恢复出的 run 集合即当前的 dirty 集：启动后协调循环要先看一遍
        // 在飞 run，才能把中断的等待与派发接上。
        let state =
            crate::journal_history::load(&store, journal.id(), journal.durable_lsn()).await?;
        let backend = Arc::new(Self {
            state: Arc::new(Mutex::new(CommittedState {
                dirty_runs: state.runs.keys().cloned().collect(),
                state,
                pending: false,
                failure: None,
            })),
            decision: Arc::new(Mutex::new(())),
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
            commits: tokio::sync::watch::channel(()).0,
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

    /// 组装包含全部历史工作流与 run 的完整状态快照（诊断/导出用）。
    ///
    /// 与启动恢复的差别在于：这里把每个工作流的全部版本和每个 run 的完整
    /// 记录都拼出来，代价随历史增长，因此生产读面不得走这条路径。
    pub async fn full_state(&self) -> Result<State> {
        let _decision = self.begin_decision().await?;
        let mut state = self.inspect(Clone::clone).await;
        let upper = state.applied_lsn;
        // Explicit diagnostic export may assemble history; normal recovery and
        // command decisions retain only the two hot versions per workflow.
        for (id, workflow) in &mut state.workflows {
            if let Some(row) = self
                .store
                .get("workflow", id)
                .await
                .map_err(|e| JournalError::Storage(e.to_string()))?
                .1
            {
                *workflow = serde_json::from_value(row)?;
            }
        }
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

    /// 读单个 run 的当前状态；内存里没有时回落到只读 store。
    pub(crate) async fn run_status(
        &self,
        id: &str,
    ) -> Result<Option<flow_engine::journal_state::RunStatus>> {
        if let Some(status) = self.inspect(|s| s.runs.get(id).map(|run| run.status)).await {
            return Ok(Some(status));
        }
        let _decision = self.begin_decision().await?;
        let upper = self.inspect(|s| s.applied_lsn).await;
        self.store
            .current_at("run", id, upper)
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?
            .map(|v| serde_json::from_value(v["status"].clone()).map_err(JournalError::from))
            .transpose()
    }

    /// 读单个 run 的当前快照（内存优先，回落只读 store）。
    pub async fn run_snapshot(&self, id: &str) -> Result<Option<flow_engine::journal_state::Run>> {
        Ok(self.run_snapshot_with_cursor(id).await?.1)
    }

    /// 同 [`Self::run_snapshot`]，但一并返回读取所依据的 LSN 游标。
    ///
    /// 游标必须与快照同源：读面用它来定位「快照之后的新事实」，跨游标读取
    /// 会让调用方看到拼接出来的、不属于任何单一提交的状态。
    pub(crate) async fn run_snapshot_with_cursor(
        &self,
        id: &str,
    ) -> Result<(u64, Option<flow_engine::journal_state::Run>)> {
        let (upper, run) = self
            .inspect(|s| (s.applied_lsn, s.runs.get(id).cloned()))
            .await;
        if run.is_some() {
            return Ok((upper, run));
        }
        let _decision = self.begin_decision().await?;
        let upper = self.inspect(|s| s.applied_lsn).await;
        Ok((
            upper,
            crate::journal_history::run_at(&self.store, id, upper, false).await?,
        ))
    }

    /// 列出该 run 直接创建的子 run（按节点 ID 索引）。
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

    /// 查某个 dispatch 的归属（run、node、attempt），用于迟到结果的归属对账。
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
                // attempt 缺失或不可解析说明索引行畸形，宁可报错也不猜。
                let attempt = owner["attempt"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| JournalError::Storage("invalid indexed attempt".into()))?;
                Ok((run, node, attempt))
            })
            .transpose()
    }

    /// 确保被命令选中的 run 已载入内存状态（否则决策会看到「run 不存在」）。
    async fn load_selected(&self, id: Option<&str>) -> Result<()> {
        if let Some(id) = id {
            let missing = self.inspect(|s| !s.runs.contains_key(id)).await;
            if missing {
                let upper = self.inspect(|s| s.applied_lsn).await;
                if let Some(run) =
                    crate::journal_history::run_at(&self.store, id, upper, false).await?
                {
                    self.update_state(|s| {
                        s.runs.insert(id.into(), run);
                    })
                    .await;
                }
            }
        }
        Ok(())
    }

    /// 在状态锁保护下修改内存状态投影。
    ///
    /// 调用方必须已持有 `decision` 锁；本函数自身不落盘，持久化由后续
    /// 提交路径负责——内存先行、落盘随后是崩溃语义的关键（未提交的状态
    /// 在重启后自然消失）。
    pub(crate) async fn update_state<T>(&self, update: impl FnOnce(&mut State) -> T) -> T {
        update(&mut self.state.lock().await.state)
    }

    /// 取得决策锁，并在权威库已不可信时 fail closed。
    ///
    /// `failure` 表示 writer 或 reducer 在提交后永久失败，`pending` 表示
    /// 某个提交任务在落盘途中被中止——两种情况下的内存状态都不再可信，
    /// 必须拒绝新决策，而不是基于可能过期的状态继续迁移。
    async fn begin_decision(&self) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        let decision = self.decision.clone().lock_owned().await;
        let committed = self.state.lock().await;
        if let Some(error) = &committed.failure {
            return Err(JournalError::Storage(error.clone()));
        }
        if committed.pending {
            return Err(JournalError::Storage(
                "commit task stopped before publishing state".into(),
            ));
        }
        Ok(decision)
    }

    /// 历史上同时在飞的执行器数量峰值（诊断用）。
    pub fn execution_peak(&self) -> usize {
        self.execution_peak.load(Ordering::Relaxed)
    }

    /// 只读地观察当前已提交状态。
    pub(crate) async fn inspect<T>(&self, read: impl FnOnce(&State) -> T) -> T {
        read(&self.state.lock().await.state)
    }

    /// Finish any commit whose caller was cancelled before its receipt arrived.
    /// Workflow preflight must see that committed version before storing values.
    pub(crate) async fn inspect_committed<T>(
        &self,
        read: impl FnOnce(&State) -> flow_journal::Result<T>,
    ) -> Result<T> {
        let _decision = self.begin_decision().await?;
        Ok(self.inspect(read).await?)
    }

    /// Read a historical definition without growing the coordinator's hot state.
    pub(crate) async fn definition_version(
        &self,
        workflow: &str,
        version: Option<u64>,
    ) -> Result<Option<flow_engine::journal_state::DefinitionVersion>> {
        let _decision = self.begin_decision().await?;
        let selected = self
            .inspect(|state| {
                let metadata = state.workflows.get(workflow)?;
                let version =
                    version.or_else(|| metadata.versions.last_key_value().map(|(v, _)| *v))?;
                Some((
                    version,
                    metadata.versions.get(&version).cloned(),
                    state.applied_lsn,
                ))
            })
            .await;
        let Some((version, hit, upper)) = selected else {
            return Ok(None);
        };
        if hit.is_some() {
            return Ok(hit);
        }
        self.store
            .current_at(
                "workflow_version",
                &crate::journal_history::version_key(workflow, version)?,
                upper,
            )
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))?
            .map(serde_json::from_value)
            .transpose()
            .map_err(JournalError::from)
    }

    /// 读某个工作流的全部已发布/草稿版本（历史 UI 用）。
    pub(crate) async fn definition_versions(
        &self,
        workflow: &str,
    ) -> Result<Vec<flow_engine::journal_state::DefinitionVersion>> {
        // Store::get assembles versions in one bounded SQLite read transaction.
        // Historical UI reads must not hold the coordinator's decision lock.
        let row = self
            .store
            .get("workflow", workflow)
            .await
            .map_err(|error| match error {
                flow_store::store::Error::ResponseTooLarge => {
                    JournalError::Journal(flow_journal::Error::Limit("RESPONSE_TOO_LARGE".into()))
                }
                other => JournalError::Storage(other.to_string()),
            })?
            .1
            .ok_or_else(|| flow_journal::Error::NotFound("workflow not found".into()))?;
        let workflow: flow_engine::journal_state::Workflow = serde_json::from_value(row)?;
        Ok(workflow.versions.into_values().rev().collect())
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
    /// 为某个 run 提交一条内部事实（协调器专用路径，无命令回执）。
    pub(crate) async fn internal_for_run<T>(
        &self,
        id: &str,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        self.internal_selected(Some(id), None, decide).await
    }

    /// 提交一条内部事实（不针对特定 run）。
    pub(crate) async fn internal<T>(
        &self,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        self.internal_selected(None, None, decide).await
    }

    /// 提交一条内部事实，并附带一个待发布的业务值（与事实同事务落盘）。
    pub(crate) async fn internal_for_run_with_value<T>(
        &self,
        id: &str,
        value: Option<flow_journal::value::PreparedValue>,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        self.internal_selected(Some(id), value, decide).await
    }

    /// 内部事实的提交路径：决策 → 提交 → 发布状态。
    ///
    /// 关键顺序：`decide` 在状态锁下算出事件，`trim` 立即裁剪热状态，
    /// 随后才把事件与正文一起提交。未提交的事实与状态在崩溃后自然消失，
    /// 这正是「内存先行、落盘随后」想要的崩溃语义。
    async fn internal_selected<T>(
        &self,
        selected: Option<&str>,
        value: Option<flow_journal::value::PreparedValue>,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, T)>,
    ) -> Result<T> {
        let guard = self.begin_decision().await?;
        self.load_selected(selected).await?;
        let decision = self
            .update_state(|state| {
                let result = decide(state);
                crate::journal_history::trim(state);
                result
            })
            .await;
        let (mut events, result) = decision?;
        if !events.is_empty() {
            let values: Vec<_> = value
                .and_then(|value| value.into_staged())
                .into_iter()
                .collect();
            // 值发布事件必须排在最前：reducer 只有先看到 ValuePublished，
            // 才能把后续引用它的事件还原出来。
            for value in &values {
                events.insert(0, value.publication()?);
            }
            self.commit_values(guard, events, values).await?;
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
        self.command_selected(scope, request_id, request, None, Vec::new(), decide)
            .await
    }
    /// 针对某个 run 的命令提交（会先把该 run 载入内存再决策）。
    pub(crate) async fn command_for_run(
        &self,
        run_id: &str,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, Value)>,
    ) -> Result<CommandReceipt> {
        self.command_selected(scope, request_id, request, Some(run_id), Vec::new(), decide)
            .await
    }
    /// 带业务值的命令提交：值与事实同事务落盘，不会出现「事实已提交、
    /// 正文未发布」的中间态。
    pub(crate) async fn command_with_value(
        &self,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        selected: Option<&str>,
        value: flow_journal::value::PreparedValue,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, Value)>,
    ) -> Result<CommandReceipt> {
        self.command_selected(scope, request_id, request, selected, vec![value], decide)
            .await
    }
    /// 所有产品命令的共同入口：幂等校验 → 决策 → 事实 + 回执 + 正文同事务提交。
    ///
    /// 幂等语义由 `scope` + `request_id` + 请求指纹三者共同定义：命中已有
    /// 回执且指纹相同则原样返回；指纹不同则报 Conflict——同一身份携带不同
    /// 参数说明调用方有 bug，静默挑一个结果比报错危险。
    pub(crate) async fn command_selected(
        &self,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        selected: Option<&str>,
        values: Vec<flow_journal::value::PreparedValue>,
        decide: impl FnOnce(&State) -> flow_journal::Result<(Vec<Event>, Value)>,
    ) -> Result<CommandReceipt> {
        validate_command_identity(scope, request_id)?;
        // 指纹预算取 9 MiB：略大于单条值的 8 MiB 上限，保证大输入也能
        // 参与幂等比较，而不是因为超预算而跳过去重。
        let fingerprint = flow_journal::codec::fingerprint(request, 9 * 1024 * 1024)?;
        let guard = self.begin_decision().await?;
        if let Some(id) = request_id {
            if let Some(original) = self.command_record(scope, id).await? {
                if original.fingerprint != fingerprint {
                    return Err(flow_journal::Error::Conflict(
                        "same request identity with different arguments".into(),
                    )
                    .into());
                }
                return self.visible(self.receipt(&original)).await;
            }
        }
        self.load_selected(selected).await?;
        crate::journal_history::prepare_command(self, request).await?;
        let decision = self
            .update_state(|state| {
                let result = decide(state);
                crate::journal_history::trim(state);
                result
            })
            .await;
        let (mut events, result) = decision?;
        // 回执正文必须能内联进一行事务，超限就报错而不是让提交在写者侧失败。
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
        let values: Vec<_> = values
            .into_iter()
            .filter_map(|value| value.into_staged())
            .collect();
        // 值发布排在最前，与 internal_selected 保持同一条 reducer 约定。
        for value in &values {
            events.insert(0, value.publication()?);
        }
        let commit = self.commit_values(guard, events, values).await?;
        let mut receipt = self.receipt(&record);
        receipt.commit_cursor.lsn = commit;
        self.visible(receipt).await
    }

    /// 直接追加原始事实（离线导入与维护路径）。
    ///
    /// 走的是与命令完全相同的提交协议，因此离线维护写入的事实同样受
    /// 「事实 → 当前行」同一提交约束，不能绕过投影。
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
        let guard = self.begin_decision().await?;
        if let [event] = events.as_slice() {
            if event.audit_seq > 0 {
                // 审计事件按身份幂等：同一审计身份且内容一致时直接返回原
                // 回执，内容不同则报冲突——审计行是永久回执，不能被改写。
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
        self.commit_values(guard, events, Vec::new()).await
    }

    /// 把一条字节流写成正文并返回其引用（分块落盘后发布 ValuePublished）。
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

    /// 按大小选择内联或落盘：超过内联上限的值走正文分块，避免撑爆事务行。
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

    /// 查一条已提交命令的永久回执（内存优先，回落只读 store）。
    async fn command_record(&self, scope: &str, id: &str) -> Result<Option<CommandRecord>> {
        if let Some(record) = self.inspect(|s| s.command_record(scope, id).cloned()).await {
            return Ok(Some(record));
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

    /// 把事件、正文与投影行提交为一个 SQLite 事务，并把结果应用到内存状态。
    ///
    /// 这是「同一提交决定」的落点：事实、当前行、永久回执与正文在一次
    /// COMMIT 里可见。提交任务被独立 spawn——调用方取消时任务仍会跑完，
    /// 否则取消会留下「事实已落盘、状态未发布」的裂缝。
    async fn commit_values(
        &self,
        decision: tokio::sync::OwnedMutexGuard<()>,
        events: Vec<Event>,
        values: Vec<flow_journal::value::StagedValue>,
    ) -> Result<u64> {
        let upper = self.inspect(|s| s.applied_lsn).await;
        let chunks: u64 = values.iter().map(|value| value.chunk_count()).sum();
        // 预留正文分块占用的 LSN，保证事务号大于所有已用 LSN，避免倒退。
        let final_lsn = upper
            .checked_add(chunks)
            .and_then(|lsn| lsn.checked_add(1))
            .ok_or_else(|| flow_journal::Error::Invalid("LSN overflow".into()))?;
        let tx = Transaction {
            v: 2,
            journal_id: self.journal.id().into(),
            lsn: final_lsn,
            tx_id: uuid::Uuid::now_v7().to_string(),
            events,
        };
        crate::journal_history::prepare(self, &tx).await?;
        let mut transactions = Vec::new();
        for value in &values {
            transactions.extend(value.prefix(upper + transactions.len() as u64 + 1));
        }
        transactions.push(tx.clone());
        // preview 先把本次事务的投影行算出来：reducer 的 UndoLog 让失败
        // 时的回滚与成功时的落库共用同一条路径。
        let rows = self
            .update_state(|state| {
                state.preview_batch(&transactions, |next| {
                    crate::journal_history::index_rows(next, &tx)
                })
            })
            .await?;
        let journal = self.journal.clone();
        let changed = self.execution_changed.clone();
        let state = self.state.clone();
        let shutdown = self.shutdown.clone();
        let commits = self.commits.clone();
        self.state.lock().await.pending = true;
        // The accepted task owns the decision guard through durable commit and apply.
        // Dropping the caller only drops its JoinHandle; this task still finishes.
        tokio::spawn(async move {
            let result = journal.submit_prepared_values(tx, rows, values).await;
            // A rejected transition rolls back cleanly and must not poison the backend.
            // A writer failure, or reducer failure after COMMIT, is unrecoverable here.
            let durable = result.is_ok();
            let writer_failed = journal.stats().failure.is_some();
            let mut committed = state.lock().await;
            let result = result.and_then(|commit| Self::apply_commit(&mut committed, commit));
            committed.pending = false;
            if let Err(error) = &result {
                if durable || writer_failed {
                    // 已落盘或写者已坏：内存状态不再可信，永久失败并停机。
                    // 未落盘的拒绝（reducer 拒绝的非法迁移）只回滚，不停机。
                    committed.failure = Some(error.to_string());
                    shutdown.cancel();
                }
            }
            drop(committed);
            if result.is_ok() {
                commits.send_replace(());
            }
            changed.notify_one();
            drop(decision);
            result
        })
        .await
        .map_err(|e| JournalError::Storage(e.to_string()))?
        .map_err(JournalError::from)
    }

    /// 把已提交的事务应用到内存状态，并标记需要重新调度的 run。
    fn apply_commit(
        committed: &mut CommittedState,
        commit: flow_journal::Commit,
    ) -> flow_journal::Result<u64> {
        let tx = &commit.transaction;
        if commit.value_prefix.is_empty() {
            committed.state.apply(tx)?;
        } else {
            // 有正文分块时，先应用分块前缀事务再应用主事务，reducer 才能
            // 看到「值已发布」这一事实。
            let mut transactions = commit.value_prefix;
            transactions.push((**tx).clone());
            committed.state.apply_batch(&transactions)?;
        }
        // 子 run 终态会改变父 run 的等待条件，父 run 必须重新评估。
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
                    // 取消父 run 会级联影响子 run，子 run 必须一起重新评估。
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
        Ok(tx.lsn)
    }

    /// 由已提交的命令记录构造回执（`visible` 由调用方按可见性设置）。
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

    /// 标记回执可见：当前行与回执在同一次 COMMIT 里可见，没有投影追赶窗口。
    async fn visible(&self, mut receipt: CommandReceipt) -> Result<CommandReceipt> {
        // The current rows and receipt became visible at the same COMMIT as their facts.
        receipt.visible = true;
        Ok(receipt)
    }

    /// 按 scope + request_id 查历史命令回执（供不确定交付后的重试核对）。
    pub async fn command_status(
        &self,
        scope: &str,
        request_id: &str,
    ) -> Result<Option<CommandReceipt>> {
        let _decision = self.begin_decision().await?;
        let receipt = self
            .command_record(scope, request_id)
            .await?
            .map(|r| self.receipt(&r));
        let Some(mut receipt) = receipt else {
            return Ok(None);
        };
        // 可见性以已应用 LSN 为准：回执已写入但状态尚未应用时不能对外
        // 声称「已可见」。
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
        if request_id.is_none() {
            validate_command_identity(scope, request_id)?;
            return Ok(None);
        }
        self.preflight_command(scope, request_id, request, None, |_| Ok(()))
            .await
    }

    /// Check permanent receipts and current state under the decision guard. A concurrent
    /// duplicate must return its receipt before checking a now-terminal state.
    pub(crate) async fn preflight_command(
        &self,
        scope: &str,
        request_id: Option<&str>,
        request: &Value,
        selected: Option<&str>,
        validate: impl FnOnce(&State) -> flow_journal::Result<()>,
    ) -> Result<Option<CommandReceipt>> {
        validate_command_identity(scope, request_id)?;
        let fingerprint = flow_journal::codec::fingerprint(request, 9 * 1024 * 1024)?;
        let _decision = self.begin_decision().await?;
        if let Some(id) = request_id {
            if let Some(original) = self.command_record(scope, id).await? {
                if original.fingerprint != fingerprint {
                    return Err(flow_journal::Error::Conflict(
                        "same request identity with different arguments".into(),
                    )
                    .into());
                }
                return Ok(Some(self.visible(self.receipt(&original)).await?));
            }
        }
        self.load_selected(selected).await?;
        self.update_state(|state| {
            let result = validate(state);
            crate::journal_history::trim(state);
            result
        })
        .await?;
        Ok(None)
    }
    /// 关闭后端：停协调循环、等待在途执行会话回收、再关闭写者与读侧。
    ///
    /// 顺序不可颠倒：必须先让执行端口停下并回收子进程，再关写者——
    /// 反过来会让执行器在权威库关闭后仍尝试写入。
    pub async fn close(&self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(task) = self.execution.lock().await.take() {
            let _ = task.await;
        }
        // 取决策锁确保没有提交在途，然后读出永久失败原因（如有）。
        let _decision = self.decision.lock().await;
        let failure = self.state.lock().await.failure.clone();
        let closed = self.journal.close().await;
        self.store.close().await;
        if let Some(error) = failure {
            return Err(JournalError::Storage(error));
        }
        closed?;
        Ok(())
    }

    /// 对 SQLite 做一次 WAL checkpoint（离线维护用）。
    pub async fn checkpoint(&self) -> Result<()> {
        let root = self.journal.root().to_path_buf();
        tokio::task::spawn_blocking(move || flow_journal::maintenance::checkpoint(&root))
            .await
            .map_err(|e| JournalError::Storage(e.to_string()))??;
        Ok(())
    }

    /// 停机信号：权威库不可信时会被取消，协调循环随即退出。
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// 状态变更通知（提交完成或协调器需要重新调度时唤醒）。
    pub(crate) fn execution_notification(&self) -> Arc<Notify> {
        self.execution_changed.clone()
    }

    /// 订阅提交完成事件（每次成功提交后发一次）。
    pub(crate) fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<()> {
        self.commits.subscribe()
    }

    /// 取走需要重新调度的 run 集合（协调循环每轮取一次）。
    pub(crate) async fn take_dirty_runs(&self) -> Vec<String> {
        let mut committed = self.state.lock().await;

        std::mem::take(&mut committed.dirty_runs)
            .into_iter()
            .collect()
    }

    /// 把一个非终态 run 标记为待重新调度。
    pub(crate) async fn defer_run(&self, id: String) {
        let mut state = self.state.lock().await;
        // 终态 run 不再需要调度，标记它只会浪费协调循环的一轮。
        if state.state.runs.get(&id).is_some_and(|r| !r.terminal()) {
            state.dirty_runs.insert(id);
        }
    }

    /// 批量把非终态 run 标记为待重新调度。
    pub(crate) async fn defer_runs(&self, ids: Vec<String>) {
        let mut state = self.state.lock().await;
        for id in ids {
            if state.state.runs.get(&id).is_some_and(|r| !r.terminal()) {
                state.dirty_runs.insert(id);
            }
        }
    }

    /// 诊断快照：挂死排查用（零日志冻结时看内部游标、队列与等待者）。
    ///
    /// 字段刻意保持诊断语义而非业务语义：`applied_lsn` 与 `durable_lsn`
    /// 的差距直接指向「已提交但未应用」的裂缝。
    pub async fn diagnostics(&self) -> serde_json::Value {
        let committed = self.state.lock().await;
        let applied_lsn = committed.state.applied_lsn;
        let resident_runs = committed.state.runs.len();
        let resident_waiting_nodes = committed
            .state
            .runs
            .values()
            .flat_map(|r| r.nodes.values())
            .filter(|n| n.status == flow_engine::journal_state::NodeStatus::Waiting)
            .count();
        let resident_definition_versions: usize = committed
            .state
            .workflows
            .values()
            .map(|w| w.versions.len())
            .sum();
        let resident_attempts: usize = committed
            .state
            .runs
            .values()
            .flat_map(|r| r.nodes.values())
            .map(|n| n.attempts.len())
            .sum();
        let cached_publications = committed.state.values.cached_publications();
        let pending = committed.pending;
        let dirty_count = committed.dirty_runs.len();
        let dirty: Vec<String> = committed.dirty_runs.iter().take(256).cloned().collect();
        drop(committed);
        let stats = self.journal.stats();
        serde_json::json!({
            "applied_lsn": applied_lsn,
            "resident_definition_versions": resident_definition_versions,
            "resident_waiting_nodes": resident_waiting_nodes,
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

/// 审计事件的索引键：run + node + dispatch + audit_seq。
///
/// 四个字段缺一不可——同一节点的不同尝试必须落到不同的审计行上，
/// 否则后一次尝试的证据会覆盖前一次。
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

#[cfg(test)]
mod tests;

/// 审计事件投影出的永久回执行：只保存内容摘要与 LSN，不复制累积历史。
pub(crate) fn audit_rows(tx: &Transaction) -> flow_journal::Result<Vec<EntityChange>> {
    tx.events.iter().filter(|event| event.audit_seq > 0).map(|event| {
        Ok(EntityChange { kind: flow_journal::sqlite::EntityKind::Audit, key: audit_key(event)?, value: Some(serde_json::json!({
            "digest": flow_journal::codec::digest(&flow_journal::codec::bounded_json(event, flow_journal::MAX_LINE_BYTES)?),
            "lsn": tx.lsn.to_string(),
        })) })
    }).collect()
}

/// run 的当前行内容（产品面读的是摘要，不是完整执行历史）。
fn run_row(run: &flow_engine::journal_state::Run) -> flow_journal::Result<Value> {
    Ok(serde_json::to_value(run.summary())?)
}

/// 把一个事务涉及的事件翻译成要写入 `current_entities` 的行。
///
/// 只写「受影响实体」而不是全量重写：事务行必须与事实一一对应，
/// 否则投影检查（`projection_check`）就无法证明当前行可由事实重放得到。
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
                    // run 启动时一次性铺开全部节点行，之后的节点事件只需
                    // 更新自己那一行。
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
                            // 版本号补零到 20 位：索引键按字典序排序，
                            // 补零后字典序即数值序。
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
            // 每行的值都从「已应用本次事务后的 state」现取，保证写入的
            // 当前行与事实严格对应，而不是用事件载荷反推。
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
                _ => return Err(flow_journal::Error::Invalid(format!("unknown projected entity kind: {kind}"))),
            };
            Ok(EntityChange {
                kind: flow_journal::sqlite::EntityKind::parse(kind)?,
                key,
                value,
            })
        })
        .collect()
}
