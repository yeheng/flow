//! V2 coordinator. Persistent waits occupy no active execution task.
//! Node execution semantics live only in the executor: every platform drives
//! them through the subprocess pool (unix socketpair, others loopback dialback)
//! or remote agents; the coordinator never runs node logic in-process.
use crate::execution::remote::{RemoteDispatcher, RemoteOptions};
use crate::execution::{ExecutionMode, IpcDispatcher};
use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::node_event;
use crate::journal_execution::Attempt;
use flow_engine::journal_state::{Node as NodeState, NodeStatus, Run, RunStatus, Wait, WaitKind};
use flow_engine::{Definition, NodeLogger, NodeType};
use flow_journal::{Event, EventKind, StoredValue};
use futures::FutureExt;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

/// 节点执行端口：本地 IPC 子进程池 / 远程 agent。
#[derive(Clone)]
pub(crate) enum ExecutionPort {
    Ipc(Arc<IpcDispatcher>),
    Remote(Arc<RemoteDispatcher>),
}

/// 一个在飞节点尝试的归还键与结果。
type NodeKey = (String, String);
type NodeTask = (NodeKey, Result<()>);

/// 协调循环的在飞状态：执行任务集、在飞节点键、持久唤醒表与派发槽位
/// 上限（IPC 的 x_max 既是池容量也是 A_max）。
struct Coordinator {
    tasks: JoinSet<NodeTask>,
    active: HashSet<NodeKey>,
    wakes: BTreeMap<i64, BTreeSet<String>>,
    limit: usize,
}

impl Coordinator {
    fn new(limit: usize) -> Self {
        Self {
            tasks: JoinSet::new(),
            active: HashSet::new(),
            wakes: BTreeMap::new(),
            limit,
        }
    }
}

/// 纯状态推断（reducer）：一个节点在当前快照的入边结论。只读 committed
/// facts，决策与副作用彻底分离——副作用全部落在 journal 事实里。
#[derive(Debug, Clone)]
enum NodeGate {
    /// 前驱未全部终态，本轮不动。
    Blocked,
    /// 分支不可达或上游失败/跳过：写 NodeSkipped（携带原因）。
    Skip(&'static str),
    /// 可派发，携带已接受的前驱输出。
    Ready(BTreeMap<String, StoredValue>),
}

/// 入边判定所需的最小前驱视图：状态 / 分支结论 / 输出声明。
/// （不带 attempts 账本——那是结果屏障的账，与汇合判定无关。）
#[derive(Debug, Clone)]
struct GateNode {
    status: NodeStatus,
    branch: Option<bool>,
    output: Option<StoredValue>,
}

type Result<T> = std::result::Result<T, JournalError>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

impl JournalBackend {
    /// 二期 IPC 模式：先持久提交新 master_epoch，再创建进程池与派发器
    /// （取得独占写权后持久提交新任期，再派发任务；二期 §3.2）。
    /// 全平台：unix 走 socketpair + argv 传通道 FD，其余平台走 loopback
    /// 回连（承载差异在 pool 内部收口）。
    pub async fn start_execution_ipc(self: &Arc<Self>, mode: ExecutionMode) -> Result<()> {
        self.start_execution_ipc_prewarm(mode, false).await
    }

    /// 同 [`Self::start_execution_ipc`]，但可在启动时预热一个空闲执行器
    /// （桌面 app 用；来源 `flow_config::ExecutionConfig::prewarm`）。
    /// 预热在协调器任务内、开始调度前执行一次：串行于后续派发，不与
    /// `acquire` 竞争槽位；失败只告警，首次派发仍会懒加载。
    pub async fn start_execution_ipc_prewarm(
        self: &Arc<Self>,
        mode: ExecutionMode,
        prewarm: bool,
    ) -> Result<()> {
        // ExecutionMode 只有 Ipc 一个变体（进程内路径已全平台删除）。
        let ExecutionMode::Ipc(options) = mode;
        // I09：启用 IPC 时二进制缺失直接失败，不静默回退进程内执行。
        if !options.executor.program.is_file() {
            return Err(invalid(format!(
                "executor binary missing: {}",
                options.executor.program.display()
            ))
            .into());
        }
        let epoch = self.bump_master_epoch().await?;
        // 协调器的派发窗口与执行槽位是同一个旋钮：IPC 的 x_max 同时是池容量与
        // A_max。16 是协议上限。
        let limit = options.x_max.clamp(1, 16);
        let pool = crate::execution::ExecutorPool::new(options, self.journal.id().into(), epoch);
        let dispatcher = Arc::new(IpcDispatcher::new(pool));
        self.start_execution_with_port(limit, ExecutionPort::Ipc(dispatcher), prewarm)
            .await
    }
    /// 持久提交新任期并返回新 epoch。
    pub(crate) async fn bump_master_epoch(self: &Arc<Self>) -> Result<u64> {
        self.internal(|state| {
            let epoch = state.master_epoch + 1;
            Ok((
                vec![Event::new(
                    EventKind::MasterEpochStarted,
                    json!({"epoch": epoch.to_string()}),
                )],
                epoch,
            ))
        })
        .await
    }
    /// 三期远程模式：epoch + TLS 监听 + 远程派发器（R1）。
    pub async fn start_execution_remote(self: &Arc<Self>, options: RemoteOptions) -> Result<()> {
        let epoch = self.bump_master_epoch().await?;
        let manager = crate::execution::remote::AgentManager::new(
            self.clone(),
            self.journal.id().into(),
            epoch,
        );
        manager.listen_tls(options.clone()).await?;
        // T13：配置了 WSS 时附加启动代理接入监听（直连 mTLS 不变）。
        // 身份由 token→agent_id 映射表决定，凭据表不可留空。
        if let Some(addr) = &options.wss_addr {
            if options.wss_agents.is_empty() {
                manager.shutdown().await;
                return Err(crate::journal::JournalError::Journal(
                    flow_journal::Error::Invalid(
                        "execution.remote.wss_addr 配置了但 agents 凭据表为空".into(),
                    ),
                ));
            }
            if let Err(error) = manager
                .listen_wss(addr.clone(), options.wss_agents.clone())
                .await
            {
                manager.shutdown().await;
                return Err(error.into());
            }
        }
        let dispatcher = Arc::new(RemoteDispatcher::new(manager.clone(), options.clone()));
        let result = self
            .start_execution_with_port(16, ExecutionPort::Remote(dispatcher), false)
            .await;
        if result.is_err() {
            manager.shutdown().await;
        }
        result
    }

    /// 三期远程（测试/in-process 入口）：用既有 manager 启动远程端口。
    pub async fn start_execution_remote_manager(
        self: &Arc<Self>,
        manager: Arc<crate::execution::remote::AgentManager>,
        options: RemoteOptions,
    ) -> Result<()> {
        let dispatcher = Arc::new(RemoteDispatcher::new(manager, options));
        self.start_execution_with_port(16, ExecutionPort::Remote(dispatcher), false)
            .await
    }

    async fn start_execution_with_port(
        self: &Arc<Self>,
        limit: usize,
        port: ExecutionPort,
        prewarm: bool,
    ) -> Result<()> {
        let mut driver = self.execution.lock().await;
        if driver.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return Ok(());
        }
        let observation = self.observations.clone();
        let weak = Arc::downgrade(self);
        let cancel = self.cancellation();
        let notify = self.execution_notification();
        notify.notify_one();
        *driver = Some(tokio::spawn(async move {
            // 预热只在协调器任务启动时执行一次（不随 panic 重启重复）。
            // 本任务独占 `port` 且尚未进入调度循环，故不与任何 acquire 竞争。
            if prewarm {
                if let ExecutionPort::Ipc(dispatcher) = &port {
                    match dispatcher.prewarm(1).await {
                        Ok(warmed) => tracing::info!(warmed, "executor prewarmed"),
                        Err(error) => tracing::warn!(
                            %error,
                            "executor prewarm failed; first dispatch will spawn lazily"
                        ),
                    }
                }
            }
            loop {
                let restarted = AssertUnwindSafe(async {
            let mut coord = Coordinator::new(limit);
            loop {
                let delay = coord
                    .wakes
                    .first_key_value()
                    .map_or(Duration::from_secs(60), |(at, _)| {
                        Duration::from_millis(
                            at.saturating_sub(chrono::Utc::now().timestamp_millis())
                                .max(0) as u64,
                        )
                    });
                let mut finished = None;
                tokio::select! {
                    _=cancel.cancelled()=>{coord.tasks.abort_all();while coord.tasks.join_next().await.is_some(){}return},
                    completed=coord.tasks.join_next(),if !coord.tasks.is_empty()=>{
                        match completed {
                            Some(Ok((key,result)))=>{coord.active.remove(&key);finished=Some(key.0);if let Err(error)=result {tracing::error!(%error,"v2 node task stopped; committed evidence retained");}},
                            Some(Err(error))=>{tracing::error!(%error,"v2 task exited unexpectedly; rebuilding coordinator");coord.tasks.abort_all();while coord.tasks.join_next().await.is_some(){}return},
                            None=>{}
                        }
                    },
                    _=notify.notified()=>{},
                    _=tokio::time::sleep(delay)=>{}
                }
                let Some(backend) = weak.upgrade() else {
                    return;
                };
                backend
                    .coordinator_ticks
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                backend.coordinator_last_tick_ms.store(
                    chrono::Utc::now().timestamp_millis(),
                    std::sync::atomic::Ordering::Relaxed,
                );
                if let Some(failure) = backend.journal.stats().failure {
                    // journal 写入永久故障：协调循环持续空转，不吭声会让停摆
                    // 看起来像挂死。限频报错（每 2s 至多一条）。
                    static FAILURE_LOGGED_AT: std::sync::atomic::AtomicI64 =
                        std::sync::atomic::AtomicI64::new(0);
                    let now = chrono::Utc::now().timestamp_millis();
                    let last = FAILURE_LOGGED_AT.load(std::sync::atomic::Ordering::Relaxed);
                    if now.saturating_sub(last) > 2000 {
                        FAILURE_LOGGED_AT.store(now, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!(%failure, "coordinator stalled: journal writer failed");
                    }
                    continue;
                }
                if let Some(run) = finished {
                    backend.defer_run(run).await;
                }
                let now = chrono::Utc::now().timestamp_millis();
                while coord.wakes.first_key_value().is_some_and(|(at, _)| *at <= now) {
                    let (_, runs) = coord.wakes.pop_first().unwrap();
                    for run in runs {
                        backend.defer_run(run).await;
                    }
                }
                let mut runs = backend.take_dirty_runs().await.into_iter();
                while let Some(run_id) = runs.next() {
                    if coord.tasks.len() >= coord.limit {
                        backend
                            .defer_runs(std::iter::once(run_id).chain(runs).collect())
                            .await;
                        break;
                    }
                    let result = AssertUnwindSafe(advance_run(
                        &backend,
                        &port,
                        &observation,
                        &run_id,
                        &mut coord,
                    ))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(invalid("coordinator panicked while advancing run").into())
                    });
                    if let Err(error) = result {
                        tracing::error!(%run_id,%error,"v2 coordinator will retry from committed evidence");
                        // Retry reads/coordination with a delay; new events may also wake the run.
                        coord.wakes.entry(chrono::Utc::now().timestamp_millis().saturating_add(1000))
                            .or_default().insert(run_id);
                    }
                }
            }
            }).catch_unwind().await;
                if cancel.is_cancelled() {
                    return;
                }
                let Some(backend) = weak.upgrade() else {
                    return;
                };
                tracing::error!(
                    panicked = restarted.is_err(),
                    "restarting v2 coordinator from committed state"
                );
                let pending = backend
                    .inspect(|state| {
                        state
                            .runs
                            .values()
                            .filter(|run| !run.terminal())
                            .map(|run| run.run_id.clone())
                            .collect()
                    })
                    .await;
                backend.defer_runs(pending).await;
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                notify.notify_one();
            }
        }));
        Ok(())
    }
}

/// Drive one dirty run as far as committed facts allow: parent-cancel
/// propagation, waiting nodes, ready gates, task spawn and terminal
/// settlement. Every mutation lands as journal facts; transient errors
/// surface to the coordinator loop for a delayed retry.
async fn advance_run(
    backend: &Arc<JournalBackend>,
    port: &ExecutionPort,
    observation: &Option<flow_engine::observation::ObservationStore>,
    run_id: &str,
    coord: &mut Coordinator,
) -> Result<()> {
    let Some(run) = backend.inspect(|s| s.runs.get(run_id).cloned()).await else {
        backend.evict_definition(run_id).await;
        return Ok(());
    };
    if run.terminal() {
        // 终态 run 的定义缓存条目随之逐出（定义不可变，重进即重物化）。
        backend.evict_definition(run_id).await;
        return Ok(());
    }
    if cancel_if_parent_cancelled(backend, &run).await? {
        return Ok(());
    }
    let definition = backend
        .cached_definition(&run.run_id, &run.definition)
        .await?;
    for node in &definition.nodes {
        if coord.active.contains(&(run_id.to_owned(), node.id.clone())) {
            continue;
        }
        // 循环内会写事件（NodeSkipped/DispatchStarted），状态读取必须保持
        // 每节点新鲜；但只取推进所需的最小视图（终态位 + 本节点记录 + 入边
        // 前驱的 gate 三元组），不再整克隆 Run（definition/输入/attempts
        // 账本与单节点推进无关）。
        let (terminal, record, gate_view) = backend
            .inspect(|s| {
                let run = s.runs.get(run_id)?;
                let view: BTreeMap<String, GateNode> = definition
                    .incoming(&node.id)
                    .into_iter()
                    .filter_map(|edge| {
                        run.nodes.get(&edge.from).map(|from| {
                            (
                                edge.from.clone(),
                                GateNode {
                                    status: from.status,
                                    branch: from.branch,
                                    output: from.output.clone(),
                                },
                            )
                        })
                    })
                    .collect();
                Some((run.terminal(), run.nodes.get(&node.id).cloned(), view))
            })
            .await
            .ok_or_else(|| invalid("run missing"))?;
        if terminal {
            break;
        }
        if let Some(record) = record {
            if advance_committed_node(backend, run_id, node, &record, &mut coord.wakes).await? {
                continue;
            }
        }
        match incoming_gate(&definition, &gate_view, node) {
            NodeGate::Skip(reason) => {
                backend
                    .internal_for_run(run_id, |s| {
                        Ok((
                            vec![node_event(
                                &s.runs[run_id],
                                &node.id,
                                EventKind::NodeSkipped,
                                json!({"reason": reason}),
                                true,
                            )],
                            (),
                        ))
                    })
                    .await?;
            }
            NodeGate::Blocked => {}
            NodeGate::Ready(predecessors) => {
                if coord.tasks.len() >= coord.limit {
                    backend.defer_run(run_id.to_owned()).await;
                    continue;
                }
                spawn_node_attempt(
                    backend,
                    port,
                    observation,
                    run_id,
                    node,
                    predecessors,
                    coord,
                )
                .await?;
            }
        }
    }
    settle_run(backend, run_id, &definition).await
}

/// A cancelled parent durably drives bounded one-child cancellation, so
/// restart resumes propagation without a giant transaction.
async fn cancel_if_parent_cancelled(backend: &Arc<JournalBackend>, run: &Run) -> Result<bool> {
    if let Some(parent) = &run.parent {
        let cancelled = backend.run_status(&parent.run_id).await? == Some(RunStatus::Cancelled);
        if cancelled {
            backend
                .run_cancel(&run.run_id, Some("parent-cancel"))
                .await?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Advance a node that already has committed facts. Returns true when this
/// pass fully handled the node (still waiting, terminal, or uncertain-op
/// bookkeeping); false means gate evaluation may proceed.
async fn advance_committed_node(
    backend: &Arc<JournalBackend>,
    run_id: &str,
    node: &flow_engine::Node,
    record: &NodeState,
    wakes: &mut BTreeMap<i64, BTreeSet<String>>,
) -> Result<bool> {
    let now = chrono::Utc::now().timestamp_millis();
    if record.status == NodeStatus::Waiting {
        let wait = record
            .wait
            .as_ref()
            .ok_or_else(|| invalid("waiting node missing wait"))?;
        if let Wait::Child { child_run_id } = wait {
            let child = backend
                .run_snapshot(child_run_id)
                .await?
                .ok_or_else(|| invalid("waiting child missing"))?;
            if child.terminal() {
                backend.internal_for_run(run_id, |s| {
                    let parent = &s.runs[run_id];
                    let current = &parent.nodes[&node.id];
                    if parent.terminal() || current.wait.as_ref().and_then(Wait::child_run_id) != Some(child_run_id) {
                        return Ok((vec![], ()));
                    }
                    let (kind, payload) = if child.status == RunStatus::Succeeded {
                        (EventKind::WaitResolved, json!({"output":child.output.clone().unwrap_or(StoredValue::Inline(Value::Null))}))
                    } else {
                        (EventKind::NodeFailed, json!({"error":child.error.clone().unwrap_or_else(||format!("child run {}",child.status)),"sealed_through":current.attempts[&current.dispatch_id].audit_seq.to_string()}))
                    };
                    Ok((vec![node_event(parent,&node.id,kind,payload,true)], ()))
                }).await?;
            } else {
                wakes
                    .entry(now.saturating_add(1000))
                    .or_default()
                    .insert(run_id.to_owned());
            }
            return Ok(true);
        }
        if wait.kind() == WaitKind::Delay && wait.wake_at().is_some_and(|at| at <= now) {
            backend
                .internal_for_run(run_id, |s| {
                    let r = &s.runs[run_id];
                    Ok((vec![node_event(
                        r,
                        &node.id,
                        EventKind::WaitResolved,
                        json!({"output": wait.output().cloned().unwrap_or(StoredValue::Inline(Value::Null))}),
                        true,
                    )], ()))
                })
                .await?;
        }
        if matches!(wait.kind(), WaitKind::Delay | WaitKind::Retry) {
            if let Some(at) = wait.wake_at().filter(|at| *at > now) {
                wakes.entry(at).or_default().insert(run_id.to_owned());
            }
        }
        if wait.kind() != WaitKind::Retry || wait.wake_at().is_none_or(|at| at > now) {
            return Ok(true);
        }
    }
    if matches!(record.status, NodeStatus::Succeeded | NodeStatus::Skipped) {
        return Ok(true);
    }
    // v1 语义（DESIGN.md §9.2，engine_recovery 钉死）：节点失败不立即终结
    // run——独立分支继续跑完，失败分支的下游被跳过（reason=upstream_failed），
    // 全部节点终态后由收尾判定写 RunFailed。
    if record.status == NodeStatus::Failed {
        return Ok(true);
    }
    if record
        .operation
        .as_ref()
        .is_some_and(|op| op.outcome.is_none())
    {
        let attempt = Attempt {
            backend: backend.clone(),
            run_id: run_id.to_owned(),
            node_id: node.id.clone(),
            dispatch_id: record.dispatch_id.clone(),
        };
        attempt
            .finish(
                EventKind::WaitRegistered,
                json!({"wait": Wait::Uncertain,"integrity":"unknown"}),
            )
            .await?;
        return Ok(true);
    }
    Ok(false)
}

/// 纯函数：入边在当前快照下的推进结论（无 IO、无副作用）。
/// 入参是入边前驱的最小视图（[`GateNode`]），不是整份 Run——本函数只读
/// 汇合判定所需的状态/分支/输出三件事。
fn incoming_gate(
    definition: &Definition,
    view: &BTreeMap<String, GateNode>,
    node: &flow_engine::Node,
) -> NodeGate {
    let mut ready = true;
    let mut skip = false;
    let mut predecessors = BTreeMap::new();
    let mut skip_reason = "";
    for edge in definition.incoming(&node.id) {
        match view.get(&edge.from) {
            Some(n) if n.status == NodeStatus::Skipped => {
                skip = true;
                ready = false;
                if skip_reason.is_empty() {
                    skip_reason = "upstream_skipped";
                }
            }
            Some(n) if n.status == NodeStatus::Failed => {
                skip = true;
                ready = false;
                if skip_reason.is_empty() {
                    skip_reason = "upstream_failed";
                }
            }
            Some(n) if n.status == NodeStatus::Succeeded => {
                let accepted = edge
                    .port
                    .as_deref()
                    .is_none_or(|port| n.branch == Some(port == "true"));
                if !accepted {
                    skip = true;
                    ready = false;
                    if skip_reason.is_empty() {
                        skip_reason = "branch_not_taken";
                    }
                } else if let Some(output) = &n.output {
                    predecessors.insert(edge.from.clone(), output.clone());
                }
            }
            _ => ready = false,
        }
    }
    if skip {
        NodeGate::Skip(skip_reason)
    } else if !ready {
        NodeGate::Blocked
    } else {
        NodeGate::Ready(predecessors)
    }
}

/// Reserve one dispatch slot and spawn the node attempt; in-flight
/// bookkeeping (active key, peak gauge) happens before the task ever runs.
async fn spawn_node_attempt(
    backend: &Arc<JournalBackend>,
    port: &ExecutionPort,
    observation: &Option<flow_engine::observation::ObservationStore>,
    run_id: &str,
    node: &flow_engine::Node,
    predecessors: BTreeMap<String, StoredValue>,
    coord: &mut Coordinator,
) -> Result<()> {
    let attempt = Attempt::begin(backend.clone(), run_id.to_owned(), node.id.clone()).await?;
    let key: NodeKey = (run_id.to_owned(), node.id.clone());
    coord.active.insert(key.clone());
    backend
        .execution_peak
        .fetch_max(coord.tasks.len() + 1, std::sync::atomic::Ordering::Relaxed);
    let node = node.clone();
    let observation = observation.clone();
    let port = port.clone();
    coord
        .tasks
        .spawn(async move { execute_node(port, attempt, node, predecessors, observation).await });
    Ok(())
}

/// One node attempt through the execution port. Failures and panics are
/// converted into committed evidence: uncertain operations wait for
/// adjudication, prepare-stage script/condition failures schedule a retry
/// wake, everything else lands as NodeFailed.
async fn execute_node(
    port: ExecutionPort,
    attempt: Attempt,
    node: flow_engine::Node,
    predecessors: BTreeMap<String, StoredValue>,
    observation: Option<flow_engine::observation::ObservationStore>,
) -> NodeTask {
    let key: NodeKey = (attempt.run_id.clone(), attempt.node_id.clone());
    let result = AssertUnwindSafe(async {
        match &port {
            ExecutionPort::Ipc(dispatcher) => {
                dispatcher
                    .execute(&attempt, &node, predecessors.clone(), observation.clone())
                    .await
            }
            ExecutionPort::Remote(dispatcher) => {
                dispatcher
                    .execute(&attempt, &node, predecessors.clone(), observation.clone())
                    .await
            }
        }
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| {
        Err(invalid("node execution panicked; committed operation evidence retained").into())
    });
    if let Err(error) = &result {
        if let Ok(snapshot) = attempt.snapshot().await {
            let Some(n) = snapshot.nodes.get(&attempt.node_id) else {
                return (key, Err(invalid("dispatch node missing").into()));
            };
            let uncertain = n.operation.as_ref().is_some_and(|o| o.outcome.is_none());
            let mut payload = if uncertain {
                json!({"wait": Wait::Uncertain, "integrity":"unknown"})
            } else {
                json!({"error": error.to_string(), "preparation_failed": n.prepared.is_none(), "original_input": snapshot.input, "predecessors": predecessors, "integrity":"incomplete"})
            };
            let policy = node.retry();
            if !uncertain
                && n.operation.is_none()
                && n.prepared.is_some()
                && matches!(node.kind(), Some(NodeType::Script | NodeType::Condition))
                && n.attempt < policy.max_attempts.min(100)
            {
                let delay = policy.backoff_ms.min(i64::MAX as u64) as i64;
                payload["retry_wake_at"] =
                    json!(chrono::Utc::now().timestamp_millis().saturating_add(delay));
                if let Some(store) = &observation {
                    NodeLogger::observation(
                        store.logger(attempt.run_id.clone(), attempt.dispatch_id.clone()),
                        attempt.node_id.clone(),
                        n.attempt,
                    )
                    .warn(format!(
                        "第 {} 次尝试失败，{}ms 后自动重试",
                        n.attempt, delay
                    ));
                }
            }
            let _ = attempt
                .finish(
                    if uncertain {
                        EventKind::WaitRegistered
                    } else {
                        EventKind::NodeFailed
                    },
                    payload,
                )
                .await;
        }
    }
    (key, result)
}

/// v1 收尾判定（§6.6）：全部节点终态后收口 run——任一 failed → RunFailed
/// （错误取定义序第一个 failed 节点）；否则全部 end 计入输出（无输出/
/// skipped 的 end 记 null，不再丢弃——多 end 时形状恒为 map，与 v1
/// singular_or_map 一致）。
async fn settle_run(
    backend: &Arc<JournalBackend>,
    run_id: &str,
    definition: &Definition,
) -> Result<()> {
    let Some(snapshot) = backend.inspect(|s| s.runs.get(run_id).cloned()).await else {
        return Ok(());
    };
    if snapshot.terminal()
        || !definition.nodes.iter().all(|n| {
            snapshot.nodes.get(&n.id).is_some_and(|n| {
                matches!(
                    n.status,
                    NodeStatus::Succeeded | NodeStatus::Skipped | NodeStatus::Failed
                )
            })
        })
    {
        return Ok(());
    }
    let failed = definition.nodes.iter().find(|n| {
        snapshot
            .nodes
            .get(&n.id)
            .is_some_and(|r| r.status == NodeStatus::Failed)
    });
    match failed {
        Some(failed_node) => {
            let error = snapshot.nodes[&failed_node.id].error.clone();
            finish_run(backend, run_id, None, error).await?;
        }
        None => {
            let ends = definition
                .nodes
                .iter()
                .filter(|n| n.kind() == Some(NodeType::End))
                .map(|n| {
                    (
                        n.id.clone(),
                        snapshot
                            .nodes
                            .get(&n.id)
                            .and_then(|r| r.output.clone())
                            .unwrap_or(StoredValue::Inline(Value::Null)),
                    )
                })
                .collect();
            let output = join_values(backend, ends).await?;
            finish_run(backend, run_id, Some(output), None).await?;
        }
    }
    Ok(())
}

async fn join_values(
    backend: &JournalBackend,
    mut values: BTreeMap<String, StoredValue>,
) -> Result<StoredValue> {
    if values.len() == 1 {
        return Ok(values.pop_first().unwrap().1);
    }
    if values.is_empty() {
        return Ok(StoredValue::Inline(Value::Null));
    }
    let (mut reader, producer) = flow_journal::value::compose_object(
        backend.journal.root().into(),
        backend.journal.durable_lsn(),
        values.into_iter().collect(),
    );
    let result = backend
        .store_stream(&mut reader, flow_journal::value::ValueCodec::Json)
        .await;
    drop(reader);
    let production = producer.await.map_err(|e| invalid(e.to_string()))?;
    let value = result?;
    production?;
    Ok(StoredValue::Ref(value))
}

async fn finish_run(
    backend: &JournalBackend,
    run_id: &str,
    output: Option<StoredValue>,
    error: Option<String>,
) -> Result<()> {
    backend.internal_for_run(run_id, |s|{
        let run=&s.runs[run_id];if run.terminal(){return Ok((vec![],()))}
        let mut event=Event::new(if error.is_some(){EventKind::RunFailed}else{EventKind::RunCompleted},json!({"output":output,"error":error}));event.run_id=Some(run_id.into());event.run_seq=run.last_run_seq+1;
        let mut events=vec![event];
        if let Some(parent)=&run.parent {
            if let Some(parent_run)=s.runs.get(&parent.run_id).filter(|r| !r.terminal()) {
                let node=&parent_run.nodes[&parent.node_id];
                let payload=if error.is_some(){json!({"error":error,"sealed_through":node.attempts[&node.dispatch_id].audit_seq.to_string()})}else{json!({"output":output})};
                events.push(node_event(parent_run,&parent.node_id,if error.is_some(){EventKind::NodeFailed}else{EventKind::WaitResolved},payload,true));
            }
        }
        Ok((events,()))
    }).await
}

#[cfg(test)]
mod gate_tests {
    use super::*;

    fn definition(edges: Value) -> Definition {
        serde_json::from_value(json!({
            "nodes": [
                {"id":"s","type":"start"},
                {"id":"a","type":"script"},
                {"id":"b","type":"script"},
                {"id":"x","type":"script"},
                {"id":"y","type":"script"},
                {"id":"e","type":"end"}
            ],
            "edges": edges
        }))
        .unwrap()
    }

    fn node<'a>(id: &str, def: &'a Definition) -> &'a flow_engine::Node {
        def.nodes.iter().find(|n| n.id == id).unwrap()
    }

    /// (id, status, branch, output)——output 为业务值，此处包装成 inline StoredValue。
    fn view(nodes: &[(&str, &str, Option<bool>, Option<Value>)]) -> BTreeMap<String, GateNode> {
        nodes
            .iter()
            .map(|(id, status, branch, output)| {
                let output = output
                    .as_ref()
                    .map(|value| StoredValue::inline(value.clone()).unwrap());
                (
                    (*id).to_string(),
                    GateNode {
                        status: serde_json::from_value::<NodeStatus>(json!(status)).unwrap(),
                        branch: *branch,
                        output,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn gate_blocks_until_all_predecessors_settle() {
        let def = definition(json!([
            {"from":"s","to":"a"},{"from":"s","to":"b"},
            {"from":"a","to":"e"},{"from":"b","to":"e"}
        ]));
        let settled = view(&[
            ("s", "succeeded", None, None),
            ("a", "succeeded", None, Some(json!(1))),
        ]);
        assert!(matches!(
            incoming_gate(&def, &settled, node("e", &def)),
            NodeGate::Blocked
        ));
        // 未注册的入边节点（缺前驱记录）同样阻塞。
        let empty = view(&[]);
        assert!(matches!(
            incoming_gate(&def, &empty, node("a", &def)),
            NodeGate::Blocked
        ));
    }

    #[test]
    fn gate_collects_accepted_predecessor_outputs() {
        let def = definition(json!([
            {"from":"s","to":"a"},{"from":"s","to":"b"},
            {"from":"a","to":"e"},{"from":"b","to":"e"}
        ]));
        let view = view(&[
            ("s", "succeeded", None, None),
            ("a", "succeeded", None, Some(json!({"a":1}))),
            ("b", "succeeded", None, Some(json!(2))),
        ]);
        let NodeGate::Ready(predecessors) = incoming_gate(&def, &view, node("e", &def)) else {
            panic!("expected ready");
        };
        assert_eq!(
            predecessors,
            BTreeMap::from([
                ("a".to_string(), StoredValue::Inline(json!({"a":1}))),
                ("b".to_string(), StoredValue::Inline(json!(2))),
            ])
        );
    }

    #[test]
    fn gate_skips_downstream_of_failed_or_skipped() {
        let def = definition(json!([
            {"from":"s","to":"a"},{"from":"s","to":"b"},
            {"from":"a","to":"e"},{"from":"b","to":"e"}
        ]));
        let failed = view(&[
            ("s", "succeeded", None, None),
            ("a", "failed", None, None),
            ("b", "skipped", None, None),
        ]);
        assert!(matches!(
            incoming_gate(&def, &failed, node("e", &def)),
            NodeGate::Skip("upstream_failed")
        ));
        let view = view(&[
            ("s", "succeeded", None, None),
            ("a", "skipped", None, None),
            ("b", "skipped", None, None),
        ]);
        assert!(matches!(
            incoming_gate(&def, &view, node("e", &def)),
            NodeGate::Skip("upstream_skipped")
        ));
    }

    #[test]
    fn gate_honors_condition_branch_ports() {
        let def = definition(json!([
            {"from":"a","to":"x","port":"true"},
            {"from":"a","to":"y","port":"false"}
        ]));
        let view = view(&[("a", "succeeded", Some(false), Some(json!(1)))]);
        assert!(matches!(
            incoming_gate(&def, &view, node("x", &def)),
            NodeGate::Skip("branch_not_taken")
        ));
        let NodeGate::Ready(predecessors) = incoming_gate(&def, &view, node("y", &def)) else {
            panic!("expected ready");
        };
        assert_eq!(predecessors["a"], StoredValue::Inline(json!(1)));
        // 无端口的边不受分支影响。
        let plain = definition(json!([{"from":"a","to":"x"}]));
        let NodeGate::Ready(_) = incoming_gate(&plain, &view, node("x", &plain)) else {
            panic!("expected ready");
        };
    }

    #[test]
    fn node_without_incoming_edges_is_ready() {
        let def = definition(json!([{"from":"s","to":"a"}]));
        let view = view(&[]);
        assert!(matches!(
            incoming_gate(&def, &view, node("s", &def)),
            NodeGate::Ready(_)
        ));
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    /// T7：本地执行只有 IPC（进程内路径已全平台删除）。单测显式指向工作区
    /// 产出的真实执行器（`cargo build --workspace --bins` 先产出）。
    async fn start_local(backend: &Arc<JournalBackend>, x_max: usize) {
        backend
            .start_execution_ipc(crate::execution::ExecutionMode::Ipc(
                crate::execution::IpcOptions {
                    executor:
                        flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                            flow_test_support::io::executor_bin(),
                        ),
                    x_max,
                    tag: None,
                },
            ))
            .await
            .unwrap();
    }
    async fn install(backend: &JournalBackend, definition: Value) -> String {
        let created = backend
            .workflow_create("driver-regression", None)
            .await
            .unwrap();
        let id = created.result["workflow_id"].as_str().unwrap();
        backend.workflow_update(id, definition, None).await.unwrap();
        backend.workflow_publish(id, 1, None).await.unwrap();
        let started = backend
            .run_start(id, None, json!(null), "manual", None, None)
            .await
            .unwrap();
        started.result["run_id"].as_str().unwrap().into()
    }
    async fn terminal(backend: &JournalBackend, id: &str) -> Run {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let run = backend.state().await.runs[id].clone();
                if run.terminal() {
                    return run;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    /// 观测管道回归：生命周期行 + console 行都到齐，且归属正确的节点。
    /// （跨通道乱序的尾批归属由 dispatch::resolve_dispatch 兜底，此处
    /// 钉住端到端可见性。）
    #[tokio::test]
    async fn observations_attributed_to_node_with_lifecycle_lines() {
        let root = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(root.path(), Default::default())
            .await
            .unwrap();
        let id = install(
            &backend,
            json!({
                "nodes":[{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"for (let i=0;i<5;i++) { console.log('line', i); }\nreturn 1;"}},{"id":"e","type":"end"}],
                "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}],
            }),
        )
        .await;
        start_local(&backend, 16).await;
        let run = terminal(&backend, &id).await;
        assert_eq!(run.status, RunStatus::Succeeded, "run={run:?}");
        let store = backend.observations.as_ref().expect("observation store");
        store.flush();
        let records = store.page(&id, None, 0, 256).unwrap();
        let attributed: Vec<String> = records
            .iter()
            .map(|r| format!("{}|{}", r.line.node_id, r.line.message))
            .collect();
        assert!(
            attributed.iter().any(|m| m.starts_with("n|开始执行：脚本")),
            "缺少归属 n 的开始行：{attributed:?}"
        );
        assert!(
            attributed.iter().any(|m| m.starts_with("n|执行完成（")),
            "缺少归属 n 的完成行：{attributed:?}"
        );
        for i in 0..5 {
            assert!(
                attributed.iter().any(|m| m == &format!("n|line {i}")),
                "缺少归属 n 的 console 行 line {i}：{attributed:?}"
            );
        }
        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn transient_materialize_error_retries_without_another_command() {
        let root = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(root.path(), Default::default())
            .await
            .unwrap();
        // The definition is a BLOB reference while each executable node stays small.
        // Temporarily corrupt only its bytes: current-state queries and writer admission
        // remain available, but materialization must fail and later retry without a command.
        let id = install(
            &backend,
            json!({
                "nodes":[{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"return 1;"}},{"id":"e","type":"end"}],
                "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}],
                "_padding": "x".repeat(128 * 1024),
            }),
        )
        .await;
        use sqlx::Row as _;
        let pool = sqlx::SqlitePool::connect(&format!(
            "sqlite://{}",
            root.path().join("flow.sqlite").display()
        ))
        .await
        .unwrap();
        let row = sqlx::query(
            "SELECT output_id,chunk_index,bytes FROM value_chunks ORDER BY lsn LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let output: String = row.get("output_id");
        let chunk: i64 = row.get("chunk_index");
        let bytes: Vec<u8> = row.get("bytes");
        sqlx::query("UPDATE value_chunks SET bytes=zeroblob(length(bytes)) WHERE output_id=? AND chunk_index=?").bind(&output).bind(chunk).execute(&pool).await.unwrap();
        start_local(&backend, 16).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            backend.state().await.runs[&id].nodes.is_empty(),
            "unavailable-definition nodes: {:?}",
            backend.state().await.runs[&id].nodes
        );
        sqlx::query("UPDATE value_chunks SET bytes=? WHERE output_id=? AND chunk_index=?")
            .bind(bytes)
            .bind(output)
            .bind(chunk)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let run = terminal(&backend, &id).await;
        assert_eq!(run.status, RunStatus::Succeeded, "run={run:?}");
        backend.close().await.unwrap();
    }
}
