//! V2 coordinator. Persistent waits occupy no active execution task.
//! Node execution semantics live only in the executor: every platform drives
//! them through the subprocess pool (unix socketpair, others loopback dialback)
//! or remote agents; the coordinator never runs node logic in-process.
//!
//! 本模块是协调循环：把「已提交事实」推进成「下一步动作」，并把每一次
//! 推进写成新的 journal 事实。它守护的核心不变量是「没有事实就没有决策」
//! 与「不知道就不动」——推进只依据已提交的当前状态，所有副作用都落在
//! 事实上，节点语义则一律交给执行器子进程（C2）。
//! 需要它，是因为外部副作用无法纳入本地事务：只有把意图、授权与结果
//! 分成三个可裁决的阶段（C3/C4），崩溃后的未知结果才有一个明确的出口。
use crate::execution::remote::{RemoteDispatcher, RemoteOptions};
use crate::execution::{ExecutionMode, IpcDispatcher};
use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::node_event;
use crate::journal_commands::runtime_mismatched;
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
    agents: HashSet<NodeKey>,
    wakes: BTreeMap<i64, BTreeSet<String>>,
    limit: usize,
}

impl Coordinator {
    /// 新建协调器状态（每次循环重启都会重置——重启后从已提交事实重建）。
    fn new(limit: usize) -> Self {
        Self {
            tasks: JoinSet::new(),
            active: HashSet::new(),
            agents: HashSet::new(),
            wakes: BTreeMap::new(),
            limit,
        }
    }

    /// 是否还有派发槽位。
    ///
    /// agent 节点另设子上限（上限的一半）：agent 一次占满一个执行器且
    /// 生命周期长，宽松地排满会让普通节点完全排不上队。
    fn has_capacity(&self, node: &flow_engine::Node) -> bool {
        self.tasks.len() < self.limit
            && (!node.is_harness_agent() || self.agents.len() < self.limit.div_ceil(2))
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
    inactive: bool,
}

type Result<T> = std::result::Result<T, JournalError>;

/// 统一错误构造（参数/合同类问题）。
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
    ///
    /// 任期号是执行器的「写权凭证」：新任期提交后，旧执行器即使还活着
    /// 也会因为任期不符而被拒绝提交结果。
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
                // 配了接入地址却没有凭据，等于开了一个谁都能连的口子。
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
            // 启动失败时必须关掉已监听的端口，否则会留下一个没有协调器
            // 的空壳监听。
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

    /// 启动协调循环任务（唯一持有执行端口的任务）。
    ///
    /// 端口归这一个任务所有：它负责派发、回收与关闭，别的任务不碰
    /// 执行器，避免两个任务同时 acquire 造成槽位超发。
    async fn start_execution_with_port(
        self: &Arc<Self>,
        limit: usize,
        port: ExecutionPort,
        prewarm: bool,
    ) -> Result<()> {
        let mut driver = self.execution.lock().await;
        if driver.as_ref().is_some_and(|handle| !handle.is_finished()) {
            // 已有在跑的协调器：直接复用，不重复起第二个派发者。
            return Ok(());
        }
        let observation = self.observations.clone();
        let weak = Arc::downgrade(self);
        let cancel = self.cancellation();
        let notify = self.execution_notification();
        notify.notify_one();
        *driver = Some(tokio::spawn(async move {
            let outcome = AssertUnwindSafe(async {
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
                // 睡眠时长由最近的持久唤醒时刻决定：没有等待时退到 60s，
                // 避免空转烧 CPU；有到期唤醒时精确等到那一刻。
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
                            Some(Ok((key,result)))=>{coord.active.remove(&key);coord.agents.remove(&key);finished=Some(key.0);if let Err(error)=result {tracing::error!(%error,"v2 node task stopped; failure evidence handling ran inside the attempt");}},
                            // 任务本身崩溃（不是节点失败）：放弃本轮协调器，
                            // 从已提交事实重建——内存里的在飞记账已不可信。
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
                // 到期的持久唤醒：把这些 run 重新放回 dirty 集。
                while coord.wakes.first_key_value().is_some_and(|(at, _)| *at <= now) {
                    let (_, runs) = coord.wakes.pop_first().unwrap();
                    for run in runs {
                        backend.defer_run(run).await;
                    }
                }
                let mut runs = backend.take_dirty_runs().await.into_iter();
                while let Some(run_id) = runs.next() {
                    if coord.tasks.len() >= coord.limit {
                        // 槽位已满：把本批剩余 run 全部退回 dirty 集，
                        // 等下一轮再调度，避免部分推进造成顺序不公。
                        backend
                            .defer_runs(std::iter::once(run_id).chain(runs).collect())
                            .await;
                        break;
                    }
                    // 旧执行器 runtime 的在途 run 永远无法被当前执行器恢复
                    // （执行身份随快照锁死）：跳过——保持非终态原样（不隔离、
                    // 不判死，用户可手动取消），也不调度重试（advance 对它
                    // 必败，重试就是无限空转）。新事件把它带回 dirty 集时
                    // 仍会走到这里同样跳过，代价一次内存读。
                    if backend
                        .inspect(|s| {
                            s.runs.get(&run_id).is_some_and(runtime_mismatched)
                        })
                        .await
                    {
                        continue;
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
                // 重建：把所有在飞 run 重新标为 dirty，从已提交事实重新推导，
                // 而不是沿用崩溃前的内存记账。
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
            }).catch_unwind().await;
            // The driver owns the execution port. Close must await its tracked
            // sessions, including idle/prewarmed children and cancelled starts.
            match &port {
                ExecutionPort::Ipc(dispatcher) => dispatcher.shutdown().await,
                ExecutionPort::Remote(dispatcher) => dispatcher.manager().shutdown().await,
            }
            if outcome.is_err() {
                tracing::error!("coordinator exited unexpectedly; execution port reaped");
            }
        }));
        Ok(())
    }
}

/// Drive one dirty run as far as committed facts allow: parent-cancel
/// propagation, waiting nodes, ready gates, task spawn and terminal
/// settlement. Every mutation lands as journal facts; transient errors
/// surface to the coordinator loop for a delayed retry.
///
/// 推进一个 run 直到「已提交事实允许的尽头」：父级取消传播、等待中的节点、
/// 就绪闸门、任务派发与终态收口。每一次变更都写成 journal 事实；
/// 瞬时错误上抛给协调循环，由它延迟重试。
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
                                    inactive: matches!(
                                        from.skip_reason.as_deref(),
                                        Some("branch_not_taken" | "inactive")
                                    ),
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
                if !coord.has_capacity(node) {
                    // 槽位不足：本轮不派发，把 run 退回 dirty 集等下一轮。
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
///
/// 父 run 被取消时，子 run 的取消必须「每次只推进一个」的粒度落盘，
/// 否则一次级联取消会变成一个巨大的事务。
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
///
/// 处理「已经有事实」的节点：等待、终态、重试授权与未决操作的恢复。
/// 返回 true 表示本轮已完整处理该节点，无需再走闸门判定。
async fn advance_committed_node(
    backend: &Arc<JournalBackend>,
    run_id: &str,
    node: &flow_engine::Node,
    record: &NodeState,
    wakes: &mut BTreeMap<i64, BTreeSet<String>>,
) -> Result<bool> {
    let now = chrono::Utc::now().timestamp_millis();
    // C4：只有显式声明 repeatable 的节点才能拿持久重试授权；尝试次数
    // 受 max_attempts 与 100 次硬上限双重约束。
    if node.params["effect_policy"] == "repeatable"
        && record
            .operation
            .as_ref()
            .is_some_and(|o| o.outcome.is_none())
        && record.attempt < node.retry().max_attempts.min(100)
    {
        let deadline = now.saturating_add(node.retry().backoff_ms.min(i64::MAX as u64) as i64);
        backend.internal_for_run(run_id,|s| {
            let run=&s.runs[run_id]; let current=&run.nodes[&node.id];
            if run.terminal() || current.dispatch_id!=record.dispatch_id {return Ok((vec![],()));}
            let Some(operation)=current.operation.as_ref().filter(|o|o.outcome.is_none()) else{return Ok((vec![],()));};
            Ok((vec![node_event(run,&node.id,EventKind::OperationRetryAuthorized,json!({"operation_id":operation.operation_id,"fingerprint":operation.fingerprint,"policy":"repeatable","wake_at":deadline}),true)],()))
        }).await?;
        return Ok(true);
    }
    if record.status == NodeStatus::Waiting {
        let wait = record
            .wait
            .as_ref()
            .ok_or_else(|| invalid("waiting node missing wait"))?;
        if wait.kind() == WaitKind::Signal {
            // 等待人工信号：只有 inbox 里已有信号才能唤醒，否则本轮无事可做。
            backend
                .internal_for_run(run_id, |s| {
                    let run = &s.runs[run_id];
                    let Some(output) = run.signals.get(&node.id) else {
                        return Ok((vec![], ()));
                    };
                    Ok((
                        vec![node_event(
                            run,
                            &node.id,
                            EventKind::WaitResolved,
                            json!({"output":output}),
                            true,
                        )],
                        (),
                    ))
                })
                .await?;
            return Ok(true);
        }
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
                // 子 run 还没结束：登记一次 1 秒后的唤醒，避免空转轮询。
                wakes
                    .entry(now.saturating_add(1000))
                    .or_default()
                    .insert(run_id.to_owned());
            }
            return Ok(true);
        }
        if wait.kind() == WaitKind::Delay && wait.wake_at().is_some_and(|at| at <= now) {
            // 延时已到：唤醒节点。
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
                // 到期时刻登记进唤醒表：协调循环据此精确等待而不是轮询。
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
    // 重启恢复:在飞派发只留下 operation 事实时,节点没有活动任务
    // (coord.active 为空),两种形态都折进 Uncertain 等待——outcome 未决
    // = 请求可能已发(unknown);outcome 已提交 = 外部结果已被捕获、输出
    // 推导未达(outcome_retained)。不重派发:重派发的新执行器会撞
    // 「operation already authorized」,把平台故障写成 NodeFailed。
    // 迟到的 Result(resume 对账/SubmitExistingResult)仍能正常完成节点。
    if let Some(op) = record.operation.as_ref() {
        let attempt = Attempt {
            backend: backend.clone(),
            run_id: run_id.to_owned(),
            node_id: node.id.clone(),
            dispatch_id: record.dispatch_id.clone(),
        };
        let integrity = if op.outcome.is_none() {
            "unknown"
        } else {
            "outcome_retained"
        };
        attempt
            .finish(
                EventKind::WaitRegistered,
                json!({"wait": Wait::Uncertain,"integrity":integrity}),
            )
            .await?;
        return Ok(true);
    }
    Ok(false)
}

/// 纯函数：入边在当前快照下的推进结论（无 IO、无副作用）。
/// 入参是入边前驱的最小视图（[`GateNode`]），不是整份 Run——本函数只读
/// 汇合判定所需的状态/分支/输出三件事。
///
/// 汇合语义分两套：v2 默认「等全部入边确定」（selected 模式是例外），
/// v1 则要求全部前驱成功。两者不可混用——跳过向下游传播是 v1 的语义。
fn incoming_gate(
    definition: &Definition,
    view: &BTreeMap<String, GateNode>,
    node: &flow_engine::Node,
) -> NodeGate {
    let selected = node.params["join"]
        .as_str()
        .map_or(definition.semantics >= 2, |j| j == "selected");
    if selected {
        // selected 汇合：等全部入边确定，然后只走被选中的成功路径；
        // 未选分支记 inactive，全部入边 inactive 时传播 inactive。
        let mut pending = false;
        let mut failed = false;
        let mut active = 0;
        let mut predecessors = BTreeMap::new();
        for edge in definition.incoming(&node.id) {
            match view.get(&edge.from) {
                Some(n) if n.status == NodeStatus::Skipped && n.inactive => {}
                Some(n) if matches!(n.status, NodeStatus::Failed | NodeStatus::Skipped) => {
                    failed = true
                }
                Some(n) if n.status == NodeStatus::Succeeded => {
                    if edge
                        .port
                        .as_deref()
                        .is_none_or(|p| n.branch == Some(p == "true"))
                    {
                        active += 1;
                        if let Some(output) = &n.output {
                            predecessors.insert(edge.from.clone(), output.clone());
                        }
                    }
                }
                _ => pending = true,
            }
        }
        return if pending {
            NodeGate::Blocked
        } else if failed {
            NodeGate::Skip("upstream_failed")
        } else if active == 0 && definition.incoming_count(&node.id) > 0 {
            NodeGate::Skip("inactive")
        } else {
            NodeGate::Ready(predecessors)
        };
    }
    let mut ready = true;
    let mut skip = false;
    let mut predecessors = BTreeMap::new();
    let mut skip_reason = "";
    for edge in definition.incoming(&node.id) {
        match view.get(&edge.from) {
            Some(n) if n.status == NodeStatus::Skipped => {
                // v1 语义：前驱被跳过则下游也跳过（跳转会向下游传播）。
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
///
/// 先占槽位与在飞记账，再启动任务：任务一旦运行就可能立刻回写结果，
/// 记账必须先于它完成，否则会漏掉这次尝试。
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
    if node.is_harness_agent() {
        coord.agents.insert(key.clone());
    }
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
///
/// 一次节点尝试的执行包装：把失败与 panic 都转成可提交的事实。
/// 关键是「不把平台故障写成业务失败」——已授权外部操作的节点在断链时
/// 必须落到 Uncertain 等待人工裁决。
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
            // 铁律 2:已授权外部操作的节点在执行期断链/崩溃属于平台故障面,
            // 不得写成 NodeFailed。两种形态都折进 Uncertain 等待:
            // - outcome 为 None:请求可能已发出,结果未知(unknown);
            // - outcome 已提交:外部结果已被审计事实捕获,只是输出推导/
            //   Result 未送达(outcome_retained)——写成失败等于把可能已
            //   成功的外部调用冒充成工作流失败,且节点 Failed 后
            //   run_adjudicate 不再有出口。
            let operation = n.operation.as_ref();
            let uncertain = operation.is_some();
            let mut payload = if uncertain {
                let integrity = if operation.is_some_and(|o| o.outcome.is_none()) {
                    "unknown"
                } else {
                    "outcome_retained"
                };
                json!({"wait": Wait::Uncertain, "integrity": integrity, "delivery_error": error.to_string()})
            } else {
                crate::journal_execution::failure_evidence(
                    &error.to_string(),
                    n.prepared.is_none(),
                    &snapshot.input,
                    &predecessors,
                )
            };
            let policy = node.retry();
            if !uncertain
                && n.operation.is_none()
                && n.prepared.is_some()
                && matches!(node.kind(), Some(NodeType::Script | NodeType::Condition))
                && n.attempt < policy.max_attempts.min(100)
            {
                // 准备阶段就失败（脚本/条件在派发前抛错）属于本节点自己的
                // 错误，没有碰过外部系统，可以按策略自动重试。
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
            // 失败证据落账失败必须留痕（铁律 1 的运维面）：吞错会让节点
            // 保持 Running、下一轮重派发——至少把错误打出来，避免「不知
            // 道却不报」。
            if let Err(finish_error) = attempt
                .finish(
                    if uncertain {
                        EventKind::WaitRegistered
                    } else {
                        EventKind::NodeFailed
                    },
                    payload,
                )
                .await
            {
                tracing::error!(
                    error = %finish_error,
                    run_id = %attempt.run_id,
                    node_id = %attempt.node_id,
                    "node failure evidence could not be committed"
                );
            }
        }
    }
    (key, result)
}

/// v1 收尾判定（§6.6）：全部节点终态后收口 run——任一 failed → RunFailed
/// （错误取定义序第一个 failed 节点）；否则全部 end 计入输出（无输出/
/// skipped 的 end 记 null，不再丢弃——多 end 时形状恒为 map，与 v1
/// singular_or_map 一致）。
///
/// 收口只在「全部节点都终态」时发生：还有节点在跑或等待时，
/// run 的结局尚未确定。
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
            if definition.semantics >= 2
                && !definition.nodes.iter().any(|n| {
                    n.kind() == Some(NodeType::End)
                        && snapshot
                            .nodes
                            .get(&n.id)
                            .is_some_and(|n| n.status == NodeStatus::Succeeded)
                })
            {
                // v2 语义：没有有效 end 完成即视为失败（v1 允许无 end）。
                return finish_run(
                    backend,
                    run_id,
                    None,
                    Some("no active end node completed".into()),
                )
                .await;
            }
            let ends = definition
                .nodes
                .iter()
                .filter(|n| n.kind() == Some(NodeType::End))
                .filter(|n| {
                    definition.semantics == 1
                        || snapshot
                            .nodes
                            .get(&n.id)
                            .is_some_and(|r| r.status == NodeStatus::Succeeded)
                })
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
            if let Some(schema) = &definition.output_schema {
                // 输出合同不满足时 run 失败，而不是把不合规的输出交给调用方。
                let actual = match flow_journal::value::materialize(
                    backend.journal.root(),
                    backend.journal.durable_lsn(),
                    &output,
                    flow_journal::VALUE_BUDGET,
                ) {
                    Ok(value) => value,
                    Err(flow_journal::Error::Limit(_)) => {
                        return finish_run(
                            backend,
                            run_id,
                            None,
                            Some("output contract exceeds 8 MiB validation budget".into()),
                        )
                        .await
                    }
                    Err(e) => return Err(e.into()),
                };
                if let Err(error) = flow_engine::schema::validate(schema, &actual) {
                    return finish_run(
                        backend,
                        run_id,
                        None,
                        Some(format!("output contract: {error}")),
                    )
                    .await;
                }
            }
            finish_run(backend, run_id, Some(output), None).await?;
        }
    }
    Ok(())
}

/// 把多个 end 输出合成一个 run 输出值。
///
/// 单值直接复用；多值合成一个流式对象并落成正文引用——多个 end 的
/// 输出不能塞进一行事务。
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

/// 收口一个 run：写 RunCompleted / RunFailed 事实，并顺带唤醒父 run。
///
/// 父 run 的等待节点在子 run 终态时立刻得到结论：成功则传输出，
/// 失败则传错误——父节点不必等到下一轮轮询才发现子 run 已结束。
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

    /// 构造一个带给定边的定义（闸门单测用）。
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

    /// 按 ID 取定义中的节点。
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
                        inactive: *status == "skipped",
                    },
                )
            })
            .collect()
    }

    /// 闸门：前驱未定则节点保持阻塞。
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

    /// 闸门：只收集被接受的前驱输出。
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

    /// 闸门：失败或跳过的上游会让下游被跳过。
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

    /// 闸门：条件节点的分支端口决定哪条边被接受。
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

    /// 闸门：没有入边的节点（如 start）直接就绪。
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
    /// 装一个工作流（创建 → 更新 → 发布）并启动一次 run，返回 run_id。
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
    /// 轮询等待 run 进入终态（超时即失败）。
    async fn terminal(backend: &JournalBackend, id: &str) -> Run {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let run = backend.state().await.unwrap().runs[id].clone();
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

    /// 定义物化瞬时失败后应能自动重试，不应产生额外命令。
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
            backend.state().await.unwrap().runs[&id].nodes.is_empty(),
            "unavailable-definition nodes: {:?}",
            backend.state().await.unwrap().runs[&id].nodes
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
