//! Driver：单 run 的调度循环。
//!
//! 语义与单机实现完全一致（DESIGN.md §6），事件出口抽象为 `RunEventSink`
//! （Phase 0 后端边界）：单机后端走文件日志 + RunObserver，
//! Postgres 后端走租约保护的事务追加 + 持久 inbox。所有权丢失（LeaseLost）时
//! 静默退出——不写事件、不投影状态，由新持有者从已提交日志恢复。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::backend::{CommitOutcome, PendingInput, PendingInputKind, RunEventSink};
use crate::child_run::ChildRunLauncher;
use crate::engine::{DbRunStatus, Signal};
use crate::error::EngineError;
use crate::event::{Envelope, Event, LogLevel, LogStream};
use crate::exec::{self, ChildRunSpec, NodeExecContext, NodeFailure};
use crate::fold::{NodeState, RunState};
use crate::model::{Definition, Node, NodeType};
use crate::nodelog::{
    cap_input_snapshot, redact_value, truncate_message, LogBudget, LogLine, NodeLogger,
};

/// 单次驱动所需的全部输入。
pub struct DriverSpec {
    pub run_id: String,
    pub definition: Arc<Definition>,
    pub input: Value,
    /// 本 run 的嵌套深度（来自 run_started，恢复时从日志读回）
    pub depth: u32,
    /// sub_workflow 节点的子 run 启动器；未配置时 sub_workflow 节点执行即 fatal
    pub child_launcher: Option<Arc<dyn ChildRunLauncher>>,
    /// 事件出口（单机文件日志 / Postgres 受保护追加）
    pub sink: Box<dyn RunEventSink>,
    /// 本地事件广播（单机后端）。Postgres 后端的事件订阅走共享日志轮询，传 None。
    pub events_tx: Option<broadcast::Sender<Envelope>>,
    /// 用户取消（单机后端）。Postgres 后端的取消经持久 inbox 消费，传未触发的 token。
    pub cancel: CancellationToken,
    /// 所有权丢失 / 停机（Postgres 后端）。触发后静默退出，不写任何事件。
    pub ownership_lost: CancellationToken,
    /// 信号请求通道（文件后端）；Postgres 后端的信号经持久 inbox，传 None。
    pub signal_rx: Option<mpsc::Receiver<SignalRequest>>,
    /// 持久 inbox 轮询间隔（单机后端无 inbox，占位值即可）。
    pub inbox_poll: Duration,
}

/// 单批日志行数上限：攒批窗口内到达的行一次写盘，这个上限防止一次批把
/// 驱动循环按死（也是后端单事务的语句数上限，见 sink 的 append_log_batch）。
const LOG_BATCH_MAX: usize = 256;

/// 文件后端的信号请求：oneshot 回执把校验/落盘结果还给调用方。
/// Postgres 后端不走这条通道（信号经持久 inbox），`signal_rx` 传 None 即可。
pub struct SignalRequest {
    pub signal: Signal,
    pub reply: oneshot::Sender<Result<(), EngineError>>,
}

/// 派发前的同步准备结果：节点、决定论输入面（直接前驱 id + 它们的输出）。
type NodeInputs = (Node, Vec<String>, HashMap<String, Value>);

/// 派发前的准备结果（纯计算的产物，不含任何写入）。
struct NodePrep {
    /// 展开后的执行期 params（无模板时是原节点）
    node: Node,
    /// 脱敏 + 限幅后的输入面快照，随 node_started 落盘
    snapshot: Value,
    /// 展开失败时的节点失败；Some 时不派发执行
    failure: Option<NodeFailure>,
    preds: Vec<String>,
    outputs: HashMap<String, Value>,
}

/// params 展开 + 输入面快照。**自由函数**：只吃入参、不碰 Driver，
/// 因此同一批 ready 节点可以 `join_all` 并发跑（`drive` 的扇出路径）。
///
/// 展开恰好一次（见 `exec::expand_params`）：结果**同时**是执行期 params 与
/// 输入面快照，exec 层不再展开。human_task 不执行、历史行为也不展开，
/// 输入面用原始 params。快照先脱敏（写盘前）后限幅。
async fn build_node_prep(
    node: Node,
    preds: Vec<String>,
    outputs: HashMap<String, Value>,
    input: &Value,
) -> NodePrep {
    if node.kind() == Some(NodeType::HumanTask) {
        let snapshot = cap_input_snapshot(&redact_value(&node.params));
        return NodePrep {
            node,
            snapshot,
            failure: None,
            preds,
            outputs,
        };
    }
    match exec::expand_params(&node, input, &outputs).await {
        Ok(expanded) => {
            let node = expanded.unwrap_or(node);
            let snapshot = cap_input_snapshot(&redact_value(&node.params));
            NodePrep {
                node,
                snapshot,
                failure: None,
                preds,
                outputs,
            }
        }
        Err(failure) => NodePrep {
            node,
            snapshot: Value::Null,
            failure: Some(failure),
            preds,
            outputs,
        },
    }
}

/// 启动 Driver 任务。返回的 JoinHandle 结束即 Driver 完全退出（inflight 已 abort），
/// 调用方据此释放本地 registry 与容量许可。
pub fn spawn_driver(spec: DriverSpec, state: RunState, plan: RecoveryPlan) -> JoinHandle<()> {
    // 日志通道与预算在 driver 内创建：单机 / Postgres 两个后端自动同享，
    // 无需 DriverSpec 感知。预算与广播容量共用 FLOW_RUN_LOG_BUDGET。
    // 接收端只活在 drive() 里（局部变量）——**不要**挂回 Driver 字段：
    // 排空函数再从 self 取会永远拿到 None，攒批静默退化成逐行落盘。
    let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel();
    let driver = Driver {
        run_id: spec.run_id,
        definition: spec.definition,
        input: spec.input,
        depth: spec.depth,
        child_launcher: spec.child_launcher,
        sink: spec.sink,
        state,
        events_tx: spec.events_tx,
        cancel: spec.cancel,
        lost: spec.ownership_lost,
        signal_rx: spec.signal_rx,
        inbox_poll: spec.inbox_poll,
        inflight: HashMap::new(),
        human_waiting: HashMap::new(),
        adjudicating: HashSet::new(),
        lease_lost: false,
        log_tx,
        budget: LogBudget::new(crate::nodelog::budget_from_env()),
    };
    tokio::spawn(driver.run(plan, log_rx))
}

/// 恢复期对未完成节点和待重试节点的分类结论。
/// 单机与分布式接管共用同一分类（DESIGN.md §7 / DISTRIBUTED.md §7）。
#[derive(Default, Debug)]
pub struct RecoveryPlan {
    /// 纯节点：安全重放
    pub replay: Vec<(String, u32)>,
    /// human_task：已有信号记录的补终态，没有的继续等待
    pub signal_received: Vec<(String, u32, Value)>,
    pub human_wait: Vec<(String, u32)>,
    /// 副作用节点：必须人工裁决（分布式下这是副作用准入检查的核心：
    /// 接管后绝不自动重放有外部副作用的节点）
    pub adjudicate: Vec<String>,
    pub adjudication_received: Vec<Signal>,
    pub retries: Vec<String>,
}

impl RecoveryPlan {
    pub fn classify(definition: &Definition, state: &RunState) -> RecoveryPlan {
        let mut plan = RecoveryPlan::default();
        for (node_id, record) in &state.records {
            if matches!(
                record.state,
                NodeState::Failed {
                    retryable: true,
                    ..
                }
            ) {
                plan.retries.push(node_id.clone());
                continue;
            }
            let NodeState::Running { attempt } = record.state else {
                continue;
            };
            let Some(kind) = definition.node_type(node_id) else {
                continue;
            };
            match kind {
                NodeType::HumanTask => match &record.last_signal {
                    Some(payload) => {
                        plan.signal_received
                            .push((node_id.clone(), attempt, payload.clone()))
                    }
                    None => plan.human_wait.push((node_id.clone(), attempt)),
                },
                kind if kind.has_side_effect() => {
                    plan.adjudicate.push(node_id.clone());
                    if let Some(payload) = &record.last_signal {
                        plan.adjudication_received.push(Signal {
                            node_id: node_id.clone(),
                            payload: payload.clone(),
                        });
                    }
                }
                _ => plan.replay.push((node_id.clone(), attempt)),
            }
        }
        plan
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Adjudication {
    Retry,
    Succeeded {
        #[serde(default)]
        output: Value,
    },
    Failed {
        #[serde(default)]
        error: Option<String>,
    },
}

enum SignalAction {
    Human,
    Adjudicate(Adjudication),
}

enum DriverMsg {
    Done {
        node_id: String,
        attempt: u32,
        result: Result<Value, NodeFailure>,
        duration_ms: u64,
    },
    RetryDue {
        node_id: String,
    },
}

struct Driver {
    run_id: String,
    definition: Arc<Definition>,
    input: Value,
    depth: u32,
    child_launcher: Option<Arc<dyn ChildRunLauncher>>,
    sink: Box<dyn RunEventSink>,
    state: RunState,
    /// 本地事件广播；Postgres 后端为 None（订阅走共享日志轮询）
    events_tx: Option<broadcast::Sender<Envelope>>,
    /// 用户取消（单机后端）。Postgres 后端的取消经持久 inbox 消费，此 token 不触发。
    cancel: CancellationToken,
    /// 所有权丢失 / 停机（Postgres 后端）。触发后静默退出，不写任何事件。
    lost: CancellationToken,
    /// 文件后端的信号请求通道；Postgres 后端为 None（信号经持久 inbox）
    signal_rx: Option<mpsc::Receiver<SignalRequest>>,
    inbox_poll: Duration,
    inflight: HashMap<String, JoinHandle<()>>,
    /// human_task：等待外部信号的 oneshot
    human_waiting: HashMap<String, oneshot::Sender<Value>>,
    /// 崩溃/接管遗留的副作用节点：等待人工裁决
    adjudicating: HashSet<String>,
    /// 任一受保护操作返回 LeaseLost 后置位；run() 据此静默退出。
    lease_lost: bool,
    /// 节点日志通道：exec 任务发射端（克隆进 NodeLogger），select 循环排空落盘
    log_tx: tokio::sync::mpsc::UnboundedSender<LogLine>,
    /// per-run 日志预算（发射端一处判定）
    budget: Arc<LogBudget>,
}

impl Driver {
    async fn run(mut self, plan: RecoveryPlan, log_rx: mpsc::UnboundedReceiver<LogLine>) {
        let outcome = self.drive(plan, log_rx).await;
        self.abort_inflight();
        if self.lease_lost {
            tracing::warn!(run_id = %self.run_id, "失去 run 所有权，静默退出（不写终态）");
            return;
        }
        match outcome {
            Ok(()) => {}
            Err(err) if err.is_platform_fault() => {
                // 平台故障 ≠ 工作流失败：不写 run_failed 终态，
                // 投影 awaiting_resume 等待恢复/接管（SQLite 重启恢复、
                // Postgres executor 自动接管），不把基础设施问题伪造成业务终态
                let message = err.to_string();
                tracing::error!(run_id = %self.run_id, error = %err, "基础设施故障，run 挂起等待恢复");
                if let Err(project_err) = self
                    .project_status(DbRunStatus::AwaitingResume, Some(&message))
                    .await
                {
                    tracing::error!(run_id = %self.run_id, error = %project_err, "投影 awaiting_resume 失败");
                }
            }
            Err(err) => {
                let message = err.to_string();
                match self
                    .append_terminal(Event::RunFailed {
                        error: message.clone(),
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(append_err) if append_err.is_lease_lost() => {}
                    Err(append_err) => {
                        tracing::error!(run_id = %self.run_id, error = %append_err, "写入 run_failed 失败");
                        // 无法写入 run_failed：索引保留 awaiting_resume，不伪造未持久化的终态
                        let _ = self
                            .project_status(DbRunStatus::AwaitingResume, Some(&message))
                            .await;
                    }
                }
            }
        }
    }

    async fn drive(
        &mut self,
        plan: RecoveryPlan,
        mut log_rx: mpsc::UnboundedReceiver<LogLine>,
    ) -> Result<(), EngineError> {
        let (result_tx, mut result_rx) = mpsc::channel::<DriverMsg>(256);
        let mut inbox_tick = tokio::time::interval(self.inbox_poll);
        inbox_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // 取出接收端，避免 select! 的 future 与分支体同时借用 self
        let mut signal_rx = self.signal_rx.take();

        self.project_status(DbRunStatus::Running, None).await?;
        self.apply_recovery_plan(plan, &result_tx).await?;

        loop {
            // 推进到不动点：跳过必须沿下游传递，否则深层分支会被误判为停滞
            loop {
                let (ready, skips) = self.plan();
                if ready.is_empty() && skips.is_empty() {
                    break;
                }
                for (node_id, reason) in skips {
                    self.append(Event::NodeSkipped { node_id, reason }).await?;
                }
                // 并发展开：准备是纯计算，同批节点互不依赖（本轮内 outputs
                // 不会变）。串行 await 会把 N 个 spawn_blocking 排成一条队，
                // 期间取消信号、日志排空、租约失效全部要等它跑完。
                let targets: Vec<(String, u32)> = ready
                    .iter()
                    .map(|id| Ok((id.clone(), self.next_attempt(id))))
                    .collect::<Result<_, EngineError>>()?;
                let raw = targets
                    .iter()
                    .map(|(id, _)| self.prepare_inputs(id))
                    .collect::<Result<Vec<_>, EngineError>>()?;
                let input = self.input.clone();
                let preps =
                    join_all(raw.into_iter().map(|(node, preds, outputs)| {
                        build_node_prep(node, preds, outputs, &input)
                    }))
                    .await;
                for ((node_id, attempt), prep) in targets.into_iter().zip(preps) {
                    self.dispatch_prepared(&node_id, attempt, prep, &result_tx)
                        .await?;
                }
            }

            // inflight 覆盖所有尚未消费的结果；human_waiting / adjudicating 表示外部输入未决
            if self.inflight.is_empty()
                && self.human_waiting.is_empty()
                && self.adjudicating.is_empty()
            {
                if self.state.all_terminal() {
                    return self.finalize(&mut log_rx).await;
                }
                return Err(EngineError::Node(
                    "调度停滞：存在既不可就绪也无法跳过的节点".into(),
                ));
            }

            tokio::select! {
                biased;
                _ = self.lost.cancelled() => {
                    // 停机/失去所有权：停止派发、取消本地任务、丢弃未提交结果。
                    // 只有当没有 http_call 在途时才尝试优雅释放租约；否则留给 TTL
                    // 到期后接管——取消本地 future 不能证明外部 HTTP 已停止（§7）。
                    let http_inflight = self
                        .inflight
                        .keys()
                        .any(|id| self.definition.node_type(id) == Some(NodeType::HttpCall));
                    self.abort_inflight();
                    if !http_inflight {
                        if let Err(err) = self.sink.release().await {
                            tracing::debug!(run_id = %self.run_id, error = %err, "释放租约失败，等 TTL 到期接管");
                        }
                    }
                    return Ok(());
                }
                _ = self.cancel.cancelled() => {
                    // 取消级联必须在写 RunCancelled 前发起，子 run 才有最大机会
                    // 及时停止（与 inbox 取消路径共用同一入口，§6.8）
                    Self::cascade_cancel_children(
                        self.child_launcher.clone(),
                        self.running_child_runs(),
                    )
                    .await;
                    self.abort_inflight();
                    self.append_terminal(Event::RunCancelled {}).await?;
                    return Ok(());
                }
                // 日志排在结果之前（biased）：发射端先发日志后发 Done（程序序），
                // 两通道都就绪时先排空日志，落盘顺序与执行顺序一致。
                Some(line) = log_rx.recv() => self.drain_logs(&mut log_rx, line).await?,
                Some(msg) = result_rx.recv() => self.handle_result(msg, &result_tx).await?,
                Some(request) = async {
                    match signal_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let SignalRequest { signal, reply } = request;
                    let action = match self.signal_action(&signal) {
                        Ok(action) => action,
                        Err(err) => {
                            let _ = reply.send(Err(err));
                            continue;
                        }
                    };
                    let outcome = self.handle_signal(signal, action, &result_tx).await;
                    match outcome {
                        Ok(()) => { let _ = reply.send(Ok(())); }
                        Err(err) => {
                            let _ = reply.send(Err(EngineError::Node(err.to_string())));
                            return Err(err);
                        }
                    }
                }
                _ = inbox_tick.tick() => {
                    let inputs = match self.sink.poll_inputs().await {
                        Ok(inputs) => inputs,
                        Err(err) => return Err(self.guard_lease(err)),
                    };
                    for input in inputs {
                        match input.kind {
                            PendingInputKind::Cancel => {
                                // 取消级联必须先于终态事务：inbox 取消与本地取消
                                // 是同一条契约（§6.8 best-effort 级联），不能只有
                                // 单机路径级联、分布式路径把子 run 漏成永远 running
                                Self::cascade_cancel_children(
                                    self.child_launcher.clone(),
                                    self.running_child_runs(),
                                )
                                .await;
                                self.consume_cancel(&input).await?;
                            }
                            PendingInputKind::Signal => {
                                self.handle_inbox_signal(input, &result_tx).await?;
                            }
                        }
                        if self.state.phase.is_terminal() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    // ---- sink 包装：折叠 + 本地广播 + LeaseLost 记账 ----

    /// 本地广播（单机后端）。Postgres 后端无本地订阅者，events_tx 为 None。
    fn broadcast(&self, envelope: Envelope) {
        if let Some(tx) = &self.events_tx {
            let _ = tx.send(envelope);
        }
    }

    async fn append(&mut self, event: Event) -> Result<Envelope, EngineError> {
        match self.sink.append(event).await {
            Ok(envelope) => {
                self.state.fold(&envelope);
                self.broadcast(envelope.clone());
                Ok(envelope)
            }
            Err(err) => Err(self.guard_lease(err)),
        }
    }

    async fn append_terminal(&mut self, event: Event) -> Result<Envelope, EngineError> {
        match self.sink.append_terminal(event).await {
            Ok(envelope) => {
                self.state.fold(&envelope);
                self.broadcast(envelope.clone());
                Ok(envelope)
            }
            Err(err) => Err(self.guard_lease(err)),
        }
    }

    /// 节点日志落盘（进程级持久）：fold 不消费 NodeLog，但 seq 必须推进、
    /// 订阅者必须收到，与 append 同一套包装。
    async fn write_log_batch(&mut self, lines: Vec<LogLine>) -> Result<(), EngineError> {
        if lines.is_empty() {
            return Ok(());
        }
        let events: Vec<Event> = lines
            .into_iter()
            .map(|line| Event::NodeLog {
                node_id: line.node_id,
                attempt: line.attempt,
                level: line.level,
                stream: line.stream,
                message: line.message,
            })
            .collect();
        match self.sink.append_log_batch(events).await {
            Ok(envelopes) => {
                for envelope in envelopes {
                    self.state.fold(&envelope);
                    self.broadcast(envelope);
                }
                Ok(())
            }
            Err(err) => Err(self.guard_lease(err)),
        }
    }

    /// 引擎叙事日志的**唯一**出口：与 exec 的 console 日志同一条路径
    /// （同预算、同截断、同批落盘）。driver 不再直接 `append(NodeLog)`——
    /// 那在 Postgres 上是每条一个受保护事务，与「日志是廉价层」的契约冲突
    /// （backend.rs §append_log_batch）。
    async fn write_log_line(&mut self, line: LogLine) -> Result<(), EngineError> {
        if !self.budget.admit(line.level) {
            // 丢弃计数由 flush_log_summary 统一留痕，不在这里递归摘要
            return Ok(());
        }
        self.write_log_batch(vec![LogLine {
            message: truncate_message(&line.message),
            ..line
        }])
        .await
    }

    /// 排空日志通道：首条已到，攒批窗口内继续收集一批，落盘后补预算摘要。
    ///
    /// **攒批窗口**：产者是另一个线程（节点 exec / QuickJS console 桥），
    /// 纯 `try_recv` 的排空在刷屏时每批只有 1 行——批接口形同虚设，Postgres
    /// 上等于每行一个事务。日志是观察数据、本层没有时延契约，允许为凑批
    /// 短暂等一会儿。等待按**批**摊销不是按行：窗口内到达的行一次写完，
    /// 吞吐是 窗口/批大小，不是 1/窗口。
    async fn drain_logs(
        &mut self,
        rx: &mut mpsc::UnboundedReceiver<LogLine>,
        first: LogLine,
    ) -> Result<(), EngineError> {
        const BATCH_WAIT: Duration = Duration::from_millis(1);
        let mut lines = vec![first];
        let deadline = tokio::time::Instant::now() + BATCH_WAIT;
        while lines.len() < LOG_BATCH_MAX {
            match rx.try_recv() {
                Ok(line) => lines.push(line),
                Err(_) => match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(line)) => lines.push(line),
                    // 窗口到点 / 通道关闭：手上这批就是全部
                    Ok(None) | Err(_) => break,
                },
            }
        }
        self.write_log_batch(lines).await?;
        self.flush_log_summary().await
    }

    /// 预算摘要：有丢弃就补一条 engine 级日志留痕（node_id 为空 = run 级）。
    async fn flush_log_summary(&mut self) -> Result<(), EngineError> {
        let dropped = self.budget.take_dropped();
        if dropped == 0 {
            return Ok(());
        }
        self.write_log_batch(vec![LogLine {
            node_id: String::new(),
            attempt: 0,
            level: LogLevel::Warn,
            stream: LogStream::Engine,
            message: format!("已丢弃 {dropped} 条 debug/info 日志（达到每 run 日志预算）"),
        }])
        .await
    }

    /// 终态前排空残余日志：所有节点已终态时 exec 任务的发送端已全部关闭，
    /// 通道必然可排尽。终态 append 的内建兜底 sync 会把这些行一并刷盘。
    /// 这里是唯一不走攒批窗口的地方：必须把通道排到底（循环分批，
    /// 单批仍是 LOG_BATCH_MAX——终态前多丢一条日志都是排查证据的损失）。
    async fn drain_remaining_logs(
        &mut self,
        rx: &mut mpsc::UnboundedReceiver<LogLine>,
    ) -> Result<(), EngineError> {
        loop {
            let mut lines = Vec::new();
            while lines.len() < LOG_BATCH_MAX {
                match rx.try_recv() {
                    Ok(line) => lines.push(line),
                    Err(_) => break,
                }
            }
            if lines.is_empty() {
                return self.flush_log_summary().await;
            }
            self.write_log_batch(lines).await?;
        }
    }

    async fn project_status(
        &mut self,
        status: DbRunStatus,
        error: Option<&str>,
    ) -> Result<(), EngineError> {
        match self.sink.project_status(status, error).await {
            Ok(()) => Ok(()),
            Err(err) => Err(self.guard_lease(err)),
        }
    }

    /// 仍在 Running 的 sub_workflow 节点的子 run id（同步收集，不跨 await）。
    fn running_child_runs(&self) -> Vec<String> {
        self.state
            .records
            .iter()
            .filter(|(id, rec)| {
                matches!(rec.state, NodeState::Running { .. })
                    && self.definition.node_type(id) == Some(NodeType::SubWorkflow)
            })
            .filter_map(|(_, rec)| rec.child_run_id.clone())
            .collect()
    }

    /// 取消级联（§6.8）：best-effort，失败只记日志，不升级。本地取消与 inbox
    /// 取消共用同一入口——两条路径都必须满足「发起于写 RunCancelled 之前」。
    /// 关联函数签名（而非 &self）：保证没有对 Driver 的借用跨过 await，
    /// Driver future 才是 Send。
    async fn cascade_cancel_children(
        launcher: Option<Arc<dyn ChildRunLauncher>>,
        child_runs: Vec<String>,
    ) {
        if child_runs.is_empty() {
            return;
        }
        if let Some(launcher) = launcher {
            // 并发取消全部子 run（join_all 等全部返回，仍满足上述不变量）
            join_all(child_runs.iter().map(|id| launcher.cancel(id))).await;
        }
    }

    async fn consume_cancel(&mut self, input: &PendingInput) -> Result<(), EngineError> {
        match self.sink.consume_cancel(input).await {
            Ok(CommitOutcome::Applied(envelope)) => {
                self.state.fold(&envelope);
                self.broadcast(envelope);
                tracing::info!(run_id = %self.run_id, "取消命令已提交，停止本地 Driver");
                Ok(())
            }
            Ok(CommitOutcome::Duplicate) => {
                tracing::debug!(run_id = %self.run_id, signal_id = %input.signal_id, "取消命令已被处理");
                Ok(())
            }
            Err(err) => Err(self.guard_lease(err)),
        }
    }

    fn guard_lease(&mut self, err: EngineError) -> EngineError {
        if err.is_lease_lost() {
            self.lease_lost = true;
        }
        err
    }

    async fn apply_recovery_plan(
        &mut self,
        plan: RecoveryPlan,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        for (node_id, attempt, payload) in plan.signal_received {
            // signal_received 已落盘但终态缺失：补终态，不重复等待。
            // fold 会把 output 记进 state.outputs，无需第二份手工同步。
            self.append(Event::NodeCompleted {
                node_id,
                attempt,
                output: payload,
                duration_ms: 0,
            })
            .await?;
        }

        for (node_id, attempt) in plan.human_wait {
            self.register_human_wait(&node_id, attempt, result_tx).await;
        }

        if !plan.adjudicate.is_empty() {
            for node_id in plan.adjudicate {
                // 运行叙事进事件流（NodeLog），不再是纯运维 tracing
                let _ = self
                    .write_log_line(LogLine {
                        node_id: node_id.clone(),
                        attempt: self.state.record(&node_id).attempts,
                        level: LogLevel::Warn,
                        stream: LogStream::Engine,
                        message: "副作用节点状态不明（接管/重启），等待人工裁决".into(),
                    })
                    .await;
                self.adjudicating.insert(node_id);
            }
            self.project_status(
                DbRunStatus::AwaitingResume,
                Some("存在状态不明的副作用节点，等待人工裁决"),
            )
            .await?;
        }

        for (node_id, attempt) in plan.replay {
            let _ = self
                .write_log_line(LogLine {
                    node_id: node_id.clone(),
                    attempt,
                    level: LogLevel::Info,
                    stream: LogStream::Engine,
                    message: format!("接管/重启残留节点，重新执行（attempt {}）", attempt + 1),
                })
                .await;
            self.start_node(&node_id, attempt + 1, result_tx).await?;
        }

        for node_id in plan.retries {
            self.schedule_retry(&node_id, result_tx).await;
        }
        for signal in plan.adjudication_received {
            let action = self.signal_action(&signal)?;
            self.apply_signal(signal, action, result_tx).await?;
        }

        Ok(())
    }

    fn next_attempt(&self, node_id: &str) -> u32 {
        self.state.record(node_id).attempts + 1
    }

    /// 计算可运行节点与必须跳过的节点。跳过的判定：任一入边确定不满足。
    fn plan(&self) -> (Vec<String>, Vec<(String, String)>) {
        let mut ready = Vec::new();
        let mut skips = Vec::new();

        for node in &self.definition.nodes {
            let record = self.state.record(&node.id);
            if !matches!(record.state, NodeState::Pending) {
                continue;
            }
            let incoming = self.definition.incoming(&node.id);
            if incoming.is_empty() {
                // validate 保证唯一无入边的就是 start：直接就绪
                ready.push(node.id.clone());
                continue;
            }

            let mut all_satisfied = true;
            let mut reason: Option<String> = None;
            for edge in &incoming {
                match self.edge_state(edge.from.as_str(), edge.port.as_deref()) {
                    EdgeState::Satisfied => {}
                    EdgeState::Waiting => all_satisfied = false,
                    EdgeState::Unsatisfied(why) => {
                        all_satisfied = false;
                        reason.get_or_insert(why);
                    }
                }
            }

            if let Some(why) = reason {
                skips.push((node.id.clone(), why));
            } else if all_satisfied {
                ready.push(node.id.clone());
            }
        }

        (ready, skips)
    }

    fn edge_state(&self, from: &str, port: Option<&str>) -> EdgeState {
        let record = self.state.record(from);
        match record.state {
            NodeState::Completed { .. } => {
                let from_kind = self.definition.node_type(from);
                if from_kind == Some(NodeType::Condition) {
                    let taken = self
                        .state
                        .outputs
                        .get(from)
                        .map(exec::truthy)
                        .unwrap_or(false);
                    let taken_port = if taken { "true" } else { "false" };
                    if port == Some(taken_port) {
                        EdgeState::Satisfied
                    } else {
                        EdgeState::Unsatisfied("branch_not_taken".into())
                    }
                } else {
                    EdgeState::Satisfied
                }
            }
            NodeState::Skipped { .. } => EdgeState::Unsatisfied("upstream_skipped".into()),
            NodeState::Failed {
                retryable: false, ..
            } => EdgeState::Unsatisfied("upstream_failed".into()),
            NodeState::Pending
            | NodeState::Running { .. }
            | NodeState::Failed {
                retryable: true, ..
            } => EdgeState::Waiting,
        }
    }

    /// 节点派发（就绪、重试、人工裁决 retry、接管重放的唯一入口）：
    /// 先准备（并发的纯计算），再派发（单写者的有序写入）。
    async fn start_node(
        &mut self,
        node_id: &str,
        attempt: u32,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        let (node, preds, outputs) = self.prepare_inputs(node_id)?;
        let input = self.input.clone();
        let prep = build_node_prep(node, preds, outputs, &input).await;
        self.dispatch_prepared(node_id, attempt, prep, result_tx)
            .await
    }

    /// 派发前的同步准备：克隆节点、决定论输入面。不跨 await，因此是 `&self`
    /// 的普通方法；展开部分在自由函数 `build_node_prep` 里（Driver 内的
    /// `Box<dyn RunEventSink>` 只 Send 不 Sync，`&self` 跨 await 会让 driver
    /// future 失去 Send）。
    fn prepare_inputs(&self, node_id: &str) -> Result<NodeInputs, EngineError> {
        let node = self
            .definition
            .node(node_id)
            .ok_or_else(|| EngineError::Node(format!("节点不存在：{node_id}")))?
            .clone();
        node.kind()
            .ok_or_else(|| EngineError::Node(format!("节点类型未知：{}", node.node_type)))?;
        let preds: Vec<String> = self
            .definition
            .incoming(node_id)
            .iter()
            .map(|e| e.from.clone())
            .collect();
        // 决定论输入面（DESIGN §10）：nodes 只暴露直接前驱的输出，
        // 引用非前驱节点在 JS 里是 undefined，属性访问即抛错进 fatal
        let outputs: HashMap<String, Value> = preds
            .iter()
            .filter_map(|p| self.state.outputs.get(p).cloned().map(|v| (p.clone(), v)))
            .collect();
        Ok((node, preds, outputs))
    }

    /// 写 `node_started` 并派发执行。展开失败仍先写 node_started（input 为
    /// null），再走节点失败路径——写序协议在两条分支上完全一致。
    async fn dispatch_prepared(
        &mut self,
        node_id: &str,
        attempt: u32,
        prep: NodePrep,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        let NodePrep {
            node,
            snapshot,
            failure,
            preds,
            outputs,
        } = prep;
        let kind = node.kind().expect("build_prep 已校验节点类型");

        if let Some(failure) = failure {
            self.append(Event::NodeStarted {
                node_id: node_id.to_string(),
                attempt,
                child_run_id: None,
                input: None,
            })
            .await?;
            if failure.platform {
                return Err(EngineError::Backend(failure.message));
            }
            return self.fail_node(node_id, attempt, &failure, result_tx).await;
        }

        // sub_workflow 的 child_run_id 随 node_started 一起确定并落盘（副作用前写协议）。
        // 崩溃重放（记录仍是 Running）沿用已落盘的 id 附着原子 run；
        // 重试（新 attempt）派生新 id，每次重试是独立的子 run。
        let child_run_id = if kind == NodeType::SubWorkflow {
            let record = self.state.record(node_id);
            match (&record.state, &record.child_run_id) {
                (NodeState::Running { .. }, Some(existing)) => Some(existing.clone()),
                _ => Some(format!("{}:{}:{}", self.run_id, node_id, attempt)),
            }
        } else {
            None
        };

        self.append(Event::NodeStarted {
            node_id: node_id.to_string(),
            attempt,
            child_run_id: child_run_id.clone(),
            input: Some(snapshot),
        })
        .await?;

        if kind == NodeType::HumanTask {
            self.register_human_wait(node_id, attempt, result_tx).await;
            return Ok(());
        }

        let ctx = NodeExecContext {
            node,
            input: self.input.clone(),
            outputs,
            preds,
            depth: self.depth,
            child: match (child_run_id, &self.child_launcher) {
                (Some(child_run_id), Some(launcher)) => Some(ChildRunSpec {
                    child_run_id,
                    launcher: launcher.clone(),
                }),
                _ => None,
            },
            logger: NodeLogger::new(self.log_tx.clone(), self.budget.clone(), node_id, attempt),
        };
        let cancel = self.cancel.clone();
        let result_tx = result_tx.clone();
        let nid = node_id.to_string();

        let handle = tokio::spawn(async move {
            let started = std::time::Instant::now();
            let result = exec::execute(&ctx, &cancel).await;
            let _ = result_tx
                .send(DriverMsg::Done {
                    node_id: nid,
                    attempt,
                    result,
                    duration_ms: started.elapsed().as_millis() as u64,
                })
                .await;
        });

        self.inflight.insert(node_id.to_string(), handle);
        Ok(())
    }

    /// human_task：节点已落 node_started，引擎持 oneshot 等待外部 signal。
    async fn register_human_wait(
        &mut self,
        node_id: &str,
        attempt: u32,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) {
        // 等待叙事进事件流：运行详情页一眼看出这个节点在等谁
        let _ = self
            .write_log_line(LogLine {
                node_id: node_id.to_string(),
                attempt,
                level: LogLevel::Info,
                stream: LogStream::Engine,
                message: "等待人工交付信号（run.signal）".into(),
            })
            .await;
        let (tx, rx) = oneshot::channel::<Value>();
        self.human_waiting.insert(node_id.to_string(), tx);
        let result_tx = result_tx.clone();
        let nid = node_id.to_string();
        let handle = tokio::spawn(async move {
            if let Ok(payload) = rx.await {
                let _ = result_tx
                    .send(DriverMsg::Done {
                        node_id: nid,
                        attempt,
                        result: Ok(payload),
                        duration_ms: 0,
                    })
                    .await;
            }
        });
        // 不变量：inflight 里的每个 handle 都还欠一条 DriverMsg。
        // 等待任务同样计入，否则信号到达后会被误判为「无可推进节点」。
        self.inflight.insert(node_id.to_string(), handle);
    }

    async fn handle_result(
        &mut self,
        msg: DriverMsg,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        match msg {
            DriverMsg::RetryDue { node_id } => {
                self.inflight.remove(&node_id);
                let attempt = self.next_attempt(&node_id);
                self.start_node(&node_id, attempt, result_tx).await?;
            }
            DriverMsg::Done {
                node_id,
                attempt,
                result,
                duration_ms,
            } => {
                // 防御：只有当前 attempt 的结果才生效。中断/裁决重派后旧任务的
                // 消息可能仍在通道里，迟到结果不得覆盖新 attempt 的状态。
                // 顺序很关键：inflight 不带 attempt 维度，先 remove 会把当前
                // attempt 的 JoinHandle 误摘掉（违反「每个 handle 恰欠一条
                // DriverMsg」不变量），必须确认归属后再摘。
                if !matches!(
                    self.state.record(&node_id).state,
                    NodeState::Running { attempt: current } if current == attempt
                ) {
                    tracing::debug!(
                        run_id = %self.run_id,
                        node_id = %node_id,
                        attempt,
                        "丢弃过期 attempt 的迟到结果"
                    );
                    return Ok(());
                }
                self.inflight.remove(&node_id);
                match result {
                    Ok(output) => {
                        self.append(Event::NodeCompleted {
                            node_id,
                            attempt,
                            output,
                            duration_ms,
                        })
                        .await?;
                    }
                    Err(failure) => {
                        // 平台故障（IO/Backend/日志损坏）不是工作流失败：
                        // 不写 NodeFailed，冒泡到 run() 投影 awaiting_resume
                        // （与 sink 故障同一分类出口），节点留 Running 等恢复。
                        if failure.platform {
                            return Err(EngineError::Backend(failure.message));
                        }
                        self.fail_node(&node_id, attempt, &failure, result_tx)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 节点失败统一出口：写 NodeFailed（按策略标可重试）+ 必要时调度重试。
    /// 执行结果与参数展开失败两条路径共用，避免重试逻辑分叉。
    async fn fail_node(
        &mut self,
        node_id: &str,
        attempt: u32,
        failure: &NodeFailure,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        let policy = self
            .definition
            .node(node_id)
            .map(|n| n.retry())
            .unwrap_or_default();
        let should_retry = failure.retryable && attempt < policy.max_attempts;
        self.append(Event::NodeFailed {
            node_id: node_id.to_string(),
            attempt,
            error: failure.message.clone(),
            retryable: should_retry,
        })
        .await?;
        if should_retry {
            self.schedule_retry(node_id, result_tx).await;
        }
        Ok(())
    }

    /// 重试退避。恢复路径一律等满退避：事件的 `ts` 由**写入者**的时钟决定
    /// （单机是 append 时的 Utc::now()，PG 是事务里的 clock_timestamp()），
    /// 而这里的 `Utc::now()` 来自**当前进程**。跨机器恢复时两个时钟不同源，
    /// NTP 抖动足以把「剩余退避」算成 0 甚至负数（saturating_sub 后同样是 0），
    /// 退避防护静默失效 → 重试风暴。宁可多重试一次间隔，不做不可靠的减法。
    async fn schedule_retry(&mut self, node_id: &str, result_tx: &mpsc::Sender<DriverMsg>) {
        let backoff = self
            .definition
            .node(node_id)
            .map(|node| node.retry().backoff_ms)
            .unwrap_or(0);
        // 重试叙事进事件流：第几次尝试/重试分清楚，退避多久，归属到新 attempt
        let next_attempt = self.next_attempt(node_id);
        let _ = self
            .write_log_line(LogLine {
                node_id: node_id.to_string(),
                attempt: next_attempt,
                level: LogLevel::Warn,
                stream: LogStream::Engine,
                message: format!(
                    "即将开始第 {next_attempt} 次尝试（第 {} 次重试），退避 {backoff}ms",
                    next_attempt - 1
                ),
            })
            .await;
        let result_tx = result_tx.clone();
        let nid = node_id.to_string();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(backoff)).await;
            let _ = result_tx.send(DriverMsg::RetryDue { node_id: nid }).await;
        });
        self.inflight.insert(node_id.to_string(), handle);
    }

    fn signal_action(&self, signal: &Signal) -> Result<SignalAction, EngineError> {
        if self.human_waiting.contains_key(&signal.node_id) {
            return Ok(SignalAction::Human);
        }
        if self.adjudicating.contains(&signal.node_id) {
            return serde_json::from_value(signal.payload.clone())
                .map(SignalAction::Adjudicate)
                .map_err(|err| {
                    EngineError::InvalidSignal(format!("节点 {} 的裁决非法：{err}", signal.node_id))
                });
        }
        Err(EngineError::InvalidSignal(format!(
            "节点 {} 当前不等待信号",
            signal.node_id
        )))
    }

    async fn handle_signal(
        &mut self,
        signal: Signal,
        action: SignalAction,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        self.append(Event::SignalReceived {
            node_id: signal.node_id.clone(),
            payload: signal.payload.clone(),
        })
        .await?;
        self.apply_signal(signal, action, result_tx).await
    }

    /// 持久 inbox 的信号消费：先校验（内存状态），再原子持久化（applied 与
    /// SignalReceived 同事务），提交后才更新内存并解除等待（§6.2）。
    /// inbox 信号是否「早到」：目标是 human_task、节点尚未登记等待、且状态还是
    /// Pending/Running（即将就绪）。Skipped/Failed/Completed 的 human_task 永远不会
    /// 等待——那类照常走校验拒绝，给客户端明确反馈。
    fn inbox_signal_is_early(&self, node_id: &str) -> bool {
        if self.human_waiting.contains_key(node_id) || self.adjudicating.contains(node_id) {
            return false;
        }
        if self.definition.node_type(node_id) != Some(NodeType::HumanTask) {
            return false;
        }
        matches!(
            self.state.record(node_id).state,
            NodeState::Pending | NodeState::Running { .. }
        )
    }

    async fn handle_inbox_signal(
        &mut self,
        input: PendingInput,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        let signal = Signal {
            node_id: input
                .node_id
                .clone()
                .ok_or_else(|| EngineError::Node("signal 输入缺少 node_id".into()))?,
            payload: input.payload.clone(),
        };
        // 早到的 inbox 信号（拆分部署：信号先落账、Driver 后接管；或 run.start
        // 先于节点派发返回）：目标 human_task 还没登记等待（节点尚在 Pending/
        // Running，下一刻就会就绪）时，拒绝会直接丢掉这条信号——run 随后永远
        // 等不到它。此时跳过本次消费：行留 pending，下一次 poll 时节点已等待，
        // 正常走「校验 → 落账 → 交付」。
        if self.inbox_signal_is_early(&signal.node_id) {
            tracing::debug!(
                run_id = %self.run_id,
                signal_id = %input.signal_id,
                node_id = %signal.node_id,
                "inbox 信号早到，目标 human_task 尚未等待，留待下次 poll"
            );
            return Ok(());
        }
        let action = match self.signal_action(&signal) {
            Ok(action) => action,
            Err(err) => {
                // 非法请求只标 rejected，不写事件、不改变 run 终态
                if let Err(reject_err) = self.sink.reject_signal(&input, &err.to_string()).await {
                    return Err(self.guard_lease(reject_err));
                }
                tracing::debug!(run_id = %self.run_id, signal_id = %input.signal_id, error = %err, "信号被拒绝");
                return Ok(());
            }
        };
        let event = Event::SignalReceived {
            node_id: signal.node_id.clone(),
            payload: signal.payload.clone(),
        };
        match self.sink.commit_signal(&input, event).await {
            Ok(CommitOutcome::Applied(envelope)) => {
                self.state.fold(&envelope);
                self.broadcast(envelope);
            }
            Ok(CommitOutcome::Duplicate) => {
                tracing::debug!(run_id = %self.run_id, signal_id = %input.signal_id, "重复信号，忽略");
                return Ok(());
            }
            Err(err) => return Err(self.guard_lease(err)),
        }
        self.apply_signal(signal, action, result_tx).await
    }

    async fn apply_signal(
        &mut self,
        signal: Signal,
        action: SignalAction,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        let node_id = signal.node_id;
        let attempt = self.state.record(&node_id).attempts;
        match action {
            SignalAction::Human => {
                if let Some(tx) = self.human_waiting.remove(&node_id) {
                    let _ = tx.send(signal.payload);
                }
                return Ok(());
            }
            SignalAction::Adjudicate(Adjudication::Retry) => {
                self.start_node(&node_id, attempt + 1, result_tx).await?;
            }
            SignalAction::Adjudicate(Adjudication::Succeeded { output }) => {
                self.append(Event::NodeCompleted {
                    node_id: node_id.clone(),
                    attempt,
                    output,
                    duration_ms: 0,
                })
                .await?;
            }
            SignalAction::Adjudicate(Adjudication::Failed { error }) => {
                self.append(Event::NodeFailed {
                    node_id: node_id.clone(),
                    attempt,
                    error: error.unwrap_or_else(|| "人工判定为失败".into()),
                    retryable: false,
                })
                .await?;
            }
        }
        self.adjudicating.remove(&node_id);
        if self.adjudicating.is_empty() {
            self.project_status(DbRunStatus::Running, None).await?;
        }
        Ok(())
    }

    async fn finalize(
        &mut self,
        log_rx: &mut mpsc::UnboundedReceiver<LogLine>,
    ) -> Result<(), EngineError> {
        // 终态前排空残余日志 + 预算摘要：EventLog::append 对终态事件内建兜底
        // sync，这些行会一并刷盘（终态返回 ⇒ 终态前日志已在盘上）
        self.drain_remaining_logs(log_rx).await?;

        if let Some(error) = self.state.fatal_error.clone() {
            self.append_terminal(Event::RunFailed { error }).await?;
            return Ok(());
        }

        let output = self.collect_output();
        self.append_terminal(Event::RunCompleted { output }).await?;
        Ok(())
    }

    fn collect_output(&self) -> Value {
        // 与 end 节点自身的输出同一条规则（exec::singular_or_map）
        let ends: Vec<(String, Value)> = self
            .definition
            .nodes
            .iter()
            .filter(|n| n.kind() == Some(NodeType::End))
            .map(|n| {
                (
                    n.id.clone(),
                    self.state
                        .outputs
                        .get(&n.id)
                        .cloned()
                        .unwrap_or(Value::Null),
                )
            })
            .collect();
        exec::singular_or_map(ends)
    }

    fn abort_inflight(&mut self) {
        for (_, handle) in self.inflight.drain() {
            handle.abort();
        }
        self.human_waiting.clear();
        self.adjudicating.clear();
    }
}

enum EdgeState {
    Satisfied,
    Waiting,
    Unsatisfied(String),
}
