use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::EngineError;
use crate::event::{read_events, run_dir, Envelope, Event, EventLog};
use crate::exec::{self, NodeExecContext, NodeFailure};
use crate::fold::{NodeState, RunPhase, RunState};
use crate::model::{Definition, NodeType};

const EVENT_CHANNEL_CAPACITY: usize = 1024;
const SIGNAL_CHANNEL_CAPACITY: usize = 64;

/// 落库用的 run 状态，包含初始化与人工介入状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbRunStatus {
    Initializing,
    Running,
    AwaitingResume,
    Succeeded,
    Failed,
    Cancelled,
}

impl DbRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DbRunStatus::Initializing => "initializing",
            DbRunStatus::Running => "running",
            DbRunStatus::AwaitingResume => "awaiting_resume",
            DbRunStatus::Succeeded => "succeeded",
            DbRunStatus::Failed => "failed",
            DbRunStatus::Cancelled => "cancelled",
        }
    }
}

pub struct StatusUpdate<'a> {
    pub run_id: &'a str,
    pub status: DbRunStatus,
    pub output: Option<&'a Value>,
    pub error: Option<&'a str>,
}

/// run 状态变化的出口。由外层（RPC 层）适配到数据库，引擎本身不依赖存储实现。
pub trait RunObserver: Send + Sync + 'static {
    fn on_status<'a>(&'a self, update: StatusUpdate<'a>) -> BoxFuture<'a, ()>;
}

pub struct NoopObserver;

impl RunObserver for NoopObserver {
    fn on_status<'a>(&'a self, _update: StatusUpdate<'a>) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

pub struct StartRun {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: i64,
    pub definition: Definition,
    pub input: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    Resumed,
    AlreadyTerminal(RunPhase),
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub node_id: String,
    /// human_task：任意负载，作为节点输出。
    /// 崩溃遗留的副作用节点：`{"action": "retry" | "succeeded" | "failed", "output": ..., "error": ...}`
    pub payload: Value,
}

struct SignalRequest {
    signal: Signal,
    reply: oneshot::Sender<Result<(), EngineError>>,
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

struct RunHandle {
    cancel: CancellationToken,
    signal_tx: mpsc::Sender<SignalRequest>,
}

/// 工作流执行引擎。每次 run 一个 tokio 任务、一个 event.jsonl 单写者。
pub struct Engine {
    data_dir: PathBuf,
    observer: Arc<dyn RunObserver>,
    events_tx: broadcast::Sender<Envelope>,
    registry: Arc<Mutex<HashMap<String, RunHandle>>>,
}

impl Engine {
    pub fn new(data_dir: impl Into<PathBuf>, observer: Arc<dyn RunObserver>) -> Engine {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Engine {
            data_dir: data_dir.into(),
            observer,
            events_tx,
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn events_path(&self, run_id: &str) -> PathBuf {
        run_dir(&self.data_dir, run_id).join("event.jsonl")
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Envelope> {
        self.events_tx.subscribe()
    }

    pub fn is_live(&self, run_id: &str) -> bool {
        self.registry.lock().contains_key(run_id)
    }

    pub async fn read_events(&self, run_id: &str, from_seq: Option<u64>) -> Result<Vec<Envelope>, EngineError> {
        let path = self.events_path(run_id);
        if !path.exists() {
            return Err(EngineError::RunNotFound(run_id.to_string()));
        }
        let events = read_events(&path).await?;
        Ok(match from_seq {
            Some(from) => events.into_iter().filter(|e| e.seq >= from).collect(),
            None => events,
        })
    }

    /// 从事件日志折叠出当前状态（磁盘是权威，不依赖内存镜像）。
    pub async fn snapshot(&self, run_id: &str) -> Result<RunState, EngineError> {
        let events = self.read_events(run_id, None).await?;
        RunState::from_events(&events)
    }

    pub async fn start_run(&self, spec: StartRun) -> Result<(), EngineError> {
        spec.definition
            .validate()
            .map_err(EngineError::InvalidDefinition)?;

        let mut log = EventLog::create(&self.data_dir, &spec.run_id).await?;
        let first = log
            .append(
                &spec.run_id,
                Event::RunStarted {
                    workflow_id: spec.workflow_id.clone(),
                    workflow_version: spec.workflow_version,
                    input: spec.input.clone(),
                },
            )
            .await?;

        let mut state = RunState::new();
        state.ensure_nodes(&spec.definition);
        state.fold(&first);

        self.spawn_driver(log, spec, state, RecoveryPlan::default());
        Ok(())
    }

    /// 崩溃恢复：读事件文件重建状态，重新执行残留的纯节点，副作用节点转人工裁决。
    pub async fn resume_run(&self, spec: StartRun) -> Result<ResumeOutcome, EngineError> {
        spec.definition
            .validate()
            .map_err(EngineError::InvalidDefinition)?;

        let path = self.events_path(&spec.run_id);
        if !path.exists() {
            return Err(EngineError::RunNotFound(spec.run_id));
        }
        let events = read_events(&path).await?;
        let mut state = RunState::from_events(&events)?;
        if state.workflow_id.as_deref() != Some(&spec.workflow_id)
            || state.workflow_version != Some(spec.workflow_version)
            || state.input != spec.input
        {
            return Err(EngineError::LogCorrupted("run_started 缺失或与 run 元数据不一致".into()));
        }
        if state.phase.is_terminal() {
            return Ok(ResumeOutcome::AlreadyTerminal(state.phase));
        }
        state.ensure_nodes(&spec.definition);

        let log = EventLog::open(&self.data_dir, &spec.run_id).await?;
        let plan = RecoveryPlan::classify(&spec.definition, &state);
        self.spawn_driver(log, spec, state, plan);
        Ok(ResumeOutcome::Resumed)
    }

    pub async fn cancel(&self, run_id: &str) -> bool {
        let handle = self.registry.lock().get(run_id).map(|h| h.cancel.clone());
        match handle {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    pub async fn signal(&self, run_id: &str, signal: Signal) -> Result<(), EngineError> {
        let sender = self
            .registry
            .lock()
            .get(run_id)
            .map(|h| h.signal_tx.clone())
            .ok_or_else(|| {
                EngineError::Node(format!("run {run_id} 当前不在运行中（已结束或未加载）"))
            })?;
        let (reply, received) = oneshot::channel();
        sender
            .send(SignalRequest { signal, reply })
            .await
            .map_err(|_| EngineError::Node(format!("run {run_id} 的引擎任务已退出")))?;
        received.await.map_err(|_| EngineError::Node(format!("run {run_id} 未能确认信号处理结果")))?
    }

    fn spawn_driver(&self, log: EventLog, spec: StartRun, state: RunState, plan: RecoveryPlan) {
        let (signal_tx, signal_rx) = mpsc::channel(SIGNAL_CHANNEL_CAPACITY);
        let cancel = CancellationToken::new();

        self.registry.lock().insert(
            spec.run_id.clone(),
            RunHandle {
                cancel: cancel.clone(),
                signal_tx,
            },
        );

        let driver = Driver {
            run_id: spec.run_id.clone(),
            definition: Arc::new(spec.definition),
            input: spec.input,
            log,
            state,
            events_tx: self.events_tx.clone(),
            observer: self.observer.clone(),
            cancel,
            registry: self.registry.clone(),
            inflight: HashMap::new(),
            human_waiting: HashMap::new(),
            adjudicating: HashSet::new(),
        };

        tokio::spawn(driver.run(signal_rx, plan));
    }
}

/// 恢复期对未完成节点和待重试节点的分类结论。
#[derive(Default)]
struct RecoveryPlan {
    /// 纯节点：安全重放
    replay: Vec<(String, u32)>,
    /// human_task：已有信号记录的补终态，没有的继续等待
    signal_received: Vec<(String, u32, Value)>,
    human_wait: Vec<(String, u32)>,
    /// 副作用节点：必须人工裁决
    adjudicate: Vec<String>,
    adjudication_received: Vec<Signal>,
    retries: Vec<String>,
}

impl RecoveryPlan {
    fn classify(definition: &Definition, state: &RunState) -> RecoveryPlan {
        let mut plan = RecoveryPlan::default();
        for (node_id, record) in &state.records {
            if matches!(record.state, NodeState::Failed { retryable: true, .. }) {
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
                    Some(payload) => plan
                        .signal_received
                        .push((node_id.clone(), attempt, payload.clone())),
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

struct Driver {
    run_id: String,
    definition: Arc<Definition>,
    input: Value,
    log: EventLog,
    state: RunState,
    events_tx: broadcast::Sender<Envelope>,
    observer: Arc<dyn RunObserver>,
    cancel: CancellationToken,
    registry: Arc<Mutex<HashMap<String, RunHandle>>>,
    inflight: HashMap<String, JoinHandle<()>>,
    /// human_task：等待外部信号的 oneshot
    human_waiting: HashMap<String, oneshot::Sender<Value>>,
    /// 崩溃遗留的副作用节点：等待人工裁决
    adjudicating: HashSet<String>,
}

impl Driver {
    async fn run(mut self, signal_rx: mpsc::Receiver<SignalRequest>, plan: RecoveryPlan) {
        let outcome = self.drive(signal_rx, plan).await;
        self.abort_inflight();
        match outcome {
            Ok(()) => {}
            Err(err) => {
                let message = err.to_string();
                let status = match self.append(Event::RunFailed { error: message.clone() }).await {
                    Ok(_) => DbRunStatus::Failed,
                    Err(append_err) => {
                        tracing::error!(run_id = %self.run_id, error = %append_err, "写入 run_failed 失败");
                        DbRunStatus::AwaitingResume
                    }
                };
                self.observer
                    .on_status(StatusUpdate {
                        run_id: &self.run_id,
                        status,
                        output: None,
                        error: Some(&message),
                    })
                    .await;
            }
        }
        self.registry.lock().remove(&self.run_id);
    }

    async fn drive(
        &mut self,
        mut signal_rx: mpsc::Receiver<SignalRequest>,
        plan: RecoveryPlan,
    ) -> Result<(), EngineError> {
        let (result_tx, mut result_rx) = mpsc::channel::<DriverMsg>(256);

        self.observer.on_status(StatusUpdate {
            run_id: &self.run_id,
            status: DbRunStatus::Running,
            output: None,
            error: None,
        }).await;
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
                for node_id in ready {
                    let attempt = self.next_attempt(&node_id);
                    self.start_node(&node_id, attempt, &result_tx).await?;
                }
            }

            // inflight 覆盖所有尚未消费的结果；human_waiting / adjudicating 表示外部输入未决
            if self.inflight.is_empty() && self.human_waiting.is_empty() && self.adjudicating.is_empty() {
                if self.state.all_terminal() {
                    return self.finalize().await;
                }
                return Err(EngineError::Node(
                    "调度停滞：存在既不可就绪也无法跳过的节点".into(),
                ));
            }

            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    self.abort_inflight();
                    self.append(Event::RunCancelled {}).await?;
                    self.observer.on_status(StatusUpdate {
                        run_id: &self.run_id,
                        status: DbRunStatus::Cancelled,
                        output: None,
                        error: None,
                    }).await;
                    return Ok(());
                }
                Some(msg) = result_rx.recv() => self.handle_result(msg, &result_tx).await?,
                Some(request) = signal_rx.recv() => {
                    let action = match self.signal_action(&request.signal) {
                        Ok(action) => action,
                        Err(err) => {
                            let _ = request.reply.send(Err(err));
                            continue;
                        }
                    };
                    let outcome = self.handle_signal(request.signal, action, &result_tx).await;
                    match outcome {
                        Ok(()) => { let _ = request.reply.send(Ok(())); }
                        Err(err) => {
                            let _ = request.reply.send(Err(EngineError::Node(err.to_string())));
                            return Err(err);
                        }
                    }
                },
            }
        }
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
            self.register_human_wait(&node_id, attempt, result_tx);
        }

        if !plan.adjudicate.is_empty() {
            for node_id in plan.adjudicate {
                tracing::warn!(run_id = %self.run_id, node_id = %node_id, "副作用节点状态不明，等待人工裁决");
                self.adjudicating.insert(node_id);
            }
            self.observer
                .on_status(StatusUpdate {
                    run_id: &self.run_id,
                    status: DbRunStatus::AwaitingResume,
                    output: None,
                    error: Some("存在状态不明的副作用节点，等待人工裁决"),
                })
                .await;
        }

        for (node_id, attempt) in plan.replay {
            tracing::warn!(run_id = %self.run_id, node_id = %node_id, attempt, "重启残留节点，重新执行");
            self.start_node(&node_id, attempt + 1, result_tx).await?;
        }

        for node_id in plan.retries {
            self.schedule_retry(&node_id, result_tx);
        }
        for signal in plan.adjudication_received {
            let action = self.signal_action(&signal)?;
            self.apply_signal(signal, action, result_tx).await?;
        }

        Ok(())
    }

    async fn append(&mut self, event: Event) -> Result<Envelope, EngineError> {
        let envelope = self.log.append(&self.run_id, event).await?;
        self.state.fold(&envelope);
        let _ = self.events_tx.send(envelope.clone());
        Ok(envelope)
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
                    let taken = self.state.outputs.get(from).map(exec::truthy).unwrap_or(false);
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
            NodeState::Failed { retryable: false, .. } => EdgeState::Unsatisfied("upstream_failed".into()),
            NodeState::Pending | NodeState::Running { .. }
            | NodeState::Failed { retryable: true, .. } => EdgeState::Waiting,
        }
    }

    async fn start_node(
        &mut self,
        node_id: &str,
        attempt: u32,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) -> Result<(), EngineError> {
        let node = self
            .definition
            .node(node_id)
            .ok_or_else(|| EngineError::Node(format!("节点不存在：{node_id}")))?
            .clone();
        let kind = node
            .kind()
            .ok_or_else(|| EngineError::Node(format!("节点类型未知：{}", node.node_type)))?;

        self.append(Event::NodeStarted {
            node_id: node_id.to_string(),
            attempt,
        })
        .await?;

        if kind == NodeType::HumanTask {
            self.register_human_wait(node_id, attempt, result_tx);
            return Ok(());
        }

        let preds: Vec<String> = self
            .definition
            .incoming(node_id)
            .iter()
            .map(|e| e.from.clone())
            .collect();
        let ctx = NodeExecContext {
            node,
            input: self.input.clone(),
            outputs: self.state.outputs.clone(),
            preds,
        };
        let cancel = self.cancel.clone();
        let result_tx = result_tx.clone();
        let nid = node_id.to_string();

        let handle = tokio::spawn(async move {
            let started = Instant::now();
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
    fn register_human_wait(
        &mut self,
        node_id: &str,
        attempt: u32,
        result_tx: &mpsc::Sender<DriverMsg>,
    ) {
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
                        let policy = self
                            .definition
                            .node(&node_id)
                            .map(|n| n.retry())
                            .unwrap_or_default();
                        let should_retry = failure.retryable && attempt < policy.max_attempts;
                        self.append(Event::NodeFailed {
                            node_id: node_id.clone(),
                            attempt,
                            error: failure.message.clone(),
                            retryable: should_retry,
                        })
                        .await?;

                        if should_retry {
                            self.schedule_retry(&node_id, result_tx);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn schedule_retry(&mut self, node_id: &str, result_tx: &mpsc::Sender<DriverMsg>) {
        let backoff = self.definition.node(node_id).map(|node| node.retry().backoff_ms).unwrap_or(0);
        let elapsed = self.state.record(node_id).ended_at
            .map(|ended| chrono::Utc::now().signed_duration_since(ended).num_milliseconds().max(0) as u64)
            .unwrap_or(0);
        let remaining = backoff.saturating_sub(elapsed);
        let result_tx = result_tx.clone();
        let nid = node_id.to_string();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(remaining)).await;
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
                .map_err(|err| EngineError::Node(format!("节点 {} 的裁决非法：{err}", signal.node_id)));
        }
        Err(EngineError::Node(format!("节点 {} 当前不等待信号", signal.node_id)))
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
        }).await?;
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
                    node_id: node_id.clone(), attempt, output, duration_ms: 0,
                }).await?;
            }
            SignalAction::Adjudicate(Adjudication::Failed { error }) => {
                self.append(Event::NodeFailed {
                    node_id: node_id.clone(), attempt,
                    error: error.unwrap_or_else(|| "人工判定为失败".into()), retryable: false,
                }).await?;
            }
        }
        self.adjudicating.remove(&node_id);
        if self.adjudicating.is_empty() {
            self.observer.on_status(StatusUpdate {
                run_id: &self.run_id, status: DbRunStatus::Running, output: None, error: None,
            }).await;
        }
        Ok(())
    }

    async fn finalize(&mut self) -> Result<(), EngineError> {
        if let Some(error) = self.state.fatal_error.clone() {
            self.append(Event::RunFailed {
                error: error.clone(),
            })
            .await?;
            self.observer
                .on_status(StatusUpdate {
                    run_id: &self.run_id,
                    status: DbRunStatus::Failed,
                    output: None,
                    error: Some(&error),
                })
                .await;
            return Ok(());
        }

        let output = self.collect_output();
        self.append(Event::RunCompleted {
            output: output.clone(),
        })
        .await?;
        self.observer
            .on_status(StatusUpdate {
                run_id: &self.run_id,
                status: DbRunStatus::Succeeded,
                output: Some(&output),
                error: None,
            })
            .await;
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
                    self.state.outputs.get(&n.id).cloned().unwrap_or(Value::Null),
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
