use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::backend::{CommitOutcome, PendingInput, RunEventSink};
use crate::driver::{spawn_driver, DriverSpec, RecoveryPlan, SignalRequest};
use crate::error::EngineError;
use crate::event::{read_events, run_dir, Envelope, Event, EventLog};
use crate::fold::RunState;
use crate::model::Definition;

const EVENT_CHANNEL_CAPACITY: usize = 1024;
const SIGNAL_CHANNEL_CAPACITY: usize = 64;
/// 单机后端没有持久 inbox；这个轮询间隔只是占位（poll_inputs 恒为空）。
const FILE_INBOX_POLL: Duration = Duration::from_secs(3600);

/// 落库用的 run 状态词汇表住在 flow-dto（单一来源），这里重导出保持
/// `flow_engine::DbRunStatus` 导入路径不变。
pub use flow_dto::DbRunStatus;

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
    /// 嵌套深度：根 run 为 0，sub_workflow 的子 run 为父深度 + 1。
    /// 恢复路径（resume_run）忽略此字段，深度以日志中的 run_started 为准。
    pub depth: u32,
}

#[derive(Debug, Clone)]
pub enum ResumeOutcome {
    Resumed,
    /// 事件日志已终结：携带折叠出的终态（phase/output/fatal_error），
    /// 消费方不必为回填 DB 再读一次日志。Box 控制 Resumed 变体的大小差。
    AlreadyTerminal(Box<RunState>),
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub node_id: String,
    /// human_task：任意负载，作为节点输出。
    /// 崩溃遗留的副作用节点：`{"action": "retry" | "succeeded" | "failed", "output": ..., "error": ...}`
    pub payload: Value,
}

struct RunHandle {
    cancel: CancellationToken,
    signal_tx: mpsc::Sender<SignalRequest>,
}

/// reserve_run 的占位。Drop 时自动释放注册位（失败路径零样板），
/// 成功派发时由 spawn_driver disarm，把注册位的清理责任移交给 Driver 退出任务。
struct RunReservation<'a> {
    engine: &'a Engine,
    run_id: String,
    cancel: CancellationToken,
    signal_rx: Option<mpsc::Receiver<SignalRequest>>,
    armed: bool,
}

impl RunReservation<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RunReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.engine.registry.lock().remove(&self.run_id);
        }
    }
}

/// 单机后端的事件出口：文件日志（每事件 fsync）+ RunObserver 状态投影。
/// 终态事件与状态投影这里不保证原子——单进程单写者下，观察者只是索引。
struct FileSink {
    run_id: String,
    log: EventLog,
    observer: Arc<dyn RunObserver>,
}

impl RunEventSink for FileSink {
    fn append<'a>(&'a mut self, event: Event) -> BoxFuture<'a, Result<Envelope, EngineError>> {
        Box::pin(async move { self.log.append(&self.run_id, event).await })
    }

    fn append_terminal<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, EngineError>> {
        Box::pin(async move {
            let envelope = self.log.append(&self.run_id, event).await?;
            let update = match &envelope.event {
                Event::RunCompleted { output } => StatusUpdate {
                    run_id: &self.run_id,
                    status: DbRunStatus::Succeeded,
                    output: Some(output),
                    error: None,
                },
                Event::RunFailed { error } => StatusUpdate {
                    run_id: &self.run_id,
                    status: DbRunStatus::Failed,
                    output: None,
                    error: Some(error),
                },
                Event::RunCancelled {} => StatusUpdate {
                    run_id: &self.run_id,
                    status: DbRunStatus::Cancelled,
                    output: None,
                    error: None,
                },
                other => {
                    return Err(EngineError::Node(format!(
                        "append_terminal 只接受终态事件，收到 {}",
                        other.kind()
                    )))
                }
            };
            self.observer.on_status(update).await;
            Ok(envelope)
        })
    }

    fn project_status<'a>(
        &'a mut self,
        status: DbRunStatus,
        error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            self.observer
                .on_status(StatusUpdate {
                    run_id: &self.run_id,
                    status,
                    output: None,
                    error,
                })
                .await;
            Ok(())
        })
    }

    fn poll_inputs<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<PendingInput>, EngineError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn commit_signal<'a>(
        &'a mut self,
        _input: &'a PendingInput,
        _event: Event,
    ) -> BoxFuture<'a, Result<CommitOutcome, EngineError>> {
        Box::pin(async {
            Err(EngineError::Node(
                "单机后端的信号经内存通道交付，不走持久 inbox".into(),
            ))
        })
    }

    fn reject_signal<'a>(
        &'a mut self,
        _input: &'a PendingInput,
        _reason: &'a str,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn consume_cancel<'a>(
        &'a mut self,
        _input: &'a PendingInput,
    ) -> BoxFuture<'a, Result<CommitOutcome, EngineError>> {
        Box::pin(async {
            Err(EngineError::Node(
                "单机后端的取消经 CancellationToken 交付，不走持久 inbox".into(),
            ))
        })
    }

    fn release<'a>(&'a mut self) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }
}

/// 工作流执行引擎（单机后端）。每次 run 一个 tokio 任务、一个 event.jsonl 单写者。
pub struct Engine {
    data_dir: PathBuf,
    observer: Arc<dyn RunObserver>,
    events_tx: broadcast::Sender<Envelope>,
    registry: Arc<Mutex<HashMap<String, RunHandle>>>,
    /// sub_workflow 启动器由 RPC 层在两阶段构造后注入（launcher 依赖 Engine 自身）
    child_launcher: Mutex<Option<Arc<dyn crate::child_run::ChildRunLauncher>>>,
}

impl Engine {
    pub fn new(data_dir: impl Into<PathBuf>, observer: Arc<dyn RunObserver>) -> Engine {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Engine {
            data_dir: data_dir.into(),
            observer,
            events_tx,
            registry: Arc::new(Mutex::new(HashMap::new())),
            child_launcher: Mutex::new(None),
        }
    }

    pub fn set_child_launcher(&self, launcher: Arc<dyn crate::child_run::ChildRunLauncher>) {
        *self.child_launcher.lock() = Some(launcher);
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

    pub async fn read_events(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
    ) -> Result<Vec<Envelope>, EngineError> {
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

        // 原子占位：同一 run_id 至多一个本地 Driver（event.jsonl 单写者不变量）。
        // 已被驱动时返回 RunExists，调用方按附着语义处理。
        let reservation = self.reserve_run(&spec.run_id)?;

        let mut log = EventLog::create(&self.data_dir, &spec.run_id).await?;
        let first = log
            .append(
                &spec.run_id,
                Event::RunStarted {
                    workflow_id: spec.workflow_id.clone(),
                    workflow_version: spec.workflow_version,
                    input: spec.input.clone(),
                    depth: spec.depth,
                },
            )
            .await?;

        let mut state = RunState::new();
        state.ensure_nodes(&spec.definition);
        state.fold(&first);

        self.spawn_driver(log, spec, state, RecoveryPlan::default(), reservation);
        Ok(())
    }

    /// 崩溃恢复：读事件文件重建状态，重新执行残留的纯节点，副作用节点转人工裁决。
    pub async fn resume_run(&self, spec: StartRun) -> Result<ResumeOutcome, EngineError> {
        spec.definition
            .validate()
            .map_err(EngineError::InvalidDefinition)?;

        // 原子占位：已在驱动中的 run 重复恢复是无操作，绝不允许第二个写者
        //（EventLog 各自计数 seq，双写者必然产生重复 seq 与重复副作用）。
        let reservation = match self.reserve_run(&spec.run_id) {
            Ok(reservation) => reservation,
            Err(EngineError::RunExists(_)) => return Ok(ResumeOutcome::Resumed),
            Err(err) => return Err(err),
        };

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
            return Err(EngineError::LogCorrupted(
                "run_started 缺失或与 run 元数据不一致".into(),
            ));
        }
        if state.phase.is_terminal() {
            return Ok(ResumeOutcome::AlreadyTerminal(Box::new(state)));
        }
        state.ensure_nodes(&spec.definition);

        let log = EventLog::open(&self.data_dir, &spec.run_id).await?;
        let plan = RecoveryPlan::classify(&spec.definition, &state);
        self.spawn_driver(log, spec, state, plan, reservation);
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
                EngineError::NotLive(format!("run {run_id} 当前不在运行中（已结束或未加载）"))
            })?;
        let (reply, received) = oneshot::channel();
        sender
            .send(SignalRequest { signal, reply })
            .await
            .map_err(|_| EngineError::Node(format!("run {run_id} 的引擎任务已退出")))?;
        received
            .await
            .map_err(|_| EngineError::Node(format!("run {run_id} 未能确认信号处理结果")))?
    }

    /// 原子占位 run_id：同一 run 至多一个本地 Driver。
    /// 占位后要么交给 spawn_driver（移交清理责任），要么随 RunReservation drop 自动释放。
    fn reserve_run(&self, run_id: &str) -> Result<RunReservation<'_>, EngineError> {
        let mut registry = self.registry.lock();
        if registry.contains_key(run_id) {
            return Err(EngineError::RunExists(run_id.to_string()));
        }
        let (signal_tx, signal_rx) = mpsc::channel(SIGNAL_CHANNEL_CAPACITY);
        let cancel = CancellationToken::new();
        registry.insert(
            run_id.to_string(),
            RunHandle {
                cancel: cancel.clone(),
                signal_tx,
            },
        );
        Ok(RunReservation {
            engine: self,
            run_id: run_id.to_string(),
            cancel,
            signal_rx: Some(signal_rx),
            armed: true,
        })
    }

    fn spawn_driver(
        &self,
        log: EventLog,
        spec: StartRun,
        state: RunState,
        plan: RecoveryPlan,
        mut reservation: RunReservation<'_>,
    ) {
        // 单机后端没有所有权转移；这个 token 不会触发
        let lost = CancellationToken::new();

        let sink: Box<dyn RunEventSink> = Box::new(FileSink {
            run_id: spec.run_id.clone(),
            log,
            observer: self.observer.clone(),
        });
        let handle = spawn_driver(
            DriverSpec {
                run_id: spec.run_id.clone(),
                definition: Arc::new(spec.definition),
                input: spec.input,
                // 深度以日志中的 run_started 为准（恢复路径 spec.depth 不可靠）
                depth: state.depth,
                child_launcher: self.child_launcher.lock().clone(),
                sink,
                events_tx: Some(self.events_tx.clone()),
                cancel: reservation.cancel.clone(),
                ownership_lost: lost,
                signal_rx: reservation.signal_rx.take(),
                inbox_poll: FILE_INBOX_POLL,
            },
            state,
            plan,
        );
        // 注册位移交：Driver 完全退出（含 LeaseLost 静默退出）后由退出任务清理
        reservation.disarm();
        let registry = self.registry.clone();
        let run_id = spec.run_id.clone();
        tokio::spawn(async move {
            let _ = handle.await;
            registry.lock().remove(&run_id);
        });
    }
}
