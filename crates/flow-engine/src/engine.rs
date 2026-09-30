use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Driver 任务已结束（清理任务随即摘除注册位）。注册位残留的微秒窗口内
    /// signal/cancel/is_live 据此拒绝误报 live——契约：不在跑必须回 -32012 家族
    exited: Arc<AtomicBool>,
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

    /// 节点日志：只写不 fsync（进程级持久）。同文件后续严格事件的组提交
    /// 会顺带刷盘；run 终态事件写入前 EventLog 内建兑底 sync。
    fn append_log<'a>(&'a mut self, event: Event) -> BoxFuture<'a, Result<Envelope, EngineError>> {
        Box::pin(async move { self.log.append_log(&self.run_id, event).await })
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

/// 广播通道容量：日志事件进同一广播（订阅回放 + 实时追流共用），容量必须
/// 跟随每 run 日志预算核算，否则刷屏 run 会把所有订阅者打进 Lagged。
/// 公式见可观察性设计 §4：capacity = max(基础容量, 日志预算)。
fn event_channel_capacity() -> usize {
    EVENT_CHANNEL_CAPACITY.max(crate::nodelog::budget_from_env())
}

impl Engine {
    pub fn new(data_dir: impl Into<PathBuf>, observer: Arc<dyn RunObserver>) -> Engine {
        let (events_tx, _) = broadcast::channel(event_channel_capacity());
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
        self.registry
            .lock()
            .get(run_id)
            .is_some_and(|h| !h.exited.load(Ordering::Acquire))
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
        state.fold(&first.clone());
        // run_started 写于 Driver 存在之前，但全局订阅者必须和其他事件一样看到它：
        // Postgres 后端经共享日志 + NOTIFY 扇出（任何事件都走扇出），单机后端只有
        // 在这里显式广播，两条订阅面才一致——漏掉的话全局流会从 node_started 才开始。
        // 无订阅者时 send 返回 Err，忽略即可。
        let _ = self.events_tx.send(first);

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
        // 定义外节点 = 日志损坏：必须在补 Pending / 分类之前查。幽灵记录停在
        // 非终态时终止判定永远不成立，run 会永久卡在 awaiting_resume（fold.rs
        // `validate_nodes_in_definition` 的完整论证）。
        state.validate_nodes_in_definition(&spec.definition)?;
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
        let handle = self
            .registry
            .lock()
            .get(run_id)
            .map(|h| (h.cancel.clone(), h.exited.clone()));
        match handle {
            Some((token, exited)) => {
                if exited.load(Ordering::Acquire) {
                    // Driver 已退出、注册位尚未摘除：不谎报「已交付取消」
                    return false;
                }
                token.cancel();
                true
            }
            None => false,
        }
    }

    pub async fn signal(&self, run_id: &str, signal: Signal) -> Result<(), EngineError> {
        let handle = self
            .registry
            .lock()
            .get(run_id)
            .map(|h| (h.signal_tx.clone(), h.exited.clone()))
            .ok_or_else(|| {
                EngineError::NotLive(format!("run {run_id} 当前不在运行中（已结束或未加载）"))
            })?;
        let (sender, exited) = handle;
        if exited.load(Ordering::Acquire) {
            return Err(EngineError::NotLive(format!(
                "run {run_id} 当前不在运行中（引擎任务已退出）"
            )));
        }
        let (reply, received) = oneshot::channel();
        // Driver 在发送与回执之间退出属于「不在跑」而非内部错误（契约 -32012），
        // 信号可能已提交但无法确认——由调用方按 NotLive 语义处理
        sender
            .send(SignalRequest { signal, reply })
            .await
            .map_err(|_| {
                EngineError::NotLive(format!("run {run_id} 当前不在运行中（引擎任务已退出）"))
            })?;
        received.await.map_err(|_| {
            EngineError::NotLive(format!(
                "run {run_id} 未能确认信号处理结果（引擎任务已退出）"
            ))
        })?
    }

    /// 原子占位 run_id：同一 run 至多一个本地 Driver。
    /// 占位后要么交给 spawn_driver（移交清理责任），要么随 RunReservation drop 自动释放。
    fn reserve_run(&self, run_id: &str) -> Result<RunReservation<'_>, EngineError> {
        let mut registry = self.registry.lock();
        // 「已占用」= 有注册位**且**它还没退出，与 [`Self::is_live`] 用同一条判据。
        //
        // 为什么要一致：清理任务分两步（先置 `exited` 再摘 key），两步之间
        // `is_live` 已报 false 而 `contains_key` 仍为真。若这里只看 `contains_key`，
        // `resume_run` 会在该窗口把恢复判成 `RunExists` 并**无声当作已恢复**返回
        // （`Err(RunExists) => Ok(Resumed)`），run 再无 Driver 驱动。
        //
        // 实测（`probe_window`）：driver 任务与其清理任务是背靠背被调度的，该窗口
        // 窄到单步内闭合，构造不出可观测的假恢复——所以这是**潜在**不一致，不是
        // 已知在线故障。修它的理由是「两个函数对『活着』不该有不同答案」这条不变量，
        // 以及 `RunExists => Resumed` 那条静默映射在别处也没有出口。
        if registry
            .get(run_id)
            .is_some_and(|h| !h.exited.load(Ordering::Acquire))
        {
            return Err(EngineError::RunExists(run_id.to_string()));
        }
        // 上一任已退出但注册位尚未摘除：就地覆盖。清理任务摘除前会核对
        // `Arc::ptr_eq(&h.exited, &exited)`，认得出这不是自己的注册位，
        // 不会误删新 Driver 的活跃凭据。
        let (signal_tx, signal_rx) = mpsc::channel(SIGNAL_CHANNEL_CAPACITY);
        let cancel = CancellationToken::new();
        let exited = Arc::new(AtomicBool::new(false));
        registry.insert(
            run_id.to_string(),
            RunHandle {
                cancel: cancel.clone(),
                signal_tx,
                exited: exited.clone(),
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
        // 身份令牌：本次占位的 exited Arc。清理时据此确认「注册位里那个
        // exited 还是我留下的这个」才摘除——若已被新一轮 reserve_run 覆盖，
        // 旧任务不得摘掉新 Driver 的注册位（那是另一个 run 的活跃凭据）。
        let exited = registry
            .lock()
            .get(&run_id)
            .map(|h| h.exited.clone())
            .expect("reservation.disarm 前注册位必在");
        tokio::spawn(async move {
            let _ = handle.await;
            // 先置墓碑再摘除：is_live / reserve_run 都以 exited 为准，
            // 两步之间它们对「活着的 run」给出一致答案。
            exited.store(true, Ordering::Release);
            let mut map = registry.lock();
            if map
                .get(&run_id)
                .is_some_and(|h| Arc::ptr_eq(&h.exited, &exited))
            {
                map.remove(&run_id);
            }
        });
    }
}
