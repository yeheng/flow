//! 执行子进程池与可靠回收（二期 §3.1/§3.7/§3.9，I03）。
//!
//! 每进程同一时刻一个任务。通道承载按平台选择：unix 用单 socketpair
//! （argv 传 FD 原号，pre_exec 关闭其余继承 FD）；其余平台用本地 socket
//! 单连接回连（Windows named pipe）。父关闭子端、子关闭父端，兄弟进程
//! 不继承连接。通道故障 → 会话 Draining → 宽限后 kill → wait/reap；
//! 已取消/故障任务的进程统一回收重建，不直接复用。
//!
//! 职责边界：本模块只管「执行器进程的借还与生命周期」，不管节点语义、
//! 不管审计与结果的编排（那在 dispatch）。守护的核心不变量：
//! 1. X_max bounds worker processes, with a separate allowance for parked
//!    sessions. Startup and retirement keep their capacity until cleanup.
//! 2. 每个子进程必被 wait/reap——不留僵尸，也不把已取消/故障的进程
//!    放回空闲池复用（复用一个状态不明的进程等于复用未知事实）。
//!
//! 为什么需要它：进程隔离是 C2 的落点——用户代码（任意 JS / 任意出站
//! HTTP）跑在子进程里，主进程只做裁决与调度。池把「隔离」变成可运营的
//! 资源：有限并发、可靠回收、故障后重建。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::process::ManagedChild as Child;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};

use crate::execution_protocol::contract::{
    DRAIN_GRACE_MS, HEARTBEAT_TIMEOUT_MS, STARTUP_TIMEOUT_MS,
};
use crate::execution_protocol::message::Message;
use crate::execution_protocol::transport::{
    master_handshake_with_capabilities, FrameTransport, SendBacklog,
};

use super::{FromSession, IpcOptions, ToSession};

/// 执行器池的失败原因。
///
/// 三类失败对应三种不同的上游处置：spawn 失败 = 环境问题（重试无意义，
/// 报给用户）；握手失败 = 对端版本/协议不匹配（必须显式失败，不降级）；
/// 不可用 = 池已关闭（调用方应退出而不是重试）。
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// 执行器进程无法启动（找不到二进制、权限不足等）。
    #[error("executor spawn failed: {0}")]
    Spawn(String),
    /// 握手失败：超时、版本不兼容或对端拒绝。
    #[error("executor handshake failed: {0}")]
    Handshake(String),
    /// 池已关闭或无可用会话。
    #[error("executor session unavailable: {0}")]
    Unavailable(String),
}

/// 裸会话（无信号量份额）：spawn/握手完成即可用。
///
/// 「裸」指尚未占用 X_max 槽位——预热期的会话先进空闲池，取用时才
/// 附上份额，避免预热与懒加载抢出超过 X_max 的常驻进程。
pub struct RawSession {
    /// 派发方 → 会话的命令通道。
    pub to_session: mpsc::Sender<ToSession>,
    /// 会话 → 派发方的事件通道。
    pub events: mpsc::Receiver<FromSession>,
    /// 执行器标识（`exec-N`，池内单调递增）。
    pub executor_id: String,
    /// 执行器本次启动标识（由握手取得）：用于把审计归属到具体进程实例。
    pub boot_id: String,
    /// Capabilities advertised by this process during handshake.
    capabilities: Vec<String>,
    /// 会话标识（`session-N`）。
    pub session_id: String,
    /// 子进程 PID（便于诊断与外部排障）。
    pub child_pid: Option<u32>,
    lifetime: SessionLifetime,
}

struct SessionLifetime {
    generation: u64,
    retired: oneshot::Sender<OwnedSemaphorePermit>,
    _cancel: DropGuard,
}

enum WorkerSlot {
    Active(OwnedSemaphorePermit),
    Parked {
        permit: OwnedSemaphorePermit,
        _count: ParkedCount,
    },
}

struct ParkedCount(Arc<AtomicUsize>);

impl Drop for ParkedCount {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct LeaseCapacity {
    lifetime: Option<SessionLifetime>,
    slot: Option<WorkerSlot>,
}

impl Drop for LeaseCapacity {
    fn drop(&mut self) {
        let Some(lifetime) = self.lifetime.take() else {
            return;
        };
        let permit = match self.slot.take().expect("leased session has a slot") {
            WorkerSlot::Active(permit) | WorkerSlot::Parked { permit, .. } => permit,
        };
        // The driver retains the permit until the child has been reaped. If
        // it has already exited, the closed receiver returns the permit here.
        let _ = lifetime.retired.send(permit);
    }
}

impl RawSession {
    fn lease(self, permit: OwnedSemaphorePermit) -> SessionLease {
        SessionLease {
            to_session: self.to_session,
            events: self.events,
            executor_id: self.executor_id,
            boot_id: self.boot_id,
            capabilities: self.capabilities,
            session_id: self.session_id,
            child_pid: self.child_pid,
            capacity: LeaseCapacity {
                lifetime: Some(self.lifetime),
                slot: Some(WorkerSlot::Active(permit)),
            },
        }
    }
}

/// 一个可派发的会话租约：持有信号量份额与会话命令通道。
///
/// 健康会话放回空闲池后归还槽位。丢弃租约时，槽位由驱动任务持有到进程回收。
pub struct SessionLease {
    /// 派发方 → 会话的命令通道。
    pub to_session: mpsc::Sender<ToSession>,
    /// 会话 → 派发方的事件通道（派发期间由派发侧独占消费）。
    pub events: mpsc::Receiver<FromSession>,
    /// 执行器标识。
    pub executor_id: String,
    /// 执行器本次启动标识。
    pub boot_id: String,
    /// Capabilities advertised by this process during handshake.
    capabilities: Vec<String>,
    /// 会话标识。
    pub session_id: String,
    /// 子进程 PID。
    pub child_pid: Option<u32>,
    capacity: LeaseCapacity,
}

impl SessionLease {
    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|item| item == capability)
    }
}

/// 执行器进程池。
///
/// 内部由两部分组成：信号量（X_max 并发上限）与空闲会话栈（可复用的
/// 已握手进程）。两者都受 `shutdown` 令牌约束——关停后既不再放行新租约，
/// 也让已有的会话驱动任务退出并回收子进程。
pub struct ExecutorPool {
    /// 执行器召唤方式与槽位上限等静态选项。
    options: IpcOptions,
    /// journal 标识：握手里带给执行器，执行器据此确认属于同一份日志。
    journal_id: String,
    /// master 任期号：握手里带给执行器，用于识别「换了主进程」。
    master_epoch: u64,
    /// 执行槽位信号量（X_max 份额）。
    slots: Arc<Semaphore>,
    parked: Arc<Semaphore>,
    live_parked: Arc<AtomicUsize>,
    required_capabilities: Vec<String>,
    /// 已握手但当前空闲的会话（栈顶优先复用）。
    idle: Arc<Mutex<Vec<RawSession>>>,
    /// 执行器编号计数器。
    generation: AtomicU64,
    executor_counter: AtomicU64,
    /// 会话编号计数器。
    session_counter: AtomicU64,
    /// 关停令牌：通知所有会话驱动任务与等待者退出。
    shutdown: CancellationToken,
    /// 会话驱动任务集合：关停时统一等待其结束（确保子进程已被 reap）。
    tasks: TaskTracker,
}

impl ExecutorPool {
    /// 新建执行器池。
    ///
    /// 槽位数取 `x_max` 并夹到 1..=16：0 会让派发永远等不到份额，上限
    /// 过大会让并发进程数突破内存预算——两个方向的错误都比配置错误更难
    /// 排查，因此在这里就夹住。
    pub fn new(options: IpcOptions, journal_id: String, master_epoch: u64) -> Arc<Self> {
        Self::with_capabilities(
            options,
            journal_id,
            master_epoch,
            crate::execution_protocol::REQUIRED_CAPABILITIES
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }

    pub(crate) fn with_capabilities(
        options: IpcOptions,
        journal_id: String,
        master_epoch: u64,
        required_capabilities: Vec<String>,
    ) -> Arc<Self> {
        let x_max = options.x_max.clamp(1, 16);
        Arc::new(Self {
            options,
            journal_id,
            master_epoch,
            slots: Arc::new(Semaphore::new(x_max)),
            parked: Arc::new(Semaphore::new(crate::execution_protocol::A_MAX)),
            live_parked: Arc::new(AtomicUsize::new(0)),
            required_capabilities,
            idle: Arc::new(Mutex::new(Vec::new())),
            generation: AtomicU64::new(0),
            executor_counter: AtomicU64::new(0),
            session_counter: AtomicU64::new(0),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        })
    }

    /// 关停：回收全部会话（kill + wait/reap）。
    ///
    /// 顺序有讲究：先取消令牌（让所有会话驱动任务停止收发），再关闭信号量
    /// （让等待中的 acquire 立即失败而不是永远挂着），最后等待任务集合
    /// 收敛——只有等到会话驱动任务结束，才能保证每个子进程都被 reap。
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.slots.close();
        let idle = std::mem::take(&mut *self.idle.lock().await);
        for session in idle {
            let _ = session.to_session.try_send(ToSession::Recycle);
        }
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// 预热：预先 spawn `count` 个空闲会话放入复用池，省去首次派发的
    /// 进程启动 + 握手延迟。
    ///
    /// Existing idle workers and concurrent leases count against capacity.
    pub async fn prewarm(self: &Arc<Self>, count: usize) -> Result<usize, PoolError> {
        let mut warmed = 0;
        for _ in 0..count {
            let Ok(permit) = self.slots.clone().try_acquire_owned() else {
                break;
            };
            {
                let mut idle = self.idle.lock().await;
                idle.retain(|session| !session.to_session.is_closed());
                if idle.len() > self.slots.available_permits() {
                    break;
                }
            }
            let lease = self.spawn_lease(permit).await?;
            self.release(lease, true).await;
            warmed += 1;
        }
        Ok(warmed)
    }

    /// 获取一个执行会话：优先复用空闲会话，否则 spawn 新进程并完成握手。
    ///
    /// 先取信号量份额再取会话：份额是并发上限的守门人，先拿份额才能保证
    /// 「常驻进程数 ≤ X_max」——否则并发 acquire 会各自 spawn，进程数会
    /// 随并发数增长。
    pub async fn acquire(self: &Arc<Self>) -> Result<SessionLease, PoolError> {
        loop {
            let permit = tokio::select! {
                permit = self.slots.clone().acquire_owned() => permit.map_err(|_| PoolError::Unavailable("pool closed".into()))?,
                _ = self.shutdown.cancelled() => return Err(PoolError::Unavailable("pool closed".into())),
            };
            let session = {
                let mut idle = self.idle.lock().await;
                if self.shutdown.is_cancelled() {
                    return Err(PoolError::Unavailable("pool closed".into()));
                }
                idle.retain(|session| !session.to_session.is_closed());
                idle.pop()
            };
            let lease = match session {
                Some(session) => session.lease(permit),
                None => self.spawn_lease(permit).await?,
            };
            if self.current(&lease) {
                return Ok(lease);
            }
            // The lease transfers its capacity to the reaper before the next acquire.
            drop(lease);
        }
    }

    /// Old workers finish their current lease. A subsequent acquisition recycles
    /// stale idle workers through the same slot-accounted retirement path.
    pub fn rotate(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn current(&self, lease: &SessionLease) -> bool {
        lease
            .capacity
            .lifetime
            .as_ref()
            .is_some_and(|lifetime| lifetime.generation == self.generation.load(Ordering::Acquire))
    }

    pub fn parked_count(&self) -> usize {
        // Retiring workers still hold their physical slots, but no longer
        // count as parked tasks for the coordinator's admission decisions.
        self.live_parked.load(Ordering::Relaxed)
    }

    /// 审批挂起：交还租约的 X_max 份额（会话进程保留、绑定原派发，不进
    /// 空闲栈）。挂起总数受 [`A_MAX`] 约束，超限时保持占槽（降级而非失败）。
    ///
    /// 返回 true 表示确实交还了份额（调用方随后必须 [`Self::unpark`]）。
    pub fn park(&self, lease: &mut SessionLease) -> bool {
        if !matches!(lease.capacity.slot, Some(WorkerSlot::Active(_))) {
            return false;
        }
        let Ok(parked) = self.parked.clone().try_acquire_owned() else {
            return false;
        };
        self.live_parked.fetch_add(1, Ordering::Relaxed);
        lease.capacity.slot = Some(WorkerSlot::Parked {
            permit: parked,
            _count: ParkedCount(self.live_parked.clone()),
        });
        true
    }

    /// 结束挂起：重新取得一个 X_max 份额（可能等待其他派发释放）。
    ///
    /// # Errors
    /// 池已关停时返回 [`PoolError::Unavailable`]。
    pub async fn unpark(&self, lease: &mut SessionLease) -> Result<(), PoolError> {
        if matches!(lease.capacity.slot, Some(WorkerSlot::Active(_))) {
            return Ok(());
        }
        let permit = tokio::select! {
            permit = self.slots.clone().acquire_owned() => {
                permit.map_err(|_| PoolError::Unavailable("pool closed".into()))?
            }
            _ = self.shutdown.cancelled() => {
                return Err(PoolError::Unavailable("pool closed".into()))
            }
        };
        lease.capacity.slot = Some(WorkerSlot::Active(permit));
        Ok(())
    }

    /// 派发结束后归还会话（健康时复用；draining/故障时回收重建）。
    ///
    /// 复用只对「健康且未关停」的会话开放：被取消或出过故障的进程内部
    /// 状态不明（可能还挂着半个任务、半个审计窗口），复用它等于把未知
    /// 事实带进下一次派发。
    pub async fn release(self: &Arc<Self>, lease: SessionLease, healthy: bool) {
        if !healthy || !matches!(lease.capacity.slot, Some(WorkerSlot::Active(_))) {
            return;
        }
        let mut idle = self.idle.lock().await;
        if self.shutdown.is_cancelled() || lease.to_session.is_closed() || !self.current(&lease) {
            return;
        }
        let SessionLease {
            to_session,
            events,
            executor_id,
            boot_id,
            capabilities,
            session_id,
            child_pid,
            mut capacity,
        } = lease;
        idle.push(RawSession {
            to_session,
            events,
            executor_id,
            boot_id,
            capabilities,
            session_id,
            child_pid,
            lifetime: capacity
                .lifetime
                .take()
                .expect("leased session has a lifetime"),
        });
        drop(capacity);
    }

    // Startup owns the slot even if the caller times out or drops its future.
    async fn spawn_lease(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
    ) -> Result<SessionLease, PoolError> {
        let pool = self.clone();
        let shutdown = self.shutdown.child_token();
        let cancelled = shutdown.clone().drop_guard();
        let (tx, rx) = oneshot::channel();
        self.tasks.spawn(async move {
            let result = pool
                .spawn_session(shutdown)
                .await
                .map(|session| session.lease(permit));
            let _ = tx.send(result);
        });
        let lease = rx
            .await
            .map_err(|_| PoolError::Unavailable("worker startup stopped".into()))??;
        cancelled.disarm();
        Ok(lease)
    }

    async fn spawn_session(&self, shutdown: CancellationToken) -> Result<RawSession, PoolError> {
        if shutdown.is_cancelled() {
            return Err(PoolError::Unavailable("worker startup cancelled".into()));
        }
        let executor_id = format!(
            "exec-{}",
            self.executor_counter.fetch_add(1, Ordering::Relaxed)
        );
        let session_id = format!(
            "session-{}",
            self.session_counter.fetch_add(1, Ordering::Relaxed)
        );
        spawn_and_handshake(self, &executor_id, &session_id, shutdown).await
    }
}

/// spawn 执行器并完成握手；子进程由会话驱动任务持有并负责 kill/reap。
///
/// 握手失败必须显式失败并杀掉刚 spawn 的子进程：执行器缺失或版本不兼容
/// 都是部署问题，静默降级会让节点以错误的语义执行，而错误往往要到
/// 很久以后才被发现。
async fn spawn_and_handshake(
    pool: &ExecutorPool,
    executor_id: &str,
    session_id: &str,
    shutdown: CancellationToken,
) -> Result<RawSession, PoolError> {
    let generation = pool.generation.load(Ordering::Acquire);
    let (mut transport, mut child, session_token) =
        crate::execution_protocol::bootstrap::spawn_executor_transport(
            &pool.options.executor,
            pool.options.tag.as_deref(),
            &[],
            128,
        )
        .await
        .map_err(|e| PoolError::Spawn(e.to_string()))?;
    let child_pid = child.id();
    // 握手时限 = 2 × STARTUP_TIMEOUT_MS：一次握手包含「进程启动 + 双方
    // Hello/Welcome 往返」，给两倍窗口覆盖冷启动（首次读盘、动态链接）。
    // 超时同样按失败处理，不重试——反复 spawn 只会放大启动风暴。
    let handshake = tokio::select! {
        result=tokio::time::timeout(Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
            master_handshake_with_capabilities(&mut transport,&pool.journal_id,pool.master_epoch,executor_id,session_id,session_token.as_deref(),pool.required_capabilities.iter().map(String::as_str))) =>
            match result { Ok(Ok(hello))=>Ok(hello), Ok(Err(e))=>Err(e.to_string()), Err(_)=>Err("handshake timeout".into()) },
        _=shutdown.cancelled()=>Err("pool closed during handshake".into()),
    };
    let hello = match handshake {
        Ok(hello) => hello,
        Err(error) => {
            // 握手失败：先杀再 reap 再关传输——顺序保证不留僵尸进程，也不
            // 让失败的子进程继续持有继承来的通道端。
            let _ = child.start_kill();
            let _ = child.wait().await;
            transport.close();
            transport.join().await;
            return Err(PoolError::Handshake(error));
        }
    };
    let boot_id = hello.boot_id.clone();
    // 命令队列 64 / 事件队列 256：命令侧要吸收「一次派发的多条控制帧」
    // 与审计批，事件侧要吸收审计批与心跳；两个队列都是有界的，背压由
    // 发送方 await 承担，从而把「执行器停读」显式暴露成阻塞而非丢帧。
    let (to_session, command_rx) = mpsc::channel::<ToSession>(64);
    let (event_tx, events) = mpsc::channel::<FromSession>(256);
    let (retired, retain_slot) = oneshot::channel();
    let driver = session_driver(
        transport,
        child,
        command_rx,
        event_tx,
        executor_id.to_string(),
        shutdown.clone(),
    );
    pool.tasks.spawn(async move {
        driver.await;
        drop(retain_slot);
    });
    Ok(RawSession {
        to_session,
        events,
        executor_id: executor_id.to_string(),
        boot_id,
        capabilities: hello.capabilities,
        session_id: session_id.to_string(),
        child_pid,
        lifetime: SessionLifetime {
            generation,
            retired,
            _cancel: shutdown.drop_guard(),
        },
    })
}

/// 会话驱动：独占 transport 与子进程句柄；命令发送、事件转发、死亡回收。
///
/// 它是执行器进程的唯一持有者：谁杀掉它，谁就得负责 reap 子进程。所有
/// 出站帧在这里汇聚成一条序列，因此「谁先谁后」在这个任务里是可判定的。
async fn session_driver(
    mut transport: FrameTransport,
    mut child: Child,
    mut command_rx: mpsc::Receiver<ToSession>,
    event_tx: mpsc::Sender<FromSession>,
    executor_id: String,
    shutdown: CancellationToken,
) {
    let mut draining = false;
    // last_seen 只记入站：出站成功不代表对端还活着（数据可能卡在内核
    // 缓冲区里）。心跳超时的判据必须来自对端方向。
    let mut last_seen = tokio::time::Instant::now();
    let timeout = Duration::from_millis(HEARTBEAT_TIMEOUT_MS);
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + timeout, timeout);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // 出站积压（T03）：执行器停读/队列满时暂存重试，不让阻塞中的 send
    // 卡住会话循环——心跳超时与入站 Stopped/审计始终可被处理。
    // 容量 8：够放下一次派发的主要控制帧，又不至于把背压拖到不可察觉。
    let mut send_backlog = SendBacklog::with_capacity(8);
    loop {
        if send_backlog.flush(&transport).is_err() {
            break;
        }
        tokio::select! {
            _=shutdown.cancelled()=>break,
            command = command_rx.recv(), if send_backlog.is_empty() => {
                // 积压非空时不拉新命令：既保住帧的提交序，又让背压自然
                // 传导到派发方（await send 挂起），而不是丢帧。
                match command {
                    Some(ToSession::Send(message)) => {
                        if send_backlog.push(&transport, *message).is_err() {
                            break;
                        }
                    }
                    Some(ToSession::CancelDispatch(dispatch_id)) => {
                        let _ = send_backlog.push(
                            &transport,
                            Message::Cancel {
                                command_id: format!("cancel-{dispatch_id}"),
                                dispatch_id,
                            },
                        );
                        draining = true;
                    }
                    Some(ToSession::Recycle) | None => {
                        transport.close();
                        break;
                    }
                }
            }
            received = transport.recv() => {
                match received {
                    Ok(message) => {
                        last_seen = tokio::time::Instant::now();
                        // 心跳也作为事件转发：派发侧的空闲计时把它算作活跃
                        // 证据（长任务执行期间唯一持续的流量），handle()
                        // 对 Heartbeat 是 no-op。空闲会话（无租约）时接收端
                        // 无人排空，心跳用 try_send——满则丢弃这一拍，绝不
                        // 阻塞会话驱动；其余消息仍是 awaited 正确性路径。
                        let heartbeat = matches!(message, Message::Heartbeat { .. });
                        let delivered = if heartbeat {
                            event_tx
                                .try_send(FromSession::Incoming(Box::new(message)))
                                .is_ok()
                        } else {
                            tokio::select! {
                                result=event_tx.send(FromSession::Incoming(Box::new(message)))=>result.is_ok(),
                                _=shutdown.cancelled()=>break,
                            }
                        };
                        if !delivered && !heartbeat {
                            // 派发端已离开：保留会话等待下一个租约或回收。
                        }
                    }
                    Err(error) => {
                        tokio::select! { _=event_tx.send(FromSession::Dead(format!("channel failed: {error}")))=>{}, _=shutdown.cancelled()=>{} }
                        break;
                    }
                }
            }
            _ = heartbeat.tick() => {
                // draining 期间不判心跳超时：此时执行器可能正在封口窗口，
                // 迟迟不回心跳是正常的，不能把它误杀成「会话死亡」。
                if last_seen.elapsed() > timeout && !draining {
                    tokio::select! { _=event_tx.send(FromSession::Dead("heartbeat timeout".into()))=>{}, _=shutdown.cancelled()=>{} }
                    break;
                }
            }
        }
    }
    // 回收：宽限期收尾（尽力 Stopped/迟到审计）→ kill → wait/reap。
    // No dispatch owns this event stream after coordinator shutdown. There is
    // nobody left to persist late audit, so waiting for idle heartbeats cannot
    // improve durability; close the channel and reap immediately.
    if event_tx.is_closed() {
        transport.close();
    }
    let grace = Duration::from_millis(DRAIN_GRACE_MS);
    // 宽限期（5s）内尽力把迟到的审计/Stopped 转给派发方，让派发侧拿到
    // 最后一批证据；到期无条件 kill——宽限期是「尽力」而非「保证」，不能
    // 因为对端不吭声而无限期占着槽位。
    let _ = tokio::time::timeout(grace, async {
        while let Ok(message) = transport.recv().await {
            if event_tx
                .send(FromSession::Incoming(Box::new(message)))
                .await
                .is_err()
            {
                break;
            }
        }
    })
    .await;
    let _ = child.start_kill();
    // wait 必须在 kill 之后：先 reap 才能回收僵尸；顺序反过来会先挂住等
    // 一个不会自己退出的子进程。
    let status = child.wait().await; // wait/reap：不留僵尸
    if let Ok(status) = status {
        tracing::debug!(%executor_id, %status, "executor reaped");
    }
    tracing::debug!(%executor_id, "executor session recycled");
    transport.close();
    transport.join().await;
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    fn single_slot_pool() -> Arc<ExecutorPool> {
        ExecutorPool::new(
            IpcOptions {
                executor: crate::execution_protocol::contract::ExecutorInvocation::explicit(
                    "unused",
                ),
                x_max: 1,
                tag: None,
            },
            "j".into(),
            1,
        )
    }

    fn fixture_session(
        commands: mpsc::Sender<ToSession>,
        events: mpsc::Receiver<FromSession>,
        cancel: CancellationToken,
    ) -> (RawSession, oneshot::Receiver<OwnedSemaphorePermit>) {
        let (retired, retain_slot) = oneshot::channel();
        (
            RawSession {
                to_session: commands,
                events,
                executor_id: "e".into(),
                boot_id: "b".into(),
                capabilities: Vec::new(),
                session_id: "s".into(),
                child_pid: None,
                lifetime: SessionLifetime {
                    generation: 0,
                    retired,
                    _cancel: cancel.drop_guard(),
                },
            },
            retain_slot,
        )
    }

    #[tokio::test]
    async fn rotation_keeps_an_active_worker_until_its_lease_finishes() {
        let pool = single_slot_pool();
        let cancel = CancellationToken::new();
        let (commands, _command_rx) = mpsc::channel(1);
        let (_event_tx, events) = mpsc::channel(1);
        let (session, retained) = fixture_session(commands, events, cancel.clone());
        let lease = session.lease(pool.slots.clone().acquire_owned().await.unwrap());

        pool.rotate();
        assert!(!cancel.is_cancelled());
        pool.release(lease, true).await;
        assert!(cancel.is_cancelled());
        assert!(pool.idle.lock().await.is_empty());

        let permit = retained.await.unwrap();
        assert_eq!(pool.slots.available_permits(), 0);
        drop(permit);
        assert_eq!(pool.slots.available_permits(), 1);
    }

    /// 归还不健康的租约、或干脆丢弃租约，都不能阻塞在满掉的命令队列上——
    /// 否则一次故障就能让整个池卡死。
    #[tokio::test]
    async fn unhealthy_release_and_abandoned_lease_do_not_wait_for_command_capacity() {
        let pool = single_slot_pool();
        for abandon in [false, true] {
            let cancel = CancellationToken::new();
            let (commands, _held_receiver) = mpsc::channel(1);
            commands.try_send(ToSession::Recycle).unwrap();
            let (_, events) = mpsc::channel(1);
            let (session, retain_slot) = fixture_session(commands, events, cancel.clone());
            let lease = session.lease(pool.slots.clone().acquire_owned().await.unwrap());
            if abandon {
                drop(lease)
            } else {
                tokio::time::timeout(Duration::from_millis(100), pool.release(lease, false))
                    .await
                    .expect("release cannot wait behind a full command queue");
            }
            assert!(cancel.is_cancelled());
            assert!(
                pool.slots.clone().try_acquire_owned().is_err(),
                "the process still being reaped must hold its slot"
            );
            drop(retain_slot);
            assert_eq!(pool.slots.available_permits(), 1);
        }
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn warm_idle_session_counts_against_prewarm_capacity() {
        let pool = single_slot_pool();
        let (commands, _command_rx) = mpsc::channel(1);
        let (_event_tx, events) = mpsc::channel(1);
        let (session, _retain_slot) =
            fixture_session(commands, events, pool.shutdown.child_token());
        pool.idle.lock().await.push(session);
        assert_eq!(pool.prewarm(1).await.unwrap(), 0);
        let lease = pool.acquire().await.unwrap();
        assert_eq!(lease.session_id, "s");
        pool.release(lease, false).await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn parked_retirement_keeps_physical_quota_without_counting_as_a_parked_task() {
        let pool = single_slot_pool();
        let (commands, _command_rx) = mpsc::channel(1);
        let (_event_tx, events) = mpsc::channel(1);
        let (session, retain_slot) = fixture_session(commands, events, pool.shutdown.child_token());
        let mut lease = session.lease(pool.slots.clone().acquire_owned().await.unwrap());
        let available = pool.parked.available_permits();
        assert!(pool.park(&mut lease));
        assert_eq!(pool.parked_count(), 1);
        assert_eq!(pool.slots.available_permits(), 1);

        pool.release(lease, true).await;
        assert!(pool.idle.lock().await.is_empty());
        assert_eq!(pool.parked_count(), 0);
        assert_eq!(pool.parked.available_permits(), available - 1);
        drop(retain_slot);
        assert_eq!(pool.parked.available_permits(), available);
        pool.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn abandoned_handshake_is_reaped_before_shutdown_returns() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("worker");
        let pid_file = dir.path().join("worker.pid");
        // Report startup, then keep the IPC endpoint open without a Hello.
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$0.pid\"\nexec /bin/sleep 60\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let pool = ExecutorPool::new(
            IpcOptions {
                executor: crate::execution_protocol::ExecutorInvocation::explicit(script),
                x_max: 1,
                tag: None,
            },
            "fixture".into(),
            1,
        );
        let starting_pool = pool.clone();
        let starting = tokio::spawn(async move { starting_pool.acquire().await });
        let pid: libc::pid_t = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(pid) = std::fs::read_to_string(&pid_file)
                    .ok()
                    .and_then(|value| value.trim().parse().ok())
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker spawned");

        starting.abort();
        assert!(matches!(starting.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), pool.shutdown())
            .await
            .expect("shutdown waits for the abandoned handshake");
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    }
    /// 关停必须打断被填满的事件队列，并保证子进程被回收（不留僵尸）。
    #[cfg(unix)]
    #[tokio::test]
    async fn session_shutdown_interrupts_full_event_queue_and_reaps_child() {
        let child =
            Child::spawn_executor(tokio::process::Command::new("/bin/sleep").arg("60")).unwrap();
        let pid = child.id().unwrap();
        let (a, b) = tokio::io::duplex(4096);
        let transport = FrameTransport::spawn(a, 8);
        let mut peer = FrameTransport::spawn(b, 8);
        let (_commands, command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let mut task = tokio::spawn(session_driver(
            transport,
            child,
            command_rx,
            event_tx,
            "fixture".into(),
            cancel.clone(),
        ));
        peer.send(Message::Reject {
            reason: "first".into(),
        })
        .await
        .unwrap();
        peer.send(Message::Reject {
            reason: "second".into(),
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while event_rx.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(DRAIN_GRACE_MS + 1000), &mut task)
            .await
            .expect("session must be reaped despite an abandoned runner")
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(pid as i32, 0) },
            -1,
            "child remains after shutdown"
        );
        peer.close();
        peer.join().await;
    }
}
