//! 唯一写者：把事务排队、组提交进 SQLite，并把回执送回提交方。
//!
//! 本模块拥有全部写连接——只有专用泵线程可以写库，其他线程只能提交请求并
//! 等待回执。这条边界让「提交序号」和「写顺序」有唯一解释。
//! 核心不变量：一批事务要么整体提交、要么整体回滚，绝不确认半套数据；事实、
//! 当前状态与回执在同一个 SQLite 事务里生效；平台故障必须冻结写者，而不是
//! 在状态可疑的权威上继续写。
//! 需要它的原因：协调器并发产生状态迁移，而权威库只能有一个写者；组提交
//! 既提供吞吐，又必须让每个提交方看到「全部生效或全部不生效」。
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

use crate::codec::{self, Event, EventKind, Transaction};
use crate::sqlite::{Disk, EntityChange};
use crate::storage::{FaultInjection, Location};
use crate::value::{PreparedValue, StagedValue};
use crate::{invalid, Error, Result, MAX_LINE_BYTES};

/// 写者的资源预算。
///
/// 这些是上限而不是目标：每个预算都对应一种过载来源（字节预算对应载荷体积，
/// 队列长度对应请求数量，聚合等待对应延迟）。字节预算必须至少容纳一条最大
/// 事务，否则合法事务永远无法提交。
#[derive(Debug, Clone)]
pub struct JournalOptions {
    pub batch_bytes: usize,
    pub control_bytes: usize,
    pub audit_bytes: usize,
    pub queue_requests: usize,
    pub group_wait: Duration,
    pub faults: FaultInjection,
}
impl Default for JournalOptions {
    /// 默认预算：控制面 4 MiB、审计面 16 MiB、单批 4 MiB、队列 1024 请求、
    /// 聚合等待 5 ms。审计面额度更大是因为值分块体积远大于命令事件。
    fn default() -> Self {
        Self {
            batch_bytes: 4 * 1024 * 1024,
            control_bytes: 4 * 1024 * 1024,
            audit_bytes: 16 * 1024 * 1024,
            queue_requests: 1024,
            group_wait: Duration::from_millis(5),
            faults: FaultInjection::default(),
        }
    }
}

/// 提交队列分类。
///
/// 控制面与审计面分开计量与排队：控制面承载业务状态迁移，延迟敏感；审计面
/// 承载大值分块等审计事实，体积大但可以等待。两类共享同一条写序，因此不
/// 存在「谁先谁后」的顺序歧义。
#[derive(Debug, Clone)]
pub enum QueueClass {
    /// 控制面：业务状态迁移与命令回执。
    Control,
    /// 审计面：按 dispatch/output 身份分桶，同桶内保持提交顺序。
    Audit(String),
}

/// 一次成功提交的回执。
///
/// 提交方拿到的不是「写完了」而是「写在哪」：`location` 指出事实的提交序号
/// 与字节数，`transaction` 是已分配最终 LSN 的事实，`value_prefix` 是紧随其前
/// 的值分块事务（字节正文不在其中，正文存在值存储里）。
#[derive(Debug, Clone)]
pub struct Commit {
    pub transaction: Arc<Transaction>,
    pub location: Location,
    /// Dataset chunk transactions preceding the command; byte payloads omitted.
    pub value_prefix: Vec<Transaction>,
}

/// 写者的可观测快照。
///
/// 用于健康检查与基准：区分「在飞」（正在提交的一批）与「排队」（已受理未
/// 提交），并暴露组提交比（transactions / data_syncs）与 fsync 延迟分布，
/// 以便判断权威是否退化成每事务一次 fsync。
#[derive(Debug, Clone, Default, Serialize)]
pub struct WriterStats {
    pub in_flight: bool,
    pub queued_requests: usize,
    pub queued_bytes: usize,
    pub transactions: u64,
    pub events: u64,
    pub encoded_bytes: u64,
    pub batches: u64,
    pub data_syncs: u64,
    pub directory_syncs: u64,
    pub data_sync_latency: crate::storage::SyncLatency,
    pub directory_sync_latency: crate::storage::SyncLatency,
    pub max_batch_transactions: usize,
    pub max_batch_bytes: usize,
    pub peak_queued_bytes: usize,
    pub peak_queued_requests: usize,
    pub durable_lsn: u64,
    pub failure: Option<String>,
}

/// 队列中一条待提交请求。
///
/// `_bytes` / `_slot` 是信号量许可：随请求一起排队、被弹出时释放，保证队列
/// 长度与字节量同时受背压约束，不会因为「已编码但未提交」而超发。
struct Pending {
    tx: Transaction,
    values: Vec<StagedValue>,
    expected_lsn: Option<u64>,
    /// Canonical size/digest when the LSN was known at submission time.
    wire: Option<(u64, String)>,
    changes: Vec<EntityChange>,
    bytes: usize,
    ack: oneshot::Sender<std::result::Result<Commit, String>>,
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}
/// 写者的排队结构。
///
/// 控制面是单队列；审计面按 dispatch 身份分桶，并用轮转表公平调度，避免
/// 一个大值流饿死其他审计提交。
#[derive(Default)]
struct Queue {
    control: VecDeque<Pending>,
    audit: BTreeMap<String, VecDeque<Pending>>,
    round: VecDeque<String>,
    control_turn: bool,
    closing: bool,
    in_flight: bool,
    bytes: usize,
    requests: usize,
    stats: WriterStats,
}

impl Queue {
    /// 预看队首请求的字节数，用于组批时判断「再放一条会不会超预算」。
    ///
    /// 只读不改状态：真正的弹出由 `pop` 决定，预看不能改变轮转顺序。
    fn next_bytes(&self) -> Option<usize> {
        if !self.control.is_empty() && (self.control_turn || self.round.is_empty()) {
            self.control.front().map(|p| p.bytes)
        } else {
            self.round
                .front()
                .and_then(|key| self.audit.get(key)?.front())
                .map(|p| p.bytes)
        }
    }
    /// 弹出队首请求并更新字节/请求计数。
    ///
    /// 控制面与审计面交替轮转：每个审计桶出队一次后重新排到轮转表尾部，
    /// 保证大流量审计桶不会独占写序。
    fn pop(&mut self) -> Option<Pending> {
        let control = !self.control.is_empty() && (self.control_turn || self.round.is_empty());
        self.control_turn = !control;
        let pending = if control {
            self.control.pop_front()
        } else {
            self.round.pop_front().and_then(|key| {
                let queue = self.audit.get_mut(&key).expect("round robin membership");
                let item = queue.pop_front();
                if queue.is_empty() {
                    self.audit.remove(&key);
                } else {
                    self.round.push_back(key);
                }
                item
            })
        };
        if let Some(p) = &pending {
            self.bytes -= p.bytes;
            self.requests -= 1;
        }
        pending
    }
}
/// 泵线程与提交方之间的共享状态。
///
/// 用普通线程原语而非 channel：提交方需要「按类排队 + 轮转 + 字节记账」的
/// 结构，channel 表达不了；条件变量负责唤醒泵线程。
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
}
/// 写者的进程内状态。
///
/// 字节与槽位配额用信号量表达：排队方在入队前就拿到许可，避免「先入队
/// 再发现超限」导致队列无限增长。
struct Inner {
    id: String,
    root: PathBuf,
    shared: Arc<Shared>,
    control: Arc<Semaphore>,
    staging: Arc<Semaphore>,
    audit: Arc<Semaphore>,
    control_slots: Arc<Semaphore>,
    audit_slots: Arc<Semaphore>,
    control_budget: usize,
    audit_budget: usize,
    batch_budget: usize,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Drop for Inner {
    /// 关闭写者时先标记队列关闭并唤醒泵线程，再 join 泵线程。
    ///
    /// 顺序不能反：先置 `closing` 才能让泵线程从等待中醒来并正常退出，
    /// 否则泵线程会永远阻塞在条件变量上，join 变成死等。
    fn drop(&mut self) {
        lock_queue(&self.shared).closing = true;
        self.shared.ready.notify_one();
        if let Some(handle) = self
            .thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

/// Cloneable submission handle. Only the dedicated thread owns journal write descriptors.
///
/// 可克隆的提交句柄：克隆只复制提交能力，不复制写权限——写描述符始终由
/// 专用泵线程独占。
#[derive(Clone)]
pub struct Journal {
    inner: Arc<Inner>,
}
impl Journal {
    /// 打开（或创建）数据目录下的唯一写者。
    ///
    /// 会取得数据目录的排他文件锁并启动专用泵线程；已有写者在运行时直接
    /// 失败（`Conflict`），因为两个写者无法对同一份权威给出两个提交序。
    ///
    /// # Errors
    /// 预算不合法、目录被锁、存储格式不支持时返回对应错误。
    pub async fn open(root: impl AsRef<Path>, options: JournalOptions) -> Result<Self> {
        // 每个字节预算都必须至少能容纳一条最大事务，否则合法事务永远无法
        // 入队；group_wait 是延迟承诺，超过 5 ms 就不再满足组提交语义。
        if options.batch_bytes < MAX_LINE_BYTES
            || options.control_bytes < MAX_LINE_BYTES
            || options.audit_bytes < MAX_LINE_BYTES
            || options.control_bytes > u32::MAX as usize
            || options.audit_bytes > u32::MAX as usize
            || options.queue_requests == 0
            || options.group_wait > Duration::from_millis(5)
        {
            return Err(invalid("invalid journal resource options"));
        }
        let root = root.as_ref().to_path_buf();
        let disk_root = root.clone();
        let faults = options.faults;
        // 打开数据库要拿排他锁并可能恢复，做在阻塞线程上，避免占住异步
        // 运行时的工作线程。
        let disk = tokio::task::spawn_blocking(move || Disk::open(&disk_root, faults))
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))??;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                stats: WriterStats {
                    durable_lsn: disk.last_lsn,
                    data_syncs: disk.data_syncs,
                    directory_syncs: disk.directory_latency.samples,
                    data_sync_latency: disk.data_latency.clone(),
                    directory_sync_latency: disk.directory_latency.clone(),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ready: Condvar::new(),
        });
        let inner = Arc::new(Inner {
            id: disk.id.clone(),
            root,
            shared: shared.clone(),
            control: Arc::new(Semaphore::new(options.control_bytes)),
            staging: Arc::new(Semaphore::new(2 * crate::VALUE_BUDGET)),
            audit: Arc::new(Semaphore::new(options.audit_bytes)),
            control_slots: Arc::new(Semaphore::new(options.queue_requests)),
            audit_slots: Arc::new(Semaphore::new(options.queue_requests)),
            control_budget: options.control_bytes,
            audit_budget: options.audit_bytes,
            batch_budget: options.batch_bytes,
            thread: Mutex::new(None),
        });
        // 泵线程是唯一持有写连接的线程：它决定提交序、组批与 COMMIT 时机。
        let handle = std::thread::Builder::new()
            .name("flow-journal".into())
            .spawn(move || pump(disk, shared, options))?;
        *inner
            .thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(handle);
        Ok(Self { inner })
    }
    /// 返回本数据集的身份标识（journal_id）。
    ///
    /// 提交的事务必须携带同一个标识，否则会被判为外来事务拒绝。
    pub fn id(&self) -> &str {
        &self.inner.id
    }
    /// 返回数据目录路径。
    pub fn root(&self) -> &Path {
        &self.inner.root
    }
    /// 取写者状态快照：在飞、排队、提交计数与 fsync 延迟分布。
    pub fn stats(&self) -> WriterStats {
        let queue = lock_queue(&self.inner.shared);
        let mut stats = queue.stats.clone();
        stats.in_flight = queue.in_flight;
        stats.queued_requests = queue.requests;
        stats.queued_bytes = queue.bytes;
        stats
    }
    /// 返回已持久化的最高提交序号。
    ///
    /// 该值只在 COMMIT 成功后前进：它是「事实已安全落盘」的公开承诺。
    pub fn durable_lsn(&self) -> u64 {
        self.stats().durable_lsn
    }

    /// 提交一批事件，返回落盘回执。
    ///
    /// 事件在入队前完成序列化与字节核算；最终提交序号由泵线程分配，返回值
    /// 里的序号才是事实的真实位置。
    ///
    /// # Errors
    /// 队列已关闭、权威已冻结、字节预算不足时返回对应错误。
    pub async fn submit(
        &self,
        tx_id: String,
        events: Vec<Event>,
        class: QueueClass,
    ) -> Result<Commit> {
        self.submit_inner(tx_id, events, class, None, Vec::new(), Vec::new())
            .await
    }

    /// Submit a checked domain transition. Facts, current state and receipts are atomic.
    ///
    /// 领域状态迁移的正规入口：调用方已用 reducer 预演过状态变化，写者只
    /// 负责把「事实 + 当前行 + 回执」放进同一事务落盘。
    pub async fn submit_prepared(
        &self,
        tx: Transaction,
        changes: Vec<EntityChange>,
    ) -> Result<Commit> {
        self.submit_prepared_values(tx, changes, Vec::new()).await
    }

    /// Stage without publishing facts. The byte permits and anonymous file live
    /// until cancellation/rollback drops them or the atomic command completes.
    ///
    /// 大值先落到匿名临时文件（进程退出即消失），不进入事实账本；只有随命令
    /// 一起提交时才切成块入库。这样「命令失败」不会留下无主的值正文。
    pub async fn prepare_json(
        &self,
        value: serde_json::Value,
        limit: usize,
    ) -> Result<PreparedValue> {
        // 暂存额度不得超过单值物化预算，否则一次暂存就能占满写者内存。
        if limit > crate::VALUE_BUDGET {
            return Err(Error::Limit("staged command value".into()));
        }
        let permit = self
            .inner
            .staging
            .clone()
            .acquire_many_owned(limit as u32)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        let identity = self.id().to_owned();
        tokio::task::spawn_blocking(move || PreparedValue::json(&identity, value, limit, permit))
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
    }

    /// Stream staged chunks and the final transition inside one SQLite COMMIT.
    ///
    /// 值分块与最终状态迁移共享一个提交：要么命令与其值一起生效，要么都不
    /// 生效，避免出现「命令已生效但值还没写完」的中间态。
    ///
    /// LSN 前置条件（生产不变量）：`expected_lsn` 由 backend 在决策锁内从
    /// `applied_lsn` 推算，而决策锁横跨「落盘 + 应用 + 发布」，因此任一
    /// 时刻至多一个 prepared 事务在队，它的 LSN 不可能被同队事务挤占。
    /// 生产路径里与它同批的只有本数据集自己的组批顺序，冲突分支只对
    /// 绕过决策锁的调用方（bench / 独立 store_stream）生效；新增裸
    /// `submit` 调用方前必须先读这段——冲突会让整批一起回滚。
    ///
    /// # Errors
    /// 事务属于别的数据集、值不属于本数据集、或缺少对应的发布事实时返回
    /// [`Error::Invalid`]。
    pub async fn submit_prepared_values(
        &self,
        tx: Transaction,
        changes: Vec<EntityChange>,
        values: Vec<StagedValue>,
    ) -> Result<Commit> {
        if tx.journal_id != self.id() {
            return Err(invalid("foreign prepared transition"));
        }
        // 逐个校验暂存值：发布事实必须由本数据集产生，且必须出现在最终
        // 事务里，否则会出现「值已入库但没有任何事实引用它」的孤儿块。
        for value in &values {
            let publication = value.publication()?;
            if publication.payload["journal_id"].as_str() != Some(self.id()) {
                return Err(invalid("foreign staged value"));
            }
            if !tx.events.contains(&publication) {
                return Err(invalid(
                    "staged value requires its publication in the final transaction",
                ));
            }
        }
        self.submit_inner(
            tx.tx_id,
            tx.events,
            QueueClass::Control,
            Some(tx.lsn),
            changes,
            values,
        )
        .await
    }

    /// 所有提交路径的公共实现：核算字节、取得许可、入队并等待回执。
    ///
    /// 顺序敏感：先算准字节再取许可，队列里的字节账才与实际占用一致；许可
    /// 与请求同生共死，超限立即失败而不是排到队尾再失败。
    async fn submit_inner(
        &self,
        tx_id: String,
        events: Vec<Event>,
        class: QueueClass,
        expected_lsn: Option<u64>,
        changes: Vec<EntityChange>,
        values: Vec<StagedValue>,
    ) -> Result<Commit> {
        // 分段事件由写者自己拥有：外部提交伪造分段事件会破坏段边界约定。
        if events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SegmentStarted | EventKind::SegmentSealed))
        {
            return Err(invalid("structural events are writer-owned"));
        }
        // 控制面与审计面各自计量：审计面的大值不能把控制面饿死。
        let (semaphore, slots) = match &class {
            QueueClass::Control => (&self.inner.control, &self.inner.control_slots),
            QueueClass::Audit(key) if !key.is_empty() => {
                (&self.inner.audit, &self.inner.audit_slots)
            }
            QueueClass::Audit(_) => return Err(invalid("audit dispatch identity required")),
        };
        // Reserve an entire line BEFORE serialization; then return the unused byte permits.
        // Slot limits additionally bound tiny queued records and channel overhead.
        let slot = slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        // Changes are already bounded by the reducer. Account for their encoded size as
        // well as facts; never silently exceed the configured byte budget.
        let change_bytes = if changes.is_empty() {
            0
        } else {
            codec::bounded_json(&changes, crate::sqlite::MAX_ENTITY_BYTES)?.len()
        };
        // Staged values stream one bounded chunk at a time. Keep its working
        // record reserved instead of charging the whole file to the queue.
        let scratch = if values.is_empty() { 0 } else { MAX_LINE_BYTES };
        let reserve = MAX_LINE_BYTES
            .checked_add(scratch)
            .and_then(|n| n.checked_add(change_bytes))
            .ok_or_else(|| invalid("queue byte overflow"))?;
        let max_capacity = match class {
            QueueClass::Control => self.inner.control_budget,
            QueueClass::Audit(_) => self.inner.audit_budget,
        };
        if reserve > max_capacity || reserve > self.inner.batch_budget {
            return Err(Error::Limit("transition exceeds writer byte budget".into()));
        }
        let mut permit = semaphore
            .clone()
            .acquire_many_owned(reserve as u32)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        // Prepared transitions already know their LSN. Encode once here and reuse the
        // canonical body/digest at append; non-prepared events encode again after the
        // writer assigns the LSN. Never three times, never just to count bytes.
        let tx = Transaction {
            v: 2,
            journal_id: self.id().into(),
            lsn: expected_lsn.unwrap_or(u64::MAX),
            tx_id,
            events,
        };
        let body = codec::bounded_json(&tx, MAX_LINE_BYTES)?;
        let bytes = body.len() + change_bytes + scratch;
        if matches!(class, QueueClass::Audit(_)) && bytes > 512 * 1024 {
            return Err(Error::Limit("audit transaction exceeds 512 KiB".into()));
        }
        let wire = expected_lsn.map(|_| (body.len() as u64, codec::digest(&body)));
        // 预留量高于实际用量时立刻归还差额，避免队列字节账虚高。
        drop(permit.split(reserve - bytes));
        let (ack, result) = oneshot::channel();
        {
            let mut queue = lock_queue(&self.inner.shared);
            // 冻结后不接受新请求：权威状态已不可信，继续写只会扩大损害面。
            if let Some(failure) = &queue.stats.failure {
                return Err(Error::Unavailable(failure.clone()));
            }
            if queue.closing {
                return Err(Error::Unavailable("writer closed".into()));
            }
            queue.bytes += bytes;
            queue.requests += 1;
            queue.stats.peak_queued_bytes = queue.stats.peak_queued_bytes.max(queue.bytes);
            queue.stats.peak_queued_requests = queue.stats.peak_queued_requests.max(queue.requests);
            let pending = Pending {
                tx,
                values,
                changes,
                expected_lsn,
                wire,
                bytes,
                ack,
                _bytes: permit,
                _slot: slot,
            };
            match class {
                QueueClass::Control => queue.control.push_back(pending),
                QueueClass::Audit(key) => {
                    // 新桶首次出现时才登记进轮转表，否则轮转会重复调度同一桶。
                    if !queue.audit.contains_key(&key) {
                        queue.round.push_back(key.clone());
                    }
                    queue.audit.entry(key).or_default().push_back(pending);
                }
            }
        }
        // 入队后唤醒泵线程；泵线程在持锁状态下检查队列，不会错过这次通知。
        self.inner.shared.ready.notify_one();
        result
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
            .map_err(Error::Unavailable)
    }

    /// 关闭写者并等待泵线程退出。
    ///
    /// 关闭后所有信号量关闭，等待中的提交方立即得到明确错误而不是永久挂起。
    /// 若此前发生过平台故障，关闭会返回该故障。
    ///
    /// # Errors
    /// 泵线程 panic 或此前存在写失败时返回 `Unavailable`。
    pub async fn close(&self) -> Result<()> {
        self.inner.control.close();
        self.inner.staging.close();
        self.inner.audit.close();
        self.inner.control_slots.close();
        self.inner.audit_slots.close();
        lock_queue(&self.inner.shared).closing = true;
        self.inner.shared.ready.notify_one();
        let handle = self
            .inner
            .thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(handle) = handle {
            tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| Error::Unavailable(e.to_string()))?
                .map_err(|_| Error::Unavailable("journal thread panicked".into()))?;
        }
        if let Some(failure) = self.stats().failure {
            return Err(Error::Unavailable(failure));
        }
        Ok(())
    }
}

/// 泵线程主循环。
///
/// 每次迭代处理一批事务；任何 panic 都被捕获并转成「冻结权威」，因为唯一
/// 写者线程挂掉必须对运维可见，不能只表现为提交方永久等待。
fn pump(mut disk: Disk, shared: Arc<Shared>, options: JournalOptions) {
    loop {
        // pump 线程 panic 必须可见（铁律 2 的运维面）：排队者不能永久挂在
        // oneshot 上、健康检查（stats.failure）不能永远看不到故障。panic
        // 视同平台故障冻结权威——批次内的 Pending 随 unwind 丢失 ack，
        // 提交方经 RecvError 得到 Unavailable，幂等由命令回执兜底。
        let step = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump_step(&mut disk, &shared, &options)
        })) {
            Ok(step) => step,
            Err(panic) => {
                let reason = if let Some(s) = panic.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".into()
                };
                freeze(&shared, format!("journal pump panicked: {reason}"));
                return;
            }
        };
        match step {
            PumpStep::Continue => continue,
            PumpStep::Closing | PumpStep::Frozen => return,
        }
    }
}

/// 一次泵迭代的结果：继续、队列关闭退出、或权威已冻结退出。
enum PumpStep {
    Continue,
    Closing,
    Frozen,
}

/// 平台故障冻结：记录 failure、清空队列（排队者拿到明确错误）、持有
/// flock 直到全部句柄关闭——新写者不得在旧权威仍被持有时启动。
fn freeze(shared: &Shared, failure: String) {
    let mut queue = lock_queue(shared);
    queue.in_flight = false;
    queue.stats.failure = Some(failure.clone());
    while let Some(pending) = queue.pop() {
        let _ = pending.ack.send(Err(failure.clone()));
    }
    while !queue.closing {
        queue = shared
            .ready
            .wait(queue)
            .unwrap_or_else(|poison| poison.into_inner());
    }
}

/// 锁中毒不连锁 panic：pump panic 可能发生在持锁窗口；队列不变量在
/// panic 后只剩「可能少了已弹出的项」，清理/关闭路径仍可安全恢复。
fn lock_queue(shared: &Shared) -> std::sync::MutexGuard<'_, Queue> {
    shared
        .queue
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

/// 一次泵迭代：等队列、组批、在同一事务里提交整批并回执。
///
/// 组批窗口最多等待 5 ms：再长会拖慢单笔提交，再短则组提交比过低、fsync
/// 次数过多。
fn pump_step(disk: &mut Disk, shared: &Arc<Shared>, options: &JournalOptions) -> PumpStep {
    let mut batch = {
        let mut queue = lock_queue(shared);
        while queue.requests == 0 && !queue.closing {
            queue = shared
                .ready
                .wait(queue)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        if queue.requests == 0 && queue.closing {
            return PumpStep::Closing;
        }
        // 组批窗口：从本轮开始计时，最多等 group_wait，让零星请求也能凑成
        // 一批共享一次 fsync。
        let deadline = Instant::now() + options.group_wait;
        while !queue.closing && queue.bytes < options.batch_bytes {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            queue = shared
                .ready
                .wait_timeout(queue, remaining)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
        let mut batch = Vec::new();
        let mut bytes = 0;
        // Bound admitted logical bytes. SQLite page/index/WAL writes are separate costs.
        while queue
            .next_bytes()
            .is_some_and(|n| bytes + n <= options.batch_bytes)
        {
            let item = queue.pop().expect("nonempty queue");
            bytes += item.bytes;
            batch.push(item);
        }
        queue.in_flight = true;
        batch
    };
    let mut commits = Vec::with_capacity(batch.len());
    let mut encoded_bytes = 0;
    let outcome: Result<()> = (|| {
        // 一个 SQLite 事务覆盖整批：事实、当前行、回执与提交水位同生共死。
        disk.begin()?;
        for pending in &mut batch {
            let chunks: u64 = pending.values.iter().map(|value| value.chunk_count()).sum();
            let final_lsn = disk
                .last_lsn
                .checked_add(chunks)
                .and_then(|lsn| lsn.checked_add(1))
                .ok_or_else(|| invalid("LSN overflow"))?;
            if pending
                .expected_lsn
                .is_some_and(|expected| expected != final_lsn)
            {
                return Err(Error::Conflict(
                    "prepared transition sequence changed".into(),
                ));
            }
            let mut value_prefix = Vec::new();
            for value in &mut pending.values {
                let prefix = value.prefix(
                    disk.last_lsn
                        .checked_add(1)
                        .ok_or_else(|| invalid("LSN overflow"))?,
                );
                for (index, metadata) in prefix.into_iter().enumerate() {
                    let mut chunk_tx = metadata.clone();
                    chunk_tx.events[0].payload =
                        serde_json::to_value(value.read_chunk(index as u64)?)?;
                    chunk_tx.validate()?;
                    let body = codec::bounded_json(&chunk_tx, MAX_LINE_BYTES)?;
                    disk.append(
                        &chunk_tx,
                        &[],
                        true,
                        body.len() as u64,
                        &codec::digest(&body),
                    )?;
                    encoded_bytes += body.len();
                    value_prefix.push(metadata);
                }
            }
            let mut tx = pending.tx.clone();
            tx.lsn = disk
                .last_lsn
                .checked_add(1)
                .ok_or_else(|| invalid("LSN overflow"))?;
            // 写侧校验闭合（I1/I2 的精神）：读路径 transaction_with_size
            // 强制同一套规则，写路径此前只受 schema CHECK 约束——一条绕过
            // 调用方校验的畸形事务能提交成功却永远读不回（audit_seq 无
            // dispatch 之类），并截断 scan。落盘前拒绝（Invalid 不冻结）。
            tx.validate()?;
            if pending
                .expected_lsn
                .is_some_and(|expected| expected != tx.lsn)
            {
                return Err(Error::Conflict(
                    "prepared transition sequence changed".into(),
                ));
            }
            let (bytes, digest) = match &pending.wire {
                // Prepared transactions already carry the final LSN and digest.
                Some((bytes, digest)) => (*bytes, digest.clone()),
                None => {
                    let body = codec::bounded_json(&tx, MAX_LINE_BYTES)?;
                    (body.len() as u64, codec::digest(&body))
                }
            };
            let location = disk.append(
                &tx,
                &pending.changes,
                pending.expected_lsn.is_some(),
                bytes,
                &digest,
            )?;
            encoded_bytes += pending.bytes
                - if pending.values.is_empty() {
                    0
                } else {
                    MAX_LINE_BYTES
                };
            commits.push(Commit {
                transaction: Arc::new(tx),
                location,
                value_prefix,
            });
        }
        // COMMIT 是整批唯一的持久化点：成功之后这批事实才对读者可见。
        disk.sync()?;
        Ok(())
    })();
    let mut queue = lock_queue(shared);
    queue.in_flight = false;
    queue.stats.data_syncs = disk.data_syncs;
    queue.stats.directory_syncs = disk.directory_latency.samples;
    queue.stats.data_sync_latency = disk.data_latency.clone();
    queue.stats.directory_sync_latency = disk.directory_latency.clone();
    match outcome {
        Ok(()) => {
            // 持久化水位只在 COMMIT 成功后前进，对外承诺的事实到此为止。
            queue.stats.durable_lsn = disk.last_lsn;
            queue.stats.batches += 1;
            // 事务计数含值分块事务：它们同样是已提交的事实。
            queue.stats.transactions += commits
                .iter()
                .map(|c| c.value_prefix.len() as u64 + 1)
                .sum::<u64>();
            queue.stats.events += commits
                .iter()
                .map(|c| {
                    c.transaction.events.len() as u64
                        + c.value_prefix
                            .iter()
                            .map(|tx| tx.events.len() as u64)
                            .sum::<u64>()
                })
                .sum::<u64>();
            queue.stats.encoded_bytes += encoded_bytes as u64;
            queue.stats.max_batch_transactions = queue
                .stats
                .max_batch_transactions
                .max(commits.iter().map(|c| c.value_prefix.len() + 1).sum());
            queue.stats.max_batch_bytes = queue
                .stats
                .max_batch_bytes
                .max(batch.iter().map(|p| p.bytes).sum());
            drop(queue);
            // Lost receivers do not roll back the persisted transaction.
            // 提交方可能已经放弃等待（超时或取消），但事实已落盘就不该回滚：
            // 回滚会让「已确认的命令」凭空消失。
            for (pending, commit) in batch.into_iter().zip(commits) {
                let _ = pending.ack.send(Ok(commit));
            }
            PumpStep::Continue
        }
        Err(error) => {
            // 整批回滚：宁可这批全部失败，也不能让事实与当前行只落一半。
            disk.rollback();
            let failure = error.to_string();
            for pending in batch {
                let _ = pending.ack.send(Err(failure.clone()));
            }
            // A rejected transaction (invalid input, limit, stale prepared LSN) must not
            // brick the only writer. Only platform/storage faults freeze the authority.
            if !matches!(
                error,
                Error::Invalid(_) | Error::Limit(_) | Error::Conflict(_)
            ) {
                drop(queue);
                freeze(shared, failure);
                return PumpStep::Frozen;
            }
            PumpStep::Continue
        }
    }
}
