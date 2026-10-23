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

#[derive(Debug, Clone)]
pub enum QueueClass {
    Control,
    Audit(String),
}

#[derive(Debug, Clone)]
pub struct Commit {
    pub transaction: Arc<Transaction>,
    pub location: Location,
    /// Dataset chunk transactions preceding the command; byte payloads omitted.
    pub value_prefix: Vec<Transaction>,
}

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
    admission: Option<Arc<WriteAdmission>>,
}

/// A conservative local lease deadline. Expiry is permanent for this writer;
/// delayed renewal must never revive a process that has lost authority.
pub struct WriteAdmission {
    deadline: Mutex<Option<Instant>>,
}
impl WriteAdmission {
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline: Mutex::new(Some(deadline)),
        }
    }
    fn lock_deadline(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        self.deadline.lock().unwrap_or_else(|poison| {
            let mut guard = poison.into_inner();
            // A poisoned authority lease is uncertain: never revive it silently.
            *guard = None;
            guard
        })
    }
    pub fn renew(&self, deadline: Instant) -> bool {
        let mut current = self.lock_deadline();
        if current.is_some_and(|value| value > Instant::now()) && deadline > Instant::now() {
            *current = Some(deadline);
            true
        } else {
            *current = None;
            false
        }
    }
    pub fn revoke(&self) {
        *self.lock_deadline() = None;
    }
    fn check(&self) -> Result<()> {
        let mut deadline = self.lock_deadline();
        if deadline.is_some_and(|value| value > Instant::now()) {
            return Ok(());
        }
        *deadline = None;
        Err(Error::Unavailable("journal membership lease lost".into()))
    }
}
impl Queue {
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
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
}
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
#[derive(Clone)]
pub struct Journal {
    inner: Arc<Inner>,
}
impl Journal {
    pub async fn open(root: impl AsRef<Path>, options: JournalOptions) -> Result<Self> {
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
        let handle = std::thread::Builder::new()
            .name("flow-journal".into())
            .spawn(move || pump(disk, shared, options))?;
        *inner
            .thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(handle);
        Ok(Self { inner })
    }
    pub fn id(&self) -> &str {
        &self.inner.id
    }
    pub fn root(&self) -> &Path {
        &self.inner.root
    }
    pub fn stats(&self) -> WriterStats {
        let queue = lock_queue(&self.inner.shared);
        let mut stats = queue.stats.clone();
        stats.in_flight = queue.in_flight;
        stats.queued_requests = queue.requests;
        stats.queued_bytes = queue.bytes;
        stats
    }
    pub fn durable_lsn(&self) -> u64 {
        self.stats().durable_lsn
    }

    pub fn set_write_admission(&self, admission: Arc<WriteAdmission>) -> Result<()> {
        let mut queue = lock_queue(&self.inner.shared);
        if queue.admission.is_some() || queue.requests != 0 || queue.in_flight || queue.closing {
            return Err(invalid("write admission already installed or writer busy"));
        }
        admission.check()?;
        queue.admission = Some(admission);
        Ok(())
    }

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
    pub async fn submit_prepared(
        &self,
        tx: Transaction,
        changes: Vec<EntityChange>,
    ) -> Result<Commit> {
        self.submit_prepared_values(tx, changes, Vec::new()).await
    }

    /// Stage without publishing facts. The byte permits and anonymous file live
    /// until cancellation/rollback drops them or the atomic command completes.
    pub async fn prepare_json(
        &self,
        value: serde_json::Value,
        limit: usize,
    ) -> Result<PreparedValue> {
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
    pub async fn submit_prepared_values(
        &self,
        tx: Transaction,
        changes: Vec<EntityChange>,
        values: Vec<StagedValue>,
    ) -> Result<Commit> {
        if tx.journal_id != self.id() {
            return Err(invalid("foreign prepared transition"));
        }
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

    async fn submit_inner(
        &self,
        tx_id: String,
        events: Vec<Event>,
        class: QueueClass,
        expected_lsn: Option<u64>,
        changes: Vec<EntityChange>,
        values: Vec<StagedValue>,
    ) -> Result<Commit> {
        if events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SegmentStarted | EventKind::SegmentSealed))
        {
            return Err(invalid("structural events are writer-owned"));
        }
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
        drop(permit.split(reserve - bytes));
        let (ack, result) = oneshot::channel();
        {
            let mut queue = lock_queue(&self.inner.shared);
            if let Some(failure) = &queue.stats.failure {
                return Err(Error::Unavailable(failure.clone()));
            }
            if queue.closing {
                return Err(Error::Unavailable("writer closed".into()));
            }
            if let Some(admission) = &queue.admission {
                admission.check()?;
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
                    if !queue.audit.contains_key(&key) {
                        queue.round.push_back(key.clone());
                    }
                    queue.audit.entry(key).or_default().push_back(pending);
                }
            }
        }
        self.inner.shared.ready.notify_one();
        result
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
            .map_err(Error::Unavailable)
    }

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

fn pump_step(disk: &mut Disk, shared: &Arc<Shared>, options: &JournalOptions) -> PumpStep {
    let (mut batch, admission) = {
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
        (batch, queue.admission.clone())
    };
    let mut commits = Vec::with_capacity(batch.len());
    let mut encoded_bytes = 0;
    let outcome: Result<()> = (|| {
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
                    if let Some(admission) = &admission {
                        admission.check()?;
                    }
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
            if let Some(admission) = &admission {
                admission.check()?;
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
            if let Some(admission) = &admission {
                admission.check()?;
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
        disk.sync()?;
        if let Some(admission) = &admission {
            admission.check()?;
        }
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
            queue.stats.durable_lsn = disk.last_lsn;
            queue.stats.batches += 1;
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
            for (pending, commit) in batch.into_iter().zip(commits) {
                let _ = pending.ack.send(Ok(commit));
            }
            PumpStep::Continue
        }
        Err(error) => {
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

#[cfg(test)]
mod admission_tests {
    use super::*;
    #[test]
    fn poisoned_lease_is_revoked_without_panicking_or_being_revived() {
        let lease = WriteAdmission::new(Instant::now() + Duration::from_secs(60));
        let _ = std::panic::catch_unwind(|| {
            let _guard = lease.deadline.lock().unwrap();
            panic!("lease update interrupted");
        });
        assert!(matches!(lease.check(), Err(Error::Unavailable(_))));
        assert!(!lease.renew(Instant::now() + Duration::from_secs(120)));
        lease.revoke();
        assert!(lease.check().is_err());
    }
}
