use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

use crate::codec::{self, Event, EventKind, Transaction};
use crate::storage::{Disk, FaultInjection, Location};
use crate::{invalid, Error, Result, MAX_LINE_BYTES};

#[derive(Debug, Clone)]
pub struct JournalOptions {
    pub segment_bytes: u64,
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
            segment_bytes: 256 * 1024 * 1024,
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
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WriterStats {
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
    bytes: usize,
    requests: usize,
    stats: WriterStats,
}
impl Queue {
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
    audit: Arc<Semaphore>,
    control_slots: Arc<Semaphore>,
    audit_slots: Arc<Semaphore>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.shared.queue.lock().unwrap().closing = true;
        self.shared.ready.notify_one();
        if let Some(handle) = self.thread.lock().unwrap().take() {
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
            || options.segment_bytes < 4096
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
            audit: Arc::new(Semaphore::new(options.audit_bytes)),
            control_slots: Arc::new(Semaphore::new(options.queue_requests)),
            audit_slots: Arc::new(Semaphore::new(options.queue_requests)),
            thread: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("flow-journal".into())
            .spawn(move || pump(disk, shared, options))?;
        *inner.thread.lock().unwrap() = Some(handle);
        Ok(Self { inner })
    }
    pub fn id(&self) -> &str {
        &self.inner.id
    }
    pub fn root(&self) -> &Path {
        &self.inner.root
    }
    pub fn stats(&self) -> WriterStats {
        self.inner.shared.queue.lock().unwrap().stats.clone()
    }
    pub fn durable_lsn(&self) -> u64 {
        self.stats().durable_lsn
    }

    pub async fn submit(
        &self,
        tx_id: String,
        events: Vec<Event>,
        class: QueueClass,
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
        let mut permit = semaphore
            .clone()
            .acquire_many_owned(MAX_LINE_BYTES as u32)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        let tx = Transaction {
            v: 2,
            journal_id: self.id().into(),
            lsn: u64::MAX,
            tx_id,
            events,
        };
        let bytes = codec::encode(&tx)?.len();
        if matches!(class, QueueClass::Audit(_)) && bytes > 512 * 1024 {
            return Err(Error::Limit("audit transaction exceeds 512 KiB".into()));
        }
        drop(permit.split(MAX_LINE_BYTES - bytes));
        let (ack, result) = oneshot::channel();
        {
            let mut queue = self.inner.shared.queue.lock().unwrap();
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
        self.inner.audit.close();
        self.inner.control_slots.close();
        self.inner.audit_slots.close();
        self.inner.shared.queue.lock().unwrap().closing = true;
        self.inner.shared.ready.notify_one();
        let handle = self.inner.thread.lock().unwrap().take();
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
        let batch = {
            let mut queue = shared.queue.lock().unwrap();
            while queue.requests == 0 && !queue.closing {
                queue = shared.ready.wait(queue).unwrap();
            }
            if queue.requests == 0 && queue.closing {
                return;
            }
            let deadline = Instant::now() + options.group_wait;
            while !queue.closing && queue.bytes < options.batch_bytes {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                queue = shared.ready.wait_timeout(queue, remaining).unwrap().0;
            }
            let mut batch = Vec::new();
            let mut bytes = 0;
            // Conservative reservation means no transaction can make the physical batch exceed its cap.
            while queue.requests > 0 && bytes + MAX_LINE_BYTES <= options.batch_bytes {
                let item = queue.pop().expect("nonempty queue");
                bytes += item.bytes;
                batch.push(item);
            }
            batch
        };
        let mut commits = Vec::with_capacity(batch.len());
        let mut encoded_bytes = 0;
        let outcome: Result<()> = (|| {
            for pending in &batch {
                if disk.bytes + pending.bytes as u64 > options.segment_bytes
                    && disk.last_lsn > disk.first_lsn
                {
                    disk.rotate()?;
                }
                let mut tx = pending.tx.clone();
                tx.lsn = disk
                    .last_lsn
                    .checked_add(1)
                    .ok_or_else(|| invalid("LSN overflow"))?;
                let line = codec::encode(&tx)?;
                let location = disk.append(&line, tx.lsn)?;
                encoded_bytes += line.len();
                commits.push(Commit {
                    transaction: Arc::new(tx),
                    location,
                });
            }
            disk.sync()
        })();
        let mut queue = shared.queue.lock().unwrap();
        queue.stats.data_syncs = disk.data_syncs;
        queue.stats.directory_syncs = disk.directory_latency.samples;
        queue.stats.data_sync_latency = disk.data_latency.clone();
        queue.stats.directory_sync_latency = disk.directory_latency.clone();
        match outcome {
            Ok(()) => {
                queue.stats.durable_lsn = disk.last_lsn;
                queue.stats.batches += 1;
                queue.stats.transactions += batch.len() as u64;
                queue.stats.events += batch.iter().map(|p| p.tx.events.len() as u64).sum::<u64>();
                queue.stats.encoded_bytes += encoded_bytes as u64;
                queue.stats.max_batch_transactions =
                    queue.stats.max_batch_transactions.max(batch.len());
                queue.stats.max_batch_bytes = queue.stats.max_batch_bytes.max(encoded_bytes);
                drop(queue);
                // Lost receivers do not roll back the persisted transaction.
                for (pending, commit) in batch.into_iter().zip(commits) {
                    let _ = pending.ack.send(Ok(commit));
                }
            }
            Err(error) => {
                let failure = error.to_string();
                queue.stats.failure = Some(failure.clone());
                for pending in batch {
                    let _ = pending.ack.send(Err(failure.clone()));
                }
                while let Some(pending) = queue.pop() {
                    let _ = pending.ack.send(Err(failure.clone()));
                }
                // Keep the data-directory lock until all handles close. A new writer must not
                // start while callers still hold this unavailable authority.
                while !queue.closing {
                    queue = shared.ready.wait(queue).unwrap();
                }
                return;
            }
        }
    }
}
