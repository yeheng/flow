//! Lossy, separately retained observations. Never submitted to the authoritative journal.
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use crate::nodelog::LogLine;
use serde::{Deserialize, Serialize};

const LINE_BYTES: usize = 16 * 1024;
const QUEUE_RECORDS: usize = 64; // serialized bytes <= 1 MiB, for every level including warn/error

#[derive(Debug, Clone)]
pub struct ObservationOptions {
    pub segment_bytes: u64,
    pub retained_bytes: u64,
}
impl Default for ObservationOptions {
    fn default() -> Self {
        Self {
            segment_bytes: 16 * 1024 * 1024,
            retained_bytes: 64 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObservationLoss {
    pub queue_dropped: u64,
    pub retention_dropped: u64,
    pub storage_dropped: u64,
    pub history_incomplete: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    #[serde(with = "flow_journal::decimal")]
    pub seq: u64,
    pub run_id: String,
    pub dispatch_id: String,
    pub ts: chrono::DateTime<chrono::Utc>,
    pub line: LogLine,
}
#[derive(Debug, Serialize, Deserialize, Default)]
struct Manifest {
    #[serde(default)]
    scopes: BTreeMap<String, ObservationLoss>,
    last_seq: u64,
    loss: ObservationLoss,
    segments: BTreeMap<u64, (u64, u64)>,
}

#[derive(Default)]
struct ScopeCounters {
    queue: AtomicU64,
    storage: AtomicU64,
}
type Scopes = Arc<Mutex<BTreeMap<String, Arc<ScopeCounters>>>>;
fn scope_key(run: &str, dispatch: &str) -> String {
    serde_json::to_string(&(run, dispatch)).unwrap()
}
struct Pending {
    counters: Option<Arc<ScopeCounters>>,
    run_id: String,
    dispatch_id: String,
    line: LogLine,
}
enum Message {
    Line(Pending),
    Flush(mpsc::Sender<()>),
    Stop,
}

struct Inner {
    root: PathBuf,
    tx: mpsc::SyncSender<Message>,
    manifest: Arc<Mutex<Manifest>>,
    dropped: Arc<AtomicU64>,
    scopes: Scopes,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.tx.send(Message::Stop);
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }
}

#[derive(Clone)]
pub struct ObservationStore {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct ObservationLogger {
    counters: Option<Arc<ScopeCounters>>,
    store: ObservationStore,
    run_id: String,
    dispatch_id: String,
}
impl ObservationLogger {
    pub fn emit(&self, line: LogLine) {
        let pending = Pending {
            counters: self.counters.clone(),
            run_id: self.run_id.clone(),
            dispatch_id: self.dispatch_id.clone(),
            line,
        };
        // IDs come from bounded authoritative identities; count rejected oversize metadata too.
        let encoded = serde_json::to_vec(&pending.line);
        if encoded.as_ref().map_or(true, |v| {
            v.len() + pending.run_id.len() + pending.dispatch_id.len() + 256 > LINE_BYTES
        }) || self
            .store
            .inner
            .tx
            .try_send(Message::Line(pending))
            .is_err()
        {
            self.store.inner.dropped.fetch_add(1, Ordering::Relaxed);
            if let Some(counters) = &self.counters {
                counters.queue.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl ObservationStore {
    pub fn open(root: impl AsRef<Path>, options: ObservationOptions) -> std::io::Result<Self> {
        if options.segment_bytes < LINE_BYTES as u64
            || options.retained_bytes < options.segment_bytes
        {
            return Err(std::io::Error::other("invalid observation budget"));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        }
        let manifest_path = root.join("manifest.json");
        let loaded = File::open(&manifest_path).ok().and_then(|file| {
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1).read_to_end(&mut bytes).ok()?;
            if bytes.len() > 1024 * 1024 {
                return None;
            }
            serde_json::from_slice::<Manifest>(&bytes).ok()
        });
        let missing_manifest = loaded.is_none();
        let mut manifest = loaded.unwrap_or_default();
        if missing_manifest {
            manifest.loss.history_incomplete = true;
        }
        // Rebuild small segment inventory from disk rather than trusting a lagging manifest.
        manifest.segments.clear();
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if let Some(number) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.strip_suffix(".jsonl"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                let mut lines = 0;
                let mut reader = BufReader::new(File::open(entry.path())?);
                loop {
                    let mut line = Vec::new();
                    let n = reader
                        .by_ref()
                        .take((LINE_BYTES + 1) as u64)
                        .read_until(b'\n', &mut line)?;
                    if n == 0 {
                        break;
                    }
                    if n > LINE_BYTES || line.last() != Some(&b'\n') {
                        manifest.loss.history_incomplete = true;
                        break;
                    }
                    match serde_json::from_slice::<Observation>(&line).ok() {
                        Some(record) => {
                            manifest.last_seq = manifest.last_seq.max(record.seq);
                            lines += 1;
                        }
                        None => manifest.loss.history_incomplete = true,
                    }
                }
                manifest
                    .segments
                    .insert(number, (entry.metadata()?.len(), lines));
            }
        }
        if missing_manifest && (!manifest.segments.is_empty() || manifest_path.exists()) {
            manifest.loss.history_incomplete = true;
        }
        let dropped = Arc::new(AtomicU64::new(manifest.loss.queue_dropped));
        let scopes: Scopes = Arc::new(Mutex::new(
            manifest
                .scopes
                .iter()
                .take(4096)
                .map(|(key, loss)| {
                    (
                        key.clone(),
                        Arc::new(ScopeCounters {
                            queue: AtomicU64::new(loss.queue_dropped),
                            storage: AtomicU64::new(loss.storage_dropped),
                        }),
                    )
                })
                .collect(),
        ));
        let shared = Arc::new(Mutex::new(manifest));
        let (tx, rx) = mpsc::sync_channel(QUEUE_RECORDS);
        let inner = Arc::new(Inner {
            root: root.clone(),
            tx,
            manifest: shared.clone(),
            dropped: dropped.clone(),
            scopes: scopes.clone(),
            thread: Mutex::new(None),
        });
        let thread = std::thread::Builder::new()
            .name("flow-observation".into())
            .spawn(move || pump(root, options, shared, dropped, scopes, rx))?;
        *inner.thread.lock().unwrap() = Some(thread);
        Ok(Self { inner })
    }
    pub fn logger(&self, run_id: String, dispatch_id: String) -> ObservationLogger {
        let key = scope_key(&run_id, &dispatch_id);
        let mut scopes = self.inner.scopes.lock().unwrap();
        let counters = if scopes.len() < 4096 || scopes.contains_key(&key) {
            Some(scopes.entry(key).or_default().clone())
        } else {
            None
        };
        ObservationLogger {
            counters,
            store: self.clone(),
            run_id,
            dispatch_id,
        }
    }
    pub fn loss(&self) -> ObservationLoss {
        let mut loss = self.inner.manifest.lock().unwrap().loss.clone();
        loss.queue_dropped = self.inner.dropped.load(Ordering::Relaxed);
        loss
    }
    /// Exact queue/storage loss for tracked dispatches. Retention/corruption makes
    /// the historical count unknown; never misrepresent a missing scope as zero.
    pub fn scoped_loss(&self, run: &str, dispatch: Option<&str>) -> ObservationLoss {
        let global = self.loss();
        let scopes = self.inner.scopes.lock().unwrap();
        let mut result = ObservationLoss {
            history_incomplete: global.history_incomplete || global.retention_dropped > 0,
            ..Default::default()
        };
        let mut found = false;
        for (key, counters) in scopes.iter() {
            let Ok((r, d)) = serde_json::from_str::<(String, String)>(key) else {
                continue;
            };
            if r == run && dispatch.is_none_or(|id| id == d) {
                found = true;
                result.queue_dropped += counters.queue.load(Ordering::Relaxed);
                result.storage_dropped += counters.storage.load(Ordering::Relaxed);
            }
        }
        result.history_incomplete |= !found || scopes.len() >= 4096;
        result
    }
    pub fn flush(&self) {
        let (tx, rx) = mpsc::channel();
        if self.inner.tx.send(Message::Flush(tx)).is_ok() {
            let _ = rx.recv();
        }
    }
    pub fn page(
        &self,
        run_id: &str,
        dispatch_id: Option<&str>,
        from_seq: u64,
        limit: usize,
    ) -> std::io::Result<Vec<Observation>> {
        let segments = self
            .inner
            .manifest
            .lock()
            .unwrap()
            .segments
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut records = Vec::new();
        let mut bytes = 0;
        for segment in segments {
            let Ok(file) = File::open(self.inner.root.join(format!("{segment:08}.jsonl"))) else {
                continue;
            };
            let mut reader = BufReader::new(file);
            loop {
                let mut line = Vec::new();
                let n = std::io::Read::by_ref(&mut reader)
                    .take((LINE_BYTES + 1) as u64)
                    .read_until(b'\n', &mut line)?;
                if n == 0 {
                    break;
                }
                if n > LINE_BYTES || line.last() != Some(&b'\n') {
                    self.inner.manifest.lock().unwrap().loss.history_incomplete = true;
                    break;
                }
                let Ok(record) = serde_json::from_slice::<Observation>(&line) else {
                    self.inner.manifest.lock().unwrap().loss.history_incomplete = true;
                    continue;
                };
                if record.run_id == run_id
                    && record.seq >= from_seq
                    && dispatch_id.is_none_or(|id| id == record.dispatch_id)
                {
                    if records.len() >= limit.min(256) || bytes + n > 1024 * 1024 {
                        return Ok(records);
                    }
                    bytes += n;
                    records.push(record);
                }
            }
        }
        Ok(records)
    }
}

fn pump(
    root: PathBuf,
    options: ObservationOptions,
    shared: Arc<Mutex<Manifest>>,
    dropped: Arc<AtomicU64>,
    scopes: Scopes,
    rx: mpsc::Receiver<Message>,
) {
    let mut segment = shared
        .lock()
        .unwrap()
        .segments
        .last_key_value()
        .map_or(1, |(n, _)| n + 1);
    while let Ok(message) = rx.recv() {
        let pending = match message {
            Message::Line(p) => p,
            Message::Flush(ack) => {
                persist_manifest(&root, &shared, &dropped, &scopes);
                let _ = ack.send(());
                continue;
            }
            Message::Stop => {
                persist_manifest(&root, &shared, &dropped, &scopes);
                break;
            }
        };
        let mut manifest = shared.lock().unwrap();
        manifest.last_seq += 1;
        let record = Observation {
            seq: manifest.last_seq,
            run_id: pending.run_id,
            dispatch_id: pending.dispatch_id,
            ts: chrono::Utc::now(),
            line: pending.line,
        };
        let Ok(mut bytes) = serde_json::to_vec(&record) else {
            manifest.loss.storage_dropped += 1;
            if let Some(c) = &pending.counters {
                c.storage.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        };
        bytes.push(b'\n');
        if manifest
            .segments
            .get(&segment)
            .is_some_and(|(n, _)| n + bytes.len() as u64 > options.segment_bytes)
        {
            segment += 1;
        }
        while manifest
            .segments
            .values()
            .map(|(bytes, _)| bytes)
            .sum::<u64>()
            + bytes.len() as u64
            > options.retained_bytes
        {
            let Some((&old, &(_, count))) = manifest.segments.first_key_value() else {
                break;
            };
            if fs::remove_file(root.join(format!("{old:08}.jsonl"))).is_err() {
                manifest.loss.history_incomplete = true;
                break;
            }
            manifest.segments.remove(&old);
            manifest.loss.retention_dropped += count;
        }
        // If retention deletion failed, stop storing rather than exceeding the disk budget.
        if manifest.segments.values().map(|(n, _)| n).sum::<u64>() + bytes.len() as u64
            > options.retained_bytes
        {
            manifest.loss.storage_dropped += 1;
            if let Some(c) = &pending.counters {
                c.storage.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        let mut open = OpenOptions::new();
        open.append(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open.mode(0o600);
        }
        match open
            .open(root.join(format!("{segment:08}.jsonl")))
            .and_then(|mut f| f.write_all(&bytes))
        {
            Ok(()) => {
                let entry = manifest.segments.entry(segment).or_default();
                entry.0 += bytes.len() as u64;
                entry.1 += 1;
            }
            Err(_) => {
                manifest.loss.storage_dropped += 1;
                if let Some(c) = &pending.counters {
                    c.storage.fetch_add(1, Ordering::Relaxed);
                }
                manifest.loss.history_incomplete = true;
                segment += 1;
            }
        }
    }
}
fn persist_manifest(root: &Path, shared: &Mutex<Manifest>, dropped: &AtomicU64, scopes: &Scopes) {
    let snapshot = scopes
        .lock()
        .unwrap()
        .iter()
        .map(|(key, c)| {
            (
                key.clone(),
                ObservationLoss {
                    queue_dropped: c.queue.load(Ordering::Relaxed),
                    storage_dropped: c.storage.load(Ordering::Relaxed),
                    ..Default::default()
                },
            )
        })
        .collect();
    shared.lock().unwrap().scopes = snapshot;
    shared.lock().unwrap().loss.queue_dropped = dropped.load(Ordering::Relaxed);
    if let Ok(bytes) = serde_json::to_vec(&*shared.lock().unwrap()) {
        let _ = flow_journal::maintenance::atomic_write(&root.join("manifest.json"), &bytes);
    }
}
