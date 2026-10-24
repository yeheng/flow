//! Lossy, separately retained observations. Never submitted to the authoritative journal.
//!
//! 本模块负责「非权威观测日志」的独立存储：有界队列 → 分段 JSONL → 独立 manifest。
//! 不变量：观测永不进权威 journal；磁盘占用有硬上限，淘汰失败即停止落盘；
//! 丢失必须可计量，历史不完整要显式标记，绝不把「不知道」报成「零丢失」。
//! 不这样做的代价：日志量大且允许丢弃，混进权威库会污染事务边界、撑爆磁盘，
//! 「丢日志」与「丢业务事实」将无法解耦。
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use crate::nodelog::LogLine;
use serde::{Deserialize, Serialize};

/// 单条观测记录的字节上限（整行，含 JSON 编码）。
///
/// 它是**解析侧的硬边界**：读取时超过它的行一律判定为截断/损坏并标记
/// 历史不完整，避免用无界缓冲去吃内存。
const LINE_BYTES: usize = 16 * 1024;
/// 内存队列的记录条数上限。
///
/// 队列满即丢弃（fire-and-forget）：观测面必须小到永不成为节点执行的
/// 阻塞点，所以这里取小值，量的上限由磁盘预算负责。
const QUEUE_RECORDS: usize = 64; // serialized bytes <= 1 MiB, for every level including warn/error

/// 观测存储的磁盘预算参数。
///
/// `segment_bytes` 是单个分段文件的滚动阈值，`retained_bytes` 是全部分段
/// 的总预算上限；两者是嵌套关系——保留预算至少要装得下一个分段，否则
/// 刚写满的段会立刻被淘汰，存储会原地抖动。
#[derive(Debug, Clone)]
pub struct ObservationOptions {
    /// 单个分段文件的最大字节数，超过后写入新分段。
    pub segment_bytes: u64,
    /// 全部观测分段的总字节上限，超过后从最老分段开始删除。
    pub retained_bytes: u64,
}
/// 默认预算：单段 16 MiB、总量 64 MiB。
///
/// 总量是单段的 4 倍，保证淘汰时始终有多个分段可轮转，不会出现
/// 「刚写满就被删」的抖动。
impl Default for ObservationOptions {
    /// 返回默认预算参数。
    fn default() -> Self {
        Self {
            segment_bytes: 16 * 1024 * 1024,
            retained_bytes: 64 * 1024 * 1024,
        }
    }
}
/// 观测日志的丢失计量：区分「队列丢弃」「留存淘汰」「写盘失败」三类原因。
///
/// 分开计数是因为三者的排查方向不同：队列丢弃说明生产快于消费，
/// 留存淘汰说明超出磁盘预算，写盘失败说明介质或权限有问题。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObservationLoss {
    /// 因队列满或记录超限而未进入存储的条数。
    pub queue_dropped: u64,
    /// 因超出留存预算被删除的历史条数。
    pub retention_dropped: u64,
    /// 因序列化或写盘失败而未落盘的条数。
    pub storage_dropped: u64,
    /// 历史是否不完整（manifest 缺失、记录损坏或分段曾被淘汰）。
    pub history_incomplete: bool,
}
/// 一条落盘的观测记录。
///
/// `seq` 由存储侧在写入时分配（全局单调），供分页读取定位增量；调用方
/// 不提供 seq——多个写入者各自编号必然撞号。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    /// 存储内全局单调递增的序号。
    #[serde(with = "flow_journal::decimal")]
    pub seq: u64,
    /// 所属 run。
    pub run_id: String,
    /// 所属派发（dispatch）。
    pub dispatch_id: String,
    /// 落盘时刻（UTC）。
    pub ts: chrono::DateTime<chrono::Utc>,
    /// 原始日志行（发射前已脱敏）。
    pub line: LogLine,
}
/// 观测存储的元数据：分段清单、序号水位与丢失计数。
///
/// 单独一个 JSON 文件，与分段日志分离：分段是「数据」，manifest 是
/// 「账本」。账本丢了数据还在，只是丢失量无法计量。
#[derive(Debug, Serialize, Deserialize, Default)]
struct Manifest {
    /// 各 (run, dispatch) 作用域的丢失计数。
    #[serde(default)]
    scopes: BTreeMap<String, ObservationLoss>,
    /// 已分配的最大序号。
    last_seq: u64,
    /// 全局丢失计数。
    loss: ObservationLoss,
    /// 分段号 → (字节数, 记录条数)。
    segments: BTreeMap<u64, (u64, u64)>,
}

/// 单个 (run, dispatch) 作用域的原子计数器。
///
/// 用原子量而不是互斥锁：发射路径跑在节点执行线程上，不能被存储线程的
/// 落盘 I/O 拖住。
#[derive(Default)]
struct ScopeCounters {
    /// 该作用域的队列丢弃数。
    queue: AtomicU64,
    /// 该作用域的写盘丢弃数。
    storage: AtomicU64,
}
/// 全部作用域计数器的共享表。
type Scopes = Arc<Mutex<BTreeMap<String, Arc<ScopeCounters>>>>;
/// 作用域键：由 (run, dispatch) 序列化而来。
///
/// 用 JSON 元组而不是字符串拼接：拼接会让 `run="ab" + dispatch="c"` 与
/// `run="a" + dispatch="bc"` 撞成同一个键，丢失量就归错账。
fn scope_key(run: &str, dispatch: &str) -> String {
    serde_json::to_string(&(run, dispatch)).unwrap()
}
/// 等待落盘的一条观测。
///
/// `counters` 可能为 `None`：作用域表满了（4096 上限）时不再新建作用域，
/// 这条记录仍会写入磁盘，只是丢弃量无法按作用域归因。
struct Pending {
    /// 该记录所属作用域的计数器（可能因作用域表满而缺失）。
    counters: Option<Arc<ScopeCounters>>,
    /// 所属 run。
    run_id: String,
    /// 所属派发。
    dispatch_id: String,
    /// 日志内容。
    line: LogLine,
}
/// 存储线程的消息类型。
enum Message {
    /// 一条待落盘观测。
    Line(Pending),
    /// 落盘 manifest 并回执（`flush` 用）。
    Flush(mpsc::Sender<()>),
    /// 停止线程（Drop 时发送）。
    Stop,
}

/// 存储的共享内部状态。
struct Inner {
    /// 分段根目录。
    root: PathBuf,
    /// 发射端（有界通道）。
    tx: mpsc::SyncSender<Message>,
    /// manifest，与存储线程共享。
    manifest: Arc<Mutex<Manifest>>,
    /// 全局队列丢弃计数（发射侧原子量）。
    dropped: Arc<AtomicU64>,
    /// 作用域计数器表。
    scopes: Scopes,
    /// 存储线程句柄（Drop 时 join）。
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Drop for Inner {
    /// 停线程并等它落完 manifest：直接退出会丢掉最后一批计数，
    /// 丢失量被低报。
    fn drop(&mut self) {
        let _ = self.tx.send(Message::Stop);
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }
}

/// 观测日志存储句柄（可克隆，克隆共享同一份存储）。
///
/// Clone 只是共享同一个 `Inner`：多个写者共用一条队列和一个存储线程，
/// 才能保证序号唯一、磁盘预算只有一份。
#[derive(Clone)]
pub struct ObservationStore {
    inner: Arc<Inner>,
}
/// 绑定到某个 (run, dispatch) 的观测发射器。
///
/// 发射是 fire-and-forget：队列满或记录超限就丢并计数，绝不阻塞节点
/// 执行——观测面永远不能拖慢业务。
#[derive(Clone)]
pub struct ObservationLogger {
    counters: Option<Arc<ScopeCounters>>,
    store: ObservationStore,
    run_id: String,
    dispatch_id: String,
}
impl ObservationLogger {
    /// 发射一条观测日志。
    ///
    /// 两条丢弃路径：记录本身超过单行上限（身份字段虽短，但留给
    /// 编码余量的 256 字节是保险），以及有界队列已满。任何一条命中
    /// 都只计数不报错——观测面不能把业务带崩。
    pub fn emit(&self, line: LogLine) {
        let pending = Pending {
            counters: self.counters.clone(),
            run_id: self.run_id.clone(),
            dispatch_id: self.dispatch_id.clone(),
            line,
        };
        // IDs come from bounded authoritative identities; count rejected oversize metadata too.
        let encoded = serde_json::to_vec(&pending.line);
        // 两个丢弃口径同归「队列丢弃」：记录超限、或有界队列已满。
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
    /// 打开（或初始化）观测存储目录，启动后台写入线程。
    ///
    /// 启动时**不信任**已有 manifest 的分段清单，而是扫描目录重建：
    /// manifest 可能在写入途中崩溃而滞后于磁盘，重建才是真相来源。
    /// 重建过程中发现的问题（manifest 缺失、记录损坏、行超限）统一记为
    /// `history_incomplete`——宁可说「不知道」，不可说「没丢过」。
    ///
    /// # Errors
    /// 预算非法（保留额小于单段、单段小于单行上限）或目录不可访问时
    /// 返回错误；此时不会留下半启动的线程。
    pub fn open(root: impl AsRef<Path>, options: ObservationOptions) -> std::io::Result<Self> {
        // 保留预算必须装得下一个分段：否则滚动即淘汰，存储原地抖动。
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
            // 目录收紧到 0700：观测内容虽已脱敏，仍按敏感数据对待。
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        }
        let manifest_path = root.join("manifest.json");
        // manifest 可能不存在、被截断或超过上限：读不出来就当作没有，
        // 下面的重建会把真实情况补回来。
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
                    // 半行或超限行：写盘崩溃留下的残行，标记历史不完整并
                    // 放弃该分段的剩余部分（顺序已不可信）。
                    if n > LINE_BYTES || line.last() != Some(&b'\n') {
                        manifest.loss.history_incomplete = true;
                        break;
                    }
                    match serde_json::from_slice::<Observation>(&line).ok() {
                        Some(record) => {
                            // 序号水位取磁盘上的最大值：manifest 可能滞后。
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
        // 有分段却没有 manifest（或 manifest 文件存在但读不出来）：账本丢了，
        // 历史不完整。
        if missing_manifest && (!manifest.segments.is_empty() || manifest_path.exists()) {
            manifest.loss.history_incomplete = true;
        }
        let dropped = Arc::new(AtomicU64::new(manifest.loss.queue_dropped));
        let scopes: Scopes = Arc::new(Mutex::new(
            manifest
                .scopes
                .iter()
                // 作用域表上限 4096：防止 (run, dispatch) 组合无限增长撑爆内存。
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
        // 有界通道：队列满时发射侧立刻放弃，而不是把内存堆起来。
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
    /// 取得某个 (run, dispatch) 的发射器。
    ///
    /// 作用域表有 4096 上限：超过后新作用域不再登记（`counters` 为
    /// `None`），记录照常落盘，但丢弃量无法按作用域归因——这是有意的
    /// 内存上限，不是正确性豁免。
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
    /// 全局丢失计量。
    ///
    /// 队列丢弃数取自发射侧的原子量（实时），留存/写盘丢失取自
    /// manifest（磁盘事实）；两者含义不同，不要互相替代。
    pub fn loss(&self) -> ObservationLoss {
        let mut loss = self.inner.manifest.lock().unwrap().loss.clone();
        loss.queue_dropped = self.inner.dropped.load(Ordering::Relaxed);
        loss
    }
    /// Exact queue/storage loss for tracked dispatches. Retention/corruption makes
    /// the historical count unknown; never misrepresent a missing scope as zero.
    ///
    /// 按 (run, dispatch) 精确归因：只统计登记在作用域表里的丢弃数。
    /// 作用域未登记（表满或从未出现）时**不**返回 0，而是把
    /// `history_incomplete` 置真——「没记录」不等于「没丢」。
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
        // 没找到作用域、或作用域表已满：无法证明「零丢失」，必须标记。
        result.history_incomplete |= !found || scopes.len() >= 4096;
        result
    }
    /// 落盘 manifest 并等待完成。
    ///
    /// 用于测试与「要确保计数已持久化」的时点；发射路径不依赖它。
    pub fn flush(&self) {
        let (tx, rx) = mpsc::channel();
        if self.inner.tx.send(Message::Flush(tx)).is_ok() {
            let _ = rx.recv();
        }
    }
    /// 按 (run, dispatch) 分页读取观测记录。
    ///
    /// 上限双保险：条数取 `limit` 与 256 的较小值，字节数取 1 MiB。
    /// 观测是旁路读取，不能因为调用方要得多就把内存吃光。读到损坏行
    /// 时标记 `history_incomplete` 并跳过该行。
    ///
    /// # Errors
    /// 读取分段文件失败时返回错误。
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

/// 存储线程：串行消费队列，负责序号分配、分段滚动、留存淘汰与落盘。
///
/// 单线程写入是刻意的：序号唯一、分段清单一致、磁盘预算只有一份，
/// 都不需要额外加锁；并发发射只需往有界队列里塞。
fn pump(
    root: PathBuf,
    options: ObservationOptions,
    shared: Arc<Mutex<Manifest>>,
    dropped: Arc<AtomicU64>,
    scopes: Scopes,
    rx: mpsc::Receiver<Message>,
) {
    // 从 manifest 里最后一个分段号继续：写新分段不能覆盖已有数据。
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
        // 序号只由存储线程分配：并发发射不会撞号。
        manifest.last_seq += 1;
        let record = Observation {
            seq: manifest.last_seq,
            run_id: pending.run_id,
            dispatch_id: pending.dispatch_id,
            ts: chrono::Utc::now(),
            line: pending.line,
        };
        let Ok(mut bytes) = serde_json::to_vec(&record) else {
            // 序列化失败：计为写盘丢弃并继续，不让一条坏记录卡住整条队列。
            manifest.loss.storage_dropped += 1;
            if let Some(c) = &pending.counters {
                c.storage.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        };
        // JSONL 以换行结尾：读取侧靠它判断一行是否完整。
        bytes.push(b'\n');
        // 本段写满则换新分段：分段是淘汰的最小单位，滚动让淘汰按段发生。
        if manifest
            .segments
            .get(&segment)
            .is_some_and(|(n, _)| n + bytes.len() as u64 > options.segment_bytes)
        {
            segment += 1;
        }
        // 超出总预算就删最老分段：日志可丢，但磁盘上限是硬约束。
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
                // 删不掉最老分段（权限/占用）：标记历史不完整并停止淘汰循环。
                manifest.loss.history_incomplete = true;
                break;
            }
            manifest.segments.remove(&old);
            // 记录被淘汰的条数：让「历史不完整」可计量，而不是一句空话。
            manifest.loss.retention_dropped += count;
        }
        // If retention deletion failed, stop storing rather than exceeding the disk budget.
        if manifest.segments.values().map(|(n, _)| n).sum::<u64>() + bytes.len() as u64
            > options.retained_bytes
        {
            // 淘汰失败后仍超预算：计为写盘丢弃并放弃这条记录。
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
            // 分段文件只对属主可读写：内容虽已脱敏，仍按敏感数据对待。
            open.mode(0o600);
        }
        match open
            .open(root.join(format!("{segment:08}.jsonl")))
            .and_then(|mut f| f.write_all(&bytes))
        {
            Ok(()) => {
                // 写成功：分段字节数与条数随之更新，账本与数据保持一致。
                let entry = manifest.segments.entry(segment).or_default();
                entry.0 += bytes.len() as u64;
                entry.1 += 1;
            }
            Err(_) => {
                // 写失败：计为丢弃并标记历史不完整；换新分段，避免坏分段被反复追加。
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
/// 把 manifest 原子落盘（tmp + rename）。
///
/// 作用域计数先快照进 manifest：原子量本身不可序列化，只能在快照时刻
/// 取值；持久化失败时保留内存里的值，下次 flush 再试。
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
    // 全局队列丢弃数以发射侧原子量为准：manifest 里存的是上一次快照。
    shared.lock().unwrap().loss.queue_dropped = dropped.load(Ordering::Relaxed);
    if let Ok(bytes) = serde_json::to_vec(&*shared.lock().unwrap()) {
        let _ = flow_journal::maintenance::atomic_write(&root.join("manifest.json"), &bytes);
    }
}
