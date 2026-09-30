use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{oneshot, watch};

use crate::error::EngineError;

/// 节点日志级别（NodeLog.level）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// 节点日志来源：engine=引擎叙事，stdout/stderr=脚本 console 输出。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Engine,
    Stdout,
    Stderr,
}

/// 写入 event.jsonl 的事件。
///
/// 写序协议：执行副作用之前先写 `node_started`，拿到结果后再写终态事件。
/// 崩溃窗口因此被限定为「有 node_started、无终态」——恢复时据此判定。
///
/// 兼容：旧日志的 `node_started` 里可能还有已废弃的 `idempotency_key` /
/// `params_hash` 字段，反序列化时被忽略。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    RunStarted {
        workflow_id: String,
        workflow_version: i64,
        input: Value,
        /// 嵌套深度：sub_workflow 子 run 为父深度 + 1，根 run 为 0。
        /// 旧日志没有该字段，反序列化默认为 0。
        #[serde(default)]
        depth: u32,
    },
    NodeStarted {
        node_id: String,
        attempt: u32,
        /// sub_workflow 节点确定性派生的子 run id（`{父run}:{节点}:{attempt}`），
        /// 随 node_started 一起落盘，崩溃重放时据此附着原子 run。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_run_id: Option<String>,
        /// 节点输入面快照：模板展开后的 params（可观察性数据，fold 不消费）。
        /// 旧日志没有该字段，反序列化默认为 None。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
    },
    NodeCompleted {
        node_id: String,
        attempt: u32,
        output: Value,
        duration_ms: u64,
    },
    NodeFailed {
        node_id: String,
        attempt: u32,
        error: String,
        retryable: bool,
    },
    NodeSkipped {
        node_id: String,
        reason: String,
    },
    /// 节点运行日志（可观察性）。进程级持久：`append_log` 只写不 fsync，
    /// 搭同文件后续严格事件 fsync 的便车；run 终态事件写入前强制兜底 sync。
    /// fold 不消费本事件（日志不是恢复状态）。
    NodeLog {
        node_id: String,
        attempt: u32,
        level: LogLevel,
        stream: LogStream,
        message: String,
    },
    SignalReceived {
        node_id: String,
        payload: Value,
    },
    RunCompleted {
        output: Value,
    },
    RunFailed {
        error: String,
    },
    RunCancelled {},
}

impl Event {
    pub fn kind(&self) -> &'static str {
        match self {
            Event::RunStarted { .. } => "run_started",
            Event::NodeStarted { .. } => "node_started",
            Event::NodeCompleted { .. } => "node_completed",
            Event::NodeFailed { .. } => "node_failed",
            Event::NodeSkipped { .. } => "node_skipped",
            Event::NodeLog { .. } => "node_log",
            Event::SignalReceived { .. } => "signal_received",
            Event::RunCompleted { .. } => "run_completed",
            Event::RunFailed { .. } => "run_failed",
            Event::RunCancelled {} => "run_cancelled",
        }
    }

    pub fn node_id(&self) -> Option<&str> {
        match self {
            Event::NodeStarted { node_id, .. }
            | Event::NodeCompleted { node_id, .. }
            | Event::NodeFailed { node_id, .. }
            | Event::NodeSkipped { node_id, .. }
            | Event::NodeLog { node_id, .. }
            | Event::SignalReceived { node_id, .. } => Some(node_id),
            _ => None,
        }
    }

    /// run 终态事件（run_completed / run_failed / run_cancelled）。
    /// 订阅方据此判定「该 run 的日志已追平、流可结束」。
    pub fn is_run_terminal(&self) -> bool {
        matches!(
            self,
            Event::RunCompleted { .. } | Event::RunFailed { .. } | Event::RunCancelled {}
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub seq: u64,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    #[serde(flatten)]
    pub event: Event,
}

/// run 日志文件的共享句柄：写行（互斥，保单文件字节顺序）与 fsync（组提交）。
pub struct LogHandle {
    file: tokio::sync::Mutex<tokio::fs::File>,
}

impl LogHandle {
    fn new(file: tokio::fs::File) -> LogHandle {
        LogHandle {
            file: tokio::sync::Mutex::new(file),
        }
    }

    async fn write_line(&self, line: &str) -> Result<(), EngineError> {
        let mut file = self.file.lock().await;
        file.write_all(line.as_bytes()).await?;
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), EngineError> {
        let file = self.file.lock().await;
        file.sync_all().await?;
        Ok(())
    }
}

/// 组提交统计（测试断言组批真的发生了用）。单调累计，测试取差值。
#[derive(Debug, Clone, Copy, Default)]
pub struct CommitStats {
    /// 跑过的批数（一批 = leader 一次排空队列）。
    pub batches: u64,
    /// 实际 fsync 的文件次数。
    pub file_syncs: u64,
}

struct PendingSync {
    handle: Arc<LogHandle>,
    reply: oneshot::Sender<Result<(), String>>,
}

/// 严格组提交器（DESIGN §3.2）：`append` 返回即 durable，但 fsync 跨 run
/// 组提交。
///
/// 模型：写行由调用方自己完成（保单文件 seq 顺序），fsync 进组；leader
/// 一次排空队列里全部待 sync 的请求合成一批，批内每个文件只 fsync 一次且
/// 并发发出——同批多文件的写回合并在同一轮设备刷盘里，首个 fsync 付刷盘
/// 代价，其余近似免费。
///
/// 攒批是 drain 式的：leader 每跑完一批就取走排队中的下一批，无定时器——
/// 低负载批=1（零额外延迟），高负载批≈同时在飞的 append 数。
///
/// 没有后台任务：泵跑在抢到领导权的调用方任务里，跨 tokio runtime 安全
/// （每个 #[tokio::test] 一个 runtime）。leader 被取消时守卫把未完成的请求
/// 退回队列并交还领导权，等待者经 watch 领导权世代号接手，不会被锁死。
pub struct GroupCommitter {
    state: Mutex<CommitState>,
    /// 领导权易手世代号：watch 无丢失唤醒，取消路径也不丢接棒通知。
    lead_gen: watch::Sender<u64>,
}

struct CommitState {
    queue: Vec<PendingSync>,
    pumping: bool,
    stats: CommitStats,
}

impl Default for GroupCommitter {
    fn default() -> Self {
        let (lead_gen, _) = watch::channel(0u64);
        GroupCommitter {
            state: Mutex::new(CommitState {
                queue: Vec::new(),
                pumping: false,
                stats: CommitStats::default(),
            }),
            lead_gen,
        }
    }
}

impl GroupCommitter {
    pub fn new() -> Arc<GroupCommitter> {
        Arc::new(GroupCommitter::default())
    }

    pub fn stats(&self) -> CommitStats {
        self.state.lock().unwrap().stats
    }

    /// 把 `handle` 的 fsync 排进组；返回时该文件已 durable（严格语义）。
    pub async fn sync(&self, handle: Arc<LogHandle>) -> Result<(), EngineError> {
        let (reply, mut done) = oneshot::channel();
        self.state
            .lock()
            .unwrap()
            .queue
            .push(PendingSync { handle, reply });
        let mut lead_gen = self.lead_gen.subscribe();
        loop {
            // 队列里有活且没人跑批：自己当 leader 排空它
            if let Some(_leader) = self.try_lead() {
                self.pump().await;
            }
            tokio::select! {
                biased;
                result = &mut done => {
                    return result
                        .map_err(|_| EngineError::Io(std::io::Error::other("组提交泵意外退出")))
                        .and_then(|r| {
                            r.map_err(|message| EngineError::Io(std::io::Error::other(message)))
                        });
                }
                // 领导权易手/有请求被退回：重新看看要不要自己接手
                _ = lead_gen.changed() => {}
            }
        }
    }

    /// 抢领导权。同一时刻至多一个 leader 在跑泵。
    fn try_lead(&self) -> Option<LeaderGuard<'_>> {
        let mut state = self.state.lock().unwrap();
        if state.pumping || state.queue.is_empty() {
            return None;
        }
        state.pumping = true;
        Some(LeaderGuard { committer: self })
    }

    /// 跑批：排空 → 每文件一次 fsync（并发）→ 回复；队列空才放手。
    async fn pump(&self) {
        loop {
            let pendings: Vec<PendingSync> = {
                let mut state = self.state.lock().unwrap();
                if state.queue.is_empty() {
                    return;
                }
                state.stats.batches += 1;
                std::mem::take(&mut state.queue)
            };
            let mut batch = InflightBatch {
                committer: self,
                pendings,
            };

            // 批内按文件去重：一个文件一批只 fsync 一次
            let mut files: Vec<Arc<LogHandle>> = Vec::new();
            for pending in &batch.pendings {
                if !files.iter().any(|f| Arc::ptr_eq(f, &pending.handle)) {
                    files.push(pending.handle.clone());
                }
            }
            self.state.lock().unwrap().stats.file_syncs += files.len() as u64;

            // 同批多文件的 fsync 并发发出：设备把它们合并进同一轮刷盘，
            // 严格语义不打折——每个调用方等的是自己文件的 sync_all
            let results: Vec<Result<(), String>> = join_all(files.iter().map(|f| f.sync_all()))
                .await
                .into_iter()
                .map(|r| r.map_err(|e| e.to_string()))
                .collect();

            for pending in batch.pendings.drain(..) {
                let index = files
                    .iter()
                    .position(|f| Arc::ptr_eq(f, &pending.handle))
                    .expect("批内文件表必含该请求");
                let _ = pending.reply.send(results[index].clone());
            }
        }
    }
}

/// leader 守卫：正常走完/被取消都会交还领导权并唤醒接棒者。
struct LeaderGuard<'a> {
    committer: &'a GroupCommitter,
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.committer.state.lock().unwrap();
        state.pumping = false;
        drop(state);
        self.committer.lead_gen.send_modify(|gen| *gen += 1);
    }
}

/// 在飞批次守卫：leader 被取消时把没回复的请求退回队列，不丢任何 append。
struct InflightBatch<'a> {
    committer: &'a GroupCommitter,
    pendings: Vec<PendingSync>,
}

impl Drop for InflightBatch<'_> {
    fn drop(&mut self) {
        if !self.pendings.is_empty() {
            self.committer
                .state
                .lock()
                .unwrap()
                .queue
                .append(&mut self.pendings);
        }
    }
}

/// 进程级组提交器：没有后台任务，跨 tokio runtime 安全。
fn global_committer() -> &'static GroupCommitter {
    static GLOBAL: OnceLock<GroupCommitter> = OnceLock::new();
    GLOBAL.get_or_init(GroupCommitter::default)
}

/// 进程级组提交统计。**不对外**：消费者只有本文件末尾的组提交单测，
/// 它们直接读内部计数器（见末尾 `#[cfg(test)] mod group_commit_tests`）——
/// 不为一个测试事实在公开 API 上开洞（DESIGN §13 的硬规则）。
#[cfg(test)]
fn commit_stats_for_test() -> CommitStats {
    global_committer().stats()
}

/// 追加写的 run 事件日志。`append` 返回即 durable（严格组提交，§3.2）。
pub struct EventLog {
    handle: Arc<LogHandle>,
    seq: u64,
}

pub fn run_dir(data_dir: &Path, run_id: &str) -> PathBuf {
    data_dir.join("runs").join(run_id)
}

impl EventLog {
    /// 新建 run 的日志文件（已存在则报错，run_id 唯一）。
    pub async fn create(data_dir: &Path, run_id: &str) -> Result<EventLog, EngineError> {
        let dir = run_dir(data_dir, run_id);
        tokio::fs::create_dir_all(&dir).await?;
        let path = dir.join("event.jsonl");
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::AlreadyExists => EngineError::RunExists(run_id.to_string()),
                _ => EngineError::Io(e),
            })?;
        Ok(EventLog {
            handle: Arc::new(LogHandle::new(file)),
            seq: 0,
        })
    }

    /// 打开已有日志续写：先修复残缺尾行，再从最后一个有效 seq 接着写。
    pub async fn open(data_dir: &Path, run_id: &str) -> Result<EventLog, EngineError> {
        let dir = run_dir(data_dir, run_id);
        let path = dir.join("event.jsonl");
        if !path.exists() {
            return Err(EngineError::RunNotFound(run_id.to_string()));
        }
        let (events, valid_len) = read_events_repairing(&path).await?;
        let actual_len = tokio::fs::metadata(&path).await?.len();
        if actual_len != valid_len {
            // 崩溃时可能留下没有换行的半行 JSON，物理截断掉，保证后续追加不粘连
            let truncated = OpenOptions::new().write(true).open(&path).await?;
            truncated.set_len(valid_len).await?;
            truncated.sync_all().await?;
        }
        let seq = events.last().map(|e| e.seq).unwrap_or(0);
        let file = OpenOptions::new().append(true).open(&path).await?;
        Ok(EventLog {
            handle: Arc::new(LogHandle::new(file)),
            seq,
        })
    }

    /// 追加一条事件，返回时已 durable。
    ///
    /// 严格组提交（§3.2）：write_all 保单文件顺序，fsync 跨 run 组提交——
    /// 同时在飞的多个 run 的事件共享同一轮刷盘，但本 future 只在**自己的文件**
    /// sync_all 完成后才返回。崩溃语义与「每事件 fsync」完全一致。
    ///
    /// 耐久性分层（可观察性设计 §3.3）：终态事件写入前，先把同文件上先于它
    /// 写入的 node_log 行强制刷盘（搭同一次组提交）——终态返回 ⇒ 终态前的
    /// 日志已在盘上。这条保证内建在数据结构里，不依赖调用方记得。
    pub async fn append(&mut self, run_id: &str, event: Event) -> Result<Envelope, EngineError> {
        if event.is_run_terminal() {
            global_committer().sync(self.handle.clone()).await?;
        }
        self.write(run_id, event, true).await
    }

    /// 追加一条节点日志（进程级持久）：只 write_all，不进组提交队列。
    /// 单文件字节顺序由 LogHandle 互斥保证；后续同文件任何严格事件的 fsync
    /// 会把先于它写入的日志行一并刷盘。
    pub async fn append_log(
        &mut self,
        run_id: &str,
        event: Event,
    ) -> Result<Envelope, EngineError> {
        debug_assert!(
            matches!(event, Event::NodeLog { .. }),
            "append_log 只收 NodeLog"
        );
        self.write(run_id, event, false).await
    }

    /// 两个 append 的共用尾部：分配 seq、组行、可选组提交。
    async fn write(
        &mut self,
        run_id: &str,
        event: Event,
        durable: bool,
    ) -> Result<Envelope, EngineError> {
        let seq = self.seq + 1;
        let envelope = Envelope {
            seq,
            ts: Utc::now(),
            run_id: run_id.to_string(),
            event,
        };
        let mut line = serde_json::to_string(&envelope)?;
        line.push('\n');
        self.handle.write_line(&line).await?;
        if durable {
            global_committer().sync(self.handle.clone()).await?;
        }
        self.seq = seq;
        Ok(envelope)
    }
}

/// 读取事件流，并校验 seq 连续（防截断/损坏）。
pub(crate) async fn read_events(path: &Path) -> Result<Vec<Envelope>, EngineError> {
    let (events, _) = read_events_repairing(path).await?;
    validate_sequence(&events)?;
    Ok(events)
}

/// 读取面（`Engine::read_events`）的实现：全量或增量，由 `from_seq` 与缓存决定。
///
/// **为什么要增量**：订阅补齐路径（`run_tail` 的每 10s 兜底、每次缺口/唤醒）与
/// `run.events(from_seq)` 都会反复读同一个追加式日志。旧实现每次都整份
/// `fs::read` + 逐行反序列化 + 全序列校验，成本随日志线性增长——一个 1 万事件的
/// run，每个新事件都要为每个订阅者重解析 1 万行。日志是纯追加的，只需解析上次
/// 之后新增的字节。
///
/// **正确性护栏**（任一不满足就整体重读，绝不猜）：
/// 1. 文件只变长——变短（截断/轮转）丢弃缓存；
/// 2. 新块首条 `seq` 必须紧接已缓存的末尾 `seq`——接不上说明不是同一个追加流
///    （文件被重建、路径被复用、另一个 run 占了同名文件），整体重读。
///
/// 护栏 2 顺带取代了早先的「32 字节文件头指纹」：追加式日志只要 seq 接得上
/// 就是同一条流，接不上时全量重读本来就会给出正确结果或与旧实现一致的
/// LogCorrupted；头指纹只对「同长度整体替换」这一种 contrived 情形额外敏感，
/// 代价是每次增量读多两次 syscall（open + read 32 字节）。一条写者的 run
/// 日志不存在这种替换，删掉。
///
/// 缓存窗口之外（`from_seq` 低于窗口首条，或 `None`）一律全量读并全序列校验，
/// 与旧实现逐字等价。
pub(crate) async fn read_events_from(
    path: &Path,
    from_seq: Option<u64>,
) -> Result<Vec<Envelope>, EngineError> {
    let file_len = tokio::fs::metadata(path).await?.len();

    // 快照 / 全量读取：不走增量（它也负责把缓存刷热）
    if from_seq.is_none() {
        return read_events_uncached(path).await;
    }

    // 取可用缓存；不满足护栏就连缓存一起丢掉
    let resume = {
        let mut cache = tail_cache().lock().unwrap();
        let usable = cache
            .entries
            .get(path)
            .is_some_and(|entry| entry.byte_len <= file_len);
        if usable {
            let entry = cache.entries.get(path).unwrap();
            Some((entry.byte_len, entry.last_seq))
        } else {
            cache.entries.remove(path);
            cache.order.retain(|p| p != path);
            None
        }
    };

    if let Some((start, last_seq)) = resume {
        let bytes = read_tail_bytes(path, start).await?;
        let (fresh, consumed) = parse_complete_lines(&bytes, start)?;
        if !fresh.is_empty() && fresh[0].seq != last_seq + 1 {
            // 接不上：文件被改写或不是同一个追加流。整体重读（它会给出正确结果，
            // 或给出与旧实现一致的 LogCorrupted），不用缓存拼一个似是而非的答案。
            return read_events_uncached(path).await;
        }
        for pair in fresh.windows(2) {
            if pair[0].seq + 1 != pair[1].seq {
                return Err(EngineError::LogCorrupted(format!(
                    "事件 seq 不连续：{} 之后是 {}",
                    pair[0].seq, pair[1].seq
                )));
            }
        }
        let new_last = fresh.last().map(|e| e.seq).unwrap_or(last_seq);
        cache_store(path, consumed, new_last, &fresh, false);
    } else {
        return read_events_uncached(path).await;
    }

    // 窗口够不着（回放从很早的 seq 起）：全量读。命中窗口时才走缓存切片。
    //
    // 锁只在下面这个块里活着，块内就切成 owned Vec 交出来——**绝不**把
    // MutexGuard 带过任何 await：这个 future 被 `Box<dyn Future + Send>` 装着
    // （`EventReader::read_events`），带着 std Guard 就直接编译不过。
    let from = from_seq.unwrap_or(0);
    let window = {
        let cache = tail_cache().lock().unwrap();
        match cache.entries.get(path) {
            Some(entry) if from > entry.first_seq => Some(
                entry
                    .events
                    .iter()
                    .filter(|e| e.seq >= from)
                    .cloned()
                    .collect::<Vec<Envelope>>(),
            ),
            _ => None,
        }
    };
    match window {
        Some(events) => Ok(events),
        None => read_events_uncached(path).await,
    }
}

/// 全量读 + 全序列校验，并用结果刷新缓存（`read_events_from` 的慢路径）。
async fn read_events_uncached(path: &Path) -> Result<Vec<Envelope>, EngineError> {
    let bytes = tokio::fs::read(path).await?;
    let (events, consumed) = parse_complete_lines(&bytes, 0)?;
    validate_sequence(&events)?;
    let last_seq = events.last().map(|e| e.seq).unwrap_or(0);
    cache_store(path, consumed, last_seq, &events, true);
    Ok(events)
}

/// 只读 [start, EOF)：增量路径不把整份文件搬进内存。
async fn read_tail_bytes(path: &Path, start: u64) -> Result<Vec<u8>, EngineError> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// 每个 run 缓存的事件窗口（条）。窗口之外的 `from_seq` 走全量读。
const TAIL_CACHE_EVENTS: usize = 512;
/// 缓存条目上限（个 run）。超出按 FIFO 淘汰，内存有界。
const TAIL_CACHE_RUNS: usize = 64;

/// 一个 run 的追加式日志缓存窗口。
struct CachedTail {
    /// 已解析到（已消费）的字节偏移：最后一条完整行的末尾（护栏 1 的基准）
    byte_len: u64,
    /// 窗口内最小 seq：`from_seq` 低于它就说明窗口够不着，必须全量读
    first_seq: u64,
    /// 窗口内最大 seq（= 已解析的末尾 seq）
    last_seq: u64,
    events: VecDeque<Envelope>,
}

#[derive(Default)]
struct TailCache {
    entries: HashMap<PathBuf, CachedTail>,
    /// 插入顺序，用于 FIFO 淘汰（不用 LRU：读取顺序不改变容量上界）
    order: VecDeque<PathBuf>,
}

fn tail_cache() -> &'static Mutex<TailCache> {
    static CACHE: OnceLock<Mutex<TailCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(TailCache::default()))
}

/// 维护缓存窗口。`replace_window` 为真时**整体替换**（全量读路径：它交上来的
/// 就是完整事件表，窗口里已有的都含在里面，再追加一遍会重复），为假时追加
/// （增量路径：交上来的只有新解析的那些）。
fn cache_store(
    path: &Path,
    byte_len: u64,
    last_seq: u64,
    fresh: &[Envelope],
    replace_window: bool,
) {
    let mut cache = tail_cache().lock().unwrap();
    let key = path.to_path_buf();
    let existed = cache.entries.contains_key(&key);
    if !existed && cache.order.len() >= TAIL_CACHE_RUNS {
        if let Some(oldest) = cache.order.pop_front() {
            cache.entries.remove(&oldest);
        }
    }
    if !existed {
        cache.order.push_back(key.clone());
    }
    let entry = cache.entries.entry(key).or_insert_with(|| CachedTail {
        byte_len: 0,
        first_seq: 1,
        last_seq: 0,
        events: VecDeque::new(),
    });
    entry.byte_len = byte_len;
    entry.last_seq = last_seq;
    if replace_window {
        entry.events.clear();
    }
    for envelope in fresh {
        entry.events.push_back(envelope.clone());
    }
    while entry.events.len() > TAIL_CACHE_EVENTS {
        entry.events.pop_front();
    }
    entry.first_seq = entry.events.front().map(|e| e.seq).unwrap_or(last_seq);
}

pub fn validate_sequence(events: &[Envelope]) -> Result<(), EngineError> {
    for (i, env) in events.iter().enumerate() {
        let expected = i as u64 + 1;
        if env.seq != expected {
            return Err(EngineError::LogCorrupted(format!(
                "事件 seq 不连续：第 {} 行是 {}，期望 {}",
                i + 1,
                env.seq,
                expected
            )));
        }
    }
    Ok(())
}

/// 增量读取的连续性校验：只要求相邻事件 seq 连续，不要求首条为 1。
/// PG 订阅轮询等场景从中间 seq 开始读，首条=1 的全量语义不适用。
pub fn validate_sequence_contiguous(events: &[Envelope]) -> Result<(), EngineError> {
    for pair in events.windows(2) {
        if pair[0].seq + 1 != pair[1].seq {
            return Err(EngineError::LogCorrupted(format!(
                "事件 seq 不连续：{} 之后是 {}",
                pair[0].seq, pair[1].seq
            )));
        }
    }
    Ok(())
}

/// 逐行解析；允许最后一行是被崩溃截断的半行，返回有效字节长度供调用方截断文件。
async fn read_events_repairing(path: &Path) -> Result<(Vec<Envelope>, u64), EngineError> {
    let bytes = tokio::fs::read(path).await?;
    parse_complete_lines(&bytes, 0)
}

/// 解析一段字节里的**完整行**（末行没有 LF 就是半行，不算——它可能被续写完整，
/// 留给下一次）。`base` 是该段在文件中的起始偏移，用于错误定位与「已消费字节数」。
fn parse_complete_lines(bytes: &[u8], base: u64) -> Result<(Vec<Envelope>, u64), EngineError> {
    let mut events = Vec::new();
    let mut offset = 0usize;
    let mut consumed = base;

    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let Some(nl) = rest.iter().position(|b| *b == b'\n') else {
            break; // 残缺尾行
        };
        let line = &rest[..nl];
        let text = String::from_utf8_lossy(line);
        offset += nl + 1;
        consumed = base + offset as u64;
        if text.trim().is_empty() {
            continue;
        }
        let envelope: Envelope = serde_json::from_str(&text).map_err(|e| {
            EngineError::LogCorrupted(format!(
                "第 {} 字节处的事件无法解析：{e}",
                base + offset as u64 - line.len() as u64 - 1
            ))
        })?;
        events.push(envelope);
    }

    Ok((events, consumed))
}

/// 严格组提交语义（DESIGN §3.2）的内联单测。
///
/// 断言放在本文件内而非 `tests/`，因为放到集成测试就得把 `commit_stats()` /
/// `read_events()` 从 `pub use` 导到公开 API——不为一个测试事实在公开 API 上
/// 开洞（DESIGN §13 的硬规则）。放这里直接读内部统计，断言也离被测代码只有几行。
///
/// 注意：`stats` 是进程级计数器，本二进制里的用例必须串行取差值，否则并行
/// 用例的 append 会混进统计。（跨测试二进制是独立进程，本来没这问题。）
#[cfg(test)]
mod group_commit_tests {
    use super::*;
    use flow_test_support::io::TempDir;
    use serde_json::json;

    fn test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(tokio::sync::Mutex::default)
    }

    fn log_path(root: &Path, run_id: &str) -> PathBuf {
        run_dir(root, run_id).join("event.jsonl")
    }

    fn started(index: usize) -> Event {
        Event::NodeStarted {
            node_id: format!("n{index}"),
            attempt: 1,
            child_run_id: None,
            input: None,
        }
    }

    /// 8 个日志 × 5 事件并发追加：每个文件 seq 有序、行完整；fsync 必须被组批
    /// （批数远小于事件数——攒批是 drain 式的，一攒就是同时在飞的 append）。
    #[tokio::test]
    async fn concurrent_appends_group_fsyncs_and_stay_ordered() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-group-commit");
        const LOGS: usize = 8;
        const EVENTS: usize = 5;
        let before = commit_stats_for_test();

        let mut tasks = Vec::new();
        for log_index in 0..LOGS {
            let root = dir.path().to_path_buf();
            tasks.push(tokio::spawn(async move {
                let run_id = format!("run-{log_index}");
                let mut log = EventLog::create(&root, &run_id).await.unwrap();
                for event_index in 0..EVENTS {
                    let envelope = log.append(&run_id, started(event_index)).await.unwrap();
                    assert_eq!(envelope.seq, event_index as u64 + 1);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        for log_index in 0..LOGS {
            let run_id = format!("run-{log_index}");
            let events = read_events(&log_path(dir.path(), &run_id)).await.unwrap();
            assert_eq!(events.len(), EVENTS, "{run_id} 事件数不符");
            for (position, envelope) in events.iter().enumerate() {
                assert_eq!(envelope.seq, position as u64 + 1, "{run_id} seq 不连续");
                assert_eq!(envelope.run_id, run_id, "{run_id} 行串扰到别的 run");
            }
        }

        let after = commit_stats_for_test();
        let batches = after.batches - before.batches;
        let file_syncs = after.file_syncs - before.file_syncs;
        let total = (LOGS * EVENTS) as u64;
        println!("组批统计：{total} 个 append → {batches} 批 / {file_syncs} 次文件 fsync");
        assert_eq!(file_syncs, total, "每事件一个文件 fsync（严格语义不打折）");
        assert!(
            batches * 2 <= total,
            "fsync 必须被组批：{total} 个 append 只该跑几批，实际 {batches} 批"
        );
    }

    /// append 返回即可被全新读取者完整读到；EventLog::open 续写接在返回的 seq 后。
    /// 「返回即 durable」的 fsync 面由 SIGKILL 用例覆盖，这里钉读回一致性。
    #[tokio::test]
    async fn append_returns_with_line_fully_readable() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-group-commit");

        let mut log = EventLog::create(dir.path(), "r").await.unwrap();
        let first = log.append("r", started(1)).await.unwrap();
        let events = read_events(&log_path(dir.path(), "r")).await.unwrap();
        assert_eq!(events, vec![first.clone()], "append 返回即须读到完整行");

        // 崩溃恢复路径：open 续写必须接在已返回的 seq 之后，不重不漏
        let mut reopened = EventLog::open(dir.path(), "r").await.unwrap();
        let second = reopened
            .append(
                "r",
                Event::NodeCompleted {
                    node_id: "n1".into(),
                    attempt: 1,
                    output: json!(null),
                    duration_ms: 3,
                },
            )
            .await
            .unwrap();
        assert_eq!(second.seq, first.seq + 1);

        let events = read_events(&log_path(dir.path(), "r")).await.unwrap();
        assert_eq!(events, vec![first, second]);
    }

    fn node_log(index: usize) -> Event {
        Event::NodeLog {
            node_id: format!("n{index}"),
            attempt: 1,
            level: LogLevel::Info,
            stream: LogStream::Stdout,
            message: format!("log {index}"),
        }
    }

    /// NodeLog 与状态事件同流：seq 统一编号、读回顺序一致、连续性校验通过。
    #[tokio::test]
    async fn node_log_lines_share_one_seq_space() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-node-log");
        let mut log = EventLog::create(dir.path(), "r").await.unwrap();

        log.append("r", started(1)).await.unwrap();
        log.append_log("r", node_log(1)).await.unwrap();
        log.append_log("r", node_log(2)).await.unwrap();
        log.append("r", started(2)).await.unwrap();

        let events = read_events(&log_path(dir.path(), "r")).await.unwrap();
        let kinds: Vec<_> = events.iter().map(|e| e.event.kind()).collect();
        assert_eq!(
            kinds,
            vec!["node_started", "node_log", "node_log", "node_started"]
        );
        for (position, envelope) in events.iter().enumerate() {
            assert_eq!(envelope.seq, position as u64 + 1);
        }
    }

    /// 耐久性分层：日志 append_log 不付 fsync；终态事件写入前强制兑底 sync
    /// （先刷日志、再写终态、终态自身组提交）——共 2 次 file_sync。
    #[tokio::test]
    async fn terminal_append_syncs_pending_log_lines() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-terminal-sync");
        let mut log = EventLog::create(dir.path(), "r").await.unwrap();
        log.append("r", started(1)).await.unwrap();

        let before = commit_stats_for_test();
        log.append_log("r", node_log(1)).await.unwrap();
        log.append_log("r", node_log(2)).await.unwrap();
        let after_logs = commit_stats_for_test();
        assert_eq!(
            after_logs.file_syncs - before.file_syncs,
            0,
            "日志行不得触发 fsync"
        );

        log.append(
            "r",
            Event::RunCompleted {
                output: json!(null),
            },
        )
        .await
        .unwrap();
        let after_terminal = commit_stats_for_test();
        assert_eq!(
            after_terminal.file_syncs - after_logs.file_syncs,
            2,
            "终态前兑底刷盘一次 + 终态自身组提交一次"
        );

        // 读回：日志行与终态事件全部在盘，seq 连续
        let events = read_events(&log_path(dir.path(), "r")).await.unwrap();
        assert_eq!(events.len(), 4);
        assert!(matches!(events[3].event, Event::RunCompleted { .. }));
    }

    /// 增量读与全量读对每个 `from_seq` 都给同一答案（含越界的 from）。
    #[tokio::test]
    async fn incremental_read_matches_full_read_for_every_from_seq() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-incremental-read");
        let mut log = EventLog::create(dir.path(), "r").await.unwrap();
        for i in 0..8 {
            log.append("r", started(i)).await.unwrap();
        }
        let path = log_path(dir.path(), "r");
        let full = read_events(&path).await.unwrap();
        assert_eq!(full.len(), 8);

        for from in 1..=10u64 {
            let got = read_events_from(&path, Some(from)).await.unwrap();
            let expect: Vec<Envelope> = full.iter().filter(|e| e.seq >= from).cloned().collect();
            assert_eq!(got, expect, "from_seq={from} 与全量读不一致");
        }
        assert_eq!(read_events_from(&path, None).await.unwrap(), full);
    }

    /// 缓存之后日志继续追加：增量读必须看见新事件，且不许把旧答案再交出去。
    #[tokio::test]
    async fn incremental_read_picks_up_appended_events() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-incremental-append");
        let mut log = EventLog::create(dir.path(), "r").await.unwrap();
        for i in 0..4 {
            log.append("r", started(i)).await.unwrap();
        }
        let path = log_path(dir.path(), "r");
        assert_eq!(read_events_from(&path, Some(1)).await.unwrap().len(), 4);

        let mut reopened = EventLog::open(dir.path(), "r").await.unwrap();
        for i in 4..8 {
            reopened.append("r", started(i)).await.unwrap();
        }
        let all = read_events_from(&path, Some(1)).await.unwrap();
        assert_eq!(all.len(), 8, "追加后增量读漏掉了新事件");
        assert_eq!(all.last().unwrap().seq, 8);
        // 增量切片：只要窗口内的后缀
        let tail = read_events_from(&path, Some(6)).await.unwrap();
        assert_eq!(
            tail.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![6, 7, 8]
        );
    }

    /// 缓存之后日志被改写（ seq 接不上）：必须整体重读并报损坏，
    /// 不许用缓存拼一个似是而非的连续链。
    #[tokio::test]
    async fn incremental_read_rejects_a_gap_it_crosses() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-incremental-gap");
        let mut log = EventLog::create(dir.path(), "r").await.unwrap();
        for i in 0..4 {
            log.append("r", started(i)).await.unwrap();
        }
        let path = log_path(dir.path(), "r");
        assert_eq!(read_events_from(&path, Some(1)).await.unwrap().len(), 4);

        // 手工接一段 seq=6 起的事件：与已缓存的末尾 4 接不上
        let mut text = String::new();
        for seq in 6..=8u64 {
            let envelope = Envelope {
                seq,
                ts: Utc::now(),
                run_id: "r".into(),
                event: started(seq as usize),
            };
            text.push_str(&serde_json::to_string(&envelope).unwrap());
            text.push('\n');
        }
        let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
        file.write_all(text.as_bytes()).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        let err = read_events_from(&path, Some(5)).await.unwrap_err();
        assert!(matches!(err, EngineError::LogCorrupted(_)), "{err}");
    }

    /// 缓存之后文件被截断：必须丢掉缓存整体重读，而不是从错的偏移继续解析。
    #[tokio::test]
    async fn incremental_read_drops_cache_when_file_shrinks() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-incremental-truncate");
        let mut log = EventLog::create(dir.path(), "r").await.unwrap();
        for i in 0..6 {
            log.append("r", started(i)).await.unwrap();
        }
        let path = log_path(dir.path(), "r");
        assert_eq!(read_events_from(&path, Some(1)).await.unwrap().len(), 6);

        let full_len = tokio::fs::metadata(&path).await.unwrap().len();
        let truncated = OpenOptions::new().write(true).open(&path).await.unwrap();
        truncated.set_len(full_len / 2).await.unwrap();
        truncated.sync_all().await.unwrap();
        drop(truncated);

        let kept = read_events_from(&path, Some(1)).await.unwrap();
        assert_eq!(kept.len(), 3, "截断后应只读到剩下的 3 条");
        assert_eq!(kept.last().unwrap().seq, 3);
    }

    /// 同一路径下的文件被整个换掉（测试 TempDir 复用 / 日志重建）：缓存答案
    /// 绝不能被交出去。
    ///
    /// 这里换成一个**等长**的新日志：旧的 32 字节头指纹靠「头部字节不同」认出
    /// 它，指纹删掉后就靠护栏 2——新日志从 seq=1 重新开始，与缓存的末尾 5
    /// 接不上（`fresh[0].seq != last_seq + 1`）→ 整体重读。等长是为了让
    /// 「文件只变长」这条护栏失效，单独验证 seq 护栏自己就够。
    #[tokio::test]
    async fn incremental_read_does_not_serve_a_replaced_file_from_cache() {
        let _guard = test_lock().lock().await;
        let dir = TempDir::new("flow-incremental-replace");
        let path = log_path(dir.path(), "r");

        let mut first = EventLog::create(dir.path(), "r").await.unwrap();
        for i in 0..5 {
            first.append("r", started(i)).await.unwrap();
        }
        drop(first);
        let cached = read_events_from(&path, Some(1)).await.unwrap();
        assert_eq!(cached.len(), 5);
        let old_len = tokio::fs::metadata(&path).await.unwrap().len();

        std::fs::remove_file(&path).unwrap();
        // 同路径、内容不同（node_id 换成 "m…"，seq 从 1 重来）。写 8 条而不是
        // 5 条：保证新文件**更长**，让「文件只变长」这条护栏失效，只剩下 seq
        // 护栏在干活（否则一次等长/更短的替换会被另一条护栏顺手挡住，测不到
        // 想测的那条）。
        let mut second = EventLog::create(dir.path(), "r").await.unwrap();
        for seq in 1..=8u64 {
            second
                .write(
                    "r",
                    Event::NodeStarted {
                        node_id: format!("m{seq}"),
                        attempt: 1,
                        child_run_id: None,
                        input: None,
                    },
                    false,
                )
                .await
                .unwrap();
        }
        drop(second);
        assert!(
            tokio::fs::metadata(&path).await.unwrap().len() >= old_len,
            "本测试要的是「文件只变长」护栏失效（等长/更长的替换）"
        );

        let got = read_events_from(&path, Some(1)).await.unwrap();
        assert_eq!(got.len(), 8, "换文件后必须读到新日志，不是缓存的旧答案");
        assert_eq!(got[1].seq, 2);
        assert!(
            matches!(&got[1].event, Event::NodeStarted { node_id, .. } if node_id == "m2"),
            "读到的是新日志内容：{:?}",
            got[1].event
        );
    }

    /// 序列化兼容：NodeLog 往返一致；旧格式 node_started（无 input 字段）
    /// 反序列化默认 None，不炸。
    #[test]
    fn node_log_roundtrip_and_old_node_started_compat() {
        let envelope = Envelope {
            seq: 7,
            ts: Utc::now(),
            run_id: "r".into(),
            event: node_log(1),
        };
        let text = serde_json::to_string(&envelope).unwrap();
        let back: Envelope = serde_json::from_str(&text).unwrap();
        assert_eq!(back, envelope);
        assert!(matches!(
            back.event,
            Event::NodeLog {
                level: LogLevel::Info,
                stream: LogStream::Stdout,
                ..
            }
        ));

        let old = r#"{"seq":1,"ts":"2024-01-01T00:00:00Z","run_id":"r","type":"node_started","node_id":"n1","attempt":1}"#;
        let envelope: Envelope = serde_json::from_str(old).unwrap();
        assert!(matches!(
            envelope.event,
            Event::NodeStarted { input: None, .. }
        ));
    }
}
