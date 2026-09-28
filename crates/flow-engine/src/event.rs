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

/// 进程级组提交统计。**不对外**：它的消费者只有本文件末尾的组提交单测，
///
/// 曾为了一份 `tests/group_commit.rs` 把它 `pub` 到 `flow_engine::commit_stats`——
/// 公开 API 为一个测试事实开了洞。现在这条断言搬回本文件（见末尾
/// `#[cfg(test)] mod group_commit_tests`），直接读内部计数器，不再需要导出。
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
    pub async fn append(&mut self, run_id: &str, event: Event) -> Result<Envelope, EngineError> {
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
        global_committer().sync(self.handle.clone()).await?;
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
    let mut events = Vec::new();
    let mut offset = 0usize;
    let mut valid_len = 0u64;

    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let Some(nl) = rest.iter().position(|b| *b == b'\n') else {
            break; // 残缺尾行
        };
        let line = &rest[..nl];
        let text = String::from_utf8_lossy(line);
        if text.trim().is_empty() {
            offset += nl + 1;
            valid_len = offset as u64;
            continue;
        }
        let envelope: Envelope = serde_json::from_str(&text).map_err(|e| {
            EngineError::LogCorrupted(format!("{path:?} 第 {offset} 字节处的事件无法解析：{e}"))
        })?;
        events.push(envelope);
        offset += nl + 1;
        valid_len = offset as u64;
    }

    Ok((events, valid_len))
}

/// 严格组提交语义（DESIGN §3.2）的内联单测。
///
/// 这些断言原来放在 `tests/group_commit.rs`，为此把 `commit_stats()` /
/// `read_events()` 从 `pub use` 导出到了公开 API——一个测试事实开了两个公开
/// 符号的洞。搬回本文件后：直接读 `GroupCommitter` 的内部统计，公开 API 收
/// 回原样，而且断言离被测代码只有几行。
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
}
