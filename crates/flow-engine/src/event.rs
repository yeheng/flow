use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub seq: u64,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    #[serde(flatten)]
    pub event: Event,
}

/// 追加写的 run 事件日志，每事件 fsync。
pub struct EventLog {
    path: PathBuf,
    file: tokio::fs::File,
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
        Ok(EventLog { path, file, seq: 0 })
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
        Ok(EventLog { path, file, seq })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn last_seq(&self) -> u64 {
        self.seq
    }

    /// 追加一条事件并 fsync。这是崩溃安全的写入边界。
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
        self.file.write_all(line.as_bytes()).await?;
        self.file.sync_all().await?;
        self.seq = seq;
        Ok(envelope)
    }
}

/// 读取事件流，并校验 seq 连续（防截断/损坏）。
pub async fn read_events(path: &Path) -> Result<Vec<Envelope>, EngineError> {
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
