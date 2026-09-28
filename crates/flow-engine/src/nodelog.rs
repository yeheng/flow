//! 节点运行日志的发射面：预算、logger、脱敏。
//!
//! 分层契约（可观察性设计 §3.3/§3.4）：
//! - 日志发射即忘、永不阻塞节点执行（unbounded 通道，量由预算封顶）；
//! - 预算是 **per-run** 共享状态（`Arc<LogBudget>`），driver 持有，NodeLogger
//!   克隆 Arc——logger 是 per-node-attempt 的，判定必须落在 per-run 一处；
//! - EventLog 对预算无感知，写盘策略（进程级持久）由 `EventLog::append_log` 承担。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use crate::event::{LogStream, LogLevel};

/// 单行日志截断上限（字节）。发射端截断，事件日志里不会出现超限行。
pub const MAX_LOG_LINE_BYTES: usize = 8 * 1024;

/// 每 run 日志条数默认预算。`FLOW_RUN_LOG_BUDGET` 可覆盖。
pub const DEFAULT_LOG_BUDGET: usize = 10_000;

/// per-run 日志预算：超限后 debug/info 丢弃，warn/error 永远放行
///（错误可观测性不打折），丢弃量计数、由调用方写摘要行留痕。
pub struct LogBudget {
    max: usize,
    emitted: AtomicUsize,
    dropped: AtomicUsize,
}

impl LogBudget {
    pub fn new(max: usize) -> Arc<LogBudget> {
        Arc::new(LogBudget {
            max,
            emitted: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        })
    }

    /// 本条日志是否放行。放行时内部计数 +1。
    pub fn admit(&self, level: LogLevel) -> bool {
        match level {
            LogLevel::Warn | LogLevel::Error => {
                self.emitted.fetch_add(1, Ordering::Relaxed);
                true
            }
            LogLevel::Debug | LogLevel::Info => {
                let emitted = self.emitted.fetch_add(1, Ordering::Relaxed);
                if emitted < self.max {
                    true
                } else {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
        }
    }

    /// 当前累计丢弃条数（摘要行用；取走后清零）。
    pub fn take_dropped(&self) -> usize {
        self.dropped.swap(0, Ordering::Relaxed)
    }
}

pub(crate) fn budget_from_env() -> usize {
    std::env::var("FLOW_RUN_LOG_BUDGET")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_LOG_BUDGET)
}

/// 通道载荷：driver 排空后转成 `Event::NodeLog` 落盘。
#[derive(Debug, Clone, PartialEq)]
pub struct LogLine {
    pub node_id: String,
    pub attempt: u32,
    pub level: LogLevel,
    pub stream: LogStream,
    pub message: String,
}

/// 单节点的日志发射器。Clone 进 exec 任务 / QuickJS console 桥接。
/// 通道满/接收端已关一律静默（fire-and-forget，日志永远不影响执行）。
#[derive(Clone)]
pub struct NodeLogger {
    tx: UnboundedSender<LogLine>,
    budget: Arc<LogBudget>,
    node_id: String,
    attempt: u32,
}

impl NodeLogger {
    pub fn new(
        tx: UnboundedSender<LogLine>,
        budget: Arc<LogBudget>,
        node_id: impl Into<String>,
        attempt: u32,
    ) -> NodeLogger {
        NodeLogger {
            tx,
            budget,
            node_id: node_id.into(),
            attempt,
        }
    }

    /// 无接收端的 logger（直接调用 exec 的测试路径用）：发射即丢，预算放行。
    pub fn disabled() -> NodeLogger {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        NodeLogger::new(tx, LogBudget::new(usize::MAX), "", 0)
    }

    pub fn log(&self, level: LogLevel, stream: LogStream, message: impl Into<String>) {
        if !self.budget.admit(level) {
            return;
        }
        let _ = self.tx.send(LogLine {
            node_id: self.node_id.clone(),
            attempt: self.attempt,
            level,
            stream,
            message: truncate_message(&message.into()),
        });
    }

    pub fn debug(&self, message: impl Into<String>) {
        self.log(LogLevel::Debug, LogStream::Engine, message);
    }

    pub fn info(&self, message: impl Into<String>) {
        self.log(LogLevel::Info, LogStream::Engine, message);
    }

    pub fn warn(&self, message: impl Into<String>) {
        self.log(LogLevel::Warn, LogStream::Engine, message);
    }

    pub fn error(&self, message: impl Into<String>) {
        self.log(LogLevel::Error, LogStream::Engine, message);
    }
}

/// 超限截断：按 UTF-8 字符边界切，尾部带截断标记（保留原字节数信息）。
pub fn truncate_message(message: &str) -> String {
    if message.len() <= MAX_LOG_LINE_BYTES {
        return message.to_string();
    }
    let mut end = MAX_LOG_LINE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated {} bytes]", &message[..end], message.len() - end)
}

/// 固定敏感键列表（小写子串匹配）：脱敏三个出口共用——http 日志行的 headers、
/// node_started.input（写入时）、timeline 的 output（展示时）。
const SENSITIVE_KEYS: [&str; 8] = [
    "authorization",
    "cookie",
    "set-cookie",
    "token",
    "password",
    "secret",
    "api_key",
    "apikey",
];

pub fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    SENSITIVE_KEYS.iter().any(|s| key.contains(s))
}

const REDACTED: &str = "***";

/// 递归脱敏一个 JSON 值（返回新值，不改输入）。对象键命中敏感列表时值替换
/// 为 "***"；数组逐位递归。
pub fn redact_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    if is_sensitive_key(k) {
                        (k.clone(), Value::String(REDACTED.into()))
                    } else {
                        (k.clone(), redact_value(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_value).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn logger_with_budget(max: usize) -> (NodeLogger, tokio::sync::mpsc::UnboundedReceiver<LogLine>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (NodeLogger::new(tx, LogBudget::new(max), "n1", 2), rx)
    }

    #[test]
    fn budget_drops_info_but_keeps_warn_error() {
        let (logger, mut rx) = logger_with_budget(2);
        logger.info("a");
        logger.debug("b");
        logger.info("c"); // 第 3 条 info：超限丢弃
        logger.warn("d");
        logger.error("e");

        let messages: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(messages.len(), 4, "info a、debug b、warn d、error e 放行");
        assert_eq!(messages[2].level, LogLevel::Warn);
        assert_eq!(messages[3].level, LogLevel::Error);
        assert_eq!(messages[0].attempt, 2, "日志归属到具体 attempt");
    }

    #[test]
    fn message_truncated_at_char_boundary_with_marker() {
        let (logger, mut rx) = logger_with_budget(usize::MAX);
        logger.info("宁".repeat(5000)); // 15000 字节 > 8KB
        let line = rx.try_recv().unwrap();
        assert!(line.message.ends_with(']'), "{}", &line.message[..80]);
        assert!(line.message.contains("[truncated "));
        assert!(line.message.len() < 9000, "截断后必须小于上限");
    }

    #[test]
    fn redact_walks_nested_structures() {
        let value = json!({
            "Authorization": "Bearer x",
            "url": "http://x",
            "headers": {"token": "t", "ok": 1},
            "items": [{"password": "p", "n": 2}]
        });
        let redacted = redact_value(&value);
        assert_eq!(redacted["Authorization"], json!("***"));
        assert_eq!(redacted["headers"]["token"], json!("***"));
        assert_eq!(redacted["headers"]["ok"], json!(1));
        assert_eq!(redacted["items"][0]["password"], json!("***"));
        assert_eq!(redacted["items"][0]["n"], json!(2));
        assert_eq!(redacted["url"], json!("http://x"));
        // 原值不动
        assert_eq!(value["Authorization"], json!("Bearer x"));
    }

    #[tokio::test]
    async fn disabled_logger_never_panics() {
        let logger = NodeLogger::disabled();
        logger.info("没人听");
        logger.error("也没人听");
    }
}
