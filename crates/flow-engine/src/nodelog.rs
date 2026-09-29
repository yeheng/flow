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

use crate::event::{LogLevel, LogStream};

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
    ///
    /// `max` 是 **debug/info 合计**的上限，不是总量：warn/error 永远放行
    /// （错误可观测性不打折，DESIGN §12.17）且**不消耗 debug/info 预算**。
    /// 共享一个计数器会让一个刷 warn 的节点把整条 run 的 info 日志预算吃光。
    pub fn admit(&self, level: LogLevel) -> bool {
        match level {
            LogLevel::Warn | LogLevel::Error => true,
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

/// 取不超过 `max` 字节的 UTF-8 前缀（字符边界安全）。两个截断器共用。
fn byte_prefix(text: &str, max: usize) -> &str {
    let mut end = text.len().min(max);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// 超限截断：按 UTF-8 字符边界切，尾部带截断标记（保留原字节数信息）。
pub fn truncate_message(message: &str) -> String {
    if message.len() <= MAX_LOG_LINE_BYTES {
        return message.to_string();
    }
    let kept = byte_prefix(message, MAX_LOG_LINE_BYTES);
    format!("{}…[truncated {} bytes]", kept, message.len() - kept.len())
}

/// 输入面快照（node_started.input）的序列化字节上限：日志行有 8KB 截断，
/// 输入面快照同理——预算管住了日志条数，这里管住单条事件的字节面。
pub const MAX_INPUT_SNAPSHOT_BYTES: usize = 8 * 1024;

/// 超限快照保留的前缀字节数。与 `truncate_message` 同一取法：**截断而非丢弃**，
/// 标注原始字节数并留下可读的开头——输入面快照存在的意义就是调试"到底传了
/// 什么进去"，一刀切成空壳恰好在最需要它的时候什么都不剩。
pub const INPUT_SNAPSHOT_PREVIEW_BYTES: usize = 512;

/// 超限的输入面快照 → `{"__truncated": true, "size": N, "preview": "…"}`。
/// 先脱敏后截断：脱敏可能缩小体积，以展示值为准。
///
/// 借引用签名（而非消费式）：脱敏要改值、执行要原值，调用方**必然**要留一份
/// `params.clone()`。改成拿走所有权只是把那次克隆藏进 `..line` 语法糖里，
/// 一个字节都没省下，还让「谁拥有这份 params」变成要读三遍才看得懂的事。
pub fn cap_input_snapshot(value: &Value) -> Value {
    // 序列化一次，量长度与取预览复用同一个 String——不为「只量长度不分配」
    // 引入第二个序列化（省下的一个 String 分配换不回多跑的一趟全量序列化）
    let text = serde_json::to_string(value).unwrap_or_default();
    if text.len() <= MAX_INPUT_SNAPSHOT_BYTES {
        return value.clone();
    }
    serde_json::json!({
        "__truncated": true,
        "size": text.len(),
        "preview": byte_prefix(&text, INPUT_SNAPSHOT_PREVIEW_BYTES),
    })
}

/// 固定敏感键列表（小写子串匹配）：脱敏三个出口共用——http 日志行的 headers、
/// node_started.input（写入时）、timeline 的 output（展示时）。
// 子串匹配。「set-cookie」被「cookie」完全覆盖，是死条目；同理
// 「password_policy」「tokenizer」「secretary」这类合法字段会被误脱敏成
// 「***」。改成精确/后缀匹配要考虑 `Authorization` / `x-apiKey` /
// `my_auth_token` 这些真实形状，属于独立决策，暂按现状。
const SENSITIVE_KEYS: [&str; 7] = [
    "authorization",
    "cookie",
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
///
/// 借引用签名：调用方还要拿原值执行（见 `cap_input_snapshot` 的说明），
/// 消费式签名只会让每个调用点补一个 `.clone()`。
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

    fn logger_with_budget(
        max: usize,
    ) -> (NodeLogger, tokio::sync::mpsc::UnboundedReceiver<LogLine>) {
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

    /// 预算是 debug/info 的上限，warn/error 不参与消耗（DESIGN §12.17）。
    /// 修复前 warn 也 `emitted += 1`，于是刷满 max 条 warn 之后整条 run 的
    /// info 日志全部丢弃——包括重试叙事与 HTTP 请求行这些最该看的。
    #[test]
    fn warn_burst_does_not_consume_the_info_budget() {
        let budget = LogBudget::new(3);
        for i in 0..50 {
            assert!(budget.admit(LogLevel::Warn), "warn {i} 必须放行");
            assert!(budget.admit(LogLevel::Error), "error {i} 必须放行");
        }
        for i in 0..3 {
            assert!(budget.admit(LogLevel::Info), "info {i} 必须在预算内");
        }
        assert!(!budget.admit(LogLevel::Info), "第 4 条 info 超限");
        assert!(!budget.admit(LogLevel::Debug), "debug 与 info 共享上限");
        // warn 永久放行，且不因预算耗尽而改变
        assert!(budget.admit(LogLevel::Warn));
        assert!(budget.admit(LogLevel::Error));
        assert_eq!(budget.take_dropped(), 2, "只丢 2 条 debug/info");
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

    /// `[truncated N bytes]` 里的 N 必须是**真实丢掉**的字节数。
    /// 修复前按 `len() - MAX_LOG_LINE_BYTES` 算，而实际截断点为字符边界回退
    /// 后的位置（≤ 上限），于是 N 系统性偏小——多字节内容上偏得更多。
    #[test]
    fn truncated_marker_reports_actual_dropped_bytes() {
        // 2730 个「宁」= 8190 字节，再加一个是 8193 > 8192 上限
        let text = "宁".repeat(5000); // 15000 字节
        let kept_bytes = MAX_LOG_LINE_BYTES - (MAX_LOG_LINE_BYTES % 3);
        assert_eq!(kept_bytes, 8190);
        let cut = truncate_message(&text);
        assert!(
            cut.contains(&format!("[truncated {} bytes]", text.len() - kept_bytes)),
            "标注的截断字节数必须等于真正丢掉的字节数（字符边界回退后为 6810，\
             修复前错报 6808）：{cut:?}"
        );
        // 单字节内容（无回退）时两者恰好相等，钉住 ASCII 路径没被改坏
        let ascii = "x".repeat(MAX_LOG_LINE_BYTES + 100);
        assert!(truncate_message(&ascii).contains("[truncated 100 bytes]"));
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
        // 消费式：调用方的原值不受影响
        assert_eq!(value["Authorization"], json!("Bearer x"));
    }

    #[tokio::test]
    async fn disabled_logger_never_panics() {
        let logger = NodeLogger::disabled();
        logger.info("没人听");
        logger.error("也没人听");
    }

    #[test]
    fn input_snapshot_capped_at_byte_limit() {
        // 限内：原样保留
        let small = json!({"url": "http://x", "n": 1});
        assert_eq!(cap_input_snapshot(&small), small);
        // 超限：占位携带原始序列化大小 + 可读前缀（截断而非丢弃）
        let big = json!({"body": "x".repeat(MAX_INPUT_SNAPSHOT_BYTES)});
        let capped = cap_input_snapshot(&big);
        assert_eq!(capped["__truncated"], json!(true));
        assert!(capped["size"].as_u64().unwrap() > MAX_INPUT_SNAPSHOT_BYTES as u64);
        let preview = capped["preview"].as_str().unwrap();
        assert!(preview.starts_with("{\"body\":\"xxx"), "{preview}");
        assert!(preview.len() <= INPUT_SNAPSHOT_PREVIEW_BYTES);
    }

    #[test]
    fn input_snapshot_preview_stays_on_char_boundary() {
        // 多字节内容：前缀不得切在字符中间
        let big = json!({"body": "宁".repeat(MAX_INPUT_SNAPSHOT_BYTES)});
        let capped = cap_input_snapshot(&big);
        let preview = capped["preview"].as_str().unwrap();
        assert!(preview.ends_with("宁") || preview.ends_with('"'));
    }
}
