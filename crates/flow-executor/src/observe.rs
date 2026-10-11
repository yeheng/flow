//! 观测传输（二期 §4.3，I05）：可丢弃可观察性记录。
//!
//! nodelog（console/stdout/stderr 叙事）是可丢弃观测：发送不阻塞 JS
//! 执行、不占审计序号、不占持久窗口、无确认协议；量由有界预算封顶，
//! 超限丢弃并计数。golden source（节点实际输入/输出）走 AuditBatch。
//!
//! 丢弃面（契约「超限丢弃并计数」的完整实现）：
//! - 批内字节预算 [`OBSERVABILITY_BATCH_BYTES`]：按**转义膨胀后**的保守
//!   估计累计，超预算即发批——编码后的 ObservabilityBatch 帧必然落在
//!   DATA_MAX_FRAME 之内，绝不会因为观测流量本身把帧打爆；
//! - 出站队列满：丢整批并计数（观测永不反压 golden source，也永不拖死
//!   会话）；
//! - 单行超过预算：丢行并计数（NodeLogger 已把行截到 8 KiB，这一条只是
//!   防御性兜底）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use flow_engine::execution_protocol::contract::{
    OBSERVABILITY_BATCH_BYTES, OBSERVABILITY_BATCH_LINES,
};
use flow_engine::execution_protocol::message::{Message, ObservationLine};
use flow_engine::log_types::{LogLevel, LogStream};
use flow_engine::nodelog::{LogLine, NodeLogger};

/// 观测丢弃计数（进程级，退出前回报诊断用）。
#[derive(Default)]
pub struct ObservationLoss {
    pub dropped_lines: AtomicU64,
    pub dropped_bytes: AtomicU64,
}

/// 观测转发的命名空间：`start` 起后台转发，`disabled_logger` 起静默出口；
/// 无实例状态（所有事实经 `Arc<ObservationLoss>` 返回给调用方）。
pub struct ObservationBridge;

impl ObservationBridge {
    /// 启动观测转发：NodeLogger 的 unbounded 出口（日志永远不影响执行）
    /// 转成有界 ObservabilityBatch（data 通道）。超限丢弃并计数。
    pub fn start(
        dispatch_id: String,
        outbound: mpsc::Sender<Message>,
    ) -> (NodeLogger, Arc<ObservationLoss>) {
        let loss = Arc::new(ObservationLoss::default());
        let (tx, mut rx) = mpsc::unbounded_channel::<LogLine>();
        let logger = NodeLogger::new(tx, flow_engine::nodelog::LogBudget::new(usize::MAX), "", 1);
        let forward_loss = loss.clone();
        let forward_dispatch = dispatch_id.clone();
        let forward_outbound = outbound.clone();
        tokio::spawn(async move {
            let mut lines: Vec<ObservationLine> = Vec::new();
            let mut batch_bytes = 0usize;
            loop {
                let maybe_line = rx.recv().await;
                match maybe_line {
                    None => {
                        flush(
                            &forward_outbound,
                            &forward_loss,
                            &forward_dispatch,
                            &mut lines,
                        );
                        return;
                    }
                    Some(line) => {
                        let message = line.message;
                        let estimated = escaped_estimate(&message);
                        if estimated > OBSERVABILITY_BATCH_BYTES {
                            // 单行超预算：丢行计数（防御兜底，正常不可能——
                            // NodeLogger 已把行截到 8 KiB）。
                            forward_loss.dropped_lines.fetch_add(1, Ordering::Relaxed);
                            forward_loss
                                .dropped_bytes
                                .fetch_add(message.len() as u64, Ordering::Relaxed);
                            continue;
                        }
                        if lines.len() >= OBSERVABILITY_BATCH_LINES
                            || batch_bytes + estimated > OBSERVABILITY_BATCH_BYTES
                        {
                            flush(
                                &forward_outbound,
                                &forward_loss,
                                &forward_dispatch,
                                &mut lines,
                            );
                            batch_bytes = 0;
                        }
                        batch_bytes += estimated;
                        lines.push(ObservationLine {
                            level: level_name(line.level).into(),
                            stream: stream_name(line.stream).into(),
                            message,
                        });
                    }
                }
            }
        });
        (logger, loss)
    }

    /// 观测关闭：logger 静默丢弃（主进程未配置观测存储时不产生流量）。
    pub fn disabled_logger() -> NodeLogger {
        NodeLogger::disabled()
    }
}

/// 发一批：观测永不反压、永不弑会话——队列满/关闭都按丢弃计数处理。
fn flush(
    outbound: &mpsc::Sender<Message>,
    loss: &ObservationLoss,
    dispatch_id: &str,
    lines: &mut Vec<ObservationLine>,
) {
    if lines.is_empty() {
        return;
    }
    let dropped_bytes: u64 = lines.iter().map(|line| line.message.len() as u64).sum();
    match outbound.try_send(Message::ObservabilityBatch {
        dispatch_id: Some(dispatch_id.to_string()),
        lines: std::mem::take(lines),
    }) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => {
            let Message::ObservabilityBatch { lines, .. } = message else {
                return;
            };
            loss.dropped_lines
                .fetch_add(lines.len() as u64, Ordering::Relaxed);
            loss.dropped_bytes
                .fetch_add(dropped_bytes, Ordering::Relaxed);
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(message)) => {
            let Message::ObservabilityBatch { lines, .. } = message else {
                return;
            };
            loss.dropped_lines
                .fetch_add(lines.len() as u64, Ordering::Relaxed);
            loss.dropped_bytes
                .fetch_add(dropped_bytes, Ordering::Relaxed);
        }
    }
}

/// JSON 字符串转义后长度的保守估计：普通字节 1:1，需要转义的字节
/// （`"`、`\`、控制字符）按最坏 6 字节（`\u00XX`）计。估计值 ≥ 实际编码
/// 长度，预算据此累计即可保证编码帧有界。
fn escaped_estimate(message: &str) -> usize {
    let escapes = message
        .bytes()
        .filter(|byte| matches!(byte, b'"' | b'\\' | 0x00..=0x1f))
        .count();
    message.len() + escapes * 5
}

fn level_name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
    }
}

fn stream_name(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
        LogStream::Engine => "engine",
    }
}

/// 当前丢弃计数（诊断）。
pub fn dropped(loss: &ObservationLoss) -> (u64, u64) {
    (
        loss.dropped_lines.load(Ordering::Relaxed),
        loss.dropped_bytes.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 回归：引号/控制字符洪水下，批的编码体积必须落在帧限内，且不丢
    /// 行（预算触发分批，而不是超限）；出站队列满时丢批并计数。
    #[tokio::test]
    async fn flood_of_escapable_lines_stays_within_frame_budget() {
        let (outbound, mut rx) = mpsc::channel::<Message>(2); // 故意小出站，制造 Full
        let (logger, loss) = ObservationBridge::start("dispatch-test".into(), outbound);
        // 8 KiB 的引号洪水：转义估计 ~48 KiB/行。
        let flood = "\"".repeat(8 * 1024);
        for _ in 0..64 {
            logger.info(flood.clone());
        }
        // 让转发任务跑（异步 spawn）。
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut encoded_batches = 0;
        while let Ok(message) = rx.try_recv() {
            let Message::ObservabilityBatch { lines, .. } = message else {
                continue;
            };
            let frame = flow_engine::execution_protocol::frame::encode_frame(
                &Message::ObservabilityBatch {
                    dispatch_id: Some("dispatch-test".into()),
                    lines,
                },
            )
            .expect("observability batch must encode within DATA_MAX_FRAME");
            assert!(frame.len() <= flow_engine::execution_protocol::DATA_MAX_FRAME);
            encoded_batches += 1;
        }
        assert!(encoded_batches > 0, "至少应有一批送达");
        // 出站容量 2：丢批必须被计数，而不是无界排队或阻塞。
        let (dropped_lines, _) = dropped(&loss);
        assert!(
            dropped_lines > 0 || encoded_batches >= 30,
            "小出站队列下：要么丢批计数，要么全部送达（实际批数 {encoded_batches}，丢弃 {dropped_lines}）"
        );
    }
}
