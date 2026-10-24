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
//!
//! 不变量：观测可丢，审计不可丢。因此本模块的一切背压/丢弃路径都不
//! 影响审计流，也绝不以阻塞用户代码的方式实现。

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::mpsc;

use flow_engine::execution_protocol::contract::{
    OBSERVABILITY_BATCH_BYTES, OBSERVABILITY_BATCH_LINES,
};
use flow_engine::execution_protocol::message::{Message, ObservationLine};
use flow_engine::log_types::{LogLevel, LogStream};
use flow_engine::nodelog::{LogLine, NodeLogger};

/// 观测丢弃计数（进程级，退出前回报诊断用）。
pub use flow_engine::nodelog::LogQueueLoss as ObservationLoss;

/// 观测转发的命名空间：`start` 起后台转发，`disabled_logger` 起静默出口；
/// 无实例状态（所有事实经 `Arc<ObservationLoss>` 返回给调用方）。
pub struct ObservationBridge;

/// 观测转发任务的收尾句柄。
///
/// 出站通道是 FIFO：批进入队列的顺序就是主进程看到的顺序。尾批若晚于
/// Result 控制帧发出，派发已结束、会话已归还进程池，下一个租约会把这批
/// 日志**错认领到别的节点头上**。所以调用方必须在发任何 Result 之前
/// 释放全部 logger 克隆并 `close()`，让尾批先入队。
pub struct ObservationClose {
    join: tokio::task::JoinHandle<()>,
}

impl ObservationClose {
    /// 等转发任务排空退出。带超时防御：若有 logger 克隆泄漏，转发任务
    /// 永远等不到通道关闭；超时后 abort 并等待任务退出，关闭观测入口。
    ///
    /// 必须调用：尾批要在 Result 控制帧之前入队，否则会被下一个派发
    /// 错认领（顺序契约见 [`ObservationClose`] 的类型文档）。
    pub async fn close(mut self) {
        if tokio::time::timeout(std::time::Duration::from_millis(500), &mut self.join)
            .await
            .is_err()
        {
            self.join.abort();
            let _ = (&mut self.join).await;
        }
    }
}

impl ObservationBridge {
    /// 启动有界非阻塞日志入口，按容量、100 ms 周期或关闭发送观测批次。
    /// 入口与出站超限都丢弃并计数，不阻塞执行。
    ///
    /// 返回三元组：logger（写日志）、丢弃计数（诊断）、收尾句柄（必须
    /// 在发 Result 前 close）。
    pub fn start(
        dispatch_id: String,
        outbound: mpsc::Sender<Message>,
    ) -> (NodeLogger, Arc<ObservationLoss>, ObservationClose) {
        let loss = Arc::new(ObservationLoss::default());
        // 256 lines, each capped by NodeLogger to 8 KiB plus a small truncation suffix.
        let (tx, mut rx) = mpsc::channel::<LogLine>(256);
        let logger = NodeLogger::bounded(tx, loss.clone());
        let forward_loss = loss.clone();
        let forward_dispatch = dispatch_id.clone();
        let forward_outbound = outbound.clone();
        let join = tokio::spawn(async move {
            let mut lines: Vec<ObservationLine> = Vec::new();
            let mut batch_bytes = 0usize;
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
            // Skip：积压时跳过错过的 tick，而不是补发——观测是尽力而为
            // 的叙事，补发只会放大延迟。
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await;
            loop {
                let maybe_line = tokio::select! {
                    line = rx.recv() => line,
                    _ = tick.tick() => {
                        // 周期兜底：即使日志稀疏也要定期发出已积累的行，
                        // 否则叙事会一直卡在缓冲里。
                        flush(&forward_outbound, &forward_loss, &forward_dispatch, &mut lines);
                        batch_bytes = 0;
                        continue;
                    }
                };
                match maybe_line {
                    None => {
                        // 入口关闭：这是最后一次 flush，尾批必须在这里送出。
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
                            // 批预算到顶先发一批：保证编码后的帧不会超出
                            // DATA_MAX_FRAME，也避免无限攒批。
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
        (logger, loss, ObservationClose { join })
    }

    /// 观测关闭：logger 静默丢弃（主进程未配置观测存储时不产生流量）。
    pub fn disabled_logger() -> NodeLogger {
        NodeLogger::disabled()
    }
}

/// 发一批：观测永不反压、永不弑会话——队列满/关闭都按丢弃计数处理。
///
/// `try_send` 而非 `send`：观测不构成背压通道，队列满就丢整批。
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
    // +5 而不是 +6：原字节已被 len() 计过一次，追加的是替换增量。
    message.len() + escapes * 5
}

/// 日志级别名（观测帧的 wire 字面量，冻结）。
fn level_name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
    }
}

/// 流名（观测帧的 wire 字面量，冻结）。
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

    /// 稀疏日志也要在 logger 存活期间被周期发出。
    #[tokio::test]
    async fn sparse_logs_flush_while_logger_is_alive() {
        let (tx, mut rx) = mpsc::channel(8);
        let (logger, _, close) = ObservationBridge::start("sparse".into(), tx);
        logger.info("live");
        let message = tokio::time::timeout(std::time::Duration::from_millis(300), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(message, Message::ObservabilityBatch { .. }));
        drop(logger);
        close.close().await;
    }

    /// 转发侧堵死时入口容量有界，丢弃被计数而不是无界排队。
    #[tokio::test]
    async fn stalled_forwarder_has_bounded_ingress_and_counts_loss() {
        let (tx, _rx) = mpsc::channel(1);
        let (logger, loss, close) = ObservationBridge::start("flood".into(), tx);
        for _ in 0..1000 {
            logger.error("x".repeat(8192));
        }
        assert!(dropped(&loss).0 >= 744);
        drop(logger);
        close.close().await;
    }

    /// 回归：引号/控制字符洪水下，批的编码体积必须落在帧限内，且不丢
    /// 行（预算触发分批，而不是超限）；出站队列满时丢批并计数。
    #[tokio::test]
    async fn flood_of_escapable_lines_stays_within_frame_budget() {
        let (outbound, mut rx) = mpsc::channel::<Message>(2); // 故意小出站，制造 Full
        let (logger, loss, close) = ObservationBridge::start("dispatch-test".into(), outbound);
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
        drop(logger);
        close.close().await;
    }

    /// 尾批必须能在 close() 里被冲出去：调用方靠它保证尾批先于 Result 帧入队。
    #[tokio::test]
    async fn close_flushes_the_tail_batch() {
        let (outbound, mut rx) = mpsc::channel::<Message>(8);
        let (logger, _loss, close) = ObservationBridge::start("dispatch-tail".into(), outbound);
        logger.info("尾批");
        // Before the forwarding task runs, the tail remains in the ingress queue.
        assert!(rx.try_recv().is_err());
        drop(logger);
        close.close().await;
        match rx.try_recv() {
            Ok(Message::ObservabilityBatch { dispatch_id, lines }) => {
                assert_eq!(dispatch_id.as_deref(), Some("dispatch-tail"));
                assert_eq!(lines.len(), 1);
                assert_eq!(lines[0].message, "尾批");
            }
            other => panic!("close 后应收到尾批：{other:?}"),
        }
    }
}
