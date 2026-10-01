//! 观测传输（二期 §4.3，I05）：可丢弃可观察性记录。
//!
//! nodelog（console/stdout/stderr 叙事）是可丢弃观测：发送不阻塞 JS
//! 执行、不占审计序号、不占持久窗口、无确认协议；量由有界预算封顶，
//! 超限丢弃并计数。golden source（节点实际输入/输出）走 AuditBatch。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use flow_engine::execution_protocol::contract::OBSERVABILITY_BATCH_LINES;
use flow_engine::execution_protocol::message::{Message, ObservationLine};use flow_engine::event::{LogLevel, LogStream};
use flow_engine::nodelog::{LogLine, NodeLogger};

/// 观测丢弃计数（进程级，退出前回报诊断用）。
#[derive(Default)]
pub struct ObservationLoss {
    pub dropped_lines: AtomicU64,
    pub dropped_bytes: AtomicU64,
}

pub struct ObservationBridge {
    dispatch_id: Option<String>,
    outbound: mpsc::Sender<Message>,
    loss: Arc<ObservationLoss>,
}

impl ObservationBridge {
    /// 启动观测转发：NodeLogger 的 unbounded 出口（日志永远不影响执行）
    /// 转成有界 ObservabilityBatch（data 通道）。超限丢弃并计数。
    pub fn start(
        dispatch_id: String,
        outbound: mpsc::Sender<Message>,
    ) -> (NodeLogger, Arc<ObservationLoss>) {
        let loss = Arc::new(ObservationLoss::default());
        let (tx, mut rx) = mpsc::unbounded_channel::<LogLine>();
        let logger = NodeLogger::new(
            tx,
            flow_engine::nodelog::LogBudget::new(usize::MAX),
            "",
            1,
        );
        let forward_loss = loss.clone();
        let forward_dispatch = dispatch_id.clone();
        tokio::spawn(async move {
            let mut lines: Vec<ObservationLine> = Vec::new();
            loop {
                let maybe_line = rx.recv().await;
                match maybe_line {
                    None => {
                        if !lines.is_empty() {
                            let _ = outbound
                                .send(Message::ObservabilityBatch {
                                    dispatch_id: Some(forward_dispatch.clone()),
                                    lines: std::mem::take(&mut lines),
                                })
                                .await;
                        }
                        return;
                    }
                    Some(line) => {
                        if lines.len() >= OBSERVABILITY_BATCH_LINES {
                            let _ = outbound
                                .send(Message::ObservabilityBatch {
                                    dispatch_id: Some(forward_dispatch.clone()),
                                    lines: std::mem::take(&mut lines),
                                })
                                .await;
                        }
                        lines.push(ObservationLine {
                            level: level_name(line.level).into(),
                            stream: stream_name(line.stream).into(),
                            message: line.message,
                        });
                    }
                }
            }
        });
        (logger, forward_loss)
    }

    /// 观测关闭：logger 静默丢弃（主进程未配置观测存储时不产生流量）。
    pub fn disabled_logger() -> NodeLogger {
        NodeLogger::disabled()
    }
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
