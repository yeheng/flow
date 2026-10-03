//! 执行器侧审计流：持久确认窗口 + 幂等重传（二期 §3.5，I05）。
//!
//! 发送端保存原始编码用于重传；窗口按 `B(S) - B(D) ≤ W` 计费（编码后
//! 字节数）。AuditAck 只随主进程连续持久前缀前进；socket write/receive、
//! InputReady、Heartbeat 都不改变 D。满窗口时可被取消唤醒。

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use flow_engine::execution_protocol::contract::{ACK_RETRANSMIT_MS, DATA_MAX_FRAME, W_BYTES};
use flow_engine::execution_protocol::message::{AuditRecord, Message};
use flow_engine::execution_protocol::record_bytes;

#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    #[error("audit stream closed")]
    Closed,
    #[error("audit wait cancelled")]
    Cancelled,
    #[error("audit record exceeds window: {0} > {1}")]
    Oversize(u64, u64),
}

/// 单派发审计流。dispatch 内从 1 开始；重传不重复占用逻辑窗口，但受
/// 重传节奏限制，防止重复流量淹没接收端。
pub struct AuditStream {
    dispatch_id: String,
    outbound: mpsc::Sender<Message>,
    /// 尚未发送（或因窗口暂缓）的记录，按序。
    pending: VecDeque<AuditRecord>,
    /// 已发送未确认（重传副本源），按序。
    inflight: VecDeque<AuditRecord>,
    /// 下一条 audit_seq。
    next_seq: u64,
    /// B(S)：已发送最高连续序号的累计字节。
    sent_bytes: u64,
    sent_seq: u64,
    /// D：主进程确认的最高连续持久序号及其累计字节。
    durable_seq: u64,
    durable_bytes: u64,
    window: u64,
    acks: watch::Receiver<(u64, u64)>,
    last_send: Option<tokio::time::Instant>,
}

fn batch_overhead() -> u64 {
    128
}

impl AuditStream {
    pub fn new(
        dispatch_id: String,
        outbound: mpsc::Sender<Message>,
        window: u64,
        acks: watch::Receiver<(u64, u64)>,
    ) -> Self {
        Self {
            dispatch_id,
            outbound,
            pending: VecDeque::new(),
            inflight: VecDeque::new(),
            next_seq: 1,
            sent_bytes: 0,
            sent_seq: 0,
            durable_seq: 0,
            durable_bytes: 0,
            window: window.min(W_BYTES),
            acks,
            last_send: None,
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn durable_seq(&self) -> u64 {
        self.durable_seq
    }

    /// 主进程 AuditAck 到达（运行时转发）。只单调前进。
    pub fn ack(&mut self, durable_seq: u64, durable_bytes: u64) {
        if durable_seq >= self.durable_seq {
            self.durable_seq = durable_seq;
            self.durable_bytes = durable_bytes;
            while self
                .inflight
                .front()
                .is_some_and(|record| record.audit_seq <= durable_seq)
            {
                self.inflight.pop_front();
            }
        }
    }

    fn unacked_bytes(&self) -> u64 {
        self.sent_bytes.saturating_sub(self.durable_bytes)
    }

    /// 追加一条记录（audit_seq 自动分配）。单条记录必须能被窗口容纳。
    pub fn push(&mut self, kind: &str, payload: serde_json::Value) -> Result<u64, AuditError> {
        let seq = self.next_seq;
        let record = AuditRecord {
            audit_seq: seq,
            kind: kind.into(),
            payload,
        };
        if record.encoded_bytes() > self.window {
            return Err(AuditError::Oversize(record.encoded_bytes(), self.window));
        }
        self.next_seq += 1;
        self.pending.push_back(record);
        Ok(seq)
    }

    /// 尽力发送：按窗口许可把 pending 打批发出去。写不动（outbound 满）
    /// 自然背压。返回本轮发出的条数。
    pub async fn flush(&mut self) -> Result<usize, AuditError> {
        let mut sent = 0;
        while let Some(first) = self.pending.front() {
            let unacked = self.unacked_bytes();
            let first_bytes = first.encoded_bytes();
            let first_seq = first.audit_seq;
            if unacked + first_bytes > self.window {
                break;
            }
            // 组批：窗口许可内的连续记录合并为一个 AuditBatch。
            let mut batch: Vec<AuditRecord> = Vec::new();
            let mut batch_bytes = 0u64;
            while let Some(record) = self.pending.front() {
                let record_bytes = record.encoded_bytes();
                if unacked + batch_bytes + record_bytes > self.window
                    || batch_bytes + record_bytes + batch_overhead() > DATA_MAX_FRAME as u64
                {
                    break;
                }
                batch_bytes += record_bytes;
                batch.push(self.pending.pop_front().unwrap());
            }
            let last_seq = batch.last().map(|r| r.audit_seq).unwrap_or(first_seq);
            self.sent_bytes += batch_bytes;
            self.sent_seq = last_seq;
            for record in &batch {
                self.inflight.push_back(record.clone());
            }
            self.outbound
                .send(Message::AuditBatch {
                    dispatch_id: self.dispatch_id.clone(),
                    first_seq,
                    records: batch,
                })
                .await
                .map_err(|_| AuditError::Closed)?;
            sent += 1;
            self.last_send = Some(tokio::time::Instant::now());
        }
        Ok(sent)
    }

    /// 等待 `seq` 持久确认；期间按 ACK_RETRANSMIT_MS 节奏重传未确认前缀
    /// （同序号同内容幂等）。可被取消唤醒。
    pub async fn await_durable(
        &mut self,
        seq: u64,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<(), AuditError> {
        loop {
            if self.durable_seq >= seq {
                return Ok(());
            }
            // 先尝试发送更多（可能有等待窗口的记录）。
            self.flush().await?;
            if self.durable_seq >= seq {
                return Ok(());
            }
            let retransmit_due = self
                .last_send
                .is_none_or(|at| at.elapsed() >= Duration::from_millis(ACK_RETRANSMIT_MS));
            if retransmit_due && !self.inflight.is_empty() {
                // 重传未确认前缀（内容必须一致）。
                let batch: Vec<AuditRecord> = self.inflight.iter().cloned().collect();
                let first_seq = batch.first().map(|r| r.audit_seq).unwrap_or(1);
                self.outbound
                    .send(Message::AuditBatch {
                        dispatch_id: self.dispatch_id.clone(),
                        first_seq,
                        records: batch,
                    })
                    .await
                    .map_err(|_| AuditError::Closed)?;
                self.last_send = Some(tokio::time::Instant::now());
            }
            // clone 携带的是 self.acks 的旧版本：changed() 在 clone 上恒立即
            // 就绪 → 等待循环退化成 CPU 热自旋。处理完变更后必须把自身
            // receiver 的版本同步到最新，下一轮 clone 才会真正等待。
            let mut acks = self.acks.clone();
            tokio::select! {
                changed = acks.changed() => {
                    if changed.is_err() {
                        return Err(AuditError::Closed);
                    }
                    let (seq_now, bytes_now) = *acks.borrow_and_update();
                    self.ack(seq_now, bytes_now);
                    let _ = self.acks.borrow_and_update();
                }
                _ = cancel.cancelled() => return Err(AuditError::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(ACK_RETRANSMIT_MS / 2 + 1)) => {}
            }
        }
    }
}

/// 校验编码字节数与主进程侧同一规则一致（跨进程一致性测试用）。
pub fn encoded_len(record: &AuditRecord) -> u64 {
    record_bytes(record.audit_seq, &record.kind, &record.payload)
}
