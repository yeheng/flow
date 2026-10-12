//! 执行器侧审计流：有界生产 + 持久确认；远端中继另保留幂等重放。
//!
//! 本地流只发送一次；远端中继保存未确认记录用于重放。待发送与未确认记录
//! 共用窗口 W（编码后字节数）。AuditAck 只随主进程连续持久前缀前进；socket write/receive、
//! InputReady、Heartbeat 都不改变 D。满窗口时可被取消唤醒。

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use flow_engine::execution_protocol::contract::{
    ACK_RETRANSMIT_MS, DATA_MAX_FRAME, MAX_AUDIT_RECORD_BYTES, W_BYTES,
};
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
    #[error("audit production window full: {0} > {1}; use push_wait")]
    WindowFull(u64, u64),
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
    remote_replay: bool,
    pending_bytes: u64,
    cancel: tokio_util::sync::CancellationToken,
}

fn batch_overhead() -> u64 {
    // Include the routed remote envelope and JSON escaping of bounded identities.
    8192
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
            remote_replay: false,
            pending_bytes: 0,
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn configure(&mut self, remote_replay: bool, cancel: tokio_util::sync::CancellationToken) {
        self.remote_replay = remote_replay;
        self.cancel = cancel;
    }

    pub fn queued_bytes(&self) -> u64 {
        self.pending_bytes + self.unacked_bytes()
    }
    pub async fn await_published(&mut self, seq: u64) -> Result<(), AuditError> {
        let cancel = self.cancel.clone();
        self.await_durable(seq, &cancel).await
    }

    /// Reserve production bytes before creating another queued chunk.
    pub async fn push_wait(
        &mut self,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<u64, AuditError> {
        let bytes = record_bytes(self.next_seq, kind, &payload);
        let limit = self.window.min(MAX_AUDIT_RECORD_BYTES);
        if bytes > limit {
            return Err(AuditError::Oversize(bytes, limit));
        }
        while self.queued_bytes() + bytes > self.window {
            let cancel = self.cancel.clone();
            self.await_durable(self.durable_seq + 1, &cancel).await?;
        }
        self.push(kind, payload)
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
        let bytes = record.encoded_bytes();
        let limit = self.window.min(MAX_AUDIT_RECORD_BYTES);
        if bytes > limit {
            return Err(AuditError::Oversize(bytes, limit));
        }
        if self.queued_bytes() + bytes > self.window {
            return Err(AuditError::WindowFull(
                self.queued_bytes() + bytes,
                self.window,
            ));
        }
        self.pending_bytes += bytes;
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
                self.pending_bytes -= record_bytes;
                batch.push(self.pending.pop_front().unwrap());
            }
            let last_seq = batch.last().map(|r| r.audit_seq).unwrap_or(first_seq);
            self.sent_bytes += batch_bytes;
            self.sent_seq = last_seq;
            if self.remote_replay {
                for record in &batch {
                    self.inflight.push_back(record.clone());
                }
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

    /// 等待 `seq` 持久确认，可被取消或通道关闭唤醒。
    /// 仅远端中继按 ACK_RETRANSMIT_MS 重放同序号同内容的未确认前缀。
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
            tokio::select! {
                result = self.flush() => { result?; },
                _ = cancel.cancelled() => return Err(AuditError::Cancelled),
            }
            if self.durable_seq >= seq {
                return Ok(());
            }
            let retransmit_due = self
                .last_send
                .is_none_or(|at| at.elapsed() >= Duration::from_millis(ACK_RETRANSMIT_MS));
            if self.remote_replay && retransmit_due && !self.inflight.is_empty() {
                // 重传未确认前缀（内容必须一致）。
                let mut records = self.inflight.iter().peekable();
                while records.peek().is_some() {
                    let mut batch = Vec::new();
                    let mut bytes = batch_overhead();
                    while let Some(record) = records.peek() {
                        if bytes + record.encoded_bytes() > DATA_MAX_FRAME as u64 {
                            break;
                        }
                        bytes += record.encoded_bytes();
                        batch.push((*records.next().unwrap()).clone());
                    }
                    let first_seq = batch[0].audit_seq;
                    tokio::select! {
                        result = self.outbound.send(Message::AuditBatch {dispatch_id:self.dispatch_id.clone(),first_seq,records:batch}) => {result.map_err(|_|AuditError::Closed)?;},
                        _ = cancel.cancelled() => return Err(AuditError::Cancelled),
                    }
                }
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
                _ = tokio::time::sleep(Duration::from_millis(ACK_RETRANSMIT_MS / 2 + 1)), if self.remote_replay => {}
            }
        }
    }
}

/// 校验编码字节数与主进程侧同一规则一致（跨进程一致性测试用）。
pub fn encoded_len(record: &AuditRecord) -> u64 {
    record_bytes(record.audit_seq, &record.kind, &record.payload)
}
