//! 输入分块传输装配与输出值分块（二期 §3.3，I06 执行器侧）。
//!
//! 输入是主进程已知的权威内容：TransferChunk(offset, bytes, digest) +
//! InputReady(total, digest)。接收完成即释放传输缓冲语义（对执行器而言
//! 是装配完成）；InputReady 只表示输入可用，不释放审计持久窗口。
//!
//! 输出复用一期连续 chunk 语义：ValueChunk(ValueCodec 编码字节按 256 KiB
//! 分块、base64) + ValuePublished(固定描述) 审计记录，主进程按同一规则
//! 持久化，journal 字节与一期进程内路径一致。

use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;
use sha2::{Digest, Sha256};

use flow_engine::execution_protocol::message::Message;
use flow_journal::value::{Chunk, ValueCodec, ValueRef};
use flow_journal::{StoredValue, CHUNK_BYTES, INLINE_BYTES, MAX_VALUE_BYTES};

use crate::audit::AuditStream;

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("transfer chunk digest mismatch at offset {0}")]
    Digest(u64),
    #[error("transfer incomplete: {0}")]
    Incomplete(String),
    #[error("transfer size exceeds budget: {0}")]
    Oversize(u64),
    #[error("duplicate transfer chunk: {0}@{1}")]
    Duplicate(String, u64),
}

/// 输入装配器：按 transfer_id 聚合。
#[derive(Default)]
pub struct IncomingTransfers {
    streams: BTreeMap<String, TransferBuffer>,
}

/// 单个 transfer 的连续接收缓冲（T07）：v4 单流下块按序到达，接收端只
/// 维护连续前缀（`buf`）。`offset == 已接收长度` 时追加；完全落在已接收
/// 前缀且字节相同的是幂等重复（重连后全量重发）；缺口、跨边界重叠或异
/// 内容一律拒绝。不再维护块树与装配副本（消除排序 + 整体二次拷贝）。
struct TransferBuffer {
    buf: Vec<u8>,
    ready: Option<(u64, String)>,
    assembled: bool,
}

impl IncomingTransfers {
    pub fn feed(&mut self, chunk: &Message) -> Result<(), TransferError> {
        let Message::TransferChunk {
            // 路由字段由主进程/中继消费；执行器会话单任务，不校验归属。
            dispatch_id: _,
            transfer_id,
            offset,
            bytes,
            digest,
        } = chunk
        else {
            return Ok(());
        };
        if transfer_id.len() > 256
            || (!self.streams.contains_key(transfer_id) && self.streams.len() >= 4096)
        {
            return Err(TransferError::Oversize(self.streams.len() as u64));
        }
        if bytes.len() > CHUNK_BYTES {
            return Err(TransferError::Oversize(bytes.len() as u64));
        }
        // T12：内存中 bytes 已是原始字节（v3 wire 的 base64 在解码层还原）。
        let raw = bytes.clone();
        if hex::encode(Sha256::digest(&raw)) != *digest {
            return Err(TransferError::Digest(*offset));
        }
        let end = offset
            .checked_add(raw.len() as u64)
            .ok_or(TransferError::Oversize(*offset))?;
        let limit = if transfer_id.starts_with("in:") {
            crate::task::INPUT_BUDGET as u64
        } else {
            MAX_VALUE_BYTES
        };
        if raw.is_empty() || raw.len() > CHUNK_BYTES || end > limit {
            return Err(TransferError::Oversize(end));
        }
        let total: u64 = self.streams.values().map(|s| s.buf.len() as u64).sum();
        let inputs: u64 = self
            .streams
            .iter()
            .filter(|(id, _)| id.starts_with("in:"))
            .map(|(_, s)| s.buf.len() as u64)
            .sum();
        let stream = self.streams.entry(transfer_id.clone()).or_default();
        let received = stream.buf.len() as u64;
        if end <= received {
            // 幂等重复：完全落在已接收前缀且字节相同（重连后全量重发）。
            if stream.buf[*offset as usize..end as usize] == raw[..] {
                return Ok(());
            }
            return Err(TransferError::Duplicate(transfer_id.clone(), *offset));
        }
        if *offset != received {
            // 缺口或跨边界重叠：单流按序到达下不存在合法乱序，直接拒绝。
            if *offset > received {
                return Err(TransferError::Incomplete(format!(
                    "gap: offset {offset} > received {received}"
                )));
            }
            return Err(TransferError::Duplicate(transfer_id.clone(), *offset));
        }
        if let Some((total_ready, _)) = &stream.ready {
            if end > *total_ready {
                return Err(TransferError::Oversize(end));
            }
        }
        if total + raw.len() as u64 > MAX_VALUE_BYTES
            || (transfer_id.starts_with("in:")
                && inputs + raw.len() as u64 > crate::task::INPUT_BUDGET as u64)
        {
            return Err(TransferError::Oversize(total + raw.len() as u64));
        }
        stream.buf.extend_from_slice(&raw);
        self.try_assemble(transfer_id)
    }

    pub fn input_ready(&mut self, ready: &Message) -> Result<(), TransferError> {
        let Message::InputReady {
            dispatch_id: _,
            transfer_id,
            total_bytes,
            digest,
        } = ready
        else {
            return Ok(());
        };
        let limit = if transfer_id.starts_with("in:") {
            crate::task::INPUT_BUDGET as u64
        } else {
            MAX_VALUE_BYTES
        };
        if *total_bytes > limit
            || transfer_id.len() > 256
            || (!self.streams.contains_key(transfer_id) && self.streams.len() >= 4096)
        {
            return Err(TransferError::Oversize(*total_bytes));
        }
        let stream = self
            .streams
            .entry(transfer_id.clone())
            .or_insert(TransferBuffer {
                buf: Vec::new(),
                ready: None,
                assembled: false,
            });
        if let Some(previous) = &stream.ready {
            if *previous != (*total_bytes, digest.clone()) {
                return Err(TransferError::Incomplete("conflicting readiness".into()));
            }
            if stream.assembled {
                return Ok(());
            }
        }
        stream.ready = Some((*total_bytes, digest.clone()));
        // 标记就绪后等前缀长齐再校验装配（InputReady 与块到达顺序无关）。
        self.try_assemble(transfer_id)
    }

    fn try_assemble(&mut self, transfer_id: &str) -> Result<(), TransferError> {
        let Some(stream) = self.streams.get_mut(transfer_id) else {
            return Err(TransferError::Incomplete(transfer_id.into()));
        };
        let Some((total, digest)) = stream.ready.clone() else {
            return Ok(()); // 就绪标记未到：继续等。
        };
        let received = stream.buf.len() as u64;
        if received < total {
            return Ok(());
        }
        if received > total {
            return Err(TransferError::Oversize(received));
        }
        // 整值摘要：连续前缀即完整内容，无需排序与二次拷贝（T07）。
        if hex::encode(Sha256::digest(&stream.buf)) != digest {
            return Err(TransferError::Digest(total));
        }
        stream.assembled = true;
        Ok(())
    }

    /// 取出装配完成的字节（InputReady 且校验通过）。保留缓存副本供同一
    /// 任务后续阶段（如 End 汇合）再次读取；任务结束时随装配器释放。
    pub fn take(&mut self, transfer_id: &str) -> Option<Vec<u8>> {
        let stream = self.streams.get_mut(transfer_id)?;
        if stream.ready.is_some() && stream.assembled {
            Some(stream.buf.clone())
        } else {
            None
        }
    }

    /// 本地生成内容的注入（如刚捕获的 HTTP 原始响应），供后续输出派生
    /// 复用；与网络到达的 transfer 同一命名空间，幂等覆盖仅允许一次。
    pub fn insert_ready(&mut self, transfer_id: impl Into<String>, bytes: Vec<u8>) {
        let id = transfer_id.into();
        let stream = self.streams.entry(id).or_default();
        if !stream.assembled {
            let total = bytes.len() as u64;
            stream.ready = Some((total, hex::encode(Sha256::digest(&bytes))));
            stream.buf = bytes;
            stream.assembled = true;
        }
    }

    pub fn is_ready(&self, transfer_id: &str) -> bool {
        self.streams
            .get(transfer_id)
            .is_some_and(|s| s.ready.is_some() && s.assembled)
    }
}

impl TransferBuffer {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            ready: None,
            assembled: false,
        }
    }
}

impl Default for TransferBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// 执行器侧大值编码：≤ INLINE_BYTES 直接 inline；否则按一期 Capture 语义
/// 产出 ValueChunk/ValuePublished 审计记录并返回 Ref。journal 中的记录
/// 字节与一期 `Attempt::json` 完全一致（journal_id 来自会话身份）。
pub async fn store_value(
    audit: &mut AuditStream,
    journal_id: &str,
    value: &Value,
    limit: usize,
) -> Result<StoredValue, crate::task::TaskError> {
    let bytes = flow_journal::codec::bounded_json(value, limit)?;
    flow_journal::codec::validate_depth(value, 0)?;
    store_bytes(audit, journal_id, &bytes, ValueCodec::Json).await
}

/// 任意字节（HTTP 原始响应）走 Bytes 编码。
pub async fn store_bytes(
    audit: &mut AuditStream,
    journal_id: &str,
    bytes: &[u8],
    codec: ValueCodec,
) -> Result<StoredValue, crate::task::TaskError> {
    if bytes.len() <= INLINE_BYTES && codec == ValueCodec::Json {
        // inline 语义要求反序列化成功（与一期 StoredValue::inline 一致）。
        if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
            return Ok(StoredValue::Inline(value));
        }
    }
    store_bytes_ref(audit, journal_id, bytes, codec).await
}

/// [`store_bytes`] 的分块分支：只发审计分块 + 发布，绝不 inline。
pub async fn store_bytes_ref(
    audit: &mut AuditStream,
    journal_id: &str,
    bytes: &[u8],
    codec: ValueCodec,
) -> Result<StoredValue, crate::task::TaskError> {
    if bytes.len() as u64 > MAX_VALUE_BYTES {
        return Err(TransferError::Oversize(bytes.len() as u64).into());
    }
    let output_id = uuid::Uuid::now_v7().to_string();
    let mut index = 0u64;
    let mut total = 0u64;
    let mut hash = Sha256::new();
    for chunk in bytes.chunks(CHUNK_BYTES) {
        let record = Chunk {
            output_id: output_id.clone(),
            chunk_index: index,
            data: STANDARD.encode(chunk),
        };
        audit
            .push_wait(
                "value_chunk",
                serde_json::to_value(&record).expect("chunk json"),
            )
            .await?;
        hash.update(chunk);
        total += chunk.len() as u64;
        index += 1;
    }
    let reference = ValueRef {
        journal_id: journal_id.to_string(),
        output_id,
        codec,
        version: 1,
        chunk_count: index,
        total_bytes: total,
        digest: hex::encode(hash.finalize()),
    };
    let seq = audit
        .push_wait(
            "value_published",
            serde_json::to_value(&reference).expect("ref json"),
        )
        .await?;
    // Publication is a barrier: callers can safely queue small metadata afterwards.
    audit.await_published(seq).await?;
    Ok(StoredValue::Ref(reference))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: &str, offset: u64, data: &[u8]) -> Message {
        Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: id.into(),
            offset,
            bytes: data.to_vec(),
            digest: hex::encode(Sha256::digest(data)),
        }
    }

    #[test]
    fn transfers_assemble_only_after_input_ready() {
        let mut incoming = IncomingTransfers::default();
        let data = b"hello world".to_vec();
        incoming
            .feed(&chunk("t1", 0, &data))
            .expect("chunk accepted");
        assert!(incoming.take("t1").is_none(), "InputReady 未到不可取");
        incoming
            .input_ready(&Message::InputReady {
                dispatch_id: "d".into(),
                transfer_id: "t1".into(),
                total_bytes: data.len() as u64,
                digest: hex::encode(Sha256::digest(&data)),
            })
            .expect("ready");
        assert_eq!(incoming.take("t1").unwrap(), data);
    }

    #[test]
    fn gap_and_digest_rejected() {
        let mut incoming = IncomingTransfers::default();
        // T07：单流按序到达，非连续 offset 不再暂存等 ready，feed 时立即拒绝。
        incoming
            .feed(&chunk("t", 10, b"late"))
            .expect_err("gap must fail at feed");
        // 连续前缀正确接收后装配校验整值摘要。
        let mut ok = IncomingTransfers::default();
        ok.feed(&chunk("t2", 0, b"late")).unwrap();
        ok.input_ready(&Message::InputReady {
            dispatch_id: "d".into(),
            transfer_id: "t2".into(),
            total_bytes: 4,
            digest: hex::encode(Sha256::digest(b"late")),
        })
        .expect("contiguous transfer assembles");
        let mut bad = incoming;
        bad.feed(&chunk("u", 0, b"data")).unwrap();
        let error = bad.input_ready(&Message::InputReady {
            dispatch_id: "d".into(),
            transfer_id: "u".into(),
            total_bytes: 4,
            digest: "00".repeat(32),
        });
        assert!(error.is_err());
    }
}
