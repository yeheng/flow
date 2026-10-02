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

struct TransferBuffer {
    chunks: BTreeMap<u64, Vec<u8>>,
    received: u64,
    ready: Option<(u64, String)>,
    assembled: Option<Vec<u8>>,
}

impl IncomingTransfers {
    pub fn feed(&mut self, chunk: &Message) -> Result<(), TransferError> {
        let Message::TransferChunk {
            transfer_id,
            offset,
            bytes,
            digest,
        } = chunk
        else {
            return Ok(());
        };
        let stream = self.streams.entry(transfer_id.clone()).or_default();
        let raw = STANDARD
            .decode(bytes)
            .map_err(|_| TransferError::Digest(*offset))?;
        if hex::encode(Sha256::digest(&raw)) != *digest {
            return Err(TransferError::Digest(*offset));
        }
        // 幂等重传：同 offset 同内容跳过；异内容拒绝（三期重连补发）。
        if let Some(previous) = stream.chunks.get(offset) {
            if *previous == raw {
                return Ok(());
            }
            return Err(TransferError::Duplicate(transfer_id.clone(), *offset));
        }
        stream.received += raw.len() as u64;
        if stream.received > MAX_VALUE_BYTES {
            return Err(TransferError::Oversize(stream.received));
        }
        stream.chunks.insert(*offset, raw);
        self.try_assemble(transfer_id)
    }

    pub fn input_ready(&mut self, ready: &Message) -> Result<(), TransferError> {
        let Message::InputReady {
            transfer_id,
            total_bytes,
            digest,
        } = ready
        else {
            return Ok(());
        };
        let stream = self
            .streams
            .entry(transfer_id.clone())
            .or_insert(TransferBuffer {
                chunks: BTreeMap::new(),
                received: 0,
                ready: None,
                assembled: None,
            });
        stream.ready = Some((*total_bytes, digest.clone()));
        // control 帧可能先于 data 块到达（双通道乱序，二期 §3.6）：标记就绪
        // 后等块齐再装配，不在此处报错。
        self.try_assemble(transfer_id)
    }

    fn try_assemble(&mut self, transfer_id: &str) -> Result<(), TransferError> {
        let Some(stream) = self.streams.get_mut(transfer_id) else {
            return Err(TransferError::Incomplete(transfer_id.into()));
        };
        let Some((total, digest)) = stream.ready.clone() else {
            return Ok(()); // 就绪标记未到：继续等。
        };
        // 块未齐：等（不报错；块齐判定用字节数，块序在齐后校验）。
        if stream.received < total {
            return Ok(());
        }
        if stream.received > total {
            return Err(TransferError::Oversize(stream.received));
        }
        let mut assembled = Vec::with_capacity(total as usize);
        let mut expected = 0u64;
        for (offset, bytes) in &stream.chunks {
            if *offset != expected {
                return Err(TransferError::Incomplete(format!("gap at {expected}")));
            }
            expected += bytes.len() as u64;
            assembled.extend_from_slice(bytes);
        }
        if assembled.len() as u64 != total {
            return Err(TransferError::Incomplete(format!(
                "assembled {} of {total}",
                assembled.len()
            )));
        }
        if hex::encode(Sha256::digest(&assembled)) != digest {
            return Err(TransferError::Digest(total));
        }
        self.streams.get_mut(transfer_id).unwrap().assembled = Some(assembled);
        Ok(())
    }

    /// 取出装配完成的字节（InputReady 且校验通过）。保留缓存副本供同一
    /// 任务后续阶段（如 End 汇合）再次读取；任务结束时随装配器释放。
    pub fn take(&mut self, transfer_id: &str) -> Option<Vec<u8>> {
        let stream = self.streams.get_mut(transfer_id)?;
        if stream.ready.is_some() {
            stream.assembled.clone()
        } else {
            None
        }
    }

    /// 本地生成内容的注入（如刚捕获的 HTTP 原始响应），供后续输出派生
    /// 复用；与网络到达的 transfer 同一命名空间，幂等覆盖仅允许一次。
    pub fn insert_ready(&mut self, transfer_id: impl Into<String>, bytes: Vec<u8>) {
        let id = transfer_id.into();
        let stream = self.streams.entry(id).or_default();
        if stream.assembled.is_none() {
            let total = bytes.len() as u64;
            stream.ready = Some((total, hex::encode(Sha256::digest(&bytes))));
            stream.assembled = Some(bytes);
        }
    }

    pub fn is_ready(&self, transfer_id: &str) -> bool {
        self.streams
            .get(transfer_id)
            .is_some_and(|s| s.ready.is_some() && s.assembled.is_some())
    }
}

impl TransferBuffer {
    fn new() -> Self {
        Self {
            chunks: BTreeMap::new(),
            received: 0,
            ready: None,
            assembled: None,
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
        audit.push(
            "value_chunk",
            serde_json::to_value(&record).expect("chunk json"),
        )?;
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
    audit.push(
        "value_published",
        serde_json::to_value(&reference).expect("ref json"),
    )?;
    Ok(StoredValue::Ref(reference))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: &str, offset: u64, data: &[u8]) -> Message {
        Message::TransferChunk {
            transfer_id: id.into(),
            offset,
            bytes: STANDARD.encode(data),
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
        incoming.feed(&chunk("t", 10, b"late")).expect("stored");
        incoming
            .input_ready(&Message::InputReady {
                transfer_id: "t".into(),
                total_bytes: 4,
                digest: hex::encode(Sha256::digest(b"late")),
            })
            .expect_err("gap must fail");
        let mut bad = incoming;
        bad.feed(&chunk("u", 0, b"data")).unwrap();
        let error = bad.input_ready(&Message::InputReady {
            transfer_id: "u".into(),
            total_bytes: 4,
            digest: "00".repeat(32),
        });
        assert!(error.is_err());
    }
}
