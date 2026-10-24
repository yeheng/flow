//! 输入分块传输装配与输出值分块（二期 §3.3，I06 执行器侧）。
//!
//! 输入是主进程已知的权威内容：TransferChunk(offset, bytes, digest) +
//! InputReady(total, digest)。接收完成即释放传输缓冲语义（对执行器而言
//! 是装配完成）；InputReady 只表示输入可用，不释放审计持久窗口。
//!
//! 输出复用一期连续 chunk 语义：ValueChunk(ValueCodec 编码字节按 256 KiB
//! 分块、base64) + ValuePublished(固定描述) 审计记录，主进程按同一规则
//! 持久化，journal 字节与一期进程内路径一致。
//!
//! 本模块守住的契约是「字节要么完整校验通过、要么不存在」：块摘要
//! 逐块校验、整值摘要在装配完成时校验、长度与总量受预算封顶。任何
//! 半截的输入若被放行，后面的模板展开与脚本求值会读到损坏的 JSON，
//! 而错误会伪装成用户代码的失败——那是最难排查的一类偏差。

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;
use sha2::{Digest, Sha256};

use flow_engine::execution_protocol::message::Message;
use flow_journal::value::{Chunk, ValueCodec, ValueRef};
use flow_journal::{StoredValue, CHUNK_BYTES, INLINE_BYTES, MAX_VALUE_BYTES};

use crate::audit::AuditStream;

/// 传输装配故障：装配失败一律拒绝该派发，不做「尽力而为」的部分使用。
#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    /// 块摘要与字节不符：传输损坏或被篡改。
    #[error("transfer chunk digest mismatch at offset {0}")]
    Digest(u64),
    /// 装配不完整：缺块、就绪声明冲突或流不存在。
    #[error("transfer incomplete: {0}")]
    Incomplete(String),
    /// 超出体积或条数预算。
    #[error("transfer size exceeds budget: {0}")]
    Oversize(u64),
    /// 重复块的内容与已收前缀不符。
    #[error("duplicate transfer chunk: {0}@{1}")]
    Duplicate(String, u64),
}

/// 输入装配器：按 transfer_id 聚合。
///
/// 生命周期覆盖一次派发：装配、派生输出、恢复重放都从这里取字节。
#[derive(Default)]
pub struct IncomingTransfers {
    streams: BTreeMap<String, TransferBuffer>,
}

/// 单个 transfer 的连续接收缓冲（T07）：v4 单流下块按序到达，接收端只
/// 维护连续前缀（`buf`）。`offset == 已接收长度` 时追加；完全落在已接收
/// 前缀且字节相同的是幂等重复（重连后全量重发）；缺口、跨边界重叠或异
/// 内容一律拒绝。不再维护块树与装配副本（消除排序 + 整体二次拷贝）。
/// 装配期独占可写（Arc::make_mut 无克隆）；装配完成后 [`IncomingTransfers::take`]
/// 只发共享句柄，重复读（End 汇合、恢复重放）不再整段复制。
struct TransferBuffer {
    buf: Arc<Vec<u8>>,
    ready: Option<(u64, String)>,
    assembled: bool,
}

impl IncomingTransfers {
    /// 接收一个数据块并累加到对应流的连续前缀。
    ///
    /// # Errors
    /// 摘要不符、长度超限、出现缺口或重叠冲突时返回错误（不缓存半截
    /// 数据，避免后续误用）。
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
            // transfer_id 长度与并发流条数双限额：id 长度防畸形标识撑爆
            // 内存，条数上限防大量短流耗尽执行器内存。
            return Err(TransferError::Oversize(self.streams.len() as u64));
        }
        if bytes.len() > CHUNK_BYTES {
            return Err(TransferError::Oversize(bytes.len() as u64));
        }
        // T12：内存中 bytes 已是原始字节（v3 wire 的 base64 在解码层还原）。
        if hex::encode(Sha256::digest(bytes)) != *digest {
            // 逐块摘要：损坏要在写入缓冲前发现，不能等整值摘要。
            return Err(TransferError::Digest(*offset));
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(TransferError::Oversize(*offset))?;
        // 输入面（`in:` 前缀）按执行器输入预算封顶，其余按单值上限封顶。
        let limit = if transfer_id.starts_with("in:") {
            crate::task::INPUT_BUDGET as u64
        } else {
            MAX_VALUE_BYTES
        };
        if bytes.is_empty() || bytes.len() > CHUNK_BYTES || end > limit {
            return Err(TransferError::Oversize(end));
        }
        let total: u64 = self.streams.values().map(|s| s.buf.len() as u64).sum();
        // 输入流单独累计：单值上限 8 MiB 挡不住「N 个各 8 MiB 的输入」
        // 一起把执行器内存撑爆。
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
            if stream.buf[*offset as usize..end as usize] == bytes[..] {
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
        if total + bytes.len() as u64 > MAX_VALUE_BYTES
            || (transfer_id.starts_with("in:")
                && inputs + bytes.len() as u64 > crate::task::INPUT_BUDGET as u64)
        {
            return Err(TransferError::Oversize(total + bytes.len() as u64));
        }
        Arc::make_mut(&mut stream.buf).extend_from_slice(bytes);
        self.try_assemble(transfer_id)
    }

    /// 记录一条流的「就绪」声明（总长 + 整值摘要），并尝试完成装配。
    ///
    /// # Errors
    /// 总长超限、id 畸形、或与既有就绪声明冲突时返回错误。
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
                buf: Arc::new(Vec::new()),
                ready: None,
                assembled: false,
            });
        if let Some(previous) = &stream.ready {
            if *previous != (*total_bytes, digest.clone()) {
                // 同一流出现两份不同的就绪声明：无法判定哪份为真，
                // 只能拒绝（静默取其一等于把不确定当确定）。
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

    /// 尝试把某条流标记为「已装配」。
    ///
    /// 装配完成的判据是三个条件同时成立：就绪声明已到、已收字节数等于
    /// 声明总长、整值摘要匹配。任一不满足就继续等或报错，绝不「差不多
    /// 就用」。
    ///
    /// # Errors
    /// 流不存在、超出声明总长或摘要不符时返回错误。
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
        if hex::encode(Sha256::digest(stream.buf.as_slice())) != digest {
            return Err(TransferError::Digest(total));
        }
        stream.assembled = true;
        Ok(())
    }

    /// 取出装配完成的字节（InputReady 且校验通过）。返回共享句柄——缓存
    /// 保留给同一任务后续阶段（如 End 汇合）重复读取，重复读零拷贝；
    /// 任务结束时随装配器释放。
    pub fn take(&self, transfer_id: &str) -> Option<Arc<Vec<u8>>> {
        let stream = self.streams.get(transfer_id)?;
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
            stream.buf = Arc::new(bytes);
            stream.assembled = true;
        }
    }

    /// 该流是否已就绪且装配完成（可取用）。
    pub fn is_ready(&self, transfer_id: &str) -> bool {
        self.streams
            .get(transfer_id)
            .is_some_and(|s| s.ready.is_some() && s.assembled)
    }
}

impl TransferBuffer {
    /// 新建空缓冲。
    fn new() -> Self {
        Self {
            buf: Arc::new(Vec::new()),
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
///
/// # Errors
/// 值编码超预算、深度超限或分块发布失败时返回错误。
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
///
/// # Errors
/// 字节超单值上限或发布失败时返回错误。
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
///
/// # Errors
/// 字节超单值上限或审计发布失败时返回错误。
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
        // 分块逐条入审计流：每条都占窗口额度，所以 push_wait 会在窗口
        // 满时等待主进程确认，而不是把内存撑住。
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
    //
    // 发布记录是屏障：主进程按它才知道这些块属于同一个值，且必须先落
    // 盘；此后才允许追加引用它的小元数据。
    audit.await_published(seq).await?;
    Ok(StoredValue::Ref(reference))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一个合法的 TransferChunk 测试帧。
    fn chunk(id: &str, offset: u64, data: &[u8]) -> Message {
        Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: id.into(),
            offset,
            bytes: data.to_vec(),
            digest: hex::encode(Sha256::digest(data)),
        }
    }

    /// InputReady 未到不得取用；重复读取共享同一份字节。
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
        assert_eq!(incoming.take("t1").unwrap().as_slice(), data.as_slice());
        // 重复读共享同一份字节（Arc），不再整段克隆。
        let first = incoming.take("t1").unwrap();
        let second = incoming.take("t1").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    /// 缺口在 feed 时即被拒；整值摘要在装配时校验。
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
