use super::*;
use crate::codec::{bounded_json, Event, EventKind};
use crate::{
    invalid, Error, Journal, QueueClass, Result, CHUNK_BYTES, INLINE_BYTES, MAX_VALUE_BYTES,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::Path;

/// A command-local value. Large JSON stays in an anonymous, delete-on-close
/// file until the writer commits it with the command in one SQLite transaction.
///
/// 已准备好但尚未发布的大值。值由匿名临时文件承载，直到随命令一起提交；
/// 命令失败时文件随对象一起消失，不会留下无主的值正文。
pub struct PreparedValue {
    stored: StoredValue,
    staged: Option<StagedValue>,
}
/// 已暂存的大值与其占用的字节配额。
///
/// 配额（`_permit`）在对象被丢弃时自动归还，保证暂存内存总量始终有界。
pub struct StagedValue {
    reference: ValueRef,
    file: std::fs::File,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl PreparedValue {
    /// 把一个 JSON 值准备成「可内联或可暂存」的形式。
    ///
    /// 小于内联上限的值直接内联；更大的值写入匿名文件并生成描述符，字节
    /// 配额随对象一起持有。
    pub(crate) fn json(
        journal_id: &str,
        value: Value,
        limit: usize,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Self> {
        use std::io::{Seek, SeekFrom};
        let bytes = bounded_json(&value, limit)?;
        if bytes.len() <= INLINE_BYTES {
            return Ok(Self {
                stored: StoredValue::Inline(value),
                staged: None,
            });
        }
        let reference = ValueRef {
            journal_id: journal_id.into(),
            output_id: uuid::Uuid::now_v7().to_string(),
            codec: ValueCodec::Json,
            version: 1,
            chunk_count: bytes.len().div_ceil(CHUNK_BYTES) as u64,
            total_bytes: bytes.len() as u64,
            digest: crate::codec::digest(&bytes),
        };
        reference.validate()?;
        let mut file = tempfile::tempfile()?;
        file.write_all(&bytes)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            stored: StoredValue::Ref(reference.clone()),
            staged: Some(StagedValue {
                reference,
                file,
                _permit: permit,
            }),
        })
    }
    /// 取准备好的值形态（内联或引用）。
    pub fn stored(&self) -> &StoredValue {
        &self.stored
    }
    /// 取出可暂存的部分；内联值返回 `None`。
    pub fn into_staged(self) -> Option<StagedValue> {
        self.staged
    }
}
impl StagedValue {
    /// 生成与该值对应的发布事实。
    ///
    /// 发布事实必须出现在最终事务里，否则值会成为没有事实引用的孤儿块。
    pub fn publication(&self) -> Result<Event> {
        Ok(Event::new(
            EventKind::ValuePublished,
            serde_json::to_value(&self.reference)?,
        ))
    }
    /// 该值会被切成多少块。
    pub fn chunk_count(&self) -> u64 {
        self.reference.chunk_count
    }
    /// Metadata is sufficient for reducer preview and apply: raw chunk bytes
    /// are validated by the writer and rehydrated only by journal readers.
    ///
    /// 返回每个分块事务的元数据（正文为 null）：reducer 预演只需要块的存在
    /// 与顺序，真正的字节由写者校验、由读者复原。
    pub fn prefix(&self, first_lsn: u64) -> Vec<crate::Transaction> {
        (0..self.reference.chunk_count).map(|index| crate::Transaction {
            v: 2, journal_id: self.reference.journal_id.clone(), lsn: first_lsn + index,
            tx_id: uuid::Uuid::now_v7().to_string(),
            events: vec![Event::new(EventKind::ValueChunk, serde_json::json!({
                "output_id":self.reference.output_id, "chunk_index":index.to_string(), "data":null,
            }))],
        }).collect()
    }
    /// 读出一个分块的实际字节（base64 编码后返回）。
    ///
    /// 期望长度由总字节数与块序号推出：最后一块是余数，其余都是满块。
    pub(crate) fn read_chunk(&mut self, index: u64) -> Result<Chunk> {
        let expected = (self.reference.total_bytes - index * CHUNK_BYTES as u64)
            .min(CHUNK_BYTES as u64) as usize;
        let mut bytes = vec![0; expected];
        self.file.read_exact(&mut bytes)?;
        Ok(Chunk {
            output_id: self.reference.output_id.clone(),
            chunk_index: index,
            data: STANDARD.encode(bytes),
        })
    }
}

/// Scan only this object's chunks. Memory is bounded by one transaction and one raw chunk.
/// A candidate may be checked before publication; consumers must set require_published=true.
/// Bytes can be delivered before the final checksum: callers must abort their stream on error.
///
/// 只扫描本对象的分块：内存占用限于一个事务与一块原始字节。
/// `require_published=false` 时可以在发布前先检查候选值（用于预演）；
/// 真正的消费方必须置 `true`。
/// 注意字节可能在最终摘要校验之前就被写出：调用方必须在出错时中止自己的
/// 流，否则会消费到一段后来被证明不完整的内容。
pub fn read_value(
    root: &Path,
    upper_lsn: u64,
    value: &ValueRef,
    require_published: bool,
    out: &mut impl Write,
) -> Result<()> {
    value.validate()?;
    let conn = crate::sqlite::read_connection(root)?;
    // 整个读取在一个只读快照里：避免读到写者提交一半的中间状态。
    conn.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let (identity, durable) = crate::sqlite::metadata(&conn)?;
    if identity != value.journal_id || upper_lsn > durable {
        return Err(invalid("foreign value dataset/boundary"));
    }
    let publication = crate::sqlite::entity(&conn, "published", &value.output_id)?;
    let published = if let Some(record) = publication {
        let lsn = record["lsn"]
            .as_str()
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| invalid("invalid publication sequence"))?;
        if lsn <= upper_lsn {
            if serde_json::from_value::<ValueRef>(record["value"].clone())? != *value {
                return Err(invalid("mismatched publication"));
            }
            true
        } else {
            false
        }
    } else {
        false
    };
    // 要求已发布时，未发布（含「发布在快照之外」）一律拒绝。
    if require_published && !published {
        return Err(invalid("value is not published"));
    }
    let mut index = 0;
    let mut total = 0;
    let mut hash = Sha256::new();
    let mut statement=conn.prepare("SELECT chunk_index,bytes,digest FROM value_chunks WHERE output_id=? AND lsn<=? ORDER BY chunk_index").map_err(crate::sqlite::db)?;
    let mut rows = statement
        .query(rusqlite::params![
            value.output_id,
            format!("{upper_lsn:020}")
        ])
        .map_err(crate::sqlite::db)?;
    while let Some(row) = rows.next().map_err(crate::sqlite::db)? {
        let chunk_index: i64 = row.get(0).map_err(crate::sqlite::db)?;
        let bytes: Vec<u8> = row.get(1).map_err(crate::sqlite::db)?;
        let digest: String = row.get(2).map_err(crate::sqlite::db)?;
        // 分块必须连续、非空且不超限，并与自己记录的摘要相符。
        if chunk_index != index as i64
            || bytes.is_empty()
            || bytes.len() > CHUNK_BYTES
            || crate::codec::digest(&bytes) != digest
        {
            return Err(invalid("non-contiguous or corrupt chunk"));
        }
        total += bytes.len() as u64;
        if total > value.total_bytes {
            return Err(invalid("value length exceeds descriptor"));
        }
        hash.update(&bytes);
        out.write_all(&bytes)?;
        index += 1;
    }
    if index != value.chunk_count
        || total != value.total_bytes
        || hex::encode(hash.finalize()) != value.digest
        || (require_published && !published)
    {
        return Err(invalid("value incomplete, unpublished or digest mismatch"));
    }
    Ok(())
}

/// Bounded producer. Callers supply a real stream; no whole-body buffering before chunking.
/// Execution audit identity is attached at the execution facade, not manufactured here.
///
/// 有界生产者：调用方提供真实流，切块前不整体缓冲。执行审计身份由执行门面
/// 附加，这里不凭空制造。
pub async fn store_stream(
    journal: &Journal,
    input: &mut (impl tokio::io::AsyncRead + Unpin),
    codec: ValueCodec,
) -> Result<ValueRef> {
    store_stream_using(journal.id(), input, codec, |event| async {
        let class = if event.kind == EventKind::ValueChunk {
            QueueClass::Audit(
                event.payload["output_id"]
                    .as_str()
                    .ok_or_else(|| invalid("value chunk missing output_id"))?
                    .into(),
            )
        } else {
            QueueClass::Control
        };
        journal
            .submit(uuid::Uuid::now_v7().to_string(), vec![event], class)
            .await?;
        Ok(())
    })
    .await
}

/// Share chunk encoding while letting the backend enforce its normal fact admission.
///
/// 复用同一套分块编码，但把「如何提交」交给调用方：backend 可以走自己的
/// 事实准入通道，而不是绕过它。
pub async fn store_stream_using<F, Fut, E>(
    journal_id: &str,
    input: &mut (impl tokio::io::AsyncRead + Unpin),
    codec: ValueCodec,
    mut submit: F,
) -> std::result::Result<ValueRef, E>
where
    F: FnMut(Event) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), E>>,
    E: From<Error>,
{
    use tokio::io::AsyncReadExt;
    let output_id = uuid::Uuid::now_v7().to_string();
    let mut hash = Sha256::new();
    let mut total = 0;
    let mut index = 0;
    let mut buffer = vec![0; CHUNK_BYTES];
    loop {
        let n = input.read(&mut buffer).await.map_err(Error::from)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        // 超限时保留已捕获的前缀：调用方据此判断续传还是放弃，丢弃前缀等于
        // 白做。
        if total > MAX_VALUE_BYTES {
            return Err(Error::Limit("value bytes; captured prefix retained".into()).into());
        }
        hash.update(&buffer[..n]);
        let chunk = Chunk {
            output_id: output_id.clone(),
            chunk_index: index,
            data: STANDARD.encode(&buffer[..n]),
        };
        submit(Event::new(
            EventKind::ValueChunk,
            serde_json::to_value(chunk).map_err(Error::from)?,
        ))
        .await?;
        index += 1;
    }
    let value = ValueRef {
        journal_id: journal_id.into(),
        output_id,
        codec,
        version: 1,
        chunk_count: index,
        total_bytes: total,
        digest: hex::encode(hash.finalize()),
    };
    submit(Event::new(
        EventKind::ValuePublished,
        serde_json::to_value(&value).map_err(Error::from)?,
    ))
    .await?;
    Ok(value)
}

/// 存一个 JSON 值：小值内联，大值转为分块存储。
///
/// # Errors
/// 物化超限、流式写入失败或超出行上限时返回错误。
pub async fn store_json(
    journal: &Journal,
    value: Value,
    materialization_limit: usize,
) -> Result<StoredValue> {
    let bytes = bounded_json(&value, materialization_limit)?;
    if bytes.len() <= INLINE_BYTES {
        return Ok(StoredValue::Inline(value));
    }
    let mut input = bytes.as_slice();
    Ok(StoredValue::Ref(
        store_stream(journal, &mut input, ValueCodec::Json).await?,
    ))
}

/// 把存储的值物化回内存 JSON。
///
/// 引用值只允许物化为 JSON 或 UTF-8；二进制值无法表示成 JSON，直接报错，
/// 而不是产出一个含义不明的值。
///
/// # Errors
/// 超出物化上限、值未发布或摘要不符时返回错误。
pub fn materialize(
    root: &Path,
    upper_lsn: u64,
    value: &StoredValue,
    limit: usize,
) -> Result<Value> {
    match value {
        StoredValue::Inline(value) => {
            bounded_json(value, limit.min(INLINE_BYTES))?;
            Ok(value.clone())
        }
        StoredValue::Ref(reference) => {
            if reference.total_bytes > limit as u64 {
                return Err(Error::Limit("aggregate materialization".into()));
            }
            let mut out = crate::codec::BoundedBuffer {
                bytes: Vec::new(),
                limit,
            };
            read_value(root, upper_lsn, reference, true, &mut out)?;
            match reference.codec {
                ValueCodec::Json => Ok(serde_json::from_slice(&out.bytes)?),
                ValueCodec::Utf8 => Ok(Value::String(
                    String::from_utf8(out.bytes).map_err(|e| invalid(e.to_string()))?,
                )),
                ValueCodec::Bytes => Err(invalid("binary value cannot be materialized as JSON")),
            }
        }
    }
}

/// Compose one JSON object from stored entries without materializing whole
/// values: inline entries serialize through serde, references stream through
/// the fixed 64 KiB pipe. Fan-in does not materialize whole objects. The
/// returned task must be joined; dropping the reader cancels production.
///
/// 用存储条目合成一个 JSON 对象而不整体物化：内联条目经 serde 序列化，引用
/// 条目经固定 64 KiB 管道流式写出。扇入过程不物化整个对象。
/// 返回的任务必须被 join；丢弃读端会取消生产，合成结果也就不会完整。
pub fn compose_object(
    root: std::path::PathBuf,
    upper: u64,
    entries: Vec<(String, StoredValue)>,
) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<Result<()>>) {
    let (reader, writer) = tokio::io::duplex(64 * 1024);
    let task = tokio::task::spawn_blocking(move || {
        let mut writer = tokio_util::io::SyncIoBridge::new(writer);
        let mut object = ObjectWriter::begin(&mut writer)?;
        for (key, value) in entries {
            object.key(&key)?;
            match value {
                StoredValue::Inline(value) => {
                    bounded_json(&value, INLINE_BYTES)?;
                    serde_json::to_writer(object.value_writer(), &value)?;
                }
                StoredValue::Ref(value) => {
                    if value.codec != ValueCodec::Json {
                        return Err(invalid("non-JSON reference in JSON composition"));
                    }
                    read_value(&root, upper, &value, true, object.value_writer())?;
                }
            }
        }
        object.finish()?;
        writer.flush()?;
        Ok(())
    });
    (reader, task)
}
