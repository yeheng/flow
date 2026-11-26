use super::{internal, JournalBackend, Result};
use flow_domain::journal_state::Run;
use flow_journal::{StoredValue, ValueRef};
use std::io::Write;
use tokio::sync::{mpsc, OwnedSemaphorePermit};

/// Resolve and stream a run-bound value at the same committed upper bound.
pub async fn value_download(
    backend: &JournalBackend,
    run_id: &str,
    output_id: &str,
    permit: OwnedSemaphorePermit,
) -> Result<Option<mpsc::Receiver<std::io::Result<Vec<u8>>>>> {
    let (upper, row) = backend
        .store
        .get_execution(run_id)
        .await
        .map_err(internal)?;
    let Some(run) = row.and_then(|v| serde_json::from_value::<Run>(v).ok()) else {
        return Ok(None);
    };
    let root = backend.journal.root().to_path_buf();
    let journal = backend.journal.id().to_owned();
    let value = match reference(&run, output_id) {
        Some(value) if value.journal_id == journal => value,
        _ => {
            let root = root.clone();
            let run_id = run_id.to_owned();
            let output_id = output_id.to_owned();
            let fallback = tokio::task::spawn_blocking(move || {
                historical_reference(&root, &journal, &run_id, &output_id, upper)
            })
            .await
            .map_err(internal)??;
            let Some(value) = fallback else {
                return Ok(None);
            };
            value
        }
    };
    let (tx, rx) = mpsc::channel(4);
    tracing::info!(principal="local-token",%run_id,%output_id,action="value.download","JSONL value access");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut chunks = Chunks(tx);
        if let Err(error) = flow_journal::value::read_value(&root, upper, &value, true, &mut chunks)
        {
            // Report corruption through the body stream instead of silently truncating it.
            let _ = chunks
                .0
                .blocking_send(Err(std::io::Error::other(error.to_string())));
        }
    });
    Ok(Some(rx))
}

/// 在 run 投影快照里找 output_id 对应的值引用。
///
/// 覆盖 run 的全部可见值位（input / definition / output / 各节点 output /
/// prepared 输入与参数 / wait 输出 / operation 请求与结果）——只要引用在
/// 快照里出现过，就算这个 run 的合法输出。
fn reference(run: &Run, id: &str) -> Option<ValueRef> {
    let mut values = vec![&run.input, &run.definition];
    values.extend(run.output.iter());
    for node in run.nodes.values() {
        values.extend(node.output.iter());
        if let Some(prepared) = &node.prepared {
            values.push(&prepared.input);
            values.push(&prepared.params);
            values.extend(prepared.predecessors.values());
        }
        if let Some(wait) = &node.wait {
            values.extend(wait.output());
        }
        if let Some(op) = &node.operation {
            values.push(&op.request);
            values.extend(op.outcome.iter());
            // 外部调用的原始响应体里可能内嵌了值引用（body_raw），
            // 它同样属于这个 run 的可下载输出。
            if let Some(StoredValue::Inline(outcome)) = &op.outcome {
                if let Ok(raw) = serde_json::from_value::<ValueRef>(outcome["body_raw"].clone()) {
                    if raw.output_id == id {
                        return Some(raw);
                    }
                }
            }
        }
    }
    values.into_iter().find_map(|v| match v {
        StoredValue::Ref(r) if r.output_id == id => Some(r.clone()),
        _ => None,
    })
}

/// 在任意 JSON 载荷里递归查找 output_id 对应的值引用。
///
/// 用于历史事件载荷：事件的 payload 形状随事件种类变化，值引用可能嵌在
/// 任意深度，只有按 output_id 匹配才能可靠找回。
fn payload_reference(payload: &serde_json::Value, id: &str) -> Option<ValueRef> {
    if payload.get("output_id").and_then(serde_json::Value::as_str) == Some(id) {
        if let Ok(value) = serde_json::from_value::<ValueRef>(payload.clone()) {
            // validate 是结构自检：形状不对的引用会指向不存在的文件。
            if value.validate().is_ok() {
                return Some(value);
            }
        }
    }
    match payload {
        serde_json::Value::Object(fields) => fields.values().find_map(|v| payload_reference(v, id)),
        serde_json::Value::Array(values) => values.iter().find_map(|v| payload_reference(v, id)),
        _ => None,
    }
}

/// 回退路径：在 journal 历史事件里找回某个 output_id 的值引用。
///
/// 投影快照会滚动压缩，早期 run 的输出可能只剩引用而快照里已无对应条目；
/// 这时扫描历史事件。journal_id 必须匹配——同名 output_id 可能来自另一份
/// 数据集，串台会导致下载到错误内容。
fn historical_reference(
    root: &std::path::Path,
    journal: &str,
    run: &str,
    id: &str,
    upper: u64,
) -> flow_journal::Result<Option<ValueRef>> {
    let mut tail = flow_journal::tail::TailReader::for_run(root, journal, run);
    while let Some((tx, _)) = tail.next(upper)? {
        for event in tx.events {
            if event.run_id.as_deref() == Some(run) {
                if let Some(value) = payload_reference(&event.payload, id) {
                    if value.journal_id == journal {
                        return Ok(Some(value));
                    }
                }
            }
        }
    }
    Ok(None)
}

/// 把阻塞式写适配成异步流：`Write` 实现把字节切成 CHUNK_BYTES 的块，
/// 经 mpsc 通道交给调用方。
struct Chunks(tokio::sync::mpsc::Sender<std::io::Result<Vec<u8>>>);
impl Write for Chunks {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // 分块切分是硬约束：单块过大会让调用方在内存里攒一个巨大缓冲，
        // 取消的下载也无法及时释放。
        for bytes in bytes.chunks(flow_journal::CHUNK_BYTES) {
            self.0.blocking_send(Ok(bytes.to_vec())).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "download disconnected")
            })?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
