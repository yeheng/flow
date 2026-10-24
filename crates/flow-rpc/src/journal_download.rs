//! Bounded value downloads using the same single-user token and projected run snapshot.
//!
//! 本模块负责「把 journal 里存的大值按字节流取回来」这一件事，边界很窄：
//! 只接受 (run_id, output_id) 这一对标识，返回**有界**的值体，不做查询、
//! 不做鉴权以外的策略判断。
//!
//! 它守护的核心不变量：**下载的字节一定来自权威 journal，且能被引用关系
//! 追溯到某个具体输出**。值不在投影快照里时，会回退到 tail 扫描历史事件
//! 找回引用——但引用必须指向当前 journal，否则不予返回（跨 journal 的
//! output_id 可能指向另一份数据集的同名文件）。
//!
//! 之所以要单独成模块：下载路径与 RPC 路径的失败语义完全不同（HTTP 状态码
//! + 流式 body，而非 JSON-RPC 错误对象），混在一起会让两边的错误分层互相
//! 污染。
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::Run;
use flow_journal::{StoredValue, ValueRef};
use std::{io::Write, sync::Arc};

/// 下载面共享状态：journal 句柄、单一用户 token 与并发闸门。
struct DownloadState {
    backend: Arc<JournalBackend>,
    token: String,
    slots: Arc<tokio::sync::Semaphore>,
}

/// 装配下载路由（`GET /runs/{run_id}/values/{output_id}`）。
///
/// # Errors
/// token 短于 32 字节时返回 Err：这条路由只靠这一个凭据防护，宁可起不来，
/// 也不能用弱 token 开放值下载。
pub fn router(
    backend: Arc<JournalBackend>,
    token: String,
) -> Result<Router, Box<dyn std::error::Error>> {
    if token.len() < 32 {
        return Err("download token must contain at least 32 bytes".into());
    }
    Ok(Router::new()
        .route(
            "/runs/{run_id}/values/{output_id}",
            get(download).options(|| async { StatusCode::NO_CONTENT }),
        )
        // CORS 只对下载面开放且只允许 GET/OPTIONS + Authorization 头：
        // 浏览器前端要能直接取二进制，但不能借此发起带凭据的写请求。
        .layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                let mut response = next.run(request).await;
                let headers = response.headers_mut();
                headers.insert("access-control-allow-origin", "*".parse().unwrap());
                headers.insert(
                    "access-control-allow-methods",
                    "GET, OPTIONS".parse().unwrap(),
                );
                headers.insert(
                    "access-control-allow-headers",
                    "Authorization".parse().unwrap(),
                );
                response
            },
        ))
        // 并发闸门：8 个在途下载。值可能很大，全量并发会把磁盘 IO 与内存
        // 打满，反而让正常的小值下载排队。
        .with_state(Arc::new(DownloadState {
            backend,
            token,
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
        })))
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
/// 经 mpsc 通道交给 HTTP body 流。
struct Chunks(tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>);
impl Write for Chunks {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // 分块切分是硬约束：单块过大会让 axum 在内存里攒一个巨大缓冲，
        // 取消的下载也无法及时释放。
        for bytes in bytes.chunks(flow_journal::CHUNK_BYTES) {
            self.0
                .blocking_send(Ok(Bytes::copy_from_slice(bytes)))
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "download disconnected")
                })?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// `GET /runs/{run_id}/values/{output_id}` 的鉴权入口。
///
/// 鉴权只有一种凭据：与 RPC 面同一个单用户 token。它是单用户凭据而不是
/// 多租户授权（DESIGN §0 C7），所以对外暴露必须由外层自备 TLS 与认证。
async fn download(
    State(state): State<Arc<DownloadState>>,
    Path((run_id, output_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    // 常时比较：不在第一个差异字节就返回，避免用响应时间探测 token。
    if supplied.len() != state.token.len()
        || supplied
            .bytes()
            .zip(state.token.bytes())
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            != 0
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    download_authorized(&state.backend, &state.slots, run_id, output_id).await
}

/// 已鉴权后的下载实现（`pub(crate)`：触发器面的公开结果下载复用同一段）。
///
/// 流程：抢并发闸门 → 读 run 投影 → 找值引用（快照优先，历史事件回退）
/// → 以流式 body 返回。失败语义一律映射为 HTTP 状态码，不复用 JSON-RPC
/// 的错误码。
pub(crate) async fn download_authorized(
    backend: &Arc<JournalBackend>,
    slots: &Arc<tokio::sync::Semaphore>,
    run_id: String,
    output_id: String,
) -> Response {
    // 非阻塞抢闸门：拿不到就立刻 429，而不是排队把调用方挂住——下载是
    // 交互式动作，用户宁可看到「忙」也不要界面卡住。
    let permit = match slots.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let (upper, row) = match backend.store.get_execution(&run_id).await {
        Ok(v) => v,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let run = match row.and_then(|v| serde_json::from_value::<Run>(v).ok()) {
        Some(v) => v,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let value = match reference(&run, &output_id) {
        // 引用必须属于当前 journal：否则 output_id 可能命中另一份数据集的
        // 同名文件，下载到的内容会与用户预期无关。
        Some(v) if v.journal_id == backend.journal.id() => v,
        _ => {
            // 快照里没有（老 run 被压缩掉了）→ 回退扫描历史事件。
            let root = backend.journal.root().to_path_buf();
            let journal = backend.journal.id().to_owned();
            let run = run_id.clone();
            let output = output_id.clone();
            match tokio::task::spawn_blocking(move || {
                historical_reference(&root, &journal, &run, &output, upper)
            })
            .await
            {
                Ok(Ok(Some(value))) => value,
                Ok(Ok(None)) => return StatusCode::NOT_FOUND.into_response(),
                _ => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            }
        }
    };
    let root = backend.journal.root().to_path_buf();
    // 有界通道（4 块）：让生产者与消费者形成背压，避免大值一次性灌进内存。
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tracing::info!(principal="local-token",%run_id,%output_id,action="value.download","JSONL value access");
    tokio::task::spawn_blocking(move || {
        // permit 随任务结束释放：闸门覆盖的是整个下载过程而非单次读。
        let _permit = permit;
        let mut chunks = Chunks(tx);
        if let Err(error) = flow_journal::value::read_value(&root, upper, &value, true, &mut chunks)
        {
            // 读失败必须以流内错误的形式传给客户端，而不是静默截断——
            // 静默截断会让客户端拿到一个「看起来完整」的坏文件。
            let _ = chunks
                .0
                .blocking_send(Err(std::io::Error::other(error.to_string())));
        }
    });
    let stream = futures::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    // No Content-Length: a corrupted or missing chunk propagates as a body-stream error.
    (
        [
            ("content-type", "application/octet-stream"),
            ("content-disposition", "attachment"),
            // no-store：值是用户数据，不能被中间缓存留存。
            ("cache-control", "no-store"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}
