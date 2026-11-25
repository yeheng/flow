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
//!   污染。
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use flow_backend::journal::JournalBackend;
use std::sync::Arc;

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
    let resources = service_ui_router(backend.clone());
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
        }))
        .merge(resources))
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
    let rx = match flow_backend::journal_views::value_download(backend, &run_id, &output_id, permit)
        .await
    {
        Ok(Some(body)) => body,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
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

/// Read-only ephemeral plugin documents. This exposes no value downloads or RPC methods.
pub fn service_ui_router(backend: Arc<JournalBackend>) -> Router {
    Router::new()
        .route("/plugin-ui/{id}", get(plugin_ui))
        .with_state(backend)
}

async fn plugin_ui(State(state): State<Arc<JournalBackend>>, Path(id): Path<String>) -> Response {
    let Some(html) = state.service_document(&id).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        [("content-type","text/html; charset=utf-8"),
         ("content-security-policy","default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'; base-uri 'none'; form-action 'none'; navigate-to 'none'; sandbox allow-scripts"),
         ("cache-control","no-store"),
         ("referrer-policy","no-referrer"),
         ("x-content-type-options","nosniff")],html
    ).into_response()
}

#[cfg(test)]
mod service_ui_tests {
    use super::*;
    #[tokio::test]
    async fn ui_resources_are_bounded_handles_with_isolation_headers() {
        let root = std::env::temp_dir().join(format!(
            "flow-service-ui-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let backend = JournalBackend::open(&root, Default::default())
            .await
            .unwrap();
        backend.service_ui.lock().await.insert(
            "resource-handle".into(),
            (
                "instance".into(),
                "<!doctype html><title>fixture</title>".into(),
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let routes = router(backend.clone(), "a".repeat(32)).unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, routes).await.unwrap();
        });
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /plugin-ui/resource-handle HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("sandbox allow-scripts"));
        assert!(response.contains("navigate-to 'none'"));
        assert!(response.contains("connect-src 'none'"));
        assert!(response.contains("cache-control: no-store"));
        assert!(response.contains("<title>fixture</title>"));
        backend.service_ui.lock().await.clear();
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /plugin-ui/resource-handle HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.unwrap();
        response.clear();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 404"));
        task.abort();
        let _ = task.await;
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
