//! JSONL triggers: stable fire identity and run creation share one authoritative command.
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Local, Utc};
use flow_backend::journal::{JournalBackend, JournalError};
use serde_json::Value;
use std::sync::Arc;

pub async fn fire_due(backend: &JournalBackend, now: DateTime<Local>) {
    for config in backend.trigger_configs("schedule").await {
        if config["enabled"] != true {
            continue;
        }
        let Some(id) = config["id"].as_str() else {
            continue;
        };
        let Ok(cron) = config["cron_expr"]
            .as_str()
            .unwrap_or("")
            .parse::<cron_parser::Schedule>()
        else {
            // 坏 cron 的静默跳过曾让 schedule 永不触发且无迹可查——至少留一条日志。
            tracing::warn!(schedule_id = %id, "schedule cron invalid; fire skipped");
            continue;
        };
        let Some(fire) = cron.previous_before(&(now + chrono::Duration::nanoseconds(1))) else {
            continue;
        };
        // Older configurations have no activation timestamp; their creation time
        // is the lower bound. Never interpret a historical cron slot as a new fire.
        let active_since = config["active_since"]
            .as_str()
            .or_else(|| config["created_at"].as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
        if active_since.is_none_or(|since| fire < since) {
            continue;
        }
        let key = fire.with_timezone(&Utc).to_rfc3339();
        if let Err(error) = backend
            .trigger_start("schedule", id, &key, Value::Null)
            .await
        {
            tracing::warn!(schedule_id=%id,%error,"JSONL scheduled command did not become visible");
        }
    }
}
/// journal 侧触发器扫描循环；tick 由 `[server].journal_trigger_tick_secs` 配置。
pub fn start(
    backend: Arc<JournalBackend>,
    tick: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(tick);
        loop {
            tick.tick().await;
            fire_due(&backend, Local::now()).await;
        }
    })
}
struct HookState {
    backend: Arc<JournalBackend>,
    token: String,
    downloads: Arc<tokio::sync::Semaphore>,
}
pub fn router(backend: Arc<JournalBackend>, token: String) -> Router {
    Router::new()
        .route("/hooks/{id}", post(hook))
        .route("/invoke/{id}", post(invoke).options(preflight))
        .route(
            "/invocations/{id}",
            get(invocation_result).options(preflight),
        )
        .route(
            "/invocations/{id}/events",
            get(invocation_events).options(preflight),
        )
        .route(
            "/invocations/{id}/values/{output}",
            get(invocation_value).options(preflight),
        )
        .route(
            "/invocations/{id}/signals/{node}",
            post(invocation_signal).options(preflight),
        )
        .route(
            "/invocations/{id}/cancel",
            post(invocation_cancel).options(preflight),
        )
        .layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                let business = request.uri().path().starts_with("/invoke/")
                    || request.uri().path().starts_with("/invocations/");
                let mut response = next.run(request).await;
                if business {
                    let headers = response.headers_mut();
                    for (key, value) in [
                        ("cache-control", "no-store"),
                        ("access-control-allow-origin", "*"),
                        ("access-control-allow-methods", "GET, POST, OPTIONS"),
                        (
                            "access-control-allow-headers",
                            "Authorization, Content-Type, Idempotency-Key, X-Correlation-Id",
                        ),
                    ] {
                        headers.insert(
                            axum::http::HeaderName::from_static(key),
                            axum::http::HeaderValue::from_static(value),
                        );
                    }
                }
                response
            },
        ))
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(Arc::new(HookState {
            backend,
            token,
            downloads: Arc::new(tokio::sync::Semaphore::new(8)),
        }))
}
async fn preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}
fn equal(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |d, (a, b)| d | (a ^ b)) == 0
}
fn request_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128)
}

/// HMAC-SHA256 capabilities bind one run and one operation-independent namespace.
/// The management token is never sent to an invocation client.
fn run_token(secret: &str, run: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut key = [0u8; 64];
    let raw = secret.as_bytes();
    if raw.len() > 64 {
        key[..32].copy_from_slice(&Sha256::digest(raw));
    } else {
        key[..raw.len()].copy_from_slice(raw);
    }
    let mut inner = Sha256::new();
    inner.update(key.map(|b| b ^ 0x36));
    inner.update(b"flow.invocation.v1\0");
    inner.update(run.as_bytes());
    let mut outer = Sha256::new();
    outer.update(key.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    hex::encode(outer.finalize())
}
fn run_authorized(state: &HookState, id: &str, headers: &HeaderMap) -> bool {
    equal(bearer(headers), &run_token(&state.token, id))
}
fn invocation_error(error: JournalError) -> Response {
    match error {
        JournalError::Journal(flow_journal::Error::Conflict(message)) => {
            (StatusCode::CONFLICT, message).into_response()
        }
        JournalError::Journal(flow_journal::Error::Invalid(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        JournalError::Journal(flow_journal::Error::Limit(message)) => {
            (StatusCode::TOO_MANY_REQUESTS, message).into_response()
        }
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
async fn invoke(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key = bearer(&headers);
    let hash = flow_journal::codec::digest(key.as_bytes());
    let config = state
        .backend
        .trigger_configs("webhook")
        .await
        .into_iter()
        .find(|c| c["token"] == id);
    if key.len() < 32
        || !config.as_ref().is_some_and(|c| {
            c["invoke_key_hash"]
                .as_str()
                .is_some_and(|expected| equal(expected, &hash))
        })
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(request) = request_key(&headers) else {
        return (StatusCode::BAD_REQUEST, "Idempotency-Key required").into_response();
    };
    let correlation = headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(request);
    let input = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        }
    };
    match state
        .backend
        .invoke_start(&id, request, input, correlation, &hash)
        .await
    {
        Ok(receipt) => {
            let Some(id) = receipt.result["run_id"].as_str() else {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            };
            (StatusCode::ACCEPTED,Json(serde_json::json!({"run_id":id,"run_token":run_token(&state.token,id),"result_url":format!("/invocations/{id}"),"events_url":format!("/invocations/{id}/events"),"receipt":receipt}))).into_response()
        }
        Err(e) => invocation_error(e),
    }
}
async fn invocation_value(
    State(state): State<Arc<HookState>>,
    Path((id, output)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // End users may download the public result, not definitions, prepared
    // credentials or arbitrary intermediate values from the management trace.
    match state.backend.run_snapshot(&id).await {
        Ok(Some(run)) if matches!(run.output.as_ref(),Some(flow_journal::StoredValue::Ref(v)) if v.output_id==output) =>
            {}
        Ok(Some(_)) => return StatusCode::FORBIDDEN.into_response(),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return invocation_error(e),
    }
    crate::journal_download::download_authorized(&state.backend, &state.downloads, id, output).await
}
async fn invocation_result(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.backend.run_snapshot(&id).await {
        Ok(Some(run)) => {
            let output = run
                .output
                .as_ref()
                .map(|v| {
                    flow_journal::value::materialize(
                        state.backend.journal.root(),
                        state.backend.journal.durable_lsn(),
                        v,
                        8 * 1024 * 1024,
                    )
                })
                .transpose();
            match output{
                Ok(output)=>Json(serde_json::json!({"run_id":id,"status":run.status,"complete":run.terminal(),"output":output,"error":run.error.as_ref().map(|_|"WORKFLOW_FAILED"),"workflow_version":run.workflow_version,"correlation_id":run.execution.as_ref().and_then(|e|e.correlation_id.as_ref())})).into_response(),
                Err(flow_journal::Error::Limit(_))=>(StatusCode::PAYLOAD_TOO_LARGE,Json(serde_json::json!({"run_id":id,"status":run.status,"complete":run.terminal(),"output_ref":run.output,"download_url":run.output.as_ref().and_then(|v|match v {flow_journal::StoredValue::Ref(v)=>Some(format!("/invocations/{id}/values/{}",v.output_id)),_=>None})}))).into_response(),
                Err(_)=>StatusCode::SERVICE_UNAVAILABLE.into_response(),
            }
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => invocation_error(e),
    }
}
#[derive(serde::Deserialize)]
struct EventQuery {
    cursor: Option<String>,
    after_lsn: Option<String>,
}
async fn invocation_events(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<EventQuery>,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let upper = state.backend.journal.durable_lsn();
    let cursor = match query.cursor {
        Some(raw) => match serde_json::from_str(&raw) {
            Ok(c) => c,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        None => {
            let mut cursor = flow_journal::page::Cursor::first(
                state.backend.journal.id().into(),
                id.clone(),
                flow_journal::page::Filter::Events,
                upper,
            );
            if let Some(raw) = query.after_lsn {
                let Ok(after) = raw.parse::<u64>() else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                if after.to_string() != raw || after > upper {
                    return StatusCode::BAD_REQUEST.into_response();
                }
                let Some(next) = after.checked_add(1) else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                cursor.next_lsn = next;
            }
            cursor
        }
    };
    match flow_journal::page::page(
        state.backend.journal.root(),
        state.backend.journal.id(),
        &id,
        flow_journal::page::Filter::Events,
        cursor,
        upper,
        100,
    ) {
        Ok(mut page) => {
            use flow_journal::EventKind;
            page.events.retain(|v| {
                matches!(
                    v.event.kind,
                    EventKind::RunStarted
                        | EventKind::RunCompleted
                        | EventKind::RunFailed
                        | EventKind::RunCancelled
                        | EventKind::DispatchStarted
                        | EventKind::NodeCompleted
                        | EventKind::NodeFailed
                        | EventKind::NodeSkipped
                        | EventKind::WaitRegistered
                        | EventKind::WaitResolved
                        | EventKind::SignalReceived
                )
            });
            for positioned in &mut page.events {
                let event = &mut positioned.event;
                event.payload = match event.kind {
                    EventKind::RunStarted => {
                        serde_json::json!({"workflow_version":event.payload["workflow_version"]})
                    }
                    EventKind::WaitRegistered => {
                        serde_json::json!({"wait":{"kind":event.payload["wait"]["kind"],"wake_at":event.payload["wait"]["wake_at"]}})
                    }
                    EventKind::NodeSkipped => serde_json::json!({"reason":event.payload["reason"]}),
                    EventKind::NodeFailed | EventKind::RunFailed => {
                        serde_json::json!({"error":"EXECUTION_FAILED"})
                    }
                    _ => serde_json::json!({}),
                };
            }
            Json(page).into_response()
        }
        Err(e) => invocation_error(e.into()),
    }
}
async fn invocation_signal(
    State(state): State<Arc<HookState>>,
    Path((id, node)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(request) = request_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let input = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    // A business capability cannot adjudicate external operations, including through run.signal's legacy alias.
    let run = match state.backend.run_snapshot(&id).await {
        Ok(Some(r)) => r,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let definition = match flow_journal::value::materialize(
        state.backend.journal.root(),
        state.backend.journal.durable_lsn(),
        &run.definition,
        flow_journal::MAX_LINE_BYTES,
    )
    .and_then(|v| flow_engine::journal_state::validate_definition(&v))
    {
        Ok(d) => d,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    if !definition.node_type(&node).is_some_and(|k| {
        matches!(
            k,
            flow_engine::NodeType::HumanTask | flow_engine::NodeType::Input
        )
    }) {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state
        .backend
        .run_signal(&id, &node, input, Some(request))
        .await
    {
        Ok(r) => Json(r).into_response(),
        Err(e) => invocation_error(e),
    }
}
async fn invocation_cancel(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(request) = request_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match state.backend.run_cancel(&id, Some(request)).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => invocation_error(e),
    }
}
async fn hook(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    if token.len() != state.token.len()
        || token
            .bytes()
            .zip(state.token.bytes())
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            != 0
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(request) = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128)
    else {
        return (StatusCode::BAD_REQUEST, "Idempotency-Key required").into_response();
    };
    let input = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        }
    };
    match state
        .backend
        .trigger_start("webhook", &id, request, input)
        .await
    {
        Ok(receipt) => Json(receipt).into_response(),
        Err(error) => invocation_error(error),
    }
}
