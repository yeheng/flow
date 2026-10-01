//! JSONL triggers: stable fire identity and run creation share one authoritative command.
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Local, Utc};
use flow_backend::journal::{JournalBackend, JournalError};
use serde_json::{json, Value};
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
            continue;
        };
        let Some(fire) = cron.previous_before(&(now + chrono::Duration::seconds(1))) else {
            continue;
        };
        let key = fire.with_timezone(&Utc).to_rfc3339();
        if let Err(error) = backend
            .trigger_start("schedule", id, &key, Value::Null)
            .await
        {
            tracing::warn!(schedule_id=%id,%error,"JSONL scheduled command did not become visible");
        }
    }
}
pub fn start(backend: Arc<JournalBackend>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(20));
        loop {
            tick.tick().await;
            fire_due(&backend, Local::now()).await;
        }
    })
}
struct HookState {
    backend: Arc<JournalBackend>,
    token: String,
}
pub fn router(backend: Arc<JournalBackend>, token: String) -> Router {
    Router::new()
        .route("/hooks/{id}", post(hook))
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(Arc::new(HookState { backend, token }))
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
        Err(JournalError::CommittedNotVisible(receipt)) => (
            StatusCode::ACCEPTED,
            Json(json!({"code":"COMMITTED_NOT_VISIBLE","receipt":receipt})),
        )
            .into_response(),
        Err(JournalError::Journal(flow_journal::Error::Conflict(_))) => {
            StatusCode::CONFLICT.into_response()
        }
        Err(JournalError::Journal(flow_journal::Error::Invalid(_))) => {
            StatusCode::BAD_REQUEST.into_response()
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
