//! webhook 触发器的 HTTP 入口（独立于 JSON-RPC 的 WebSocket 端口）：
//! `POST /hook/:token`，body 作为 run 的 input。
//!
//! 语义：token 未知或已禁用 → 404（不区分，避免探测）；body 非合法 JSON → 400；
//! workflow 无 published 版本 → 409；成功 → 200 `{"run_id": "..."}`。

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use flow_backend::CreateRun;

use crate::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/hook/{token}", post(hook))
        .with_state(state)
}

async fn hook(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    body: Bytes,
) -> Response {
    let webhook = match state.backend.get_webhook(&token).await {
        Ok(Some(webhook)) if webhook.enabled => webhook,
        Ok(_) => {
            return error(StatusCode::NOT_FOUND, "webhook 不存在或已禁用");
        }
        Err(err) => {
            tracing::error!(error = %err, "webhook 查询失败");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "内部错误");
        }
    };

    // body 必须是 JSON（空 body 视为 null 输入）；不要求 Content-Type，
    // webhook 调用方经常不带
    let input: Value = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(input) => input,
            Err(err) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    format!("body 不是合法 JSON：{err}"),
                )
            }
        }
    };

    // 无 published 版本是调用方能感知的状态（409），单独前置检查给出明确语义
    match state.backend.latest_published(&webhook.workflow_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error(
                StatusCode::CONFLICT,
                format!("workflow {} 没有已发布版本", webhook.workflow_id),
            );
        }
        Err(err) => {
            tracing::error!(error = %err, "webhook 查询 published 版本失败");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "内部错误");
        }
    }

    match state
        .backend
        .create_run(CreateRun {
            workflow_id: webhook.workflow_id,
            version: None,
            input,
        })
        .await
    {
        Ok(created) => (StatusCode::OK, Json(json!({ "run_id": created.run_id }))).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "webhook 触发 run 失败");
            error(StatusCode::INTERNAL_SERVER_ERROR, "内部错误")
        }
    }
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}
