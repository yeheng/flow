//! harness agent e2e 的 mock OpenAI 兼容 provider：可编排的多轮 SSE 响应
//! + 请求捕获（Bearer/请求体），让全链路测试不依赖真实模型服务。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use serde_json::{json, Value};

/// 一次 provider 调用的编排：要么给 tool_calls（多轮），要么给最终文本。
#[derive(Clone)]
pub enum Turn {
    /// 让模型调用 bash 工具（命令原样透传），下一轮拿到工具结果。
    Tool { command: String },
    /// 最终文本（finish_reason=stop）。
    Final { text: String },
    /// HTTP 500（transport 失败路径）。
    InternalError,
}

/// 被捕获的一次请求：Bearer 头 + 解析后的请求体。
#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub bearer: String,
    pub body: Value,
}

type Shared = (
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<CapturedRequest>>>,
    Vec<Turn>,
);

pub struct MockProvider {
    pub calls: Arc<AtomicUsize>,
    pub captured: Arc<Mutex<Vec<CapturedRequest>>>,
    pub addr: std::net::SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl MockProvider {
    pub fn hits(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.captured.lock().unwrap().clone()
    }

    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    pub async fn stop(self) {
        self.handle.abort();
    }
}

fn sse_body(chunks: &[Value]) -> String {
    let mut body = String::new();
    for chunk in chunks {
        body.push_str(&format!("data: {chunk}\n\n"));
    }
    body.push_str("data: [DONE]\n\n");
    body
}

async fn completions(
    State((calls, captured, turns)): State<Shared>,
    headers: axum::http::HeaderMap,
    body: String,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let turn = calls.fetch_add(1, Ordering::SeqCst);
    captured.lock().unwrap().push(CapturedRequest {
        bearer: headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        body: serde_json::from_str(&body).unwrap_or(Value::Null),
    });
    let chunks: Vec<Value> = match turns.get(turn).unwrap_or(turns.last().unwrap()) {
        Turn::Tool { command } => {
            let arguments = serde_json::to_string(&json!({ "command": command })).unwrap();
            vec![
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"bash","arguments":arguments}}]}}]}),
                json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
            ]
        }
        Turn::Final { text } => vec![
            json!({"choices":[{"delta":{"content":text}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        ],
        Turn::InternalError => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "mock provider exploded",
            )
                .into_response();
        }
    };
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        sse_body(&chunks),
    )
        .into_response()
}

/// 起一个 mock provider：`turns` 按调用序消费（超出后重复最后一项）。
pub async fn spawn_mock_openai(turns: Vec<Turn>) -> MockProvider {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shared: Shared = (calls.clone(), captured.clone(), turns);
    let app = axum::Router::new()
        .route("/v1/chat/completions", axum::routing::post(completions))
        .with_state(shared);
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    MockProvider {
        calls,
        captured,
        addr,
        handle,
    }
}
