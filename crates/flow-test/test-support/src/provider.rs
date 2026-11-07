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
    /// Holds a real request open, for deadline/cancel and mixed-load checks.
    DelayedFinal { text: String, delay_ms: u64 },
    /// 等待测试放行，验证运行结束前就能看到进度日志。
    GatedFinal {
        text: String,
        gate: Arc<tokio::sync::Notify>,
    },
    /// Reusable per-session two-turn script, safe under concurrent requests.
    ToolThenFinal {
        command: String,
        text: String,
        delay_ms: u64,
    },
    /// HTTP 500（transport 失败路径）。
    InternalError,
}

/// 被捕获的一次请求：Bearer 头 + 解析后的请求体。
#[derive(Clone, Debug)]
pub struct CapturedRequest {
    /// `Authorization` 头的原值（通常形如 `Bearer sk-...`）。
    ///
    /// 断言它是为了确认**密钥确实走了请求头**而不是被塞进 body 或环境变量。
    pub bearer: String,
    /// 解析后的 JSON 请求体（解析失败时为 `Null`）。
    pub body: Value,
}

/// provider 的共享状态：调用计数、请求捕获与编排脚本。
type Shared = (
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<CapturedRequest>>>,
    Vec<Turn>,
);

/// 运行中的 mock provider 句柄。
pub struct MockProvider {
    /// 已处理的请求数（每次请求自增）。
    pub calls: Arc<AtomicUsize>,
    /// 捕获到的全部请求。
    pub captured: Arc<Mutex<Vec<CapturedRequest>>>,
    /// 监听地址。
    pub addr: std::net::SocketAddr,
    /// 服务任务句柄；`stop` 时中止它。
    handle: tokio::task::JoinHandle<()>,
}

impl MockProvider {
    /// 已收到的请求次数（用 SeqCst 读取：测试要看到确定的先后顺序）。
    pub fn hits(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// 取出全部捕获到的请求（克隆一份，避免持锁跨越断言）。
    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.captured.lock().unwrap().clone()
    }

    /// provider 的 base URL（供被测配置写入 `harness.agent.base_url`）。
    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    /// 停止服务：直接 abort 任务，因为测试结束时不关心优雅停机。
    pub async fn stop(self) {
        self.handle.abort();
    }
}

/// 把若干 chunk 拼成 SSE 响应体，末尾补上 `[DONE]` 结束标记。
///
/// `[DONE]` 不能省：OpenAI 兼容客户端靠它判断流已正常结束，缺了会让
/// 客户端一直等下去或把流当成异常中断。
fn sse_body(chunks: &[Value]) -> String {
    let mut body = String::new();
    for chunk in chunks {
        body.push_str(&format!("data: {chunk}\n\n"));
    }
    body.push_str("data: [DONE]\n\n");
    body
}

/// `/v1/chat/completions` 的处理逻辑。
///
/// 每次调用按序号从编排脚本里取一个 turn；同时把请求（Bearer 头与请求体）
/// 记录下来，供测试断言「密钥是否被正确传递」。
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
        // 工具调用轮：先发 tool_calls，再发 finish_reason=tool_calls，
        // 客户端据此把工具结果拼进下一轮消息。
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
        Turn::DelayedFinal { text, delay_ms } => {
            tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
            vec![
                json!({"choices":[{"delta":{"content":text}}]}),
                json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
            ]
        }
        Turn::GatedFinal { text, gate } => {
            gate.notified().await;
            vec![
                json!({"choices":[{"delta":{"content":text}}]}),
                json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
            ]
        }
        Turn::ToolThenFinal {
            command,
            text,
            delay_ms,
        } => {
            tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
            let request: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            // 第二轮的特征是消息里已经带了 tool 角色的结果：
            // 有则回最终文本，没有则发工具调用。这让同一个 turn 能服务
            // 任意轮数的会话（并发安全，不依赖全局状态）。
            if request["messages"]
                .as_array()
                .is_some_and(|messages| messages.iter().any(|m| m["role"] == "tool"))
            {
                vec![
                    json!({"choices":[{"delta":{"content":text}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
                ]
            } else {
                let arguments = serde_json::to_string(&json!({"command":command})).unwrap();
                vec![
                    json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"bash","arguments":arguments}}]}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
                ]
            }
        }
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
///
/// 重复最后一项而不是报错，是为了让「只关心最后一轮」的用例只需写一个
/// turn 就能覆盖多轮会话。
pub async fn spawn_mock_openai(turns: Vec<Turn>) -> MockProvider {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    // 绑 0 端口让内核分配，避免与并行用例抢端口。
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
