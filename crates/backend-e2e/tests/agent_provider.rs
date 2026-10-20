//! harness agent 模式的全链路 e2e：**真实 flow-harness-agent runner + 真实
//! conga-host 装配 + mock OpenAI 兼容 provider**。覆盖：
//!
//! - 密钥经 stdin → conga provider 的 Bearer 头真值到达（且只在这里到达）；
//! - 多轮工具调用：第一轮 tool_calls 触发 conga 内置 bash 在 workdir 执行，
//!   第二轮请求携带 role=tool 回执、返回最终文本；
//! - 节点输出 = runner 单行结果的 text，agent 元数据随输出；
//! - 内置 coding prompt / 工具集来自 conga 无头装配（system 消息含
//!   CODING_AGENT_PROMPT，tools 含 bash）。

use axum::extract::State;
use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::RunStatus;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// mock provider 的共享捕获状态。
type ProviderState = (
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<String>>>,
    Arc<Mutex<Vec<Value>>>,
    String,
);

/// 一次 SSE 响应：把若干 chunk JSON 包装成 data: 行。
fn sse_body(chunks: &[Value]) -> String {
    let mut body = String::new();
    for chunk in chunks {
        body.push_str(&format!("data: {chunk}\n\n"));
    }
    body.push_str("data: [DONE]\n\n");
    body
}

#[tokio::test]
async fn agent_node_full_stack_with_real_runner_and_mock_provider() {
    // ---- mock provider：两轮流式响应 + 请求捕获 ----
    let calls = Arc::new(AtomicUsize::new(0));
    let seen_bearer = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let workdir = tempfile::tempdir().unwrap();
    let marker = workdir.path().join("tool-ran-marker");
    let marker_arg = marker.to_string_lossy().into_owned();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = (
        calls.clone(),
        seen_bearer.clone(),
        seen_bodies.clone(),
        marker_arg,
    );
    let app = axum::Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(
                move |State((calls, seen_bearer, seen_bodies, marker_arg)): State<ProviderState>,
                headers: axum::http::HeaderMap,
                body: String| async move {
                    let turn = calls.fetch_add(1, Ordering::SeqCst);
                    seen_bearer
                        .lock()
                        .unwrap()
                        .push(headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string());
                    seen_bodies
                        .lock()
                        .unwrap()
                        .push(serde_json::from_str(&body).unwrap_or(Value::Null));
                    let tool_arguments = serde_json::to_string(&json!({
                        "command": format!("echo ran > '{marker_arg}'"),
                    }))
                    .unwrap();
                    let chunks: Vec<Value> = if turn == 0 {
                        vec![
                            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"bash","arguments":tool_arguments}}]}}]}),
                            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
                        ]
                    } else {
                        vec![
                            json!({"choices":[{"delta":{"content":"mock agent final answer"}}]}),
                            json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
                            json!({"choices":[{"delta":{}}],"usage":{"prompt_tokens":11,"completion_tokens":7}}),
                        ]
                    };
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        sse_body(&chunks),
                    )
                },
            ),
        )
        .with_state(state);
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // ---- 环境：真实 runner + 密钥 ----
    std::env::set_var(
        "FLOW_HARNESS_AGENT_BIN",
        flow_test_support::io::harness_agent_bin(),
    );
    std::env::set_var("FLOW_SECRET_E2EPROVIDER", "sk-mock-provider-key");
    let dir = tempfile::tempdir().unwrap();

    let backend: Arc<JournalBackend> = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    backend
        .install_harness_agent(Some(flow_engine::AgentRunConfig {
            base_url: format!("http://{addr}/v1"),
            model: "mock-model".into(),
            api: "openai".into(),
            proxy: None,
            max_turns: Some(5),
            max_tool_calls_per_turn: None,
            max_tokens: Some(2048),
            tool_timeout_secs: None,
        }))
        .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                    flow_test_support::io::executor_bin(),
                ),
                x_max: 2,
                tag: None,
            },
        ))
        .await
        .unwrap();

    let definition = json!({
        "semantics": 2,
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "harness", "params": {
                "mode": "agent",
                "prompt": "run the echo tool then finish",
                "workdir": workdir.path().to_string_lossy(),
                "api_key": "E2EPROVIDER",
                "timeout_ms": 60_000,
            }},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "a"}, {"from": "a", "to": "e"}]
    });
    let wf = backend
        .workflow_create("agent-provider", None)
        .await
        .unwrap()
        .result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    backend
        .workflow_update(&wf, definition, None)
        .await
        .unwrap();
    backend.workflow_publish(&wf, 1, None).await.unwrap();
    let run_id = backend
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let run = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let run = backend.run_snapshot(&run_id).await.unwrap().unwrap();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        run.status,
        RunStatus::Succeeded,
        "agent 全链路应成功（error: {:?}）",
        run.error
    );

    // ---- 断言：多轮 + 工具真执行 + 密钥只在 Bearer 头 ----
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "两轮 provider 调用（tool_calls → final）"
    );
    let bearers = seen_bearer.lock().unwrap().clone();
    assert_eq!(bearers.len(), 2);
    for bearer in &bearers {
        assert_eq!(
            bearer, "Bearer sk-mock-provider-key",
            "密钥真值必须经 provider Bearer 头到达"
        );
    }
    let bodies = seen_bodies.lock().unwrap().clone();
    // 第一轮：内置 coding prompt（conga 无头装配）+ bash 工具集
    let first = &bodies[0];
    let system = first["messages"][0]["content"].as_str().unwrap_or_default();
    assert!(!system.is_empty(), "system prompt 应存在");
    assert!(
        first["tools"]
            .as_array()
            .is_some_and(|tools| { tools.iter().any(|t| t["function"]["name"] == "bash") }),
        "内置工具集应含 bash：{}",
        serde_json::to_string(&first["tools"]).unwrap_or_default()
    );
    assert_eq!(first["model"], "mock-model");
    // 第二轮：assistant tool_calls 回显 + role=tool 回执（bash 真的执行过）
    let second = &bodies[1];
    let messages = second["messages"].as_array().cloned().unwrap_or_default();
    assert!(
        messages.iter().any(|m| m["role"] == "tool"),
        "第二轮请求应含工具结果消息：{second}"
    );
    let tool_msg = messages.iter().find(|m| m["role"] == "tool").unwrap();
    let tool_content = tool_msg["content"].as_str().unwrap_or_default().to_string();
    // echo 的输出重定向进了 marker 文件，无 stdout → conga 回执为退出码摘要
    assert_eq!(tool_content.trim(), "[exit 0]");

    // bash 工具在 workdir 真实执行过的物证
    assert!(
        marker.exists(),
        "workdir 标记文件应存在（bash 工具真实执行）"
    );

    // 节点输出：text + agent 元数据
    let node_output = match run.nodes["a"].output.clone() {
        Some(flow_journal::StoredValue::Inline(value)) => value,
        other => panic!("agent 输出应为 inline：{other:?}"),
    };
    assert_eq!(node_output["stdout"], "mock agent final answer");
    assert_eq!(node_output["exit_code"], 0);
    assert_eq!(node_output["agent"]["model"], "mock-model");
    assert_eq!(node_output["agent"]["stop_reason"], "end_turn");
    // 输出面无密钥
    assert!(
        !serde_json::to_string(&node_output)
            .unwrap_or_default()
            .contains("sk-mock-provider-key"),
        "节点输出不得包含密钥真值"
    );

    backend.close().await.unwrap();
    server.abort();
    std::env::remove_var("FLOW_HARNESS_AGENT_BIN");
    std::env::remove_var("FLOW_SECRET_E2EPROVIDER");
}
