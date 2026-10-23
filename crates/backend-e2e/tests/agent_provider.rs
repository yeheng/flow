//! harness agent 模式的全链路 e2e：executor **内联**调用 conga-host 无头
//! 装配 + mock OpenAI 兼容 provider。覆盖：
//!
//! - 密钥经参数 → conga provider 的 Bearer 头真值到达（且只在这里到达）；
//! - 多轮工具调用：第一轮 tool_calls 触发 conga 内置 bash 在 workdir 执行，
//!   第二轮请求携带 role=tool 回执、返回最终文本；
//! - 节点输出 = 最终文本 + agent 元数据（model/stop_reason）；
//! - 内置 coding prompt / 工具集来自无头装配（system 消息非空、tools 含 bash）。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::RunStatus;
use flow_test_support::provider::{spawn_mock_openai, Turn};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn agent_node_full_stack_with_inline_conga_and_mock_provider() {
    let workdir = tempfile::tempdir().unwrap();
    let marker = workdir.path().join("tool-ran-marker");
    let final_gate = Arc::new(tokio::sync::Notify::new());
    let provider = spawn_mock_openai(vec![
        Turn::Tool {
            command: format!("echo ran > '{}'", marker.display()),
        },
        Turn::GatedFinal {
            text: "mock agent final answer".into(),
            gate: final_gate.clone(),
        },
    ])
    .await;
    std::env::set_var("FLOW_SECRET_E2EPROVIDER", "sk-mock-provider-key");
    let dir = tempfile::tempdir().unwrap();

    let backend: Arc<JournalBackend> = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    backend
        .install_harness_agent(Some(flow_engine::AgentRunConfig {
            base_url: provider.base_url(),
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

    // 第二轮 provider 尚未返回时，第一轮与工具进度就必须可读。
    let observations = backend.observations.as_ref().unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            observations.flush();
            let logs = observations.page(&run_id, None, 0, 256).unwrap();
            if logs
                .iter()
                .any(|record| record.line.message.contains("第 2 轮 → 模型请求"))
            {
                assert!(logs.iter().all(|record| record.line.node_id == "a"));
                assert!(logs
                    .iter()
                    .any(|record| record.line.message.contains("第 1 轮开始")));
                assert!(logs
                    .iter()
                    .any(|record| record.line.message.contains("工具 bash 开始")));
                assert!(logs
                    .iter()
                    .any(|record| record.line.message.contains("工具 bash 完成")));
                assert!(logs
                    .iter()
                    .any(|record| record.line.message.contains("第 1 轮结束")));
                assert_eq!(
                    backend.run_snapshot(&run_id).await.unwrap().unwrap().status,
                    RunStatus::Running
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("agent 运行中应实时显示多轮进度");
    final_gate.notify_one();

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
        provider.hits(),
        2,
        "两轮 provider 调用（tool_calls → final）"
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(
            request.bearer, "Bearer sk-mock-provider-key",
            "密钥真值必须经 provider Bearer 头到达"
        );
    }
    // 第一轮：内置 coding prompt（无头装配）+ bash 工具集
    let first = &requests[0];
    let system = first.body["messages"][0]["content"]
        .as_str()
        .unwrap_or_default();
    assert!(!system.is_empty(), "system prompt 应存在");
    assert!(
        first.body["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().any(|t| t["function"]["name"] == "bash")),
        "内置工具集应含 bash"
    );
    assert_eq!(first.body["model"], "mock-model");
    // 第二轮：assistant tool_calls 回显 + role=tool 回执（bash 真的执行过）
    let second = &requests[1];
    let messages = second.body["messages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let tool_msg = messages
        .iter()
        .find(|m| m["role"] == "tool")
        .expect("第二轮请求应含工具结果消息");
    // echo 的输出重定向进了 marker 文件，无 stdout → conga 回执为退出码摘要
    assert_eq!(
        tool_msg["content"].as_str().unwrap_or_default().trim(),
        "[exit 0]"
    );

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
    assert!(
        !serde_json::to_string(&node_output)
            .unwrap_or_default()
            .contains("sk-mock-provider-key"),
        "节点输出不得包含密钥真值"
    );

    observations.flush();
    let logs = observations.page(&run_id, None, 0, 256).unwrap();
    assert!(logs
        .iter()
        .any(|record| record.line.message.contains("多轮循环结束（共 2 轮）")));
    let display = serde_json::to_string(&logs).unwrap();
    for private in [
        "sk-mock-provider-key",
        "run the echo tool then finish",
        "echo ran",
        "mock agent final answer",
    ] {
        assert!(
            !display.contains(private),
            "进度日志不应记录密钥、prompt 或工具/模型正文：{private}"
        );
    }

    backend.close().await.unwrap();
    provider.stop().await;
    std::env::remove_var("FLOW_SECRET_E2EPROVIDER");
}
