//! harness agent 模式的 workdir 三分支（executor 内联执行）：缺省 = 每次
//! run 的随机临时沙箱；显式配置不存在 → 授权前明确失败；显式配置存在 →
//! 按配置执行。沙箱路径不进请求/指纹（回归在缺省模式下仍可匹配）。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus};
use flow_test_support::provider::{spawn_mock_openai, Turn};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn definition(with_workdir: Option<&str>) -> Value {
    let mut params = json!({
        "mode": "agent",
        "prompt": "scratch run",
        "api_key": "E2EWD",
        "timeout_ms": 30_000,
    });
    if let Some(workdir) = with_workdir {
        params["workdir"] = json!(workdir);
    }
    json!({
        "semantics": 2,
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "harness", "params": params},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "a"}, {"from": "a", "to": "e"}]
    })
}

async fn until(b: &JournalBackend, id: &str) -> Run {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let run = b.run_snapshot(id).await.unwrap().unwrap();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

async fn run_agent(b: &Arc<JournalBackend>, definition: Value) -> Run {
    let wf = b.workflow_create("agent-wd", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.workflow_update(&wf, definition, None).await.unwrap();
    b.workflow_publish(&wf, 1, None).await.unwrap();
    let run_id = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(b, &run_id).await
}

#[tokio::test]
async fn agent_workdir_default_scratch_configured_checked_and_regression_stable() {
    let provider = spawn_mock_openai(vec![Turn::Final {
        text: "scratch ok".into(),
    }])
    .await;
    std::env::set_var("FLOW_SECRET_E2EWD", "sk-wd-secret");
    let dir = tempfile::tempdir().unwrap();

    let b: Arc<JournalBackend> = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    b.install_harness_agent(Some(flow_engine::AgentRunConfig {
        base_url: provider.base_url(),
        model: "wd-model".into(),
        api: "openai".into(),
        proxy: None,
        max_turns: Some(5),
        max_tool_calls_per_turn: None,
        max_tokens: None,
        tool_timeout_secs: None,
    }))
    .unwrap();
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
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

    // ---- 缺省：不配 workdir，run 成功（临时沙箱） ----
    let run = run_agent(&b, definition(None)).await;
    assert_eq!(
        run.status,
        RunStatus::Succeeded,
        "缺省 workdir 应使用临时沙箱（error: {:?}）",
        run.error
    );

    // ---- 配置了才检查：不存在的路径明确失败且不再调用 provider ----
    let hits_before = provider.hits();
    let run = run_agent(&b, definition(Some("/absolute/path/that/does/not/exist"))).await;
    assert_eq!(run.status, RunStatus::Failed);
    assert!(
        run.error.unwrap_or_default().contains("workdir 不可用"),
        "应明确指出 workdir 不可用"
    );
    assert_eq!(provider.hits(), hits_before, "失败路径不得调用 provider");

    // ---- 配置了且存在：按配置走 ----
    let configured = dir.path().join("configured-workdir");
    std::fs::create_dir_all(&configured).unwrap();
    let run = run_agent(&b, definition(Some(&configured.to_string_lossy()))).await;
    assert_eq!(
        run.status,
        RunStatus::Succeeded,
        "存在的 workdir 应直接使用（error: {:?}）",
        run.error
    );

    // ---- 缺省模式下回归可匹配（沙箱路径不进指纹的判别）----
    let baseline = run_agent(&b, definition(None)).await;
    assert_eq!(baseline.status, RunStatus::Succeeded);
    let candidate = b
        .run_regression(&baseline.run_id, None, None, "wd-regress")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let replay = until(&b, &candidate).await;
    assert_eq!(
        replay.status,
        RunStatus::Succeeded,
        "缺省 workdir 的回归应匹配（error: {:?}）",
        replay.error
    );

    b.close().await.unwrap();
    provider.stop().await;
    std::env::remove_var("FLOW_SECRET_E2EWD");
}
