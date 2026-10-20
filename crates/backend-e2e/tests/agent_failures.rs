//! harness agent 模式的失败形状：runner 失败（exit 1 + error 行）与输出
//! 不可解析（outcome B / agent_error）都要落成**封闭 outcome**——恢复派生
//! 与重启后的重放（run.regress 走同一 outcome 派生）必须得到同一个业务
//! 失败，而不是半形状 journal。
//!
//! 单一假 runner 按 prompt 关键字分流行为（一个脚本一个
//! FLOW_HARNESS_AGENT_BIN 值，避免进程内多测试争抢环境）。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn fake_runner() -> std::path::PathBuf {
    let script = "#!/bin/sh\n\
        input=$(cat)\n\
        case \"$input\" in\n\
          *FAIL*) printf '{\"text\":\"\",\"stop_reason\":\"error\",\"error\":\"boom from runner\"}\\n'; exit 1;;\n\
          *GARBAGE*) printf 'not-json\\n';;\n\
          *) printf '{\"text\":\"ok\",\"stop_reason\":\"end_turn\",\"error\":null}\\n';;\n\
        esac\n";
    let path = std::env::temp_dir().join(format!("fake-runner-fail-{}", std::process::id()));
    std::fs::write(&path, script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

fn agent_config() -> flow_engine::AgentRunConfig {
    flow_engine::AgentRunConfig {
        base_url: "https://api.example.internal/v1".into(),
        model: "contract-model".into(),
        api: "openai".into(),
        proxy: None,
        max_turns: None,
        max_tool_calls_per_turn: None,
        max_tokens: None,
        tool_timeout_secs: None,
    }
}

fn definition(prompt: &str) -> Value {
    json!({
        "semantics": 2,
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "harness", "params": {
                "mode": "agent",
                "prompt": prompt,
                "workdir": "/tmp",
                "api_key": "E2EAGENT",
                "timeout_ms": 20_000,
            }},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "a"},
            {"from": "a", "to": "e"}
        ]
    })
}

async fn until(b: &JournalBackend, id: &str) -> Run {
    tokio::time::timeout(Duration::from_secs(30), async {
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

async fn fresh_backend(dir: &std::path::Path) -> Arc<JournalBackend> {
    let backend: Arc<JournalBackend> = JournalBackend::open(dir, Default::default()).await.unwrap();
    backend.install_harness_agent(Some(agent_config())).unwrap();
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
    backend
}

/// 一个失败形状的完整生命周期：跑失败 → 断言文案 → 关闭重开（不装配置，
/// 回归仍能派生同一失败——封闭 outcome + 冻结配置继承的双重检查）。
async fn assert_failure_shape(dir: &std::path::Path, prompt: &str, fragment: &str) {
    let b = fresh_backend(dir).await;
    let id = b.workflow_create("agent-fail", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.workflow_update(&id, definition(prompt), None)
        .await
        .unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    let run_id = b
        .run_start(&id, None, Value::Null, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let run = until(&b, &run_id).await;
    assert_eq!(
        run.status,
        RunStatus::Failed,
        "prompt {prompt:?} 应业务失败"
    );
    let error = run.error.unwrap_or_default();
    assert!(
        error.contains(fragment),
        "失败文案应包含 {fragment:?}：{error}"
    );
    b.close().await.unwrap();
    drop(b);

    // 重启 + 不安装配置：回归重放走 previous_outcome 派生，必须得到同一失败
    let b2 = fresh_backend(dir).await;
    let candidate = b2
        .run_regression(&run_id, None, None, "replay")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let replay = until(&b2, &candidate).await;
    assert_eq!(
        replay.status,
        RunStatus::Failed,
        "回归重放应得到同一业务失败（prompt {prompt:?}）"
    );
    let replay_error = replay.error.unwrap_or_default();
    assert!(
        replay_error.contains(fragment),
        "重放失败文案应稳定包含 {fragment:?}：{replay_error}"
    );
    b2.close().await.unwrap();
}

#[tokio::test]
async fn agent_failure_shapes_are_closed_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    std::env::set_var("FLOW_HARNESS_AGENT_BIN", fake_runner());
    std::env::set_var("FLOW_SECRET_E2EAGENT", "sk-e2e-secret-value");

    // runner 汇报失败（exit 1 + error 行）→ outcome A，stderr 带文案
    assert_failure_shape(dir.path(), "task FAIL please", "boom from runner").await;
    // 输出不可解析 → outcome B（agent_error），不把半截 stdout 当成功
    assert_failure_shape(
        dir.path(),
        "task GARBAGE please",
        "output is not a JSON line",
    )
    .await;

    std::env::remove_var("FLOW_HARNESS_AGENT_BIN");
    std::env::remove_var("FLOW_SECRET_E2EAGENT");
}
