//! harness agent 模式的生命周期契约（executor 内联执行）：
//!
//! 1. 未配置 `[harness.agent]` 时，agent 节点的失败发生在 **run 提交前**
//!    （错误直达设置页指引，而不是派发到 executor 深处才报）；
//! 2. provider 失败（HTTP 500）落**封闭 outcome**——重启后的回归重放
//!    （走 previous_outcome 派生）得到同一个业务失败；
//! 3. 回归候选继承 baseline 冻结配置：重开 journal 且**不安装**配置，
//!    回归仍匹配（0 provider 调用）。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus};
use flow_test_support::provider::{spawn_mock_openai, MockProvider, Turn};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn agent_config(base_url: &str) -> flow_engine::AgentRunConfig {
    flow_engine::AgentRunConfig {
        base_url: base_url.into(),
        model: "lifecycle-model".into(),
        api: "openai".into(),
        proxy: None,
        max_turns: Some(5),
        max_tool_calls_per_turn: None,
        max_tokens: None,
        tool_timeout_secs: None,
    }
}

fn definition(prompt: &str, with_workdir: bool) -> Value {
    let mut params = json!({
        "mode": "agent",
        "prompt": prompt,
        "api_key": "E2ELIFE",
        "timeout_ms": 30_000,
    });
    if with_workdir {
        params["workdir"] = json!("/tmp");
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

async fn fresh_backend(
    dir: &std::path::Path,
    config: Option<flow_engine::AgentRunConfig>,
) -> Arc<JournalBackend> {
    let backend: Arc<JournalBackend> = JournalBackend::open(dir, Default::default()).await.unwrap();
    backend.install_harness_agent(config).unwrap();
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

async fn start_run(b: &Arc<JournalBackend>, definition: Value) -> String {
    let wf = b.workflow_create("agent-life", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.workflow_update(&wf, definition, None).await.unwrap();
    b.workflow_publish(&wf, 1, None).await.unwrap();
    b.run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn agent_lifecycle_guards_closed_outcomes_and_regression_inheritance() {
    std::env::set_var("FLOW_SECRET_E2ELIFE", "sk-life-secret");
    let dir = tempfile::tempdir().unwrap();

    // ---- 1. 未配置：run 提交前拒绝，文案直达修法 ----
    {
        let b = fresh_backend(dir.path(), None).await;
        let wf = b
            .workflow_create("missing-config", None)
            .await
            .unwrap()
            .result["workflow_id"]
            .as_str()
            .unwrap()
            .to_owned();
        // api_key 用独占名（本测试独进程，无需防并行）
        let mut def = definition("any", true);
        def["nodes"][1]["params"]["api_key"] = json!("E2ELIFE");
        b.workflow_update(&wf, def, None).await.unwrap();
        b.workflow_publish(&wf, 1, None).await.unwrap();
        let error = b
            .run_start(&wf, None, Value::Null, "manual", None, None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("[harness.agent] 未启用"), "{error}");
        assert!(error.contains("设置页"), "{error}");
        b.close().await.unwrap();
    }

    // ---- 2. provider 失败 → 封闭 outcome；重启后回归重放同一失败 ----
    let fail_provider = spawn_mock_openai(vec![Turn::InternalError]).await;
    {
        let b = fresh_backend(dir.path(), Some(agent_config(&fail_provider.base_url()))).await;
        let run_id = start_run(&b, definition("make it fail", true)).await;
        let run = until(&b, &run_id).await;
        assert_eq!(run.status, RunStatus::Failed, "provider 500 应业务失败");
        let error = run.error.clone().unwrap_or_default();
        assert!(
            error.contains("harness agent")
                && (error.contains("退出码 1") || error.contains("turn failed")),
            "失败文案应指向 agent 回合：{error}"
        );
        b.close().await.unwrap();
        drop(b);
        // 重开（仍安装失败配置）：回归走 outcome 派生，0 provider 调用
        let b2 = fresh_backend(dir.path(), Some(agent_config(&fail_provider.base_url()))).await;
        let hits_before = fail_provider.hits();
        let candidate = b2
            .run_regression(&run_id, None, None, "replay")
            .await
            .unwrap()
            .result["run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let replay = until(&b2, &candidate).await;
        assert_eq!(replay.status, RunStatus::Failed, "回归重放应得到同一失败");
        assert!(
            replay.error.unwrap_or_default().contains("harness agent"),
            "重放失败文案应稳定"
        );
        assert_eq!(
            fail_provider.hits(),
            hits_before,
            "回归重放 0 provider 调用"
        );
        b2.close().await.unwrap();
    }
    fail_provider.stop().await;

    // ---- 3. 成功 baseline → 重开且不安装配置：回归仍匹配（继承冻结配置） ----
    let ok_provider: MockProvider = spawn_mock_openai(vec![Turn::Final {
        text: "baseline done".into(),
    }])
    .await;
    {
        let b = fresh_backend(dir.path(), Some(agent_config(&ok_provider.base_url()))).await;
        let run_id = start_run(&b, definition("baseline run", true)).await;
        let run = until(&b, &run_id).await;
        assert_eq!(
            run.status,
            RunStatus::Succeeded,
            "baseline 应成功（error: {:?}）",
            run.error
        );
        b.close().await.unwrap();
        drop(b);
        // 判别测试：不安装配置重开。若候选按当前安装（None）构造快照，
        // run_start 的前置校验会直接拒绝——能跑通且匹配即证明继承生效。
        let b2 = fresh_backend(dir.path(), None).await;
        let hits_before = ok_provider.hits();
        let candidate = b2
            .run_regression(&run_id, None, None, "restart-inherit")
            .await
            .unwrap()
            .result["run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let replay = until(&b2, &candidate).await;
        assert_eq!(
            replay.status,
            RunStatus::Succeeded,
            "重开且未安装配置的回归应成功（error: {:?}）",
            replay.error
        );
        assert_eq!(
            b2.compare_runs(&run_id, &candidate).await.unwrap()["passed"],
            true
        );
        assert_eq!(ok_provider.hits(), hits_before, "回归 0 provider 调用");
        b2.close().await.unwrap();
    }
    ok_provider.stop().await;
    std::env::remove_var("FLOW_SECRET_E2ELIFE");
}
