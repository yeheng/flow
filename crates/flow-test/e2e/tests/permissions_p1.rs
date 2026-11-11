//! 09 权限模型 P1 端到端：harness agent 工具级裁决（G5 反向通道）。
//!
//! 链路：插件进程 `permission/request` → 执行器 ToolPermissionRequest →
//! master 冻结策略裁决 → ToolPermissionDecision → 插件执行/拒绝。
//! 契约：免批工具仍逐次落 Decision（§8.3）；`git push` 弹批、批准后继续；
//! 拒绝后工具失败但回合继续；未知脚本（无 Shell 能力）被拒。
//!
//! mock provider 见 test-support::provider（每轮 Tool::bash + id 唯一）。

use flow_test_support::fixtures::timeline_node;
use flow_test_support::provider::{spawn_mock_openai, Turn};
use flow_test_support::{
    approvals, approve, call_json, deny, publish_legacy_workflow, run_case_with_env, start_run,
    Ctx, TIMEOUT,
};
use serde_json::{json, Value};

/// 在临时目录写 [harness.agent] 配置并返回 FLOW_CONFIG 路径（mock provider
/// 的地址动态，配置必须先行）。
fn harness_config(base_url: &str, model: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flow.config.toml");
    std::fs::write(
        &path,
        format!(
            "[harness.agent]\nbase_url = \"{base_url}\"\nmodel = \"{model}\"\napi = \"openai\"\n"
        ),
    )
    .unwrap();
    let path = path.display().to_string();
    // TempDir 生命周期由调用方持有（drop 即清理）。
    (dir, path)
}

/// harness agent 节点定义 + 显式权限（llm_call/secret 来自 mock provider，
/// shell 逐条按脚本摘要授权）。
fn agent_def(provider_base: &str, model: &str, allow: Vec<Value>) -> Value {
    json!({
        "semantics": 2,
        "permissions": {"version": 1, "mode": "execute", "allow": allow},
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "harness", "params": {
                "mode": "agent", "prompt": "按指示执行命令", "api_key": "P1AGENT",
                "timeout_ms": 60000}},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "a"}, {"from": "a", "to": "e"}
        ]
    })
    // llm_call/secret 由测试补充（base_url 端口动态）。
}

fn llm_caps(provider: &str, model: &str) -> Vec<Value> {
    vec![
        json!({"id": "llm", "action": "llm_call", "providers": [provider],
               "models": [model], "approval": "auto"}),
        json!({"id": "key", "action": "secret", "names": ["P1AGENT"], "approval": "auto"}),
    ]
}

fn shell_cap(id: &str, script: &str, approval: &str) -> Value {
    json!({"id": id, "action": "shell", "script_digest":
           flow_test_support::permissions::sha256_hex(script.as_bytes()),
           "approval": approval})
}

#[tokio::test]
async fn tool_allowed_by_capability_still_records_a_decision() {
    let provider = spawn_mock_openai(vec![
        Turn::Tool { command: "git status".into() },
        Turn::Final { text: "done".into() },
    ])
    .await;
    let (_dir, config) = harness_config(&provider.base_url(), "mock-model");
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_CONFIG", &config), ("FLOW_TEST_AUTO_GOVERN", "0")],
        |ctx| {
            Box::pin(async move {
        let client = ctx.client().await;
        call_json(&client, "secrets.set", json!({"name": "P1AGENT", "value": "sk-p1"})).await;
        let provider_id = flow_test_support::permissions::normalize_provider(&provider.base_url());
        let mut definition = agent_def(&provider.base_url(), "mock-model", vec![]);
        let mut allow = llm_caps(&provider_id, "mock-model");
        allow.push(shell_cap("status", "git status", "auto"));
        definition["permissions"]["allow"] = json!(allow);
        let (workflow_id, _) = publish_legacy_workflow(&client, "P1 工具免批", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = flow_test_support::wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        // 免批 ≠ 免记录：逐次落 Decision（by=System，matched=shell 能力）。
        let events: Value =
            call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let tool_decisions: Vec<&Value> = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["event"]["kind"] == json!("permission_decided")
                && e["event"]["payload"]["gate"] == json!("tool"))
            .collect();
        assert_eq!(tool_decisions.len(), 1, "{}", serde_json::to_string(&events).unwrap());
        assert_eq!(
            tool_decisions[0]["event"]["payload"]["record"]["by"],
            json!({"kind": "system"})
        );
        assert!(serde_json::to_string(&tool_decisions[0]["event"]["payload"]["record"]["matched_capabilities"])
            .unwrap()
            .contains("shell"));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn tool_ask_suspends_turn_until_approved_then_runs() {
    let script = "git push origin main";
    let provider = spawn_mock_openai(vec![
        Turn::Tool { command: script.into() },
        Turn::Final { text: "pushed".into() },
    ])
    .await;
    let (_dir, config) = harness_config(&provider.base_url(), "mock-model");
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_CONFIG", &config), ("FLOW_TEST_AUTO_GOVERN", "0")],
        |ctx| {
            Box::pin(async move {
        let client = ctx.client().await;
        call_json(&client, "secrets.set", json!({"name": "P1AGENT", "value": "sk-p1"})).await;
        let provider_id = flow_test_support::permissions::normalize_provider(&provider.base_url());
        let mut definition = agent_def(&provider.base_url(), "mock-model", vec![]);
        let mut allow = llm_caps(&provider_id, "mock-model");
        allow.push(shell_cap("push", script, "ask"));
        definition["permissions"]["allow"] = json!(allow);
        let (workflow_id, _) = publish_legacy_workflow(&client, "P1 工具弹批", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        // 回合挂起在工具前：收件箱出现 gate=tool 的询问。
        let pending = flow_test_support::wait_pending_approval(&client, &run_id, "a", TIMEOUT).await;
        assert_eq!(pending["gate"], json!("tool"));
        assert_eq!(pending["specs"][0]["action"], json!("shell"));
        let run = call_json(&client, "run.get.view", json!({"run_id": run_id})).await;
        assert_eq!(run["run"]["status"], json!("running"), "{run}");
        // provider 只见过第一轮（工具结果未回，回合停在裁决上）。
        assert_eq!(provider.hits(), 1);
        approve(
            &client, &run_id, "a",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(), "once",
        )
        .await;
        let run = flow_test_support::wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        // 批准后工具执行、回合完成：provider 至少见过第二轮。
        assert!(provider.hits() >= 2, "provider hits = {}", provider.hits());
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "a")["approval"]["status"], json!("allowed"));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn tool_denied_fails_the_tool_but_not_the_turn() {
    let script = "git push origin main";
    let provider = spawn_mock_openai(vec![
        Turn::Tool { command: script.into() },
        Turn::Final { text: "handled denial".into() },
    ])
    .await;
    let (_dir, config) = harness_config(&provider.base_url(), "mock-model");
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_CONFIG", &config), ("FLOW_TEST_AUTO_GOVERN", "0")],
        |ctx| {
            Box::pin(async move {
        let client = ctx.client().await;
        call_json(&client, "secrets.set", json!({"name": "P1AGENT", "value": "sk-p1"})).await;
        let provider_id = flow_test_support::permissions::normalize_provider(&provider.base_url());
        let mut definition = agent_def(&provider.base_url(), "mock-model", vec![]);
        let mut allow = llm_caps(&provider_id, "mock-model");
        allow.push(shell_cap("push", script, "ask"));
        definition["permissions"]["allow"] = json!(allow);
        let (workflow_id, _) = publish_legacy_workflow(&client, "P1 工具拒绝", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let pending = flow_test_support::wait_pending_approval(&client, &run_id, "a", TIMEOUT).await;
        deny(
            &client, &run_id, "a",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(),
            "不允许推送",
        )
        .await;
        // 工具被拒是工具级失败：回合拿到失败结果继续，run 仍可成功收口。
        let run = flow_test_support::wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        assert!(provider.hits() >= 2, "provider hits = {}", provider.hits());
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "a")["approval"]["status"], json!("denied"));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn ungranted_script_is_denied_without_asking() {
    let provider = spawn_mock_openai(vec![
        Turn::Tool { command: "rm -rf /".into() },
        Turn::Final { text: "blocked".into() },
    ])
    .await;
    let (_dir, config) = harness_config(&provider.base_url(), "mock-model");
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_CONFIG", &config), ("FLOW_TEST_AUTO_GOVERN", "0")],
        |ctx| {
            Box::pin(async move {
        let client = ctx.client().await;
        call_json(&client, "secrets.set", json!({"name": "P1AGENT", "value": "sk-p1"})).await;
        let provider_id = flow_test_support::permissions::normalize_provider(&provider.base_url());
        let mut definition = agent_def(&provider.base_url(), "mock-model", vec![]);
        // 只授予另一个脚本的 Shell 能力。
        let mut allow = llm_caps(&provider_id, "mock-model");
        allow.push(shell_cap("other", "git status", "auto"));
        definition["permissions"]["allow"] = json!(allow);
        let (workflow_id, _) = publish_legacy_workflow(&client, "P1 未授予脚本", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = flow_test_support::wait_run_terminal(&client, &run_id, TIMEOUT).await;
        // 工具拒绝不判死节点：回合继续到 Final，run 成功。
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        let events: Value =
            call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let denied = events["events"].as_array().unwrap().iter().any(|e| {
            e["event"]["kind"] == json!("permission_decided")
                && e["event"]["payload"]["gate"] == json!("tool")
                && e["event"]["payload"]["record"]["decision"] == json!("deny")
        });
        assert!(denied, "{}", serde_json::to_string(&events).unwrap());
        // 未授予的动作直接 Deny（不进审批收件箱）。
        let inbox = approvals(&client, Some(&run_id)).await;
        assert_eq!(inbox["items"].as_array().map(Vec::len), Some(0), "{inbox}");
            })
        },
    )
    .await;
}
