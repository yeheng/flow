//! 09 权限模型 G4 端到端：动态插件节点接入授权链路（manifest 声明 ⊆
//! 授予 ∩ workflow ∩ node）、legacy 静态门兑现已确认决策 2（legacy 定义含
//! 动态插件节点拒绝启动）、legacy-unrestricted 插件逐次人工确认。
//!
//! 契约来源：docs/design/09-permission-model.md §7（插件授予）与
//! docs/plans/2026-11-09-permission-model-impl.md「已定决策 2」。

use flow_test_support::{
    approve, call_err, call_json, publish_legacy_workflow, run_case_with_env, start_run,
    wait_pending_approval, wait_run_terminal, Ctx, TIMEOUT,
};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;

/// 部署测试插件（python echo 参考实现）到独立 plugins 目录。
/// `declared` = manifest 是否带 [permissions] 声明（secret demo-key required）。
fn deploy_echo_plugin(declared: bool) -> (tempfile::TempDir, String) {
    let home = tempfile::tempdir().unwrap();
    let plugin_root = home.path().join("plugins").join("echo-plugin");
    let bin = plugin_root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let python = std::process::Command::new("python3")
        .arg("-c")
        .arg("import sys; print(sys.executable)")
        .output()
        .expect("probe python3");
    let python = String::from_utf8_lossy(&python.stdout).trim().to_string();
    let script = flow_test_support::io::repo_root().join("examples/plugin-py/node_echo_plugin.py");
    assert!(
        script.is_file(),
        "echo plugin missing: {}",
        script.display()
    );
    let entry = bin.join("echo-node");
    std::fs::write(
        &entry,
        format!("#!/bin/sh\nexec '{python}' '{}'\n", script.display()),
    )
    .unwrap();
    std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755)).unwrap();

    let permissions_section = if declared {
        r#"
[permissions]
schema = 1
required_features = ["master-permit-v1"]

[[permissions.capabilities]]
id = "echo-secret"
action = "secret"
names = ["demo-key"]
required = true
"#
    } else {
        ""
    };
    let manifest = format!(
        r#"
[plugin]
id = "echo-plugin"
name = "Echo"
version = "0.1.0"
entry = "{entry}"
protocol = 1
node_types = ["echo"]

[methods]
invoke = "node/invoke"
{permissions_section}
"#,
        entry = entry.display(),
    );
    std::fs::write(plugin_root.join("plugin.toml"), manifest).unwrap();
    let plugins_dir = home.path().join("plugins").display().to_string();
    (home, plugins_dir)
}

/// start → echo（动态插件节点）→ end。
fn echo_definition(workflow_permissions: Value) -> Value {
    json!({
        "semantics": 2,
        "permissions": workflow_permissions,
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "n", "type": "echo", "params": {"text": "hi", "times": 1}},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
    })
}

fn workflow_permissions(approval: &str) -> Value {
    json!({"version": 1, "mode": "execute", "allow": [
        {"id": "w-secret", "action": "secret", "names": ["demo-key"], "approval": approval}
    ]})
}

/// 发布受管 echo 工作流并预先授予插件（Declared manifest 的全部声明能力）。
async fn publish_governed_echo(
    client: &flow_test_support::Conn,
    name: &str,
    permissions: Value,
) -> String {
    let grant: Value = call_json(
        client,
        "permissions.grant",
        json!({"plugin_id": "echo-plugin", "request_id": format!("grant-{name}")}),
    )
    .await;
    assert_eq!(
        grant["plugin_id"],
        json!("echo-plugin"),
        "授予必须落盘：{grant}"
    );
    assert!(
        grant["content_hash"]
            .as_str()
            .is_some_and(|h| h.len() == 64),
        "回执携带内容哈希：{grant}"
    );
    let (workflow_id, _) =
        publish_legacy_workflow(client, name, echo_definition(permissions)).await;
    workflow_id
}

/// 决策 2：legacy 定义（无 permissions 块）含动态插件节点 → run.start 拒绝
/// （-32020），与内置副作用节点同等待遇。
#[tokio::test]
async fn legacy_definition_with_dynamic_plugin_node_cannot_start() {
    let (_keep, plugins_dir) = deploy_echo_plugin(true);
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[
            ("FLOW_PLUGINS_DIR", plugins_dir.as_str()),
            ("FLOW_TEST_AUTO_GOVERN", "0"),
        ],
        |ctx: &mut Ctx| {
            Box::pin(async move {
                let client = ctx.client().await;
                let mut definition = echo_definition(json!(null));
                definition["permissions"].take();
                let (workflow_id, _) =
                    publish_legacy_workflow(&client, "legacy 动态插件", definition).await;
                let error = call_err(
                    &client,
                    "run.start",
                    json!({"workflow_id": workflow_id, "input": {}}),
                )
                .await;
                assert_eq!(
                    error.code(),
                    -32020,
                    "决策 2：legacy 含动态插件节点必须拒绝：{error}"
                );
                assert!(
                    error.message().contains("legacy"),
                    "拒绝原因提示迁移：{error}"
                );
            })
        },
    )
    .await;
}

/// 未授予的 Declared 插件：受管定义 run.start 即拒（fail-fast，不进派发）。
#[tokio::test]
async fn ungranted_declared_plugin_rejects_at_start() {
    let (_keep, plugins_dir) = deploy_echo_plugin(true);
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[
            ("FLOW_PLUGINS_DIR", plugins_dir.as_str()),
            ("FLOW_TEST_AUTO_GOVERN", "0"),
        ],
        |ctx: &mut Ctx| {
            Box::pin(async move {
                let client = ctx.client().await;
                let (workflow_id, _) = publish_legacy_workflow(
                    &client,
                    "未授予插件",
                    echo_definition(workflow_permissions("auto")),
                )
                .await;
                let error = call_err(
                    &client,
                    "run.start",
                    json!({"workflow_id": workflow_id, "input": {}}),
                )
                .await;
                assert_eq!(error.code(), -32020, "未授予必须 fail-fast：{error}");
                assert!(
                    error.message().contains("未授予"),
                    "拒绝原因点名授予：{error}"
                );
            })
        },
    )
    .await;
}

/// 已授予 + workflow 层覆盖 manifest 需求：System Allow + 执行完成 +
/// PermissionDecided 落盘（auto 不是审计豁免）。
#[tokio::test]
async fn granted_plugin_need_covered_allows_and_records_decision() {
    let (_keep, plugins_dir) = deploy_echo_plugin(true);
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[
            ("FLOW_PLUGINS_DIR", plugins_dir.as_str()),
            ("FLOW_TEST_AUTO_GOVERN", "0"),
        ],
        |ctx: &mut Ctx| {
            Box::pin(async move {
                let client = ctx.client().await;
                let workflow_id =
                    publish_governed_echo(&client, "covered", workflow_permissions("auto")).await;
                let run_id = start_run(&client, &workflow_id, json!({})).await;
                let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
                assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
                let events: Value =
                    call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
                let decided: Vec<&Value> = events["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|event| event["event"]["kind"] == json!("permission_decided"))
                    .collect();
                assert_eq!(decided.len(), 1, "插件节点必须恰好一条裁决：{events:?}");
                assert_eq!(
                    decided[0]["event"]["payload"]["record"]["decision"],
                    json!("allow")
                );
                assert_eq!(
                    decided[0]["event"]["payload"]["record"]["by"],
                    json!({"kind": "system"})
                );
            })
        },
    )
    .await;
}

/// workflow 层要求 Ask：挂起 → 收件箱出现插件需求（needs 字段）→ 批准 →
/// 执行完成（run.approve 的重评估走 manifest 需求包含检查）。
#[tokio::test]
async fn plugin_need_ask_suspends_until_approved_then_executes() {
    let (_keep, plugins_dir) = deploy_echo_plugin(true);
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[
            ("FLOW_PLUGINS_DIR", plugins_dir.as_str()),
            ("FLOW_TEST_AUTO_GOVERN", "0"),
        ],
        |ctx: &mut Ctx| {
            Box::pin(async move {
                let client = ctx.client().await;
                let workflow_id =
                    publish_governed_echo(&client, "ask", workflow_permissions("ask")).await;
                let run_id = start_run(&client, &workflow_id, json!({})).await;
                let pending = wait_pending_approval(&client, &run_id, "n", TIMEOUT).await;
                assert_eq!(pending["gate"], json!("node"));
                let needs = pending["needs"].as_array().expect("needs 字段");
                assert!(
                    needs.iter().any(|need| need["action"] == json!("secret")),
                    "needs 展示 manifest 声明：{pending}"
                );
                approve(
                    &client,
                    &run_id,
                    "n",
                    pending["operation_id"].as_str().unwrap(),
                    pending["request_hash"].as_str().unwrap(),
                    "once",
                )
                .await;
                let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
                assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
            })
        },
    )
    .await;
}

/// legacy-unrestricted 插件（无 [permissions]）：管理员 legacy_confirm 授予后
/// 逐次人工确认（Ask → 批准 → 执行）。
#[tokio::test]
async fn unrestricted_plugin_asks_after_admin_confirm() {
    let (_keep, plugins_dir) = deploy_echo_plugin(false);
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[
            ("FLOW_PLUGINS_DIR", plugins_dir.as_str()),
            ("FLOW_TEST_AUTO_GOVERN", "0"),
        ],
        |ctx: &mut Ctx| {
            Box::pin(async move {
                let client = ctx.client().await;
                let grant: Value = call_json(
                    &client,
                    "permissions.grant",
                    json!({"plugin_id": "echo-plugin", "legacy_confirm": true,
                           "request_id": "grant-legacy"}),
                )
                .await;
                assert_eq!(grant["legacy_confirmed"], json!(true), "{grant}");
                let (workflow_id, _) = publish_legacy_workflow(
                    &client,
                    "unrestricted",
                    echo_definition(json!({"version": 1, "mode": "execute", "allow": []})),
                )
                .await;
                let run_id = start_run(&client, &workflow_id, json!({})).await;
                let pending = wait_pending_approval(&client, &run_id, "n", TIMEOUT).await;
                assert!(
                    pending["matched"].as_array().is_some_and(|matched| matched
                        .iter()
                        .any(|m| m.as_str().unwrap_or("").contains("legacy-confirmed"))),
                    "unrestricted 插件的询问必须标注 legacy-confirmed：{pending}"
                );
                approve(
                    &client,
                    &run_id,
                    "n",
                    pending["operation_id"].as_str().unwrap(),
                    pending["request_hash"].as_str().unwrap(),
                    "once",
                )
                .await;
                let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
                assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
            })
        },
    )
    .await;
}

/// I35：授予后插件内容（二进制/manifest）变化 → 精确哈希授予失配，
/// run.start 拒绝（换内容必须重新授予）。
#[tokio::test]
async fn plugin_content_change_after_grant_requires_regrant() {
    let (home, plugins_dir) = deploy_echo_plugin(true);
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[
            ("FLOW_PLUGINS_DIR", plugins_dir.as_str()),
            ("FLOW_TEST_AUTO_GOVERN", "0"),
        ],
        |ctx: &mut Ctx| {
            let home = home.path().to_path_buf();
            Box::pin(async move {
                let client = ctx.client().await;
                let workflow_id =
                    publish_governed_echo(&client, "regrant", workflow_permissions("auto")).await;
                // 授予后换掉入口内容（同一插件，哈希变化）。
                let entry = home
                    .join("plugins")
                    .join("echo-plugin")
                    .join("bin")
                    .join("echo-node");
                let original = std::fs::read_to_string(&entry).unwrap();
                std::fs::write(&entry, format!("{original}# patched\n")).unwrap();
                let error = call_err(
                    &client,
                    "run.start",
                    json!({"workflow_id": workflow_id, "input": {}}),
                )
                .await;
                assert_eq!(error.code(), -32020, "内容变化未重授予必须拒绝：{error}");
                assert!(
                    error.message().contains("未授予"),
                    "拒绝原因提示重新授予：{error}"
                );
            })
        },
    )
    .await;
}
