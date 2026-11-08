//! 动态节点类型 e2e：manifest 声明全新 node type → listing → publish →
//! 运行时派发进 python echo 插件进程 → 输出落 journal → run 详情带投影。
//!
//! 这是「插件 UI 层 + 动态节点类型」整条特性的验收用例。

use flow_backend::journal::JournalBackend;
use flow_test_support::client::{call_json, connect};
use serde_json::{json, Value};

const ECHO_MANIFEST: &str = r#"
[plugin]
id = "echo-plugin"
name = "Echo"
version = "0.1.0"
entry = "{ENTRY}"
protocol = 1
node_types = ["echo"]

[methods]
invoke = "node/invoke"

[[ui.params]]
for = "echo"
[[ui.params.field]]
key = "text"
type = "code"
format = "text"
required = true
[[ui.params.field]]
key = "times"
type = "number"
min = 1
max = 5
default = "1"
[[ui.params.field]]
key = "mode"
type = "enum"
values = ["upper", "lower"]
default = "upper"

[ui]
[ui.palette]
category = "plugin"
icon_key = "echo"

[ui.run_view]
summary_keys = ["echo", "mode"]
"#;

#[tokio::test]
async fn plugin_declared_node_type_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let plugins_home = tempfile::tempdir().unwrap();
    let plugins = plugins_home.path().join("plugins");
    let plugin_dir = plugins.join("echo-plugin").join("bin");
    std::fs::create_dir_all(&plugin_dir).unwrap();

    // 入口脚本：绝对路径引用仓库内的 python 参考实现（解释器插件无
    // manifest args 字段，用 shell 包装器落地——真实部署是自带解释器的
    // 单文件二进制）。
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
    let entry = plugin_dir.join("echo-node");
    std::fs::write(
        &entry,
        format!("#!/bin/sh\nexec '{python}' '{}'\n", script.display()),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755)).unwrap();
    let manifest = ECHO_MANIFEST.replace("{ENTRY}", &entry.display().to_string());
    std::fs::write(plugins.join("echo-plugin").join("plugin.toml"), manifest).unwrap();
    std::env::set_var("FLOW_PLUGINS_DIR", &plugins);

    let backend: std::sync::Arc<JournalBackend> =
        JournalBackend::open(dir.path(), Default::default())
            .await
            .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                    flow_test_support::io::executor_bin(),
                ),
                x_max: 1,
                tag: None,
            },
        ))
        .await
        .unwrap();

    // ---- listing：echo 类型出现，且携带 manifest 表单转换的 schema ----
    let plugins_dir = plugins.display().to_string();
    flow_test_support::server::run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_PLUGINS_DIR", plugins_dir.as_str())],
        |ctx| Box::pin(async move {
            let client = connect(ctx.addr(), ctx.token()).await;
            let types: Value =
                call_json(&client, "nodetypes.list", json!({})).await;
            let echo = types["node_types"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["type"] == "echo")
                .expect("echo 类型必须出现在 listing");
            assert_eq!(echo["x-plugin"], "echo-plugin");
            assert_eq!(echo["params_schema"]["properties"]["mode"]["enum"][0], "upper");
            assert_eq!(echo["params_schema"]["properties"]["times"]["minimum"].as_f64(), Some(1.0));

            // plugins.list：发现条目。
            let plugin_list: Value =
                call_json(&client, "plugins.list", json!({})).await;
            assert!(plugin_list["plugins"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["id"] == "echo-plugin"));

            // ---- publish：动态类型被接受；参数违反 manifest 闭集被拒 ----
            let wf = call_json(&client, "workflow.create", json!({"name": "echo-e2e"})).await;
            let workflow_id = wf["workflow_id"].as_str().unwrap().to_owned();

            let bad = flow_test_support::client::call_err(&client, "workflow.update", json!({
                "workflow_id": workflow_id,
                "definition": {
                    "semantics": 2,
                    "nodes": [
                        {"id": "s", "type": "start"},
                        {"id": "n", "type": "echo", "params": {"text": "hi", "times": 99}},
                        {"id": "e", "type": "end"}
                    ],
                    "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
                }
            }))
            .await;
            assert!(bad.message().contains("上限"), "times=99 超上限必须被拒：{bad}");

            let good = call_json(&client, "workflow.update", json!({
                "workflow_id": workflow_id,
                "definition": {
                    "semantics": 2,
                    "nodes": [
                        {"id": "s", "type": "start"},
                        {"id": "n", "type": "echo", "params": {"text": "flow", "times": 3, "mode": "upper"}},
                        {"id": "e", "type": "end"}
                    ],
                    "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
                }
            }))
            .await;
            assert!(good.get("error").is_none(), "合法 echo 定义必须被接受：{good}");
            call_json(&client, "workflow.publish", json!({"workflow_id": workflow_id, "version": 1})).await;

            // ---- run：运行时经插件进程执行 ----
            let run = call_json(&client, "run.start", json!({
                "workflow_id": workflow_id, "input": {}
            })).await;
            let run_id = run["run_id"].as_str().unwrap().to_owned();

            let terminal = loop {
                let detail = call_json(&client, "run.get.view", json!({"run_id": run_id})).await;
                let status = detail["run"]["status"].as_str().unwrap_or_default().to_owned();
                if matches!(status.as_str(), "succeeded" | "failed") {
                    break detail;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            };
            assert_eq!(
                terminal["run"]["status"], "succeeded",
                "echo 插件节点应成功：{terminal}"
            );

            // 输出来自 python 插件进程（文本重复 3 次 + 大写）。
            // 节点输出走 timeline（run.get.view 的 RunRecord 不含台账）。
            let timeline = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
            let node_output = timeline["nodes"]
                .as_array()
                .and_then(|nodes| {
                    nodes
                        .iter()
                        .find(|entry| entry["id"] == "n")
                        .map(|entry| entry["output"].clone())
                })
                .expect("timeline 应含 echo 节点输出");
            let inline = node_output;
            assert_eq!(inline["echo"], "FLOW FLOW FLOW");
            assert_eq!(inline["mode"], "upper");
            assert_eq!(inline["via"], "python-echo");

            // run 详情携带冻结的插件投影（历史渲染自持）。
            let projections = terminal["plugin_projections"].as_array().cloned().unwrap_or_default();
            assert!(
                projections.iter().any(|projection| projection["id"] == "echo-plugin"),
                "run 详情应冻结 echo 插件投影：{projections:?}"
            );

            // ---- recheck：当前 manifest 下无违规 ----
            let recheck = call_json(&client, "plugins.recheck", json!({})).await;
            assert_eq!(recheck["violations"].as_array().map(Vec::len), Some(0), "{recheck}");

            // 下架插件后 recheck 报引用悬空。
            let _ = std::fs::remove_dir_all(plugins.join("echo-plugin"));
            let recheck = call_json(&client, "plugins.recheck", json!({})).await;
            assert!(
                recheck["violations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|violation| violation["node_type"] == "echo"),
                "插件下架后 recheck 必须点名 echo 引用：{recheck}"
            );
        }),
    )
    .await;

    backend.close().await.unwrap();
    std::env::remove_var("FLOW_PLUGINS_DIR");
    drop(plugins_home);
}
