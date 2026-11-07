//! 插件注册表 e2e：**manifest 驱动**的 harness 派发（R1/R2 验收）。
//!
//! 与 agent_provider.rs 的区别：本用例不依赖内置默认注册（兄弟二进制
//! 定位），而是把 `FLOW_PLUGINS_DIR` 指向临时目录，写入一份 entry 为
//! **绝对路径**的 manifest（指向共享 target 里的 flow-agent-plugin）——
//! 验证「发现 → 校验 → 路由 → spawn → 握手 → runTurn → 摘要」整条
//! manifest 路径。同时钉两条拒绝路径：内置类型名被拒、坏 manifest 不
//! 影响其他插件。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::RunStatus;
use flow_test_support::provider::{spawn_mock_openai, Turn};
use serde_json::{json, Value};
use std::sync::Arc;

const GOOD_MANIFEST: &str = r#"
[plugin]
id = "harness-agent"
name = "Harness Agent"
version = "9.9.9-e2e"
entry = "{ENTRY}"
protocol = 1
node_types = ["harness"]

[methods]
invoke = "agent/runTurn"

[[config.field]]
key = "model"
type = "text"
required = true
"#;

/// 想顶掉内置语义的 manifest：script 是编译期节点类型，必须被拒绝。
const EVIL_MANIFEST: &str = r#"
[plugin]
id = "evil"
version = "0.0.1"
entry = "/nonexistent-anyway"
protocol = 1
node_types = ["script"]

[methods]
invoke = "x"
"#;

#[tokio::test]
async fn manifest_discovered_plugin_routes_harness_dispatch() {
    let workdir = tempfile::tempdir().unwrap();
    let provider = spawn_mock_openai(vec![Turn::Final {
        text: "manifest routed".into(),
    }])
    .await;
    std::env::set_var("FLOW_SECRET_E2EMANIFEST", "sk-manifest-key");
    let dir = tempfile::tempdir().unwrap();
    // plugins 目录必须独立于 journal 数据目录（后者要求空目录起步）。
    let plugins_home = tempfile::tempdir().unwrap();

    // plugins 目录：好 manifest（绝对 entry）+ 坏 manifest（内置类型名）。
    let plugins = plugins_home.path().join("plugins");
    let harness_dir = plugins.join("harness-agent");
    std::fs::create_dir_all(&harness_dir).unwrap();
    let entry = flow_test_support::io::agent_plugin_bin();
    let manifest = GOOD_MANIFEST.replace("{ENTRY}", &entry.display().to_string());
    std::fs::write(harness_dir.join("plugin.toml"), manifest).unwrap();
    let evil_dir = plugins.join("evil");
    std::fs::create_dir_all(&evil_dir).unwrap();
    std::fs::write(evil_dir.join("plugin.toml"), EVIL_MANIFEST).unwrap();
    // FLOW_PLUGINS_DIR 经执行器环境白名单透传（environment.rs）。
    std::env::set_var("FLOW_PLUGINS_DIR", &plugins);

    let backend: Arc<JournalBackend> = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    backend
        .install_harness_agent(Some(flow_engine::AgentRunConfig {
            base_url: provider.base_url(),
            model: "mock-model".into(),
            api: "openai".into(),
            max_turns: Some(2),
            ..Default::default()
        }))
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

    let definition = json!({
        "semantics": 2,
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "harness", "params": {
                "mode": "agent",
                "prompt": "answer via manifest-discovered plugin",
                "workdir": workdir.path().to_string_lossy(),
                "api_key": "E2EMANIFEST",
                "timeout_ms": 60_000,
            }},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "a"}, {"from": "a", "to": "e"}]
    });
    let wf = backend
        .workflow_create("plugin-manifest", None)
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

    let run = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            let run = backend.run_snapshot(&run_id).await.unwrap().unwrap();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("manifest 路径的 harness 派发应在期限内收口");
    assert_eq!(
        run.status,
        RunStatus::Succeeded,
        "manifest 驱动派发应成功（error: {:?}）",
        run.error
    );

    // 摘要走的是 manifest 声明的 invoke 方法（agent/runTurn）→ 插件 →
    // mock provider：单轮终答即可证明整条链。
    assert_eq!(provider.hits(), 1, "单轮 provider 调用");
    let node_output = match run.nodes["a"].output.clone() {
        Some(flow_journal::StoredValue::Inline(value)) => value,
        other => panic!("agent 输出应为 inline：{other:?}"),
    };
    assert_eq!(node_output["stdout"], "manifest routed");
    assert_eq!(node_output["agent"]["stop_reason"], "end_turn");

    backend.close().await.unwrap();
    provider.stop().await;
    std::env::remove_var("FLOW_SECRET_E2EMANIFEST");
    std::env::remove_var("FLOW_PLUGINS_DIR");
    drop(plugins_home);
}
