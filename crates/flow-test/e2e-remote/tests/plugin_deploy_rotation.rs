use flow_backend::execution::{
    remote::{AgentCredential, AgentManager, RemoteOptions},
    ExecutionMode, IpcOptions,
};
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::contract::ExecutorInvocation;
use flow_engine::journal_state::RunStatus;
use flow_test_support::{fixtures::StubHttp, io};
use serde_json::{json, Value};
use std::{fs, path::Path, time::Duration};

const TOKEN: &str = "test-token-for-plugin-deployment-rpc";

fn model_bundle(root: &Path, version: &str) {
    fs::create_dir_all(root.join("bin")).unwrap();
    fs::copy(
        io::workspace_bin("flow-llm-plugin"),
        root.join("bin/flow-llm-plugin"),
    )
    .unwrap();
    let manifest = include_str!("../../../../plugins/llm/plugin/plugin.toml")
        .replace("version = \"0.1.0\"", &format!("version = \"{version}\""));
    fs::write(root.join("plugin.toml"), manifest).unwrap();
}

async fn start_node(backend: &JournalBackend, node_type: &str, params: Value) -> String {
    let created = backend
        .workflow_create("deployment test", None)
        .await
        .unwrap();
    let id = created.result["workflow_id"].as_str().unwrap();
    backend
        .workflow_update(
            id,
            json!({
                "permissions":{"version":1,"mode":"bypass","allow":[]},
                "nodes":[{"id":"s","type":"start"},
                    {"id":"n","type":node_type,"params":params},{"id":"e","type":"end"}],
                "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]
            }),
            None,
        )
        .await
        .unwrap();
    backend.workflow_publish(id, 1, None).await.unwrap();
    let receipt = backend
        .run_start(id, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    receipt.result["run_id"].as_str().unwrap().to_owned()
}

async fn verify_model(backend: &JournalBackend) {
    let model = StubHttp::fixed(200, json!({"choices":[{"message":{"content":"fixture"}}]})).await;
    let run_id = start_node(
        backend,
        "llm",
        json!({
            "url": model.url("/model"),
            "body": {"model":"fixture","messages":[{"role":"user","content":"hello"}]}
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let run = backend.run_snapshot(&run_id).await.unwrap().unwrap();
            if run.terminal() {
                assert_eq!(run.status, RunStatus::Succeeded, "{:?}", run.error);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("model workflow must finish");
    assert_eq!(model.requests().len(), 1);
}

#[tokio::test]
async fn deploy_rotates_the_local_pool_and_refreshes_live_agents() {
    let root = tempfile::tempdir().unwrap();
    let shared = root.path().join("shared-plugins");
    // This test binary owns its environment and the test deployment.
    unsafe {
        std::env::set_var("FLOW_PLUGINS_DIR", &shared);
    }
    let initial = root.path().join("initial");
    let replacement = root.path().join("replacement");
    model_bundle(&initial, "1");
    model_bundle(&replacement, "2");

    let local = JournalBackend::open(&root.path().join("local"), Default::default())
        .await
        .unwrap();
    local
        .plugin_install(initial.to_str().unwrap().into(), false, "first".into())
        .await
        .unwrap();
    local
        .start_execution_ipc_prewarm(
            ExecutionMode::Ipc(IpcOptions {
                executor: ExecutorInvocation::explicit(io::executor_bin()),
                x_max: 1,
                tag: None,
            }),
            true,
        )
        .await
        .unwrap();
    verify_model(&local).await;
    local
        .plugin_install(replacement.to_str().unwrap().into(), true, "upgrade".into())
        .await
        .unwrap();
    verify_model(&local).await;
    local.close().await.unwrap();

    let remote = JournalBackend::open(&root.path().join("remote"), Default::default())
        .await
        .unwrap();
    let manager = AgentManager::new(remote.clone(), remote.journal.id().into(), 1);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    remote
        .start_execution_remote_manager(
            manager.clone(),
            RemoteOptions {
                control_addr: "127.0.0.1:0".into(),
                ca_cert: "unused".into(),
                server_cert: "unused".into(),
                server_key: "unused".into(),
                attach_timeout_ms: 10_000,
                wss_addr: None,
                wss_agents: Vec::new(),
            },
        )
        .await
        .unwrap();
    let names = ["agent-a", "agent-b"];
    manager
        .listen_wss(
            addr.to_string(),
            names
                .into_iter()
                .map(|id| AgentCredential {
                    id: id.into(),
                    token: format!("test-{id}"),
                })
                .collect(),
        )
        .await
        .unwrap();
    let mut agents = names
        .into_iter()
        .map(|id| {
            tokio::process::Command::new(io::agent_bin())
                .args([
                    "--wss-url",
                    &format!("ws://{addr}/ws/agent"),
                    "--agent-id",
                    id,
                    "--token",
                    &format!("test-{id}"),
                    "--slots",
                    "1",
                ])
                .env("FLOW_PLUGINS_DIR", &shared)
                .kill_on_drop(true)
                .spawn()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let old_revision = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let deployment = remote.plugin_deployment("llm").await.unwrap();
            if names
                .iter()
                .all(|agent| deployment["agents"][*agent] == "ready")
            {
                break deployment["publication_revision"].clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("initial catalog advertised");

    let held = StubHttp::hang().await;
    let held_run = start_node(
        &remote,
        "http_call",
        json!({"url": held.url("/held"), "timeout_ms": 30000}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while held.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("unrelated HTTP operation must be started before deployment");
    let before = remote.run_snapshot(&held_run).await.unwrap().unwrap();

    // A package-only change retains the executable identity. Readiness must
    // still wait for both live Agents to observe the new publication revision.
    fs::write(replacement.join("asset.txt"), b"new package asset").unwrap();
    let (rpc, rpc_addr) =
        flow_rpc::journal_v2::serve(remote.clone(), TOKEN.into(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let deployed = tokio::process::Command::new(io::cli_bin())
        .args([
            "--url",
            &format!("ws://{rpc_addr}"),
            "journal",
            "plugins",
            "deploy",
            "--source",
            replacement.to_str().unwrap(),
            "--replace",
            "--agent",
            names[0],
            "--agent",
            names[1],
            "--timeout",
            "15",
        ])
        .env("FLOW_JOURNAL_TOKEN", TOKEN)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        deployed.status.success(),
        "{}",
        String::from_utf8_lossy(&deployed.stderr)
    );
    let result: Value = serde_json::from_slice(&deployed.stdout).unwrap();
    assert_eq!(result["activation"], "ready");
    assert_ne!(result["deployment"]["publication_revision"], old_revision);
    assert!(names
        .iter()
        .all(|agent| result["deployment"]["agents"][*agent] == "ready"));
    let after = remote.run_snapshot(&held_run).await.unwrap().unwrap();
    assert!(
        !after.terminal(),
        "deployment must preserve the in-flight operation"
    );
    assert_eq!(after.nodes["n"].dispatch_id, before.nodes["n"].dispatch_id);
    assert_eq!(held.requests().len(), 1);
    remote
        .run_cancel(&held_run, Some("cancel-held"))
        .await
        .unwrap();
    verify_model(&remote).await;

    let unavailable = tokio::process::Command::new(io::cli_bin())
        .args([
            "--url",
            &format!("ws://{rpc_addr}"),
            "journal",
            "plugins",
            "deploy",
            "--source",
            replacement.to_str().unwrap(),
            "--agent",
            "not-registered",
            "--timeout",
            "1",
        ])
        .env("FLOW_JOURNAL_TOKEN", TOKEN)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(!unavailable.status.success());
    assert!(String::from_utf8_lossy(&unavailable.stderr).contains("not_connected"));
    assert_eq!(
        remote.plugin_deployment("llm").await.unwrap()["publication_revision"],
        result["deployment"]["publication_revision"]
    );
    for agent in &mut agents {
        assert!(agent.try_wait().unwrap().is_none());
        agent.kill().await.unwrap();
    }
    rpc.stop().unwrap();
    manager.shutdown().await;
    remote.close().await.unwrap();
}
