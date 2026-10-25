//! Real executor processes, deterministic provider stalls, and mixed workloads.
use flow_backend::{
    execution::{ExecutionMode, IpcOptions},
    journal::JournalBackend,
};
use flow_engine::{
    execution_protocol::ExecutorInvocation,
    journal_state::{NodeStatus, RunStatus, WaitKind},
};
use flow_test_support::provider::{spawn_mock_openai, MockProvider, Turn};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

async fn start(root: &std::path::Path, provider: &MockProvider) -> (Arc<JournalBackend>, String) {
    let key = format!("ISOLATION_{}", provider.addr.port());
    std::env::set_var(format!("FLOW_SECRET_{key}"), "mock-key");
    let backend = JournalBackend::open(root, Default::default())
        .await
        .unwrap();
    backend
        .install_harness_agent(Some(flow_engine::AgentRunConfig {
            base_url: provider.base_url(),
            model: "mock-model".into(),
            max_turns: Some(4),
            ..Default::default()
        }))
        .unwrap();
    backend
        .start_execution_ipc(ExecutionMode::Ipc(IpcOptions {
            executor: ExecutorInvocation::explicit(flow_test_support::io::executor_bin()),
            x_max: 2,
            tag: None,
        }))
        .await
        .unwrap();
    (backend, key)
}

async fn workflow(backend: &JournalBackend, name: &str, params: Value, kind: &str) -> String {
    backend
        .workflow_import(
            name,
            json!({"semantics":2,"nodes":[
        {"id":"s","type":"start"},{"id":"n","type":kind,"params":params},{"id":"e","type":"end"}
    ],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
            true,
            None,
        )
        .await
        .unwrap()
        .result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn run(backend: &JournalBackend, workflow: &str) -> String {
    backend
        .run_start(workflow, None, Value::Null, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn until(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn slow_agents_preserve_normal_capacity_and_cancel_releases_the_quota() {
    let provider = spawn_mock_openai(vec![Turn::DelayedFinal {
        text: "done".into(),
        delay_ms: 30_000,
    }])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (backend, key) = start(root.path(), &provider).await;
    let agent = workflow(
        &backend,
        "slow",
        json!({"mode":"agent","prompt":"wait","api_key":key,"timeout_ms":60_000}),
        "harness",
    )
    .await;
    let first = run(&backend, &agent).await;
    until(async || provider.hits() == 1).await;
    let second = run(&backend, &agent).await;
    let ordinary = workflow(&backend, "ordinary", json!({"code":"return 42;"}), "script").await;
    let normal = run(&backend, &ordinary).await;
    until(async || {
        backend
            .run_snapshot(&normal)
            .await
            .unwrap()
            .unwrap()
            .terminal()
    })
    .await;
    assert_eq!(
        backend.run_snapshot(&normal).await.unwrap().unwrap().status,
        RunStatus::Succeeded
    );
    assert_eq!(
        provider.hits(),
        1,
        "second agent must wait without taking a dispatch slot"
    );
    backend.run_cancel(&first, None).await.unwrap();
    until(async || provider.hits() == 2).await;
    backend.run_cancel(&second, None).await.unwrap();
    assert!(backend.execution_peak() <= 2);
    backend.close().await.unwrap();
    provider.stop().await;
}

#[tokio::test]
async fn provider_stall_hits_lifecycle_deadline_and_retains_uncertain_evidence() {
    let provider = spawn_mock_openai(vec![Turn::DelayedFinal {
        text: "late".into(),
        delay_ms: 30_000,
    }])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (backend, key) = start(root.path(), &provider).await;
    // The lifecycle deadline must clear executor cold boot (spawn + harness
    // init + first provider request). On loaded 2-vCPU arm CI runners that
    // routinely takes >1.5s; a tighter deadline fires before the request is
    // ever sent, the executor is killed with zero provider hits, and the
    // "no automatic retry" assertion below fails with hits()==0.
    let agent = workflow(
        &backend,
        "deadline",
        json!({"mode":"agent","prompt":"wait","api_key":key,"timeout_ms":6000}),
        "harness",
    )
    .await;
    let id = run(&backend, &agent).await;
    until(async || {
        backend
            .run_snapshot(&id)
            .await
            .unwrap()
            .unwrap()
            .nodes
            .get("n")
            .is_some_and(|n| {
                n.wait
                    .as_ref()
                    .is_some_and(|w| w.kind() == WaitKind::Uncertain)
            })
    })
    .await;
    let snapshot = backend.run_snapshot(&id).await.unwrap().unwrap();
    let node = &snapshot.nodes["n"];
    assert_eq!(node.status, NodeStatus::Waiting);
    assert!(node.operation.as_ref().unwrap().outcome.is_none());
    assert_eq!(
        provider.hits(),
        1,
        "uncertain agent must not be retried automatically"
    );
    let ordinary = workflow(
        &backend,
        "after-timeout",
        json!({"code":"return 42;"}),
        "script",
    )
    .await;
    let normal = run(&backend, &ordinary).await;
    until(async || {
        backend
            .run_snapshot(&normal)
            .await
            .unwrap()
            .unwrap()
            .terminal()
    })
    .await;
    backend.run_cancel(&id, None).await.unwrap();
    backend.close().await.unwrap();
    provider.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn unterminated_shell_output_cannot_hold_an_agent_slot_forever() {
    // conga 425d786's persistent shell expects a separate sentinel line. printf
    // without a newline merges that line into stdout. Keep this failure case
    // visible; a normal mixed-load benchmark uses newline-terminated output.
    let provider = spawn_mock_openai(vec![Turn::Tool {
        command: "printf partial".into(),
    }])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (backend, key) = start(root.path(), &provider).await;
    // Same cold-boot constraint as the deadline test above: the sentinel line
    // merged into stdout leaves the tool call stuck in `collect` (its own
    // timeout is 120s), so the node deadline is what ends the task. It must
    // fire after boot + the first provider turn, not during boot.
    let agent = workflow(
        &backend,
        "unterminated-output",
        json!({"mode":"agent","prompt":"run tool",
        "api_key":key,"timeout_ms":6000}),
        "harness",
    )
    .await;
    let id = run(&backend, &agent).await;
    until(async || {
        backend
            .run_snapshot(&id)
            .await
            .unwrap()
            .unwrap()
            .nodes
            .get("n")
            .is_some_and(|n| {
                n.wait
                    .as_ref()
                    .is_some_and(|w| w.kind() == WaitKind::Uncertain)
            })
    })
    .await;
    assert_eq!(provider.hits(), 1);
    assert!(backend.run_snapshot(&id).await.unwrap().unwrap().nodes["n"]
        .operation
        .as_ref()
        .unwrap()
        .outcome
        .is_none());
    backend.run_cancel(&id, None).await.unwrap();
    backend.close().await.unwrap();
    provider.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn successful_agent_retires_tool_descendants_before_reusing_capacity() {
    let workdir = tempfile::tempdir().unwrap();
    let marker = workdir.path().join("leaked");
    let provider = spawn_mock_openai(vec![
        Turn::Tool {
            command: format!(
                "(sleep 6; echo leaked > '{}') >/dev/null 2>&1 & echo started",
                marker.display()
            ),
        },
        Turn::Final {
            text: "done".into(),
        },
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (backend, key) = start(root.path(), &provider).await;
    let agent = workflow(
        &backend,
        "descendants",
        json!({"mode":"agent","prompt":"start background tool then finish",
        "api_key":key,"workdir":workdir.path(),"timeout_ms":30_000}),
        "harness",
    )
    .await;
    let id = run(&backend, &agent).await;
    until(async || backend.run_snapshot(&id).await.unwrap().unwrap().terminal()).await;
    assert_eq!(
        backend.run_snapshot(&id).await.unwrap().unwrap().status,
        RunStatus::Succeeded
    );
    assert_eq!(provider.hits(), 2);
    // Keep the backend alive: shutdown cleanup must not hide a successful-slot leak.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        !marker.exists(),
        "background tool survived a successful agent task"
    );
    backend.close().await.unwrap();
    provider.stop().await;
}
