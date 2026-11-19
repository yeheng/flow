//! Local process fault tests live here to use the same fault-injection build as the remote matrix.
#![cfg(unix)]

use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use flow_backend::{
    execution::{dispatch::fault, ExecutionMode, IpcOptions},
    journal::JournalBackend,
};
use flow_engine::{
    execution_protocol::ExecutorInvocation,
    journal_state::{NodeStatus, Run, RunStatus},
};
use flow_journal::{EventKind, JournalOptions};
use flow_test_support::io::LocalDeployment;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};

async fn start(backend: &Arc<JournalBackend>, tag: Option<String>) {
    start_with_executor(backend, flow_test_support::io::executor_bin(), tag).await;
}

async fn start_with_executor(
    backend: &Arc<JournalBackend>,
    executor: PathBuf,
    tag: Option<String>,
) {
    backend
        .start_execution_ipc(ExecutionMode::Ipc(IpcOptions {
            executor: ExecutorInvocation::explicit(executor),
            x_max: 1,
            tag,
        }))
        .await
        .unwrap();
}

static LOCAL_SCENARIO: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const ACCEPTED: &str = "prepared_job:accepted:n";
const PREPARED: &str = "input_prepared:committed:n";

async fn launch(backend: &JournalBackend, params: serde_json::Value) -> String {
    launch_definition(
        backend,
        json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"script","params":params},
        {"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
        json!({"answer":42}),
    )
    .await
}

async fn launch_condition(backend: &JournalBackend, answer: u32) -> String {
    launch_definition(
        backend,
        json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"condition","params":{"expr":"input.answer > 100 ? {mark: true} : 0"}},
        {"id":"e","type":"end"},
        {"id":"x","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"e","port":"true"},
                 {"from":"n","to":"x","port":"false"}]}),
        json!({"answer":answer}),
    )
    .await
}

async fn launch_definition(
    backend: &JournalBackend,
    definition: serde_json::Value,
    input: serde_json::Value,
) -> String {
    let created = backend.workflow_create("prepared job", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(id, definition, None).await.unwrap();
    backend.workflow_publish(id, 1, None).await.unwrap();
    backend
        .run_start(id, None, input, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .into()
}

async fn wait_for(point: &str) {
    tokio::time::timeout(Duration::from_secs(20), fault::wait_reached(point))
        .await
        .expect("fault checkpoint reached");
}

async fn finished(backend: &JournalBackend, run_id: &str) -> Run {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = backend.state().await.unwrap().runs[run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("run settles after restart")
}

#[tokio::test]
async fn kill_worker_after_prepared_commit_reuses_the_frozen_input() {
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    let tag = format!("script-prepare-{}", uuid::Uuid::now_v7().simple());
    let _scenario = LOCAL_SCENARIO.lock().await;
    let holds = fault::register(&[PREPARED, ACCEPTED]);
    start(&backend, Some(tag.clone())).await;
    let id = launch(
        &backend,
        json!({"code":"return {answer: input.answer};","retry":{"max_attempts":2,"backoff_ms":5}}),
    )
    .await;
    wait_for(PREPARED).await;
    let before = backend.state().await.unwrap().runs[&id].nodes["n"]
        .prepared
        .clone()
        .unwrap();
    // One slot and a per-test tag identify the leased worker unambiguously.
    let killed = tokio::process::Command::new("pkill")
        .args(["-KILL", "-f", &format!("flow-executor.*--tag {tag}")])
        .status()
        .await
        .unwrap();
    assert!(killed.success());
    holds[PREPARED].send_replace(false);
    wait_for(ACCEPTED).await;
    holds[ACCEPTED].send_replace(false);
    let after = finished(&backend, &id).await;
    assert_eq!(after.status, RunStatus::Succeeded);
    let script = &after.nodes["n"];
    assert_eq!(script.attempts.len(), 2);
    assert_eq!(script.prepared.as_ref(), Some(&before));
    backend.close().await.unwrap();
    assert_eq!(
        count_fact(dir.path(), &id, "n", EventKind::InputPrepared),
        1
    );
}

#[tokio::test]
async fn dispatcher_crash_fixture() {
    let Some(root) = std::env::var_os("FLOW_SCRIPT_CRASH_FIXTURE") else {
        return;
    };
    let backend = JournalBackend::open(Path::new(&root), JournalOptions::default())
        .await
        .unwrap();
    let point = "dispatch:before:e";
    let holds = fault::register(&[point, ACCEPTED]);
    let executor = std::env::current_dir().unwrap().join("flow-executor");
    start_with_executor(&backend, executor, None).await;
    let run = if std::env::var_os("FLOW_CONDITION_CRASH_FIXTURE").is_some() {
        launch_condition(&backend, 101).await
    } else {
        launch(&backend, json!({"code":"return {answer: input.answer};"})).await
    };
    wait_for(ACCEPTED).await;
    holds[ACCEPTED].send_replace(false);
    wait_for(point).await;
    assert!(!backend.state().await.unwrap().runs[&run]
        .nodes
        .contains_key("e"));
    std::io::stdout()
        .write_all(format!("crash-ready:{run}\n").as_bytes())
        .unwrap();
    std::io::stdout().flush().unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn kill_dispatcher_after_script_commit_advances_the_successor_once() {
    assert_dispatcher_restart(false).await;
}

#[tokio::test]
async fn kill_dispatcher_after_condition_commit_advances_only_the_selected_branch_once() {
    assert_dispatcher_restart(true).await;
}

async fn assert_dispatcher_restart(condition: bool) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let deployment = LocalDeployment::copy_to(&dir.path().join("local bundle")).unwrap();
    let mut cmd = deployment.command(&std::env::current_exe().unwrap());
    cmd.args(["--exact", "dispatcher_crash_fixture", "--nocapture"])
        .env("FLOW_SCRIPT_CRASH_FIXTURE", &data)
        .stdout(Stdio::piped());
    if condition {
        cmd.env("FLOW_CONDITION_CRASH_FIXTURE", "1");
    }
    let mut child = cmd.spawn().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    let run = tokio::time::timeout(Duration::from_secs(25), async {
        while let Some(line) = output.next_line().await.unwrap() {
            if let Some(run) = line.strip_prefix("crash-ready:") {
                return run.to_owned();
            }
        }
        panic!("fixture exited before the crash window");
    })
    .await
    .unwrap();
    child.kill().await.unwrap();
    let backend = JournalBackend::open(&data, JournalOptions::default())
        .await
        .unwrap();
    let before = backend.state().await.unwrap().runs[&run].clone();
    assert!(!before.nodes.contains_key("e"));
    let committed = &before.nodes["n"];
    assert!(committed.output.is_some());
    if condition {
        assert_eq!(committed.branch, Some(true));
    }
    backend.close().await.unwrap();
    drop(backend);

    let output = tokio::time::timeout(
        Duration::from_secs(30),
        deployment.journal(&data).arg("resume").output(),
    )
    .await
    .expect("isolated journal runner resumes without optional components")
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let backend = JournalBackend::open(&data, JournalOptions::default())
        .await
        .unwrap();
    let after = backend.state().await.unwrap().runs[&run].clone();
    assert_eq!(after.status, RunStatus::Succeeded);
    assert_eq!(after.nodes["n"].dispatch_id, committed.dispatch_id);
    assert_eq!(after.nodes["n"].branch, committed.branch);
    if condition {
        assert_eq!(after.nodes["x"].status, NodeStatus::Skipped);
        assert!(after.nodes["x"].attempts.is_empty());
    }
    assert_eq!(after.nodes["e"].attempts.len(), 1);
    backend.close().await.unwrap();
    assert_eq!(count_fact(&data, &run, "n", EventKind::NodeCompleted), 1);
    assert_eq!(count_fact(&data, &run, "e", EventKind::NodeCompleted), 1);
}

#[tokio::test]
async fn cancelling_a_prepared_job_before_execute_releases_the_only_slot() {
    let _scenario = LOCAL_SCENARIO.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    let ready = "prepared_job:ready:n";
    let holds = fault::register(&[ready]);
    start(&backend, None).await;
    let cancelled = launch(&backend, json!({"code":"return 99;"})).await;
    wait_for(ready).await;
    backend.run_cancel(&cancelled, None).await.unwrap();
    holds[ready].send_replace(false);
    let clean = launch(&backend, json!({"code":"return 42;"})).await;
    assert_eq!(
        finished(&backend, &clean).await.status,
        RunStatus::Succeeded
    );
    backend.close().await.unwrap();
    assert_eq!(
        count_fact(dir.path(), &cancelled, "n", EventKind::NodeCompleted),
        0
    );
}

#[tokio::test]
async fn prepared_job_uses_the_frozen_timeout() {
    let _scenario = LOCAL_SCENARIO.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    let holds = fault::register(&[ACCEPTED]);
    start(&backend, None).await;
    let run = launch(&backend, json!({"code":"while(true) {}","timeout_ms":10})).await;
    wait_for(ACCEPTED).await;
    holds[ACCEPTED].send_replace(false);
    let done = finished(&backend, &run).await;
    assert_eq!(done.status, RunStatus::Failed);
    assert_eq!(
        done.nodes["n"].prepared.as_ref().unwrap().params,
        flow_journal::StoredValue::Inline(json!({"code":"while(true) {}","timeout_ms":10}))
    );
    assert!(
        done.nodes["n"]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("表达式求值失败")),
        "{:?}",
        done.nodes["n"].error
    );
    backend.close().await.unwrap();
}

#[tokio::test]
async fn prepared_condition_preserves_truthiness_and_runs_only_the_chosen_branch() {
    let _scenario = LOCAL_SCENARIO.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    start(&backend, None).await;
    for (answer, branch, output, chosen, skipped) in [
        (101, true, json!({"mark":true}), "e", "x"),
        (42, false, json!(0), "x", "e"),
    ] {
        let holds = fault::register(&[ACCEPTED]);
        let run = launch_condition(&backend, answer).await;
        wait_for(ACCEPTED).await;
        holds[ACCEPTED].send_replace(false);
        let done = finished(&backend, &run).await;
        assert_eq!(done.status, RunStatus::Succeeded);
        assert_eq!(done.nodes["n"].branch, Some(branch));
        assert_eq!(
            done.nodes["n"].output,
            Some(flow_journal::StoredValue::Inline(output))
        );
        assert_eq!(done.nodes[chosen].attempts.len(), 1);
        assert_eq!(done.nodes[skipped].status, NodeStatus::Skipped);
        assert!(done.nodes[skipped].attempts.is_empty());
    }
    backend.close().await.unwrap();
}

fn count_fact(root: &Path, run: &str, node: &str, kind: EventKind) -> usize {
    let mut count = 0;
    let scan = flow_journal::scan(root, |transaction, _| {
        count += transaction
            .events
            .iter()
            .filter(|event| {
                event.kind == kind
                    && event.run_id.as_deref() == Some(run)
                    && event.node_id.as_deref() == Some(node)
            })
            .count();
        Ok(())
    })
    .unwrap();
    assert!(scan.fault.is_none());
    count
}
