//! Executable checks for the full-codebase review, including rejected claims.
use flow_backend::{journal::JournalBackend, journal_views};
use flow_journal::{Event, EventKind, Journal, StoredValue, Transaction};
use serde_json::{json, Value};
use std::time::Duration;

async fn install(b: &JournalBackend, definition: Value) -> String {
    b.workflow_import("review-check", definition, true, None)
        .await
        .unwrap()
        .result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned()
}
fn definition(kind: &str, params: Value) -> Value {
    json!({"semantics":2,"nodes":[{"id":"s","type":"start"},
        {"id":"n","type":kind,"params":params},{"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]})
}
async fn execution(b: &std::sync::Arc<JournalBackend>) {
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 1,
            tag: None,
        },
    ))
    .await
    .unwrap();
}
async fn finished(b: &JournalBackend, id: &str) -> flow_engine::journal_state::Run {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let run = b.run_snapshot(id).await.unwrap().unwrap();
            if run.terminal() {
                break run;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
fn chunks(dir: &std::path::Path) -> i64 {
    flow_journal::sqlite::read_connection(dir)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM value_chunks", [], |row| row.get(0))
        .unwrap()
}

#[tokio::test]
async fn rejected_large_signals_and_adjudications_publish_no_values() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let large = json!({"output":"private".repeat(12_000)});
    let initial = chunks(dir.path());
    assert!(b
        .run_start("absent", None, large.clone(), "manual", None, None)
        .await
        .is_err());
    let wf = install(&b, definition("human_task", json!({}))).await;
    let started = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let id = started.result["run_id"].as_str().unwrap();
    b.run_cancel(id, None).await.unwrap();
    for _ in 0..3 {
        assert!(b
            .run_signal(id, "n", large.clone(), Some("rejected"))
            .await
            .is_err());
        assert!(b
            .run_adjudicate(
                id,
                "n",
                "absent",
                "manual",
                large.clone(),
                "accept_output",
                "rejected"
            )
            .await
            .is_err());
    }
    assert_eq!(chunks(dir.path()), initial);
    // Early human-task inbox accepts the whole payload. Its `output` member
    // must not be independently published as a second unreferenced value.
    let started = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let id = started.result["run_id"].as_str().unwrap();
    assert_eq!(
        b.run_signal(id, "n", large, Some("queued"))
            .await
            .unwrap()
            .result["status"],
        "queued"
    );
    assert_eq!(chunks(dir.path()), initial + 1);
    b.close().await.unwrap();
}

#[tokio::test]
async fn malformed_control_row_returns_invalid_instead_of_panicking_on_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    j.submit_prepared(
        Transaction {
            v: 2,
            journal_id: j.id().into(),
            lsn: 1,
            tx_id: "malformed".into(),
            events: vec![Event::new(EventKind::Command, json!({}))],
        },
        vec![flow_journal::sqlite::EntityChange {
            kind: flow_journal::sqlite::EntityKind::Schedule,
            key: "s".into(),
            value: Some(json!([])),
        }],
    )
    .await
    .unwrap();
    j.close().await.unwrap();
    let result = JournalBackend::open(dir.path(), Default::default()).await;
    assert!(matches!(
        result,
        Err(flow_backend::journal::JournalError::Journal(
            flow_journal::Error::Invalid(_)
        ))
    ));
}

#[tokio::test]
async fn console_logs_and_timeline_redact_secrets_but_execution_output_keeps_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    execution(&b).await;
    let wf = install(
        &b,
        definition(
            "script",
            json!({"code":"console.log('payload', input); return input;"}),
        ),
    )
    .await;
    let input =
        json!({"token":"json-secret", "url":"https://example.test/?api_key=url-secret&ok=1"});
    let started = b
        .run_start(&wf, None, input.clone(), "manual", None, None)
        .await
        .unwrap();
    let id = started.result["run_id"].as_str().unwrap();
    let run = finished(&b, id).await;
    assert_eq!(
        run.status,
        flow_engine::journal_state::RunStatus::Succeeded,
        "{:?}",
        run.error
    );
    assert_eq!(run.output, Some(StoredValue::Inline(input)));
    let timeline = journal_views::timeline(&b, id).await.unwrap();
    let display = timeline["nodes"].to_string();
    assert!(
        !display.contains("json-secret") && !display.contains("url-secret"),
        "{display}"
    );
    assert_eq!(
        timeline["output"]["token"], "json-secret",
        "run-level output is the documented data contract"
    );
    let observations = b.observations.as_ref().unwrap();
    observations.flush();
    let logs = serde_json::to_string(&observations.page(id, None, 0, 100).unwrap()).unwrap();
    assert!(logs.contains("payload"), "{logs}");
    assert!(
        !logs.contains("json-secret") && !logs.contains("url-secret"),
        "{logs}"
    );
    let wf = install(&b, definition("script", json!({"code":"throw new Error('request failed https://example.test/?token=error-secret&ok=1');"}))).await;
    let started = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let id = started.result["run_id"].as_str().unwrap();
    let run = finished(&b, id).await;
    assert!(run
        .error
        .as_ref()
        .is_some_and(|e| e.contains("error-secret")));
    let timeline = journal_views::timeline(&b, id).await.unwrap().to_string();
    assert!(!timeline.contains("error-secret"), "{timeline}");
    b.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn executor_harness_does_not_inherit_any_ipc_socket() {
    let dir = tempfile::tempdir().unwrap();
    let evidence = dir.path().join("harness-fds");
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    execution(&b).await;
    let command = format!("echo inspected > '{}'; for fd in /dev/fd/*; do if test -S \"$fd\"; then echo socket >> '{}'; fi; done", evidence.display(), evidence.display());
    let wf = install(&b, definition("harness", json!({"command":"/bin/sh", "args":["-c",command], "prompt":"inspect inherited file descriptors"}))).await;
    let started = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let run = finished(&b, started.result["run_id"].as_str().unwrap()).await;
    assert_eq!(
        run.status,
        flow_engine::journal_state::RunStatus::Succeeded,
        "{:?}",
        run.error
    );
    assert_eq!(std::fs::read_to_string(evidence).unwrap(), "inspected\n");
    b.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_large_start_retries_publish_one_value_and_one_run() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(
        dir.path(),
        flow_journal::JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                sync_delay_ms: 100,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let wf = install(&b, definition("human_task", json!({}))).await;
    let input = json!({"large":"x".repeat(700_000)});
    let expected = serde_json::to_vec(&input)
        .unwrap()
        .len()
        .div_ceil(flow_journal::CHUNK_BYTES) as i64;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (b, wf, input, barrier) = (b.clone(), wf.clone(), input.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            b.run_start(&wf, None, input, "manual", None, Some("same-large-request"))
                .await
                .unwrap()
        }));
    }
    let mut receipts = Vec::new();
    for task in tasks {
        receipts.push(task.await.unwrap());
    }
    assert!(receipts
        .iter()
        .all(|r| r.commit_cursor == receipts[0].commit_cursor));
    assert_eq!(b.state().await.unwrap().runs.len(), 1);
    assert_eq!(
        chunks(dir.path()),
        expected,
        "concurrent retries must not publish unused values"
    );
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(b.state().await.unwrap().runs.len(), 1);
    assert_eq!(chunks(dir.path()), expected);
    b.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_large_start_caller_keeps_complete_atomic_commit_and_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(
        dir.path(),
        flow_journal::JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                sync_delay_ms: 150,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let wf = install(&b, definition("human_task", json!({}))).await;
    let input = json!({"large":"x".repeat(700_000)});
    let expected = serde_json::to_vec(&input)
        .unwrap()
        .len()
        .div_ceil(flow_journal::CHUNK_BYTES) as i64;
    let task = {
        let b = b.clone();
        let wf = wf.clone();
        let input = input.clone();
        tokio::spawn(async move {
            b.run_start(
                &wf,
                None,
                input,
                "manual",
                None,
                Some("cancelled-large-request"),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !b.journal.stats().in_flight {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let receipt = b
        .command_status("run.start:manual:", "cancelled-large-request")
        .await
        .unwrap()
        .unwrap();
    let retry = b
        .run_start(
            &wf,
            None,
            input,
            "manual",
            None,
            Some("cancelled-large-request"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.commit_cursor, retry.commit_cursor);
    assert_eq!(chunks(dir.path()), expected);
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(b.state().await.unwrap().runs.len(), 1);
    assert_eq!(chunks(dir.path()), expected);
    b.close().await.unwrap();
}

#[tokio::test]
async fn large_definition_and_version_commit_together_and_equal_import_reuses_value() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let def = definition(
        "script",
        json!({"code":format!("/*{}*/ return input;","x".repeat(200_000))}),
    );
    let receipt = b
        .workflow_import("large definition", def.clone(), true, Some("definition"))
        .await
        .unwrap();
    let count = chunks(dir.path());
    assert!(count > 0);
    assert_eq!(
        b.journal.stats().data_syncs,
        1,
        "definition, version and receipt have one COMMIT"
    );
    let equal = b
        .workflow_import("large definition", def, true, None)
        .await
        .unwrap();
    assert_eq!(receipt.result["workflow_id"], equal.result["workflow_id"]);
    assert_eq!(equal.result["version"], 1);
    assert_eq!(chunks(dir.path()), count);
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(chunks(dir.path()), count);
    b.close().await.unwrap();
}
