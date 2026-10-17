//! Regressions from the single-machine core review.
use flow_backend::journal::JournalBackend;
use flow_journal::{Event, EventKind};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[cfg(unix)]
#[tokio::test]
async fn termination_signals_commit_and_exit_cleanly() {
    use backend_e2e::common::wire as common;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    for signal in [libc::SIGTERM, libc::SIGINT] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("journal");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_flow-journal-server-e2e"));
        cmd.env("FLOW_DATA_DIR", dir.path().join("secrets"))
            .env("FLOW_JOURNAL_DATA_DIR", &root)
            .env("FLOW_JOURNAL_TOKEN", common::TOKEN)
            .env("FLOW_EXECUTOR_BIN", flow_test_support::io::executor_bin())
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        let (child, ports) = common::spawn_reporting_ports(
            &mut cmd,
            "FLOW_JOURNAL_ADDR",
            "FLOW_JOURNAL_HTTP_ADDR",
            None,
        )
        .unwrap();
        let pid = child.id();
        let mut server = common::ServerProc::from_parts(child, ports.rpc, ports.http);
        let client = server.client().await;
        let created: serde_json::Value =
            common::call(&client, "workflow.create", json!({"name":"before-signal"})).await;
        // The child is owned by ServerProc until it has been reaped.
        assert_eq!(unsafe { libc::kill(pid as i32, signal) }, 0);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = server.exit_status() {
                assert!(status.success(), "signal {signal}: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "shutdown timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(root.join("flow.sqlite").is_file());
        assert!(!root.join("checkpoints").exists());
        let backend = JournalBackend::open(&root, Default::default())
            .await
            .unwrap();
        assert!(backend
            .full_state()
            .await
            .unwrap()
            .workflows
            .contains_key(created["workflow_id"].as_str().unwrap()));
        backend.close().await.unwrap();
    }
}

#[tokio::test]
async fn evicted_request_identity_keeps_original_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let first = b.workflow_create("once", Some("stable-id")).await.unwrap();
    for batch in 0..65 {
        let events = (0..128)
            .map(|i| {
                Event::new(
                    EventKind::Command,
                    json!({
                        "scope":"filler", "request_id":format!("{}",batch*128+i),
                        "fingerprint":"filler", "result":null, "lsn":"0"
                    }),
                )
            })
            .collect();
        b.append(events).await.unwrap();
    }
    assert!(b
        .command_status("workflow.create", "stable-id")
        .await
        .unwrap()
        .is_some());
    let second = b.workflow_create("once", Some("stable-id")).await.unwrap();
    assert_eq!(first.result, second.result);
    assert_eq!(first.commit_cursor, second.commit_cursor);
    assert!(b
        .workflow_create("different", Some("stable-id"))
        .await
        .is_err());
    b.close().await.unwrap();
    drop(b);
    for file in [
        "execution-index.sqlite",
        "execution-index.sqlite-wal",
        "execution-index.sqlite-shm",
    ] {
        let path = dir.path().join(file);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let retry = b.workflow_create("once", Some("stable-id")).await.unwrap();
    assert_eq!(first.commit_cursor, retry.commit_cursor);
    assert_eq!(first.result, retry.result);
    b.close().await.unwrap();
}

#[tokio::test]
async fn stream_progress_restores_from_current_sqlite_state() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let chunk = |index| {
        Event::new(
            EventKind::ValueChunk,
            json!({"output_id":"partial","chunk_index":format!("{index}"),"data":"eA=="}),
        )
    };
    b.append(vec![chunk(0)]).await.unwrap();
    b.checkpoint().await.unwrap();
    b.append(vec![chunk(1)]).await.unwrap();
    let value = flow_journal::ValueRef {
        journal_id: b.journal.id().into(),
        output_id: "partial".into(),
        codec: flow_journal::value::ValueCodec::Bytes,
        version: 1,
        chunk_count: 2,
        total_bytes: 2,
        digest: flow_journal::codec::digest(b"xx"),
    };
    b.append(vec![Event::new(
        EventKind::ValuePublished,
        serde_json::to_value(&value).unwrap(),
    )])
    .await
    .unwrap();
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let mut bytes = Vec::new();
    flow_journal::value::read_value(
        dir.path(),
        b.journal.durable_lsn(),
        &value,
        true,
        &mut bytes,
    )
    .unwrap();
    assert_eq!(bytes, b"xx");
    b.close().await.unwrap();
}

#[tokio::test]
async fn current_state_corruption_is_rejected_without_replaying_or_repairing() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    b.workflow_create("original", None).await.unwrap();
    b.close().await.unwrap();
    drop(b);
    let path = dir.path().join("flow.sqlite");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(&path))
        .await
        .unwrap();
    sqlx::query(r#"UPDATE current_entities SET body='{"name":"corrupted"}' WHERE kind='workflow'"#)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert!(JournalBackend::open(dir.path(), Default::default())
        .await
        .is_err());
    assert!(flow_journal::maintenance::verify(dir.path()).is_err());
}

#[tokio::test]
async fn unfinished_streams_do_not_exhaust_admission_or_poison_replay() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    for batch in 0..64 {
        let events = (0..128).map(|i|Event::new(EventKind::ValueChunk,json!({
            "output_id":format!("abandoned-{}",batch*128+i),"chunk_index":"0","data":"eA=="
        }))).collect();
        b.append(events).await.unwrap();
    }
    let reference = b
        .store_stream(
            &mut b"next".as_slice(),
            flow_journal::value::ValueCodec::Bytes,
        )
        .await
        .unwrap();
    assert_eq!(reference.total_bytes, 4);
    assert!(b.run_snapshot("any").await.unwrap().is_none());
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let mut bytes = Vec::new();
    flow_journal::value::read_value(
        dir.path(),
        b.journal.durable_lsn(),
        &reference,
        true,
        &mut bytes,
    )
    .unwrap();
    assert_eq!(bytes, b"next");
    // A previously cold unfinished stream can still receive its next chunk.
    b.append(vec![Event::new(
        EventKind::ValueChunk,
        json!({"output_id":"abandoned-0","chunk_index":"1","data":"eA=="}),
    )])
    .await
    .unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn config_patch_rejects_system_fields_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let w = b.workflow_create("w", Some("create-w")).await.unwrap();
    let before = b.journal.durable_lsn();
    let error = b
        .config_change(
            "webhook.change",
            EventKind::WebhookChanged,
            "hook",
            json!({"workflow_id":w.result["workflow_id"],"enabled":true,"created_at":"not-a-time"}),
            Some("bad-config"),
        )
        .await
        .unwrap_err();
    assert_eq!(b.journal.durable_lsn(), before);
    assert!(error.to_string().contains("not writable"));
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    b.workflow_create("still healthy", Some("after-reopen"))
        .await
        .unwrap();
    b.close().await.unwrap();
}

async fn ipc(b: &std::sync::Arc<JournalBackend>, tag: Option<String>, prewarm: bool) {
    b.start_execution_ipc_prewarm(
        flow_backend::execution::ExecutionMode::Ipc(flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 1,
            tag,
        }),
        prewarm,
    )
    .await
    .unwrap();
}

async fn install(b: &JournalBackend, definition: serde_json::Value) -> String {
    let c = b.workflow_create("probe", None).await.unwrap();
    let w = c.result["workflow_id"].as_str().unwrap();
    b.workflow_update(w, definition, None).await.unwrap();
    b.workflow_publish(w, 1, None).await.unwrap();
    w.to_owned()
}

#[tokio::test]
async fn cancelling_child_settles_parent_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    ipc(&b, None, false).await;
    let child = install(&b,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"human_task"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]})).await;
    let parent = install(&b,json!({"nodes":[{"id":"s","type":"start"},{"id":"c","type":"sub_workflow","params":{"workflow_id":child}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"c"},{"from":"c","to":"e"}]})).await;
    let receipt = b
        .run_start(&parent, None, json!(null), "manual", None, None)
        .await
        .unwrap();
    let parent_id = receipt.result["run_id"].as_str().unwrap();
    let child_id = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let r = b.run_snapshot(parent_id).await.unwrap().unwrap();
            if let Some(w) = r.nodes.get("c").and_then(|n| n.wait.as_ref()) {
                if let Some(id) = w.child_run_id() {
                    break id.to_owned();
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    b.run_cancel(&child_id, Some("cancel-child")).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if b.run_snapshot(parent_id).await.unwrap().unwrap().terminal() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    ipc(&b, None, false).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let r = b.run_snapshot(parent_id).await.unwrap().unwrap();
    assert!(r.terminal());
    assert_eq!(r.status, flow_engine::journal_state::RunStatus::Failed);
    assert!(r.nodes["c"].wait.is_none());
    assert_eq!(
        b.run_snapshot(&child_id).await.unwrap().unwrap().status,
        flow_engine::journal_state::RunStatus::Cancelled
    );
    b.close().await.unwrap();
}
