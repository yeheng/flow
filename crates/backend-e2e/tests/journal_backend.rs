use flow_backend::journal::{JournalBackend, JournalError};
use flow_engine::journal_state::Workflow;
use flow_journal::{Event, EventKind, JournalOptions};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

fn creation(id: &str) -> (Vec<Event>, Value) {
    let w = Workflow {
        workflow_id: id.into(),
        name: "test".into(),
        created_at: "2026-09-30T00:00:00Z".into(),
        versions: BTreeMap::new(),
    };
    (
        vec![Event::new(
            EventKind::WorkflowCreated,
            serde_json::to_value(w).unwrap(),
        )],
        json!({"workflow_id":id}),
    )
}
fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-core-{}", uuid::Uuid::now_v7()))
}

#[tokio::test]
async fn publication_and_business_references_require_a_complete_value_closure() {
    use flow_journal::value::ValueCodec;
    use flow_journal::{StoredValue, ValueRef};
    let root = temp();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let value = ValueRef {
        journal_id: backend.journal.id().into(),
        output_id: "object".into(),
        codec: ValueCodec::Json,
        version: 1,
        chunk_count: 1,
        total_bytes: 2,
        digest: flow_journal::codec::digest(b"{}"),
    };
    let before = backend.journal.durable_lsn();
    assert!(backend
        .append(vec![Event::new(EventKind::ValuePublished, json!(value))])
        .await
        .is_err());
    assert_eq!(backend.journal.durable_lsn(), before);
    let chunk = Event::new(
        EventKind::ValueChunk,
        json!({"output_id":"object","chunk_index":"0","data":"e30="}),
    );
    // A later malformed sibling must also roll back the streaming hash/counters.
    assert!(backend
        .append(vec![
            chunk.clone(),
            Event::new(
                EventKind::WorkflowPublished,
                json!({"workflow_id":"missing","version":"1"})
            )
        ])
        .await
        .is_err());
    assert_eq!(backend.journal.durable_lsn(), before);
    backend.append(vec![chunk]).await.unwrap();
    let mut wrong = value.clone();
    wrong.digest = "0".repeat(64);
    assert!(backend
        .append(vec![Event::new(EventKind::ValuePublished, json!(wrong))])
        .await
        .is_err());
    backend
        .append(vec![Event::new(EventKind::ValuePublished, json!(value))])
        .await
        .unwrap();
    assert!(backend
        .state()
        .await
        .values
        .check(&StoredValue::Ref(value.clone()))
        .is_ok());
    assert!(backend
        .state()
        .await
        .values
        .check(&StoredValue::Ref(wrong))
        .is_err());
    let before = backend.journal.durable_lsn();
    assert!(backend
        .append(vec![Event::new(
            EventKind::ValueChunk,
            json!({"output_id":"object","chunk_index":"1","data":"e30="})
        )])
        .await
        .is_err());
    assert_eq!(backend.journal.durable_lsn(), before);
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    assert!(backend
        .state()
        .await
        .values
        .check(&StoredValue::Ref(value))
        .is_ok());
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn concurrent_same_identity_attaches_and_projection_is_disposable() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let b = Arc::clone(&backend);
        tasks.push(tokio::spawn(async move {
            b.command(
                "workflow.create",
                Some("request-1"),
                &json!({"name":"test"}),
                |_| Ok(creation("w1")),
            )
            .await
            .unwrap()
        }));
    }
    let mut lsns = Vec::new();
    for t in tasks {
        let r = t.await.unwrap();
        assert!(r.visible);
        lsns.push(r.commit_cursor.lsn);
    }
    assert!(lsns.iter().all(|v| *v == lsns[0]));
    assert_eq!(backend.journal.stats().transactions, 1);
    assert!(backend
        .projection
        .get("workflow", "w1")
        .await
        .unwrap()
        .1
        .is_some());
    assert!(backend
        .command(
            "workflow.create",
            Some("request-1"),
            &json!({"name":"changed"}),
            |_| Ok(creation("w2"))
        )
        .await
        .is_err());
    let before = serde_json::to_value(backend.state().await).unwrap();
    backend.close().await.unwrap();
    drop(backend);
    let rebuilt = root.join("rebuilt.sqlite");
    let rebuilt_lsn = JournalBackend::rebuild_projection(&root, &rebuilt)
        .await
        .unwrap();
    let projected =
        flow_store::projection::Projector::open(&rebuilt, before["journal_id"].as_str().unwrap())
            .await
            .unwrap();
    assert_eq!(
        projected.get("workflow", "w1").await.unwrap().0,
        rebuilt_lsn
    );
    assert!(projected.get("workflow", "w1").await.unwrap().1.is_some());
    projected.close().await;
    assert!(JournalBackend::rebuild_projection(&root, &rebuilt)
        .await
        .is_err());
    std::fs::remove_file(root.join("projection.sqlite")).unwrap();
    let recovered = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(recovered.state().await).unwrap(),
        before
    );
    assert!(recovered
        .projection
        .get("workflow", "w1")
        .await
        .unwrap()
        .1
        .is_some());
    let original = recovered
        .command_status("workflow.create", "request-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.result["workflow_id"], "w1");
    assert!(original.visible);
    recovered.close().await.unwrap();
    drop(recovered);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn paused_projection_returns_committed_result_and_retry_does_not_duplicate() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    backend.set_projection_wait(100);
    backend.pause_projection(true);
    let result = backend
        .command("create", Some("r"), &json!({}), |_| Ok(creation("w")))
        .await;
    let receipt = match result {
        Err(JournalError::CommittedNotVisible(r)) => r,
        other => panic!("{other:?}"),
    };
    assert!(receipt.committed);
    assert!(!receipt.visible);
    assert_eq!(receipt.result["workflow_id"], "w");
    assert!(backend
        .projection
        .get("workflow", "w")
        .await
        .unwrap()
        .1
        .is_none());
    assert!(
        !backend
            .command_status("create", "r")
            .await
            .unwrap()
            .unwrap()
            .visible
    );
    backend.pause_projection(false);
    let retry = backend
        .command("create", Some("r"), &json!({}), |_| {
            panic!("must not execute duplicate decision")
        })
        .await
        .unwrap();
    assert_eq!(retry.commit_cursor, receipt.commit_cursor);
    assert!(retry.visible);
    assert_eq!(backend.journal.stats().transactions, 1);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn invalid_multi_event_transaction_never_reaches_journal_or_projection() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let before = backend.journal.durable_lsn();
    let result = backend
        .command("create", None, &json!({}), |_| {
            let (mut events, result) = creation("w");
            events.push(Event::new(
                EventKind::WorkflowPublished,
                json!({"workflow_id":"missing","version":"1"}),
            ));
            Ok((events, result))
        })
        .await;
    assert!(result.is_err());
    assert!(backend.state().await.workflows.is_empty());
    assert_eq!(backend.journal.durable_lsn(), before);
    assert!(backend
        .projection
        .get("workflow", "w")
        .await
        .unwrap()
        .1
        .is_none());
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn disconnected_command_is_joined_before_retry_decision() {
    let root = temp();
    let options = JournalOptions {
        faults: flow_journal::storage::FaultInjection {
            sync_delay_ms: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    let backend = JournalBackend::open(&root, options).await.unwrap();
    let b = backend.clone();
    let first = tokio::spawn(async move {
        b.command("create", Some("lost"), &json!({}), |_| {
            Ok(creation("original"))
        })
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while backend.journal.stats().peak_queued_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    let _ = first.await;
    let status = backend
        .command_status("create", "lost")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.result["workflow_id"], "original");
    let receipt = backend
        .command("create", Some("lost"), &json!({}), |_| {
            panic!("retry must attach to the original in-flight command")
        })
        .await
        .unwrap();
    assert_eq!(receipt.result["workflow_id"], "original");
    assert!(receipt.visible);
    assert_eq!(backend.journal.stats().transactions, 1);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn checkpoint_resumes_partial_value_and_corrupt_cache_replays() {
    let root = temp();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    backend
        .append(vec![Event::new(
            EventKind::ValueChunk,
            json!({"output_id":"partial","chunk_index":"0","data":"ew=="}),
        )])
        .await
        .unwrap();
    let identity = backend.journal.id().to_owned();
    backend.close().await.unwrap();
    drop(backend);
    assert!(root.join("checkpoints/latest.json").exists());
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    backend
        .append(vec![Event::new(
            EventKind::ValueChunk,
            json!({"output_id":"partial","chunk_index":"1","data":"fQ=="}),
        )])
        .await
        .unwrap();
    let value = flow_journal::ValueRef {
        journal_id: identity,
        output_id: "partial".into(),
        codec: flow_journal::value::ValueCodec::Json,
        version: 1,
        chunk_count: 2,
        total_bytes: 2,
        digest: flow_journal::codec::digest(b"{}"),
    };
    backend
        .append(vec![Event::new(EventKind::ValuePublished, json!(value))])
        .await
        .unwrap();
    backend.close().await.unwrap();
    drop(backend);
    std::fs::write(root.join("checkpoints/latest.json"), b"corrupt cache").unwrap();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    backend
        .state()
        .await
        .values
        .check(&flow_journal::StoredValue::Ref(value))
        .unwrap();
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

/// 服务端在写响应前等到投影可见：并发写全部拿到 visible=true 回执，
/// 健康路径不出现 COMMITTED_NOT_VISIBLE（等待器无信号量上限，谓词为
/// 原子读，不逐唤醒查 SQLite）。
#[tokio::test]
async fn concurrent_writes_all_return_visible_without_committed_not_visible() {
    let root = temp();
    let backend = Arc::new(
        JournalBackend::open(&root, Default::default())
            .await
            .unwrap(),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..500 {
        let b = backend.clone();
        tasks.spawn(async move {
            let scope = "workflow.create".to_string();
            let request = format!("concurrent-{i}");
            let receipt = b
                .command(&scope, Some(&request), &json!({"n": i}), |state| {
                    let id = format!("w-concurrent-{i}");
                    let (events, result) = creation(&id);
                    let _ = state;
                    Ok((events, result))
                })
                .await
                .unwrap();
            assert!(receipt.visible, "receipt not visible: {receipt:?}");
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn audit_receipts_survive_compaction_restart_and_index_rebuild() {
    let root = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let created = b.workflow_create("audit-index", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    b.workflow_update(workflow, json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), None).await.unwrap();
    b.workflow_publish(workflow, 1, None).await.unwrap();
    let started = b
        .run_start(workflow, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let run_id = started.result["run_id"].as_str().unwrap().to_owned();
    let mut dispatch = Event::new(
        EventKind::DispatchStarted,
        json!({"node_execution_id":"exec-s","attempt":"1"}),
    );
    dispatch.run_id = Some(run_id.clone());
    dispatch.run_seq = 2;
    dispatch.node_id = Some("s".into());
    dispatch.dispatch_id = Some("dispatch-s".into());
    b.append(vec![dispatch]).await.unwrap();
    let audit = |seq| {
        let mut event = Event::new(
            EventKind::LateAudit,
            json!({"evidence":"retained in journal"}),
        );
        event.run_id = Some(run_id.clone());
        event.node_id = Some("s".into());
        event.dispatch_id = Some("dispatch-s".into());
        event.audit_seq = seq;
        event
    };
    let first = audit(1);
    let original = b.append(vec![first.clone()]).await.unwrap();
    for seq in 2..=128 {
        b.append(vec![audit(seq)]).await.unwrap();
    }
    let before = b.journal.durable_lsn();
    assert!(b.append(vec![audit(129), audit(131)]).await.is_err());
    assert_eq!(b.journal.durable_lsn(), before);
    let snapshot = b.state().await;
    let attempt = &snapshot.runs[&run_id].nodes["s"].attempts["dispatch-s"];
    assert_eq!(attempt.audit_seq, 128);
    assert!(attempt.audit_digests.is_empty());
    assert!(attempt.audit_lsns.is_empty());
    b.append(vec![audit(129)]).await.unwrap();
    let before_fallback = serde_json::to_value(b.state().await).unwrap();
    let mut fallback = audit(130);
    fallback.node_id = None;
    fallback.payload["node_id"] = json!("s");
    assert!(b.append(vec![fallback, audit(132)]).await.is_err());
    assert_eq!(
        serde_json::to_value(b.state().await).unwrap(),
        before_fallback
    );
    let mut missing_run = audit(130);
    missing_run.run_id = None;
    assert!(b.append(vec![missing_run]).await.is_err());
    assert_eq!(b.append(vec![first.clone()]).await.unwrap(), original);
    let mut changed = first.clone();
    changed.payload = json!({"evidence":"different"});
    assert!(b.append(vec![changed.clone()]).await.is_err());
    b.close().await.unwrap();
    drop(b);
    for rebuild in [false, true] {
        if rebuild {
            std::fs::remove_file(root.path().join("execution-index.sqlite")).unwrap();
        }
        let b = JournalBackend::open(root.path(), Default::default())
            .await
            .unwrap();
        assert_eq!(b.append(vec![first.clone()]).await.unwrap(), original);
        assert!(b.append(vec![changed.clone()]).await.is_err());
        assert_eq!(
            b.state().await.runs[&run_id].nodes["s"].attempts["dispatch-s"].audit_seq,
            129
        );
        b.close().await.unwrap();
    }
}

#[tokio::test]
async fn metadata_does_not_read_definitions_and_old_run_projection_is_rebuilt() {
    let root = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let created = b.workflow_create("metadata", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    b.workflow_update(workflow, json!({"nodes":[{"id":"s","type":"start","name":"x".repeat(100 * 1024)},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), None).await.unwrap();
    b.workflow_publish(workflow, 1, None).await.unwrap();
    assert!(matches!(
        b.state().await.workflows[workflow].versions[&1].definition,
        flow_journal::StoredValue::Ref(_)
    ));
    // Temporarily hide the journal bytes: a metadata list has no reason to touch them.
    std::fs::rename(
        root.path().join("journal"),
        root.path().join("held-journal"),
    )
    .unwrap();
    let metadata = flow_backend::journal_views::version_metadata(&b, workflow).await;
    let definition = flow_backend::journal_views::get_version(&b, workflow, Some(1)).await;
    std::fs::rename(
        root.path().join("held-journal"),
        root.path().join("journal"),
    )
    .unwrap();
    assert_eq!(metadata.unwrap()[0]["version"], 1);
    assert!(definition.is_err());
    let started = b
        .run_start(workflow, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let run_id = started.result["run_id"].as_str().unwrap().to_owned();
    b.run_cancel(&run_id, None).await.unwrap();
    let full = b.projection.get("run", &run_id).await.unwrap().1.unwrap();
    b.close().await.unwrap();
    drop(b);
    assert!(flow_journal::maintenance::load_checkpoint(root.path(), 5)
        .unwrap()
        .is_some());
    assert!(flow_journal::maintenance::load_checkpoint(root.path(), 3)
        .unwrap()
        .is_none());
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        root.path().join("projection.sqlite").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE journal_entities SET body=? WHERE kind='run' AND key=?")
        .bind(full.to_string())
        .bind(&run_id)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let b = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    assert!(!b.projection.requires_split_runs().await.unwrap());
    assert_eq!(
        b.projection.get("run", &run_id).await.unwrap().1.unwrap(),
        full
    );
    b.close().await.unwrap();
}
