use flow_backend::journal::JournalBackend;
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
        .unwrap()
        .values
        .check(&StoredValue::Ref(value.clone()))
        .is_ok());
    assert!(backend
        .state()
        .await
        .unwrap()
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
    flow_journal::value::read_value(
        &root,
        backend.journal.durable_lsn(),
        &value,
        true,
        &mut std::io::sink(),
    )
    .unwrap();
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn concurrent_identity_attaches_and_state_receipt_restore_from_one_database() {
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
        .store
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
    let before = backend.journal.durable_lsn();
    backend.close().await.unwrap();
    drop(backend);
    assert!(root.join("flow.sqlite").is_file());
    for obsolete in [
        "journal",
        "checkpoints",
        "indexes",
        "execution-index.sqlite",
        "projection.sqlite",
    ] {
        assert!(!root.join(obsolete).exists());
    }
    let recovered = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(recovered.journal.durable_lsn(), before);
    assert_eq!(recovered.state().await.unwrap().workflows.len(), 1);
    assert!(recovered
        .store
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
async fn committed_state_is_immediately_visible_and_retry_does_not_duplicate() {
    let root = temp();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let receipt = backend
        .command("create", Some("r"), &json!({}), |_| Ok(creation("w")))
        .await
        .unwrap();
    assert!(receipt.committed && receipt.visible);
    assert!(backend
        .store
        .get("workflow", "w")
        .await
        .unwrap()
        .1
        .is_some());
    assert!(
        backend
            .command_status("create", "r")
            .await
            .unwrap()
            .unwrap()
            .visible
    );
    let retry = backend
        .command("create", Some("r"), &json!({}), |_| {
            panic!("duplicate decision")
        })
        .await
        .unwrap();
    assert_eq!(retry.commit_cursor, receipt.commit_cursor);
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
    assert!(backend.state().await.unwrap().workflows.is_empty());
    assert_eq!(backend.journal.durable_lsn(), before);
    assert!(backend
        .store
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
async fn cancelled_run_start_is_scheduled_without_a_retry_or_state_read() {
    let root = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(
        root.path(),
        JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                sync_delay_ms: 100,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let workflow = b
        .workflow_create("cancelled-caller", None)
        .await
        .unwrap()
        .result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.workflow_update(&workflow,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    b.workflow_publish(&workflow, 1, None).await.unwrap();
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
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
    let owner = b.clone();
    let caller = tokio::spawn(async move {
        owner
            .run_start(
                &workflow,
                None,
                Value::Null,
                "manual",
                None,
                Some("lost-start"),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !b.journal.stats().in_flight {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    // Query only SQLite. No call here can finish the coordinator's pending reducer.
    let run = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let (_, rows) = b
                .store
                .runs_page(flow_store::store::RunsQuery {
                    status: Some("succeeded"),
                    limit: 1,
                    ..Default::default()
                })
                .await
                .unwrap();
            if let Some(run) = rows.into_iter().next() {
                break run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        b.command_status("run.start:manual:", "lost-start")
            .await
            .unwrap()
            .unwrap()
            .result["run_id"],
        run["run_id"]
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn unfinished_value_resumes_from_sqlite_progress_after_restart() {
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
    assert!(!root.join("checkpoints").exists());
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

    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let mut bytes = Vec::new();
    flow_journal::value::read_value(
        &root,
        backend.journal.durable_lsn(),
        &value,
        true,
        &mut bytes,
    )
    .unwrap();
    assert_eq!(bytes, b"{}");
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
async fn audit_receipts_survive_cache_eviction_and_repeated_restart() {
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
    let snapshot = b.state().await.unwrap();
    let attempt = &snapshot.runs[&run_id].nodes["s"].attempts["dispatch-s"];
    assert_eq!(attempt.audit_seq, 128);
    assert!(attempt.audit_digests.is_empty());
    assert!(attempt.audit_lsns.is_empty());
    b.append(vec![audit(129)]).await.unwrap();
    let before_fallback = serde_json::to_value(b.state().await.unwrap()).unwrap();
    let mut fallback = audit(130);
    fallback.node_id = None;
    fallback.payload["node_id"] = json!("s");
    assert!(b.append(vec![fallback, audit(132)]).await.is_err());
    assert_eq!(
        serde_json::to_value(b.state().await.unwrap()).unwrap(),
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
    for _ in 0..2 {
        let b = JournalBackend::open(root.path(), Default::default())
            .await
            .unwrap();
        assert_eq!(b.append(vec![first.clone()]).await.unwrap(), original);
        assert!(b.append(vec![changed.clone()]).await.is_err());
        assert_eq!(
            b.state().await.unwrap().runs[&run_id].nodes["s"].attempts["dispatch-s"].audit_seq,
            129
        );
        b.close().await.unwrap();
    }
}

#[tokio::test]
async fn metadata_skips_blob_reads_and_definition_versions_are_stored_once() {
    let root = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let created = b.workflow_create("metadata", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    b.workflow_update(workflow,json!({"nodes":[{"id":"s","type":"start","name":"x".repeat(100*1024)},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    b.workflow_publish(workflow, 1, None).await.unwrap();
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        root.path().join("flow.sqlite").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE value_chunks SET bytes=zeroblob(length(bytes))")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        flow_backend::journal_views::version_metadata(&b, workflow)
            .await
            .unwrap()[0]["version"],
        1
    );
    assert!(
        flow_backend::journal_views::get_version(&b, workflow, Some(1))
            .await
            .is_err()
    );
    let summary: String =
        sqlx::query_scalar("SELECT body FROM current_entities WHERE kind='workflow'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&summary).unwrap()["versions"],
        json!({})
    );
    let versions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM current_entities WHERE kind='workflow_version'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(versions, 1);
    pool.close().await;
    b.close().await.unwrap();
}
