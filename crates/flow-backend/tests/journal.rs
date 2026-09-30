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
