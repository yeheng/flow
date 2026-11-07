use flow_backend::journal::{CommandReceipt, JournalBackend};
use flow_engine::journal_state::command_key;
use flow_journal::{Event, EventKind};
use serde_json::json;
use std::sync::Arc;

async fn fixture(root: &std::path::Path) -> Arc<JournalBackend> {
    let backend = JournalBackend::open(root, Default::default())
        .await
        .unwrap();
    let created = backend.workflow_create("schedule", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow, json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    backend.config_change("schedule.change", EventKind::ScheduleChanged, "cron", json!({
        "workflow_id":workflow,"enabled":true,"cron_expr":"* * * * *","input":{"original":true}
    }), None).await.unwrap();
    backend
}

async fn evict_receipts(backend: &JournalBackend) {
    for batch in 0..65 {
        let events = (0..128)
            .map(|n| {
                Event::new(
                    EventKind::Command,
                    json!({
                        "scope":"unrelated", "request_id": format!("{}", batch * 128 + n),
                        "fingerprint":"test", "result":null, "lsn":"0"
                    }),
                )
            })
            .collect();
        backend.append(events).await.unwrap();
    }
}

async fn fire(backend: &JournalBackend, slot: &str) -> CommandReceipt {
    backend
        .trigger_start("schedule", "cron", slot, json!(null))
        .await
        .unwrap()
}

#[tokio::test]
async fn schedule_receipt_survives_eviction_config_edits_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let backend = fixture(root.path()).await;
    let at = chrono::Utc::now() + chrono::Duration::minutes(2);
    let slot = at.to_rfc3339();
    let (first, concurrent) = tokio::join!(fire(&backend, &slot), fire(&backend, &slot));
    assert_eq!(first.commit_cursor, concurrent.commit_cursor);
    evict_receipts(&backend).await;
    let state = backend.state().await.unwrap();
    assert!(!state
        .commands
        .contains_key(&command_key("run.start:schedule:cron", &slot)));
    assert_eq!(state.schedule_fires.len(), 1);
    let upper = backend.journal.durable_lsn();
    assert_eq!(
        fire(&backend, &slot).await.commit_cursor,
        first.commit_cursor
    );
    assert_eq!(
        backend.journal.durable_lsn(),
        upper,
        "retry must not write orphan inputs or receipts"
    );
    backend
        .config_change(
            "schedule.change",
            EventKind::ScheduleChanged,
            "cron",
            json!({"input":{"edited":true},"enabled":false}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(fire(&backend, &slot).await.result, first.result);
    let workflow = state.runs[first.result["run_id"].as_str().unwrap()]
        .workflow_id
        .clone();
    assert!(
        backend
            .run_start(
                &workflow,
                None,
                json!({"wrong":true}),
                "schedule",
                Some("cron"),
                Some(&slot)
            )
            .await
            .is_err(),
        "retained receipts still check fingerprints"
    );
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(
        fire(&backend, &slot).await.commit_cursor,
        first.commit_cursor
    );
    assert_eq!(backend.state().await.unwrap().runs.len(), 1);
    backend
        .config_change(
            "schedule.change",
            EventKind::ScheduleChanged,
            "cron",
            json!({"enabled":true}),
            None,
        )
        .await
        .unwrap();
    let later = (at + chrono::Duration::minutes(1)).to_rfc3339();
    fire(&backend, &later).await;
    assert_eq!(backend.state().await.unwrap().runs.len(), 2);
    let older = (at - chrono::Duration::seconds(10)).to_rfc3339();
    assert!(backend
        .trigger_start("schedule", "cron", &older, json!(null))
        .await
        .is_err());
    assert_eq!(backend.state().await.unwrap().runs.len(), 2);
    backend
        .config_change(
            "schedule.change",
            EventKind::ScheduleChanged,
            "cron",
            json!({"deleted":true}),
            None,
        )
        .await
        .unwrap();
    assert!(
        backend.state().await.unwrap().schedule_fires.is_empty(),
        "deleted schedules release watermark memory"
    );
    backend.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_restores_evicted_schedule_identity_without_executing_again() {
    let root = tempfile::tempdir().unwrap();
    let backend = fixture(root.path()).await;
    let slot = (chrono::Utc::now() + chrono::Duration::minutes(2)).to_rfc3339();
    let first = fire(&backend, &slot).await;
    evict_receipts(&backend).await;
    backend.close().await.unwrap();
    drop(backend);
    assert!(!root.path().join("checkpoints").exists());
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(backend.state().await.unwrap().schedule_fires.len(), 1);
    assert_eq!(
        fire(&backend, &slot).await.commit_cursor,
        first.commit_cursor
    );
    assert_eq!(backend.state().await.unwrap().runs.len(), 1);
    backend.close().await.unwrap();
}

#[tokio::test]
async fn rejected_transaction_rolls_back_schedule_watermark_and_receipt() {
    let root = tempfile::tempdir().unwrap();
    let backend = fixture(root.path()).await;
    let at = chrono::Utc::now() + chrono::Duration::minutes(2);
    let slot = at.to_rfc3339();
    fire(&backend, &slot).await;
    let state = backend.state().await.unwrap();
    let mut command = state.schedule_fires["cron"].command.clone();
    let later = (at + chrono::Duration::minutes(1)).to_rfc3339();
    command.request_id = Some(later.clone());
    let events = vec![
        Event::new(EventKind::Command, json!(command)),
        Event::new(
            EventKind::WorkflowPublished,
            json!({"workflow_id":"missing","version":"1"}),
        ),
    ];
    assert!(backend.append(events).await.is_err());
    let after = backend.state().await.unwrap();
    assert_eq!(
        after.schedule_fires["cron"].at,
        state.schedule_fires["cron"].at
    );
    assert!(after
        .command_record("run.start:schedule:cron", &later)
        .is_none());
    backend.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_caller_does_not_lose_the_committed_schedule_slot() {
    let root = tempfile::tempdir().unwrap();
    let backend = fixture(root.path()).await;
    let slot = (chrono::Utc::now() + chrono::Duration::minutes(2)).to_rfc3339();
    let task_backend = backend.clone();
    let task_slot = slot.clone();
    let caller = tokio::spawn(async move { fire(&task_backend, &task_slot).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while backend.state().await.unwrap().schedule_fires.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    let retry = fire(&backend, &slot).await;
    assert!(retry.visible);
    assert_eq!(backend.state().await.unwrap().runs.len(), 1);
    assert_eq!(backend.state().await.unwrap().schedule_fires.len(), 1);
    backend.close().await.unwrap();
}
