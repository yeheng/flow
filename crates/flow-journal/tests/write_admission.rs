use flow_journal::writer::WriteAdmission;
use flow_journal::{Event, EventKind, Journal, QueueClass};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn events() -> Vec<Event> {
    vec![Event::new(
        EventKind::Command,
        serde_json::json!({"test":true}),
    )]
}

#[tokio::test]
async fn lease_expiry_rejects_new_writes_and_cannot_be_revived() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(dir.path(), Default::default()).await.unwrap();
    let gate = Arc::new(WriteAdmission::new(Instant::now() + Duration::from_secs(1)));
    journal.set_write_admission(gate.clone()).unwrap();
    journal
        .submit("accepted".into(), events(), QueueClass::Control)
        .await
        .unwrap();
    assert!(gate.renew(Instant::now() + Duration::from_millis(30)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let lsn = journal.durable_lsn();
    assert!(journal
        .submit("expired".into(), events(), QueueClass::Control)
        .await
        .is_err());
    assert!(!gate.renew(Instant::now() + Duration::from_secs(30)));
    assert_eq!(journal.durable_lsn(), lsn);
    journal.close().await.unwrap();
    let reopened = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(reopened.durable_lsn(), lsn);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn revocation_rejects_a_transaction_already_waiting_for_group_commit() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(dir.path(), Default::default()).await.unwrap();
    let gate = Arc::new(WriteAdmission::new(
        Instant::now() + Duration::from_secs(30),
    ));
    journal.set_write_admission(gate.clone()).unwrap();
    // First poll enqueues the transaction; revocation happens before the writer's 5ms batch wait.
    let initial_lsn = journal.durable_lsn();
    let submit = journal.submit("queued".into(), events(), QueueClass::Control);
    tokio::pin!(submit);
    assert!(
        std::future::poll_fn(|cx| match submit.as_mut().poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Ready(true),
            std::task::Poll::Ready(_) => std::task::Poll::Ready(false),
        })
        .await
    );
    gate.revoke();
    assert!(submit.await.is_err());
    assert!(journal.close().await.is_err());
    let reopened = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(reopened.durable_lsn(), initial_lsn);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn lease_expiring_during_fsync_does_not_acknowledge_unknown_commit() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(
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
    let initial_lsn = journal.durable_lsn();
    let gate = Arc::new(WriteAdmission::new(
        Instant::now() + Duration::from_millis(50),
    ));
    journal.set_write_admission(gate.clone()).unwrap();
    assert!(journal
        .submit("unknown".into(), events(), QueueClass::Control)
        .await
        .is_err());
    assert!(!gate.renew(Instant::now() + Duration::from_secs(30)));
    assert!(journal.close().await.is_err());
    let reopened = Journal::open(dir.path(), Default::default()).await.unwrap();
    // A rejected acknowledgement does not promise that the bytes were absent.
    // Recovery must preserve the unacknowledged, already-fsynced transaction.
    assert!(reopened.durable_lsn() > initial_lsn);
    reopened.close().await.unwrap();
}
