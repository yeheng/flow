use super::*;
use serde_json::json;
use std::sync::{Condvar, Mutex as StdMutex};
use std::time::Duration;

struct CommitBarrier {
    entered: Notify,
    released: StdMutex<bool>,
    resume: Condvar,
}
static BARRIER: StdMutex<Option<Arc<CommitBarrier>>> = StdMutex::new(None);
fn pause_commit(_: u64) {
    let barrier = BARRIER.lock().unwrap().clone();
    if let Some(barrier) = barrier {
        barrier.entered.notify_one();
        let mut released = barrier.released.lock().unwrap();
        while !*released {
            released = barrier.resume.wait(released).unwrap();
        }
    }
}
// Always release the writer, even when an assertion fails.
struct Release(Arc<CommitBarrier>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.resume.notify_all();
    }
}
fn create(id: &str) -> (Vec<Event>, Value) {
    (
        vec![Event::new(
            EventKind::WorkflowCreated,
            json!({
                "workflow_id":id,"name":id,"created_at":"2026-10-24T00:00:00Z","versions":{},"deleted":false
            }),
        )],
        json!({"workflow_id":id}),
    )
}

#[tokio::test]
async fn paused_commit_allows_reads_and_survives_caller_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(
        dir.path(),
        JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                before_data_sync: Some(pause_commit),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let barrier = Arc::new(CommitBarrier {
        entered: Notify::new(),
        released: StdMutex::new(false),
        resume: Condvar::new(),
    });
    *BARRIER.lock().unwrap() = Some(barrier.clone());
    let release = Release(barrier.clone());
    let mut commits = backend.subscribe_commits();
    let writer = backend.clone();
    let first = tokio::spawn(async move {
        writer
            .command("create", Some("one"), &json!({"id":"one"}), |_| {
                Ok(create("one"))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), barrier.entered.notified())
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(
        Duration::from_secs(1),
        backend.inspect(|s| (s.applied_lsn, s.workflows.len())),
    )
    .await
    .expect("reads must not wait for SQLite COMMIT");
    assert_eq!(snapshot, (0, 0), "uncommitted state must remain invisible");
    assert!(
        !commits.has_changed().unwrap(),
        "must not notify before COMMIT and reducer apply"
    );
    let diagnostics = tokio::time::timeout(Duration::from_secs(1), backend.diagnostics())
        .await
        .unwrap();
    assert_eq!(diagnostics["commit_pending"], true);
    assert!(backend.take_dirty_runs().await.is_empty());

    let writer = backend.clone();
    let second = tokio::spawn(async move {
        writer
            .command("create", Some("two"), &json!({"id":"two"}), |_| {
                Ok(create("two"))
            })
            .await
    });
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), commits.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(
        backend.inspect(|s| s.workflows.contains_key("one")).await,
        "accepted commits must apply without a retry or refresh read"
    );
    let second = second.await.unwrap().unwrap();
    assert_eq!(second.commit_cursor.lsn, 2);
    let original = backend
        .command("create", Some("one"), &json!({"id":"one"}), |_| {
            panic!("must return original receipt")
        })
        .await
        .unwrap();
    assert_eq!(original.commit_cursor.lsn, 1);
    assert_eq!(backend.inspect(|s| s.workflows.len()).await, 2);
    assert_eq!(backend.journal.stats().transactions, 2);
    backend.close().await.unwrap();
    *BARRIER.lock().unwrap() = None;
}

#[tokio::test]
async fn failed_commit_does_not_publish_or_allow_later_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(
        dir.path(),
        JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                fail_data_sync: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(backend
        .command("create", None, &json!({}), |_| Ok(create("one")))
        .await
        .is_err());
    assert!(
        backend
            .inspect(|s| s.workflows.is_empty() && s.applied_lsn == 0)
            .await
    );
    assert!(!backend.state.lock().await.pending);
    assert!(backend
        .command("create", None, &json!({}), |_| panic!("must fail closed"))
        .await
        .is_err());
    assert!(backend.cancellation().is_cancelled());
    assert!(backend.close().await.is_err());
}

#[tokio::test]
async fn rejected_publication_does_not_poison_later_commands_or_notify_readers() {
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let commits = backend.subscribe_commits();
    let rejected = Event::new(
        EventKind::ValuePublished,
        json!({
            "journal_id":backend.journal.id(),"output_id":"missing-chunks","codec":"json","version":1,
            "chunk_count":"1","total_bytes":"2",
            "digest":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        }),
    );
    assert!(backend.append(vec![rejected]).await.is_err());
    assert!(backend.state.lock().await.failure.is_none());
    assert!(!backend.cancellation().is_cancelled());
    assert!(!commits.has_changed().unwrap());
    assert_eq!(backend.inspect(|s| s.applied_lsn).await, 0);
    let receipt = backend
        .command("create", None, &json!({}), |_| Ok(create("valid")))
        .await
        .unwrap();
    assert_eq!(receipt.commit_cursor.lsn, 1);
    assert!(commits.has_changed().unwrap());
    backend.close().await.unwrap();
}
