use flow_journal::sqlite::EntityChange;
use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass, Transaction};
use serde_json::json;

fn prepared(
    j: &Journal,
    lsn: u64,
    name: &str,
    rows: Vec<EntityChange>,
) -> (Transaction, Vec<EntityChange>) {
    (
        Transaction {
            v: 2,
            journal_id: j.id().into(),
            lsn,
            tx_id: name.into(),
            events: vec![Event::new(EventKind::Command, json!({"request":name}))],
        },
        rows,
    )
}
fn row(key: &str, value: serde_json::Value) -> EntityChange {
    EntityChange {
        kind: flow_journal::sqlite::EntityKind::Command,
        key: key.into(),
        value: Some(value),
    }
}

#[tokio::test]
async fn interrupted_empty_initialization_can_resume_but_foreign_schema_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    assert!(Journal::open(
        dir.path(),
        JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                fail_dir_sync: Some(1),
                ..Default::default()
            },
            ..Default::default()
        }
    )
    .await
    .is_err());
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(j.durable_lsn(), 0);
    j.close().await.unwrap();
    let foreign = tempfile::tempdir().unwrap();
    let conn = rusqlite::Connection::open(foreign.path().join("flow.sqlite")).unwrap();
    conn.execute_batch(
        "CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('preserved');",
    )
    .unwrap();
    assert!(Journal::open(foreign.path(), Default::default())
        .await
        .is_err());
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    let value: String = conn
        .query_row("SELECT value FROM unrelated", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    assert_eq!(value, "preserved");
}

#[tokio::test]
async fn state_facts_receipt_and_cursor_roll_back_together_on_constraint_failure() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    let (tx, rows) = prepared(
        &j,
        1,
        "first",
        vec![row("stable", json!({"result":"original"}))],
    );
    j.submit_prepared(tx, rows).await.unwrap();
    let (tx, rows) = prepared(
        &j,
        2,
        "bad",
        vec![
            row("new", json!({"result":"must rollback"})),
            row("stable", json!({"result":"conflict"})),
        ],
    );
    assert!(j.submit_prepared(tx, rows).await.is_err());
    // A rejected transaction is a caller error, not an authority failure: the writer
    // stays usable and the next prepared transition commits normally.
    let (tx, rows) = prepared(
        &j,
        2,
        "after-rejection",
        vec![row("new", json!({"result":"recovered"}))],
    );
    j.submit_prepared(tx, rows).await.unwrap();
    j.close().await.unwrap();
    drop(j);
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(j.durable_lsn(), 2);
    let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
    let facts: i64 = conn
        .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
        .unwrap();
    let current: i64 = conn
        .query_row("SELECT COUNT(*) FROM current_entities", [], |r| r.get(0))
        .unwrap();
    assert_eq!((facts, current), (2, 2));
    let body: String = conn
        .query_row(
            "SELECT body FROM current_entities WHERE key='stable'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["result"],
        "original"
    );
    j.close().await.unwrap();
}

#[tokio::test]
async fn commit_failure_preserves_only_the_confirmed_prefix_and_freezes_writes() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(
        dir.path(),
        JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                fail_data_sync: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    j.submit(
        "ok".into(),
        vec![Event::new(EventKind::Command, json!({}))],
        QueueClass::Control,
    )
    .await
    .unwrap();
    assert!(j
        .submit(
            "fail".into(),
            vec![Event::new(EventKind::Command, json!({}))],
            QueueClass::Control
        )
        .await
        .is_err());
    assert!(j
        .submit(
            "frozen".into(),
            vec![Event::new(EventKind::Command, json!({}))],
            QueueClass::Control
        )
        .await
        .is_err());
    assert!(j.close().await.is_err());
    drop(j);
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(j.durable_lsn(), 1);
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    j.close().await.unwrap();
}

#[tokio::test]
async fn current_objects_are_overwritten_and_domain_facts_cannot_bypass_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    for lsn in 1..=20 {
        let mut change = row("run", json!({"status":lsn}));
        change.kind = flow_journal::sqlite::EntityKind::Run;
        let (tx, rows) = prepared(&j, lsn, &format!("tx-{lsn}"), vec![change]);
        j.submit_prepared(tx, rows).await.unwrap();
    }
    let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM current_entities WHERE kind='run'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        flow_journal::maintenance::verify(dir.path())
            .unwrap()
            .last_lsn,
        20
    );
    assert!(j
        .submit(
            "bypass".into(),
            vec![Event::new(EventKind::Command, json!({}))],
            QueueClass::Control
        )
        .await
        .is_err());
    // Invalid domain input must not freeze the only writer.
    j.close().await.unwrap();
}

#[tokio::test]
#[ignore = "subprocess SIGKILL fixture"]
async fn uncommitted_child() {
    let root = std::path::PathBuf::from(std::env::var("FLOW_SQLITE_CRASH_ROOT").unwrap());
    let j = Journal::open(
        &root,
        JournalOptions {
            faults: flow_journal::storage::FaultInjection {
                sync_delay_ms: 1000,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (tx, rows) = prepared(
        &j,
        1,
        "confirmed",
        vec![row("first", json!({"result":"confirmed"}))],
    );
    j.submit_prepared(tx, rows).await.unwrap();
    std::fs::write(root.join("ready"), b"confirmed").unwrap();
    let (mut tx, rows) = prepared(
        &j,
        2,
        "candidate",
        vec![row("candidate", json!({"result":"uncommitted"}))],
    );
    tx.events[0].payload = json!({"bytes":"x".repeat(512*1024)});
    j.submit_prepared(tx, rows).await.unwrap();
    panic!("parent should kill while the SQLite write transaction is open");
}

#[cfg(unix)]
#[tokio::test]
async fn sigkill_with_open_sqlite_transaction_preserves_the_confirmed_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "uncommitted_child"])
        .env("FLOW_SQLITE_CRASH_ROOT", dir.path())
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !dir.path().join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let conn = rusqlite::Connection::open(dir.path().join("flow.sqlite")).unwrap();
        conn.busy_timeout(std::time::Duration::ZERO).unwrap();
        loop {
            match conn.execute_batch("BEGIN IMMEDIATE") {
                Ok(()) => conn.execute_batch("ROLLBACK").unwrap(),
                Err(rusqlite::Error::SqliteFailure(code, _))
                    if code.code == rusqlite::ErrorCode::DatabaseBusy =>
                {
                    break
                }
                Err(error) => panic!("unexpected write-lock probe: {error}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(j.durable_lsn(), 1);
    let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
    let (facts, entities): (i64, i64) = conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM transactions),(SELECT COUNT(*) FROM current_entities)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((facts, entities), (1, 1));
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    j.close().await.unwrap();
}
