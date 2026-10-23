use flow_journal::sqlite::{EntityChange, EntityKind};
use flow_journal::{Event, EventKind, Journal, JournalOptions, Transaction};
use serde_json::{json, Value};

fn transaction(j: &Journal, lsn: u64, id: &str, events: Vec<Event>) -> Transaction {
    Transaction {
        v: 2,
        journal_id: j.id().into(),
        lsn,
        tx_id: id.into(),
        events,
    }
}
fn receipt(key: &str, value: Value) -> EntityChange {
    EntityChange {
        kind: EntityKind::Command,
        key: key.into(),
        value: Some(value),
    }
}
fn counts(root: &std::path::Path) -> (i64, i64, i64) {
    let conn = flow_journal::sqlite::read_connection(root).unwrap();
    conn.query_row("SELECT (SELECT COUNT(*) FROM transactions),(SELECT COUNT(*) FROM value_chunks),(SELECT COUNT(*) FROM current_entities WHERE kind='published')", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
}

#[tokio::test]
async fn seven_mib_value_and_receipt_share_one_commit_with_bounded_queue() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    let value = json!({"large":"x".repeat(7*1024*1024)});
    let prepared = j
        .prepare_json(value.clone(), flow_journal::VALUE_BUDGET)
        .await
        .unwrap();
    let stored = prepared.stored().clone();
    let staged = prepared.into_staged().unwrap();
    let chunks = staged.chunk_count();
    assert_eq!(
        counts(dir.path()),
        (0, 0, 0),
        "staging does not publish any facts"
    );
    let tx = transaction(
        &j,
        chunks + 1,
        "large",
        vec![
            staged.publication().unwrap(),
            Event::new(EventKind::Command, json!({"result":stored})),
        ],
    );
    let commit = j
        .submit_prepared_values(
            tx,
            vec![receipt("r", json!({"result":stored}))],
            vec![staged],
        )
        .await
        .unwrap();
    assert_eq!(commit.value_prefix.len() as u64, chunks);
    let stats = j.stats();
    assert_eq!(stats.data_syncs, 1, "chunks and command have one COMMIT");
    assert_eq!(stats.transactions, chunks + 1);
    assert!(stats.max_batch_bytes <= 4 * 1024 * 1024);
    assert!(stats.peak_queued_bytes <= 4 * 1024 * 1024);
    assert_eq!(counts(dir.path()), ((chunks + 1) as i64, chunks as i64, 1));
    assert_eq!(
        flow_journal::value::materialize(
            dir.path(),
            j.durable_lsn(),
            &stored,
            flow_journal::VALUE_BUDGET
        )
        .unwrap(),
        value
    );
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    j.close().await.unwrap();
}

#[tokio::test]
async fn final_receipt_conflict_rolls_back_every_chunk_and_writer_remains_usable() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    j.submit_prepared(
        transaction(
            &j,
            1,
            "original",
            vec![Event::new(EventKind::Command, json!({}))],
        ),
        vec![receipt("stable", json!("original"))],
    )
    .await
    .unwrap();
    let prepared = j
        .prepare_json(json!("x".repeat(700_000)), flow_journal::VALUE_BUDGET)
        .await
        .unwrap();
    let staged = prepared.into_staged().unwrap();
    let tx = transaction(
        &j,
        staged.chunk_count() + 2,
        "conflict",
        vec![
            staged.publication().unwrap(),
            Event::new(EventKind::Command, json!({})),
        ],
    );
    assert!(j
        .submit_prepared_values(tx, vec![receipt("stable", json!("conflict"))], vec![staged])
        .await
        .is_err());
    assert_eq!(counts(dir.path()), (1, 0, 0));
    assert_eq!(j.durable_lsn(), 1);
    assert!(j.stats().failure.is_none());
    j.submit_prepared(
        transaction(
            &j,
            2,
            "after",
            vec![Event::new(EventKind::Command, json!({}))],
        ),
        vec![],
    )
    .await
    .unwrap();
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    j.close().await.unwrap();
}

#[tokio::test]
async fn failed_commit_keeps_no_staged_value_or_business_fact_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(
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
    let prepared = j
        .prepare_json(json!("x".repeat(700_000)), flow_journal::VALUE_BUDGET)
        .await
        .unwrap();
    let staged = prepared.into_staged().unwrap();
    let tx = transaction(
        &j,
        staged.chunk_count() + 1,
        "failed",
        vec![
            staged.publication().unwrap(),
            Event::new(EventKind::Command, json!({})),
        ],
    );
    assert!(j
        .submit_prepared_values(tx, vec![receipt("r", json!({}))], vec![staged])
        .await
        .is_err());
    assert_eq!(counts(dir.path()), (0, 0, 0));
    let _ = j.close().await;
    drop(j);
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(j.durable_lsn(), 0);
    assert_eq!(counts(dir.path()), (0, 0, 0));
    j.close().await.unwrap();
}

#[tokio::test]
async fn staged_values_require_a_publication_in_the_final_transition() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    let prepared = j
        .prepare_json(json!("x".repeat(700_000)), flow_journal::VALUE_BUDGET)
        .await
        .unwrap();
    let staged = prepared.into_staged().unwrap();
    let tx = transaction(
        &j,
        staged.chunk_count() + 1,
        "missing-publication",
        vec![Event::new(EventKind::Command, json!({}))],
    );
    assert!(j
        .submit_prepared_values(tx, vec![], vec![staged])
        .await
        .is_err());
    assert_eq!(counts(dir.path()), (0, 0, 0));
    assert!(j.stats().failure.is_none());
    j.close().await.unwrap();
}

#[cfg(unix)]
#[test]
fn crash_fixture() {
    let Some(root) = std::env::var_os("FLOW_ATOMIC_VALUE_CRASH_ROOT") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let j = Journal::open(
                std::path::PathBuf::from(root),
                JournalOptions {
                    faults: flow_journal::storage::FaultInjection {
                        sync_delay_ms: 30_000,
                        before_data_sync: Some(|lsn| {
                            let root = std::path::PathBuf::from(
                                std::env::var_os("FLOW_ATOMIC_VALUE_CRASH_ROOT").unwrap(),
                            );
                            std::fs::write(root.join("before-commit.tmp"), lsn.to_string())
                                .unwrap();
                            std::fs::rename(
                                root.join("before-commit.tmp"),
                                root.join("before-commit"),
                            )
                            .unwrap();
                        }),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // Stop only after every chunk and the final receipt were appended.
            // The barrier is independent of SQLite's page-cache spill policy.
            let mut values = Vec::new();
            let mut events = Vec::new();
            for _ in 0..2 {
                let prepared = j
                    .prepare_json(
                        json!("x".repeat(7 * 1024 * 1024)),
                        flow_journal::VALUE_BUDGET,
                    )
                    .await
                    .unwrap();
                let staged = prepared.into_staged().unwrap();
                events.push(staged.publication().unwrap());
                values.push(staged);
            }
            events.push(Event::new(EventKind::Command, json!({})));
            let chunks: u64 = values.iter().map(|v| v.chunk_count()).sum();
            let tx = transaction(&j, chunks + 1, "crash", events);
            j.submit_prepared_values(tx, vec![receipt("r", json!({}))], values)
                .await
                .unwrap();
            panic!("fixture must be killed before COMMIT");
        });
}

#[cfg(unix)]
#[tokio::test]
async fn sigkill_before_atomic_commit_recovers_no_partial_command() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_fixture", "--nocapture"])
        .env("FLOW_ATOMIC_VALUE_CRASH_ROOT", dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Ok(lsn) = std::fs::read_to_string(dir.path().join("before-commit")) {
            assert!(
                lsn.parse::<u64>().unwrap() > 2,
                "barrier must follow all chunks and the command"
            );
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "fixture exited before the COMMIT barrier"
        );
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fixture never reached the COMMIT barrier");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let visible = counts(dir.path());
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(visible, (0, 0, 0), "uncommitted prefix must be invisible");
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    assert_eq!(counts(dir.path()), (0, 0, 0));
    assert_eq!(j.durable_lsn(), 0);
    assert!(flow_journal::maintenance::verify(dir.path())
        .unwrap()
        .fault
        .is_none());
    j.close().await.unwrap();
}

#[tokio::test]
async fn dropping_uncommitted_staging_releases_the_temporary_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    let first = j
        .prepare_json(json!("x".repeat(70_000)), flow_journal::VALUE_BUDGET)
        .await
        .unwrap();
    let second = j
        .prepare_json(json!("x".repeat(70_000)), flow_journal::VALUE_BUDGET)
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(30),
        j.prepare_json(json!("x".repeat(70_000)), flow_journal::VALUE_BUDGET)
    )
    .await
    .is_err());
    drop(first);
    let third = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        j.prepare_json(json!("x".repeat(70_000)), flow_journal::VALUE_BUDGET),
    )
    .await
    .unwrap()
    .unwrap();
    drop(second);
    drop(third);
    assert_eq!(counts(dir.path()), (0, 0, 0));
    j.close().await.unwrap();
}
