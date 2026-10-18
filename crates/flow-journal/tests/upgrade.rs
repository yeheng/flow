use flow_journal::{Event, EventKind, Journal};

#[tokio::test]
async fn upgrade_copies_legacy_format_without_modifying_source_or_overwriting_targets() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("old");
    let target = parent.path().join("new");
    let journal = Journal::open(&root, Default::default()).await.unwrap();
    journal
        .submit(
            "fact".into(),
            vec![Event::new(
                EventKind::Command,
                serde_json::json!({"original":true}),
            )],
            flow_journal::QueueClass::Control,
        )
        .await
        .unwrap();
    journal.close().await.unwrap();
    drop(journal);
    let path = root.join(flow_journal::sqlite::DATABASE);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA user_version=3; PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);
    assert!(flow_journal::sqlite::read_connection(&root).is_err());
    let _lock = flow_journal::maintenance::offline_lock(&root).unwrap();
    let report = flow_journal::maintenance::upgrade(&root, &target).unwrap();
    assert_eq!(report.last_lsn, 1);
    assert!(flow_journal::maintenance::upgrade(&root, &target).is_err());
    let source = rusqlite::Connection::open(&path).unwrap();
    let version: i64 = source
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 3);
    let copy = flow_journal::sqlite::read_connection(&target).unwrap();
    let tx = flow_journal::sqlite::transaction(&copy, &report.journal_id, 1).unwrap();
    assert_eq!(tx.events[0].payload["original"], true);
}
