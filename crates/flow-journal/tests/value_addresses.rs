use flow_journal::value::{read_value, store_stream, ValueCodec};
use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass};
use serde_json::json;

#[tokio::test]
async fn addressed_reads_skip_unrelated_history_and_validate_own_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    let unrelated = journal
        .submit(
            "unrelated".into(),
            vec![Event::new(
                EventKind::Command,
                json!({"padding":"old history"}),
            )],
            QueueClass::Control,
        )
        .await
        .unwrap();
    let bytes = vec![b'x'; 900_000];
    let value = store_stream(&journal, &mut bytes.as_slice(), ValueCodec::Bytes)
        .await
        .unwrap();
    let upper = journal.durable_lsn();
    let mut out = Vec::new();
    read_value(dir.path(), upper, &value, true, &mut out).unwrap();
    assert_eq!(out, bytes);
    // A value read uses its BLOB index, not a scan of unrelated accepted facts.
    let conn = rusqlite::Connection::open(dir.path().join("flow.sqlite")).unwrap();
    let original: String = conn
        .query_row(
            "SELECT body FROM events WHERE lsn=?",
            [format!("{:020}", unrelated.transaction.lsn)],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE events SET body='{}' WHERE lsn=?",
        [format!("{:020}", unrelated.transaction.lsn)],
    )
    .unwrap();
    let mut addressed = Vec::new();
    read_value(dir.path(), upper, &value, true, &mut addressed).unwrap();
    assert_eq!(addressed, bytes);
    assert!(flow_journal::scan(dir.path(), |_, _| Ok(()))
        .unwrap()
        .fault
        .is_some());
    let mut wrong = value.clone();
    wrong.digest = "0".repeat(64);
    assert!(read_value(dir.path(), upper, &wrong, true, &mut std::io::sink()).is_err());
    assert!(read_value(dir.path(), upper - 1, &value, true, &mut std::io::sink()).is_err());
    conn.execute(
        "UPDATE events SET body=? WHERE lsn=?",
        rusqlite::params![original, format!("{:020}", unrelated.transaction.lsn)],
    )
    .unwrap();
    journal.close().await.unwrap();
}

#[tokio::test]
async fn blob_reads_need_no_external_address_cache_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let options = JournalOptions::default();
    let journal = Journal::open(dir.path(), options.clone()).await.unwrap();
    let bytes = vec![b'a'; 800_000];
    let value = store_stream(&journal, &mut bytes.as_slice(), ValueCodec::Bytes)
        .await
        .unwrap();
    let upper = journal.durable_lsn();
    read_value(dir.path(), upper, &value, true, &mut std::io::sink()).unwrap();
    assert!(!dir.path().join("indexes").exists());
    let mut out = Vec::new();
    read_value(dir.path(), upper, &value, true, &mut out).unwrap();
    assert_eq!(out, bytes);
    let mut foreign = value.clone();
    foreign.journal_id = "another".into();
    assert!(read_value(dir.path(), upper, &foreign, true, &mut std::io::sink()).is_err());
    journal.close().await.unwrap();
    drop(journal);
    let journal = Journal::open(dir.path(), options).await.unwrap();
    out.clear();
    read_value(dir.path(), journal.durable_lsn(), &value, true, &mut out).unwrap();
    assert_eq!(out, bytes);
    journal.close().await.unwrap();
}
