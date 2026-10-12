use flow_journal::value::{read_value, store_stream, ValueCodec};
use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass};
use serde_json::json;
use std::io::{Read, Seek, SeekFrom, Write};

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
    // Damage a different transaction after indexing. An addressed read must not
    // parse it again; the explicit whole-journal verifier must still notice it.
    let path = flow_journal::storage::segment_path(dir.path(), unrelated.location.segment);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(unrelated.location.offset + 20))
        .unwrap();
    let mut original = [0];
    file.read_exact(&mut original).unwrap();
    file.seek(SeekFrom::Start(unrelated.location.offset + 20))
        .unwrap();
    file.write_all(&[original[0] ^ 1]).unwrap();
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
    file.seek(SeekFrom::Start(unrelated.location.offset + 20))
        .unwrap();
    file.write_all(&original).unwrap();
    journal.close().await.unwrap();
}

#[tokio::test]
async fn missing_addresses_rebuild_and_rotation_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let options = JournalOptions {
        segment_bytes: 4096,
        ..Default::default()
    };
    let journal = Journal::open(dir.path(), options.clone()).await.unwrap();
    let bytes = vec![b'a'; 800_000];
    let value = store_stream(&journal, &mut bytes.as_slice(), ValueCodec::Bytes)
        .await
        .unwrap();
    let upper = journal.durable_lsn();
    read_value(dir.path(), upper, &value, true, &mut std::io::sink()).unwrap();
    std::fs::remove_dir_all(dir.path().join("indexes/value-addresses")).unwrap();
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
