use flow_journal::maintenance::{
    backup, load_checkpoint, rebuild_index, repair, verify, write_checkpoint,
};
use flow_journal::storage::segment_path;
use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass};
use serde_json::json;
use std::fs;
use std::io::Write;

#[tokio::test]
async fn caches_are_disposable_and_snapshots_do_not_copy_future_or_ambiguous_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("source");
    let journal = Journal::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let c = journal
        .submit(
            "t".into(),
            vec![Event::new(EventKind::Command, json!({"x":1}))],
            QueueClass::Control,
        )
        .await
        .unwrap();
    let upper = c.transaction.lsn;
    rebuild_index(&root, upper).unwrap();
    write_checkpoint(&root, upper, 1, json!({"runs":[]})).unwrap();
    assert!(load_checkpoint(&root, 1).unwrap().is_some());
    assert!(load_checkpoint(&root, 2).unwrap().is_none());
    fs::write(root.join("checkpoints/latest.json"), b"broken").unwrap();
    assert!(load_checkpoint(&root, 1).unwrap().is_none());
    fs::remove_dir_all(root.join("indexes")).unwrap();
    rebuild_index(&root, upper).unwrap();
    journal
        .submit(
            "later".into(),
            vec![Event::new(EventKind::Command, json!({"x":2}))],
            QueueClass::Control,
        )
        .await
        .unwrap();
    let target = temp.path().join("backup");
    assert_eq!(backup(&root, &target, upper).unwrap().last_lsn, upper);
    assert_eq!(verify(&target).unwrap().last_lsn, upper);
    assert!(repair(&root, &temp.path().join("repair-live"), true).is_err());
    journal.close().await.unwrap();
    let tail = segment_path(&root, 1);
    fs::OpenOptions::new()
        .append(true)
        .open(&tail)
        .unwrap()
        .write_all(b"half")
        .unwrap();
    let evidence = fs::read(&tail).unwrap();
    let destination = temp.path().join("repair");
    assert!(repair(&root, &destination, false).is_err());
    assert!(!destination.exists());
    let report = repair(&root, &destination, true).unwrap();
    assert_eq!(report.last_lsn, upper + 1);
    assert_eq!(fs::read(&tail).unwrap(), evidence);
    let restored = Journal::open(&destination, JournalOptions::default())
        .await
        .unwrap();
    restored.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_and_backup_manifest_cannot_hide_ahead_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("source");
    let journal = Journal::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let commit = journal
        .submit(
            "t".into(),
            vec![Event::new(EventKind::Command, json!({}))],
            QueueClass::Control,
        )
        .await
        .unwrap();
    write_checkpoint(&root, commit.transaction.lsn, 1, json!({})).unwrap();
    journal.close().await.unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(segment_path(&root, 1))
        .unwrap()
        .set_len(commit.location.offset)
        .unwrap();
    assert!(load_checkpoint(&root, 1).unwrap().is_none());
    fs::write(root.join("backup-manifest.json"),serde_json::to_vec(&json!({"version":1,"journal_id":journal.id(),"upper_lsn":"2","repair":false,"source_fault":null})).unwrap()).unwrap();
    assert!(Journal::open(&root, JournalOptions::default())
        .await
        .is_err());
}

#[tokio::test]
async fn fast_inventory_uses_checkpoint_but_missing_sealed_segments_still_fail() {
    let temp = tempfile::tempdir().unwrap();
    let journal = Journal::open(
        temp.path(),
        JournalOptions {
            segment_bytes: 4096,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for i in 0..10 {
        journal
            .submit(
                i.to_string(),
                vec![Event::new(EventKind::Command, json!("x".repeat(1200)))],
                QueueClass::Control,
            )
            .await
            .unwrap();
    }
    write_checkpoint(temp.path(), journal.durable_lsn(), 1, json!({})).unwrap();
    let full = verify(temp.path()).unwrap();
    let fast = flow_journal::storage::scan_fast(temp.path()).unwrap();
    assert_eq!(fast.last_lsn, full.last_lsn);
    assert!(fast.fault.is_none());
    journal.close().await.unwrap();
    fs::remove_file(segment_path(temp.path(), 2)).unwrap();
    assert!(load_checkpoint(temp.path(), 1).unwrap().is_none());
    assert!(flow_journal::storage::scan_fast(temp.path())
        .unwrap()
        .fault
        .is_some());
    assert!(Journal::open(temp.path(), JournalOptions::default())
        .await
        .is_err());
}

#[tokio::test]
async fn live_rotation_backup_is_a_fixed_prefix_and_restore_is_writable() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("source");
    let journal = Journal::open(
        &root,
        JournalOptions {
            segment_bytes: 4096,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for i in 0..12 {
        journal
            .submit(
                i.to_string(),
                vec![Event::new(
                    EventKind::Command,
                    json!({"n":i,"pad":"x".repeat(2000)}),
                )],
                QueueClass::Control,
            )
            .await
            .unwrap();
    }
    let upper = journal.durable_lsn();
    let j = journal.clone();
    let writes = tokio::spawn(async move {
        for i in 12..36 {
            j.submit(
                i.to_string(),
                vec![Event::new(
                    EventKind::Command,
                    json!({"n":i,"pad":"x".repeat(2000)}),
                )],
                QueueClass::Control,
            )
            .await
            .unwrap();
        }
    });
    let target = temp.path().join("backup");
    let src = root.clone();
    let dest = target.clone();
    let copied = tokio::task::spawn_blocking(move || backup(&src, &dest, upper))
        .await
        .unwrap()
        .unwrap();
    writes.await.unwrap();
    assert_eq!(copied.last_lsn, upper);
    journal.close().await.unwrap();
    drop(journal);
    assert!(!target.join("projection.sqlite").exists());
    let restored = Journal::open(&target, Default::default()).await.unwrap();
    assert_eq!(restored.durable_lsn(), upper);
    restored
        .submit(
            "after-restore".into(),
            vec![Event::new(EventKind::Command, json!({"restored":true}))],
            QueueClass::Control,
        )
        .await
        .unwrap();
    restored.close().await.unwrap();
    assert!(verify(&target).unwrap().fault.is_none());
    assert!(backup(&root, &target, upper).is_err()); // no overwrite of the restored dataset
}
