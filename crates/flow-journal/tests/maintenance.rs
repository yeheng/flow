use flow_journal::maintenance::{backup, checkpoint, verify};
use flow_journal::{Event, EventKind, Journal, QueueClass};
use serde_json::json;

#[tokio::test]
async fn consistent_backup_contains_live_wal_and_is_independently_writable() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    let j = Journal::open(&root, Default::default()).await.unwrap();
    j.submit(
        "first".into(),
        vec![Event::new(EventKind::Command, json!({"v":1}))],
        QueueClass::Control,
    )
    .await
    .unwrap();
    let target = dir.path().join("backup");
    let report = backup(&root, &target, j.durable_lsn()).unwrap();
    assert_eq!(report.last_lsn, 1);
    assert_eq!(verify(&target).unwrap().last_lsn, 1);
    j.submit(
        "second".into(),
        vec![Event::new(EventKind::Command, json!({"v":2}))],
        QueueClass::Control,
    )
    .await
    .unwrap();
    assert!(backup(&root, &dir.path().join("old-prefix"), 1).is_err());
    assert!(backup(&root, &target, u64::MAX).is_err());
    checkpoint(&root).unwrap();
    assert!(!root.join("checkpoints").exists());
    assert!(!root.join("journal").exists());
    assert!(!root.join("indexes").exists());
    let restored = Journal::open(&target, Default::default()).await.unwrap();
    assert_eq!(restored.durable_lsn(), 1);
    restored
        .submit(
            "after-restore".into(),
            vec![Event::new(EventKind::Command, json!({}))],
            QueueClass::Control,
        )
        .await
        .unwrap();
    restored.close().await.unwrap();
    j.close().await.unwrap();
}

#[tokio::test]
async fn live_backup_uses_one_snapshot_while_the_writer_advances() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    let j = Journal::open(&root, Default::default()).await.unwrap();
    let writer = j.clone();
    let task = tokio::spawn(async move {
        for i in 0..30 {
            writer
                .submit(
                    format!("tx-{i}"),
                    vec![Event::new(EventKind::Command, json!({"n":i}))],
                    QueueClass::Control,
                )
                .await
                .unwrap();
        }
    });
    let source = root.clone();
    let target = dir.path().join("backup");
    let copied = target.clone();
    let report = tokio::task::spawn_blocking(move || backup(&source, &copied, u64::MAX))
        .await
        .unwrap()
        .unwrap();
    task.await.unwrap();
    assert!(report.last_lsn <= j.durable_lsn());
    assert_eq!(verify(&target).unwrap().last_lsn, report.last_lsn);
    let restored = Journal::open(&target, Default::default()).await.unwrap();
    assert_eq!(restored.durable_lsn(), report.last_lsn);
    restored.close().await.unwrap();
    j.close().await.unwrap();
}
