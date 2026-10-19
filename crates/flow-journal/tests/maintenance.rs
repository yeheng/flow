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

/// 失败的备份不留下任何残骸：staging 与新建的目标目录都清掉，
/// 目标要么是验证过的成品、要么什么都不是。
#[tokio::test]
async fn failed_backup_cleans_destination_and_publishes_nothing() {
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
    j.close().await.unwrap();
    // 确定性校验失败：删掉事实行但保留提交水位（store_meta.committed_lsn）。
    // 外键在连接级显式关闭（不同 SQLite 构建的默认值不一），删除不受
    // events/current_entities 引用阻碍；拷贝副本在 verify 阶段必然报错
    // （foreign_key_check 或 fact count 与水位不一致）。
    let raw = rusqlite::Connection::open(root.join(flow_journal::sqlite::DATABASE)).unwrap();
    raw.execute_batch("PRAGMA foreign_keys=OFF; DELETE FROM transactions")
        .unwrap();
    drop(raw);

    let target = dir.path().join("backup");
    assert!(backup(&root, &target, u64::MAX).is_err());
    assert!(!target.exists(), "失败后目标目录不应留存");
}

/// 目标占用判定：自家 staging 残骸（上次失败/崩溃留下）可回收，同路径
/// 直接重试成功；外来内容与已发布成品拒绝且现场不动。
#[tokio::test]
async fn backup_reclaims_stale_leftovers_and_rejects_foreign_content() {
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

    // 残骸回收：仅自家 .backup-* → 清理后备份成功
    let target = dir.path().join("backup");
    let stale = target.join(".backup-stale");
    std::fs::create_dir_all(stale.join("inner")).unwrap();
    std::fs::write(stale.join("inner").join(flow_journal::sqlite::DATABASE), b"half copy").unwrap();
    let report = backup(&root, &target, u64::MAX).unwrap();
    assert!(target.join(flow_journal::sqlite::DATABASE).exists());
    assert!(!stale.exists(), "自家残骸应被回收");
    assert_eq!(verify(&target).unwrap().last_lsn, report.last_lsn);

    // 外来内容：拒绝，且自家残骸也原样保留（不动现场）
    let occupied = dir.path().join("occupied");
    std::fs::create_dir_all(occupied.join(".backup-stale")).unwrap();
    std::fs::write(occupied.join("notes.txt"), b"operator data").unwrap();
    assert!(backup(&root, &occupied, u64::MAX).is_err());
    assert!(occupied.join("notes.txt").exists());
    assert!(occupied.join(".backup-stale").exists(), "拒绝时不动现场");

    // 已发布成品：拒绝（既有语义不变）
    assert!(backup(&root, &target, u64::MAX).is_err());

    j.close().await.unwrap();
}
