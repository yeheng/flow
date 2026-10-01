use std::fs::{self, OpenOptions};
use std::io::Write;
use std::time::Duration;

use flow_journal::codec::{decode, digest, encode, read_line};
use flow_journal::storage::{segment_path, FaultInjection};
use flow_journal::value::{materialize, read_value, store_stream, ValueCodec};
use flow_journal::{
    scan, Event, EventKind, Journal, JournalOptions, QueueClass, StoredValue, Transaction,
    MAX_LINE_BYTES,
};
use serde_json::json;

fn tx() -> Transaction {
    Transaction {
        v: 2,
        journal_id: "j1".into(),
        lsn: 1,
        tx_id: "t1".into(),
        events: vec![event(), event()],
    }
}
fn event() -> Event {
    Event::new(
        EventKind::Command,
        json!({"my_auth_token":"business", "nested":{"a":1}}),
    )
}
fn raw(data: &str) -> Vec<u8> {
    format!(
        "{{\"data\":{data},\"sha256\":\"{}\"}}\n",
        digest(data.as_bytes())
    )
    .into_bytes()
}

#[test]
fn codec_raw_bytes_versions_keys_and_transaction_boundary() {
    let line = encode(&tx()).unwrap();
    assert_eq!(decode(&line).unwrap(), tx());
    let data = serde_json::to_string_pretty(&tx())
        .unwrap()
        .replace('\n', " ");
    assert_eq!(decode(&raw(&data)).unwrap(), tx()); // hashing reserialization would reject this
    assert!(decode(&line[..line.len() - 1]).is_err());
    let mut corrupt = line.clone();
    corrupt[20] ^= 1;
    assert!(decode(&corrupt).is_err());
    let data = serde_json::to_string(&tx()).unwrap();
    for changed in [
        data.replace("\"v\":2", "\"v\":3"),
        data.replace("\"v\":1", "\"v\":99"),
        data.replace("\"a\":1", "\"a\":1,\"a\":2"),
        data.replace("\"lsn\":\"1\"", "\"lsn\":\"01\""),
        data.replace("\"command\"", "\"future_required_event\""),
    ] {
        assert!(decode(&raw(&changed)).is_err(), "{changed}");
    }
    let duplicate = format!(
        "{{\"data\":{data},\"data\":{data},\"sha256\":\"{}\"}}\n",
        digest(data.as_bytes())
    );
    assert!(decode(duplicate.as_bytes()).is_err());
    let mut huge = tx();
    huge.events[1].payload = json!("x".repeat(MAX_LINE_BYTES));
    assert!(encode(&huge).is_err());
    assert!(read_line(&mut std::io::Cursor::new(vec![b'x'; MAX_LINE_BYTES + 1])).is_err());
    assert!(read_line(&mut std::io::Cursor::new(&line[..line.len() - 1])).is_err());
    let mut reader = std::io::Cursor::new(line.repeat(1000));
    for _ in 0..1000 {
        assert_eq!(
            decode(&read_line(&mut reader).unwrap().unwrap())
                .unwrap()
                .events
                .len(),
            2
        );
    }
    assert!(read_line(&mut reader).unwrap().is_none());
}

#[tokio::test]
async fn batch_durability_round_robin_lock_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    assert!(Journal::open(dir.path(), JournalOptions::default())
        .await
        .is_err());
    let mut tasks = Vec::new();
    for i in 0..128 {
        let journal = journal.clone();
        tasks.push(tokio::spawn(async move {
            journal
                .submit(
                    format!("t{i}"),
                    vec![event(), event()],
                    if i % 5 == 0 {
                        QueueClass::Control
                    } else {
                        QueueClass::Audit(format!("d{}", i % 7))
                    },
                )
                .await
                .unwrap()
        }));
    }
    let mut lsns = Vec::new();
    for task in tasks {
        let c = task.await.unwrap();
        lsns.push(c.transaction.lsn);
        assert!(journal.durable_lsn() >= c.transaction.lsn);
    }
    lsns.sort_unstable();
    assert_eq!(lsns, (2..=129).collect::<Vec<_>>());
    assert!(
        journal.stats().data_syncs < 128 / 4,
        "{:?}",
        journal.stats()
    );
    assert!(journal.stats().max_batch_bytes <= 4 * 1024 * 1024);
    journal.close().await.unwrap();
    let report = scan(dir.path(), |tx, _| {
        assert!(tx.events.len() == 1 || tx.events.len() == 2);
        Ok(())
    })
    .unwrap();
    assert!(report.fault.is_none());
    assert_eq!(report.last_lsn, 129);
    let next = Journal::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(next.id(), journal.id());
    assert_eq!(
        next.submit("after".into(), vec![event()], QueueClass::Control)
            .await
            .unwrap()
            .transaction
            .lsn,
        130
    );
    next.close().await.unwrap();
}

#[tokio::test]
async fn rotation_chain_missing_segment_and_ambiguous_tail_are_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let options = JournalOptions {
        segment_bytes: 4096,
        group_wait: Duration::ZERO,
        ..Default::default()
    };
    let journal = Journal::open(dir.path(), options.clone()).await.unwrap();
    for i in 0..15 {
        journal
            .submit(
                i.to_string(),
                vec![Event::new(EventKind::Command, json!("x".repeat(1200)))],
                QueueClass::Control,
            )
            .await
            .unwrap();
    }
    journal.close().await.unwrap();
    let report = scan(dir.path(), |_, _| Ok(())).unwrap();
    assert!(report.fault.is_none(), "{report:?}");
    assert!(report.segments.len() > 2);
    let tail = segment_path(dir.path(), report.segments.last().unwrap().number);
    OpenOptions::new()
        .append(true)
        .open(&tail)
        .unwrap()
        .write_all(b"{\"data\":")
        .unwrap();
    let before = fs::read(&tail).unwrap();
    let broken = scan(dir.path(), |_, _| Ok(())).unwrap();
    assert!(broken.fault.is_some());
    assert!(Journal::open(dir.path(), options.clone()).await.is_err());
    assert_eq!(fs::read(&tail).unwrap(), before);
    fs::remove_file(segment_path(dir.path(), 2)).unwrap();
    let broken = scan(dir.path(), |_, _| Ok(())).unwrap();
    assert!(broken.fault.unwrap().reason.contains("missing segment"));
}

#[tokio::test]
async fn injected_sync_failure_freezes_and_complete_unknown_receipt_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(
        dir.path(),
        JournalOptions {
            faults: FaultInjection {
                fail_data_sync: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(journal
        .submit("unknown-receipt".into(), vec![event()], QueueClass::Control)
        .await
        .is_err());
    assert_eq!(journal.durable_lsn(), 1);
    assert!(journal
        .submit("must-not-append".into(), vec![event()], QueueClass::Control)
        .await
        .is_err());
    assert!(Journal::open(dir.path(), JournalOptions::default())
        .await
        .is_err());
    assert!(journal.close().await.is_err());
    let mut ids = Vec::new();
    let report = scan(dir.path(), |tx, _| {
        ids.push(tx.tx_id.clone());
        Ok(())
    })
    .unwrap();
    assert!(report.fault.is_none());
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[1], "unknown-receipt");
    let recovered = Journal::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(recovered.durable_lsn(), 2);
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn short_writes_complete_but_partial_write_never_confirms_or_repairs() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(
        dir.path(),
        JournalOptions {
            faults: FaultInjection {
                short_write_bytes: Some(3),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    journal
        .submit("short".into(), vec![event()], QueueClass::Control)
        .await
        .unwrap();
    journal.close().await.unwrap();
    let journal = Journal::open(
        dir.path(),
        JournalOptions {
            faults: FaultInjection {
                fail_after_bytes: Some(19),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(journal
        .submit("partial".into(), vec![event()], QueueClass::Control)
        .await
        .is_err());
    assert_eq!(journal.durable_lsn(), 2);
    assert!(journal.close().await.is_err());
    let report = scan(dir.path(), |_, _| Ok(())).unwrap();
    assert_eq!(report.last_lsn, 2);
    assert!(report.fault.is_some());
    assert!(Journal::open(dir.path(), JournalOptions::default())
        .await
        .is_err());
}

#[tokio::test]
async fn directory_sync_failure_does_not_acknowledge_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(
        dir.path(),
        JournalOptions {
            segment_bytes: 4096,
            faults: FaultInjection {
                fail_dir_sync: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    journal
        .submit(
            "first".into(),
            vec![Event::new(EventKind::Command, json!("x".repeat(3000)))],
            QueueClass::Control,
        )
        .await
        .unwrap();
    assert!(journal
        .submit(
            "rotation".into(),
            vec![Event::new(EventKind::Command, json!("x".repeat(3000)))],
            QueueClass::Control
        )
        .await
        .is_err());
    assert_eq!(journal.durable_lsn(), 2);
    assert!(journal.close().await.is_err());
}

#[tokio::test]
async fn values_stream_and_rebuild_without_truncation_or_key_redaction() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    let source =
        serde_json::to_vec(&json!({"my_auth_token":"x".repeat(800_000),"password":"business"}))
            .unwrap();
    let value = store_stream(&journal, &mut source.as_slice(), ValueCodec::Json)
        .await
        .unwrap();
    assert!(value.chunk_count > 1);
    let mut output = Vec::new();
    read_value(dir.path(), journal.durable_lsn(), &value, true, &mut output).unwrap();
    assert_eq!(output, source);
    assert!(materialize(
        dir.path(),
        journal.durable_lsn(),
        &StoredValue::Ref(value.clone()),
        100
    )
    .is_err());
    let mut wrong = value.clone();
    wrong.digest = "0".repeat(64);
    assert!(read_value(
        dir.path(),
        journal.durable_lsn(),
        &wrong,
        true,
        &mut std::io::sink()
    )
    .is_err());
    wrong = value.clone();
    wrong.journal_id = "other".into();
    assert!(read_value(
        dir.path(),
        journal.durable_lsn(),
        &wrong,
        true,
        &mut std::io::sink()
    )
    .is_err());
    journal.close().await.unwrap();
    let recovered = Journal::open(dir.path(), JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(
        materialize(
            dir.path(),
            recovered.durable_lsn(),
            &StoredValue::Ref(value),
            1_000_000
        )
        .unwrap()["password"],
        "business"
    );
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn slow_disk_and_full_queues_keep_both_classes_progressing() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(
        dir.path(),
        JournalOptions {
            batch_bytes: MAX_LINE_BYTES,
            control_bytes: MAX_LINE_BYTES,
            audit_bytes: MAX_LINE_BYTES,
            queue_requests: 2,
            faults: FaultInjection {
                sync_delay_ms: 10,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for i in 0..32 {
        let j = journal.clone();
        tasks.push(tokio::spawn(async move {
            j.submit(
                format!("{i}"),
                vec![Event::new(EventKind::Command, json!("x".repeat(32 * 1024)))],
                if i % 2 == 0 {
                    QueueClass::Control
                } else {
                    QueueClass::Audit(format!("d{}", i % 3))
                },
            )
            .await
            .unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .unwrap();
    let stats = journal.stats();
    assert_eq!(stats.transactions, 32);
    assert!(stats.peak_queued_requests <= 4);
    assert!(stats.peak_queued_bytes <= 2 * MAX_LINE_BYTES);
    journal.close().await.unwrap();
    assert!(scan(dir.path(), |_, _| Ok(())).unwrap().fault.is_none());
}

#[tokio::test]
async fn sync_failure_matrix_never_loses_acknowledged_prefix() {
    for (data, dir) in (1..=10)
        .map(|n| (Some(n), None))
        .chain((1..=4).map(|n| (None, Some(n))))
    {
        let temp = tempfile::tempdir().unwrap();
        let opened = Journal::open(
            temp.path(),
            JournalOptions {
                segment_bytes: 4096,
                faults: FaultInjection {
                    fail_data_sync: data,
                    fail_dir_sync: dir,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        let Ok(j) = opened else { continue };
        let mut acknowledged = Vec::new();
        let mut failed = false;
        for i in 0..12 {
            match j
                .submit(
                    format!("matrix-{i}"),
                    vec![Event::new(
                        EventKind::Command,
                        json!({"n":i,"pad":"x".repeat(2500)}),
                    )],
                    QueueClass::Control,
                )
                .await
            {
                Ok(receipt) => acknowledged.push(receipt.transaction.tx_id.clone()),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            let before = j.durable_lsn();
            assert!(j
                .submit("after-failure".into(), vec![event()], QueueClass::Control)
                .await
                .is_err());
            assert_eq!(j.durable_lsn(), before);
        }
        let _ = j.close().await;
        drop(j);
        let mut recovered = Vec::new();
        flow_journal::scan(temp.path(), |tx, _| {
            recovered.push(tx.tx_id.clone());
            Ok(())
        })
        .unwrap();
        for id in acknowledged {
            assert!(
                recovered.contains(&id),
                "lost ack data={data:?} dir={dir:?}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated 32 MiB FLOW_ENOSPC_VOLUME disk image"]
async fn real_enospc_freezes_writer_and_preserves_acknowledged_prefix() {
    use std::io::Write;
    let volume = std::path::PathBuf::from(
        std::env::var("FLOW_ENOSPC_VOLUME").expect("isolated volume required"),
    );
    assert_eq!(volume, std::path::Path::new("/tmp/flow-acceptance-volume"));
    let root = volume.join(format!("journal-{}", uuid::Uuid::now_v7()));
    let j = Journal::open(&root, Default::default()).await.unwrap();
    let ack = j
        .submit("before-full".into(), vec![event()], QueueClass::Control)
        .await
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            std::fs::metadata(&volume).unwrap().dev(),
            std::fs::metadata(volume.parent().unwrap()).unwrap().dev(),
            "requires an isolated mounted filesystem"
        );
    }
    let fill = volume.join("fill.bin");
    let mut file = std::fs::File::create(&fill).unwrap();
    let block = vec![0xab; 1024 * 1024];
    let mut filled = 0;
    loop {
        assert!(
            filled < 40 * 1024 * 1024,
            "refuse to fill an oversized filesystem"
        );
        match file.write_all(&block) {
            Ok(()) => filled += block.len(),
            Err(e) => {
                assert_eq!(e.raw_os_error(), Some(28));
                break;
            }
        }
    }
    assert!(filled < 40 * 1024 * 1024, "unexpected backing filesystem");
    let result = j
        .submit(
            "disk-full".into(),
            vec![Event::new(
                EventKind::Command,
                json!({"data":"x".repeat(700_000)}),
            )],
            QueueClass::Control,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(j.durable_lsn(), ack.transaction.lsn);
    assert!(j
        .submit("after-full".into(), vec![event()], QueueClass::Control)
        .await
        .is_err());
    let _ = j.close().await;
    drop(j);
    drop(file);
    std::fs::remove_file(fill).unwrap();
    let mut ids = Vec::new();
    let report = flow_journal::scan(&root, |tx, _| {
        ids.push(tx.tx_id.clone());
        Ok(())
    })
    .unwrap();
    assert!(ids.contains(&"before-full".to_string()));
    let destination = volume.join("repaired");
    if report.fault.is_some() {
        flow_journal::maintenance::repair(&root, &destination, true).unwrap();
    }
    println!(
        "real ENOSPC after {filled} bytes; acknowledged prefix retained; ambiguous_tail={}",
        report.fault.is_some()
    );
}
