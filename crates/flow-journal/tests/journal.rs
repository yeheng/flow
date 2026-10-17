use std::time::Duration;

use flow_journal::codec::{decode, digest, encode, read_line};
use flow_journal::storage::FaultInjection;
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
    assert_eq!(lsns, (1..=128).collect::<Vec<_>>());
    assert!(
        journal.stats().data_syncs < 128 / 4,
        "{:?}",
        journal.stats()
    );
    assert!(journal.stats().max_batch_bytes <= 4 * 1024 * 1024);
    journal.close().await.unwrap();
    let report = scan(dir.path(), |tx, _| {
        let structural = tx.events.iter().all(|event| {
            matches!(
                event.kind,
                EventKind::SegmentStarted | EventKind::SegmentSealed
            )
        });
        assert_eq!(tx.events.len(), if structural { 1 } else { 2 });
        Ok(())
    })
    .unwrap();
    assert!(report.fault.is_none());
    assert_eq!(report.last_lsn, 128);
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
        129
    );
    next.close().await.unwrap();
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
