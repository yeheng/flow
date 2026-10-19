use flow_backend::journal::JournalBackend;
use flow_journal::{Event, EventKind, StoredValue};
use serde_json::{json, Value};
fn inline(value: Value) -> StoredValue {
    StoredValue::Inline(value)
}
fn event(
    kind: EventKind,
    id: &str,
    seq: u64,
    dispatch: Option<&str>,
    audit: u64,
    payload: Value,
) -> Event {
    let mut e = Event::new(kind, payload);
    e.run_id = Some(id.into());
    e.run_seq = seq;
    e.dispatch_id = dispatch.map(str::to_owned);
    e.node_id = dispatch.map(|_| "s".into());
    e.audit_seq = audit;
    e
}
fn start(id: &str) -> Event {
    event(
        EventKind::RunStarted,
        id,
        1,
        None,
        0,
        json!({"run_id":id,"workflow_id":"w","workflow_version":1,"definition":inline(json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]})),"input":inline(Value::Null),"source":"test","source_detail":null,"parent":null,"depth":0,"created_at":"2026-10-15T00:00:00Z","status":"running","output":null,"error":null,"last_run_seq":"0","nodes":{}}),
    )
}
fn dispatch(id: &str, n: u64, seq: u64) -> Event {
    event(
        EventKind::DispatchStarted,
        id,
        seq,
        Some(&format!("{id}-{n}")),
        0,
        json!({"node_execution_id":"s","attempt":n.to_string()}),
    )
}
fn prepared(id: &str) -> Event {
    event(
        EventKind::InputPrepared,
        id,
        0,
        Some(&format!("{id}-1")),
        1,
        json!({"prepared":{"input":inline(Value::Null),"predecessors":{},"params":inline(json!({"pad":"x".repeat(32 * 1024)})),"engine_build":"test","node_semantics":1}}),
    )
}
#[tokio::test]
async fn cold_terminal_history_and_receipts_restore_from_current_sqlite_rows() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let original = prepared("r000");
    let mut original_lsn = 0;
    for n in 0..64 {
        let id = format!("r{n:03}");
        let d = format!("{id}-1");
        let lsn = b
            .append(vec![
                start(&id),
                dispatch(&id, 1, 2),
                prepared(&id),
                event(
                    EventKind::NodeCompleted,
                    &id,
                    3,
                    Some(&d),
                    2,
                    json!({"output":inline(json!(n)),"sealed_through":"2","integrity":"complete"}),
                ),
                event(
                    EventKind::RunCompleted,
                    &id,
                    4,
                    None,
                    0,
                    json!({"output":inline(json!(n))}),
                ),
            ])
            .await
            .unwrap();
        if n == 0 {
            original_lsn = lsn
        }
        assert_eq!(b.diagnostics().await["resident_runs"], 0);
        assert_eq!(b.diagnostics().await["resident_attempts"], 0);
        assert_eq!(b.diagnostics().await["dirty_run_count"], 0);
    }
    assert_eq!(
        b.append(vec![original.clone()]).await.unwrap(),
        original_lsn
    );
    assert_eq!(
        b.run_snapshot("r000").await.unwrap().unwrap().nodes["s"].output,
        Some(inline(json!(0)))
    );
    b.close().await.unwrap();
    drop(b);
    assert!(!dir.path().join("checkpoints").exists());
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let late = event(
        EventKind::LateAudit,
        "r000",
        0,
        Some("r000-1"),
        3,
        json!({"evidence":"late"}),
    );
    let late_lsn = b.append(vec![late.clone()]).await.unwrap();
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(
        b.run_snapshot("r000").await.unwrap().unwrap().nodes["s"].attempts["r000-1"].audit_seq,
        3
    );
    assert_eq!(b.append(vec![late.clone()]).await.unwrap(), late_lsn);
    b.close().await.unwrap();
    drop(b);
    assert!(!dir.path().join("execution-index.sqlite").exists());
    assert!(!dir.path().join("projection.sqlite").exists());
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(b.append(vec![original]).await.unwrap(), original_lsn);
    assert_eq!(b.append(vec![late]).await.unwrap(), late_lsn);
    assert_eq!(b.state().await.unwrap().runs.len(), 64);
    assert_eq!(b.diagnostics().await["resident_runs"], 0);
    assert_eq!(
        b.store.get_execution("r000").await.unwrap().1.unwrap()["nodes"]["s"]["output"],
        json!(inline(json!(0)))
    );
    b.close().await.unwrap();
}
#[tokio::test]
async fn retry_attempts_are_offloaded_and_old_dispatch_audit_is_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    b.append(vec![start("r"), dispatch("r", 1, 2), prepared("r")])
        .await
        .unwrap();
    let mut seq = 2;
    for n in 1..=256 {
        seq += 1;
        let mut events = vec![event(
            EventKind::NodeFailed,
            "r",
            seq,
            Some(&format!("r-{n}")),
            0,
            json!({"error":"retry","sealed_through":if n==1{"1"}else{"0"},"retry_wake_at":1}),
        )];
        if n < 256 {
            seq += 1;
            events.push(dispatch("r", n + 1, seq))
        }
        b.append(events).await.unwrap();
        assert_eq!(b.diagnostics().await["resident_attempts"], 1);
    }
    let late = event(
        EventKind::LateAudit,
        "r",
        0,
        Some("r-1"),
        2,
        json!({"evidence":"old attempt"}),
    );
    b.append(vec![late]).await.unwrap();
    let run = b.run_snapshot("r").await.unwrap().unwrap();
    assert_eq!(run.nodes["s"].attempt, 256);
    assert_eq!(run.nodes["s"].attempts.len(), 1);
    assert_eq!(run.nodes["s"].attempts["r-256"].audit_seq, 0);
    // 派发归属索引保留每个 dispatch 自己的历史尝试号（观测归属反查用）：
    // 重试前的 dispatch 不回落节点当前值。
    let owner = b.store.dispatch_owner("r-1").await.unwrap().unwrap();
    assert_eq!(owner["run_id"], "r");
    assert_eq!(owner["node_id"], "s");
    assert_eq!(owner["attempt"], "1");
    let latest = b.store.dispatch_owner("r-256").await.unwrap().unwrap();
    assert_eq!(latest["attempt"], "256");
    let full = b.full_state().await.unwrap();
    assert_eq!(full.runs["r"].nodes["s"].attempts.len(), 256);
    assert_eq!(full.runs["r"].nodes["s"].attempts["r-1"].audit_seq, 2);
    b.command("barrier", None, &Value::Null, |_| Ok((vec![], Value::Null)))
        .await
        .unwrap();
    assert_eq!(
        b.store.get_execution("r").await.unwrap().1.unwrap()["nodes"]["s"]["attempts"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        b.store.get("run", "r").await.unwrap().1.unwrap()["nodes"]["s"]["attempts"]
            .as_object()
            .unwrap()
            .len(),
        256
    );
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(b.diagnostics().await["resident_attempts"], 1);
    assert_eq!(
        b.run_snapshot("r").await.unwrap().unwrap().nodes["s"].attempt,
        256
    );
    b.close().await.unwrap();
}
#[tokio::test]
async fn evicted_publications_remain_valid_and_cannot_be_republished() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let descriptor = |n| flow_journal::ValueRef {
        journal_id: b.journal.id().into(),
        output_id: format!("published-{n:05}"),
        codec: flow_journal::value::ValueCodec::Utf8,
        version: 1,
        chunk_count: 0,
        total_bytes: 0,
        digest: flow_journal::codec::digest(b""),
    };
    let old = descriptor(0);
    for batch in 0..33 {
        b.append(
            (batch * 256..(batch + 1) * 256)
                .map(|n| Event::new(EventKind::ValuePublished, json!(descriptor(n))))
                .collect(),
        )
        .await
        .unwrap();
    }
    assert_eq!(b.diagnostics().await["cached_publications"], 8192);
    let before = b.journal.durable_lsn();
    assert!(b
        .append(vec![Event::new(EventKind::ValuePublished, json!(old))])
        .await
        .is_err());
    assert_eq!(b.journal.durable_lsn(), before);
    let mut e = start("old-ref");
    e.payload["input"] = json!(StoredValue::Ref(old.clone()));
    b.append(vec![e]).await.unwrap();
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let mut e = start("old-ref-after-restart");
    e.payload["input"] = json!(StoredValue::Ref(old.clone()));
    b.append(vec![e]).await.unwrap();
    assert_eq!(
        b.run_snapshot("old-ref").await.unwrap().unwrap().input,
        StoredValue::Ref(old)
    );
    b.close().await.unwrap();
}
