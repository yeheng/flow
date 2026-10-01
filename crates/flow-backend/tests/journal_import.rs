use flow_backend::{journal::JournalBackend, journal_import};
use serde_json::{json, Value};

#[tokio::test]
async fn legacy_import_is_reentrant_preserves_raw_tail_and_reports_missing_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("legacy");
    std::fs::create_dir(&source).unwrap();
    let db = source.join("flow.db");
    let store = flow_store::Store::open(&db).await.unwrap();
    let workflow = store.create_workflow("legacy").await.unwrap();
    store.update_workflow(&workflow,&json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]})).await.unwrap();
    drop(store);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs(id,workflow_id,workflow_version,status,input,source,started_at) VALUES('run-1',?,1,'running','{}','manual','2026-09-30T00:00:00Z')").bind(&workflow).execute(&pool).await.unwrap();
    pool.close().await;
    let dir = source.join("runs/run-1");
    std::fs::create_dir_all(&dir).unwrap();
    let raw=b"{\"seq\":1,\"run_id\":\"run-1\",\"ts\":\"2026-09-30T00:00:00Z\",\"type\":\"run_completed\",\"output\":{}}\npartial";
    std::fs::write(dir.join("event.jsonl"), raw).unwrap();
    let destination = temp.path().join("v2");
    let first = journal_import::import(&source, &db, &destination)
        .await
        .unwrap();
    // Model interruption before publishing one copied evidence file. The journal
    // baseline already exists, and unrelated partial temp data must not become truth.
    let copied = destination.join("legacy-source/runs/run-1/event.jsonl");
    std::fs::remove_file(&copied).unwrap();
    std::fs::write(copied.with_file_name(".flow-copy-interrupted"), b"partial").unwrap();
    let second = journal_import::import(&source, &db, &destination)
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(std::fs::read(&copied).unwrap(), raw);
    assert_eq!(std::fs::read(dir.join("event.jsonl")).unwrap(), raw);
    let backend = JournalBackend::open(&destination, Default::default())
        .await
        .unwrap();
    let state = backend.state().await;
    let entry = &state.legacy["reports/run-1"];
    let value: flow_journal::StoredValue = serde_json::from_value(entry["data"].clone()).unwrap();
    let report = flow_journal::value::materialize(
        &destination,
        backend.journal.durable_lsn(),
        &value,
        1024 * 1024,
    )
    .unwrap();
    assert_eq!(report["actual_node_inputs"], "missing_unrecoverable");
    assert_eq!(report["log"]["metadata_conflict"], true);
    assert_eq!(report["log"]["partial_or_oversize"], true);
    let entry = &state.legacy["raw/runs/run-1/event.jsonl/0"];
    let flow_journal::StoredValue::Ref(reference) =
        serde_json::from_value::<flow_journal::StoredValue>(entry["data"].clone()).unwrap()
    else {
        panic!("raw reference")
    };
    let mut restored = Vec::new();
    flow_journal::value::read_value(
        &destination,
        backend.journal.durable_lsn(),
        &reference,
        true,
        &mut restored,
    )
    .unwrap();
    assert_eq!(restored, raw);
    assert!(
        state.runs.is_empty(),
        "legacy importing must never schedule an old side effect"
    );
    backend.close().await.unwrap();
    drop(backend);
    std::fs::write(dir.join("event.jsonl"), b"changed").unwrap();
    assert!(journal_import::import(&source, &db, &destination)
        .await
        .is_err());
    assert_eq!(first["counts"]["runs"], Value::from(1));
}
