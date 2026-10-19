//! The offline projection checker must prove facts == current rows, and must
//! surface semantic divergence, not just checksum corruption.

use flow_backend::journal::JournalBackend;
use flow_backend::projection_check::check;
use flow_journal::sqlite::entity_digest;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

async fn pool(root: &std::path::Path) -> sqlx::SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join(flow_journal::sqlite::DATABASE))
                .read_only(false),
        )
        .await
        .unwrap()
}

async fn close_pool(pool: sqlx::SqlitePool) {
    pool.close().await;
}

#[tokio::test]
async fn replaying_facts_reproduces_current_rows() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("journal");
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let created = backend.workflow_create("projection", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend
        .workflow_update(
            workflow,
            json!({
                "nodes": [{"id":"s","type":"start"},{"id":"e","type":"end"}],
                "edges": [{"from":"s","to":"e"}]
            }),
            None,
        )
        .await
        .unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    // A large input becomes a chunked value plus a published descriptor.
    backend
        .run_start(
            workflow,
            None,
            json!({"blob": "x".repeat(200_000)}),
            "manual",
            None,
            None,
        )
        .await
        .unwrap();
    backend.close().await.unwrap();
    drop(backend);

    let report = check(&root).await.unwrap();
    assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    assert!(report.entities > 0);
    assert!(report.last_lsn > 0);

    // Rewrite one row with a matching checksum: only a replay can catch this.
    let db = pool(&root).await;
    let (key, body): (String, String) =
        sqlx::query_as("SELECT key,body FROM current_entities WHERE kind='run' LIMIT 1")
            .fetch_one(&db)
            .await
            .unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&body).unwrap();
    value["status"] = json!("succeeded");
    let body = serde_json::to_string(&value).unwrap();
    sqlx::query("UPDATE current_entities SET body=?,digest=? WHERE kind='run' AND key=?")
        .bind(&body)
        .bind(entity_digest("run", &key, &body))
        .bind(&key)
        .execute(&db)
        .await
        .unwrap();
    close_pool(db).await;

    let report = check(&root).await.unwrap();
    assert_eq!(report.mismatches.len(), 1, "{:?}", report.mismatches);
    assert_eq!(report.mismatches[0].kind, "run");
    assert!(
        report.mismatches[0]
            .reason
            .contains("differs from replayed facts"),
        "{}",
        report.mismatches[0].reason
    );
}

#[tokio::test]
async fn missing_and_unexpected_rows_are_reported() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("journal");
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    backend.workflow_create("projection", None).await.unwrap();
    backend.close().await.unwrap();
    drop(backend);

    let db = pool(&root).await;
    sqlx::query("DELETE FROM current_entities WHERE kind='workflow'")
        .execute(&db)
        .await
        .unwrap();
    let body = json!({"bogus": true}).to_string();
    sqlx::query(
        "INSERT INTO current_entities(kind,key,body,digest,created_order,last_lsn) VALUES('bogus','x',?,?,'','00000000000000000001')",
    )
    .bind(&body)
    .bind(entity_digest("bogus", "x", &body))
    .execute(&db)
    .await
    .unwrap();
    close_pool(db).await;

    let report = check(&root).await.unwrap();
    let reasons: Vec<_> = report
        .mismatches
        .iter()
        .map(|m| (m.kind.as_str(), m.reason.as_str()))
        .collect();
    assert!(
        reasons
            .iter()
            .any(|(kind, reason)| *kind == "workflow" && reason.contains("row missing")),
        "{reasons:?}"
    );
    assert!(
        reasons
            .iter()
            .any(|(kind, reason)| *kind == "bogus" && reason.contains("unexpected")),
        "{reasons:?}"
    );
}
