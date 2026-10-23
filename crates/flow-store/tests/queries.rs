use flow_journal::sqlite::EntityChange;
use flow_journal::{Event, EventKind, Journal, Transaction};
use flow_store::store::{RunsQuery, Store};
use serde_json::json;

async fn write(j: &Journal, rows: Vec<EntityChange>) {
    let lsn = j.durable_lsn() + 1;
    j.submit_prepared(
        Transaction {
            v: 2,
            journal_id: j.id().into(),
            lsn,
            tx_id: format!("t-{lsn}"),
            events: vec![Event::new(EventKind::Command, json!({}))],
        },
        rows,
    )
    .await
    .unwrap();
}
fn run(id: &str, time: &str, workflow: &str, status: &str) -> EntityChange {
    EntityChange {
        kind: flow_journal::sqlite::EntityKind::Run,
        key: id.into(),
        value: Some(
            json!({"run_id":id,"created_at":time,"workflow_id":workflow,"status":status,"source":"manual","nodes":{}}),
        ),
    }
}

#[tokio::test]
async fn keyset_pagination_normalizes_time_and_filters_before_limit() {
    let dir = flow_test_support::io::TempDir::new("sqlite-query");
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    write(
        &j,
        vec![
            run("a", "2026-01-01T00:00:00Z", "w", "succeeded"),
            run("b", "2026-01-01T00:00:00.000000001Z", "w", "succeeded"),
            run("c", "2026-01-01T08:00:00.000000001+08:00", "w", "succeeded"),
            run("d", "2026-01-02T00:00:00Z", "other", "succeeded"),
            run("e", "2026-01-03T00:00:00Z", "w", "failed"),
        ],
    )
    .await;
    let p = Store::open(&dir.join("flow.sqlite"), j.id()).await.unwrap();
    let (lsn, page) = p
        .runs_page(RunsQuery {
            workflow_id: Some("w"),
            status: Some("succeeded"),
            source: Some("manual"),
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(lsn, 1);
    assert_eq!(
        page.iter()
            .map(|v| v["run_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["c", "b"]
    );
    write(
        &j,
        vec![run("new", "2026-01-04T00:00:00Z", "w", "succeeded")],
    )
    .await;
    let (_, page) = p
        .runs_page(RunsQuery {
            workflow_id: Some("w"),
            status: Some("succeeded"),
            before_run_id: Some("b"),
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page[0]["run_id"], "a");
    assert_eq!(page.len(), 1);
    assert_eq!(
        p.run_counts(Some("w"))
            .await
            .unwrap()
            .iter()
            .map(|r| r.2)
            .sum::<i64>(),
        5
    );
    let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
    let plan:String=conn.prepare("EXPLAIN QUERY PLAN SELECT key FROM current_entities WHERE kind='run' AND workflow_id='w' AND (created_order,key)<('2026','b') ORDER BY created_order DESC,key DESC LIMIT 2").unwrap().query_map([],|r|r.get::<_,String>(3)).unwrap().map(Result::unwrap).collect::<Vec<_>>().join(" ");
    assert!(plan.contains("entity_run_workflow"), "{plan}");
    assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    p.close().await;
    j.close().await.unwrap();
}

#[tokio::test]
async fn current_reads_reject_old_revision_requests_and_detect_content_corruption() {
    let dir = flow_test_support::io::TempDir::new("sqlite-checksum");
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    write(&j, vec![run("r", "2026-01-01T00:00:00Z", "w", "running")]).await;
    let p = Store::open(&dir.join("flow.sqlite"), j.id()).await.unwrap();
    assert_eq!(p.active_run_ids("").await.unwrap(), ["r"]);
    write(&j, vec![run("r", "2026-01-01T00:00:00Z", "w", "succeeded")]).await;
    assert!(p.active_run_ids("").await.unwrap().is_empty());
    assert!(p.current_at("run", "r", 1).await.is_err());
    assert_eq!(
        p.get_summary("run", "r").await.unwrap().1.unwrap()["status"],
        "succeeded"
    );
    let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
    assert!(conn.execute("DELETE FROM current_entities", []).is_err());
    let modify =
        sqlx::SqlitePool::connect(&format!("sqlite://{}", dir.join("flow.sqlite").display()))
            .await
            .unwrap();
    sqlx::query("UPDATE current_entities SET body='{}' WHERE kind='run'")
        .execute(&modify)
        .await
        .unwrap();
    assert!(p.get("run", "r").await.is_err());
    modify.close().await;
    p.close().await;
    j.close().await.unwrap();
}

#[tokio::test]
async fn aggregate_response_budget_returns_a_typed_limit() {
    let dir = flow_test_support::io::TempDir::new("sqlite-response-budget");
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    // Individually valid rows exceed the collection budget together.
    for index in 0..9 {
        write(
            &j,
            vec![EntityChange {
                kind: flow_journal::sqlite::EntityKind::Template,
                key: format!("t{index}"),
                value: Some(json!({"id":format!("t{index}"),"nodes":["x".repeat(1024*1024)]})),
            }],
        )
        .await;
    }
    let store = Store::open(&dir.join("flow.sqlite"), j.id()).await.unwrap();
    assert!(matches!(
        store.ordered("template", None, false).await,
        Err(flow_store::store::Error::ResponseTooLarge)
    ));
    store.close().await;
    j.close().await.unwrap();
}

#[tokio::test]
async fn list_budget_keeps_a_contiguous_prefix_and_cursor_pagination() {
    let dir = flow_test_support::io::TempDir::new("list-budget");
    let j = Journal::open(dir.path(), Default::default()).await.unwrap();
    for key in ["a", "b", "c", "d", "e"] {
        write(
            &j,
            vec![EntityChange {
                kind: flow_journal::sqlite::EntityKind::Template,
                key: key.into(),
                value: Some(json!({"id": key, "padding": "x".repeat(1_100_000)})),
            }],
        )
        .await;
    }
    let store = Store::open(&dir.join("flow.sqlite"), j.id()).await.unwrap();
    let (_, first) = store.list("template", "", 256).await.unwrap();
    assert_eq!(
        first
            .iter()
            .map(|v| v["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    let (_, second) = store.list("template", "c", 256).await.unwrap();
    assert_eq!(
        second
            .iter()
            .map(|v| v["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["d", "e"]
    );
    assert!(store.list("template", "", 0).await.unwrap().1.is_empty());
    store.close().await;
    j.close().await.unwrap();
}

#[test]
fn entity_kind_rejects_unknown_namespace_and_keeps_existing_wire_names() {
    assert!(
        serde_json::from_value::<EntityChange>(json!({"kind":"rn","key":"r","value":null}))
            .is_err()
    );
    let row: EntityChange =
        serde_json::from_value(json!({"kind":"run_node","key":"r","value":null})).unwrap();
    assert_eq!(serde_json::to_value(row).unwrap()["kind"], "run_node");
}
