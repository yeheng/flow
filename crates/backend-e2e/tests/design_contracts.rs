//! Regression coverage for shared command rules and typed RPC boundaries.
use flow_backend::journal::JournalBackend;
use serde_json::{json, Value};

const TOKEN: &str = "design-contract-test-token-with-32-bytes";
fn definition() -> Value {
    json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"e"}]})
}
async fn rpc(b: &std::sync::Arc<JournalBackend>, method: &str, mut params: Value) -> Value {
    params["_token"] = json!(TOKEN);
    if flow_rpc::journal_v2::is_write_method(method) && params.get("request_id").is_none() {
        params["request_id"] = json!(uuid::Uuid::now_v7().to_string());
    }
    let module = flow_rpc::journal_v2::module(b.clone(), TOKEN.into()).unwrap();
    let request = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string();
    let (response, _) = module.raw_json_request(&request, 1).await.unwrap();
    serde_json::from_str(response.get()).unwrap()
}

#[tokio::test]
async fn workflow_entry_points_share_name_normalization_and_version_rules() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    for name in ["", " ", "\t\n", "　"] {
        assert!(b.workflow_create(name, None).await.is_err());
        assert!(b
            .workflow_import(name, definition(), false, None)
            .await
            .is_err());
    }
    assert!(flow_backend::journal_views::list_workflows(&b)
        .await
        .unwrap()
        .is_empty());
    let wf = b.workflow_create("shared", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(b.workflow_validate(definition()).unwrap()["valid"], true);
    assert_eq!(
        b.workflow_update(&wf, definition(), Some("save"))
            .await
            .unwrap()
            .result["version"],
        1
    );
    let mut explicit = definition();
    explicit["semantics"] = json!(2);
    let imported = b
        .workflow_import("shared", explicit.clone(), true, Some("import"))
        .await
        .unwrap();
    assert_eq!(imported.result["workflow_id"], wf);
    assert_eq!(imported.result["version"], 1);
    assert_eq!(imported.result["status"], "published");
    assert_eq!(
        b.workflow_update(&wf, explicit, Some("save2"))
            .await
            .unwrap()
            .result["version"],
        1
    );
    assert_eq!(
        flow_backend::journal_views::version_metadata(&b, &wf)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        b.workflow_import("shared", definition(), true, Some("import"))
            .await
            .unwrap()
            .commit_cursor,
        imported.commit_cursor
    );
    let mut changed = definition();
    changed["nodes"][0]["name"] = json!("changed");
    assert_eq!(
        b.workflow_import("shared", changed.clone(), false, None)
            .await
            .unwrap()
            .result["version"],
        2
    );
    assert_eq!(
        b.workflow_update(&wf, changed, None).await.unwrap().result["version"],
        2
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn invalid_enabled_is_rejected_without_creating_or_mutating_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let wf = b.workflow_create("flags", None).await.unwrap().result["workflow_id"].clone();
    for enabled in [
        json!("false"),
        json!(123),
        json!(null),
        json!({}),
        json!([]),
    ] {
        let response = rpc(
            &b,
            "schedule.create",
            json!({"workflow_id":wf,"cron":"* * * * *","enabled":enabled}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32602, "{response}");
    }
    assert!(b.trigger_configs("schedule").await.is_empty());
    for enabled in [Some(false), None] {
        let mut params = json!({"workflow_id":wf,"cron":"* * * * *"});
        if let Some(enabled) = enabled {
            params["enabled"] = json!(enabled);
        }
        let response = rpc(&b, "schedule.create", params).await;
        let result = &response["result"]["result"];
        assert_eq!(result["enabled"], enabled.unwrap_or(true), "{response}");
        let key = result["id"].as_str().unwrap();
        let bad = rpc(&b, "schedule.update", json!({"id":key,"enabled":"false"})).await;
        assert_eq!(bad["error"]["code"], -32602);
        let before = b
            .trigger_configs("schedule")
            .await
            .into_iter()
            .find(|v| v["id"] == key)
            .unwrap();
        assert_eq!(before["enabled"], enabled.unwrap_or(true));
        let updated = rpc(
            &b,
            "schedule.update",
            json!({"id":key,"enabled":false,"input":null}),
        )
        .await;
        assert!(updated.get("error").is_none(), "{updated}");
        let after = b
            .trigger_configs("schedule")
            .await
            .into_iter()
            .find(|v| v["id"] == key)
            .unwrap();
        assert_eq!(after["enabled"], false);
        assert!(after["input"].is_null());
    }
    for (method, params) in [
        ("schedule.delete", json!({"id":"ghost"})),
        (
            "webhook.set_enabled",
            json!({"token":"ghost","enabled":false}),
        ),
        (
            "workflow.publish",
            json!({"workflow_id":"ghost","version":1}),
        ),
        ("run.start", json!({"workflow_id":"ghost"})),
    ] {
        let response = rpc(&b, method, params).await;
        assert_eq!(response["error"]["code"], -32011, "{method}: {response}");
    }
    b.close().await.unwrap();
}

#[tokio::test]
async fn template_updates_distinguish_omitted_category_from_explicit_null() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let created = rpc(
        &b,
        "template.create",
        json!({"name":"snippet","category":"group","nodes":[{"id":"n","type":"script","params":{"code":"return 1;"}}],"edges":[]}),
    )
    .await;
    let id = created["result"]["result"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{created}"));
    for params in [
        json!({"id":id,"name":"renamed"}),
        json!({"id":id,"category":null}),
    ] {
        let response = rpc(&b, "template.update", params).await;
        assert!(response.get("error").is_none(), "{response}");
    }
    let value = flow_backend::journal_views::template_get(&b, id)
        .await
        .unwrap();
    assert_eq!(value.name, "renamed");
    assert!(value.category.is_none());
    let response = rpc(&b, "template.update", json!({"id":id,"category":123})).await;
    assert_eq!(response["error"]["code"], -32602);
    b.close().await.unwrap();
}

#[tokio::test]
async fn rpc_registration_uses_the_shared_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let module = flow_rpc::journal_v2::module(b.clone(), TOKEN.into()).unwrap();
    let mut methods = std::collections::BTreeSet::new();
    for &name in flow_rpc::journal_v2::READ_METHODS
        .iter()
        .chain(flow_rpc::journal_v2::WRITE_METHODS)
    {
        assert!(methods.insert(name), "duplicate method: {name}");
        assert!(
            module.method_names().any(|registered| registered == name),
            "unregistered: {name}"
        );
    }
    assert!(flow_rpc::journal_v2::is_write_method(
        "journal.cluster.remove"
    ));
    b.close().await.unwrap();
}
