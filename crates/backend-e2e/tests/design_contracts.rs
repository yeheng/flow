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

fn padded_definition() -> Value {
    let mut value = definition();
    value["nodes"][0]["name"] = json!("x".repeat(100_000));
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_saves_and_imports_publish_one_value_even_when_concurrent() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let wf = b.workflow_create("large", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut tasks = Vec::new();
    for index in 0..12 {
        let b = b.clone();
        let wf = wf.clone();
        tasks.push(tokio::spawn(async move {
            let key = format!("save-{index}");
            let receipt = if index % 2 == 0 {
                b.workflow_update(&wf, padded_definition(), Some(&key))
                    .await
                    .unwrap()
            } else {
                b.workflow_import("large", padded_definition(), true, Some(&key))
                    .await
                    .unwrap()
            };
            assert_eq!(receipt.result["version"], 1);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let before = b.store.ordered("published", None, false).await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(
        flow_backend::journal_views::version_metadata(&b, &wf)
            .await
            .unwrap()
            .len(),
        1
    );
    b.workflow_update(&wf, padded_definition(), Some("save-0"))
        .await
        .unwrap();
    let mut changed = padded_definition();
    changed["nodes"][0]["name"] = json!("y".repeat(100_000));
    assert!(b
        .workflow_update(&wf, changed, Some("save-0"))
        .await
        .is_err());
    for key in [String::new(), "x".repeat(129)] {
        let mut changed = padded_definition();
        changed["nodes"][0]["name"] = json!("z".repeat(100_000));
        assert!(b.workflow_update(&wf, changed, Some(&key)).await.is_err());
    }
    assert!(b
        .workflow_update("missing", padded_definition(), None)
        .await
        .is_err());
    assert_eq!(
        b.store.ordered("published", None, false).await.unwrap(),
        before
    );
    b.close().await.unwrap();
    let reopened = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .store
            .ordered("published", None, false)
            .await
            .unwrap(),
        before
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn ambiguous_name_import_is_a_conflict_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let a = b.workflow_create("duplicate", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let z = b.workflow_create("duplicate", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let before = b.journal.durable_lsn();
    let response = rpc(
        &b,
        "workflow.import",
        json!({"name":"duplicate","definition":padded_definition(),"publish":true}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32012, "{response}");
    assert_eq!(b.journal.durable_lsn(), before);
    assert!(b
        .store
        .ordered("published", None, false)
        .await
        .unwrap()
        .is_empty());
    for id in [&a, &z] {
        assert!(flow_backend::journal_views::version_metadata(&b, id)
            .await
            .unwrap()
            .is_empty());
    }
    b.workflow_delete(&a, None).await.unwrap();
    let imported = b
        .workflow_import("duplicate", definition(), true, None)
        .await
        .unwrap();
    assert_eq!(imported.result["workflow_id"], z);
    b.close().await.unwrap();
}

#[tokio::test]
async fn every_template_entry_point_enforces_the_same_domain_rules() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let valid = json!({"name":"valid","nodes":[{"id":"a","type":"script","params":{"code":"return 1;"}}],"edges":[]});
    let mut invalid = Vec::new();
    for (field, value) in [
        ("name", json!(" ")),
        ("name", json!("x".repeat(101))),
        ("name", json!(format!("{}a", " ".repeat(1024)))),
        ("nodes", json!([])),
        ("nodes", json!([{"id":"a","type":"unknown"}])),
        ("edges", json!([{"source":"a","target":"missing"}])),
        ("edges", json!([{"source":"a","target":"a"}])),
    ] {
        let mut params = valid.clone();
        params[field] = value;
        invalid.push(params);
    }
    for (index, params) in invalid.into_iter().enumerate() {
        let before = b.journal.durable_lsn();
        let command = flow_backend::ProductCommand::parse("template.create", &params).unwrap();
        assert!(b
            .product_command(command, &params, &format!("direct-{index}"))
            .await
            .is_err());
        assert!(b
            .template_change(&format!("raw-{index}"), params.clone(), None)
            .await
            .is_err());
        assert_eq!(
            rpc(&b, "template.create", params).await["error"]["code"],
            -32602
        );
        assert_eq!(b.journal.durable_lsn(), before);
    }
    assert!(flow_backend::journal_views::template_list(&b)
        .await
        .unwrap()
        .is_empty());
    b.close().await.unwrap();
}

#[tokio::test]
async fn template_partial_updates_validate_the_merged_fragment_and_preserve_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let created = rpc(
        &b,
        "template.create",
        json!({"name":"fragment","request_id":"fragment-create",
        "nodes":[{"id":"a","type":"script","params":{"code":"return 1;"}},
                 {"id":"b","type":"script","params":{"code":"return 2;"}}],
        "edges":[{"source":"a","target":"b"}]}),
    )
    .await;
    let id = created["result"]["result"]["id"].as_str().unwrap();
    let update = json!({"id":id,"request_id":"fragment-update","nodes":[
        {"id":"a","type":"script","name":"renamed","params":{"code":"return 1;"}},
        {"id":"b","type":"script","params":{"code":"return 2;"}}]});
    let response = rpc(&b, "template.update", update.clone()).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(rpc(&b, "template.update", update).await, response);
    let saved = flow_backend::journal_views::template_get(&b, id)
        .await
        .unwrap();
    assert_eq!(saved.nodes[0]["name"], "renamed");
    assert_eq!(saved.edges.as_array().unwrap().len(), 1);
    let response = rpc(
        &b,
        "template.update",
        json!({"id":id,"edges":[{"source":"a","target":"missing"}]}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        flow_backend::journal_views::template_get(&b, id)
            .await
            .unwrap()
            .edges,
        saved.edges
    );
    assert!(b
        .template_change(id, json!({"category":123}), None)
        .await
        .is_err());
    b.close().await.unwrap();
}

fn parent_definition(child: &str, marker: usize) -> Value {
    json!({"nodes":[{"id":"s","type":"start","name":format!("parent-{marker}")},
        {"id":"c","type":"sub_workflow","params":{"workflow_id":child}},
        {"id":"e","type":"end"}],"edges":[{"from":"s","to":"c"},{"from":"c","to":"e"}]})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependency_freezing_keeps_a_consistent_view_during_atomic_publication() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let child = b.workflow_create("child", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let parent = b.workflow_create("parent", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for version in 1..=12 {
        let mut def = definition();
        def["nodes"][0]["name"] = json!(format!("child-{version}"));
        b.workflow_update(&child, def, None).await.unwrap();
        b.workflow_update(&parent, parent_definition(&child, version), None)
            .await
            .unwrap();
    }
    for (id, version) in [(&child, 1), (&parent, 1)] {
        b.workflow_publish(id, version, None).await.unwrap();
    }
    // Unrelated history must not be part of the frozen execution contract.
    let other = b.workflow_create("other", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for version in 0..8 {
        let mut def = definition();
        def["nodes"][0]["name"] = json!(format!("other-{version}"));
        b.workflow_update(&other, def, None).await.unwrap();
    }
    b.workflow_delete(&other, None).await.unwrap();
    let publisher = {
        let b = b.clone();
        let child = child.clone();
        let parent = parent.clone();
        tokio::spawn(async move {
            for version in 2..=12 {
                b.append(
                    [&parent, &child]
                        .into_iter()
                        .map(|id| {
                            flow_journal::Event::new(
                                flow_journal::EventKind::WorkflowPublished,
                                json!({"workflow_id":id,"version":version.to_string()}),
                            )
                        })
                        .collect(),
                )
                .await
                .unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let mut starters = Vec::new();
    for _ in 0..32 {
        let b = b.clone();
        let child = child.clone();
        let parent = parent.clone();
        starters.push(tokio::spawn(async move {
            let receipt = b
                .run_start(&parent, None, Value::Null, "manual", None, None)
                .await
                .unwrap();
            let id = receipt.result["run_id"].as_str().unwrap();
            let run = b.run_snapshot(id).await.unwrap().unwrap();
            let frozen = run.execution.unwrap();
            assert_eq!(frozen.definitions.len(), 2);
            let root_key = format!("{parent}@{}", run.workflow_version);
            let child_key = &frozen.bindings[&format!("{root_key}/c")];
            assert_eq!(child_key, &format!("{child}@{}", run.workflow_version));
            assert!(frozen.definitions.contains_key(&root_key));
            assert!(frozen.definitions.contains_key(child_key));
        }));
    }
    publisher.await.unwrap();
    for starter in starters {
        starter.await.unwrap();
    }
    b.close().await.unwrap();
}
