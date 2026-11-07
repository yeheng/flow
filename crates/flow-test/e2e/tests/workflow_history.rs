use flow_backend::journal::JournalBackend;
use serde_json::json;

#[tokio::test]
async fn historical_versions_are_cold_but_remain_publishable_and_executable_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let id = backend
        .workflow_create("history", None)
        .await
        .unwrap()
        .result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for version in 1..=50 {
        backend.workflow_update(&id, json!({"semantics":2,"nodes":[
            {"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":format!("return {version};")}},
            {"id":"e","type":"end"}
        ],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}), None).await.unwrap();
        if version == 1 || version == 40 {
            backend.workflow_publish(&id, version, None).await.unwrap();
        }
    }
    assert_eq!(
        backend.diagnostics().await["resident_definition_versions"],
        2
    );
    assert_eq!(
        flow_backend::journal_views::version_metadata(&backend, &id)
            .await
            .unwrap()
            .len(),
        50
    );
    assert_eq!(
        flow_backend::journal_views::get_version(&backend, &id, Some(1))
            .await
            .unwrap()
            .version,
        1
    );
    assert_eq!(
        backend.state().await.unwrap().workflows[&id].versions.len(),
        50
    );
    // Publishing an older cold version must not displace the highest published version.
    backend.workflow_publish(&id, 2, None).await.unwrap();
    assert_eq!(
        backend.diagnostics().await["resident_definition_versions"],
        2
    );
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(
        backend.diagnostics().await["resident_definition_versions"],
        2
    );
    assert_eq!(
        flow_backend::journal_views::get_version(&backend, &id, Some(2))
            .await
            .unwrap()
            .status,
        "published"
    );
    backend
        .config_change(
            "webhook.change",
            flow_journal::EventKind::WebhookChanged,
            "historical-hook",
            json!({"workflow_id":id,"enabled":true,
            "invoke_version":1,"invoke_key_hash":"a".repeat(64)}),
            None,
        )
        .await
        .unwrap();
    backend
        .config_change(
            "webhook.change",
            flow_journal::EventKind::WebhookChanged,
            "historical-hook",
            json!({"enabled":false}),
            None,
        )
        .await
        .unwrap();
    let latest = backend
        .run_start(&id, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    assert_eq!(latest.result["workflow_version"], 40);
    let old = backend
        .run_start(&id, Some(1), json!({}), "manual", None, None)
        .await
        .unwrap();
    let old_run = backend
        .run_snapshot(old.result["run_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_run.workflow_version, 1);
    let parent = backend.workflow_import("pinned-child", json!({"semantics":2,"nodes":[
        {"id":"s","type":"start"},{"id":"c","type":"sub_workflow","params":{"workflow_id":id,"version":1}},
        {"id":"e","type":"end"}
    ],"edges":[{"from":"s","to":"c"},{"from":"c","to":"e"}]}), true, None).await.unwrap();
    backend
        .run_start(
            parent.result["workflow_id"].as_str().unwrap(),
            None,
            json!({}),
            "manual",
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        backend.diagnostics().await["resident_definition_versions"],
        3
    );
    backend.close().await.unwrap();
}
