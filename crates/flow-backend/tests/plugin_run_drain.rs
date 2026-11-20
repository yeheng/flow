use flow_backend::journal::JournalBackend;
use serde_json::{json, Value};
use std::fs;

async fn publish(backend: &JournalBackend, name: &str, definition: Value) -> String {
    let receipt = backend.workflow_create(name, None).await.unwrap();
    let id = receipt.result["workflow_id"].as_str().unwrap().to_owned();
    backend
        .workflow_update(&id, definition, None)
        .await
        .unwrap();
    backend.workflow_publish(&id, 1, None).await.unwrap();
    id
}

#[tokio::test]
async fn plugin_needed_by_a_future_child_is_protected_until_root_finishes() {
    let temp = tempfile::tempdir().unwrap();
    let plugins = temp.path().join("plugins");
    // This test binary contains one test and uses a private plugin root.
    unsafe {
        std::env::set_var("FLOW_PLUGINS_DIR", plugins);
    }
    let source = temp.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("entry"), b"plugin version 1").unwrap();
    fs::write(
        source.join("plugin.toml"),
        r#"
[plugin]
id = "demo"
version = "1"
entry = "entry"
protocol = 2
node_types = ["demo-node"]
[methods]
invoke = "invoke"
"#,
    )
    .unwrap();
    let source = source.to_str().unwrap().to_owned();
    let backend = JournalBackend::open(&temp.path().join("journal"), Default::default())
        .await
        .unwrap();
    backend
        .plugin_install(source.clone(), false, "install".into())
        .await
        .unwrap();
    backend
        .plugin_grant("demo", None, true, "grant")
        .await
        .unwrap();
    let child = publish(&backend, "child", json!({
        "semantics": 2,
        "permissions": {"version":1,"mode":"execute","allow":[]},
        "nodes":[{"id":"s","type":"start"},{"id":"p","type":"demo-node"},{"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"p"},{"from":"p","to":"e"}]
    })).await;
    let root = publish(
        &backend,
        "root",
        json!({
            "semantics": 2,
            "nodes":[{"id":"s","type":"start"},{"id":"c","type":"sub_workflow",
                "params":{"workflow_id":child}},{"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"c"},{"from":"c","to":"e"}]
        }),
    )
    .await;
    let receipt = backend
        .run_start(&root, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = receipt.result["run_id"].as_str().unwrap();

    let rejected = backend
        .plugin_uninstall("demo".into(), false, "busy-remove".into())
        .await;
    assert!(
        rejected.is_err(),
        "uninstall should protect the not-yet-started child"
    );
    assert!(rejected.unwrap_err().to_string().contains(run_id));
    fs::write(
        std::path::Path::new(&source).join("entry"),
        b"plugin version 2",
    )
    .unwrap();
    let rejected = backend
        .plugin_install(source, true, "busy-replace".into())
        .await;
    assert!(rejected.unwrap_err().to_string().contains(run_id));
    let rejected = backend
        .run_start(&root, None, json!({}), "manual", None, None)
        .await;
    assert!(rejected
        .unwrap_err()
        .to_string()
        .contains("plugin demo is draining"));
    assert!(backend
        .plugins_view()
        .await
        .iter()
        .any(|row| { row["id"] == "demo" && row["draining"] == true }));

    let unrelated = publish(
        &backend,
        "unrelated",
        json!({
            "nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"e"}]
        }),
    )
    .await;
    backend
        .run_start(&unrelated, None, json!({}), "manual", None, None)
        .await
        .unwrap();

    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(&temp.path().join("journal"), Default::default())
        .await
        .unwrap();
    let rejected = backend
        .run_start(&root, None, json!({}), "manual", None, None)
        .await;
    assert!(rejected
        .unwrap_err()
        .to_string()
        .contains("plugin demo is draining"));
    backend.run_cancel(run_id, Some("cancel")).await.unwrap();
    let removed = backend
        .plugin_uninstall("demo".into(), false, "remove".into())
        .await
        .unwrap();
    assert_eq!(removed["removed"], true);
    backend.close().await.unwrap();
}
