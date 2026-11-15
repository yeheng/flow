//! One process-isolated scenario: the plugin root environment is not shared with
//! unit test binaries, and all requests exercise the public host API.
use flow_backend::journal::JournalBackend;
use serde_json::json;
use std::{fs, path::Path};

fn package(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::write(path.join("plugin.toml"), "[plugin]\nid = \"demo\"\nversion = \"1\"\nentry = \"entry\"\nprotocol = 2\nnode_types = []\n[service]\nmethods = [\"echo\"]\nweb_ui = \"ui.html\"\n").unwrap();
    fs::write(path.join("entry"), b"not executed during installation").unwrap();
    fs::write(path.join("ui.html"), b"old ui").unwrap();
}
#[tokio::test]
async fn managed_install_replace_uninstall_and_replay_preserve_identity_and_grants() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("plugins");
    // This integration binary contains just this test and spawns no child processes.
    unsafe {
        std::env::set_var("FLOW_PLUGINS_DIR", &root);
    }
    let source = temp.path().join("source");
    package(&source);
    let backend = JournalBackend::open(&temp.path().join("journal"), Default::default())
        .await
        .unwrap();
    let path = source.to_str().unwrap().to_owned();
    let first = backend
        .plugin_install(path.clone(), false, "first".into())
        .await
        .unwrap();
    assert_eq!(first["service"], true);
    assert_eq!(first["executor_reload_required"], false);
    assert_eq!(first["grant_state"], "pending");
    backend
        .plugin_grant("demo", None, true, "grant-demo")
        .await
        .unwrap();
    let marker = fs::read(root.join("demo/.flow-install.json")).unwrap();
    let same = backend
        .plugin_install(path.clone(), false, "same".into())
        .await
        .unwrap();
    assert_eq!(same["idempotent"], true);
    assert_eq!(same["grant_state"], "granted");
    assert_eq!(
        fs::read(root.join("demo/.flow-install.json")).unwrap(),
        marker
    );

    fs::write(source.join("ui.html"), b"new ui").unwrap();
    let error = backend
        .plugin_install(path.clone(), false, "no-replace".into())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already installed"));
    assert_eq!(fs::read(root.join("demo/ui.html")).unwrap(), b"old ui");
    // A terminal pre-publication rejection must not fence every later request.
    let replaced = backend
        .plugin_install(path.clone(), true, "replace".into())
        .await
        .unwrap();
    assert_eq!(replaced["replaced"], true);
    assert_eq!(replaced["grant_state"], "granted");
    assert_eq!(replaced["content_hash"], first["content_hash"]);
    assert_ne!(
        fs::read(root.join("demo/.flow-install.json")).unwrap(),
        marker
    );
    assert_eq!(fs::read(root.join("demo/ui.html")).unwrap(), b"new ui");
    assert!(backend
        .plugin_install(path.clone(), true, "no-replace".into())
        .await
        .unwrap_err()
        .to_string()
        .contains("different arguments"));
    fs::remove_dir_all(&source).unwrap();
    assert_eq!(
        backend
            .plugin_install(path.clone(), false, "first".into())
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        backend
            .plugin_install(path.clone(), false, "no-replace".into())
            .await
            .unwrap_err()
            .to_string(),
        error.to_string()
    );

    let removed = backend
        .plugin_uninstall("demo".into(), false, "remove".into())
        .await
        .unwrap();
    assert_eq!(removed["removed"], true);
    assert_eq!(removed["grants_retained"], true);
    assert!(!root.join("demo").exists());
    package(&source);
    let later = backend
        .plugin_install(path, false, "later".into())
        .await
        .unwrap();
    assert_eq!(later["grant_state"], "granted");
    let later_marker = fs::read(root.join("demo/.flow-install.json")).unwrap();
    assert_eq!(
        backend
            .plugin_uninstall("demo".into(), false, "remove".into())
            .await
            .unwrap(),
        removed
    );
    assert_eq!(
        fs::read(root.join("demo/.flow-install.json")).unwrap(),
        later_marker
    );

    fs::create_dir(root.join("broken")).unwrap();
    fs::write(root.join("broken/plugin.toml"), b"invalid toml").unwrap();
    let rows = backend.plugins_view().await;
    assert!(rows
        .iter()
        .any(|row| row["id"] == "broken" && row["load_state"] == "broken"));
    let broken = backend
        .plugin_uninstall("broken".into(), false, "broken".into())
        .await
        .unwrap();
    assert_eq!(broken["removed"], true);
    assert_eq!(broken["executor_reload_required"], true);
    let absent = backend
        .plugin_uninstall("absent".into(), false, "absent".into())
        .await
        .unwrap();
    assert_eq!(absent["already_absent"], true);
    assert_eq!(absent["removed"], false);
    assert_eq!(absent["trash"], json!(null));
    // The first mutation after restart reconciles this ID's durable old intent
    // under its reservation before admitting a new request.
    let orphan = root.join("orphan");
    package(&orphan);
    let manifest = fs::read_to_string(orphan.join("plugin.toml"))
        .unwrap()
        .replace("id = \"demo\"", "id = \"orphan\"");
    fs::write(orphan.join("plugin.toml"), manifest).unwrap();
    let mut intent = flow_backend::plugin_operation::Operation::begin(
        &root,
        "plugins.uninstall",
        "old-orphan",
        &json!({"plugin_id":"orphan","stop_first":false}),
        "orphan",
    )
    .unwrap();
    {
        let _lease = flow_config::plugin_fs::RootLease::exclusive(&root).unwrap();
        let identity = intent.target_identity().unwrap().unwrap();
        intent
            .remove_intent(identity, json!({"removed":true}))
            .unwrap();
        fs::rename(intent.live(), intent.trash()).unwrap();
    }
    drop(intent);
    assert!(flow_backend::plugin_operation::Operation::has_unfinished(&root, "orphan").unwrap());
    let absent = backend
        .plugin_uninstall("orphan".into(), false, "new-orphan".into())
        .await
        .unwrap();
    assert_eq!(absent["already_absent"], true);
    let old = flow_backend::plugin_operation::Operation::lookup(
        &root,
        "plugins.uninstall",
        "old-orphan",
        &json!({"plugin_id":"orphan","stop_first":false}),
    )
    .unwrap()
    .unwrap();
    assert_eq!(old.replay().unwrap(), Some(json!({"removed":true})));
    assert!(!flow_backend::plugin_operation::Operation::has_unfinished(&root, "orphan").unwrap());
    // Capacity counts broken scan rows, and final publication serializes
    // competing node-type owners across different plugin IDs.
    for index in 0..63 {
        let directory = root.join(format!("capacity-{index}"));
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("plugin.toml"), b"broken").unwrap();
    }
    let extra = temp.path().join("extra");
    package(&extra);
    let manifest = fs::read_to_string(extra.join("plugin.toml"))
        .unwrap()
        .replace("id = \"demo\"", "id = \"extra\"");
    fs::write(extra.join("plugin.toml"), &manifest).unwrap();
    assert!(backend
        .plugin_install(
            extra.to_string_lossy().into_owned(),
            false,
            "capacity-denied".into()
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("plugin limit reached"));
    for index in 0..3 {
        fs::remove_dir_all(root.join(format!("capacity-{index}"))).unwrap();
    }
    backend
        .plugin_install(
            extra.to_string_lossy().into_owned(),
            false,
            "capacity-allowed".into(),
        )
        .await
        .unwrap();
    let mut contenders = Vec::new();
    for id in ["owner-a", "owner-b"] {
        let path = temp.path().join(id);
        package(&path);
        let manifest = fs::read_to_string(path.join("plugin.toml"))
            .unwrap()
            .replace("id = \"demo\"", &format!("id = \"{id}\""))
            .replace(
                "node_types = []",
                "node_types = [\"contested-node\"]\n[methods]\ninvoke = \"invoke\"",
            );
        fs::write(path.join("plugin.toml"), manifest).unwrap();
        contenders.push((id, path));
    }
    let ((a, ap), (b, bp)) = (contenders.remove(0), contenders.remove(0));
    let (first_owner, second_owner) = tokio::join!(
        backend.plugin_install(ap.to_string_lossy().into_owned(), false, a.into()),
        backend.plugin_install(bp.to_string_lossy().into_owned(), false, b.into()),
    );
    assert_ne!(first_owner.is_ok(), second_owner.is_ok());
    assert!(first_owner
        .err()
        .or_else(|| second_owner.err())
        .unwrap()
        .to_string()
        .contains("node type overlaps"));
    for id in ["../demo", ".hidden", "a/b", ""] {
        assert!(backend
            .plugin_uninstall(id.into(), false, format!("invalid-{id}"))
            .await
            .is_err());
    }
    let mismatch = root.join("mismatch");
    package(&mismatch);
    assert!(backend
        .plugin_uninstall("mismatch".into(), false, "mismatch".into())
        .await
        .unwrap_err()
        .to_string()
        .contains("source="));
    assert!(mismatch.exists());
    #[cfg(unix)]
    {
        let target = temp.path().join("external");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), b"untouched").unwrap();
        std::os::unix::fs::symlink(&target, root.join("alias")).unwrap();
        assert!(backend
            .plugin_uninstall("alias".into(), false, "alias".into())
            .await
            .is_err());
        assert!(fs::symlink_metadata(root.join("alias"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read(target.join("keep")).unwrap(), b"untouched");
    }
    backend.close().await.unwrap();
}
