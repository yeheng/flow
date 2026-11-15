use flow_backend::{plugin_bundle, plugin_operation::Operation};
use flow_config::plugin_fs::RootLease;
use serde_json::json;
use std::{fs, path::PathBuf};

#[test]
fn child_exits_after_publication_without_releasing_leases() {
    let Some(root) = std::env::var_os("FLOW_PLUGIN_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut op = Operation::begin(
        &root,
        "plugins.install",
        "crash",
        &json!({"fixture":true}),
        "demo",
    )
    .unwrap();
    let _lease = RootLease::exclusive(&root).unwrap();
    op.install_intent(None, json!({"installed":true})).unwrap();
    fs::rename(op.stage(), op.live()).unwrap();
    for path in [&root, &root.join(".staging")] {
        fs::File::open(path).unwrap().sync_all().unwrap();
    }
    // No Drop runs for the operation worker or exclusive publication lease.
    std::process::exit(73);
}

#[test]
fn actual_process_exit_releases_locks_and_reconciles_published_generation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("plugins");
    let source = temp.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("plugin.toml"), "[plugin]\nid = \"demo\"\nversion = \"1\"\nentry = \"entry\"\nprotocol = 2\nnode_types = [\"custom\"]\n[methods]\ninvoke = \"invoke\"\n").unwrap();
    fs::write(source.join("entry"), b"original entry").unwrap();
    let mut op = Operation::begin(
        &root,
        "plugins.install",
        "crash",
        &json!({"fixture":true}),
        "demo",
    )
    .unwrap();
    let bundle = plugin_bundle::capture(&source, &op.stage(), &root).unwrap();
    op.seal(bundle.bundle_digest).unwrap();
    drop(op);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_exits_after_publication_without_releasing_leases",
            "--nocapture",
        ])
        .env("FLOW_PLUGIN_CRASH_ROOT", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    fs::remove_dir_all(source).unwrap();
    let mut retry = Operation::begin(
        &root,
        "plugins.install",
        "crash",
        &json!({"fixture":true}),
        "demo",
    )
    .unwrap();
    let _lease = RootLease::exclusive(&root).unwrap();
    assert_eq!(retry.recover().unwrap(), json!({"installed":true}));
    assert_eq!(
        fs::read(retry.live().join("entry")).unwrap(),
        b"original entry"
    );
}
