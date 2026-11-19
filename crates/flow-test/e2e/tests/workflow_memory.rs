#![cfg(unix)]

use std::{path::Path, process::Output, time::Duration};

use flow_test_support::io::workspace_bin;
use serde_json::{json, Value};
use tokio::process::Command;

#[tokio::test]
async fn isolated_workflow_runs_a_branch_and_join_with_only_the_script_worker() {
    let root = tempfile::tempdir().unwrap();
    install_binaries(root.path());
    let input = root.path().join("input.json");
    std::fs::write(&input, br#"{"value":21}"#).unwrap();
    let definition = json!({
        "input_schema": {"type": "object", "required": ["value"]},
        "output_schema": {"type": "integer"},
        "nodes": [
            {"id": "end", "type": "end"},
            {"id": "double", "type": "script", "params": {"code": "return input.value * 2;"}},
            {"id": "unused", "type": "script", "params": {"code": "throw new Error('wrong branch');"}},
            {"id": "branch", "type": "condition", "params": {"expr": "'false'"}},
            {"id": "start", "type": "start"}
        ],
        "edges": [
            {"from": "start", "to": "branch"},
            {"from": "branch", "port": "true", "to": "double"},
            {"from": "branch", "port": "false", "to": "unused"},
            {"from": "double", "to": "end"},
            {"from": "unused", "to": "end"}
        ]
    });

    let result = run(root.path(), &definition, &input).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["status"], "succeeded");
    assert_eq!(report["output"], json!({"type": "inline", "value": 42}));
    assert_eq!(report["nodes"]["branch"]["branch"], true);
    assert_eq!(report["nodes"]["unused"]["status"], "skipped");
    assert_eq!(report["nodes"]["unused"]["skip_reason"], "inactive");
    assert_eq!(report["nodes"]["double"]["status"], "succeeded");
    assert!(!root.path().join("home").exists());

    let mut legacy = definition.clone();
    legacy["semantics"] = json!(1);
    legacy.as_object_mut().unwrap().remove("output_schema");
    let result = run(root.path(), &legacy, &input).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["nodes"]["end"]["status"], "skipped");
    assert_eq!(report["output"], json!({"type": "inline", "value": null}));

    let mut failed = definition.clone();
    failed["nodes"][1]["params"]["code"] = json!("throw new Error('worker failure');");
    let result = run(root.path(), &failed, &input).await;
    assert!(!result.status.success());
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["status"], "failed");
    assert_eq!(report["nodes"]["double"]["status"], "failed");
    assert!(report["error"].as_str().unwrap().contains("worker failure"));

    // A managed capability is rejected even when placed on an inactive branch.
    let mut managed = definition;
    managed["nodes"][2]["type"] = json!("delay");
    managed["nodes"][2]["params"] = json!({"ms": 1});
    let result = run(root.path(), &managed, &input).await;
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert!(String::from_utf8_lossy(&result.stderr).contains("requires managed execution"));
}

#[tokio::test]
async fn converging_inline_values_can_exceed_the_journal_record_limit() {
    let root = tempfile::tempdir().unwrap();
    install_binaries(root.path());
    let input = root.path().join("input.json");
    std::fs::write(&input, b"null").unwrap();
    let mut nodes = vec![json!({"id":"s", "type":"start"})];
    let mut edges = Vec::new();
    for index in 0..17 {
        let id = format!("p{index}");
        nodes.push(json!({
            "id": id, "type":"script", "params": {"code": "return 'a'.repeat(63000);"}
        }));
        edges.push(json!({"from":"s", "to":id}));
        edges.push(json!({"from":id, "to":"join"}));
    }
    nodes.push(json!({
        "id":"join", "type":"script", "params": {"code": "return Object.keys(nodes).length;"}
    }));
    nodes.push(json!({"id":"e", "type":"end"}));
    edges.push(json!({"from":"join", "to":"e"}));
    let result = run(root.path(), &json!({"nodes":nodes,"edges":edges}), &input).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["status"], "succeeded");
    assert_eq!(report["output"], json!({"type":"inline","value":17}));
}

fn install_binaries(root: &Path) {
    for name in ["flow-workflow", "flow-script-worker"] {
        std::fs::copy(workspace_bin(name), root.join(name)).unwrap();
    }
}

async fn run(root: &Path, definition: &Value, input: &Path) -> Output {
    let path = root.join("definition.json");
    std::fs::write(&path, serde_json::to_vec(definition).unwrap()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        Command::new(root.join("flow-workflow"))
            .current_dir(root)
            .env_clear()
            .env("FLOW_HOME", root.join("home"))
            .arg("--worker")
            .arg(root.join("flow-script-worker"))
            .arg("--definition")
            .arg(path)
            .arg("--input")
            .arg(input)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("local workflow must finish")
    .unwrap()
}
