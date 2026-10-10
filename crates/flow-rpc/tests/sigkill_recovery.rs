//! 回归：journal 后端 SIGKILL 恢复的两条路径。
//!
//! 1. pre-auth 窗口（DispatchStarted 与授权之间被杀）：未授权 = 请求从未
//!    发出，重启重新派发是安全语义；重放请求按节点 timeout_ms 计超时后
//!    进 uncertain（此前误报「挂起」——实为 http 默认 30s 超时 > 测试 20s
//!    等待窗口）。
//! 2. 恢复后人工裁决 failed → run 终态 failed（裁决决策面回归）。

mod common;

use serde_json::{json, Value};
use std::time::{Duration, Instant};

use common::call;

async fn spawn_journal(dir: &std::path::Path) -> common::ServerProc {
    use common::spawn_reporting_ports;
    use std::process::{Command, Stdio};

    let mut cmd = Command::new(common::server_bin());
    cmd.env("FLOW_BACKEND", "journal")
        .env("FLOW_DATA_DIR", dir.join("secrets"))
        .env("FLOW_JOURNAL_DATA_DIR", dir.join("journal"))
        .env("FLOW_JOURNAL_TOKEN", common::TOKEN)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::process::Stdio::inherit());
    let (child, ports) = spawn_reporting_ports(
        &mut cmd,
        "FLOW_JOURNAL_ADDR",
        "FLOW_JOURNAL_HTTP_ADDR",
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    common::ServerProc::from_parts(child, ports.rpc, ports.http)
}

#[tokio::test]
async fn sigkill_preauth_window_recovers_via_safe_redispatch() {
    let dir = flow_test_support::io::TempDir::new("flow-journal-sigkill");

    // 挂起 stub：只接受连接，永不响应
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stub = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            std::mem::forget(socket);
        }
    });

    let mut proc = spawn_journal(dir.path()).await;
    let client = proc.client().await;
    let created: Value = call(&client, "workflow.create", json!({"name": "pay"})).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
    let def = json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "call", "type": "http_call", "params": {"method": "POST", "url": format!("http://{stub}/pay"), "timeout_ms": 500}},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "call"}, {"from": "call", "to": "e"}]
    });
    let updated: Value = call(
        &client,
        "workflow.update",
        json!({"workflow_id": workflow_id, "definition": def}),
    )
    .await;
    call::<Value>(
        &client,
        "workflow.publish",
        json!({"workflow_id": workflow_id, "version": updated["version"]}),
    )
    .await;
    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": workflow_id, "input": {}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // 等 call 节点出现在时间线（DispatchStarted 已提交）
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let timeline: Value = call(&client, "run.timeline", json!({"run_id": run_id})).await;
        let node = timeline["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == json!("call"))
            .cloned();
        match node {
            Some(n) if n["state"] == json!("running") => break,
            _ => {
                assert!(Instant::now() < deadline, "call 未启动：{timeline}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    // pre-auth 窗口：立即杀（DispatchStarted 与授权之间）
    // SIGKILL + 重启
    proc.kill();
    let mut proc = spawn_journal(dir.path()).await;
    let client = proc.client().await;

    let deadline = Instant::now() + Duration::from_secs(40);
    let mut reached = false;
    loop {
        let run: Value = call(&client, "run.get", json!({"run_id": run_id})).await;
        if run["run"]["status"] == json!("awaiting_resume") {
            reached = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !reached {
        proc.kill();
        drop(client);
        let output = std::process::Command::new(flow_test_support::io::journal_dev_bin())
            .arg("--data-dir")
            .arg(dir.path())
            .arg("status")
            .output()
            .expect("journal-dev status");
        panic!(
            "awaiting_resume 未达成；stderr: {}\nstate: {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
