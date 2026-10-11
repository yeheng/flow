//! flow-journal-server 的端到端契约：
//! FLOW_BACKEND=journal 起真实服务进程，跑全量 RPC 面
//! （workflow / run / 触发器 / 模板 / config）。

mod common;

use serde_json::{json, Value};
use std::time::{Duration, Instant};

use common::call;

/// journal 后端的被测服务进程：独占临时目录即 journal 根。
async fn spawn_journal(dir: std::path::PathBuf) -> common::ServerProc {
    spawn_journal_opt(dir, true).await
}

/// `explicit_backend=false` 模拟「未配置后端」——缺省必须是 journal。
async fn spawn_journal_opt(dir: std::path::PathBuf, explicit_backend: bool) -> common::ServerProc {
    use common::spawn_reporting_ports;
    use std::process::{Command, Stdio};

    let mut cmd = Command::new(common::server_bin());
    if explicit_backend {
        cmd.env("FLOW_BACKEND", "journal");
    }
    cmd.env("FLOW_DATA_DIR", dir.join("secrets"))
        .env("FLOW_JOURNAL_DATA_DIR", dir.join("journal"))
        .env("FLOW_JOURNAL_TOKEN", common::TOKEN)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let (child, ports) = spawn_reporting_ports(
        &mut cmd,
        "FLOW_JOURNAL_ADDR",
        "FLOW_JOURNAL_HTTP_ADDR",
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    common::ServerProc::from_parts(child, ports.rpc, ports.http)
}

struct JournalWorkspace {
    proc: common::ServerProc,
    client: jsonrpsee::ws_client::WsClient,
    _dir: flow_test_support::io::TempDir,
}

impl JournalWorkspace {
    async fn start() -> JournalWorkspace {
        let dir = flow_test_support::io::TempDir::new("flow-rpc-journal");
        let proc = spawn_journal(dir.path().to_path_buf()).await;
        let client = proc.client().await;
        JournalWorkspace {
            proc,
            client,
            _dir: dir,
        }
    }

    /// SIGKILL 后用同一份 journal 目录重拉（TempDir 保持存活——目录删除在 Drop）。
    async fn restart(&mut self) {
        self.proc.kill();
        self.proc = spawn_journal(self._dir.path().to_path_buf()).await;
        self.client = self.proc.client().await;
    }
}

fn line_def() -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "n1", "type": "script", "name": "脚本", "params": {"code": "return { greeting: 'hi ' + (input.who ?? 'world') };"}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    })
}

async fn wait_status(
    client: &jsonrpsee::ws_client::WsClient,
    run_id: &str,
    expected: &str,
) -> Value {
    let run = wait_terminal(client, run_id).await;
    assert_eq!(
        run["run"]["status"],
        json!(expected),
        "run failed: {:?}",
        run["run"]["error"]
    );
    run
}

/// 轮询到终态（succeeded / failed / cancelled）。
async fn wait_terminal(client: &jsonrpsee::ws_client::WsClient, run_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let run: Value = call(client, "run.get", json!({"run_id": run_id})).await;
        if matches!(
            run["run"]["status"].as_str(),
            Some("succeeded" | "failed" | "cancelled")
        ) {
            return run;
        }
        assert!(
            Instant::now() < deadline,
            "等待 run {run_id} 终态超时，当前 {}",
            run["run"]["status"]
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn journal_backend_serves_full_rpc_surface() {
    let ws = JournalWorkspace::start().await;
    let client = &ws.client;

    // ---- workflow 生命周期 ----
    let created: Value = call(client, "workflow.create", json!({"name": "journal-demo"})).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
    let updated: Value = call(
        client,
        "workflow.update",
        json!({"workflow_id": workflow_id, "definition": line_def()}),
    )
    .await;
    let version = updated["version"].as_i64().unwrap();
    call::<Value>(
        client,
        "workflow.publish",
        json!({"workflow_id": workflow_id, "version": version}),
    )
    .await;

    let got: Value = call(client, "workflow.get", json!({"workflow_id": workflow_id})).await;
    assert_eq!(got["definition"]["nodes"][1]["id"], json!("n1"));
    assert_eq!(got["status"], json!("published"));

    let versions: Value = call(
        client,
        "workflow.versions",
        json!({"workflow_id": workflow_id}),
    )
    .await;
    assert_eq!(versions["versions"].as_array().unwrap().len(), 1);

    let list: Value = call(client, "workflow.list", json!({})).await;
    assert_eq!(list["workflows"][0]["workflow_id"], json!(workflow_id));

    // ---- run：start → get → timeline → events → stats ----
    let started: Value = call(
        client,
        "run.start",
        json!({"workflow_id": workflow_id, "input": {"who": "journal"}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();
    let done = wait_status(client, &run_id, "succeeded").await;
    assert_eq!(done["run"]["output"], json!({"greeting": "hi journal"}));
    assert_eq!(done["live"], json!(false));

    let timeline: Value = call(client, "run.timeline", json!({"run_id": run_id})).await;
    assert_eq!(timeline["status"], json!("succeeded"));
    let nodes = timeline["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[1]["id"], json!("n1"));
    assert_eq!(nodes[1]["state"], json!("completed"));
    assert_eq!(nodes[1]["output"], json!({"greeting": "hi journal"}));

    let events: Value = call(client, "run.events", json!({"run_id": run_id})).await;
    let list = events["events"].as_array().unwrap();
    assert!(!list.is_empty());
    assert_eq!(list[0]["event"]["kind"], json!("run_started"));
    assert_eq!(
        list.last().unwrap()["event"]["kind"],
        json!("run_completed")
    );

    let stats: Value = call(client, "run.stats", json!({})).await;
    assert_eq!(stats["total"], json!(1));
    assert_eq!(stats["by_status"]["succeeded"], json!(1));

    let runs: Value = call(
        client,
        "run.list",
        json!({"workflow_id": workflow_id, "limit": 10}),
    )
    .await;
    assert_eq!(runs["runs"].as_array().unwrap().len(), 1);

    // ---- 触发器：schedule / webhook CRUD ----
    let schedule: Value = call(
        client,
        "schedule.create",
        json!({"workflow_id": workflow_id, "cron": "*/5 * * * *", "input": null, "enabled": false}),
    )
    .await;
    let schedule_id = schedule["id"].as_str().unwrap().to_string();
    let schedules: Value = call(client, "schedule.list", json!({})).await;
    assert_eq!(schedules["schedules"].as_array().unwrap().len(), 1);
    call::<Value>(
        client,
        "schedule.update",
        json!({"id": schedule_id, "enabled": true}),
    )
    .await;
    call::<Value>(client, "schedule.delete", json!({"id": schedule_id})).await;

    let webhook: Value = call(
        client,
        "webhook.create",
        json!({"workflow_id": workflow_id}),
    )
    .await;
    let token = webhook["token"].as_str().unwrap().to_string();
    let webhooks: Value = call(client, "webhook.list", json!({})).await;
    assert_eq!(webhooks["webhooks"].as_array().unwrap().len(), 1);
    call::<Value>(
        client,
        "webhook.set_enabled",
        json!({"token": token, "enabled": false}),
    )
    .await;
    call::<Value>(client, "webhook.delete", json!({"token": token})).await;

    // ---- 模板 ----
    let template: Value = call(
        client,
        "template.create",
        json!({
            "name": "journal片段",
            "category": "通用",
            "nodes": [{"id": "a", "type": "script", "params": {"code": "return 1;"}}],
            "edges": []
        }),
    )
    .await;
    let template_id = template["id"].as_str().unwrap().to_string();
    let templates: Value = call(client, "template.list", json!({})).await;
    assert_eq!(templates["templates"][0]["name"], json!("journal片段"));
    call::<Value>(
        client,
        "template.update",
        json!({"id": template_id, "name": "journal片段2"}),
    )
    .await;
    call::<Value>(client, "template.delete", json!({"id": template_id})).await;

    // ---- 进程级配置面（与后端无关，journal 臂同样可用）----
    let config: Value = call(client, "config.get", json!({})).await;
    assert!(
        config["env_overrides"]
            .as_array()
            .is_some_and(|list| list.iter().any(|v| v == "FLOW_BACKEND")),
        "env override must be listed: {}",
        config["env_overrides"]
    );

    // ---- 已有 run 的工作流删除被拒（v1 Conflict）----
    let err = common::call_err(
        client,
        "workflow.delete",
        json!({"workflow_id": workflow_id, "yes": true}),
    )
    .await;
    assert!(err.contains("-32012"), "{err}");
}

#[tokio::test]
async fn journal_backend_survives_restart_from_journal_only() {
    let mut ws = JournalWorkspace::start().await;
    let client = &ws.client;
    let created: Value = call(client, "workflow.create", json!({"name": "persist"})).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
    call::<Value>(
        client,
        "workflow.update",
        json!({"workflow_id": workflow_id, "definition": line_def()}),
    )
    .await;
    call::<Value>(
        client,
        "workflow.publish",
        json!({"workflow_id": workflow_id, "version": 1}),
    )
    .await;
    let started: Value = call(
        client,
        "run.start",
        json!({"workflow_id": workflow_id, "input": {"who": "persist"}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();
    wait_status(client, &run_id, "succeeded").await;

    // SIGKILL 重启：唯一权威是 journal，公共面完整恢复
    ws.restart().await;
    let run: Value = call(&ws.client, "run.get", json!({"run_id": run_id})).await;
    assert_eq!(run["run"]["status"], json!("succeeded"));
    let list: Value = call(&ws.client, "workflow.list", json!({})).await;
    assert_eq!(list["workflows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn default_backend_is_journal_on_fresh_directory() {
    // 缺省后端切换的验收：不设 FLOW_BACKEND、全新目录 → journal 权威，
    // v1 布局目录（有 flow.db）则拒绝启动（防混用护栏）。
    let dir = flow_test_support::io::TempDir::new("flow-rpc-default");
    let proc = spawn_journal_opt(dir.path().to_path_buf(), false).await;
    let client = proc.client().await;
    let created: Value = call(&client, "workflow.create", json!({"name": "default"})).await;
    assert!(created["workflow_id"].is_string());
    // journal 根布局落成（段目录存在；没有 v1 的 flow.db）
    assert!(dir.path().join("journal").is_dir());
    assert!(!dir.path().join("flow.db").exists());

    // v1 布局目录 + 缺省（journal）→ 启动即拒绝
    let v1dir = flow_test_support::io::TempDir::new("flow-rpc-default-v1");
    std::fs::write(v1dir.path().join("flow.db"), b"legacy").unwrap();
    use std::process::{Command, Stdio};
    let output = Command::new(common::server_bin())
        .env("FLOW_DATA_DIR", v1dir.path().join("secrets"))
        .env("FLOW_JOURNAL_DATA_DIR", v1dir.path())
        .env("FLOW_JOURNAL_TOKEN", common::TOKEN)
        .env("FLOW_JOURNAL_ADDR", "127.0.0.1:0")
        .env("FLOW_JOURNAL_HTTP_ADDR", "127.0.0.1:0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn flow-journal-server on v1 layout");
    assert!(!output.status.success(), "v1 布局必须拒绝启动");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("empty data directory") && stderr.contains("import legacy data offline"),
        "错误必须指明 v1 布局与迁移指引：{stderr}"
    );
}
