//! journal 臂（AnyBackend::Journal）公共契约回归：v2 journal 事实映射到
//! v1 公共面（workflow/run/触发器/模板/事件/信号）。历史数据不迁移——
//! 全新数据集上验证「补齐接口后的 v2 直接可服务」。

use flow_backend::journal::JournalBackend;
use flow_backend::{AnyBackend, BackendError, CreateRun, SignalRequest};
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-arm-{tag}-{}", uuid::Uuid::now_v7()))
}

async fn backend(root: &std::path::Path) -> Arc<JournalBackend> {
    let backend = JournalBackend::open(root, JournalOptions::default())
        .await
        .unwrap();
    flow_backend::start_execution(&flow_config::ExecutionConfig::default(), &backend)
        .await
        .unwrap();
    backend
}

/// start → script → end（同步完成）。
fn script_def() -> Value {
    json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "n", "type": "script", "params": {"code": "return {ok: input.x + 1};"}},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
    })
}

/// start → human_task → end（等待外部信号）。
fn human_def() -> Value {
    json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "h", "type": "human_task"},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "h"}, {"from": "h", "to": "e"}]
    })
}

async fn wait_terminal(backend: &JournalBackend, run_id: &str) -> flow_engine::journal_state::Run {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = backend.state().await.runs[run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run terminates")
}

async fn install(backend: &JournalBackend, name: &str, definition: Value) -> (String, i64) {
    let created = backend.workflow_create(name, None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    let updated = backend
        .workflow_update(&id, definition, None)
        .await
        .unwrap();
    let version = updated.result["version"].as_u64().unwrap() as i64;
    backend
        .workflow_publish(&id, version as u64, None)
        .await
        .unwrap();
    (id, version)
}

#[tokio::test]
async fn workflow_lifecycle_maps_to_public_contract() {
    let root = temp("workflow");
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());

    let id = arm.create_workflow("demo").await.unwrap();
    let v1 = arm.update_workflow(&id, &script_def()).await.unwrap();
    assert_eq!(v1, 1);
    // 落库即校验：非法定义在 update 处拒绝
    let bad = arm
        .update_workflow(
            &id,
            &json!({"nodes": [{"id": "x", "type": "nope"}], "edges": []}),
        )
        .await;
    assert!(bad.is_err(), "invalid definition must be rejected");
    arm.publish(&id, v1).await.unwrap();
    // 重复发布幂等
    arm.publish(&id, v1).await.unwrap();

    let latest = arm.get_version(&id, None).await.unwrap();
    assert_eq!(latest.version, 1);
    assert_eq!(latest.status, "published");
    assert_eq!(latest.definition["nodes"][1]["id"], "n");

    let versions = arm.list_versions(&id).await.unwrap();
    assert_eq!(versions.len(), 1);

    let summaries = arm.list_workflows().await.unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].workflow_id, id);
    assert_eq!(summaries[0].published_version, Some(1));

    // 已有 run 时拒绝删除（v1 Conflict 语义）
    arm.create_run(CreateRun {
        workflow_id: id.clone(),
        version: None,
        input: json!({"x": 41}),
        source: "manual".into(),
        source_detail: None,
    })
    .await
    .unwrap();
    let err = arm.delete_workflow(&id).await.unwrap_err();
    assert!(matches!(err, BackendError::Conflict(_)), "{err:?}");

    journal.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn run_read_surface_and_events_map_from_journal() {
    let root = temp("run");
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());
    let (workflow, version) = install(&journal, "run-demo", script_def()).await;

    let created = arm
        .create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: None,
            input: json!({"x": 41}),
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    assert_eq!(created.workflow_version, version);
    let run = wait_terminal(&journal, &created.run_id).await;
    assert_eq!(run.status, "succeeded");

    let record = arm.get_run(&created.run_id).await.unwrap();
    assert_eq!(record.id, created.run_id);
    assert_eq!(record.status, "succeeded");
    assert_eq!(record.input, json!({"x": 41}));
    assert_eq!(record.output, Some(json!({"ok": 42})));
    assert_eq!(record.source, "manual");
    assert!(record.ended_at.is_none(), "journal has no wall clock");

    // 未发布版本 / 不存在工作流的精确错误
    assert!(matches!(
        arm.create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: Some(999),
            input: Value::Null,
            source: "manual".into(),
            source_detail: None,
        })
        .await,
        Err(BackendError::VersionNotFound(_, 999))
    ));

    let runs = arm.list_runs(None, None, None, None, 50).await.unwrap();
    assert_eq!(runs.len(), 1);
    let stats = arm.run_stats(None).await.unwrap();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.by_status["succeeded"], 1);

    // 事件面：v1 Envelope 序列（run_started → node_* → run_completed）
    let events = arm.read_events(&created.run_id, None).await.unwrap();
    let types: Vec<&str> = events.iter().map(|e| e.event.kind()).collect();
    assert_eq!(types.first(), Some(&"run_started"));
    assert_eq!(types.last(), Some(&"run_completed"));
    assert!(types.contains(&"node_started"));
    assert!(types.contains(&"node_completed"));
    // seq 严格递增，ts 为占位 epoch（journal 无墙钟）
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]));
    assert!(events.iter().all(|e| e.ts.timestamp() == 0));
    // node_completed 携带解析后的输出值（取 script 节点 n；start 节点的
    // completed 输出 = run 输入，v1 同语义）
    let completed = events
        .iter()
        .find(|e| e.event.kind() == "node_completed" && e.event.node_id() == Some("n"))
        .unwrap();
    let serialized = serde_json::to_value(completed).unwrap();
    assert_eq!(serialized["output"], json!({"ok": 42}));

    // 快照映射：v1 RunState（timeline 的数据源）
    let snapshot = arm.snapshot(&created.run_id).await.unwrap();
    assert!(matches!(
        snapshot.records.get("n").map(|r| &r.state),
        Some(flow_engine::NodeState::Completed { .. })
    ));
    assert_eq!(snapshot.output, Some(json!({"ok": 42})));

    // is_live：终态 run 不活
    assert!(!arm.is_live(&created.run_id).await);

    journal.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn signal_and_cancel_follow_v1_semantics() {
    let root = temp("signal");
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());
    let (workflow, _) = install(&journal, "human", human_def()).await;

    // 信号：human_task 等待中交付
    let created = arm
        .create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: None,
            input: Value::Null,
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = journal.state().await.runs[&created.run_id].clone();
            if run.nodes.get("h").is_some_and(|n| n.status == "waiting") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("human_task reaches waiting");
    let ack = arm
        .signal(SignalRequest {
            run_id: created.run_id.clone(),
            signal_id: None,
            node_id: "h".into(),
            payload: json!({"approved": true}),
        })
        .await
        .unwrap();
    assert!(ack.delivered);
    // v1 非 pg 契约：客户端未提供 signal_id 时回执不携带（幂等键在服务端内部）
    assert!(ack.signal_id.is_none(), "echo-only: {ack:?}");
    let done = wait_terminal(&journal, &created.run_id).await;
    assert_eq!(done.status, "succeeded");
    // 终态后信号 = conflict
    let err = arm
        .signal(SignalRequest {
            run_id: created.run_id.clone(),
            signal_id: None,
            node_id: "h".into(),
            payload: Value::Null,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Conflict(_)), "{err:?}");

    // 取消：活着的 run 成功，已终态 conflict
    let second = arm
        .create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: None,
            input: Value::Null,
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = journal.state().await.runs[&second.run_id].clone();
            if run.nodes.get("h").is_some_and(|n| n.status == "waiting") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second run waits");
    let cancel_ack = arm.cancel(&second.run_id, None).await.unwrap();
    assert!(cancel_ack.delivered);
    let cancelled = wait_terminal(&journal, &second.run_id).await;
    assert_eq!(cancelled.status, "cancelled");
    assert!(matches!(
        arm.cancel(&second.run_id, None).await,
        Err(BackendError::Conflict(_))
    ));

    journal.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn schedules_webhooks_and_templates_roundtrip() {
    let root = temp("triggers");
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());
    let (workflow, _) = install(&journal, "trig", script_def()).await;

    // schedule CRUD
    let schedule = arm
        .create_schedule(&workflow, "*/5 * * * *", Some(&json!({"k": 1})), true)
        .await
        .unwrap();
    assert!(schedule.enabled);
    assert_eq!(schedule.input, Some(json!({"k": 1})));
    // 触发去重：同一 fire_at 第二次必 false（命令身份已存在）——
    // 触发时 schedule 须启用（run_start 的触发校验）
    let fire_at = chrono::Utc::now();
    assert!(arm.try_insert_fire(&schedule.id, fire_at).await.unwrap());
    journal
        .trigger_start("schedule", &schedule.id, &fire_at.to_rfc3339(), Value::Null)
        .await
        .unwrap();
    assert!(!arm.try_insert_fire(&schedule.id, fire_at).await.unwrap());
    // 触发产生的 run 归因 schedule 来源
    let triggered = arm
        .list_runs(None, None, Some("schedule"), None, 10)
        .await
        .unwrap();
    assert_eq!(triggered.len(), 1);
    assert_eq!(
        triggered[0].source_detail.as_deref(),
        Some(schedule.id.as_str())
    );
    arm.update_schedule(&schedule.id, None, Some(Some(json!({"k": 2}))), Some(false))
        .await
        .unwrap();
    let schedules = arm.list_schedules(None).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert!(!schedules[0].enabled);
    assert_eq!(schedules[0].input, Some(json!({"k": 2})));
    arm.delete_schedule(&schedule.id).await.unwrap();
    assert!(arm.list_schedules(None).await.unwrap().is_empty());

    // webhook CRUD
    let webhook = arm.create_webhook(&workflow).await.unwrap();
    assert!(webhook.enabled);
    assert_eq!(
        arm.get_webhook(&webhook.token)
            .await
            .unwrap()
            .unwrap()
            .token,
        webhook.token
    );
    arm.set_webhook_enabled(&webhook.token, false)
        .await
        .unwrap();
    assert!(
        !arm.get_webhook(&webhook.token)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    let webhooks = arm.list_webhooks(Some(&workflow)).await.unwrap();
    assert_eq!(webhooks.len(), 1);
    arm.delete_webhook(&webhook.token).await.unwrap();
    assert!(arm.get_webhook(&webhook.token).await.unwrap().is_none());

    // template CRUD + 名字冲突
    let template = arm
        .template_create(
            "片段A",
            Some("通用"),
            &json!([{"id": "a", "type": "script", "params": {"code": "return 1;"}}]),
            &json!([]),
        )
        .await
        .unwrap();
    assert_eq!(template.name, "片段A");
    let conflict = arm
        .template_create(
            "片段A",
            None,
            &json!([{"id": "b", "type": "script", "params": {"code": "return 2;"}}]),
            &json!([]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(conflict, BackendError::TemplateNameTaken(_)),
        "{conflict:?}"
    );
    let updated = arm
        .template_update(&template.id, Some("片段A2"), Some(None), None, None)
        .await
        .unwrap();
    assert_eq!(updated.name, "片段A2");
    assert_eq!(updated.category, None);
    let summaries = arm.template_list().await.unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].node_count, 1);
    assert!(arm.template_delete(&template.id).await.unwrap());
    assert!(!arm.template_delete(&template.id).await.unwrap());

    journal.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn subscription_replays_then_ends_and_global_streams_live() {
    use futures::StreamExt;
    let root = temp("subscribe");
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());
    let (workflow, _) = install(&journal, "sub", script_def()).await;
    let created = arm
        .create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: None,
            input: json!({"x": 1}),
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    wait_terminal(&journal, &created.run_id).await;

    // 指定 run：回放全部 + 追平终态后流自然结束
    let mut stream = arm.subscribe(Some(created.run_id.clone()));
    let mut replayed = Vec::new();
    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("subscription event or end within timeout")
    {
        replayed.push(envelope);
    }
    let types: Vec<&str> = replayed.iter().map(|e| e.event.kind()).collect();
    assert_eq!(types.first(), Some(&"run_started"));
    assert_eq!(types.last(), Some(&"run_completed"));
    assert!(types.contains(&"node_completed"));

    // 全局：纯实时增量——订阅启动后新 run 的事件才流入
    let mut global = arm.subscribe(None);
    let second = arm
        .create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: None,
            input: json!({"x": 2}),
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(30), global.next())
        .await
        .expect("live event")
    {
        if envelope.run_id == second.run_id {
            seen.push(envelope.event.kind().to_string());
            if seen.last() == Some(&"run_completed".to_string()) {
                break;
            }
        }
    }
    assert_eq!(seen.first().unwrap(), "run_started");
    assert_eq!(seen.last().unwrap(), "run_completed");

    journal.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_recovers_state_from_journal_only() {
    let root = temp("restart");
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());
    let (workflow, _) = install(&journal, "persist", script_def()).await;
    let created = arm
        .create_run(CreateRun {
            workflow_id: workflow.clone(),
            version: None,
            input: json!({"x": 1}),
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    wait_terminal(&journal, &created.run_id).await;
    journal.close().await.unwrap();
    drop(journal);
    drop(arm);

    // 重开：全部公共面从 journal 重放恢复（投影可丢——数据面唯一权威）
    let journal = backend(&root).await;
    let arm = AnyBackend::Journal(journal.clone());
    let record = arm.get_run(&created.run_id).await.unwrap();
    assert_eq!(record.status, "succeeded");
    assert_eq!(record.output, Some(json!({"ok": 2})));
    let events = arm.read_events(&created.run_id, None).await.unwrap();
    assert!(!events.is_empty());
    journal.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
