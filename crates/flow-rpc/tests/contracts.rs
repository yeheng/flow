use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flow_backend::{AnyBackend, SqliteBackend};
use flow_engine::{Event, EventLog, RunPhase};
use flow_rpc::{build_module, AppState};
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
    backend: Arc<SqliteBackend>,
    state: Arc<AppState>,
    workflow: String,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-contracts-{}", uuid::Uuid::now_v7()));
        let backend = Arc::new(
            SqliteBackend::open(&root, root.join("flow.db"))
                .await
                .unwrap(),
        );
        let workflow = backend.store().create_workflow("test").await.unwrap();
        backend
            .store()
            .update_workflow(&workflow, &definition(7))
            .await
            .unwrap();
        let state = Arc::new(AppState {
            backend: AnyBackend::Sqlite(backend.clone()),
        });
        Self {
            root,
            backend,
            state,
            workflow,
        }
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        let module = build_module(self.state.clone()).unwrap();
        let request =
            json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}).to_string();
        let (response, _) = module.raw_json_request(&request, 1).await.unwrap();
        serde_json::from_str(response.get()).unwrap()
    }

    async fn wait_finished(&self, run: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = self.backend.store().get_run(run).await.unwrap();
                if matches!(row.status.as_str(), "succeeded" | "failed" | "cancelled")
                    && !self.backend.engine().is_live(run)
                {
                    return serde_json::to_value(row).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("run did not finish")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn definition(output: i64) -> Value {
    json!({
        "nodes": [
            {"id":"s", "type":"start"},
            {"id":"n", "type":"script", "params":{"code":format!("return {output};")}},
            {"id":"e", "type":"end"}
        ],
        "edges": [{"from":"s", "to":"n"}, {"from":"n", "to":"e"}]
    })
}

#[tokio::test]
async fn explicit_draft_is_rejected_and_historical_published_version_still_runs() {
    let f = Fixture::new().await;
    let rejected = f
        .call("run.start", json!({"workflow_id":f.workflow, "version":1}))
        .await;
    assert_eq!(rejected["error"]["code"], -32012);
    assert!(f
        .backend
        .store()
        .list_runs(None, 100)
        .await
        .unwrap()
        .is_empty());
    f.backend.store().publish(&f.workflow, 1).await.unwrap();
    f.backend
        .store()
        .update_workflow(&f.workflow, &definition(8))
        .await
        .unwrap();
    let rejected = f
        .call("run.start", json!({"workflow_id":f.workflow, "version":2}))
        .await;
    assert_eq!(rejected["error"]["code"], -32012);
    for params in [
        json!({"workflow_id":f.workflow, "version":1}),
        json!({"workflow_id":f.workflow}),
    ] {
        let response = f.call("run.start", params).await;
        assert_eq!(response["result"]["workflow_version"], 1);
        let run = response["result"]["run_id"].as_str().unwrap();
        let row = f.wait_finished(run).await;
        assert_eq!(row["output"], 7);
        assert_eq!(row["status"], "succeeded");
    }
}

#[tokio::test]
async fn incomplete_initialization_is_failed_without_replaying_missing_logs() {
    let f = Fixture::new().await;
    f.backend.store().publish(&f.workflow, 1).await.unwrap();
    for run in ["missing", "empty", "partial"] {
        f.backend
            .store()
            .insert_run(run, &f.workflow, 1, &Value::Null, "initializing")
            .await
            .unwrap();
        if run != "missing" {
            drop(EventLog::create(&f.root, run).await.unwrap());
        }
        if run == "partial" {
            tokio::fs::write(f.backend.engine().events_path(run), b"{\"seq\":1")
                .await
                .unwrap();
        }
    }
    let failures = flow_backend::recover_unfinished(&f.backend).await.unwrap();
    assert_eq!(failures.len(), 3);
    for run in ["missing", "empty", "partial"] {
        let row = f.backend.store().get_run(run).await.unwrap();
        assert_eq!(row.status, "failed");
        assert!(row.error.is_some());
        assert!(!f.backend.engine().is_live(run));
    }
    assert!(flow_backend::recover_unfinished(&f.backend)
        .await
        .unwrap()
        .is_empty());
    assert!(!f.backend.engine().events_path("missing").exists());
}

#[tokio::test]
async fn initialized_log_is_resumed_even_when_metadata_still_says_initializing() {
    let f = Fixture::new().await;
    f.backend.store().publish(&f.workflow, 1).await.unwrap();
    f.backend
        .store()
        .insert_run("r", &f.workflow, 1, &Value::Null, "initializing")
        .await
        .unwrap();
    let mut log = EventLog::create(&f.root, "r").await.unwrap();
    log.append(
        "r",
        Event::RunStarted {
            workflow_id: f.workflow.clone(),
            workflow_version: 1,
            input: Value::Null,
            depth: 0,
        },
    )
    .await
    .unwrap();
    drop(log);
    assert!(flow_backend::recover_unfinished(&f.backend)
        .await
        .unwrap()
        .is_empty());
    let row = f.wait_finished("r").await;
    assert_eq!(row["status"], "succeeded");
    assert_eq!(row["output"], 7);
    assert_eq!(
        f.backend.engine().snapshot("r").await.unwrap().phase,
        RunPhase::Succeeded
    );
}

#[tokio::test]
async fn missing_or_empty_logs_of_old_running_tasks_require_manual_recovery() {
    let f = Fixture::new().await;
    for run in ["missing", "empty"] {
        f.backend
            .store()
            .insert_run(run, &f.workflow, 1, &Value::Null, "running")
            .await
            .unwrap();
        if run == "empty" {
            drop(EventLog::create(&f.root, run).await.unwrap());
        }
    }
    for _ in 0..2 {
        assert_eq!(
            flow_backend::recover_unfinished(&f.backend)
                .await
                .unwrap()
                .len(),
            2
        );
        for run in ["missing", "empty"] {
            let row = f.backend.store().get_run(run).await.unwrap();
            assert_eq!(row.status, "awaiting_resume");
            assert!(row.error.is_some());
            assert!(!f.backend.engine().is_live(run));
        }
    }
    assert!(!f.backend.engine().events_path("missing").exists());
    assert_eq!(
        std::fs::metadata(f.backend.engine().events_path("empty"))
            .unwrap()
            .len(),
        0
    );
}

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

async fn wait_human_waiting(f: &Fixture, run: &str) -> u64 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = f.backend.engine().snapshot(run).await.unwrap();
            if matches!(
                state.record("h").state,
                flow_engine::NodeState::Running { .. }
            ) {
                return state.last_seq;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("human_task 未进入等待")
}

/// 信号错误码契约（两后端同一组码；PG 侧在 ws_pg.rs 钉住落账语义）：
/// run 不存在 → -32011，run 已终结 → -32012，节点不等待信号 → -32010。
#[tokio::test]
async fn run_signal_error_codes_follow_the_shared_contract() {
    let f = Fixture::new().await;
    f.backend.store().publish(&f.workflow, 1).await.unwrap();

    let response = f
        .call(
            "run.signal",
            json!({"run_id": "nope", "node_id": "h", "payload": {}}),
        )
        .await;
    assert_eq!(response["error"]["code"], -32011, "{response}");

    let response = f
        .call("run.start", json!({"workflow_id": f.workflow}))
        .await;
    let run = response["result"]["run_id"].as_str().unwrap().to_string();
    f.wait_finished(&run).await;
    let response = f
        .call(
            "run.signal",
            json!({"run_id": run, "node_id": "h", "payload": {}}),
        )
        .await;
    assert_eq!(response["error"]["code"], -32012, "{response}");

    let wf = f.backend.store().create_workflow("human").await.unwrap();
    f.backend
        .store()
        .update_workflow(&wf, &human_def())
        .await
        .unwrap();
    f.backend.store().publish(&wf, 1).await.unwrap();
    let response = f.call("run.start", json!({"workflow_id": wf})).await;
    let run = response["result"]["run_id"].as_str().unwrap().to_string();
    wait_human_waiting(&f, &run).await;
    let response = f
        .call(
            "run.signal",
            json!({"run_id": run, "node_id": "bogus", "payload": {}}),
        )
        .await;
    assert_eq!(response["error"]["code"], -32010, "{response}");
}

/// 指定 run_id 的订阅契约（两后端共享 run_tail）：先回放完整日志、
/// 追实时增量、seq 从 1 严格连续，run 终态追平后流自然结束。
#[tokio::test]
async fn subscribe_with_run_id_replays_follows_and_naturally_ends() {
    use futures::StreamExt;

    let f = Fixture::new().await;
    let wf = f.backend.store().create_workflow("human").await.unwrap();
    f.backend
        .store()
        .update_workflow(&wf, &human_def())
        .await
        .unwrap();
    f.backend.store().publish(&wf, 1).await.unwrap();
    let response = f.call("run.start", json!({"workflow_id": wf})).await;
    let run = response["result"]["run_id"].as_str().unwrap().to_string();
    let until = wait_human_waiting(&f, &run).await;

    let stream = f.backend.subscribe(Some(run.clone()));
    futures::pin_mut!(stream);

    // 回放段：必须覆盖到等待点之前的全部事件
    let mut seqs: Vec<u64> = Vec::new();
    while (seqs.len() as u64) < until {
        let envelope = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("回放停滞")
            .expect("回放提前结束");
        assert_eq!(envelope.run_id, run);
        seqs.push(envelope.seq);
    }

    // 交付信号推进 run 到终态：流必须吐完增量并自然结束
    let response = f
        .call(
            "run.signal",
            json!({"run_id": run, "node_id": "h", "payload": {"ok": true}}),
        )
        .await;
    assert_eq!(response["result"]["delivered"], true, "{response}");

    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("订阅流未在终态后结束")
    {
        assert_eq!(envelope.run_id, run);
        seqs.push(envelope.seq);
    }

    assert_eq!(
        seqs,
        (1..=seqs.len() as u64).collect::<Vec<u64>>(),
        "订阅事件必须从 1 起严格连续"
    );
    let events = f.backend.read_events(&run, None).await.unwrap();
    let last = events.last().unwrap();
    assert_eq!(last.seq, *seqs.last().unwrap());
    assert!(matches!(
        last.event,
        flow_engine::Event::RunCompleted { .. }
    ));
}

#[tokio::test]
async fn workflow_versions_lists_desc_and_unknown_workflow_is_not_found() {
    let f = Fixture::new().await;
    // Fixture 自带 v1 draft（definition(7)）：发布后 v2 是新 draft
    f.backend.store().publish(&f.workflow, 1).await.unwrap();
    let v2 = f
        .backend
        .store()
        .update_workflow(&f.workflow, &definition(8))
        .await
        .unwrap();
    assert_eq!(v2, 2);

    let ok = f
        .call("workflow.versions", json!({"workflow_id": f.workflow}))
        .await;
    let versions = ok["result"]["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2, "{ok}");
    // 按 version 倒序：v2 draft 在前，v1 published 在后
    assert_eq!(versions[0]["version"], 2);
    assert_eq!(versions[0]["status"], "draft");
    assert_eq!(versions[1]["version"], 1);
    assert_eq!(versions[1]["status"], "published");
    // 元数据列齐全；definition 不在版本列表里（按需走 workflow.get）
    assert!(versions[0]["checksum"].is_string());
    assert!(versions[0]["created_at"].is_string());
    assert!(versions[0].get("definition").is_none());

    // 从未保存过版本的 workflow：空列表，不报错
    let empty = f.backend.store().create_workflow("empty").await.unwrap();
    let resp = f
        .call("workflow.versions", json!({"workflow_id": empty}))
        .await;
    assert_eq!(resp["result"]["versions"].as_array().unwrap().len(), 0);

    // workflow 不存在：与 workflow.get 同一语义 -32011
    let missing = f
        .call("workflow.versions", json!({"workflow_id": "nope"}))
        .await;
    assert_eq!(missing["error"]["code"], -32011);
}
