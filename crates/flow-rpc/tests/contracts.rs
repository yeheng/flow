use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flow_engine::{Engine, Event, EventLog, RunPhase};
use flow_rpc::{build_module, recover_unfinished, AppState, StoreObserver};
use flow_store::Store;
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
    state: Arc<AppState>,
    workflow: String,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-contracts-{}", uuid::Uuid::now_v7()));
        let store = Arc::new(Store::open(root.join("flow.db")).await.unwrap());
        let workflow = store.create_workflow("test").await.unwrap();
        store
            .update_workflow(&workflow, &definition(7))
            .await
            .unwrap();
        let engine = Arc::new(Engine::new(
            &root,
            Arc::new(StoreObserver::new(store.clone())),
        ));
        Self {
            root,
            state: Arc::new(AppState { store, engine }),
            workflow,
        }
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        let module = build_module(self.state.clone()).unwrap();
        let request =
            json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}).to_string();
        let (response, _) = module.raw_json_request(&request, 1).await.unwrap();
        serde_json::from_str(&response).unwrap()
    }

    async fn wait_finished(&self, run: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = self.state.store.get_run(run).await.unwrap();
                if matches!(row.status.as_str(), "succeeded" | "failed" | "cancelled")
                    && !self.state.engine.is_live(run)
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
    assert!(f.state.store.list_runs(None, 100).await.unwrap().is_empty());
    f.state.store.publish(&f.workflow, 1).await.unwrap();
    f.state
        .store
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
    f.state.store.publish(&f.workflow, 1).await.unwrap();
    for run in ["missing", "empty", "partial"] {
        f.state
            .store
            .insert_run(run, &f.workflow, 1, &Value::Null, "initializing")
            .await
            .unwrap();
        if run != "missing" {
            drop(EventLog::create(&f.root, run).await.unwrap());
        }
        if run == "partial" {
            tokio::fs::write(f.state.engine.events_path(run), b"{\"seq\":1")
                .await
                .unwrap();
        }
    }
    let failures = recover_unfinished(&f.state).await.unwrap();
    assert_eq!(failures.len(), 3);
    for run in ["missing", "empty", "partial"] {
        let row = f.state.store.get_run(run).await.unwrap();
        assert_eq!(row.status, "failed");
        assert!(row.error.is_some());
        assert!(!f.state.engine.is_live(run));
    }
    assert!(recover_unfinished(&f.state).await.unwrap().is_empty());
    assert!(!f.state.engine.events_path("missing").exists());
}

#[tokio::test]
async fn initialized_log_is_resumed_even_when_metadata_still_says_initializing() {
    let f = Fixture::new().await;
    f.state.store.publish(&f.workflow, 1).await.unwrap();
    f.state
        .store
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
    assert!(recover_unfinished(&f.state).await.unwrap().is_empty());
    let row = f.wait_finished("r").await;
    assert_eq!(row["status"], "succeeded");
    assert_eq!(row["output"], 7);
    assert_eq!(
        f.state.engine.snapshot("r").await.unwrap().phase,
        RunPhase::Succeeded
    );
}

#[tokio::test]
async fn missing_or_empty_logs_of_old_running_tasks_require_manual_recovery() {
    let f = Fixture::new().await;
    for run in ["missing", "empty"] {
        f.state
            .store
            .insert_run(run, &f.workflow, 1, &Value::Null, "running")
            .await
            .unwrap();
        if run == "empty" {
            drop(EventLog::create(&f.root, run).await.unwrap());
        }
    }
    for _ in 0..2 {
        assert_eq!(recover_unfinished(&f.state).await.unwrap().len(), 2);
        for run in ["missing", "empty"] {
            let row = f.state.store.get_run(run).await.unwrap();
            assert_eq!(row.status, "awaiting_resume");
            assert!(row.error.is_some());
            assert!(!f.state.engine.is_live(run));
        }
    }
    assert!(!f.state.engine.events_path("missing").exists());
    assert_eq!(
        std::fs::metadata(f.state.engine.events_path("empty"))
            .unwrap()
            .len(),
        0
    );
}
