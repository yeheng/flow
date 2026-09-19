//! fix 回归：父 run 等待「初始化被中断」的子 run 时不得挂死。
//!
//! 崩溃窗口：LocalChildLauncher::start 在 insert_run 之后、engine.start_run
//! 首个事件落盘之前被杀。重启后 recover_unfinished 按 DESIGN §7.2 把
//! initializing + 空日志的 run 标 failed（不写事件）。父 run 重放 sub_workflow
//! 时撞 RunExists 附着等待——事件日志为空，snapshot 永远是 Running，
//! 修复前 await_terminal 死循环。修复后空日志时以 DB 投影为权威结束等待。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flow_engine::{Engine, RunPhase, StartRun};
use flow_rpc::child::LocalChildLauncher;
use flow_rpc::{AppState, StoreObserver};
use flow_store::Store;
use serde_json::{json, Value};
use uuid::Uuid;

struct Fixture {
    root: PathBuf,
    state: Arc<AppState>,
    workflow: String,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-child-await-{}", Uuid::now_v7()));
        let store = Arc::new(Store::open(root.join("flow.db")).await.unwrap());
        let workflow = store.create_workflow("test").await.unwrap();
        // 子工作流：start → end，输出固定值
        store
            .update_workflow(&workflow, &child_definition())
            .await
            .unwrap();
        store.publish(&workflow, 1).await.unwrap();
        let engine = Arc::new(Engine::new(
            &root,
            Arc::new(StoreObserver::new(store.clone())),
        ));
        engine.set_child_launcher(Arc::new(LocalChildLauncher::new(
            store.clone(),
            engine.clone(),
        )));
        Self {
            root,
            state: Arc::new(AppState { store, engine }),
            workflow,
        }
    }

    /// 构造崩溃残留：子 run 有 runs 行 + 空 event.jsonl，恢复流程已将其标 failed。
    async fn seed_interrupted_child(&self, child_run_id: &str) {
        self.state
            .store
            .insert_run(
                child_run_id,
                &self.workflow,
                1,
                &json!({"x": 1}),
                "initializing",
            )
            .await
            .unwrap();
        drop(
            flow_engine::EventLog::create(&self.root, child_run_id)
                .await
                .unwrap(),
        );
        // 走真实恢复路径：initializing + 空日志 → failed（不写事件）
        let failures = flow_rpc::recover_unfinished(&self.state).await.unwrap();
        assert!(failures.len() == 1, "残留子 run 应恢复失败：{failures:?}");
        let row = self.state.store.get_run(child_run_id).await.unwrap();
        assert_eq!(row.status, "failed");
    }

    async fn wait_terminal(&self, run_id: &str) -> flow_engine::RunState {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.state.engine.snapshot(run_id).await.unwrap();
                if state.phase.is_terminal() {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("run 在 5s 内到达终态（修复前空日志死循环会超时）")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn child_definition() -> Value {
    json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "e"}]
    })
}

/// 父定义：start → sub_workflow(child) → end。
/// child_run_id 是确定性的（{run_id}:{node_id}:{attempt}）。
fn parent_definition(workflow_id: &str) -> Value {
    json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "sub", "type": "sub_workflow", "params": {"workflow_id": workflow_id}},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "sub"},
            {"from": "sub", "to": "e"}
        ]
    })
}

#[tokio::test]
async fn parent_terminates_when_child_initialization_was_interrupted() {
    let f = Fixture::new().await;
    let run_id = format!("run-{}", Uuid::now_v7());
    let child_run_id = format!("{}:sub:1", run_id);
    f.seed_interrupted_child(&child_run_id).await;

    let definition: flow_engine::Definition =
        serde_json::from_value(parent_definition(&f.workflow)).unwrap();
    f.state
        .engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "parent".into(),
            workflow_version: 1,
            definition,
            input: json!({"x": 1}),
            depth: 0,
        })
        .await
        .unwrap();

    // 修复前：await_terminal 只看事件日志，空日志 → 永远 Running → 挂死超时
    let state = f.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Failed);
    let fatal = state.fatal_error.unwrap_or_default();
    assert!(
        fatal.contains(&child_run_id),
        "fatal 应指向被中断的子 run：{fatal}"
    );
}

#[tokio::test]
async fn healthy_child_run_still_succeeds() {
    // 对照：修复不影响正常路径——子 run 正常启动执行，父 run 成功
    let f = Fixture::new().await;
    let run_id = format!("run-{}", Uuid::now_v7());

    let definition: flow_engine::Definition =
        serde_json::from_value(parent_definition(&f.workflow)).unwrap();
    f.state
        .engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "parent".into(),
            workflow_version: 1,
            definition,
            input: json!({"x": 1}),
            depth: 0,
        })
        .await
        .unwrap();

    let state = f.wait_terminal(&run_id).await;
    assert_eq!(
        state.phase,
        RunPhase::Succeeded,
        "fatal={:?}",
        state.fatal_error
    );
}
