use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flow_engine::{
    ChildRunLauncher, ChildRunOutcome, Definition, Engine, EngineError, Event, EventLog, NodeState,
    NoopObserver, RunPhase, RunState, StartRun, MAX_SUB_WORKFLOW_DEPTH,
};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

/// 记录所有调用的 mock 启动器：start/await 行为按构造参数固定。
struct MockLauncher {
    starts: Mutex<Vec<(String, String, Value, u32)>>,
    cancels: Mutex<Vec<String>>,
    start_result: Result<(), String>,
    outcome: Outcome,
}

enum Outcome {
    Succeed(Value),
    Fail(String),
    /// 永不终态，直到 cancel（用于取消级联测试）
    Pending,
}

impl MockLauncher {
    fn new(outcome: Outcome) -> MockLauncher {
        MockLauncher {
            starts: Mutex::new(Vec::new()),
            cancels: Mutex::new(Vec::new()),
            start_result: Ok(()),
            outcome,
        }
    }

    fn run_exists(outcome: Outcome) -> MockLauncher {
        MockLauncher {
            start_result: Err("run 已存在".into()),
            ..MockLauncher::new(outcome)
        }
    }
}

impl ChildRunLauncher for MockLauncher {
    fn start<'a>(
        &'a self,
        child_run_id: &'a str,
        workflow_id: &'a str,
        input: Value,
        depth: u32,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        self.starts.lock().unwrap().push((
            child_run_id.to_string(),
            workflow_id.to_string(),
            input,
            depth,
        ));
        let result = match &self.start_result {
            Ok(()) => Ok(()),
            Err(e) => Err(EngineError::RunExists(e.clone())),
        };
        Box::pin(async move { result })
    }

    fn await_terminal<'a>(
        &'a self,
        _child_run_id: &'a str,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<ChildRunOutcome, EngineError>> {
        Box::pin(async move {
            match &self.outcome {
                Outcome::Succeed(value) => Ok(ChildRunOutcome::Succeeded(value.clone())),
                Outcome::Fail(error) => Ok(ChildRunOutcome::Failed(error.clone())),
                Outcome::Pending => {
                    cancel.cancelled().await;
                    Ok(ChildRunOutcome::Cancelled)
                }
            }
        })
    }

    fn cancel<'a>(&'a self, child_run_id: &'a str) -> BoxFuture<'a, ()> {
        self.cancels.lock().unwrap().push(child_run_id.to_string());
        Box::pin(async {})
    }
}

struct Harness {
    root: PathBuf,
    engine: Arc<Engine>,
    launcher: Arc<MockLauncher>,
}

impl Harness {
    fn new(launcher: MockLauncher) -> Harness {
        let root = std::env::temp_dir().join(format!("flow-subwf-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let launcher = Arc::new(launcher);
        let engine = Arc::new(Engine::new(&root, Arc::new(NoopObserver)));
        engine.set_child_launcher(launcher.clone());
        Harness {
            root,
            engine,
            launcher,
        }
    }

    async fn terminal(&self, run_id: &str) -> RunState {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.engine.snapshot(run_id).await.unwrap();
                if state.phase.is_terminal() && !self.engine.is_live(run_id) {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("run 未在 5s 内结束")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn parent_def() -> Definition {
    serde_json::from_value(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "sub", "type": "sub_workflow", "params": {"workflow_id": "child-wf"}},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "sub"}, {"from": "sub", "to": "e"}]
    }))
    .unwrap()
}

fn spec(run_id: &str, depth: u32) -> StartRun {
    StartRun {
        run_id: run_id.into(),
        workflow_id: "parent-wf".into(),
        workflow_version: 1,
        definition: parent_def(),
        input: json!({"amount": 5}),
        depth,
    }
}

#[tokio::test]
async fn child_success_output_passes_through() {
    let h = Harness::new(MockLauncher::new(Outcome::Succeed(json!({"total": 42}))));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    let state = h.terminal("r").await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    // 子 run 输出透传为节点输出，再经 end 透传为 run 输出
    assert_eq!(state.record("sub").output, Some(json!({"total": 42})));
    assert_eq!(state.output, Some(json!({"total": 42})));

    // child_run_id 确定性派生并随 node_started 落盘；子 run 输入 = 父输入，深度 +1
    let child_run_id = "r:sub:1";
    assert_eq!(state.record("sub").child_run_id.as_deref(), Some(child_run_id));
    let starts = h.launcher.starts.lock().unwrap().clone();
    assert_eq!(
        starts.as_slice(),
        &[(
            child_run_id.to_string(),
            "child-wf".to_string(),
            json!({"amount": 5}),
            1
        )]
    );
    let events = h.engine.read_events("r", None).await.unwrap();
    let started = events.iter().find_map(|env| match &env.event {
        Event::NodeStarted {
            node_id,
            child_run_id: Some(id),
            ..
        } if node_id == "sub" => Some(id.clone()),
        _ => None,
    });
    assert_eq!(started.as_deref(), Some(child_run_id));
}

#[tokio::test]
async fn child_failure_is_fatal_to_parent() {
    let h = Harness::new(MockLauncher::new(Outcome::Fail("boom".into())));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    let state = h.terminal("r").await;
    assert_eq!(state.phase, RunPhase::Failed);
    let error = state.fatal_error.clone().unwrap();
    assert!(error.contains("r:sub:1"), "{error}");
    assert!(error.contains("boom"), "{error}");
    assert!(matches!(
        state.record("sub").state,
        NodeState::Failed {
            retryable: false,
            ..
        }
    ));
    assert!(matches!(state.record("e").state, NodeState::Skipped { .. }));
}

#[tokio::test]
async fn start_run_exists_attaches_to_existing_child() {
    // 确定性 id 的幂等语义：start 撞 RunExists 时不报错，附着等待已有子 run
    let h = Harness::new(MockLauncher::run_exists(Outcome::Succeed(json!("ok"))));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    let state = h.terminal("r").await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    assert_eq!(state.output, Some(json!("ok")));
}

#[tokio::test]
async fn depth_limit_fails_fast_without_starting_child() {
    let h = Harness::new(MockLauncher::new(Outcome::Succeed(json!(1))));
    h.engine
        .start_run(spec("r", MAX_SUB_WORKFLOW_DEPTH))
        .await
        .unwrap();

    let state = h.terminal("r").await;
    assert_eq!(state.phase, RunPhase::Failed);
    let error = state.fatal_error.clone().unwrap();
    assert!(error.contains("嵌套"), "{error}");
    assert!(h.launcher.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancel_cascades_to_child_run() {
    let h = Harness::new(MockLauncher::new(Outcome::Pending));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    // 等子 run 已启动（node_started 落盘 + launcher.start 已调用）
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !h.launcher.starts.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    assert!(h.engine.cancel("r").await);
    let state = h.terminal("r").await;
    assert_eq!(state.phase, RunPhase::Cancelled);
    assert_eq!(h.launcher.cancels.lock().unwrap().as_slice(), &["r:sub:1"]);
}

#[tokio::test]
async fn crashed_sub_workflow_replays_and_reattaches_with_same_child_run_id() {
    let h = Harness::new(MockLauncher::run_exists(Outcome::Succeed(json!("resumed"))));

    // 崩溃现场：s 已完成，sub 已 node_started（child_run_id 已落盘）但没有终态
    let mut log = EventLog::create(&h.root, "r").await.unwrap();
    for event in [
        Event::RunStarted {
            workflow_id: "parent-wf".into(),
            workflow_version: 1,
            input: json!({"amount": 5}),
            depth: 0,
        },
        Event::NodeStarted {
            node_id: "s".into(),
            attempt: 1,
            child_run_id: None,
        },
        Event::NodeCompleted {
            node_id: "s".into(),
            attempt: 1,
            output: json!({"amount": 5}),
            duration_ms: 0,
        },
        Event::NodeStarted {
            node_id: "sub".into(),
            attempt: 1,
            child_run_id: Some("r:sub:1".into()),
        },
    ] {
        log.append("r", event).await.unwrap();
    }
    drop(log);

    h.engine.resume_run(spec("r", 0)).await.unwrap();
    let state = h.terminal("r").await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    assert_eq!(state.output, Some(json!("resumed")));
    // 重放沿用崩溃前落盘的 child_run_id（attempt 1 的 id），附着等待而不是另起子 run
    let starts = h.launcher.starts.lock().unwrap();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].0, "r:sub:1");
}

#[tokio::test]
async fn sub_workflow_requires_workflow_id() {
    let def: Definition = serde_json::from_value(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "sub", "type": "sub_workflow"},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "sub"}, {"from": "sub", "to": "e"}]
    }))
    .unwrap();
    let err = def.validate().unwrap_err();
    assert!(err.contains("workflow_id"), "{err}");
}

#[tokio::test]
async fn sub_workflow_without_launcher_fails_fast() {
    let root = std::env::temp_dir().join(format!("flow-subwf-nolauncher-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let engine = Engine::new(&root, Arc::new(NoopObserver));
    engine.start_run(spec("r", 0)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = engine.snapshot("r").await.unwrap();
            if state.phase.is_terminal() {
                assert_eq!(state.phase, RunPhase::Failed);
                assert!(
                    state.fatal_error.clone().unwrap().contains("启动器"),
                    "未配置启动器必须 fatal"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&root);
}
