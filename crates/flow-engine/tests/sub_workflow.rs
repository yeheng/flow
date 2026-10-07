//! 子工作流（sub_workflow）的集成回归（DESIGN.md §6.6）。
//!
//! 覆盖：平台故障挂起而非判死、子输出透传、子失败 fatal、RunExists 附着、
//! 深度上限、取消级联、崩溃重放沿用同一 child_run_id、缺 workflow_id / 无
//! launcher 的快速失败。
//!
//! Harness / 定义构造器在 `common`（engine_recovery.rs / recovery_regressions.rs 同指一份）。

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{terminal, Harness};
use flow_engine::{
    ChildRunLauncher, ChildRunOutcome, DbRunStatus, Definition, Engine, EngineError, Event,
    EventLog, NodeState, NoopObserver, RunObserver, RunPhase, StartRun, StatusUpdate,
    MAX_SUB_WORKFLOW_DEPTH,
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
    /// 等待子 run 时遭遇基础设施故障（DB 不可用等）：应挂起 run 而非判死
    PlatformFault,
    Panic,
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
                Outcome::Panic => panic!("injected child launcher panic"),
                Outcome::PlatformFault => Err(EngineError::Backend("db 连接中断".into())),
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

/// 带 mock 启动器的 Harness：`set_child_launcher` 必须在任何 run 之前装好。
fn harness(launcher: MockLauncher) -> (Harness, Arc<MockLauncher>) {
    let h = Harness::new();
    let launcher = Arc::new(launcher);
    h.engine.set_child_launcher(launcher.clone());
    (h, launcher)
}

/// 平台故障分类（DESIGN §7）：等子 run 期间的基础设施故障必须挂起 run
///（投影 awaiting_resume），绝不写 run_failed 终态——那会把运维故障
/// 伪造成业务失败，恢复后也无法自动续跑。
#[tokio::test]
async fn launcher_platform_fault_suspends_run_instead_of_failing() {
    let root = std::env::temp_dir().join(format!("flow-subwf-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let statuses = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let engine = Engine::new(
        &root,
        Arc::new(RecordingObserver {
            statuses: statuses.clone(),
        }),
    );
    engine.set_child_launcher(Arc::new(MockLauncher::new(Outcome::PlatformFault)));

    let run_id = format!("run-{}", uuid::Uuid::now_v7());
    engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "parent-wf".into(),
            workflow_version: 1,
            definition: parent_def(),
            input: json!({"amount": 1}),
            depth: 0,
        })
        .await
        .unwrap();

    // Driver 挂起后自行退出（不写终态）
    tokio::time::timeout(Duration::from_secs(5), async {
        while engine.is_live(&run_id) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("driver 未退出");

    let state = engine.snapshot(&run_id).await.unwrap();
    assert!(!state.phase.is_terminal(), "平台故障不得写终态：{state:?}");
    let events = engine.read_events(&run_id, None).await.unwrap();
    assert!(
        events.iter().all(|e| !matches!(
            e.event,
            Event::RunFailed { .. } | Event::RunCompleted { .. } | Event::RunCancelled { .. }
        )),
        "日志里不得出现终态事件：{events:?}"
    );
    // 失败节点留 Running，恢复/接管时可附着既有子 run 重试
    assert!(
        matches!(state.record("sub").state, NodeState::Running { .. }),
        "sub_workflow 节点应留 Running：{:?}",
        state.record("sub")
    );

    let seen = statuses.lock().clone();
    assert!(
        seen.iter().any(|(s, _)| *s == DbRunStatus::AwaitingResume),
        "必须投影 awaiting_resume：{seen:?}"
    );
    assert!(
        !seen.iter().any(|(s, _)| *s == DbRunStatus::Failed),
        "不得投影 failed：{seen:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn input_mapping_feeds_computed_data_to_child_run() {
    // s → compute（script）→ sub（input_mapping）→ e：子 run 输入来自上游
    // 计算结果而非父 run 输入（DESIGN §6.8）。整值模板保留 JSON 类型。
    let (h, launcher) = harness(MockLauncher::new(Outcome::Succeed(json!({"ok": true}))));
    let def: Definition = serde_json::from_value(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "compute", "type": "script",
             "params": {"code": "return { total: input.amount * input.factor };"}},
            {"id": "sub", "type": "sub_workflow",
             "params": {
                 "workflow_id": "child-wf",
                 "input_mapping": {
                     "total": "${nodes.compute.total}",
                     "raw": "${input.amount}",
                     "job": "${input.order_id}"
                 }
             }},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "compute"},
            {"from": "compute", "to": "sub"},
            {"from": "sub", "to": "e"}
        ]
    }))
    .unwrap();
    def.validate().unwrap();

    let run_id = format!("run-{}", uuid::Uuid::now_v7());
    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "parent-wf".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 5, "factor": 3, "order_id": "o-9"}),
            depth: 0,
        })
        .await
        .unwrap();

    let state = terminal(&h.engine, &run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    let starts = launcher.starts.lock().unwrap().clone();
    assert_eq!(starts.len(), 1);
    assert_eq!(
        starts[0].2,
        json!({"total": 15, "raw": 5, "job": "o-9"}),
        "input_mapping 展开结果整体作为子 run 输入，数字保留类型"
    );
    assert_eq!(starts[0].1, "child-wf");
}

type StatusLog = Arc<parking_lot::Mutex<Vec<(DbRunStatus, Option<String>)>>>;

#[derive(Clone)]
struct RecordingObserver {
    statuses: StatusLog,
}

impl RunObserver for RecordingObserver {
    fn on_status<'a>(&'a self, update: StatusUpdate<'a>) -> BoxFuture<'a, ()> {
        self.statuses
            .lock()
            .push((update.status, update.error.map(str::to_string)));
        Box::pin(async {})
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
    let (h, launcher) = harness(MockLauncher::new(Outcome::Succeed(json!({"total": 42}))));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    // 子 run 输出透传为节点输出，再经 end 透传为 run 输出
    assert_eq!(state.record("sub").output(), Some(&json!({"total": 42})));
    assert_eq!(state.output, Some(json!({"total": 42})));

    // child_run_id 确定性派生并随 node_started 落盘；子 run 输入 = 父输入，深度 +1
    let child_run_id = "r:sub:1";
    assert_eq!(
        state.record("sub").child_run_id.as_deref(),
        Some(child_run_id)
    );
    let starts = launcher.starts.lock().unwrap().clone();
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
    let (h, _launcher) = harness(MockLauncher::new(Outcome::Fail("boom".into())));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    let state = terminal(&h.engine, "r").await;
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
    let (h, _launcher) = harness(MockLauncher::run_exists(Outcome::Succeed(json!("ok"))));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    assert_eq!(state.output, Some(json!("ok")));
}

#[tokio::test]
async fn depth_limit_fails_fast_without_starting_child() {
    let (h, launcher) = harness(MockLauncher::new(Outcome::Succeed(json!(1))));
    h.engine
        .start_run(spec("r", MAX_SUB_WORKFLOW_DEPTH))
        .await
        .unwrap();

    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Failed);
    let error = state.fatal_error.clone().unwrap();
    assert!(error.contains("嵌套"), "{error}");
    assert!(launcher.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancel_cascades_to_child_run() {
    let (h, launcher) = harness(MockLauncher::new(Outcome::Pending));
    h.engine.start_run(spec("r", 0)).await.unwrap();

    // 等子 run 已启动（node_started 落盘 + launcher.start 已调用）
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !launcher.starts.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    assert!(h.engine.cancel("r").await);
    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Cancelled);
    assert_eq!(launcher.cancels.lock().unwrap().as_slice(), &["r:sub:1"]);
}

#[tokio::test]
async fn crashed_sub_workflow_replays_and_reattaches_with_same_child_run_id() {
    let (h, launcher) = harness(MockLauncher::run_exists(Outcome::Succeed(json!("resumed"))));

    // 崩溃现场：s 已完成，sub 已 node_started（child_run_id 已落盘）但没有终态
    let mut log = EventLog::create(h.dir.path(), "r").await.unwrap();
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
            input: None,
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
            input: None,
        },
    ] {
        log.append("r", event).await.unwrap();
    }
    drop(log);

    h.engine.resume_run(spec("r", 0)).await.unwrap();
    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{:?}", state.fatal_error);
    assert_eq!(state.output, Some(json!("resumed")));
    // 重放沿用崩溃前落盘的 child_run_id（attempt 1 的 id），附着等待而不是另起子 run
    let starts = launcher.starts.lock().unwrap();
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

#[tokio::test]
async fn node_panic_releases_slot_and_fails_run() {
    let (h, _) = harness(MockLauncher::new(Outcome::Panic));
    h.engine.start_run(spec("panic", 0)).await.unwrap();
    let state = terminal(&h.engine, "panic").await;
    assert_eq!(state.phase, RunPhase::Failed);
    assert!(state.fatal_error.unwrap().contains("panicked"));
}
