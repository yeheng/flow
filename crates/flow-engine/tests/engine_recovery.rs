use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use flow_engine::{
    Definition, Engine, Event, EventLog, NoopObserver, ResumeOutcome, RunPhase, RunState, Signal,
    StartRun,
};
use serde_json::{json, Value};
use uuid::Uuid;

struct Harness {
    dir: PathBuf,
    engine: Arc<Engine>,
}

impl Harness {
    fn new() -> Harness {
        let dir = std::env::temp_dir().join(format!("flow-engine-test-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let engine = Arc::new(Engine::new(&dir, Arc::new(NoopObserver)));
        Harness { dir, engine }
    }

    fn run_id(&self) -> String {
        format!("run-{}", Uuid::now_v7())
    }

    async fn snapshot(&self, run_id: &str) -> RunState {
        self.engine.snapshot(run_id).await.unwrap()
    }

    async fn wait_terminal(&self, run_id: &str) -> RunState {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let state = self.snapshot(run_id).await;
            if state.phase.is_terminal() {
                return state;
            }
            assert!(std::time::Instant::now() < deadline, "run {run_id} 超时未结束");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_not_live(&self, run_id: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while self.engine.is_live(run_id) {
            assert!(std::time::Instant::now() < deadline, "run {run_id} 引擎任务未退出");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn wait_live(&self, run_id: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !self.engine.is_live(run_id) {
            assert!(std::time::Instant::now() < deadline, "run {run_id} 未进入运行态");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn describe(state: &RunState) -> String {
    format!(
        "phase={:?} fatal_error={:?} output={:?}",
        state.phase, state.fatal_error, state.output
    )
}

fn def_from(json: Value) -> Definition {
    serde_json::from_value(json).unwrap()
}

fn linear_def() -> Definition {
    def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start", "name": "输入"},
            {"id": "n2", "type": "script", "name": "计算", "params": {"code": "return { doubled: input.amount * 2 };"}},
            {"id": "n3", "type": "delay", "name": "等待", "params": {"ms": 10}},
            {"id": "n4", "type": "end", "name": "输出"}
        ],
        "edges": [
            {"from": "n1", "to": "n2"},
            {"from": "n2", "to": "n3"},
            {"from": "n3", "to": "n4"}
        ]
    }))
}

/// 手工写出一段「崩溃残留」的事件日志：模拟进程在节点执行中被杀死。
async fn craft_partial_log(dir: &Path, run_id: &str, nodes_started: &[&str]) {
    let mut log = EventLog::create(dir, run_id).await.unwrap();
    log.append(
        run_id,
        Event::RunStarted {
            workflow_id: "w1".into(),
            workflow_version: 1,
            input: json!({"amount": 21}),
        },
    )
    .await
    .unwrap();
    for node_id in nodes_started {
        log.append(
            run_id,
            Event::NodeStarted {
                node_id: (*node_id).to_string(),
                attempt: 1,
            },
        )
        .await
        .unwrap();
    }
    drop(log);
}

#[tokio::test]
async fn linear_run_executes_and_records_ordered_events() {
    let h = Harness::new();
    let run_id = h.run_id();
    let def = linear_def();

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21}),
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.record("n2").output, Some(json!({"doubled": 42})));
    // end 单前驱透传：线性链上 end 的前驱是 delay，所以 run 输出是 delay 的输出
    assert_eq!(state.output, Some(json!({"slept_ms": 10})));

    let events = h.engine.read_events(&run_id, None).await.unwrap();
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    let kinds: Vec<&str> = events.iter().map(|e| e.event.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            "run_started",
            "node_started",
            "node_completed",
            "node_started",
            "node_completed",
            "node_started",
            "node_completed",
            "node_started",
            "node_completed",
            "run_completed",
        ]
    );
    h.wait_not_live(&run_id).await;
}

#[tokio::test]
async fn condition_branch_marks_untaken_side_skipped() {
    let h = Harness::new();
    let run_id = h.run_id();
    // 两个分支各自收敛到独立的 end 节点
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "n2", "type": "condition", "params": {"expr": "input.amount > 100"}},
            {"id": "yes", "type": "script", "params": {"code": "return 'big';"}},
            {"id": "end_yes", "type": "end"},
            {"id": "no", "type": "script", "params": {"code": "return 'small';"}},
            {"id": "end_no", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "n2"},
            {"from": "n2", "to": "yes", "port": "true"},
            {"from": "n2", "to": "no", "port": "false"},
            {"from": "yes", "to": "end_yes"},
            {"from": "no", "to": "end_no"}
        ]
    }));
    def.validate().unwrap();

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 5}),
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.record("no").output, Some(json!("small")));
    match state.record("yes").state {
        flow_engine::NodeState::Skipped { reason } => assert_eq!(reason, "branch_not_taken"),
        other => panic!("未走的分支应被跳过，实际：{other:?}"),
    }
    assert_eq!(state.record("end_no").output, Some(json!("small")));
    // 多 end：run 输出是各 end 输出的映射，被跳过的 end 为 null
    assert_eq!(state.output, Some(json!({"end_yes": null, "end_no": "small"})));
}

#[tokio::test]
async fn restarted_pure_node_is_replayed_with_new_attempt() {
    let h = Harness::new();
    let run_id = h.run_id();
    let def = linear_def();
    // 崩溃现场：start 已完成，script 已 node_started 但没有终态
    craft_partial_log(&h.dir, &run_id, &["n1", "n2"]).await;
    // 补上 start 的终态，只留 script 悬空
    let mut log = EventLog::open(&h.dir, &run_id).await.unwrap();
    log.append(
        &run_id,
        Event::NodeCompleted {
            node_id: "n1".into(),
            attempt: 1,
            output: json!({"amount": 21}),
            duration_ms: 1,
        },
    )
    .await
    .unwrap();
    drop(log);

    let outcome = h
        .engine
        .resume_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21}),
        })
        .await
        .unwrap();
    assert_eq!(outcome, ResumeOutcome::Resumed);

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.record("n2").attempts, 2, "残留的纯节点应以 attempt 2 重放");
    assert_eq!(state.record("n2").output, Some(json!({"doubled": 42})));

    let events = h.engine.read_events(&run_id, None).await.unwrap();
    let replays = events
        .iter()
        .filter(|e| matches!(&e.event, Event::NodeStarted { node_id, attempt, .. } if node_id == "n2" && *attempt == 2))
        .count();
    assert_eq!(replays, 1);
}

#[tokio::test]
async fn side_effect_node_after_crash_waits_for_human_adjudication() {
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "pay", "type": "http_call", "params": {"method": "POST", "url": "http://127.0.0.1:1/pay"}},
            {"id": "n3", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "pay"},
            {"from": "pay", "to": "n3"}
        ]
    }));

    craft_partial_log(&h.dir, &run_id, &["n1", "pay"]).await;
    let mut log = EventLog::open(&h.dir, &run_id).await.unwrap();
    log.append(
        &run_id,
        Event::NodeCompleted {
            node_id: "n1".into(),
            attempt: 1,
            output: json!({"amount": 21}),
            duration_ms: 1,
        },
    )
    .await
    .unwrap();
    drop(log);

    h.engine
        .resume_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21}),
        })
        .await
        .unwrap();

    // 引擎不应自动重放副作用节点，而是挂起等待裁决
    tokio::time::sleep(Duration::from_millis(50)).await;
    let state = h.snapshot(&run_id).await;
    assert_eq!(state.phase, RunPhase::Running);
    assert!(matches!(
        state.record("pay").state,
        flow_engine::NodeState::Running { attempt: 1 }
    ));

    h.engine
        .signal(
            &run_id,
            Signal {
                node_id: "pay".into(),
                payload: json!({"action": "succeeded", "output": {"status": 200, "body": {"ok": true}}}),
            },
        )
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded);
    assert_eq!(state.output, Some(json!({"status": 200, "body": {"ok": true}})));
}

#[tokio::test]
async fn human_task_holds_run_until_signal_arrives() {
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "approve", "type": "human_task", "name": "审批"},
            {"id": "n3", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "approve"},
            {"from": "approve", "to": "n3"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21}),
        })
        .await
        .unwrap();

    h.wait_live(&run_id).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let state = h.snapshot(&run_id).await;
    assert_eq!(state.phase, RunPhase::Running, "等信号期间 run 不能结束");
    assert!(h.engine.is_live(&run_id));

    h.engine
        .signal(
            &run_id,
            Signal {
                node_id: "approve".into(),
                payload: json!({"approved_by": "alice"}),
            },
        )
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.output, Some(json!({"approved_by": "alice"})));
}

#[tokio::test]
async fn human_task_signal_recorded_before_crash_is_completed_on_resume() {
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "approve", "type": "human_task"},
            {"id": "n3", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "approve"},
            {"from": "approve", "to": "n3"}
        ]
    }));

    craft_partial_log(&h.dir, &run_id, &["n1", "approve"]).await;
    let mut log = EventLog::open(&h.dir, &run_id).await.unwrap();
    log.append(
        &run_id,
        Event::NodeCompleted {
            node_id: "n1".into(),
            attempt: 1,
            output: json!({"amount": 21}),
            duration_ms: 1,
        },
    )
    .await
    .unwrap();
    // 信号已落盘但终态丢失：崩溃在 signal_received 与 node_completed 之间
    log.append(
        &run_id,
        Event::SignalReceived {
            node_id: "approve".into(),
            payload: json!({"approved_by": "bob"}),
        },
    )
    .await
    .unwrap();
    drop(log);

    h.engine
        .resume_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21}),
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded);
    assert_eq!(state.output, Some(json!({"approved_by": "bob"})));
}

#[tokio::test]
async fn cancelled_run_is_terminal_and_stops_work() {
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "n2", "type": "delay", "params": {"ms": 60_000}},
            {"id": "n3", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "n2"},
            {"from": "n2", "to": "n3"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: Value::Null,
        })
        .await
        .unwrap();

    h.wait_live(&run_id).await;
    assert!(h.engine.cancel(&run_id).await);
    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Cancelled, "{}", describe(&state));
    h.wait_not_live(&run_id).await;
}

#[tokio::test]
async fn skip_propagates_through_multiple_downstream_levels() {
    // 回归：未走的分支上串联了多个节点（delay → human_task → end），
    // 跳过必须沿下游传递到底，不能把 run 判成「调度停滞」。
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "n2", "type": "condition", "params": {"expr": "input.amount > 100"}},
            {"id": "slow", "type": "delay", "params": {"ms": 10}},
            {"id": "approve", "type": "human_task"},
            {"id": "end_vip", "type": "end"},
            {"id": "end_std", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "n2"},
            {"from": "n2", "to": "slow", "port": "true"},
            {"from": "slow", "to": "approve"},
            {"from": "approve", "to": "end_vip"},
            {"from": "n2", "to": "end_std", "port": "false"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 5}),
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    for skipped in ["slow", "approve", "end_vip"] {
        assert!(
            matches!(state.record(skipped).state, flow_engine::NodeState::Skipped { .. }),
            "{skipped} 应被跳过，实际：{:?}",
            state.record(skipped).state
        );
    }
    // end 透传前驱输出：false 分支的 end_std 拿到条件节点的求值结果（false）
    assert_eq!(state.output, Some(json!({"end_vip": null, "end_std": false})));
}

#[tokio::test]
async fn multi_pred_end_collects_output_map() {
    // 多前驱 end：节点输出是各前驱输出的映射，而不是透传其中一个
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "script", "params": {"code": "return 1;"}},
            {"id": "b", "type": "script", "params": {"code": "return 2;"}},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "a"},
            {"from": "s", "to": "b"},
            {"from": "a", "to": "e"},
            {"from": "b", "to": "e"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!(null),
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    // end 自身输出 = 多前驱映射；单 end 时 run 输出透传 end 的输出
    assert_eq!(state.record("e").output, Some(json!({"a": 1, "b": 2})));
    assert_eq!(state.output, Some(json!({"a": 1, "b": 2})));
}

#[tokio::test]
async fn fatal_failure_lets_independent_branch_finish() {
    // 钉住 fatal 语义：致命失败只记录，不中断独立分支——
    // slow 必须跑到 Completed，失败分支的下游必须被跳过，run 结果为 Failed。
    let h = Harness::new();
    let run_id = h.run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "bad", "type": "script", "params": {"code": "return nope.x;"}},
            {"id": "slow", "type": "delay", "params": {"ms": 80}},
            {"id": "e1", "type": "end"},
            {"id": "e2", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "bad"},
            {"from": "s", "to": "slow"},
            {"from": "bad", "to": "e1"},
            {"from": "slow", "to": "e2"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!(null),
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Failed, "{}", describe(&state));
    assert!(
        matches!(state.record("slow").state, flow_engine::NodeState::Completed { .. }),
        "独立分支必须跑完，实际：{:?}",
        state.record("slow").state
    );
    match state.record("e1").state {
        flow_engine::NodeState::Skipped { reason } => {
            assert_eq!(reason, "upstream_failed")
        }
        other => panic!("失败分支下游应被跳过，实际：{other:?}"),
    }
    assert!(
        matches!(state.record("e2").state, flow_engine::NodeState::Completed { .. }),
        "跑完的分支下游必须正常完成，实际：{:?}",
        state.record("e2").state
    );
    assert!(
        state.fatal_error.as_deref().is_some_and(|e| e.contains("bad")),
        "fatal 必须记录首个失败节点：{:?}",
        state.fatal_error
    );
}
