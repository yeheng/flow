//! 引擎恢复语义的集成回归（DESIGN.md §3.2、§7）。
//!
//! 覆盖：双写者守卫、顺序事件、condition 分支跳过、纯节点重放、
//! 副作用节点等人工裁决、human_task 跨重启派信号、取消终态、skip 传播、
//! 多前驱输出收集、`nodes` 只暴露直接前驱、独立分支收尾。
//!
//! Harness / 定义构造器在 `common`（recovery_regressions.rs 与 sub_workflow.rs
//! 同指一份）。

mod common;

use std::time::Duration;

use common::{def_from, describe, linear_def, Harness};
use flow_engine::{
    Envelope, Event, EventLog, LogLevel, LogStream, NodeState, ResumeOutcome, RunPhase, RunState,
    Signal, StartRun,
};
use serde_json::{json, Value};

#[tokio::test]
async fn resume_while_live_is_a_no_op_single_writer_invariant() {
    let h = Harness::new();
    let run_id = Harness::run_id();
    // 1.5s delay 拉开恢复窗口：resume 时节点必在途（Running 或即将派发）
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "wait", "type": "delay", "params": {"ms": 1500}},
            {"id": "n4", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "wait"},
            {"from": "wait", "to": "n4"}
        ]
    }));
    let make_spec = || StartRun {
        run_id: run_id.clone(),
        workflow_id: "w1".into(),
        workflow_version: 1,
        definition: def.clone(),
        input: json!({}),
        depth: 0,
    };

    h.engine.start_run(make_spec()).await.unwrap();
    h.wait_live(&run_id).await;

    // 重复恢复：run 仍在被驱动，必须直接返回 Resumed 且不写任何事件
    let outcome = h.engine.resume_run(make_spec()).await.unwrap();
    assert!(matches!(outcome, ResumeOutcome::Resumed));

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    h.wait_not_live(&run_id).await;

    let events = h.engine.read_events(&run_id, None).await.unwrap();
    // seq 严格连续（双写者必然交错出重复/跳号）
    RunState::from_events(&events).unwrap();
    let wait_started = events
        .iter()
        .filter(|e| matches!(&e.event, Event::NodeStarted { node_id, .. } if node_id == "wait"))
        .count();
    assert_eq!(wait_started, 1, "delay 节点被执行了多次：{events:?}");
    let completed = events
        .iter()
        .filter(|e| matches!(e.event, Event::RunCompleted { .. }))
        .count();
    assert_eq!(completed, 1, "finalize 被执行了多次：{events:?}");
}

#[tokio::test]
async fn linear_run_executes_and_records_ordered_events() {
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = linear_def();

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21}),
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.outputs.get("n2"), Some(&json!({"doubled": 42})));
    // end 单前驱透传：线性链上 end 的前驱是 delay，所以 run 输出是 delay 的输出
    assert_eq!(state.output, Some(json!({"slept_ms": 10})));

    let events = h.engine.read_events(&run_id, None).await.unwrap();
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    // 节点日志（node_log）是可观察性事件，与状态事件同流；状态事件骨架不变
    let state_kinds: Vec<&str> = events
        .iter()
        .map(|e| e.event.kind())
        .filter(|k| *k != "node_log")
        .collect();
    assert_eq!(
        state_kinds,
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
    let run_id = Harness::run_id();
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
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.outputs.get("no"), Some(&json!("small")));
    match &state.record("yes").state {
        flow_engine::NodeState::Skipped { reason } => assert_eq!(reason, "branch_not_taken"),
        other => panic!("未走的分支应被跳过，实际：{other:?}"),
    }
    assert_eq!(state.outputs.get("end_no"), Some(&json!("small")));
    // 多 end：run 输出是各 end 输出的映射，被跳过的 end 为 null
    assert_eq!(
        state.output,
        Some(json!({"end_yes": null, "end_no": "small"}))
    );
}

#[tokio::test]
async fn restarted_pure_node_is_replayed_with_new_attempt() {
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = linear_def();
    // 崩溃现场：start 已完成，script 已 node_started 但没有终态
    h.craft_partial_log(&run_id, &["n1", "n2"]).await;
    // 补上 start 的终态，只留 script 悬空
    let mut log = EventLog::open(h.dir.path(), &run_id).await.unwrap();
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
            depth: 0,
        })
        .await
        .unwrap();
    assert!(matches!(outcome, ResumeOutcome::Resumed));

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(
        state.record("n2").attempts(),
        2,
        "残留的纯节点应以 attempt 2 重放"
    );
    assert_eq!(state.outputs.get("n2"), Some(&json!({"doubled": 42})));

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
    let run_id = Harness::run_id();
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

    h.craft_partial_log(&run_id, &["n1", "pay"]).await;
    let mut log = EventLog::open(h.dir.path(), &run_id).await.unwrap();
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
            depth: 0,
        })
        .await
        .unwrap();

    // 引擎不应自动重放副作用节点，而是挂起等待裁决
    tokio::time::sleep(Duration::from_millis(50)).await;
    let state = h.engine.snapshot(&run_id).await.expect("读取快照失败");
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
    assert_eq!(
        state.output,
        Some(json!({"status": 200, "body": {"ok": true}}))
    );
}

#[tokio::test]
async fn human_task_holds_run_until_signal_arrives() {
    let h = Harness::new();
    let run_id = Harness::run_id();
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
            depth: 0,
        })
        .await
        .unwrap();

    h.wait_live(&run_id).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let state = h.engine.snapshot(&run_id).await.expect("读取快照失败");
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
    let run_id = Harness::run_id();
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

    h.craft_partial_log(&run_id, &["n1", "approve"]).await;
    let mut log = EventLog::open(h.dir.path(), &run_id).await.unwrap();
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
            depth: 0,
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
    let run_id = Harness::run_id();
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
            depth: 0,
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
    let run_id = Harness::run_id();
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
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    for skipped in ["slow", "approve", "end_vip"] {
        assert!(
            matches!(
                state.record(skipped).state,
                flow_engine::NodeState::Skipped { .. }
            ),
            "{skipped} 应被跳过，实际：{:?}",
            state.record(skipped).state
        );
    }
    // end 透传前驱输出：false 分支的 end_std 拿到条件节点的求值结果（false）
    assert_eq!(
        state.output,
        Some(json!({"end_vip": null, "end_std": false}))
    );
}

#[tokio::test]
async fn multi_pred_end_collects_output_map() {
    // 多前驱 end：节点输出是各前驱输出的映射，而不是透传其中一个
    let h = Harness::new();
    let run_id = Harness::run_id();
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
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    // end 自身输出 = 多前驱映射；单 end 时 run 输出透传 end 的输出
    assert_eq!(state.outputs.get("e"), Some(&json!({"a": 1, "b": 2})));
    assert_eq!(state.output, Some(json!({"a": 1, "b": 2})));
}

#[tokio::test]
async fn nodes_scope_is_direct_predecessors_only() {
    // 回归（决定论输入面，DESIGN §10）：nodes 只暴露直接前驱的输出。
    // 菱形图里 c 的前驱是 a、b；引用非前驱节点 s 必须是 undefined，
    // 深层访问即 TypeError → fatal。若把全部已完成输出都塞进 nodes，
    // 非前驱引用会读到真值，破坏「重放结果与首次执行一致」。
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "script", "params": {"code": "return 1;"}},
            {"id": "b", "type": "script", "params": {"code": "return 2;"}},
            {"id": "c", "type": "script", "params": {"code": "return { v: nodes.s.deep };"}},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "a"},
            {"from": "s", "to": "b"},
            {"from": "a", "to": "c"},
            {"from": "b", "to": "c"},
            {"from": "c", "to": "e"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"deep": {"x": 1}}),
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Failed, "{}", describe(&state));
    assert!(
        state.fatal_error.as_deref().unwrap_or("").contains("c"),
        "fatal 应指向 c 节点，实际：{:?}",
        state.fatal_error
    );
}

#[tokio::test]
async fn nodes_scope_exposes_predecessor_outputs() {
    // 正向钉子：直接前驱的输出在 nodes 里可见（a、b 是 c 的前驱）
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "script", "params": {"code": "return 1;"}},
            {"id": "b", "type": "script", "params": {"code": "return 2;"}},
            {"id": "c", "type": "script", "params": {"code": "return { a: nodes.a, b: nodes.b };"}},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "a"},
            {"from": "s", "to": "b"},
            {"from": "a", "to": "c"},
            {"from": "b", "to": "c"},
            {"from": "c", "to": "e"}
        ]
    }));

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!(null),
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    assert_eq!(state.output, Some(json!({"a": 1, "b": 2})));
}

#[tokio::test]
async fn fatal_failure_lets_independent_branch_finish() {
    // 钉住 fatal 语义：致命失败只记录，不中断独立分支——
    // slow 必须跑到 Completed，失败分支的下游必须被跳过，run 结果为 Failed。
    let h = Harness::new();
    let run_id = Harness::run_id();
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
            depth: 0,
        })
        .await
        .unwrap();

    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Failed, "{}", describe(&state));
    assert!(
        matches!(
            state.record("slow").state,
            flow_engine::NodeState::Completed { .. }
        ),
        "独立分支必须跑完，实际：{:?}",
        state.record("slow").state
    );
    match &state.record("e1").state {
        flow_engine::NodeState::Skipped { reason } => {
            assert_eq!(reason, "upstream_failed")
        }
        other => panic!("失败分支下游应被跳过，实际：{other:?}"),
    }
    assert!(
        matches!(
            state.record("e2").state,
            flow_engine::NodeState::Completed { .. }
        ),
        "跑完的分支下游必须正常完成，实际：{:?}",
        state.record("e2").state
    );
    assert!(
        state
            .fatal_error
            .as_deref()
            .is_some_and(|e| e.contains("bad")),
        "fatal 必须记录首个失败节点：{:?}",
        state.fatal_error
    );
}

/// 可观察性（设计 §3/§4）：脚本 console 输出以 node_log 进事件流；
/// node_started 携带模板展开后的输入面快照（脱敏后）。
#[tokio::test]
async fn node_logs_and_input_snapshot_land_in_event_stream() {
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = serde_json::from_value(json!({
        "nodes": [
            {"id": "st", "type": "start", "params": {}},
            {"id": "s", "type": "script", "params": {
                "code": "console.log('hello', {from: 'js'});\nconsole.warn('careful');\nreturn input.amount * 2;",
                "note": "${input.amount}"
            }},
            {"id": "e", "type": "end", "params": {}}
        ],
        "edges": [{"from": "st", "to": "s"}, {"from": "s", "to": "e"}]
    }))
    .unwrap();

    h.engine
        .start_run(StartRun {
            run_id: run_id.clone(),
            workflow_id: "w1".into(),
            workflow_version: 1,
            definition: def,
            input: json!({"amount": 21, "token": "sk-secret"}),
            depth: 0,
        })
        .await
        .unwrap();
    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));

    let events = h.engine.read_events(&run_id, None).await.unwrap();

    // console.log → node_log（stdout / info），console.warn → warn
    let logs: Vec<&Envelope> = events
        .iter()
        .filter(|e| matches!(e.event, Event::NodeLog { .. }))
        .collect();
    assert!(
        logs.iter().any(|e| matches!(
            &e.event,
            Event::NodeLog { stream: LogStream::Stdout, level: LogLevel::Info, message, .. }
                if message.contains("hello") && message.contains("{\"from\":\"js\"}")
        )),
        "console.log 必须进事件流：{logs:?}"
    );
    assert!(logs.iter().any(|e| matches!(
        &e.event,
        Event::NodeLog { stream: LogStream::Stderr, level: LogLevel::Warn, message, .. }
            if message.contains("careful")
    )));

    // 输入面快照：模板展开 + 敏感键脱敏
    let started = events
        .iter()
        .find_map(|e| match &e.event {
            Event::NodeStarted { node_id, input, .. } if node_id == "s" => input.clone(),
            _ => None,
        })
        .expect("node_started 必带输入面");
    assert_eq!(started["note"], json!(21), "模板展开后的值");
    assert_eq!(started["code"], json!("console.log('hello', {from: 'js'});\nconsole.warn('careful');\nreturn input.amount * 2;"), "代码字段字节级不动");
    assert!(
        !started["code"]
            .as_str()
            .unwrap()
            .contains("${input.amount}"),
        "展开后不得残留模板"
    );
    // 输入面不包含 run input 本身（token 不在节点输入面，脱敏针对 params 键）
    let snapshot_input = state.record("s").input.clone().expect("fold 记录输入面");
    assert_eq!(snapshot_input["note"], json!(21));

    // 日志不改变投影：s 的状态由 started/completed 决定
    assert!(matches!(
        state.record("s").state,
        NodeState::Completed { .. }
    ));
}

/// 契约：日志已终结的 run 恢复时必须回 `AlreadyTerminal`，绝不回 `Resumed`。
///
/// `resume_run` 里有一条静默映射 `Err(RunExists) => Ok(Resumed)`（「已有人在驱动」
/// 的正常情形），它意味着**谎报「正在驱动」不会有任何日志痕迹**。本测试钉住
/// 正常路径下这个谎报不会发生：run 跑完 → resume → 如实报终态。
///
/// 注意：这不是 `reserve_run` 清理窗口的回归测试——该窗口实测窄到不可观测
/// （见 `Engine::reserve_run` 注释），构造不出失败用例；这里守的是契约本身。
#[tokio::test]
async fn resume_after_terminal_run_reports_terminal_not_fake_resumed() {
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "wait", "type": "delay", "params": {"ms": 20}},
            {"id": "n4", "type": "end"}
        ],
        "edges": [
            {"from": "n1", "to": "wait"},
            {"from": "wait", "to": "n4"}
        ]
    }));
    let make_spec = || StartRun {
        run_id: run_id.clone(),
        workflow_id: "w1".into(),
        workflow_version: 1,
        definition: def.clone(),
        input: json!({}),
        depth: 0,
    };

    h.engine.start_run(make_spec()).await.unwrap();
    let state = h.wait_terminal(&run_id).await;
    assert_eq!(state.phase, RunPhase::Succeeded, "{}", describe(&state));
    h.wait_not_live(&run_id).await;

    // 日志已终结：resume 必须据事件日志回 AlreadyTerminal，绝不谎称 Resumed
    //（谎称 = 告诉调用方「正在驱动」而实际没有 Driver）。
    let outcome = h.engine.resume_run(make_spec()).await.unwrap();
    assert!(
        matches!(outcome, ResumeOutcome::AlreadyTerminal(_)),
        "日志已终结的 run 恢复时必须回 AlreadyTerminal，实际：{outcome:?}"
    );
}

/// 契约：run 终态且 Driver 退出后，重复恢复是幂等无操作，不报错、不重复执行、
/// 不产生第二条终态事件。
#[tokio::test]
async fn repeated_resume_after_terminal_is_idempotent() {
    let h = Harness::new();
    let run_id = Harness::run_id();
    let def = def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "n4", "type": "end"}
        ],
        "edges": [{"from": "n1", "to": "n4"}]
    }));
    let spec = || StartRun {
        run_id: run_id.clone(),
        workflow_id: "w1".into(),
        workflow_version: 1,
        definition: def.clone(),
        input: json!({}),
        depth: 0,
    };

    h.engine.start_run(spec()).await.unwrap();
    h.wait_terminal(&run_id).await;
    h.wait_not_live(&run_id).await;

    // 不得因「注册位尚未摘除」而被拒成 RunExists（那会被 resume_run 吞成假 Resumed）。
    let outcome = h.engine.resume_run(spec()).await.unwrap();
    assert!(matches!(outcome, ResumeOutcome::AlreadyTerminal(_)));
}
