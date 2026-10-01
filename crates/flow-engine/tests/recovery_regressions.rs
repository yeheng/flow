//! 失败恢复的集成回归（DESIGN.md §7）：fatal 失败保持终态、重试下游只跑一次、
//! 恢复后继续等满退避、非法/重复信号不判死 run、裁决恢复被消费。
//!
//! Harness / 定义构造器在 `common`（engine_recovery.rs / sub_workflow.rs 同指一份）。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{def_from, node_def, spec, terminal, Harness};
use flow_engine::{
    DbRunStatus, Engine, Event, NodeState, RunObserver, RunPhase, Signal, StatusUpdate,
};
use flow_test_support::io::TempDir;
use futures::future::BoxFuture;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 状态投影记录器：观察 run 状态被投影成什么、按什么顺序（投影一致性用例用）。
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

/// 本地 HTTP 桩：按给定状态行逐个回应（确定性，不依赖外部网络）。
async fn http_server(statuses: &[&str]) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let statuses: Vec<String> = statuses.iter().map(|status| status.to_string()).collect();
    let server = tokio::spawn(async move {
        for status in statuses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 4096];
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            let response =
                format!("HTTP/1.1 {status}\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (url, server)
}

#[tokio::test]
async fn recovered_fatal_failure_remains_failed_and_independent_branch_finishes() {
    let h = Harness::new();
    h.prefix(vec![
        Event::NodeFailed {
            node_id: "n".into(),
            attempt: 1,
            error: "fatal".into(),
            retryable: false,
        },
        Event::NodeStarted {
            node_id: "slow".into(),
            attempt: 1,
            child_run_id: None,
            input: None,
        },
    ])
    .await;
    let def = serde_json::from_value(json!({
        "nodes": [
            {"id":"s", "type":"start"},
            {"id":"n", "type":"script", "params":{"code":"throw new Error('fatal');"}},
            {"id":"slow", "type":"delay", "params":{"ms":20}},
            {"id":"e", "type":"end"}, {"id":"e2", "type":"end"}
        ],
        "edges": [{"from":"s", "to":"n"}, {"from":"n", "to":"e"},
                  {"from":"s", "to":"slow"}, {"from":"slow", "to":"e2"}]
    }))
    .unwrap();
    h.engine.resume_run(spec(def)).await.unwrap();
    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Failed);
    assert!(state.fatal_error.as_deref().unwrap().contains("fatal"));
    assert!(matches!(
        state.record("slow").state,
        NodeState::Completed { attempt: 2 }
    ));
    assert!(matches!(
        state.record("e2").state,
        NodeState::Completed { .. }
    ));
    assert!(matches!(state.record("e").state, NodeState::Skipped { .. }));
}

#[tokio::test]
async fn successful_retry_runs_downstream_once_with_and_without_backoff() {
    for backoff in [0, 30] {
        let h = Harness::new();
        let (url, server) = http_server(&["503 Service Unavailable", "200 OK"]).await;
        let def = node_def(json!({"id":"n", "type":"http_call", "params":{
            "url":url, "retry":{"max_attempts":2, "backoff_ms":backoff}
        }}));
        h.engine.start_run(spec(def)).await.unwrap();
        let state = terminal(&h.engine, "r").await;
        server.await.unwrap();
        assert_eq!(state.phase, RunPhase::Succeeded);
        assert_eq!(state.record("n").attempts(), 2);
        assert_eq!(state.record("e").attempts(), 1);
        assert_eq!(state.output.unwrap()["status"], 200);
        assert!(!h
            .engine
            .read_events("r", None)
            .await
            .unwrap()
            .iter()
            .any(|env| matches!(env.event, Event::NodeSkipped { .. })));
    }
}

/// 恢复时重建退避计时器。两条路径都等**满**退避：事件 `ts` 是写入者时钟
/// （SQLite=append 时 Utc::now()，PG=事务内 clock_timestamp()），当前进程的
/// Utc::now() 与之不同源，跨机器恢复时「剩余退避」的减法不可靠。
#[tokio::test]
async fn recovery_restores_retry_timer_and_waits_full_backoff() {
    for backoff_ms in [120, 400] {
        let h = Harness::new();
        let (url, server) = http_server(&["200 OK"]).await;
        h.prefix(vec![Event::NodeFailed {
            node_id: "n".into(),
            attempt: 1,
            error: "503".into(),
            retryable: true,
        }])
        .await;
        let def = node_def(json!({"id":"n", "type":"http_call", "params":{
            "url":url, "retry":{"max_attempts":2, "backoff_ms":backoff_ms}
        }}));
        h.engine.resume_run(spec(def)).await.unwrap();
        // 退避计时器重建：节点仍在重试窗口内，不得立即重放
        let state = h.engine.snapshot("r").await.unwrap();
        assert!(
            !state.record("n").state.is_terminal(),
            "恢复后不得立即重放（退避未到）"
        );
        assert_eq!(state.record("n").state.label(), "retrying");

        let state = terminal(&h.engine, "r").await;
        server.await.unwrap();
        assert_eq!(state.phase, RunPhase::Succeeded);
        assert_eq!(state.record("n").attempts(), 2);
        assert_eq!(state.output.unwrap()["status"], 200);
    }
}

#[tokio::test]
async fn invalid_and_duplicate_human_signals_do_not_fail_run() {
    let h = Harness::new();
    h.engine
        .start_run(spec(node_def(json!({"id":"n", "type":"human_task"}))))
        .await
        .unwrap();
    h.wait_node_running("n").await;
    let err = h
        .engine
        .signal(
            "r",
            Signal {
                node_id: "typo".into(),
                payload: Value::Null,
            },
        )
        .await;
    assert!(err.is_err());
    assert_eq!(
        h.engine.snapshot("r").await.unwrap().phase,
        RunPhase::Running
    );
    let signal = Signal {
        node_id: "n".into(),
        payload: json!({"approved":true}),
    };
    h.engine.signal("r", signal.clone()).await.unwrap();
    let events = h.engine.read_events("r", None).await.unwrap();
    assert!(events
        .iter()
        .any(|env| matches!(env.event, Event::SignalReceived { .. })));
    assert!(h.engine.signal("r", signal).await.is_err());
    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Succeeded);
    assert_eq!(state.output, Some(json!({"approved":true})));
    let events = h.engine.read_events("r", None).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|env| matches!(env.event, Event::SignalReceived { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn invalid_adjudication_keeps_node_waiting_for_a_valid_decision() {
    let h = Harness::new();
    h.prefix(vec![]).await;
    let def =
        node_def(json!({"id":"n", "type":"http_call", "params":{"url":"http://127.0.0.1:1"}}));
    h.engine.resume_run(spec(def)).await.unwrap();
    for payload in [
        json!({"action":"typo"}),
        json!({"action":"failed", "error":42}),
    ] {
        assert!(h
            .engine
            .signal(
                "r",
                Signal {
                    node_id: "n".into(),
                    payload
                }
            )
            .await
            .is_err());
        assert_eq!(
            h.engine.snapshot("r").await.unwrap().phase,
            RunPhase::Running
        );
    }
    h.engine
        .signal(
            "r",
            Signal {
                node_id: "n".into(),
                payload: json!({"action":"succeeded", "output":7}),
            },
        )
        .await
        .unwrap();
    let state = terminal(&h.engine, "r").await;
    assert_eq!(state.phase, RunPhase::Succeeded);
    assert_eq!(state.output, Some(json!(7)));
}

#[tokio::test]
async fn durable_adjudication_is_consumed_on_recovery() {
    for action in ["succeeded", "failed", "retry"] {
        let h = Harness::new();
        let (url, server) = http_server(if action == "retry" { &["200 OK"] } else { &[] }).await;
        h.prefix(vec![Event::SignalReceived {
            node_id: "n".into(),
            payload: json!({"action":action, "output":7, "error":"rejected"}),
        }])
        .await;
        let def = node_def(json!({"id":"n", "type":"http_call", "params":{"url":url}}));
        h.engine.resume_run(spec(def)).await.unwrap();
        let state = terminal(&h.engine, "r").await;
        server.await.unwrap();
        assert_eq!(
            state.phase,
            if action == "failed" {
                RunPhase::Failed
            } else {
                RunPhase::Succeeded
            }
        );
        assert_eq!(
            state.record("n").attempts(),
            if action == "retry" { 2 } else { 1 }
        );
        let events = h.engine.read_events("r", None).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|env| matches!(env.event, Event::SignalReceived { .. }))
                .count(),
            1
        );
    }
}

/// 幽灵节点（定义外 node_id）必须被恢复判成日志损坏，而不是被驱动。
///
/// 没有这条校验时那条幽灵记录停在 `Running`，而终止判定读的是两个不重合的
/// 键集：`plan()` 遍历 `definition.nodes`（看不见幽灵，于是不派发也不跳过），
/// `all_terminal()` 遍历 `records`（看见幽灵，于是恒假）。slots 空 +
/// `all_terminal()` 假 = 调度停滞，每次恢复都把 run 挂进 `awaiting_resume`，
/// 永远推不动——只能人工改数据。
#[tokio::test]
async fn node_outside_definition_is_rejected_on_recovery() {
    let h = Harness::new();
    // 日志里出现定义外的 ghost 节点，且它停在非终态（最容易触发挂死的那一档）
    h.prefix(vec![Event::NodeStarted {
        node_id: "ghost".into(),
        attempt: 1,
        child_run_id: None,
        input: None,
    }])
    .await;
    let def =
        node_def(json!({"id":"n", "type":"http_call", "params":{"url":"http://127.0.0.1:1"}}));

    let err = h.engine.resume_run(spec(def)).await.unwrap_err();
    assert!(
        matches!(err, flow_engine::EngineError::LogCorrupted(_)),
        "{err}"
    );
    assert!(err.to_string().contains("ghost"), "诊断要指名道姓：{err}");

    // 恢复被拒 → 没有 Driver 被拉起，run 原样停在日志末尾
    assert!(!h.engine.is_live("r"));
    let state = h.engine.snapshot("r").await.unwrap();
    assert_eq!(state.phase, RunPhase::Running);
    assert_eq!(state.record("ghost").attempts(), 1);
}

/// 已落盘的裁决载荷非法 = **日志完整性**问题，挂 `awaiting_resume` 等人工，
/// 绝不写成 `run_failed` 业务终态（与 §7.2 的分类原则同一条出口）。
#[tokio::test]
async fn corrupt_durable_adjudication_is_a_platform_fault_not_a_run_failure() {
    let h = Harness::new();
    h.prefix(vec![Event::SignalReceived {
        node_id: "n".into(),
        payload: json!({"action": "nonexistent"}),
    }])
    .await;
    let def =
        node_def(json!({"id":"n", "type":"http_call", "params":{"url":"http://127.0.0.1:1"}}));

    // resume_run 本身成功（分类只登记 Adjudicating 槽位），驱动在消费裁决时
    // 才发现载荷非法 —— 那时 run 已被投影成 awaiting_resume，不得被改写
    h.engine.resume_run(spec(def)).await.unwrap();
    h.wait_not_live("r").await;

    let state = h.engine.snapshot("r").await.unwrap();
    assert_eq!(
        state.phase,
        RunPhase::Running,
        "非法裁决不得写出 run_failed 终态：{state:?}"
    );
    let events = h.engine.read_events("r", None).await.unwrap();
    assert!(
        events.iter().all(|env| !env.event.is_run_terminal()),
        "日志里不该出现任何 run 终态事件：{events:?}"
    );
}

/// **先登记全部 Adjudicating 槽位、后消费全部裁决**——恢复期唯一的状态投影硬约束。
///
/// `apply_recovery_plan` 分两趟而不是一趟：`apply_signal` 消费掉一个裁决后会检查
/// 「还有没有 Adjudicating 槽位」，没有就投影回 `Running`。若登记与消费混在
/// 同一趟，先消费掉带裁决的那个节点时**另一个还没登记的待裁决节点**看不见，
/// 引擎误判「已无待裁决节点」把 run 投影成 `running`——而它此刻明明还挂在人工
/// 裁决上。观察者（`run.get` 的 status）能读到这段错误窗口。
///
/// 用例让 `n`（prefix 固定铺出的那个节点）带已落盘裁决、`n1` 不带：
/// 两趟实现只投影一次 `Running`（`drive` 开头那次）+ 一次 `AwaitingResume`；
/// 单趟实现在 HashMap 迭代顺序让 `n` 排在 `n1` 前时（8 轮里约一半）多投影一次
/// `Running`。断言落在「`Running` 只被投影过一次」，单趟实现 8 轮漏检约 0.4%。
#[tokio::test]
async fn awaiting_adjudication_never_projects_back_to_running() {
    for _ in 0..8 {
        let statuses: StatusLog = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let dir = TempDir::new("flow-engine-adjudication-projection");
        let engine = Arc::new(Engine::new(
            dir.path(),
            Arc::new(RecordingObserver {
                statuses: statuses.clone(),
            }),
        ));
        let h = Harness { dir, engine };
        h.prefix(vec![
            Event::NodeStarted {
                node_id: "n1".into(),
                attempt: 1,
                child_run_id: None,
                input: None,
            },
            Event::SignalReceived {
                node_id: "n".into(),
                payload: json!({"action": "succeeded", "output": 42}),
            },
        ])
        .await;
        // start → n1、start → n 两条独立副作用分支 → 各自 end
        let def = def_from(json!({
            "nodes": [
                {"id": "s", "type": "start"},
                {"id": "n1", "type": "http_call", "params": {"url": "http://127.0.0.1:1"}},
                {"id": "n", "type": "http_call", "params": {"url": "http://127.0.0.1:1"}},
                {"id": "e1", "type": "end"},
                {"id": "e2", "type": "end"}
            ],
            "edges": [
                {"from": "s", "to": "n1"}, {"from": "n1", "to": "e1"},
                {"from": "s", "to": "n"}, {"from": "n", "to": "e2"}
            ]
        }));
        h.engine.resume_run(spec(def)).await.unwrap();

        // 等 n 的已落盘裁决被消费完
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = h.engine.snapshot("r").await.unwrap();
                if matches!(state.record("n").state, NodeState::Completed { .. }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("n 的已落盘裁决未在 5s 内被消费");

        let seen = statuses.lock().clone();
        let running_count = seen
            .iter()
            .filter(|(status, _)| *status == DbRunStatus::Running)
            .count();
        assert_eq!(
            running_count, 1,
            "n1 仍在等人工裁决，run 不得被投影回 running（消费 n 的裁决时，\
             另一个 Adjudicating 槽位必须已经登记）：{seen:?}"
        );
        assert!(
            seen.iter()
                .any(|(status, _)| *status == DbRunStatus::AwaitingResume),
            "存在待裁决节点时必须投影 awaiting_resume：{seen:?}"
        );

        h.engine.cancel("r").await;
        h.wait_not_live("r").await;
    }
}
