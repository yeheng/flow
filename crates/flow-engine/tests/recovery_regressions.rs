use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flow_engine::{
    Definition, Engine, Event, EventLog, NodeState, NoopObserver, RunPhase, RunState, Signal,
    StartRun,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Harness {
    root: PathBuf,
    engine: Engine,
}

impl Harness {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-regression-{}", uuid::Uuid::now_v7()));
        Self {
            engine: Engine::new(&root, Arc::new(NoopObserver)),
            root,
        }
    }

    async fn terminal(&self) -> RunState {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.engine.snapshot("r").await.unwrap();
                if state.phase.is_terminal() && !self.engine.is_live("r") {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("run did not terminate")
    }

    async fn prefix(&self, events: Vec<Event>) {
        let mut log = EventLog::create(&self.root, "r").await.unwrap();
        for event in [
            Event::RunStarted {
                workflow_id: "w".into(),
                workflow_version: 1,
                input: Value::Null,
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
                output: Value::Null,
                duration_ms: 0,
            },
            Event::NodeStarted {
                node_id: "n".into(),
                attempt: 1,
                child_run_id: None,
            },
        ]
        .into_iter()
        .chain(events)
        {
            log.append("r", event).await.unwrap();
        }
    }

    async fn wait_started(&self, node: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    self.engine.snapshot("r").await.unwrap().record(node).state,
                    NodeState::Running { .. }
                ) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn definition(node: Value) -> Definition {
    serde_json::from_value(json!({
        "nodes": [{"id":"s", "type":"start"}, node, {"id":"e", "type":"end"}],
        "edges": [{"from":"s", "to":"n"}, {"from":"n", "to":"e"}]
    }))
    .unwrap()
}

fn spec(definition: Definition) -> StartRun {
    StartRun {
        run_id: "r".into(),
        workflow_id: "w".into(),
        workflow_version: 1,
        definition,
        input: Value::Null,
        depth: 0,
    }
}

async fn http_server(statuses: &[&str]) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let statuses: Vec<String> = statuses.iter().map(|status| status.to_string()).collect();
    let server = tokio::spawn(async move {
        for status in statuses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
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
    let state = h.terminal().await;
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
        let def = definition(json!({"id":"n", "type":"http_call", "params":{
            "url":url, "retry":{"max_attempts":2, "backoff_ms":backoff}
        }}));
        h.engine.start_run(spec(def)).await.unwrap();
        let state = h.terminal().await;
        server.await.unwrap();
        assert_eq!(state.phase, RunPhase::Succeeded);
        assert_eq!(state.record("n").attempts, 2);
        assert_eq!(state.record("e").attempts, 1);
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
        let def = definition(json!({"id":"n", "type":"http_call", "params":{
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

        let state = h.terminal().await;
        server.await.unwrap();
        assert_eq!(state.phase, RunPhase::Succeeded);
        assert_eq!(state.record("n").attempts, 2);
        assert_eq!(state.output.unwrap()["status"], 200);
    }
}

#[tokio::test]
async fn invalid_and_duplicate_human_signals_do_not_fail_run() {
    let h = Harness::new();
    h.engine
        .start_run(spec(definition(json!({"id":"n", "type":"human_task"}))))
        .await
        .unwrap();
    h.wait_started("n").await;
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
    let state = h.terminal().await;
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
        definition(json!({"id":"n", "type":"http_call", "params":{"url":"http://127.0.0.1:1"}}));
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
    let state = h.terminal().await;
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
        let def = definition(json!({"id":"n", "type":"http_call", "params":{"url":url}}));
        h.engine.resume_run(spec(def)).await.unwrap();
        let state = h.terminal().await;
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
            state.record("n").attempts,
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
