//! inbox 信号的「早到」回归（DESIGN §6.7 + `flow-pg/src/gateway.rs`）：
//! 信号先落账、Driver 后接管（或 run.start 先于节点派发返回）时，
//! 目标 human_task 还没登记等待——此时拒绝会直接丢掉信号，run 永远等不到它。
//! Driver 必须跳过本次 poll：行留 pending，下一轮 poll 节点已等待即正常交付。
//!
//! 对照用例：目标节点永远不会等待（已跳过）——照常拒绝，反馈明确。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flow_engine::{
    spawn_driver, CommitOutcome, DbRunStatus, DriverSpec, Envelope, Event, PendingInput,
    PendingInputKind, RunEventSink, RunPhase, RunState,
};
use futures::future::BoxFuture;
use serde_json::json;
use tokio_util::sync::CancellationToken;

/// 早到的 inbox 信号：human_task 节点 h、payload 固定。
fn early_input() -> PendingInput {
    PendingInput {
        signal_id: "sig-early".into(),
        kind: PendingInputKind::Signal,
        node_id: Some("h".into()),
        payload: json!({ "by": "gateway" }),
    }
}

/// 可观测状态（与 sink 共享，测试断言用）。
#[derive(Default)]
struct Shared {
    events: Mutex<Vec<Envelope>>,
    applied: AtomicBool,
    rejected: Mutex<Vec<String>>,
    seq: AtomicU64,
}

/// 内存 sink：事件全记录、fold 可回放；poll_inputs 恒投递 early_input
/// （直到被 commit_signal 消费——真实 inbox 由 applied 状态行保证）。
struct ObservableSink {
    shared: Arc<Shared>,
}

impl ObservableSink {
    fn new() -> (ObservableSink, Arc<Shared>) {
        let shared = Arc::new(Shared::default());
        (
            ObservableSink {
                shared: shared.clone(),
            },
            shared,
        )
    }

    fn commit(&self, event: Event) -> Envelope {
        let seq = self.shared.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let envelope = Envelope {
            seq,
            ts: chrono::Utc::now(),
            run_id: "r-1".into(),
            event,
        };
        self.shared.events.lock().unwrap().push(envelope.clone());
        envelope
    }
}

impl RunEventSink for ObservableSink {
    fn append<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
        Box::pin(async move { Ok(self.commit(event)) })
    }

    fn append_terminal<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
        Box::pin(async move { Ok(self.commit(event)) })
    }

    fn project_status<'a>(
        &'a mut self,
        _status: DbRunStatus,
        _error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn poll_inputs<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<Vec<PendingInput>, flow_engine::EngineError>> {
        Box::pin(async {
            // 真实 inbox 的语义：行离开 pending（applied / rejected）就不再投递
            let consumed = self.shared.applied.load(Ordering::SeqCst)
                || !self.shared.rejected.lock().unwrap().is_empty();
            if consumed {
                return Ok(Vec::new());
            }
            Ok(vec![early_input()])
        })
    }

    fn commit_signal<'a>(
        &'a mut self,
        _input: &'a PendingInput,
        event: Event,
    ) -> BoxFuture<'a, Result<CommitOutcome, flow_engine::EngineError>> {
        Box::pin(async move {
            if self.shared.applied.swap(true, Ordering::SeqCst) {
                return Ok(CommitOutcome::Duplicate);
            }
            let envelope = self.commit(event);
            Ok(CommitOutcome::Applied(envelope))
        })
    }

    fn reject_signal<'a>(
        &'a mut self,
        input: &'a PendingInput,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
        Box::pin(async move {
            self.shared
                .rejected
                .lock()
                .unwrap()
                .push(format!("{}: {reason}", input.signal_id));
            Ok(())
        })
    }

    fn consume_cancel<'a>(
        &'a mut self,
        _input: &'a PendingInput,
    ) -> BoxFuture<'a, Result<CommitOutcome, flow_engine::EngineError>> {
        Box::pin(async { Err(flow_engine::EngineError::Node("测试不取消".into())) })
    }

    fn release<'a>(&'a mut self) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
        Box::pin(async { Ok(()) })
    }
}

impl Shared {
    fn state(&self) -> RunState {
        RunState::from_events(&self.events.lock().unwrap()).expect("事件可折叠")
    }

    fn rejected(&self) -> Vec<String> {
        self.rejected.lock().unwrap().clone()
    }

    fn applied(&self) -> bool {
        self.applied.load(Ordering::SeqCst)
    }
}

fn spec_with(sink: ObservableSink, definition: flow_engine::Definition) -> flow_engine::DriverSpec {
    DriverSpec {
        run_id: "r-1".into(),
        definition: Arc::new(definition),
        input: json!({}),
        depth: 0,
        child_launcher: None,
        sink: Box::new(sink),
        events_tx: None,
        cancel: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
        signal_rx: None,
        inbox_poll: Duration::from_millis(10),
    }
}

/// 信号在 Driver 启动前就已入 inbox：第一个 inbox poll 时 human_task 尚未
/// 登记等待。当场拒绝会让 run 永久挂起，所以必须跳过本次 poll，
/// 下一轮（节点已等待）正常交付，run 成功且 output = 信号 payload。
#[tokio::test]
async fn inbox_signal_before_human_wait_is_deferred_then_delivered() {
    let definition: flow_engine::Definition = serde_json::from_value(json!({
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "h", "type": "human_task", "params": {"prompt": "审批"}},
            {"id": "end", "type": "end"}
        ],
        "edges": [
            {"from": "start", "to": "h"},
            {"from": "h", "to": "end"}
        ]
    }))
    .unwrap();

    let (sink, shared) = ObservableSink::new();
    let handle = spawn_driver(
        spec_with(sink, definition),
        RunState::default(),
        Default::default(),
    );

    // 必须走到「消费」而不是「拒绝」：两条路都让循环退出，下面分开断言
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("Driver 未在 5s 内结束")
        .expect("Driver 任务异常");

    assert!(
        shared.rejected().is_empty(),
        "早到信号绝不允许被拒绝（丢了信号 run 将永远挂起）：{:?}",
        shared.rejected()
    );
    assert!(shared.applied(), "信号最终必须被消费");
    let state = shared.state();
    assert_eq!(state.phase, RunPhase::Succeeded, "{state:?}");
    assert_eq!(state.output, Some(json!({ "by": "gateway" })));
    assert!(matches!(
        state.record("h").state,
        flow_engine::NodeState::Completed { .. }
    ));
}

/// 对照：信号目标是永远不会等待的节点（所在分支已跳过，别处还有长分支在跑）——
/// 早到跳过只是一时，节点判 Skipped 后照常拒绝，给客户端明确反馈。
#[tokio::test]
async fn inbox_signal_for_never_waiting_node_is_still_rejected() {
    let definition: flow_engine::Definition = serde_json::from_value(json!({
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "cond", "type": "condition", "params": {"expr": "true"}},
            {"id": "d", "type": "delay", "params": {"ms": 300}},
            {"id": "h", "type": "human_task"},
            {"id": "end_a", "type": "end"},
            {"id": "end_b", "type": "end"}
        ],
        "edges": [
            {"from": "start", "to": "cond"},
            {"from": "cond", "to": "d", "port": "true"},
            {"from": "cond", "to": "h", "port": "false"},
            {"from": "d", "to": "end_a"},
            {"from": "h", "to": "end_b"}
        ]
    }))
    .unwrap();

    let (sink, shared) = ObservableSink::new();
    let handle = spawn_driver(
        spec_with(sink, definition),
        RunState::default(),
        Default::default(),
    );
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("Driver 未在 5s 内结束")
        .expect("Driver 任务异常");

    let rejected = shared.rejected();
    assert_eq!(
        rejected.len(),
        1,
        "跳过分支的 human_task 永不等信号：{rejected:?}"
    );
    assert!(rejected[0].contains("sig-early"), "{rejected:?}");
    assert!(rejected[0].contains("不等待信号"), "{rejected:?}");
    let state = shared.state();
    assert_eq!(state.phase, RunPhase::Succeeded);
}
