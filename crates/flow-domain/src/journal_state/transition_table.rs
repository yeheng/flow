//! 节点与 run 生命周期迁移表：把 docs/design/03-single-node-execution.md
//! §6.1/§6.2 的状态图编码为期望表，与真实折叠（I5 唯一实现）穷尽对拍。
//!
//! 每个用例按「形态构建 → 探针事件 → 分类」运行：Err 为拒绝，Ok 后按
//! node.status / run.status 的可观测变化分为迁移与无状态变化。拒绝断言
//! 守卫错误片段，把「守卫拒绝」与「载荷非法」区分开。
//!
//! 边界：权限族事件（PermissionAsked/Decided、RunApproval*）由
//! permission_fold_tests 覆盖；工作流 CRUD、值发布、命令回执与调度
//! 水位不属于节点/run 状态机，不入表。新增事件种类属于生命周期语义时
//! 必须在此表登记，穷尽匹配会强制回答每个旧形态对新事件的行为。

use super::*;
use serde_json::json;

/// 被测形态：节点 n 在 run r 中的状态（个别形态同时固定 run 终态）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    /// 从未派发：run 内没有节点 n 的记录。
    Absent,
    /// 裁决 retry 后回炉（经过 Uncertain 全链）。
    Pending,
    /// 在飞（已派发 + 已准备输入）。
    Running,
    WaitDelay,
    WaitSignal,
    WaitChild,
    WaitRetry,
    /// Uncertain 等待；run 处于 AwaitingResume。
    WaitUncertain,
    Succeeded,
    Failed,
    Skipped,
    /// run 已取消，节点仍在飞：迟到结果的「证据照记」形态。
    CancelledRunning,
    /// run 已取消，节点仍在等待：迟到解除的无操作形态。
    CancelledWaiting,
}

const ALL_SHAPES: &[Shape] = &[
    Shape::Absent,
    Shape::Pending,
    Shape::Running,
    Shape::WaitDelay,
    Shape::WaitSignal,
    Shape::WaitChild,
    Shape::WaitRetry,
    Shape::WaitUncertain,
    Shape::Succeeded,
    Shape::Failed,
    Shape::Skipped,
    Shape::CancelledRunning,
    Shape::CancelledWaiting,
];

/// 探针事件（载荷按形态取合法值，保证能抵达状态机守卫）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Probe {
    Dispatch,
    Prepare,
    Complete,
    Fail,
    FailRetry,
    Skip,
    RegisterDelay,
    Resolve,
    Signal,
    AdjAccept,
    AdjRetry,
    AdjFail,
    RunComplete,
    RunFail,
    RunCancel,
}

const ALL_PROBES: &[Probe] = &[
    Probe::Dispatch,
    Probe::Prepare,
    Probe::Complete,
    Probe::Fail,
    Probe::FailRetry,
    Probe::Skip,
    Probe::RegisterDelay,
    Probe::Resolve,
    Probe::Signal,
    Probe::AdjAccept,
    Probe::AdjRetry,
    Probe::AdjFail,
    Probe::RunComplete,
    Probe::RunFail,
    Probe::RunCancel,
];

/// 期望分类：拒绝（含守卫错误片段）、节点迁移、节点+run 同时迁移、
/// 仅 run 迁移、接受但无状态变化（收件箱/证据入账等非状态效应）。
#[derive(PartialEq, Eq, Debug)]
enum Exp {
    Reject(&'static str),
    Node(&'static str),
    NodeRun(&'static str, &'static str),
    Run(&'static str),
    NoStatus,
}

/// 实测分类：与 [`Exp`] 同构，但持有拥有的字符串。
#[derive(PartialEq, Eq, Debug)]
enum Act {
    Reject(String),
    Node(String),
    NodeRun(String, String),
    Run(String),
    NoStatus,
}

struct Harness {
    state: State,
    lsn: u64,
}

impl Harness {
    fn apply(&mut self, events: Vec<Event>) -> Result<()> {
        let tx = Transaction {
            v: 2,
            journal_id: "table".into(),
            lsn: self.lsn + 1,
            tx_id: format!("tx-{}", self.lsn + 1),
            events,
        };
        self.state.apply(&tx)?;
        self.lsn += 1;
        Ok(())
    }

    fn seq(&self) -> u64 {
        self.state.runs.get("r").map_or(1, |r| r.last_run_seq + 1)
    }

    fn node_status(&self) -> Option<&'static str> {
        self.state
            .runs
            .get("r")
            .and_then(|r| r.nodes.get("n"))
            .map(|n| n.status.as_str())
    }

    fn run_status(&self) -> Option<&'static str> {
        self.state.runs.get("r").map(|r| r.status.as_str())
    }
}

fn inline(value: Value) -> StoredValue {
    StoredValue::Inline(value)
}

fn fingerprint(value: &Value) -> String {
    flow_facts::codec::digest(
        &flow_facts::codec::bounded_json(value, flow_facts::INLINE_BYTES).unwrap(),
    )
}

fn run_event(kind: EventKind, seq: u64, payload: Value) -> Event {
    let mut event = Event::new(kind, payload);
    event.run_id = Some("r".into());
    event.run_seq = seq;
    event
}

fn node_event(kind: EventKind, seq: u64, dispatch: &str, payload: Value) -> Event {
    let mut event = run_event(kind, seq, payload);
    event.node_id = Some("n".into());
    event.dispatch_id = Some(dispatch.into());
    event
}

/// 当前 attempt 的审计序号：结果事件的 sealed_through 必须等于它。
/// 节点缺失（Absent）或无 attempt 账本（Skipped）时取 0——探针会在
/// 更早的守卫处被拒，不会走到审计屏障。
fn sealed_through(harness: &Harness) -> String {
    let Some(node) = harness.state.runs.get("r").and_then(|r| r.nodes.get("n")) else {
        return "0".into();
    };
    node.attempts
        .get(&node.dispatch_id)
        .map_or_else(|| "0".into(), |attempt| attempt.audit_seq.to_string())
}

/// 只有 RunStarted 的基座：节点 n 尚无记录（Absent 形态）。
fn fresh_run() -> Harness {
    let mut harness = Harness {
        state: State::default(),
        lsn: 0,
    };
    let run: Run = serde_json::from_value(json!({
        "run_id":"r", "workflow_id":"w", "workflow_version":1,
        "definition": inline(Value::Null), "input": inline(Value::Null),
        "source":"manual", "source_detail":null, "parent":null, "depth":0,
        "created_at":"now", "status":"running", "output":null, "error":null,
        "last_run_seq":"0", "nodes":{}
    }))
    .unwrap();
    harness
        .apply(vec![run_event(
            EventKind::RunStarted,
            1,
            serde_json::to_value(&run).unwrap(),
        )])
        .unwrap();
    harness
}

/// 在飞：已派发 + 已准备输入（一切节点形态的共同前缀）。
fn dispatched() -> Harness {
    let mut harness = fresh_run();
    harness
        .apply(vec![node_event(
            EventKind::DispatchStarted,
            harness.seq(),
            "d1",
            json!({"node_execution_id":"x","attempt":"1"}),
        )])
        .unwrap();
    let mut prepared = node_event(
        EventKind::InputPrepared,
        0,
        "d1",
        json!({"prepared": {
            "input": inline(Value::Null), "predecessors": {},
            "params": inline(json!({})), "engine_build": "b", "node_semantics": 2
        }}),
    );
    prepared.audit_seq = 1;
    harness.apply(vec![prepared]).unwrap();
    harness
}

/// 在 Running 基础上登记一个未决外部操作（Uncertain 裁决的前提）。
/// 意图与授权必须同事务原子成对、载荷逐字一致（折叠层规则）。
fn with_pending_operation(mut harness: Harness) -> Harness {
    let request = json!({"k":1});
    let operation = json!({
        "operation_id":"op1", "permit_id":"p1",
        "fingerprint": fingerprint(&request),
        "request": inline(request), "outcome": null
    });
    let seq = harness.seq();
    harness
        .apply(vec![
            node_event(
                EventKind::OperationIntent,
                seq,
                "d1",
                json!({"operation": operation}),
            ),
            node_event(
                EventKind::OperationAuthorized,
                seq + 1,
                "d1",
                json!({"operation": operation}),
            ),
        ])
        .unwrap();
    harness
}

fn register_wait(harness: &mut Harness, wait: Value) {
    let sealed = sealed_through(harness);
    harness
        .apply(vec![node_event(
            EventKind::WaitRegistered,
            harness.seq(),
            "d1",
            json!({"wait": wait, "sealed_through": sealed}),
        )])
        .unwrap();
}

fn state_for(shape: Shape) -> Harness {
    let mut harness = dispatched();
    match shape {
        Shape::Absent => return fresh_run(),
        Shape::Running => {}
        Shape::WaitDelay => register_wait(&mut harness, json!({"kind":"delay","wake_at":123})),
        Shape::WaitSignal => register_wait(&mut harness, json!({"kind":"signal"})),
        Shape::WaitChild => register_wait(&mut harness, json!({"kind":"child","child_run_id":"c"})),
        Shape::WaitRetry => {
            let sealed = sealed_through(&harness);
            harness
                .apply(vec![node_event(
                    EventKind::NodeFailed,
                    harness.seq(),
                    "d1",
                    json!({"error":"boom","sealed_through":sealed,"retry_wake_at":456}),
                )])
                .unwrap();
        }
        Shape::WaitUncertain => {
            harness = with_pending_operation(harness);
            register_wait(&mut harness, json!({"kind":"uncertain"}));
        }
        Shape::Pending => {
            harness = with_pending_operation(harness);
            register_wait(&mut harness, json!({"kind":"uncertain"}));
            harness
                .apply(vec![node_event(
                    EventKind::Adjudicated,
                    harness.seq(),
                    "d1",
                    json!({"operation_id":"op1","reason":"external ok",
                           "decision":"retry","output":inline(Value::Null)}),
                )])
                .unwrap();
        }
        Shape::Succeeded => {
            let sealed = sealed_through(&harness);
            harness
                .apply(vec![node_event(
                    EventKind::NodeCompleted,
                    harness.seq(),
                    "d1",
                    json!({"output": inline(json!({"ok":true})), "sealed_through": sealed}),
                )])
                .unwrap();
        }
        Shape::Failed => {
            let sealed = sealed_through(&harness);
            harness
                .apply(vec![node_event(
                    EventKind::NodeFailed,
                    harness.seq(),
                    "d1",
                    json!({"error":"boom","sealed_through":sealed}),
                )])
                .unwrap();
        }
        Shape::Skipped => {
            // skip 只作用于从未派发的节点：从 fresh_run 构造。
            let mut skipped = fresh_run();
            skipped
                .apply(vec![node_event(
                    EventKind::NodeSkipped,
                    skipped.seq(),
                    "d0",
                    json!({"reason":"upstream_failed"}),
                )])
                .unwrap();
            return skipped;
        }
        Shape::CancelledRunning | Shape::CancelledWaiting => {
            if shape == Shape::CancelledWaiting {
                register_wait(&mut harness, json!({"kind":"delay","wake_at":123}));
            }
            harness
                .apply(vec![run_event(
                    EventKind::RunCancelled,
                    harness.seq(),
                    json!({"reason":"test"}),
                )])
                .unwrap();
        }
    }
    harness
}

/// 探针的派发上下文按形态取值：结果族必须匹配节点当前 dispatch（否则
/// 死在陈旧派发守卫而非被测守卫），重派发 attempt 取当前值 +1。
/// 节点缺失或派发为空（Skipped）时取合法占位：探针会在节点查找或
/// 陈旧派发守卫处被拒，占位不改变分类。
fn node_dispatch(harness: &Harness) -> String {
    harness
        .state
        .runs
        .get("r")
        .and_then(|r| r.nodes.get("n"))
        .map(|n| n.dispatch_id.clone())
        .filter(|dispatch| !dispatch.is_empty())
        .unwrap_or_else(|| "d1".into())
}

fn next_attempt(harness: &Harness) -> u64 {
    harness
        .state
        .runs
        .get("r")
        .and_then(|r| r.nodes.get("n"))
        .map_or(1, |n| n.attempt as u64 + 1)
}

fn probe_event(harness: &Harness, probe: Probe) -> Event {
    let seq = harness.seq();
    let dispatch = node_dispatch(harness);
    match probe {
        Probe::Dispatch => node_event(
            EventKind::DispatchStarted,
            seq,
            "d2",
            json!({"node_execution_id":"x","attempt": next_attempt(harness).to_string()}),
        ),
        Probe::Prepare => node_event(
            EventKind::InputPrepared,
            0,
            &dispatch,
            json!({"prepared": {
                "input": inline(Value::Null), "predecessors": {},
                "params": inline(json!({})), "engine_build": "b", "node_semantics": 2
            }}),
        ),
        Probe::Complete => node_event(
            EventKind::NodeCompleted,
            seq,
            &dispatch,
            json!({"output": inline(json!({"ok":true})),
                   "sealed_through": sealed_through(harness)}),
        ),
        Probe::Fail | Probe::FailRetry => {
            let mut payload = json!({"error":"late","sealed_through":sealed_through(harness)});
            if probe == Probe::FailRetry {
                payload["retry_wake_at"] = json!(789);
            }
            node_event(EventKind::NodeFailed, seq, &dispatch, payload)
        }
        Probe::Skip => node_event(
            EventKind::NodeSkipped,
            seq,
            "d0",
            json!({"reason":"upstream_failed"}),
        ),
        Probe::RegisterDelay => node_event(
            EventKind::WaitRegistered,
            seq,
            &dispatch,
            json!({"wait":{"kind":"delay","wake_at":9},
                   "sealed_through": sealed_through(harness)}),
        ),
        Probe::Resolve => node_event(
            EventKind::WaitResolved,
            seq,
            &dispatch,
            json!({"output": inline(json!({"ok":true}))}),
        ),
        Probe::Signal => node_event(
            EventKind::SignalReceived,
            seq,
            &dispatch,
            json!({"queued":true,"payload": inline(json!({"v":1}))}),
        ),
        Probe::AdjAccept | Probe::AdjRetry | Probe::AdjFail => {
            let decision = match probe {
                Probe::AdjAccept => "accept_output",
                Probe::AdjRetry => "retry",
                _ => "failed",
            };
            node_event(
                EventKind::Adjudicated,
                seq,
                &dispatch,
                json!({"operation_id":"op1","reason":"manual",
                       "decision":decision,"output": inline(json!({"ok":true}))}),
            )
        }
        Probe::RunComplete => run_event(
            EventKind::RunCompleted,
            seq,
            json!({"output": inline(Value::Null)}),
        ),
        Probe::RunFail => run_event(EventKind::RunFailed, seq, json!({"error":"late"})),
        Probe::RunCancel => run_event(EventKind::RunCancelled, seq, json!({"reason":"late"})),
    }
}

fn classify(base: &Harness, probe: Probe) -> Act {
    let mut harness = Harness {
        state: base.state.clone(),
        lsn: base.lsn,
    };
    let (node_before, run_before) = (base.node_status(), base.run_status());
    match harness.apply(vec![probe_event(base, probe)]) {
        Err(error) => Act::Reject(error.to_string()),
        Ok(()) => {
            let node_after = harness.node_status();
            let run_after = harness.run_status();
            match (node_before != node_after, run_before != run_after) {
                (true, true) => Act::NodeRun(
                    node_after.unwrap_or("<none>").into(),
                    run_after.unwrap_or("<none>").into(),
                ),
                (true, false) => Act::Node(node_after.unwrap_or("<none>").into()),
                (false, true) => Act::Run(run_after.unwrap_or("<none>").into()),
                (false, false) => Act::NoStatus,
            }
        }
    }
}

/// 期望表：规格源是 §6.1/§6.2 的状态图，按写者现实修正——
/// child 失败从 Waiting{Child} 写 NodeFailed（finish_run），
/// Retry/Approval 等待可重派发（到期重试、会话丢失），
/// Skipped 的补准备被 G5 拒绝。改动折叠层迁移语义时此表必须同步。
fn expected(shape: Shape, probe: Probe) -> Exp {
    use Probe::*;
    use Shape::*;
    // Skipped 节点派发为空：携带派发上下文的事件一律死在陈旧派发守卫。
    const STALE: Exp = Exp::Reject("stale dispatch cannot alter current node");
    match probe {
        // —— 派发：fresh/Pending/在飞重派（会话丢失恢复）/Retry|Approval
        //     等待可派；终态节点与其余等待形态是状态矛盾 ——
        Dispatch => match shape {
            Absent | Pending | WaitRetry => Exp::Node("running"),
            // 在飞重派：状态不变，attempt/dispatch 账本承载差异。
            Running => Exp::NoStatus,
            CancelledRunning | CancelledWaiting => Exp::Reject("dispatch on terminal run"),
            _ => Exp::Reject("unsafe dispatch retry"),
        },
        // —— 输入准备：一次且仅在飞；终态补准备是状态矛盾 ——
        Prepare => match shape {
            Absent => Exp::Reject("missing node dispatch"),
            Skipped => STALE,
            _ => Exp::Reject("input already prepared"),
        },
        // —— 结果族：只来自 Running；child 失败是唯一例外 ——
        Complete => match shape {
            Absent => Exp::Reject("missing node dispatch"),
            Skipped => STALE,
            Running => Exp::Node("succeeded"),
            CancelledRunning => Exp::NoStatus,
            _ => Exp::Reject("result for node not in flight"),
        },
        Fail => match shape {
            Absent => Exp::Reject("missing node dispatch"),
            Skipped => STALE,
            Running => Exp::Node("failed"),
            WaitChild => Exp::Node("failed"),
            CancelledRunning => Exp::NoStatus,
            _ => Exp::Reject("result for node not in flight"),
        },
        FailRetry => match shape {
            Absent => Exp::Reject("missing node dispatch"),
            Skipped => STALE,
            Running => Exp::Node("waiting"),
            // child 等待失败转重试等待：状态同为 waiting，只换等待种类。
            WaitChild => Exp::NoStatus,
            CancelledRunning => Exp::NoStatus,
            _ => Exp::Reject("result for node not in flight"),
        },
        // —— 跳过只作用于从未派发的节点（I17 审计账本保护） ——
        Skip => match shape {
            Absent => Exp::Node("skipped"),
            _ => Exp::Reject("cannot skip dispatched node"),
        },
        RegisterDelay => match shape {
            Absent => Exp::Reject("missing node dispatch"),
            Skipped => STALE,
            Running => Exp::Node("waiting"),
            CancelledRunning => Exp::NoStatus,
            _ => Exp::Reject("result for node not in flight"),
        },
        // —— 解除：只有 Delay/Signal/Child 走 WaitResolved（§6.2/§6.3） ——
        Resolve => match shape {
            Absent => Exp::Reject("missing node dispatch"),
            Skipped => STALE,
            WaitDelay | WaitSignal | WaitChild => Exp::Node("succeeded"),
            CancelledWaiting => Exp::NoStatus,
            _ => Exp::Reject("wait resolution without active wait"),
        },
        // —— 信号只进收件箱：不触碰状态；终态 run 拒收 ——
        Signal => match shape {
            CancelledRunning | CancelledWaiting => Exp::Reject("signal on terminal run"),
            _ => Exp::NoStatus,
        },
        // —— 裁决只属于 Uncertain：三向决策都把 run 拉回 Running ——
        AdjAccept => match shape {
            Skipped => STALE,
            WaitUncertain => Exp::NodeRun("succeeded", "running"),
            Absent => Exp::Reject("missing node dispatch"),
            _ => Exp::Reject("manual resolution requires the current uncertain operation"),
        },
        AdjRetry => match shape {
            Skipped => STALE,
            WaitUncertain => Exp::NodeRun("pending", "running"),
            Absent => Exp::Reject("missing node dispatch"),
            _ => Exp::Reject("manual resolution requires the current uncertain operation"),
        },
        AdjFail => match shape {
            Skipped => STALE,
            WaitUncertain => Exp::NodeRun("failed", "running"),
            Absent => Exp::Reject("missing node dispatch"),
            _ => Exp::Reject("manual resolution requires the current uncertain operation"),
        },
        // —— run 收口：AwaitingResume 必须先裁决；终态完成是矛盾、
        //     终态失败/取消是无操作（幂等不对称，§6.1） ——
        RunComplete => match shape {
            WaitUncertain => Exp::Reject("completion requires adjudication"),
            CancelledRunning | CancelledWaiting => Exp::Reject("terminal run completion"),
            _ => Exp::Run("succeeded"),
        },
        RunFail => match shape {
            WaitUncertain => Exp::Reject("failure requires adjudication"),
            CancelledRunning | CancelledWaiting => Exp::NoStatus,
            _ => Exp::Run("failed"),
        },
        RunCancel => match shape {
            CancelledRunning | CancelledWaiting => Exp::NoStatus,
            _ => Exp::Run("cancelled"),
        },
    }
}

fn assert_cell(shape: Shape, probe: Probe) {
    let base = state_for(shape);
    let actual = classify(&base, probe);
    let expect = expected(shape, probe);
    let ok = match (&actual, &expect) {
        (Act::Reject(error), Exp::Reject(fragment)) => error.contains(fragment),
        (Act::Node(actual), Exp::Node(expect)) => actual == expect,
        (Act::NodeRun(an, ar), Exp::NodeRun(en, er)) => an == en && ar == er,
        (Act::Run(actual), Exp::Run(expect)) => actual == expect,
        (Act::NoStatus, Exp::NoStatus) => true,
        _ => false,
    };
    assert!(
        ok,
        "shape {shape:?} x probe {probe:?}: expected {expect:?}, got {actual:?}"
    );
}

/// 穷尽对拍：13 个形态 × 15 个探针，全部走真实 State::apply（I5）。
/// 新增生命周期事件或形态时，穷尽匹配会强制在此登记期望。
#[test]
fn transition_table_agrees_with_fold() {
    for shape in ALL_SHAPES {
        for probe in ALL_PROBES {
            assert_cell(*shape, *probe);
        }
    }
}

/// 形态自检：构建器必须真的到达目标形态，否则整张表在测错误的东西。
#[test]
fn shape_builders_reach_target_states() {
    use Shape::*;
    for shape in ALL_SHAPES {
        let harness = state_for(*shape);
        let expect_node = match shape {
            Absent => None,
            Pending => Some("pending"),
            Running | CancelledRunning => Some("running"),
            WaitDelay | WaitSignal | WaitChild | WaitRetry | WaitUncertain | CancelledWaiting => {
                Some("waiting")
            }
            Succeeded => Some("succeeded"),
            Failed => Some("failed"),
            Skipped => Some("skipped"),
        };
        assert_eq!(harness.node_status(), expect_node, "shape {shape:?}");
        let expect_run = match shape {
            WaitUncertain => "awaiting_resume",
            CancelledRunning | CancelledWaiting => "cancelled",
            _ => "running",
        };
        assert_eq!(harness.run_status(), Some(expect_run), "shape {shape:?}");
    }
}
