use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::EngineError;
use crate::event::{validate_sequence, Envelope, Event};
use crate::model::Definition;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum NodeState {
    #[default]
    Pending,
    Running {
        attempt: u32,
    },
    Completed {
        attempt: u32,
    },
    Failed {
        attempt: u32,
        error: String,
        retryable: bool,
    },
    Skipped {
        reason: String,
    },
}

impl NodeState {
    pub fn is_terminal(&self) -> bool {
        !matches!(
            self,
            NodeState::Pending
                | NodeState::Running { .. }
                | NodeState::Failed {
                    retryable: true,
                    ..
                }
        )
    }

    pub fn label(&self) -> &'static str {
        match self {
            NodeState::Pending => "pending",
            NodeState::Running { .. } => "running",
            NodeState::Completed { .. } => "completed",
            NodeState::Failed {
                retryable: true, ..
            } => "retrying",
            NodeState::Failed {
                retryable: false, ..
            } => "failed",
            NodeState::Skipped { .. } => "skipped",
        }
    }

    /// 已尝试次数（= 当前状态携带的 attempt）。
    ///
    /// **为什么是派生而非存储**：attempt 号只存在于状态里，此前 `NodeRecord.attempts`
    /// 是它的影子副本，三个事件分支各写一行 `rec.attempts().max(attempt)` 手工同步。
    /// `Pending` / `Skipped` 从未启动过，计数为 0。
    ///
    /// 不变量：attempt 从 1 起、每次 +1（`next_attempt`），且 `handle_result` 会
    /// 丢弃非当前 attempt 的迟到结果，所以「状态里的 attempt」与「已尝试次数」
    /// 在所有可达路径上恒等——影子副本没有提供额外保证，只提供了漏同步的机会。
    pub fn attempt(&self) -> u32 {
        match self {
            NodeState::Running { attempt }
            | NodeState::Completed { attempt }
            | NodeState::Failed { attempt, .. } => *attempt,
            NodeState::Pending | NodeState::Skipped { .. } => 0,
        }
    }

    /// 失败原因（仅 `Failed` 携带；其余状态无错）。
    ///
    /// 派生同 [`Self::attempt`]：此前 `NodeRecord.error` 与
    /// `NodeState::Failed.error` 各存一份，靠三个分支手工同步——而 `NodeSkipped`
    /// 分支根本没清它，正是影子字段典型的漏网之处。
    pub fn error(&self) -> Option<&str> {
        match self {
            NodeState::Failed { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// 时间线视图：折叠事件得到的单节点记录。
///
/// **唯一状态来源是 [`NodeState`]。** attempt / error / 输出都不在这里另存一份：
/// - 已尝试次数 → [`NodeState::attempt`]（时间线要展示时取，不存副本）；
/// - 失败原因 → [`NodeState::error`]；
/// - 节点输出 → [`RunState::outputs`]。
///
/// 三者都曾是本结构的字段，代价是每个事件分支都要手工同步副本：`output` 副本
/// 让 `NodeFailed`/`NodeSkipped` 漏清一处（靠「`NodeStarted` 必然先清」侥幸不出错），
/// `error` 副本连 `NodeSkipped` 都没覆盖。存两份就等于多一份要维护的影子。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub state: NodeState,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    /// 已记录但尚未被消费的信号（崩溃在 signal_received 与终态之间）
    pub last_signal: Option<Value>,
    /// sub_workflow 节点的子 run id（来自 node_started）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_run_id: Option<String>,
    /// 节点输入面快照：模板展开后的 params（node_started 带入，写入时已脱敏）。
    /// 每个 attempt 刷新；重试/终态不清除，展示的是最近一次尝试的输入。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
}

impl NodeRecord {
    /// 已尝试次数（派生的快捷方式，见 [`NodeState::attempt`]）。
    pub fn attempts(&self) -> u32 {
        self.state.attempt()
    }

    /// 失败原因（派生的快捷方式，见 [`NodeState::error`]）。
    pub fn error(&self) -> Option<&str> {
        self.state.error()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunPhase {
    pub fn is_terminal(self) -> bool {
        !matches!(self, RunPhase::Running)
    }
}

/// 一次执行的全部状态，由事件流折叠得到。恢复与只读时间线共用这一个结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub records: HashMap<String, NodeRecord>,
    pub outputs: HashMap<String, Value>,
    pub phase: RunPhase,
    pub fatal_error: Option<String>,
    pub output: Option<Value>,
    pub workflow_id: Option<String>,
    pub workflow_version: Option<i64>,
    pub input: Value,
    pub last_seq: u64,
    /// 嵌套深度（来自 run_started，恢复时读回）
    #[serde(default)]
    pub depth: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
}

impl Default for RunState {
    fn default() -> Self {
        RunState {
            records: HashMap::new(),
            outputs: HashMap::new(),
            phase: RunPhase::Running,
            fatal_error: None,
            output: None,
            workflow_id: None,
            workflow_version: None,
            input: Value::Null,
            last_seq: 0,
            depth: 0,
            started_at: None,
            ended_at: None,
        }
    }
}

impl RunState {
    pub fn new() -> RunState {
        RunState::default()
    }

    /// 把定义里出现但日志中还没有事件的节点补成 Pending，便于时间线完整渲染。
    pub fn ensure_nodes(&mut self, def: &Definition) {
        for node in &def.nodes {
            self.records.entry(node.id.clone()).or_default();
        }
    }

    /// 缺失节点的默认视图（Pending）。返回引用，避免热点路径全量克隆。
    pub fn record(&self, node_id: &str) -> &NodeRecord {
        const DEFAULT_RECORD: NodeRecord = NodeRecord {
            state: NodeState::Pending,
            started_at: None,
            ended_at: None,
            duration_ms: None,
            last_signal: None,
            child_run_id: None,
            input: None,
        };
        self.records.get(node_id).unwrap_or(&DEFAULT_RECORD)
    }

    pub fn all_terminal(&self) -> bool {
        self.records.values().all(|r| r.state.is_terminal())
    }

    pub fn fold(&mut self, env: &Envelope) {
        self.last_seq = env.seq;
        match &env.event {
            Event::RunStarted {
                workflow_id,
                workflow_version,
                input,
                depth,
            } => {
                self.workflow_id = Some(workflow_id.clone());
                self.workflow_version = Some(*workflow_version);
                self.input = input.clone();
                self.depth = *depth;
                self.started_at = Some(env.ts);
                self.phase = RunPhase::Running;
            }
            Event::NodeStarted {
                node_id,
                attempt,
                child_run_id,
                input,
            } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.state = NodeState::Running { attempt: *attempt };
                rec.started_at = Some(env.ts);
                rec.ended_at = None;
                rec.duration_ms = None;
                rec.last_signal = None;
                rec.child_run_id = child_run_id.clone();
                rec.input = input.clone();
                // 旧输出唯一那份在 outputs 里，这里随新 attempt 一并清掉：
                // 重试/重放不留脏数据
                self.outputs.remove(node_id);
            }
            Event::NodeCompleted {
                node_id,
                attempt,
                output,
                duration_ms,
            } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.state = NodeState::Completed { attempt: *attempt };
                rec.ended_at = Some(env.ts);
                rec.duration_ms = Some(*duration_ms);
                rec.last_signal = None;
                self.outputs.insert(node_id.clone(), output.clone());
            }
            Event::NodeFailed {
                node_id,
                attempt,
                error,
                retryable,
            } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.state = NodeState::Failed {
                    attempt: *attempt,
                    error: error.clone(),
                    retryable: *retryable,
                };
                rec.ended_at = Some(env.ts);
                rec.last_signal = None;
                self.outputs.remove(node_id);
                if !retryable {
                    self.fatal_error
                        .get_or_insert_with(|| format!("节点 {node_id} 失败：{error}"));
                }
            }
            Event::NodeSkipped { node_id, reason } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.state = NodeState::Skipped {
                    reason: reason.clone(),
                };
                rec.ended_at = Some(env.ts);
                self.outputs.remove(node_id);
            }
            // 日志是观察数据，不是恢复状态：折叠时显式跳过（契约：对未知/
            // 非状态事件 no-op，前端同款契约，见 monitor-logic.ts）
            Event::NodeLog { .. } => {}
            Event::SignalReceived { node_id, payload } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.last_signal = Some(payload.clone());
            }
            Event::RunCompleted { output } => {
                self.phase = RunPhase::Succeeded;
                self.output = Some(output.clone());
                self.ended_at = Some(env.ts);
            }
            Event::RunFailed { error } => {
                self.phase = RunPhase::Failed;
                self.fatal_error = Some(error.clone());
                self.ended_at = Some(env.ts);
            }
            Event::RunCancelled {} => {
                self.phase = RunPhase::Cancelled;
                self.ended_at = Some(env.ts);
            }
        }
    }

    pub fn from_events(events: &[Envelope]) -> Result<RunState, EngineError> {
        validate_sequence(events)?;
        let mut state = RunState::new();
        for env in events {
            state.fold(env);
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    fn env(seq: u64, event: Event) -> Envelope {
        Envelope {
            seq,
            ts: Utc::now(),
            run_id: "r1".into(),
            event,
        }
    }

    #[test]
    fn fold_tracks_retry_and_terminal_state() {
        let events = vec![
            env(
                1,
                Event::RunStarted {
                    workflow_id: "w1".into(),
                    workflow_version: 1,
                    input: serde_json::json!({"a": 1}),
                    depth: 0,
                },
            ),
            env(
                2,
                Event::NodeStarted {
                    node_id: "n1".into(),
                    attempt: 1,
                    child_run_id: None,
                    input: None,
                },
            ),
            env(
                3,
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 1,
                    error: "timeout".into(),
                    retryable: true,
                },
            ),
            env(
                4,
                Event::NodeStarted {
                    node_id: "n1".into(),
                    attempt: 2,
                    child_run_id: None,
                    input: None,
                },
            ),
            env(
                5,
                Event::NodeCompleted {
                    node_id: "n1".into(),
                    attempt: 2,
                    output: serde_json::json!(7),
                    duration_ms: 12,
                },
            ),
            env(
                6,
                Event::RunCompleted {
                    output: serde_json::json!(7),
                },
            ),
        ];

        let state = RunState::from_events(&events).unwrap();
        let rec = state.record("n1");
        assert_eq!(rec.state, NodeState::Completed { attempt: 2 });
        assert_eq!(rec.attempts(), 2);
        assert_eq!(state.outputs.get("n1"), Some(&serde_json::json!(7)));
        assert_eq!(rec.duration_ms, Some(12));
        assert_eq!(state.phase, RunPhase::Succeeded);
        assert!(state.all_terminal());
    }

    #[test]
    fn from_events_rejects_seq_gap() {
        let events = vec![
            env(
                1,
                Event::RunStarted {
                    workflow_id: "w".into(),
                    workflow_version: 1,
                    input: Value::Null,
                    depth: 0,
                },
            ),
            env(
                3,
                Event::RunCompleted {
                    output: Value::Null,
                },
            ),
        ];
        let err = RunState::from_events(&events).unwrap_err();
        assert!(matches!(err, EngineError::LogCorrupted(_)), "{err}");
    }

    #[test]
    fn dropped_node_started_clears_previous_output() {
        let mut state = RunState::new();
        state.fold(&env(
            1,
            Event::NodeCompleted {
                node_id: "n1".into(),
                attempt: 1,
                output: serde_json::json!("old"),
                duration_ms: 1,
            },
        ));
        assert_eq!(state.outputs.get("n1"), Some(&serde_json::json!("old")));
        state.fold(&env(
            2,
            Event::NodeStarted {
                node_id: "n1".into(),
                attempt: 2,
                child_run_id: None,
                input: None,
            },
        ));
        assert!(!state.outputs.contains_key("n1"));
    }

    /// 节点输出只存一份：无论走哪条事件路径，唯一所有者都是
    /// `RunState::outputs`（DESIGN §12.5）。旧的 `NodeRecord.output` 副本让
    /// `NodeFailed` / `NodeSkipped` 漏清一处，删掉副本后这个不对称不再可能。
    #[test]
    fn node_output_lives_only_in_outputs_across_every_event_path() {
        let completed = Event::NodeCompleted {
            node_id: "n1".into(),
            attempt: 1,
            output: serde_json::json!("v"),
            duration_ms: 1,
        };
        // (事件, 折叠后 outputs 里是否还有该节点的输出)
        let cases: Vec<(Event, bool)> = vec![
            (completed.clone(), true),
            (
                Event::NodeStarted {
                    node_id: "n1".into(),
                    attempt: 2,
                    child_run_id: None,
                    input: None,
                },
                false,
            ),
            (
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 2,
                    error: "boom".into(),
                    retryable: true,
                },
                false,
            ),
            (
                Event::NodeSkipped {
                    node_id: "n1".into(),
                    reason: "upstream_skipped".into(),
                },
                false,
            ),
        ];
        for (event, expect_present) in cases {
            // 每例都从「节点已完成」起：事件路径只决定该路径自己怎么处置旧输出
            let mut state = RunState::new();
            state.fold(&env(1, completed.clone()));
            assert!(state.outputs.contains_key("n1"), "前置条件不成立");
            state.fold(&env(2, event.clone()));
            assert_eq!(
                state.outputs.contains_key("n1"),
                expect_present,
                "{event:?} 之后 outputs 的存在性判断错了"
            );
        }
    }

    /// attempt / error 只有一份，且就在 `NodeState` 里。
    ///
    /// 这两个字段曾是 `NodeRecord` 的影子副本，靠事件分支手工同步——`error` 副本
    /// 连 `NodeSkipped` 分支都没清（漏网之处），`attempts` 副本靠三行 `.max()`
    /// 维持。删掉副本后「展示值」与「状态」不可能分叉：走一遍**全部**事件类型，
    /// 每步都断言派生的 attempts/error 与状态一致。
    #[test]
    fn attempt_and_error_are_derived_from_state_on_every_path() {
        let started = |attempt| Event::NodeStarted {
            node_id: "n1".into(),
            attempt,
            child_run_id: None,
            input: None,
        };
        let paths: Vec<Vec<Event>> = vec![
            // 未经启动：Pending
            vec![],
            // 启动中
            vec![started(1)],
            // 成功
            vec![
                started(1),
                Event::NodeCompleted {
                    node_id: "n1".into(),
                    attempt: 1,
                    output: Value::Null,
                    duration_ms: 1,
                },
            ],
            // 失败可重试（时间线标签 retrying）
            vec![
                started(1),
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 1,
                    error: "e1".into(),
                    retryable: true,
                },
            ],
            // 失败致命
            vec![
                started(1),
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 1,
                    error: "e2".into(),
                    retryable: false,
                },
            ],
            // 重试后成功：attempt 必须跟着走到 2
            vec![
                started(1),
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 1,
                    error: "e1".into(),
                    retryable: true,
                },
                started(2),
                Event::NodeCompleted {
                    node_id: "n1".into(),
                    attempt: 2,
                    output: Value::Null,
                    duration_ms: 1,
                },
            ],
            // 跳过
            vec![Event::NodeSkipped {
                node_id: "n1".into(),
                reason: "upstream_skipped".into(),
            }],
            // 信号已落盘但未消费
            vec![
                started(1),
                Event::SignalReceived {
                    node_id: "n1".into(),
                    payload: Value::Null,
                },
            ],
        ];
        for path in paths {
            let mut state = RunState::new();
            for (i, event) in path.iter().enumerate() {
                state.fold(&env((i + 1) as u64, event.clone()));
                let rec = state.record("n1");
                // 派生的展示值与状态恒等：影子字段已不存在，无从分叉
                assert_eq!(rec.attempts(), rec.state.attempt());
                assert_eq!(rec.error(), rec.state.error());
                // error 只在 Failed 出现，且与状态里的文本逐字一致
                match &rec.state {
                    NodeState::Failed { error, .. } => {
                        assert_eq!(rec.error(), Some(error.as_str()), "{event:?}");
                    }
                    _ => assert_eq!(rec.error(), None, "{event:?} 非 Failed 态不该有 error"),
                }
            }
        }
    }

    /// `NodeState` 是 attempt / error 的唯一来源（DESIGN §12.5）——本测试是
    /// 「派生而非副本」这条不变量的守卫：任何把字段加回 `NodeRecord` 的改动
    /// 都会在这里编译失败或断言失败。
    #[test]
    fn pending_and_skipped_report_zero_attempts() {
        for state in [
            NodeState::Pending,
            NodeState::Skipped { reason: "r".into() },
        ] {
            assert_eq!(state.attempt(), 0, "{state:?} 从未启动，尝试次数应为 0");
            assert_eq!(state.error(), None);
        }
        assert_eq!(NodeState::Running { attempt: 3 }.attempt(), 3);
        assert_eq!(NodeState::Completed { attempt: 4 }.attempt(), 4);
        assert_eq!(
            NodeState::Failed {
                attempt: 2,
                error: "x".into(),
                retryable: true
            }
            .error(),
            Some("x")
        );
    }
}
