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
            NodeState::Pending | NodeState::Running { .. } | NodeState::Failed { retryable: true, .. }
        )
    }

    pub fn label(&self) -> &'static str {
        match self {
            NodeState::Pending => "pending",
            NodeState::Running { .. } => "running",
            NodeState::Completed { .. } => "completed",
            NodeState::Failed { retryable: true, .. } => "retrying",
            NodeState::Failed { retryable: false, .. } => "failed",
            NodeState::Skipped { .. } => "skipped",
        }
    }
}

/// 时间线视图：折叠事件得到的单节点记录。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub state: NodeState,
    pub attempts: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    pub output: Option<Value>,
    pub error: Option<String>,
    /// 已记录但尚未被消费的信号（崩溃在 signal_received 与终态之间）
    pub last_signal: Option<Value>,
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

    pub fn record(&self, node_id: &str) -> NodeRecord {
        self.records.get(node_id).cloned().unwrap_or_default()
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
            } => {
                self.workflow_id = Some(workflow_id.clone());
                self.workflow_version = Some(*workflow_version);
                self.input = input.clone();
                self.started_at = Some(env.ts);
                self.phase = RunPhase::Running;
            }
            Event::NodeStarted { node_id, attempt, .. } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.state = NodeState::Running { attempt: *attempt };
                rec.attempts = rec.attempts.max(*attempt);
                rec.started_at = Some(env.ts);
                rec.ended_at = None;
                rec.duration_ms = None;
                rec.error = None;
                rec.output = None;
                rec.last_signal = None;
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
                rec.attempts = rec.attempts.max(*attempt);
                rec.ended_at = Some(env.ts);
                rec.duration_ms = Some(*duration_ms);
                rec.output = Some(output.clone());
                rec.error = None;
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
                rec.attempts = rec.attempts.max(*attempt);
                rec.ended_at = Some(env.ts);
                rec.error = Some(error.clone());
                rec.last_signal = None;
                self.outputs.remove(node_id);
                if !retryable {
                    self.fatal_error.get_or_insert_with(|| format!("节点 {node_id} 失败：{error}"));
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
                },
            ),
            env(
                2,
                Event::NodeStarted {
                    node_id: "n1".into(),
                    attempt: 1,
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
            env(6, Event::RunCompleted { output: serde_json::json!(7) }),
        ];

        let state = RunState::from_events(&events).unwrap();
        let rec = state.record("n1");
        assert_eq!(rec.state, NodeState::Completed { attempt: 2 });
        assert_eq!(rec.attempts, 2);
        assert_eq!(rec.output, Some(serde_json::json!(7)));
        assert_eq!(rec.duration_ms, Some(12));
        assert_eq!(state.phase, RunPhase::Succeeded);
        assert!(state.all_terminal());
    }

    #[test]
    fn from_events_rejects_seq_gap() {
        let events = vec![
            env(1, Event::RunStarted { workflow_id: "w".into(), workflow_version: 1, input: Value::Null }),
            env(3, Event::RunCompleted { output: Value::Null }),
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
            },
        ));
        assert!(!state.outputs.contains_key("n1"));
    }
}
