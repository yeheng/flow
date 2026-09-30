use std::collections::HashMap;

use chrono::{DateTime, Utc};
use flow_dto::DbRunStatus;
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
    /// **为什么是派生而非存储**：attempt 号只存在于状态里，别处再存一份就得
    /// 靠事件分支手工同步。`Pending` / `Skipped` 从未启动过，计数为 0。
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
    /// 派生同 [`Self::attempt`]：另存一份就得靠事件分支手工同步，而
    /// `NodeSkipped` 这类分支最容易漏清。
    pub fn error(&self) -> Option<&str> {
        match self {
            NodeState::Failed { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// 时间线视图：折叠事件得到的单节点记录。
///
/// **唯一状态来源是 [`NodeState`]。** attempt / error 都不在这里另存一份：
/// - 已尝试次数 → [`NodeState::attempt`]（时间线要展示时取，不存副本）；
/// - 失败原因 → [`NodeState::error`]。
///
/// 这两个都曾是本结构的字段，代价是每个事件分支都要手工同步副本：`error`
/// 副本连 `NodeSkipped` 分支都没清过。存两份就等于多一份要维护的影子。
///
/// **output 也在这里，且只有这一份。** 它曾经是第三种形状——`RunState::outputs`
/// 那把与 `records` 同键的独立 map。挪走并没有消除同步义务（四个节点事件分支
/// 照样各自要动它：`NodeStarted`/`NodeFailed`/`NodeSkipped` 删、
/// `NodeCompleted` 插），只是把义务搬到另一把 map 上，并新增一个
/// 「两把 map 的键集可能不一致」的面。留在 `NodeRecord` 里，四个分支变成对
/// 同一个 `rec` 赋值，同步点数不变但不可能只改一处。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub state: NodeState,
    /// 节点输出（**唯一一份**）：`node_completed` 写入，新 attempt / 失败 /
    /// 跳过时清空——重试与重放不留脏数据。折叠进 `outputs` 那把同键 map 的
    /// 时代，`node_started` 清理旧值是「防止读到上一次尝试的结果」的唯一保障。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    /// 已记录但尚未被消费的信号（崩溃在 signal_received 与终态之间）。
    ///
    /// 不变量：`is_some() ⟹ 节点非终态`。只有非终态节点可能消费它——Running
    /// 节点消费（`apply_signal`），Pending 节点稍后启动时由 `NodeStarted` 清掉。
    /// **终态节点绝不携带陈旧待办**（那样它永远不会被消费）。四个节点终态分支
    /// （Completed / Failed / Skipped）与 `NodeStarted` 都要清它；`NodeSkipped`
    /// 曾是唯一漏清的分支。回归测试：`terminal_node_never_carries_a_stale_pending_signal`。
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

    /// 节点输出。缺失节点的默认视图（Pending）返回 `None`。
    pub fn output(&self) -> Option<&Value> {
        self.output.as_ref()
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

    /// 折叠相位 → runs.status 的**唯一**映射。
    ///
    /// **为什么派生而不是两处手写**：崩溃恢复要用折叠结果回填 DB 投影
    /// （`flow-backend/src/sqlite.rs` 的 `recover_unfinished` 与
    /// `flow-pg/src/sink.rs` 的 `reconcile_terminal`），两条路径原本各写一份
    /// 逐字相同的 4 臂 match。`DbRunStatus::ALL` 已经把「状态词汇表单一来源」
    /// 做成铁律（DESIGN §8/§12.8），这两处手写映射却是漏网的第二份真相——
    /// 加 `RunPhase` 变体时编译器不提醒，两处可能只改一处，回填出错的 status。
    ///
    /// 住在 flow-engine（RunPhase 的家）而不是 flow-dto：flow-dto 是零依赖叶子，
    /// 不能反向依赖引擎域类型。
    pub fn as_db_status(self) -> DbRunStatus {
        match self {
            RunPhase::Running => DbRunStatus::Running,
            RunPhase::Succeeded => DbRunStatus::Succeeded,
            RunPhase::Failed => DbRunStatus::Failed,
            RunPhase::Cancelled => DbRunStatus::Cancelled,
        }
    }
}

/// 一次执行的全部状态，由事件流折叠得到。恢复与只读时间线共用这一个结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub records: HashMap<String, NodeRecord>,
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

    /// 校验：日志里出现过的每个 node_id 都在定义内（`LogCorrupted` 硬错误）。
    ///
    /// **为什么必须查**：`records` 的键集 ⊋ 定义节点集——`fold` 对任何 node_id
    /// 都 `entry().or_default()`，日志里出现定义外的 id 就会凭空造一条记录。
    /// 而终止判定读的是两个**不重合**的键集：`plan()` 遍历 `definition.nodes`，
    /// `all_terminal()` 遍历 `records`。多出来那条幽灵记录若停在非终态
    /// （比如日志损坏只写了个 `node_started`），slots 空而 `all_terminal()` 恒假
    /// → 每次恢复都落 `EngineError::Bug` 挂 `awaiting_resume`，run 永久卡死、
    /// 要人工改数据。`RecoveryPlan::classify` 也有同一处静默 `continue`。
    ///
    /// 写者是单写者、run 钉死不可变版本，所以定义外 id 只可能来自日志损坏
    /// 或人工编辑——按损坏处理，不猜。
    ///
    /// 不放进 [`Self::from_events`]：只读路径（`Engine::snapshot` / 订阅回放）
    /// 没有定义，那里的语义是「原样呈现磁盘」，不该要求调用方提供定义。
    pub fn validate_nodes_in_definition(&self, def: &Definition) -> Result<(), EngineError> {
        for node_id in self.records.keys() {
            if def.node(node_id).is_none() {
                return Err(EngineError::LogCorrupted(format!(
                    "事件日志含定义外节点 {node_id}（定义共 {} 个节点）",
                    def.nodes.len()
                )));
            }
        }
        Ok(())
    }

    /// 缺失节点的默认视图（Pending）。返回引用，避免热点路径全量克隆。
    pub fn record(&self, node_id: &str) -> &NodeRecord {
        const DEFAULT_RECORD: NodeRecord = NodeRecord {
            state: NodeState::Pending,
            output: None,
            started_at: None,
            ended_at: None,
            duration_ms: None,
            last_signal: None,
            child_run_id: None,
            input: None,
        };
        self.records.get(node_id).unwrap_or(&DEFAULT_RECORD)
    }

    /// 节点输出（数据面，供 Driver 的 `nodes` 快照与 end 节点收集用）。
    /// 缺失节点返回 `Value::Null`——与「该节点确实输出了 null」不区分，
    /// 这是 §6.3 记载的已知取舍。
    pub fn node_output(&self, node_id: &str) -> Value {
        self.records
            .get(node_id)
            .and_then(|r| r.output.clone())
            .unwrap_or(Value::Null)
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
                // 清掉上一个 attempt 的输出：重试/重放不留脏数据
                rec.output = None;
                rec.started_at = Some(env.ts);
                rec.ended_at = None;
                rec.duration_ms = None;
                rec.last_signal = None;
                rec.child_run_id = child_run_id.clone();
                rec.input = input.clone();
            }
            Event::NodeCompleted {
                node_id,
                attempt,
                output,
                duration_ms,
            } => {
                let rec = self.records.entry(node_id.clone()).or_default();
                rec.state = NodeState::Completed { attempt: *attempt };
                rec.output = Some(output.clone());
                rec.ended_at = Some(env.ts);
                rec.duration_ms = Some(*duration_ms);
                rec.last_signal = None;
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
                rec.output = None;
                rec.ended_at = Some(env.ts);
                rec.last_signal = None;
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
                // 与其余三个节点终态分支对齐：清 output、清待消费信号。
                //
                // output：漏清会让下游读到上一个 attempt 的残留（旧的独立
                // outputs map 时代这里就漏过一次）。
                // last_signal：它是「信号已落盘、尚未被消费」的待办槽位，而只有
                // Running 节点会消费它——终态节点留着它就是一条永远不会被消费
                // 的陈旧待办。此前本分支是四个里唯一漏清它的，正好是 §4 声称
                // 已消灭的那类分支遗漏。当前不可达（信号只对 Running 节点写，
                // 而 plan() 只跳过 Pending 节点），但它让「终态 ⟹ 无待消费信号」
                // 这条不变量靠约定而非代码成立。回归测试：
                // `pending_signal_implies_running_node`。
                rec.output = None;
                rec.last_signal = None;
                rec.ended_at = Some(env.ts);
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
                // 取消必须清掉待定失败原因：fatal_error 的语义是「本 run 终态
                // 为 failed 时的原因」（finalize 据此选 RunFailed/RunCompleted）。
                // 用户主动取消时终态是 Cancelled，保留它会让同一状态在两条路径
                // 上给出不同答案：正常投影路径对 RunCancelled 明确写 error=NULL，
                // 而恢复回填路径（sqlite.rs 的 recover_unfinished 用
                // `terminal.fatal_error` 写 runs 行）会把「节点 X 失败」当成
                // cancelled run 的 error 落库——正是 DESIGN §9「同名字段必须
                // 同值」那条规矩被破掉。取消不是失败。
                self.fatal_error = None;
                self.output = None;
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
    use crate::event::{Event, LogLevel, LogStream};

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
        assert_eq!(rec.output(), Some(&serde_json::json!(7)));
        assert_eq!(state.node_output("n1"), serde_json::json!(7));
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
        assert_eq!(state.node_output("n1"), serde_json::json!("old"));
        state.fold(&env(
            2,
            Event::NodeStarted {
                node_id: "n1".into(),
                attempt: 2,
                child_run_id: None,
                input: None,
            },
        ));
        assert_eq!(state.node_output("n1"), Value::Null);
    }

    /// 节点输出只存一份，且住在 `NodeRecord` 里（DESIGN §12.5）：走遍**全部**
    /// 事件路径，输出要么是本次事件写的值、要么是 None，绝不会留下上一个
    /// attempt 的残留。
    ///
    /// 同步义务从未消失——四个节点终态分支各自都要处置旧输出。区别在于现在
    /// 四处改的是**同一个** `rec`，不可能只改一处；挪进独立的 `outputs` map
    /// 时代正是那种形状（且历史上真漏过 `NodeSkipped`）。
    #[test]
    fn node_output_is_cleared_on_every_non_completed_path() {
        let completed = Event::NodeCompleted {
            node_id: "n1".into(),
            attempt: 1,
            output: serde_json::json!("v"),
            duration_ms: 1,
        };
        // (事件, 折叠后该节点是否还持有输出)
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
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 2,
                    error: "boom".into(),
                    retryable: false,
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
            assert!(state.record("n1").output().is_some(), "前置条件不成立");
            state.fold(&env(2, event.clone()));
            assert_eq!(
                state.record("n1").output().is_some(),
                expect_present,
                "{event:?} 之后 output 的存在性判断错了"
            );
        }
    }

    /// 走一遍**全部**事件类型，每步都断言 `label()` 的展示词与 attempt 的跟随。
    ///
    /// `attempt` / `error` 只有一份且就在 `NodeState` 里（这两个字段曾是
    /// `NodeRecord` 的影子副本，靠事件分支手工同步——`error` 副本连 `NodeSkipped`
    /// 分支都没清）。影子字段已不存在，所以这里**不能**再断言「派生值等于状态值」——
    /// 那是同一个表达式比大小，恒真。要盯的是另一层会分叉的派生：`label()`。
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
                // 这里不断言 `attempts() == state.attempt()`：两者本来就是同一个
                // 表达式，恒真，不断言任何东西。真正会分叉的是**另一层派生**——
                // 时间线标签 `label()`（`retrying` 与 `failed` 靠 `retryable` 分流，
                // 是全文件唯一一处把状态翻译成展示词的分支），以及 attempt 在
                // 重试后必须跟着走到 2。走遍全部事件路径盯这两件。
                match &rec.state {
                    NodeState::Pending => assert_eq!(rec.state.label(), "pending", "{event:?}"),
                    NodeState::Running { attempt } => {
                        assert_eq!(rec.state.label(), "running", "{event:?}");
                        assert_eq!(rec.attempts(), *attempt, "{event:?}");
                    }
                    NodeState::Completed { attempt } => {
                        assert_eq!(rec.state.label(), "completed", "{event:?}");
                        assert_eq!(rec.attempts(), *attempt, "{event:?}");
                    }
                    NodeState::Failed {
                        retryable: true, ..
                    } => {
                        assert_eq!(
                            rec.state.label(),
                            "retrying",
                            "{event:?} 可重试失败应是 retrying"
                        );
                    }
                    NodeState::Failed {
                        retryable: false,
                        error,
                        ..
                    } => {
                        assert_eq!(rec.state.label(), "failed", "{event:?}");
                        // error 只在 Failed 出现，且与状态里的文本逐字一致
                        assert_eq!(rec.error(), Some(error.as_str()), "{event:?}");
                    }
                    NodeState::Skipped { .. } => {
                        assert_eq!(rec.state.label(), "skipped", "{event:?}")
                    }
                }
                if !matches!(rec.state, NodeState::Failed { .. }) {
                    assert_eq!(rec.error(), None, "{event:?} 非 Failed 态不该有 error");
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

    /// `last_signal` 是「信号已落盘、尚未被消费」的待办槽位。只有**非终态**节点
    /// 可能消费它：Running 节点消费它（`apply_signal`），Pending 节点稍后启动时
    /// 由 `NodeStarted` 清掉。所以不变量是 `last_signal.is_some() ⟹ 非终态`——
    /// **终态节点绝不携带陈旧待办**。
    ///
    /// 走遍**全部**事件类型、每步都断言这条：`NodeSkipped` 曾是四个节点终态分支
    /// 里唯一漏清 `last_signal` 的（NodeStarted / NodeCompleted / NodeFailed 都清）。
    /// 当前不可达——信号只对 AwaitSignal / Adjudicating 节点写，而那两种都是
    /// Running；`plan()` 又只跳过 Pending 节点。但漏清让这条不变量靠约定而非代码
    /// 成立，将来谁放宽 `plan()` 的跳过条件（或让信号能写给终态节点）就会变成
    /// 真 bug：一个永远不会被消费的待办挂在终态节点上。
    #[test]
    fn terminal_node_never_carries_a_stale_pending_signal() {
        let started = |attempt| Event::NodeStarted {
            node_id: "n1".into(),
            attempt,
            child_run_id: None,
            input: None,
        };
        let signal = Event::SignalReceived {
            node_id: "n1".into(),
            payload: Value::Null,
        };
        let paths: Vec<Vec<Event>> = vec![
            // 信号落在一个从未启动的节点上：Pending + 待消费信号，合法（非终态）——
            // 节点稍后启动时由 NodeStarted 清掉
            vec![signal.clone()],
            // 正常形态：启动 → 收到信号 → 仍在运行
            vec![started(1), signal.clone()],
            // 收到信号后补终态：待办被消费掉
            vec![
                started(1),
                signal.clone(),
                Event::NodeCompleted {
                    node_id: "n1".into(),
                    attempt: 1,
                    output: Value::Null,
                    duration_ms: 1,
                },
            ],
            vec![
                started(1),
                signal.clone(),
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 1,
                    error: "e".into(),
                    retryable: false,
                },
            ],
            // 收到信号后被跳过：此前唯一漏清的分支
            vec![
                started(1),
                signal.clone(),
                Event::NodeSkipped {
                    node_id: "n1".into(),
                    reason: "upstream_skipped".into(),
                },
            ],
            // 重试（新一轮 node_started）也要清掉上一轮残留的待消费信号
            vec![
                started(1),
                signal.clone(),
                Event::NodeFailed {
                    node_id: "n1".into(),
                    attempt: 1,
                    error: "e".into(),
                    retryable: true,
                },
                started(2),
            ],
            // node_log 不改状态：不该动待消费信号
            vec![
                started(1),
                signal.clone(),
                Event::NodeLog {
                    node_id: "n1".into(),
                    attempt: 1,
                    level: LogLevel::Info,
                    stream: LogStream::Engine,
                    message: "x".into(),
                },
            ],
        ];
        for path in paths {
            let mut state = RunState::new();
            for (i, event) in path.iter().enumerate() {
                state.fold(&env((i + 1) as u64, event.clone()));
                let rec = state.record("n1");
                assert!(
                    rec.last_signal.is_none() || !rec.state.is_terminal(),
                    "{event:?} 之后终态节点仍携带待消费信号（永远不会被消费）：{:?}",
                    rec.state
                );
            }
        }
    }

    fn def_with(ids: &[&str]) -> Definition {
        // Definition 私有字段强制只能经 serde 构造（model.rs 的设计）
        serde_json::from_value(serde_json::json!({
            "nodes": ids
                .iter()
                .map(|id| serde_json::json!({"id": id, "type": "script", "params": {"code": "return 1;"}}))
                .collect::<Vec<_>>(),
            "edges": []
        }))
        .expect("测试定义应可反序列化")
    }

    /// **取消不是失败**：`fatal_error` 的语义是「本 run 终态为 failed 时的
    /// 原因」，`RunCancelled` 必须把它清掉。
    ///
    /// 漏清的实际后果不止是时间线多显示一个字段：恢复回填路径
    /// （`recover_unfinished` 用 `terminal.fatal_error` 写 runs 行）会把
    /// 「节点 X 失败」当成 cancelled run 的 error 落库，而正常投影路径对
    /// RunCancelled 明确写 error=NULL——同一状态两条路径两个答案。
    #[test]
    fn run_cancelled_clears_pending_fatal_error() {
        let mut state = RunState::new();
        state.fold(&env(
            1,
            Event::NodeFailed {
                node_id: "n1".into(),
                attempt: 1,
                error: "boom".into(),
                retryable: false,
            },
        ));
        assert!(
            state.fatal_error.is_some(),
            "前置条件不成立：致命失败应记录原因"
        );

        state.fold(&env(2, Event::RunCancelled {}));
        assert_eq!(state.phase, RunPhase::Cancelled);
        assert_eq!(
            state.fatal_error, None,
            "取消后不得残留 fatal_error（会被回填成 cancelled run 的 error）"
        );
    }

    /// 成功路径同理：`RunCompleted` 与 fatal 互斥，fatal_error 不得带进
    /// 成功结果（DESIGN §8：已解决的诊断不得留在成功结果里）。
    #[test]
    fn run_failed_error_replaces_node_level_fatal() {
        let mut state = RunState::new();
        state.fold(&env(
            1,
            Event::NodeFailed {
                node_id: "n1".into(),
                attempt: 1,
                error: "node boom".into(),
                retryable: false,
            },
        ));
        state.fold(&env(
            2,
            Event::RunFailed {
                error: "run boom".into(),
            },
        ));
        assert_eq!(state.fatal_error.as_deref(), Some("run boom"));
    }

    /// 定义外节点 = 日志损坏，必须硬错误。
    ///
    /// 漏掉的后果是永久卡死：幽灵记录停在非终态时，`all_terminal()` 恒假而
    /// slots 已空，终止判定永远不成立，每次恢复都落 `EngineError::Bug` 挂
    /// `awaiting_resume`——要人工改数据才能恢复。
    #[test]
    fn node_outside_definition_is_log_corruption() {
        let mut state = RunState::new();
        // fold 本身照常收下（只读路径要能原样呈现磁盘），但恢复路径必须拒
        state.fold(&env(
            1,
            Event::NodeStarted {
                node_id: "ghost".into(),
                attempt: 1,
                child_run_id: None,
                input: None,
            },
        ));
        let def = def_with(&["n1"]);
        let err = state.validate_nodes_in_definition(&def).unwrap_err();
        assert!(matches!(err, EngineError::LogCorrupted(_)), "{err}");
        assert!(err.to_string().contains("ghost"), "诊断要指名道姓：{err}");

        // 定义内的（含 ensure_nodes 补的 Pending）必须通过
        let mut ok = RunState::new();
        ok.ensure_nodes(&def);
        ok.fold(&env(
            1,
            Event::NodeCompleted {
                node_id: "n1".into(),
                attempt: 1,
                output: Value::Null,
                duration_ms: 1,
            },
        ));
        assert!(ok.validate_nodes_in_definition(&def).is_ok());
    }

    /// `as_db_status` 是 RunPhase → runs.status 的唯一映射（两个后端的恢复
    /// 回填共用）。这里钉住四个相位各自的落库取值。
    #[test]
    fn run_phase_maps_to_db_status_on_every_phase() {
        for (phase, expected) in [
            (RunPhase::Running, DbRunStatus::Running),
            (RunPhase::Succeeded, DbRunStatus::Succeeded),
            (RunPhase::Failed, DbRunStatus::Failed),
            (RunPhase::Cancelled, DbRunStatus::Cancelled),
        ] {
            assert_eq!(phase.as_db_status(), expected, "{phase:?} 的落库状态错了");
        }
        // 与 DbRunStatus 自身的分类谓词对拍：非终态 ↔ 非终态
        for phase in [
            RunPhase::Running,
            RunPhase::Succeeded,
            RunPhase::Failed,
            RunPhase::Cancelled,
        ] {
            assert_eq!(
                phase.is_terminal(),
                phase.as_db_status().is_terminal(),
                "{phase:?} 的终态判定与落库状态不一致"
            );
        }
    }
}
