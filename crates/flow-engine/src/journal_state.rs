//! V2 fact reducer. Replaying this module never evaluates templates or executes a node.
use flow_journal::{Event, EventKind, StoredValue, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::Definition;

type Result<T> = flow_journal::Result<T>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

// Accept numeric snapshot counters from earlier development logs, emit lossless strings.
mod snapshot_decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(n: &u64, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&n.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Number {
            Text(String),
            Number(u64),
        }
        match Number::deserialize(d)? {
            Number::Number(n) => Ok(n),
            Number::Text(n) => n.parse().map_err(serde::de::Error::custom),
        }
    }
}

/// 通用幂等回执保留窗口：超过即按 lsn 淘汰最旧一半。调度执行身份另由
/// schedule_fires 保留，业务去重不依赖这个有限重试窗口。
const COMMAND_WINDOW: usize = 8192;

/// run 生命周期词汇表的唯一定义在 `flow-dto`；这里只 re-export，
/// 避免出现第二份 `ALL`/`as_str`。
pub use flow_dto::RunStatus;

/// 节点执行状态（`Node.status`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Skipped,
    Failed,
}

impl NodeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 持久等待类别（`Wait.kind`，WaitRegistered 携带）。事件载荷里的裸字符串
/// 比对一律经 [`WaitKind::as_str`]，不写第二份字面量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    Delay,
    Signal,
    Child,
    Retry,
    Uncertain,
}

impl WaitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delay => "delay",
            Self::Signal => "signal",
            Self::Child => "child",
            Self::Retry => "retry",
            Self::Uncertain => "uncertain",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub workflow_id: String,
    pub name: String,
    pub created_at: String,
    pub versions: BTreeMap<u64, DefinitionVersion>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionVersion {
    pub version: u64,
    pub definition: StoredValue,
    pub checksum: String,
    pub published: bool,
    pub created_at: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u64,
    pub definition: StoredValue,
    pub input: StoredValue,
    pub source: String,
    pub source_detail: Option<String>,
    pub parent: Option<Parent>,
    pub depth: u32,
    pub created_at: String,
    pub status: RunStatus,
    pub output: Option<StoredValue>,
    pub error: Option<String>,
    #[serde(with = "snapshot_decimal")]
    pub last_run_seq: u64,
    pub nodes: BTreeMap<String, Node>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionSnapshot>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub signals: BTreeMap<String, StoredValue>,
}

/// Immutable dependencies and deterministic inputs, inherited by child runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub runtime: String,
    pub clock_ms: i64,
    pub seed: String,
    pub path: Vec<String>,
    pub definitions: BTreeMap<String, DefinitionVersion>,
    pub bindings: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regression: Option<Regression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}
impl ExecutionSnapshot {
    pub fn node_key(&self, node: &str) -> String {
        let mut path = self.path.clone();
        path.push(node.into());
        serde_json::to_string(&path).unwrap()
    }
    pub fn signal_inputs(&self) -> BTreeMap<String, StoredValue> {
        self.regression
            .as_ref()
            .map(|r| {
                r.signals
                    .iter()
                    .filter_map(|(path, value)| {
                        let mut path: Vec<String> = serde_json::from_str(path).ok()?;
                        let node = path.pop()?;
                        (path == self.path).then(|| (node, value.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Regression {
    pub baseline: String,
    pub fixtures: BTreeMap<String, OperationFixture>,
    #[serde(default)]
    pub signals: BTreeMap<String, StoredValue>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub overrides: std::collections::BTreeSet<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationFixture {
    pub fingerprint: String,
    pub outcome: StoredValue,
}
impl Run {
    /// Run metadata without the per-node execution ledger.
    pub fn summary(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            workflow_id: self.workflow_id.clone(),
            workflow_version: self.workflow_version,
            definition: self.definition.clone(),
            input: self.input.clone(),
            source: self.source.clone(),
            source_detail: self.source_detail.clone(),
            parent: self.parent.clone(),
            depth: self.depth,
            created_at: self.created_at.clone(),
            status: self.status,
            output: self.output.clone(),
            error: self.error.clone(),
            last_run_seq: self.last_run_seq,
            nodes: BTreeMap::new(),
            execution: self.execution.clone(),
            signals: self.signals.clone(),
        }
    }
    /// Terminal facts win; otherwise all outstanding uncertain waits participate.
    pub fn refresh_status(&mut self) {
        if !self.status.terminal() {
            self.status = if self.nodes.values().any(|node| {
                node.wait
                    .as_ref()
                    .is_some_and(|wait| wait.kind() == WaitKind::Uncertain)
            }) {
                RunStatus::AwaitingResume
            } else {
                RunStatus::Running
            };
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parent {
    pub run_id: String,
    pub node_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub node_execution_id: String,
    pub attempt: u32,
    pub dispatch_id: String,
    pub status: NodeStatus,
    pub output: Option<StoredValue>,
    pub branch: Option<bool>,
    pub prepared: Option<Prepared>,
    pub wait: Option<Wait>,
    pub operation: Option<Operation>,
    pub attempts: BTreeMap<String, Attempt>,
    pub error: Option<String>,
    /// 跳过原因（NodeSkipped 事件携带；时间线展示用，不影响裁决）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
}
impl Node {
    /// Execution fields only; attempt history is staged and indexed independently.
    pub fn summary(&self) -> Self {
        Self {
            node_execution_id: self.node_execution_id.clone(),
            attempt: self.attempt,
            dispatch_id: self.dispatch_id.clone(),
            status: self.status,
            output: self.output.clone(),
            branch: self.branch,
            prepared: self.prepared.clone(),
            wait: self.wait.clone(),
            operation: self.operation.clone(),
            attempts: BTreeMap::new(),
            error: self.error.clone(),
            skip_reason: self.skip_reason.clone(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub audit_seq: u64,
    pub audit_digests: BTreeMap<u64, String>,
    #[serde(default)]
    pub audit_lsns: BTreeMap<u64, u64>,
    #[serde(default)]
    pub result: Option<Event>,
    #[serde(default)]
    pub result_lsn: Option<u64>,
    pub sealed: bool,
    pub integrity: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prepared {
    pub input: StoredValue,
    pub predecessors: BTreeMap<String, StoredValue>,
    pub params: StoredValue,
    pub engine_build: String,
    pub node_semantics: u32,
}
/// Internal waits carry the fields required by their kind. The adapter preserves
/// the original checkpoint/event JSON shape, including explicit null fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "WaitWire", into = "WaitWire")]
pub enum Wait {
    Delay {
        wake_at: i64,
        output: Option<StoredValue>,
    },
    Signal {
        output: Option<StoredValue>,
    },
    Child {
        child_run_id: String,
    },
    Retry {
        wake_at: i64,
    },
    Uncertain,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WaitWire {
    kind: WaitKind,
    wake_at: Option<i64>,
    child_run_id: Option<String>,
    #[serde(default)]
    output: Option<StoredValue>,
}
impl TryFrom<WaitWire> for Wait {
    type Error = &'static str;
    fn try_from(w: WaitWire) -> std::result::Result<Self, Self::Error> {
        if w.child_run_id.is_some() && w.kind != WaitKind::Child {
            return Err("child_run_id on non-child wait");
        }
        if w.wake_at.is_some() && !matches!(w.kind, WaitKind::Delay | WaitKind::Retry) {
            return Err("wake_at on non-timed wait");
        }
        if w.output.is_some() && !matches!(w.kind, WaitKind::Delay | WaitKind::Signal) {
            return Err("output on non-value wait");
        }
        Ok(match w.kind {
            WaitKind::Delay => Self::Delay {
                wake_at: w.wake_at.ok_or("delay missing wake_at")?,
                output: w.output,
            },
            WaitKind::Signal => Self::Signal { output: w.output },
            WaitKind::Child => Self::Child {
                child_run_id: w
                    .child_run_id
                    .filter(|id| !id.is_empty())
                    .ok_or("child missing child_run_id")?,
            },
            WaitKind::Retry => Self::Retry {
                wake_at: w.wake_at.ok_or("retry missing wake_at")?,
            },
            WaitKind::Uncertain => Self::Uncertain,
        })
    }
}
impl From<Wait> for WaitWire {
    fn from(w: Wait) -> Self {
        let mut wire = Self {
            kind: w.kind(),
            wake_at: w.wake_at(),
            child_run_id: None,
            output: w.output().cloned(),
        };
        if let Wait::Child { child_run_id } = w {
            wire.child_run_id = Some(child_run_id);
        }
        wire
    }
}
impl Wait {
    pub fn kind(&self) -> WaitKind {
        match self {
            Self::Delay { .. } => WaitKind::Delay,
            Self::Signal { .. } => WaitKind::Signal,
            Self::Child { .. } => WaitKind::Child,
            Self::Retry { .. } => WaitKind::Retry,
            Self::Uncertain => WaitKind::Uncertain,
        }
    }
    pub fn wake_at(&self) -> Option<i64> {
        match self {
            Self::Delay { wake_at, .. } | Self::Retry { wake_at } => Some(*wake_at),
            _ => None,
        }
    }
    pub fn child_run_id(&self) -> Option<&String> {
        match self {
            Self::Child { child_run_id } => Some(child_run_id),
            _ => None,
        }
    }
    pub fn output(&self) -> Option<&StoredValue> {
        match self {
            Self::Delay { output, .. } | Self::Signal { output } => output.as_ref(),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub operation_id: String,
    pub fingerprint: String,
    pub permit_id: String,
    pub request: StoredValue,
    pub outcome: Option<StoredValue>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRecord {
    pub scope: String,
    pub request_id: Option<String>,
    pub fingerprint: String,
    pub result: Value,
    #[serde(with = "flow_journal::decimal")]
    pub lsn: u64,
}
/// Business trigger watermark, independent of the bounded RPC receipt window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleFire {
    pub at: chrono::DateTime<chrono::Utc>,
    pub command: CommandRecord,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// Rebuilt by replay before accepting commands; never trusted from serialized snapshots.
    #[serde(skip)]
    pub values: flow_journal::value::Publications,
    /// Backend keeps permanent audit receipts in a rebuildable disk index.
    #[serde(skip)]
    pub compact_audits: bool,
    pub version: u32,
    pub journal_id: String,
    #[serde(with = "snapshot_decimal")]
    pub applied_lsn: u64,
    pub workflows: BTreeMap<String, Workflow>,
    pub runs: BTreeMap<String, Run>,
    /// Permanent workflow/run relationship, bounded by workflow count rather than run history.
    #[serde(default)]
    pub run_workflows: std::collections::BTreeSet<String>,
    pub schedules: BTreeMap<String, Value>,
    #[serde(default)]
    pub schedule_fires: BTreeMap<String, ScheduleFire>,
    pub webhooks: BTreeMap<String, Value>,
    /// 可复用节点模板（画布片段）：UI 资产而非 run 事实，但持久化必须随
    /// 权威走——journal 是唯一权威，模板以 TemplateChanged 事实落账。
    #[serde(default)]
    pub templates: BTreeMap<String, Value>,
    pub commands: BTreeMap<String, CommandRecord>,
    #[serde(default)]
    pub legacy: BTreeMap<String, Value>,
    /// Writer tenure (JSONL refactor phase 2, §3.2). Bumped durably each time a master
    /// process takes the exclusive journal write lock and before it dispatches tasks.
    /// Zero means a journal written before the epoch event existed.
    #[serde(default, with = "snapshot_decimal")]
    pub master_epoch: u64,
}

pub fn command_key(scope: &str, request_id: &str) -> String {
    serde_json::to_string(&(scope, request_id)).expect("string tuple")
}

impl Run {
    pub fn terminal(&self) -> bool {
        self.status.terminal()
    }
}

impl State {
    /// The latest schedule slot retains its receipt even after unrelated RPC
    /// commands fill the general dedup window.
    pub fn command_record(&self, scope: &str, request_id: &str) -> Option<&CommandRecord> {
        self.commands
            .get(&command_key(scope, request_id))
            .or_else(|| {
                let key = scope.strip_prefix("run.start:schedule:")?;
                let fire = self.schedule_fires.get(key)?;
                (fire.command.request_id.as_deref() == Some(request_id)).then_some(&fire.command)
            })
    }
    /// Transaction-local staging guarantees that a malformed later event cannot expose its
    /// earlier sibling. Only affected maps/objects are touched by the committed replacement.
    pub fn apply(&mut self, tx: &Transaction) -> Result<()> {
        self.apply_with(tx, |_| Ok(()))
    }
    /// Commit the transaction and inspect the post-transaction state. The inspect
    /// callback sees exactly what `apply` would leave behind; on error the state is
    /// restored from the affected-entity undo.
    pub fn apply_with<T>(
        &mut self,
        tx: &Transaction,
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        self.apply_mode(tx, true, inspect)
    }
    pub fn check(&mut self, tx: &Transaction) -> Result<()> {
        self.apply_mode(tx, false, |_| Ok(()))
    }
    /// Inspect the proposed state and always restore the old state, including when
    /// encoding fails. Uses the same affected-entity undo as apply(), not a full clone.
    pub fn preview<T>(
        &mut self,
        tx: &Transaction,
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        let mut output = None;
        self.apply_mode(tx, false, |state| {
            output = Some(inspect(state)?);
            Ok(())
        })?;
        Ok(output.expect("successful preview"))
    }
    fn apply_mode<T>(
        &mut self,
        tx: &Transaction,
        commit: bool,
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        tx.validate()?;
        if tx.lsn != self.applied_lsn + 1
            || (!self.journal_id.is_empty() && self.journal_id != tx.journal_id)
        {
            return Err(invalid("reducer identity/LSN mismatch"));
        }
        for (index, event) in tx.events.iter().enumerate() {
            if event.audit_seq > 0
                && (event.run_id.is_none()
                    || event
                        .node_id
                        .as_deref()
                        .or_else(|| event.payload["node_id"].as_str())
                        .is_none())
            {
                return Err(invalid("audit requires run and node identity"));
            }
            if event.kind == EventKind::MasterEpochStarted {
                if event.run_id.is_some()
                    || event.dispatch_id.is_some()
                    || event.node_id.is_some()
                    || event.run_seq != 0
                    || event.audit_seq != 0
                {
                    return Err(invalid("master epoch event must be dataset-scoped"));
                }
                let epoch = number(&event.payload, "epoch")?;
                if epoch != self.master_epoch + 1 {
                    return Err(invalid("master epoch must advance by exactly one"));
                }
            }
            if event.kind == EventKind::OperationIntent {
                let next = tx
                    .events
                    .get(index + 1)
                    .ok_or_else(|| invalid("intent without atomic authorization"))?;
                if next.kind != EventKind::OperationAuthorized
                    || next.run_id != event.run_id
                    || next.dispatch_id != event.dispatch_id
                    || next.node_id != event.node_id
                    || next.payload != event.payload
                    || event.audit_seq != 0
                    || next.audit_seq != 0
                {
                    return Err(invalid(
                        "intent/authorization must share transaction and payload",
                    ));
                }
            }
            if event.kind == EventKind::OperationAuthorized
                && (index == 0 || tx.events[index - 1].kind != EventKind::OperationIntent)
            {
                return Err(invalid("authorization without intent"));
            }
        }
        // Undo only changed entities, not the full historical state. Inline values are bounded.
        // Payload-only audits change counters and one receipt entry, never node state.
        // Their rollback cost is independent of the accumulated run history.
        let audit_only = tx.events.iter().all(|e| {
            matches!(
                e.kind,
                EventKind::ValueChunk | EventKind::ValuePublished | EventKind::LateAudit
            )
        });
        let mut audit_undo = Vec::new();
        let mut run_seq_undo = BTreeMap::new();
        let mut workflow_undo = BTreeMap::new();
        let mut run_undo = BTreeMap::new();
        let mut node_undo = BTreeMap::new();
        let mut attempt_undo = BTreeMap::new();
        let mut membership_undo = BTreeMap::new();
        let replaced_nodes: std::collections::BTreeSet<_> = tx
            .events
            .iter()
            .filter(|e| e.kind == EventKind::NodeSkipped)
            .filter_map(|e| {
                Some((
                    e.run_id.clone()?,
                    e.node_id
                        .clone()
                        .or_else(|| e.payload["node_id"].as_str().map(str::to_owned))?,
                ))
            })
            .collect();
        let mut schedule_undo = BTreeMap::new();
        let mut fire_undo = BTreeMap::new();
        let mut webhook_undo = BTreeMap::new();
        let mut template_undo = BTreeMap::new();
        let mut command_undo = BTreeMap::new();
        let mut value_undo = BTreeMap::new();
        let mut legacy_undo = BTreeMap::new();
        let mut epoch_undo: Option<u64> = None;
        for event in &tx.events {
            if event.kind == EventKind::RunStarted {
                let workflow = string(&event.payload, "workflow_id")?;
                membership_undo
                    .entry(workflow.clone())
                    .or_insert(self.run_workflows.contains(&workflow));
            }
            if event.kind == EventKind::LegacyImport {
                let key = string(&event.payload, "key")?;
                legacy_undo
                    .entry(key.clone())
                    .or_insert_with(|| self.legacy.get(&key).cloned());
            }
            if matches!(
                event.kind,
                EventKind::ValueChunk | EventKind::ValuePublished
            ) {
                let id = string(&event.payload, "output_id")?;
                value_undo
                    .entry(id.clone())
                    .or_insert_with(|| self.values.save(&id));
            }
            if let Some(id) = event.payload.get("workflow_id").and_then(Value::as_str) {
                if matches!(
                    event.kind,
                    EventKind::WorkflowCreated
                        | EventKind::WorkflowUpdated
                        | EventKind::WorkflowPublished
                        | EventKind::WorkflowDeleted
                ) {
                    workflow_undo
                        .entry(id.to_owned())
                        .or_insert_with(|| self.workflows.get(id).cloned());
                }
            }
            if let Some(id) = &event.run_id {
                if audit_only {
                    if let Some(run) = self.runs.get(id) {
                        run_seq_undo
                            .entry(id.clone())
                            .or_insert((run.last_run_seq, run.status));
                        let node_id = event
                            .node_id
                            .clone()
                            .or_else(|| event.payload["node_id"].as_str().map(str::to_owned));
                        if let (Some(node_id), Some(dispatch)) = (&node_id, &event.dispatch_id) {
                            if let Some(attempt) = run
                                .nodes
                                .get(node_id)
                                .and_then(|n| n.attempts.get(dispatch))
                            {
                                audit_undo.push((
                                    id.clone(),
                                    node_id.clone(),
                                    dispatch.clone(),
                                    event.audit_seq,
                                    attempt.audit_seq,
                                    attempt.audit_digests.get(&event.audit_seq).cloned(),
                                    attempt.audit_lsns.get(&event.audit_seq).copied(),
                                ));
                            }
                        }
                    }
                } else {
                    run_undo
                        .entry(id.clone())
                        .or_insert_with(|| self.runs.get(id).map(Run::summary));
                    if let Some(node_id) = event
                        .node_id
                        .as_deref()
                        .or_else(|| event.payload["node_id"].as_str())
                    {
                        node_undo
                            .entry((id.clone(), node_id.to_owned()))
                            .or_insert_with(|| {
                                self.runs
                                    .get(id)
                                    .and_then(|r| r.nodes.get(node_id))
                                    .map(|n| {
                                        if replaced_nodes
                                            .contains(&(id.clone(), node_id.to_owned()))
                                        {
                                            n.clone()
                                        } else {
                                            n.summary()
                                        }
                                    })
                            });
                        if let Some(dispatch) = &event.dispatch_id {
                            attempt_undo
                                .entry((id.clone(), node_id.to_owned(), dispatch.clone()))
                                .or_insert_with(|| {
                                    self.runs
                                        .get(id)
                                        .and_then(|r| r.nodes.get(node_id))
                                        .and_then(|n| n.attempts.get(dispatch))
                                        .cloned()
                                });
                        }
                    }
                }
            }
            if event.kind == EventKind::ScheduleChanged {
                let id = string(&event.payload, "id")?;
                fire_undo
                    .entry(id.clone())
                    .or_insert_with(|| self.schedule_fires.get(&id).cloned());
                schedule_undo
                    .entry(id.clone())
                    .or_insert_with(|| self.schedules.get(&id).cloned());
            }
            if event.kind == EventKind::MasterEpochStarted {
                epoch_undo.get_or_insert(self.master_epoch);
            }
            if event.kind == EventKind::WebhookChanged {
                let id = string(&event.payload, "token")?;
                webhook_undo
                    .entry(id.clone())
                    .or_insert_with(|| self.webhooks.get(&id).cloned());
            }
            if event.kind == EventKind::TemplateChanged {
                let id = string(&event.payload, "id")?;
                template_undo
                    .entry(id.clone())
                    .or_insert_with(|| self.templates.get(&id).cloned());
            }
            if event.kind == EventKind::Command {
                let c: CommandRecord = serde_json::from_value(event.payload.clone())?;
                if let Some(key) = c.scope.strip_prefix("run.start:schedule:") {
                    fire_undo
                        .entry(key.to_owned())
                        .or_insert_with(|| self.schedule_fires.get(key).cloned());
                }
                if let Some(id) = c.request_id {
                    let key = command_key(&c.scope, &id);
                    command_undo
                        .entry(key.clone())
                        .or_insert_with(|| self.commands.get(&key).cloned());
                }
            }
        }
        let result = (|| {
            for event in &tx.events {
                self.values.apply(event, &tx.journal_id)?;
                self.check_values(event)?;
                self.apply_event(event, tx.lsn, &mut command_undo)?;
                if let Some(run) = event.run_id.as_ref().and_then(|id| self.runs.get_mut(id)) {
                    if !audit_only {
                        run.refresh_status();
                    }
                }
            }
            inspect(self)
        })();
        if result.is_err() || !commit {
            restore(&mut self.workflows, workflow_undo);
            for (id, previous) in run_undo {
                if let Some(mut previous) = previous {
                    if let Some(current) = self.runs.get_mut(&id) {
                        previous.nodes = std::mem::take(&mut current.nodes);
                    }
                    self.runs.insert(id, previous);
                } else {
                    self.runs.remove(&id);
                }
            }
            for ((run_id, node_id), previous) in node_undo {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    if let Some(mut previous) = previous {
                        if !replaced_nodes.contains(&(run_id.clone(), node_id.clone())) {
                            if let Some(current) = run.nodes.get_mut(&node_id) {
                                previous.attempts = std::mem::take(&mut current.attempts);
                            }
                        }
                        run.nodes.insert(node_id, previous);
                    } else {
                        run.nodes.remove(&node_id);
                    }
                }
            }
            for ((run_id, node_id, dispatch), previous) in attempt_undo {
                if let Some(node) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|r| r.nodes.get_mut(&node_id))
                {
                    if let Some(previous) = previous {
                        node.attempts.insert(dispatch, previous);
                    } else {
                        node.attempts.remove(&dispatch);
                    }
                }
            }
            for (workflow, existed) in membership_undo {
                if !existed {
                    self.run_workflows.remove(&workflow);
                }
            }
            for (id, (seq, status)) in run_seq_undo {
                let run = self.runs.get_mut(&id).expect("existing audit run");
                run.last_run_seq = seq;
                run.status = status;
            }
            for (run, node, dispatch, seq, previous_seq, digest, lsn) in
                audit_undo.into_iter().rev()
            {
                let attempt = self
                    .runs
                    .get_mut(&run)
                    .and_then(|r| r.nodes.get_mut(&node))
                    .and_then(|n| n.attempts.get_mut(&dispatch))
                    .expect("existing audit attempt");
                attempt.audit_seq = previous_seq;
                if let Some(digest) = digest {
                    attempt.audit_digests.insert(seq, digest);
                } else {
                    attempt.audit_digests.remove(&seq);
                }
                if let Some(lsn) = lsn {
                    attempt.audit_lsns.insert(seq, lsn);
                } else {
                    attempt.audit_lsns.remove(&seq);
                }
            }
            restore(&mut self.schedules, schedule_undo);
            restore(&mut self.schedule_fires, fire_undo);
            restore(&mut self.webhooks, webhook_undo);
            restore(&mut self.templates, template_undo);
            restore(&mut self.commands, command_undo);
            restore(&mut self.legacy, legacy_undo);
            if let Some(epoch) = epoch_undo {
                self.master_epoch = epoch;
            }
            for undo in value_undo.into_values() {
                self.values.restore(undo);
            }
        } else {
            self.version = 1;
            self.journal_id = tx.journal_id.clone();
            self.applied_lsn = tx.lsn;
        }
        result
    }

    fn check_values(&self, event: &Event) -> Result<()> {
        let p = &event.payload;
        let check = |v: &Value| -> Result<()> {
            self.values
                .check(&serde_json::from_value::<StoredValue>(v.clone())?)
        };
        match event.kind {
            EventKind::LegacyImport => check(&p["data"])?,
            EventKind::WorkflowUpdated => check(&p["version"]["definition"])?,
            EventKind::RunStarted => {
                check(&p["input"])?;
                check(&p["definition"])?;
                let run: Run = serde_json::from_value(p.clone())?;
                for signal in run.signals.values() {
                    self.values.check(signal)?;
                }
                if let Some(execution) = &run.execution {
                    if execution.definitions.len() > 128
                        || execution.path.len() > crate::MAX_SUB_WORKFLOW_DEPTH as usize
                        || execution.runtime.len() != 64
                    {
                        return Err(invalid("invalid execution snapshot"));
                    }
                    for definition in execution.definitions.values() {
                        self.values.check(&definition.definition)?;
                    }
                    if let Some(regression) = &execution.regression {
                        for fixture in regression.fixtures.values() {
                            self.values.check(&fixture.outcome)?;
                            if let StoredValue::Inline(v) = &fixture.outcome {
                                if let Some(raw) = v.get("body_raw") {
                                    self.values.check(&StoredValue::Ref(
                                        serde_json::from_value(raw.clone())?,
                                    ))?;
                                }
                            }
                        }
                        for value in regression.signals.values() {
                            self.values.check(value)?;
                        }
                    }
                }
            }
            EventKind::InputPrepared => {
                let prepared: Prepared = serde_json::from_value(p["prepared"].clone())?;
                self.values.check(&prepared.input)?;
                self.values.check(&prepared.params)?;
                for value in prepared.predecessors.values() {
                    self.values.check(value)?;
                }
            }
            EventKind::NodeCompleted
            | EventKind::RunCompleted
            | EventKind::WaitResolved
            | EventKind::Adjudicated => check(&p["output"])?,
            EventKind::SignalReceived => check(&p["payload"])?,
            EventKind::OperationIntent | EventKind::OperationAuthorized => {
                check(&p["operation"]["request"])?
            }
            EventKind::OperationOutcome => {
                let outcome: StoredValue = serde_json::from_value(p["outcome"].clone())?;
                self.values.check(&outcome)?;
                if let StoredValue::Inline(value) = outcome {
                    if let Some(raw) = value.get("body_raw") {
                        self.values
                            .check(&StoredValue::Ref(serde_json::from_value(raw.clone())?))?;
                    }
                }
            }
            EventKind::WaitRegistered => {
                let wait: Wait = serde_json::from_value(p["wait"].clone())?;
                if let Some(value) = wait.output() {
                    self.values.check(value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_event(
        &mut self,
        event: &Event,
        lsn: u64,
        command_undo: &mut BTreeMap<String, Option<CommandRecord>>,
    ) -> Result<()> {
        let p = &event.payload;
        if event.kind == EventKind::LateAudit && (event.run_seq != 0 || event.audit_seq == 0) {
            return Err(invalid(
                "late evidence requires audit identity and cannot advance run sequence",
            ));
        }
        match event.kind {
            EventKind::SegmentStarted | EventKind::SegmentSealed => return Ok(()),
            EventKind::MasterEpochStarted => {
                self.master_epoch = number(p, "epoch")?;
                return Ok(());
            }
            EventKind::Command => {
                let mut command: CommandRecord = serde_json::from_value(p.clone())?;
                command.lsn = lsn;
                // Also folds old logs: the Command and RunStarted facts already
                // contain the stable slot identity and result. No business code
                // runs during this migration/replay.
                if let Some(key) = command.scope.strip_prefix("run.start:schedule:") {
                    let at = command
                        .request_id
                        .as_deref()
                        .and_then(|id| chrono::DateTime::parse_from_rfc3339(id).ok());
                    let run = command.result["run_id"]
                        .as_str()
                        .and_then(|id| self.runs.get(id));
                    if let (Some(at), Some(run)) = (at, run) {
                        if self.schedules.contains_key(key)
                            && run.source == "schedule"
                            && run.source_detail.as_deref() == Some(key)
                            && self.schedule_fires.get(key).is_none_or(|fire| at > fire.at)
                        {
                            self.schedule_fires.insert(
                                key.to_owned(),
                                ScheduleFire {
                                    at: at.with_timezone(&chrono::Utc),
                                    command: command.clone(),
                                },
                            );
                        }
                    }
                }
                if let Some(id) = &command.request_id {
                    let key = command_key(&command.scope, id);
                    if self.commands.contains_key(&key) {
                        return Err(invalid("duplicate committed command identity"));
                    }
                    self.commands.insert(key, command);
                    // 幂等窗口有界：回ceipt 保留最近 COMMAND_WINDOW 条
                    //（按 lsn 序淘汰最旧一半），长跑服务内存不随命令数
                    // 无界增长。重放确定性：阈值与淘汰规则只依赖事件序。
                    if self.commands.len() > COMMAND_WINDOW {
                        let mut order: Vec<(u64, String)> = self
                            .commands
                            .iter()
                            .map(|(key, record)| (record.lsn, key.clone()))
                            .collect();
                        order.sort_unstable();
                        let evict = self.commands.len() - COMMAND_WINDOW / 2;
                        for (_, key) in order.into_iter().take(evict) {
                            if let Some(record) = self.commands.remove(&key) {
                                command_undo.entry(key).or_insert(Some(record));
                            }
                        }
                    }
                }
                return Ok(());
            }
            EventKind::WorkflowCreated => {
                let workflow: Workflow = serde_json::from_value(p.clone())?;
                if self.workflows.contains_key(&workflow.workflow_id) {
                    return Err(invalid("workflow already exists"));
                }
                self.workflows
                    .insert(workflow.workflow_id.clone(), workflow);
                return Ok(());
            }
            EventKind::WorkflowUpdated => {
                let id = string(p, "workflow_id")?;
                let version: DefinitionVersion = serde_json::from_value(p["version"].clone())?;
                let workflow = self
                    .workflows
                    .get_mut(&id)
                    .ok_or_else(|| invalid("missing workflow"))?;
                if version.version != workflow.versions.last_key_value().map_or(1, |(v, _)| v + 1) {
                    return Err(invalid("non-contiguous definition version"));
                }
                workflow.versions.insert(version.version, version);
                return Ok(());
            }
            EventKind::WorkflowPublished => {
                let id = string(p, "workflow_id")?;
                let v = number(p, "version")?;
                let version = self
                    .workflows
                    .get_mut(&id)
                    .and_then(|w| w.versions.get_mut(&v))
                    .ok_or_else(|| invalid("missing workflow version"))?;
                version.published = true;
                return Ok(());
            }
            EventKind::WorkflowDeleted => {
                self.workflows.remove(&string(p, "workflow_id")?);
                return Ok(());
            }
            EventKind::ScheduleChanged => {
                if p["deleted"] == true {
                    self.schedule_fires.remove(&string(p, "id")?);
                }
                replace_config(&mut self.schedules, p, "id")?;
                return Ok(());
            }
            EventKind::WebhookChanged => {
                replace_config(&mut self.webhooks, p, "token")?;
                return Ok(());
            }
            EventKind::TemplateChanged => {
                replace_config(&mut self.templates, p, "id")?;
                return Ok(());
            }
            EventKind::RunStarted => {
                let mut run: Run = serde_json::from_value(p.clone())?;
                if event.run_id.as_deref() != Some(run.run_id.as_str())
                    || event.run_seq != 1
                    || self.runs.contains_key(&run.run_id)
                {
                    return Err(invalid("invalid run creation"));
                }
                run.last_run_seq = 1;
                self.run_workflows.insert(run.workflow_id.clone());
                self.runs.insert(run.run_id.clone(), run);
                return Ok(());
            }
            // Master-produced values precede their referencing business fact and have no run.
            EventKind::ValueChunk | EventKind::ValuePublished if event.run_id.is_none() => {
                return Ok(());
            }
            EventKind::LegacyImport => {
                let key = string(p, "key")?;
                if self.legacy.contains_key(&key) {
                    return Err(invalid("duplicate legacy baseline key"));
                }
                self.legacy.insert(key, p.clone());
                return Ok(());
            }
            _ => {}
        }
        let run = self
            .runs
            .get_mut(
                event
                    .run_id
                    .as_deref()
                    .ok_or_else(|| invalid("run identity required"))?,
            )
            .ok_or_else(|| invalid("missing run"))?;
        if event.run_seq > 0 {
            if event.run_seq != run.last_run_seq + 1 {
                return Err(invalid("non-contiguous run sequence"));
            }
            run.last_run_seq = event.run_seq;
        }
        match event.kind {
            EventKind::RunCancelled => {
                run.status = RunStatus::Cancelled;
                run.error = None;
                return Ok(());
            }
            EventKind::RunCompleted => {
                if run.terminal() {
                    return Err(invalid("terminal run completion"));
                }
                run.status = RunStatus::Succeeded;
                run.output = Some(serde_json::from_value(p["output"].clone())?);
                return Ok(());
            }
            EventKind::RunFailed => {
                if !run.terminal() {
                    run.status = RunStatus::Failed;
                    run.error = Some(string(p, "error")?);
                }
                return Ok(());
            }
            EventKind::SignalReceived => {
                if p["queued"] == true {
                    if run.terminal() {
                        return Err(invalid("signal on terminal run"));
                    }
                    let id = event
                        .node_id
                        .clone()
                        .ok_or_else(|| invalid("signal node required"))?;
                    if run
                        .signals
                        .insert(id, serde_json::from_value(p["payload"].clone())?)
                        .is_some()
                    {
                        return Err(invalid("signal inbox slot already occupied"));
                    }
                }
                return Ok(());
            }
            _ => {}
        }
        let node_id = event
            .node_id
            .clone()
            .map(Ok)
            .unwrap_or_else(|| string(p, "node_id"))?;
        if event.kind == EventKind::DispatchStarted {
            if run.terminal() {
                return Err(invalid("dispatch on terminal run"));
            }
            let dispatch_id = event
                .dispatch_id
                .clone()
                .ok_or_else(|| invalid("missing dispatch"))?;
            let execution = string(p, "node_execution_id")?;
            let attempt =
                u32::try_from(number(p, "attempt")?).map_err(|_| invalid("attempt overflow"))?;
            let node = run.nodes.entry(node_id).or_insert_with(|| Node {
                node_execution_id: execution.clone(),
                attempt: 0,
                dispatch_id: String::new(),
                status: NodeStatus::Pending,
                output: None,
                branch: None,
                prepared: None,
                wait: None,
                operation: None,
                attempts: BTreeMap::new(),
                error: None,
                skip_reason: None,
            });
            if node.node_execution_id != execution
                || attempt != node.attempt + 1
                || node.operation.as_ref().is_some_and(|o| o.outcome.is_none())
            {
                return Err(invalid("unsafe dispatch retry"));
            }
            run.status = RunStatus::Running;
            node.attempt = attempt;
            node.dispatch_id = dispatch_id.clone();
            node.status = NodeStatus::Running;
            node.wait = None;
            node.attempts.insert(
                dispatch_id,
                Attempt {
                    audit_seq: 0,
                    audit_digests: BTreeMap::new(),
                    audit_lsns: BTreeMap::new(),
                    result: None,
                    result_lsn: None,
                    sealed: false,
                    integrity: "unknown".into(),
                },
            );
            return Ok(());
        }
        if event.kind == EventKind::NodeSkipped {
            run.nodes.insert(
                node_id,
                Node {
                    node_execution_id: String::new(),
                    attempt: 0,
                    dispatch_id: String::new(),
                    status: NodeStatus::Skipped,
                    output: None,
                    branch: None,
                    prepared: None,
                    wait: None,
                    operation: None,
                    attempts: BTreeMap::new(),
                    error: None,
                    // 跳过原因只在事件里携带；折影保留一份供时间线展示
                    //（upstream_failed / upstream_skipped / branch_not_taken …）
                    skip_reason: p.get("reason").and_then(Value::as_str).map(str::to_owned),
                },
            );
            return Ok(());
        }
        if event.kind == EventKind::InputPrepared {
            let prepared: Prepared = serde_json::from_value(p["prepared"].clone())?;
            if prepared.input != run.input || !matches!(prepared.node_semantics, 1 | 2) {
                return Err(invalid(
                    "prepared input/semantics differs from committed run",
                ));
            }
            for (id, value) in &prepared.predecessors {
                if run.nodes.get(id).is_none_or(|n| {
                    n.status != NodeStatus::Succeeded || n.output.as_ref() != Some(value)
                }) {
                    return Err(invalid(
                        "prepared predecessor differs from committed output",
                    ));
                }
            }
        }
        let terminal = run.terminal();
        let node = run
            .nodes
            .get_mut(&node_id)
            .ok_or_else(|| invalid("missing node dispatch"))?;
        if event.dispatch_id.as_deref() != Some(node.dispatch_id.as_str())
            && event.kind != EventKind::LateAudit
        {
            return Err(invalid("stale dispatch cannot alter current node"));
        }
        if event.audit_seq > 0 {
            let dispatch = event
                .dispatch_id
                .as_deref()
                .ok_or_else(|| invalid("missing audit dispatch"))?;
            let attempt = node
                .attempts
                .get_mut(dispatch)
                .ok_or_else(|| invalid("unknown audit dispatch"))?;
            if event.audit_seq != attempt.audit_seq + 1
                || (attempt.sealed && event.kind != EventKind::LateAudit)
            {
                return Err(invalid("audit gap/duplicate or sealed dispatch"));
            }
            attempt.audit_seq = event.audit_seq;
            if !self.compact_audits {
                attempt.audit_lsns.insert(event.audit_seq, lsn);
                attempt.audit_digests.insert(
                    event.audit_seq,
                    flow_journal::codec::digest(&flow_journal::codec::bounded_json(
                        event,
                        flow_journal::MAX_LINE_BYTES,
                    )?),
                );
            }
        }
        match event.kind {
            EventKind::InputPrepared => {
                if node.prepared.is_some() {
                    return Err(invalid("input already prepared"));
                }
                node.prepared = Some(serde_json::from_value(p["prepared"].clone())?);
            }
            EventKind::OperationIntent => {
                if node.prepared.is_none() {
                    return Err(invalid("operation before input"));
                }
            }
            EventKind::OperationAuthorized => {
                if terminal || node.operation.is_some() {
                    return Err(invalid("operation already authorized/cancelled"));
                }
                let operation: Operation = serde_json::from_value(p["operation"].clone())?;
                let digest = match &operation.request {
                    StoredValue::Inline(value) => flow_journal::codec::digest(
                        &flow_journal::codec::bounded_json(value, flow_journal::INLINE_BYTES)?,
                    ),
                    StoredValue::Ref(value) => value.digest.clone(),
                };
                if operation.operation_id.is_empty()
                    || operation.permit_id.is_empty()
                    || operation.fingerprint != digest
                    || operation.outcome.is_some()
                {
                    return Err(invalid("invalid operation identity/request fingerprint"));
                }
                node.operation = Some(operation);
            }
            EventKind::OperationOutcome => {
                let op = node
                    .operation
                    .as_mut()
                    .ok_or_else(|| invalid("outcome before authorization"))?;
                if op.outcome.is_some() {
                    return Err(invalid("operation already has an outcome"));
                }
                op.outcome = Some(serde_json::from_value(p["outcome"].clone())?);
            }
            EventKind::NodeCompleted | EventKind::WaitRegistered | EventKind::NodeFailed => {
                let n = number(p, "sealed_through")?;
                let attempt = node
                    .attempts
                    .get_mut(&node.dispatch_id)
                    .ok_or_else(|| invalid("missing attempt"))?;
                if n != attempt.audit_seq
                    || (node.prepared.is_none() && p["preparation_failed"] != true)
                {
                    return Err(invalid("result audit barrier"));
                }
                attempt.sealed = true;
                attempt.result = Some(event.clone());
                attempt.result_lsn = Some(lsn);
                attempt.integrity = p["integrity"].as_str().unwrap_or("complete").into();
                if terminal {
                    return Ok(());
                }
                match event.kind {
                    EventKind::NodeCompleted => {
                        node.status = NodeStatus::Succeeded;
                        node.output = Some(serde_json::from_value(p["output"].clone())?);
                        node.branch = p.get("branch").and_then(Value::as_bool);
                    }
                    EventKind::WaitRegistered => {
                        node.status = NodeStatus::Waiting;
                        node.wait = Some(serde_json::from_value(p["wait"].clone())?);
                        // 与 v1 词汇对齐：正常业务等待（delay/human/child/
                        // retry）期间 run 仍是 running；只有不确定外部结果
                        // （平台故障面）才进入 awaiting_resume——同词同义，
                        // 运维看到 awaiting_resume 即代表需要人工介入。
                        run.status = if wait_kind_uncertain(p) {
                            RunStatus::AwaitingResume
                        } else {
                            RunStatus::Running
                        };
                    }
                    _ => {
                        node.status = NodeStatus::Failed;
                        node.wait = None;
                        node.error = Some(string(p, "error")?);
                        if let Some(wake_at) = p.get("retry_wake_at").and_then(Value::as_i64) {
                            if node.operation.is_some() {
                                return Err(invalid("external operation cannot use pure retry"));
                            }
                            node.status = NodeStatus::Waiting;
                            node.wait = Some(Wait::Retry { wake_at });
                            run.status = RunStatus::Running;
                        }
                    }
                }
            }
            EventKind::WaitResolved => {
                if !terminal {
                    run.signals
                        .remove(event.node_id.as_deref().unwrap_or_default());
                    node.status = NodeStatus::Succeeded;
                    node.wait = None;
                    node.output = Some(serde_json::from_value(p["output"].clone())?);
                }
            }
            EventKind::Adjudicated => {
                let decision = p["decision"].as_str().unwrap_or_default();
                if terminal
                    || node
                        .wait
                        .as_ref()
                        .is_none_or(|w| w.kind() != WaitKind::Uncertain)
                    || node
                        .operation
                        .as_ref()
                        .is_none_or(|o| o.outcome.is_some() || p["operation_id"] != o.operation_id)
                    || p["reason"].as_str().is_none_or(|s| s.trim().is_empty())
                    || !matches!(decision, "accept_output" | "retry" | "failed")
                {
                    return Err(invalid("invalid manual resolution"));
                }
                match decision {
                    // 人工显式接受产出（不伪造外部响应被捕获的事实）
                    "accept_output" => {
                        node.status = NodeStatus::Succeeded;
                        node.wait = None;
                        node.output = Some(serde_json::from_value(p["output"].clone())?);
                    }
                    // 人工显式授权重发：清除未决操作，节点回 pending 等驱动器
                    // 以 attempt+1 重新派发（绝不自动重发的禁令针对机器，人工
                    // 裁决就是授权凭据）。审计历史保留在 attempts 里。
                    "retry" => {
                        node.status = NodeStatus::Pending;
                        node.wait = None;
                        node.operation = None;
                        node.error = None;
                        node.output = None;
                    }
                    // 人工判定失败：节点终态 failed，run 由收尾判定写 RunFailed
                    _ => {
                        node.status = NodeStatus::Failed;
                        node.wait = None;
                        node.error = Some(string(p, "reason")?);
                    }
                }
                // 裁决即人工介入完成：awaiting_resume 回到 running（终态由
                // 后续 RunCompleted/RunFailed 事件落定）
                run.status = RunStatus::Running;
            }
            EventKind::OperationRetryAuthorized => {
                if terminal
                    || node.prepared.is_none()
                    || node.operation.as_ref().is_none_or(|o| {
                        o.outcome.is_some()
                            || p["operation_id"] != o.operation_id
                            || p["fingerprint"] != o.fingerprint
                    })
                    || p["policy"] != "repeatable"
                {
                    return Err(invalid("invalid repeatable-operation retry authorization"));
                }
                node.operation = None;
                node.status = NodeStatus::Waiting;
                node.wait = Some(Wait::Retry {
                    wake_at: p["wake_at"]
                        .as_i64()
                        .ok_or_else(|| invalid("retry deadline required"))?,
                });
                node.error = None;
                run.status = RunStatus::Running;
            }
            EventKind::ValueChunk | EventKind::ValuePublished | EventKind::LateAudit => {}
            _ => return Err(invalid("unsupported event for node")),
        }
        Ok(())
    }
}
fn restore<T>(map: &mut BTreeMap<String, T>, undo: BTreeMap<String, Option<T>>) {
    for (key, value) in undo {
        match value {
            Some(v) => {
                map.insert(key, v);
            }
            None => {
                map.remove(&key);
            }
        }
    }
}

/// WaitRegistered 载荷的等待是否「不确定外部结果」（uncertain）。
fn wait_kind_uncertain(payload: &Value) -> bool {
    payload["wait"]["kind"].as_str() == Some(WaitKind::Uncertain.as_str())
}
fn replace_config(map: &mut BTreeMap<String, Value>, payload: &Value, key: &str) -> Result<()> {
    let id = string(payload, key)?;
    if payload.get("deleted") == Some(&Value::Bool(true)) {
        map.remove(&id);
    } else {
        map.insert(id, payload.clone());
    }
    Ok(())
}
pub fn string(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("missing string {key}")))
}
pub fn number(v: &Value, key: &str) -> Result<u64> {
    v.get(key)
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid(format!("missing decimal {key}")))
}

pub fn validate_definition(value: &Value) -> Result<Definition> {
    let definition: Definition = serde_json::from_value(value.clone())
        .map_err(|e| invalid(format!("definition 结构不符：{e}")))?;
    definition.validate().map_err(|e| invalid(e.to_string()))?;
    Ok(definition)
}

#[cfg(test)]
mod command_window_tests {
    use super::*;
    fn full_window() -> State {
        let mut state = State {
            journal_id: "window".into(),
            applied_lsn: COMMAND_WINDOW as u64,
            ..Default::default()
        };
        for n in 0..COMMAND_WINDOW {
            let id = n.to_string();
            state.commands.insert(
                command_key("test", &id),
                CommandRecord {
                    scope: "test".into(),
                    request_id: Some(id),
                    fingerprint: "f".into(),
                    result: serde_json::json!(n),
                    lsn: n as u64 + 1,
                },
            );
        }
        state
    }
    fn next() -> Transaction {
        Transaction {
            v: 2,
            journal_id: "window".into(),
            lsn: COMMAND_WINDOW as u64 + 1,
            tx_id: "next".into(),
            events: vec![Event::new(
                EventKind::Command,
                serde_json::json!({"scope":"test","request_id":"next","fingerprint":"f","result":null,"lsn":"0"}),
            )],
        }
    }
    #[test]
    fn checking_and_rollback_restore_evicted_identities() {
        let mut state = full_window();
        let before = serde_json::to_value(&state).unwrap();
        let tx = next();
        state.check(&tx).unwrap();
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        let mut invalid = tx.clone();
        invalid.events.push(Event::new(
            EventKind::WorkflowDeleted,
            serde_json::json!({}),
        ));
        assert!(state.apply(&invalid).is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        state.apply(&tx).unwrap();
        assert_eq!(state.commands.len(), COMMAND_WINDOW / 2);
        let mut replay = full_window();
        replay.apply(&tx).unwrap();
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            serde_json::to_value(&replay).unwrap()
        );
    }
}

#[cfg(test)]
mod aggregate_status_tests {
    use super::*;
    #[test]
    fn unresolved_parallel_waits_survive_other_branch_activity_and_terminal_wins() {
        let mut run: Run = serde_json::from_value(serde_json::json!({
            "run_id":"r", "workflow_id":"w", "workflow_version":1,
            "definition":serde_json::to_value(StoredValue::Inline(Value::Null)).unwrap(), "input":serde_json::to_value(StoredValue::Inline(Value::Null)).unwrap(),
            "source":"manual", "source_detail":null, "parent":null, "depth":0,
            "created_at":"now", "status":"running", "output":null, "error":null,
            "last_run_seq":"1", "nodes":{}
        })).unwrap();
        let node = |wait: bool| Node {
            node_execution_id: "n".into(),
            attempt: 1,
            dispatch_id: "d".into(),
            status: if wait {
                NodeStatus::Waiting
            } else {
                NodeStatus::Running
            },
            output: None,
            branch: None,
            prepared: None,
            wait: if wait { Some(Wait::Uncertain) } else { None },
            operation: None,
            attempts: BTreeMap::new(),
            error: None,
            skip_reason: None,
        };
        run.nodes.insert("a".into(), node(true));
        run.nodes.insert("b".into(), node(true));
        run.refresh_status();
        assert_eq!(run.status, RunStatus::AwaitingResume);
        run.nodes.get_mut("b").unwrap().wait = None;
        run.status = RunStatus::Running;
        run.refresh_status();
        assert_eq!(run.status, RunStatus::AwaitingResume);
        run.nodes.get_mut("a").unwrap().wait = None;
        run.refresh_status();
        assert_eq!(run.status, RunStatus::Running);
        run.nodes.insert("a".into(), node(true));
        run.status = RunStatus::Cancelled;
        run.refresh_status();
        assert_eq!(run.status, RunStatus::Cancelled);
    }
}

#[cfg(test)]
mod wait_compat_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn legacy_wait_shapes_round_trip_and_illegal_combinations_fail() {
        for (kind, wake_at, child_run_id) in [
            ("delay", json!(123), Value::Null),
            ("retry", json!(456), Value::Null),
            ("child", Value::Null, json!("child-1")),
            ("signal", Value::Null, Value::Null),
            ("uncertain", Value::Null, Value::Null),
        ] {
            let old =
                json!({"kind":kind,"wake_at":wake_at,"child_run_id":child_run_id,"output":null});
            let wait: Wait = serde_json::from_value(old.clone()).unwrap();
            assert_eq!(serde_json::to_value(wait).unwrap(), old);
        }
        for bad in [
            json!({"kind":"delay"}),
            json!({"kind":"retry"}),
            json!({"kind":"child"}),
            json!({"kind":"child","child_run_id":""}),
            json!({"kind":"signal","wake_at":1}),
            json!({"kind":"uncertain","child_run_id":"c"}),
            json!({"kind":"retry","wake_at":1,"output":{"type":"inline","value":null}}),
        ] {
            assert!(
                serde_json::from_value::<Wait>(bad.clone()).is_err(),
                "{bad}"
            );
        }
        let signal: Wait = serde_json::from_value(json!({"kind":"signal"})).unwrap();
        assert_eq!(signal.kind(), WaitKind::Signal);
    }
}
