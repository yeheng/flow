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

/// 幂等回执保留窗口：超过即按 lsn 淘汰最旧一半（远大于 R_max=1000 的
/// 在飞 run 数，正常重试永远落在窗口内）。
const COMMAND_WINDOW: usize = 8192;

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
    pub status: String,
    pub output: Option<StoredValue>,
    pub error: Option<String>,
    #[serde(with = "snapshot_decimal")]
    pub last_run_seq: u64,
    pub nodes: BTreeMap<String, Node>,
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
    pub status: String,
    pub output: Option<StoredValue>,
    pub branch: Option<bool>,
    pub prepared: Option<Prepared>,
    pub wait: Option<Wait>,
    pub operation: Option<Operation>,
    pub attempts: BTreeMap<String, Attempt>,
    pub error: Option<String>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Wait {
    pub kind: String,
    pub wake_at: Option<i64>,
    pub child_run_id: Option<String>,
    #[serde(default)]
    pub output: Option<StoredValue>,
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// Rebuilt by replay before accepting commands; never trusted from serialized snapshots.
    #[serde(skip)]
    pub values: flow_journal::value::ValueCatalog,
    pub version: u32,
    pub journal_id: String,
    #[serde(with = "snapshot_decimal")]
    pub applied_lsn: u64,
    pub workflows: BTreeMap<String, Workflow>,
    pub runs: BTreeMap<String, Run>,
    pub schedules: BTreeMap<String, Value>,
    pub webhooks: BTreeMap<String, Value>,
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
        matches!(self.status.as_str(), "succeeded" | "failed" | "cancelled")
    }
}

impl State {
    /// Transaction-local staging guarantees that a malformed later event cannot expose its
    /// earlier sibling. Only affected maps/objects are touched by the committed replacement.
    pub fn apply(&mut self, tx: &Transaction) -> Result<()> {
        self.apply_mode(tx, true)
    }
    pub fn check(&mut self, tx: &Transaction) -> Result<()> {
        self.apply_mode(tx, false)
    }
    fn apply_mode(&mut self, tx: &Transaction, commit: bool) -> Result<()> {
        tx.validate()?;
        if tx.lsn != self.applied_lsn + 1
            || (!self.journal_id.is_empty() && self.journal_id != tx.journal_id)
        {
            return Err(invalid("reducer identity/LSN mismatch"));
        }
        for (index, event) in tx.events.iter().enumerate() {
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
        let mut workflow_undo = BTreeMap::new();
        let mut run_undo = BTreeMap::new();
        let mut schedule_undo = BTreeMap::new();
        let mut webhook_undo = BTreeMap::new();
        let mut command_undo = BTreeMap::new();
        let mut value_undo = BTreeMap::new();
        let mut legacy_undo = BTreeMap::new();
        let mut epoch_undo: Option<u64> = None;
        for event in &tx.events {
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
                run_undo
                    .entry(id.clone())
                    .or_insert_with(|| self.runs.get(id).cloned());
            }
            if event.kind == EventKind::ScheduleChanged {
                let id = string(&event.payload, "id")?;
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
            if event.kind == EventKind::Command {
                let c: CommandRecord = serde_json::from_value(event.payload.clone())?;
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
                self.apply_event(event, tx.lsn)?;
            }
            Ok(())
        })();
        if result.is_err() || !commit {
            restore(&mut self.workflows, workflow_undo);
            restore(&mut self.runs, run_undo);
            restore(&mut self.schedules, schedule_undo);
            restore(&mut self.webhooks, webhook_undo);
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
                if let Some(value) = wait.output {
                    self.values.check(&value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_event(&mut self, event: &Event, lsn: u64) -> Result<()> {
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
                            self.commands.remove(&key);
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
                replace_config(&mut self.schedules, p, "id")?;
                return Ok(());
            }
            EventKind::WebhookChanged => {
                replace_config(&mut self.webhooks, p, "token")?;
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
                self.runs.insert(run.run_id.clone(), run);
                return Ok(());
            }
            // Master-produced values precede their referencing business fact and have no run.
            EventKind::ValueChunk | EventKind::ValuePublished if event.run_id.is_none() => {
                return Ok(())
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
                run.status = "cancelled".into();
                run.error = None;
                return Ok(());
            }
            EventKind::RunCompleted => {
                if run.terminal() {
                    return Err(invalid("terminal run completion"));
                }
                run.status = "succeeded".into();
                run.output = Some(serde_json::from_value(p["output"].clone())?);
                return Ok(());
            }
            EventKind::RunFailed => {
                if !run.terminal() {
                    run.status = "failed".into();
                    run.error = Some(string(p, "error")?);
                }
                return Ok(());
            }
            EventKind::SignalReceived => return Ok(()),
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
                status: "pending".into(),
                output: None,
                branch: None,
                prepared: None,
                wait: None,
                operation: None,
                attempts: BTreeMap::new(),
                error: None,
            });
            if node.node_execution_id != execution
                || attempt != node.attempt + 1
                || node.operation.as_ref().is_some_and(|o| o.outcome.is_none())
            {
                return Err(invalid("unsafe dispatch retry"));
            }
            run.status = "running".into();
            node.attempt = attempt;
            node.dispatch_id = dispatch_id.clone();
            node.status = "running".into();
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
                    status: "skipped".into(),
                    output: None,
                    branch: None,
                    prepared: None,
                    wait: None,
                    operation: None,
                    attempts: BTreeMap::new(),
                    error: None,
                },
            );
            return Ok(());
        }
        if event.kind == EventKind::InputPrepared {
            let prepared: Prepared = serde_json::from_value(p["prepared"].clone())?;
            if prepared.input != run.input || prepared.node_semantics != 1 {
                return Err(invalid(
                    "prepared input/semantics differs from committed run",
                ));
            }
            for (id, value) in &prepared.predecessors {
                if run
                    .nodes
                    .get(id)
                    .is_none_or(|n| n.status != "succeeded" || n.output.as_ref() != Some(value))
                {
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
            attempt.audit_lsns.insert(event.audit_seq, lsn);
            attempt.audit_digests.insert(
                event.audit_seq,
                flow_journal::codec::digest(&flow_journal::codec::bounded_json(
                    event,
                    flow_journal::MAX_LINE_BYTES,
                )?),
            );
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
                        node.status = "succeeded".into();
                        node.output = Some(serde_json::from_value(p["output"].clone())?);
                        node.branch = p.get("branch").and_then(Value::as_bool);
                    }
                    EventKind::WaitRegistered => {
                        node.status = "waiting".into();
                        node.wait = Some(serde_json::from_value(p["wait"].clone())?);
                        // 与 v1 词汇对齐：正常业务等待（delay/human/child/
                        // retry）期间 run 仍是 running；只有不确定外部结果
                        // （平台故障面）才进入 awaiting_resume——同词同义，
                        // 运维看到 awaiting_resume 即代表需要人工介入。
                        run.status = if wait_kind_uncertain(&p) {
                            "awaiting_resume".into()
                        } else {
                            "running".into()
                        };
                    }
                    _ => {
                        node.status = "failed".into();
                        node.error = Some(string(p, "error")?);
                        if let Some(wake_at) = p.get("retry_wake_at").and_then(Value::as_i64) {
                            if node.operation.is_some() {
                                return Err(invalid("external operation cannot use pure retry"));
                            }
                            node.status = "waiting".into();
                            node.wait = Some(Wait {
                                kind: "retry".into(),
                                wake_at: Some(wake_at),
                                child_run_id: None,
                                output: None,
                            });
                            run.status = "running".into();
                        }
                    }
                }
            }
            EventKind::WaitResolved => {
                if !terminal {
                    node.status = "succeeded".into();
                    node.wait = None;
                    node.output = Some(serde_json::from_value(p["output"].clone())?);
                }
            }
            EventKind::Adjudicated => {
                if terminal
                    || node.wait.as_ref().is_none_or(|w| w.kind != "uncertain")
                    || node
                        .operation
                        .as_ref()
                        .is_none_or(|o| o.outcome.is_some() || p["operation_id"] != o.operation_id)
                    || p["decision"] != "accept_output"
                    || p["reason"].as_str().is_none_or(|s| s.trim().is_empty())
                {
                    return Err(invalid("invalid manual resolution"));
                }
                node.status = "succeeded".into();
                node.wait = None;
                node.output = Some(serde_json::from_value(p["output"].clone())?);
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
    payload["wait"]["kind"].as_str() == Some("uncertain")
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
    let definition: Definition = serde_json::from_value(value.clone())?;
    definition.validate().map_err(|e| invalid(e.to_string()))?;
    Ok(definition)
}
