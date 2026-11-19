//! In-memory workflow runner sharing graph decisions and node outcomes with
//! the managed coordinator. Script execution stays in an IPC worker.

use std::{collections::BTreeMap, future, io, time::Duration};

use flow_dispatcher::{
    execution_protocol::STARTUP_TIMEOUT_MS, Dispatcher, Job, JobError, MAX_STREAMED_JOB_BYTES,
};
use flow_domain::{
    journal_state::RunStatus,
    progression::{node_gate, run_settlement, NodeGate, NodeOutcome, RunSettlement},
    schema,
    script::{truthy, ScriptTask, SCRIPT_V1_HANDLER},
    Definition, Node, NodeType,
};
use flow_facts::{codec::bounded_json, Error, Result, StoredValue, INLINE_BYTES};
use serde::Serialize;
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub run_id: String,
    pub created_at: String,
    pub status: RunStatus,
    pub output: Option<StoredValue>,
    pub error: Option<String>,
    pub nodes: BTreeMap<String, NodeOutcome>,
}

/// Run an inline JSON workflow with Start, Script, Condition and End nodes.
/// The dispatcher must be configured for streamed `script.v1` jobs.
pub async fn run(
    definition: &Definition,
    input: Value,
    dispatcher: &Dispatcher,
    cancel: &CancellationToken,
) -> Result<RunReport> {
    validate(definition, &input)?;
    bounded_json(definition, INLINE_BYTES)?;
    let mut memory = MemoryRun {
        id: uuid::Uuid::now_v7().to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        input: StoredValue::inline(input)?,
        nodes: BTreeMap::new(),
    };
    loop {
        let mut advanced = false;
        for node in &definition.nodes {
            if cancel.is_cancelled() {
                return Ok(memory.report(RunStatus::Cancelled, None, None));
            }
            match node_gate(definition, &memory.nodes, node) {
                NodeGate::Blocked => continue,
                NodeGate::Skip(reason) => {
                    memory.accept(
                        node,
                        NodeOutcome::Skipped {
                            skip_reason: Some(reason.into()),
                        },
                    )?;
                }
                NodeGate::Ready(predecessors) => {
                    let kind = node.kind().expect("validated node kind");
                    match kind {
                        NodeType::Start => {
                            memory.complete_node(node, Ok(memory.input.clone()), None)?;
                        }
                        NodeType::End => {
                            let outcome = join(predecessors)
                                .and_then(StoredValue::inline)
                                .map_err(|error| error.to_string());
                            memory.complete_node(node, outcome, None)?;
                        }
                        NodeType::Script | NodeType::Condition => {
                            match memory
                                .dispatch_script(dispatcher, cancel, node, kind, predecessors)
                                .await
                            {
                                Ok(_) => {}
                                Err(JobError::Cancelled) => {
                                    return Ok(memory.report(RunStatus::Cancelled, None, None));
                                }
                                Err(error) => return Err(Error::Unavailable(error.to_string())),
                            }
                        }
                        _ => unreachable!("validated node kind"),
                    }
                }
            }
            advanced = true;
        }
        match run_settlement(definition, &memory.nodes) {
            Some(RunSettlement::Failed(error)) => {
                return Ok(memory.report(RunStatus::Failed, None, error));
            }
            Some(RunSettlement::Completed(ends)) => {
                let output = join(ends).and_then(|value| {
                    if let Some(schema) = &definition.output_schema {
                        schema::validate(schema, &value)
                            .map_err(|error| Error::Invalid(format!("output contract: {error}")))?;
                    }
                    StoredValue::inline(value)
                });
                return Ok(match output {
                    Ok(value) => memory.report(RunStatus::Succeeded, Some(value), None),
                    Err(error) => memory.report(RunStatus::Failed, None, Some(error.to_string())),
                });
            }
            None if !advanced => return Err(Error::Invalid("workflow cannot advance".into())),
            None => {}
        }
    }
}

fn validate(definition: &Definition, input: &Value) -> Result<()> {
    definition.validate().map_err(Error::Invalid)?;
    if definition.permissions.is_some() {
        return Err(Error::Invalid(
            "workflow permissions require managed execution".into(),
        ));
    }
    for node in &definition.nodes {
        match node.kind() {
            Some(NodeType::Start | NodeType::End) => {}
            Some(NodeType::Script | NodeType::Condition) => {
                if node.retry().max_attempts > 1 {
                    return Err(Error::Invalid(format!(
                        "node {} requires managed retry scheduling",
                        node.id
                    )));
                }
                if node
                    .params
                    .get("timeout_ms")
                    .is_some_and(|v| !v.is_null() && !v.is_u64())
                {
                    return Err(Error::Invalid(format!(
                        "node {} requires a literal timeout_ms for inline execution",
                        node.id
                    )));
                }
            }
            _ => {
                return Err(Error::Invalid(format!(
                    "node {} ({}) requires managed execution",
                    node.id, node.node_type
                )))
            }
        }
    }
    if let Some(schema) = &definition.input_schema {
        bounded_json(input, INLINE_BYTES)?;
        schema::validate(schema, input)
            .map_err(|error| Error::Invalid(format!("input contract: {error}")))?;
    }
    Ok(())
}

fn inline_json(value: StoredValue) -> Result<Value> {
    match value {
        StoredValue::Inline(value) => Ok(value),
        StoredValue::Ref(_) => Err(Error::Invalid(
            "inline execution requires JSON values".into(),
        )),
    }
}

fn object(values: BTreeMap<String, StoredValue>) -> Result<Value> {
    values
        .into_iter()
        .map(|(key, value)| Ok((key, inline_json(value)?)))
        .collect::<Result<serde_json::Map<_, _>>>()
        .map(Value::Object)
}

fn join(mut values: BTreeMap<String, StoredValue>) -> Result<Value> {
    match values.len() {
        0 => Ok(Value::Null),
        1 => inline_json(values.pop_first().unwrap().1),
        _ => object(values),
    }
}

struct MemoryRun {
    id: String,
    created_at: String,
    input: StoredValue,
    nodes: BTreeMap<String, NodeOutcome>,
}

impl MemoryRun {
    async fn dispatch_script(
        &mut self,
        dispatcher: &Dispatcher,
        cancel: &CancellationToken,
        node: &Node,
        kind: NodeType,
        predecessors: BTreeMap<String, StoredValue>,
    ) -> std::result::Result<(), JobError> {
        let protocol = |error: Error| JobError::Protocol(error.to_string());
        let script = ScriptTask::from_prepared(
            kind,
            &node.params,
            inline_json(self.input.clone()).map_err(protocol)?,
            object(predecessors).map_err(protocol)?,
            None,
        )
        .expect("validated compute node");
        let deadline =
            Instant::now() + Duration::from_millis(STARTUP_TIMEOUT_MS * 3) + script.timeout();
        let job = Job {
            id: uuid::Uuid::now_v7().to_string(),
            handler: SCRIPT_V1_HANDLER.into(),
            input: bounded_json(&script, MAX_STREAMED_JOB_BYTES).map_err(protocol)?,
        };
        dispatcher
            .execute(job, deadline, cancel, |completion| {
                future::ready(
                    self.complete_script(node, kind, completion.outcome)
                        .map_err(io::Error::other),
                )
            })
            .await?;
        Ok(())
    }

    fn complete_script(
        &mut self,
        node: &Node,
        kind: NodeType,
        outcome: std::result::Result<Vec<u8>, String>,
    ) -> Result<()> {
        match outcome {
            Ok(bytes) => {
                let value: Value = serde_json::from_slice(&bytes)?;
                let branch = (kind == NodeType::Condition).then(|| truthy(&value));
                let output = StoredValue::inline(value).map_err(|error| error.to_string());
                self.complete_node(node, output, branch)
            }
            Err(error) => self.complete_node(node, Err(error), None),
        }
    }

    fn complete_node(
        &mut self,
        node: &Node,
        output: std::result::Result<StoredValue, String>,
        branch: Option<bool>,
    ) -> Result<()> {
        let outcome = match output {
            Ok(output) => NodeOutcome::Succeeded { output, branch },
            Err(error) => NodeOutcome::Failed { error },
        };
        self.accept(node, outcome)
    }

    fn accept(&mut self, node: &Node, outcome: NodeOutcome) -> Result<()> {
        let std::collections::btree_map::Entry::Vacant(entry) = self.nodes.entry(node.id.clone())
        else {
            return Err(Error::Conflict(format!("node {} already settled", node.id)));
        };
        entry.insert(outcome);
        Ok(())
    }

    fn report(
        self,
        status: RunStatus,
        output: Option<StoredValue>,
        error: Option<String>,
    ) -> RunReport {
        RunReport {
            run_id: self.id,
            created_at: self.created_at,
            status,
            output,
            error,
            nodes: self.nodes,
        }
    }
}
