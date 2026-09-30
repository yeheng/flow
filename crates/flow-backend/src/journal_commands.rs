use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use flow_engine::journal_state::{DefinitionVersion, Run, Workflow};
use flow_journal::{Event, EventKind};
use serde_json::{json, Value};
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, JournalError>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}
pub(crate) fn id() -> String {
    uuid::Uuid::now_v7().to_string()
}
pub(crate) fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

impl JournalBackend {
    /// Explicit operator resolution. This records a manual decision, never fabricates an
    /// external OperationOutcome or grants permission to resend the uncertain request.
    pub async fn run_adjudicate(
        &self,
        run_id: &str,
        node_id: &str,
        operation_id: &str,
        reason: &str,
        output: Value,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        if reason.trim().is_empty() || reason.len() > 4096 {
            return Err(
                invalid("manual resolution requires a reason of at most 4096 bytes").into(),
            );
        }
        let request = json!({"run_id":run_id,"node_id":node_id,"operation_id":operation_id,"reason":reason,"output":output});
        let output =
            flow_journal::value::store_json(&self.journal, output, 8 * 1024 * 1024).await?;
        self.command(&format!("run.adjudicate:{run_id}"),Some(request_id),&request,|state|{
            let run=state.runs.get(run_id).ok_or_else(||invalid("run not found"))?;
            let node=run.nodes.get(node_id).ok_or_else(||invalid("node not found"))?;
            if run.terminal() || node.wait.as_ref().is_none_or(|w|w.kind!="uncertain")
                || node.operation.as_ref().is_none_or(|o|o.operation_id!=operation_id || o.outcome.is_some()) {
                return Err(invalid("manual resolution requires the current uncertain operation"));
            }
            let event=node_event(run,node_id,EventKind::Adjudicated,json!({"operation_id":operation_id,"reason":reason,"decision":"accept_output","output":output}),true);
            Ok((vec![event],json!({"resolved":true})))
        }).await
    }

    pub async fn workflow_create(
        &self,
        name: &str,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        if name.is_empty() || name.len() > 1024 {
            return Err(invalid("invalid workflow name").into());
        }
        self.command("workflow.create", request_id, &json!({"name":name}), |_| {
            let workflow_id = id();
            let w = Workflow {
                workflow_id: workflow_id.clone(),
                name: name.into(),
                created_at: now(),
                versions: BTreeMap::new(),
            };
            Ok((
                vec![Event::new(
                    EventKind::WorkflowCreated,
                    serde_json::to_value(w)?,
                )],
                json!({"workflow_id":workflow_id}),
            ))
        })
        .await
    }
    pub async fn workflow_update(
        &self,
        workflow_id: &str,
        definition: Value,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        flow_engine::journal_state::validate_definition(&definition)?;
        let fingerprint_request = json!({"workflow_id":workflow_id,"definition":definition});
        let checksum = flow_journal::codec::digest(&flow_journal::codec::bounded_json(
            &definition,
            flow_journal::MAX_LINE_BYTES,
        )?);
        let stored = flow_journal::value::store_json(
            &self.journal,
            definition,
            flow_journal::MAX_LINE_BYTES,
        )
        .await?;
        self.command(
            "workflow.update",
            request_id,
            &fingerprint_request,
            |state| {
                let w = state
                    .workflows
                    .get(workflow_id)
                    .ok_or_else(|| invalid("workflow not found"))?;
                let version = w.versions.last_key_value().map_or(1, |(v, _)| v + 1);
                let value = DefinitionVersion {
                    version,
                    definition: stored,
                    checksum,
                    published: false,
                    created_at: now(),
                };
                Ok((
                    vec![Event::new(
                        EventKind::WorkflowUpdated,
                        json!({"workflow_id":workflow_id,"version":value}),
                    )],
                    json!({"version":version}),
                ))
            },
        )
        .await
    }
    pub async fn workflow_publish(
        &self,
        workflow_id: &str,
        version: u64,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        self.command(
            "workflow.publish",
            request_id,
            &json!({"workflow_id":workflow_id,"version":version}),
            |state| {
                state
                    .workflows
                    .get(workflow_id)
                    .and_then(|w| w.versions.get(&version))
                    .ok_or_else(|| invalid("workflow version not found"))?;
                Ok((
                    vec![Event::new(
                        EventKind::WorkflowPublished,
                        json!({"workflow_id":workflow_id,"version":version.to_string()}),
                    )],
                    json!({"ok":true}),
                ))
            },
        )
        .await
    }
    pub async fn workflow_delete(
        &self,
        workflow_id: &str,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        self.command(
            "workflow.delete",
            request_id,
            &json!({"workflow_id":workflow_id}),
            |state| {
                if !state.workflows.contains_key(workflow_id) {
                    return Err(invalid("workflow not found"));
                }
                let mut events = vec![Event::new(
                    EventKind::WorkflowDeleted,
                    json!({"workflow_id":workflow_id}),
                )];
                for (key, value) in &state.schedules {
                    if value["workflow_id"] == workflow_id {
                        events.push(Event::new(
                            EventKind::ScheduleChanged,
                            json!({"id":key,"deleted":true}),
                        ));
                    }
                }
                for (key, value) in &state.webhooks {
                    if value["workflow_id"] == workflow_id {
                        events.push(Event::new(
                            EventKind::WebhookChanged,
                            json!({"token":key,"deleted":true}),
                        ));
                    }
                }
                Ok((events, json!({"ok":true})))
            },
        )
        .await
    }
    /// Trigger identity and run creation are one journal decision. Cron/webhook callers use
    /// a stable scope+request ID here, never a preliminary SQLite fire marker.
    pub async fn run_start(
        &self,
        workflow_id: &str,
        version: Option<u64>,
        input: Value,
        source: &str,
        source_detail: Option<&str>,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        if !flow_dto::DbRunSource::is_valid_str(source) {
            return Err(invalid("invalid run source").into());
        }
        let request = json!({"workflow_id":workflow_id,"version":version,"input":input,"source":source,"source_detail":source_detail});
        let stored = flow_journal::value::store_json(&self.journal, input, 8 * 1024 * 1024).await?;
        let scope = format!("run.start:{source}:{}", source_detail.unwrap_or(""));
        self.command(&scope, request_id, &request, |state| {
            if state.runs.values().filter(|r| !r.terminal()).count() >= 1000 {
                return Err(flow_journal::Error::Limit("R_max=1000".into()));
            }
            let workflow = state
                .workflows
                .get(workflow_id)
                .ok_or_else(|| invalid("workflow not found"))?;
            let selected = version
                .or_else(|| {
                    workflow
                        .versions
                        .iter()
                        .rev()
                        .find(|(_, v)| v.published)
                        .map(|(v, _)| *v)
                })
                .ok_or_else(|| invalid("no published version"))?;
            let definition = workflow
                .versions
                .get(&selected)
                .filter(|v| v.published)
                .ok_or_else(|| invalid("version not published"))?;
            let run_id = id();
            let run = Run {
                run_id: run_id.clone(),
                workflow_id: workflow_id.into(),
                workflow_version: selected,
                definition: definition.definition.clone(),
                input: stored,
                source: source.into(),
                source_detail: source_detail.map(str::to_owned),
                parent: None,
                depth: 0,
                created_at: now(),
                status: "running".into(),
                output: None,
                error: None,
                last_run_seq: 0,
                nodes: BTreeMap::new(),
            };
            let mut event = Event::new(EventKind::RunStarted, serde_json::to_value(run)?);
            event.run_id = Some(run_id.clone());
            event.run_seq = 1;
            Ok((
                vec![event],
                json!({"run_id":run_id,"workflow_version":selected}),
            ))
        })
        .await
    }
    pub async fn run_cancel(
        &self,
        run_id: &str,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        self.command(
            &format!("run.cancel:{run_id}"),
            request_id,
            &json!({"run_id":run_id}),
            |state| {
                let run = state
                    .runs
                    .get(run_id)
                    .ok_or_else(|| invalid("run not found"))?;
                let mut events = Vec::new();
                if !run.terminal() {
                    let mut event =
                        Event::new(EventKind::RunCancelled, json!({"cancel_children":true}));
                    event.run_id = Some(run_id.into());
                    event.run_seq = run.last_run_seq + 1;
                    events.push(event);
                }
                Ok((
                    events,
                    json!({"delivered":true,"status":"applied","signal_id":request_id}),
                ))
            },
        )
        .await
    }
    pub async fn run_signal(
        &self,
        run_id: &str,
        node_id: &str,
        payload: Value,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        let request = json!({"run_id":run_id,"node_id":node_id,"payload":payload});
        let output =
            flow_journal::value::store_json(&self.journal, payload, 8 * 1024 * 1024).await?;
        self.command(
            &format!("run.signal:{run_id}"),
            request_id,
            &request,
            |state| {
                let run = state
                    .runs
                    .get(run_id)
                    .ok_or_else(|| invalid("run not found"))?;
                let node = run
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| invalid("node not waiting"))?;
                if run.terminal() || node.wait.as_ref().is_none_or(|w| w.kind != "signal") {
                    return Err(invalid("node not waiting for signal"));
                }
                let mut signal = node_event(
                    run,
                    node_id,
                    EventKind::SignalReceived,
                    json!({"payload":output}),
                    true,
                );
                signal.run_seq = run.last_run_seq + 1;
                let mut resolved = node_event(
                    run,
                    node_id,
                    EventKind::WaitResolved,
                    json!({"output":output}),
                    true,
                );
                resolved.run_seq = run.last_run_seq + 2;
                Ok((
                    vec![signal, resolved],
                    json!({"delivered":true,"status":"applied","signal_id":request_id}),
                ))
            },
        )
        .await
    }
    pub async fn config_change(
        &self,
        scope: &str,
        kind: EventKind,
        key: &str,
        patch: Value,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        if !matches!(kind, EventKind::ScheduleChanged | EventKind::WebhookChanged) {
            return Err(invalid("invalid config kind").into());
        }
        self.command(
            scope,
            request_id,
            &json!({"key":key,"patch":patch}),
            |state| {
                let map = if kind == EventKind::ScheduleChanged {
                    &state.schedules
                } else {
                    &state.webhooks
                };
                let mut value = map
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| json!({"created_at":now()}));
                let object = patch
                    .as_object()
                    .ok_or_else(|| invalid("config must be object"))?;
                for (k, v) in object {
                    value[k] = v.clone();
                }
                value[if kind == EventKind::ScheduleChanged {
                    "id"
                } else {
                    "token"
                }] = json!(key);
                if value["deleted"] != true {
                    let workflow = value["workflow_id"]
                        .as_str()
                        .ok_or_else(|| invalid("workflow_id required"))?;
                    if !state.workflows.contains_key(workflow) {
                        return Err(invalid("workflow not found"));
                    }
                    if !value["enabled"].is_boolean() {
                        return Err(invalid("enabled flag required"));
                    }
                }
                Ok((vec![Event::new(kind, value.clone())], value))
            },
        )
        .await
    }
}

pub(crate) fn node_event(
    run: &Run,
    node_id: &str,
    kind: EventKind,
    payload: Value,
    public: bool,
) -> Event {
    let mut event = Event::new(kind, payload);
    event.run_id = Some(run.run_id.clone());
    event.node_id = Some(node_id.into());
    event.dispatch_id = run
        .nodes
        .get(node_id)
        .map(|n| n.dispatch_id.clone())
        .filter(|s| !s.is_empty());
    if public {
        event.run_seq = run.last_run_seq + 1;
    }
    event
}
