use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use flow_engine::journal_state::{DefinitionVersion, Run, State, Workflow};
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

/// 命令幂等前置检查：命中已提交回执直接返回，**不重复 store 输入值**。
/// store_json 每次都生成新 output_id，重试若先 store 再去重，journal 里
/// 会累积无主的值垃圾；顺序重试经此检查零垃圾（同键并发的窄窗口仍由
/// command() 串行去重兜底）。
async fn deduped(
    backend: &JournalBackend,
    scope: &str,
    request_id: Option<&str>,
    request: &Value,
) -> Result<Option<CommandReceipt>> {
    backend.deduped_command(scope, request_id, request).await
}
impl JournalBackend {
    /// Product CRUD uses the caller's identity and arguments for the durable command.
    /// IDs, timestamps, existence checks and the mutation are decided under the same lock.
    pub async fn product_command(
        &self,
        method: &str,
        params: &Value,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        self.command(method, Some(request_id), params, |state| {
            let field = |key: &str| {
                params[key]
                    .as_str()
                    .ok_or_else(|| invalid(format!("missing string {key}")))
            };
            let (namespace, action) = method
                .split_once('.')
                .ok_or_else(|| invalid("invalid product command"))?;
            let creating = action == "create";
            let deleting = action == "delete";
            let key = if creating {
                if namespace == "webhook" {
                    uuid::Uuid::now_v7().simple().to_string()
                } else {
                    id()
                }
            } else {
                field(if namespace == "webhook" {
                    "token"
                } else {
                    "id"
                })?
                .to_owned()
            };
            if key.is_empty() || key.len() > 128 {
                return Err(invalid("invalid entity key"));
            }
            let map = match namespace {
                "schedule" => &state.schedules,
                "webhook" => &state.webhooks,
                "template" => &state.templates,
                _ => return Err(invalid("invalid product command")),
            };
            if !creating && !map.contains_key(&key) {
                if namespace == "template" && deleting {
                    return Ok((vec![], json!({"deleted":false})));
                }
                return Err(invalid(format!("{namespace} not found")));
            }
            let mut patch = serde_json::Map::new();
            if deleting {
                patch.insert("deleted".into(), json!(true));
            } else {
                match (namespace, action) {
                    ("schedule", "create" | "update") => {
                        if creating {
                            patch.insert("workflow_id".into(), json!(field("workflow_id")?));
                            patch.insert(
                                "enabled".into(),
                                json!(params["enabled"].as_bool().unwrap_or(true)),
                            );
                            patch.insert("input".into(), params["input"].clone());
                            patch.insert("cron_expr".into(), json!(field("cron")?));
                        } else {
                            for (source, target) in [
                                ("cron", "cron_expr"),
                                ("input", "input"),
                                ("enabled", "enabled"),
                            ] {
                                if let Some(value) = params.get(source) {
                                    patch.insert(target.into(), value.clone());
                                }
                            }
                        }
                    }
                    ("webhook", "create") => {
                        patch.insert("workflow_id".into(), json!(field("workflow_id")?));
                        patch.insert("enabled".into(), json!(true));
                    }
                    ("webhook", "set_enabled") => {
                        patch.insert(
                            "enabled".into(),
                            params
                                .get("enabled")
                                .filter(|v| v.is_boolean())
                                .cloned()
                                .ok_or_else(|| invalid("enabled flag required"))?,
                        );
                    }
                    ("template", "create" | "update") => {
                        for key in ["name", "category", "nodes", "edges"] {
                            if let Some(value) = params.get(key) {
                                patch.insert(key.into(), value.clone());
                            }
                        }
                        if creating {
                            patch.entry("category").or_insert(Value::Null);
                        }
                    }
                    _ => return Err(invalid("invalid product command")),
                }
            }
            let (events, mut value) = if namespace == "template" {
                template_decision(state, &key, &Value::Object(patch))?
            } else {
                trigger_decision(
                    state,
                    if namespace == "schedule" {
                        EventKind::ScheduleChanged
                    } else {
                        EventKind::WebhookChanged
                    },
                    &key,
                    &Value::Object(patch),
                )?
            };
            let result = if deleting {
                json!({"deleted":true})
            } else if namespace == "webhook" && !creating {
                json!({"updated":true})
            } else if namespace == "schedule" {
                let next = value["cron_expr"]
                    .as_str()
                    .and_then(|expr| expr.parse::<cron_parser::Schedule>().ok())
                    .and_then(|cron| cron.next_after(&chrono::Local::now()))
                    .map(|t| t.to_rfc3339());
                value["next_fire_at"] = json!(next);
                if creating {
                    value
                } else {
                    json!({"updated":true,"schedule":value})
                }
            } else {
                value
            };
            Ok((events, result))
        })
        .await
    }

    pub async fn trigger_configs(&self, source: &str) -> Vec<Value> {
        self.inspect(|s| {
            if source == "schedule" {
                s.schedules.values().cloned().collect()
            } else {
                s.webhooks.values().cloned().collect()
            }
        })
        .await
    }

    pub async fn trigger_start(
        &self,
        source: &str,
        key: &str,
        request_id: &str,
        input: Value,
    ) -> Result<CommandReceipt> {
        if !matches!(source, "schedule" | "webhook") {
            return Err(invalid("invalid trigger source").into());
        }
        // A retry remains attached even if the operator subsequently edits/deletes the trigger.
        if let Some(receipt) = self
            .command_status(&format!("run.start:{source}:{key}"), request_id)
            .await?
        {
            let run_id = receipt.result["run_id"]
                .as_str()
                .ok_or_else(|| invalid("invalid trigger receipt"))?;
            let run = self
                .inspect(|s| s.runs.get(run_id).cloned())
                .await
                .ok_or_else(|| invalid("missing committed trigger run"))?;
            let input = if source == "schedule" {
                flow_journal::value::materialize(
                    self.journal.root(),
                    self.journal.durable_lsn(),
                    &run.input,
                    8 * 1024 * 1024,
                )?
            } else {
                input
            };
            return self
                .run_start(
                    &run.workflow_id,
                    None,
                    input,
                    source,
                    Some(key),
                    Some(request_id),
                )
                .await;
        }
        let config = self
            .inspect(|s| {
                if source == "schedule" {
                    s.schedules.get(key).cloned()
                } else {
                    s.webhooks.get(key).cloned()
                }
            })
            .await
            .ok_or_else(|| invalid("trigger not found"))?;
        let workflow = config["workflow_id"]
            .as_str()
            .ok_or_else(|| invalid("trigger workflow missing"))?;
        let input = if source == "schedule" {
            config["input"].clone()
        } else {
            input
        };
        self.run_start(workflow, None, input, source, Some(key), Some(request_id))
            .await
    }
    /// Explicit operator resolution. This records a manual decision, never fabricates an
    /// external OperationOutcome. `decision`:
    /// - `accept_output`：人工接受产出（原语义）；
    /// - `retry`：人工显式授权重发——节点回 pending，驱动器以 attempt+1 重新
    ///   派发（「绝不自动重发」禁令针对机器；人工裁决就是重发的授权凭据）；
    /// - `failed`：人工判定失败——节点终态 failed，run 由收尾判定写 RunFailed。
    #[allow(clippy::too_many_arguments)] // 参数即裁决事实面，拆 struct 只是搬家
    pub async fn run_adjudicate(
        &self,
        run_id: &str,
        node_id: &str,
        operation_id: &str,
        reason: &str,
        output: Value,
        decision: &str,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        if reason.trim().is_empty() || reason.len() > 4096 {
            return Err(
                invalid("manual resolution requires a reason of at most 4096 bytes").into(),
            );
        }
        if !matches!(decision, "accept_output" | "retry" | "failed") {
            return Err(invalid("decision must be accept_output | retry | failed").into());
        }
        let request = json!({"run_id":run_id,"node_id":node_id,"operation_id":operation_id,"reason":reason,"output":output,"decision":decision});
        if let Some(receipt) = deduped(
            self,
            &format!("run.adjudicate:{run_id}"),
            Some(request_id),
            &request,
        )
        .await?
        {
            return Ok(receipt);
        }
        let output =
            flow_journal::value::store_json(&self.journal, output, 8 * 1024 * 1024).await?;
        self.command(&format!("run.adjudicate:{run_id}"),Some(request_id),&request,|state|{
            let run=state.runs.get(run_id).ok_or_else(||invalid("run not found"))?;
            let node=run.nodes.get(node_id).ok_or_else(||invalid("node not found"))?;
            if run.terminal() || node.wait.as_ref().is_none_or(|w|w.kind!="uncertain")
                || node.operation.as_ref().is_none_or(|o|o.operation_id!=operation_id || o.outcome.is_some()) {
                return Err(invalid("manual resolution requires the current uncertain operation"));
            }
            let event=node_event(run,node_id,EventKind::Adjudicated,json!({"operation_id":operation_id,"reason":reason,"decision":decision,"output":output}),true);
            Ok((vec![event],json!({"resolved":true,"decision":decision})))
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
        let parsed = flow_engine::journal_state::validate_definition(&definition)?;
        let fingerprint_request = json!({"workflow_id":workflow_id,"definition":definition});
        let checksum = flow_journal::codec::digest(&flow_journal::codec::bounded_json(
            &definition,
            flow_journal::MAX_LINE_BYTES,
        )?);
        if let Some(receipt) =
            deduped(self, "workflow.update", request_id, &fingerprint_request).await?
        {
            return Ok(receipt);
        }
        let missing = flow_engine::secrets::missing_secrets(&parsed);
        if !missing.is_empty() {
            return Err(invalid(
                missing
                    .iter()
                    .map(|(node, name)| {
                        format!("node {node}: missing secret {name} (FLOW_SECRET_{name})")
                    })
                    .collect::<Vec<_>>()
                    .join("; "),
            )
            .into());
        }
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
                // v1 §8 语义：与最新版本 checksum 相同的定义复用该版本号
                //（编辑器重复保存不刷版本；命令回执空事件即幂等）
                if let Some((_, latest)) = w.versions.last_key_value() {
                    if latest.checksum == checksum {
                        return Ok((vec![], json!({"version": latest.version})));
                    }
                }
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
                if state
                    .runs
                    .values()
                    .any(|run| run.workflow_id == workflow_id)
                {
                    return Err(flow_journal::Error::Conflict(
                        "workflow has runs; cannot delete".into(),
                    ));
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
        let scope = format!("run.start:{source}:{}", source_detail.unwrap_or(""));
        if let Some(receipt) = deduped(self, &scope, request_id, &request).await? {
            return Ok(receipt);
        }
        let stored = flow_journal::value::store_json(&self.journal, input, 8 * 1024 * 1024).await?;
        self.command(&scope, request_id, &request, |state| {
            if matches!(source, "schedule" | "webhook") {
                let key = source_detail.ok_or_else(|| invalid("trigger identity required"))?;
                let config = if source == "schedule" {
                    state.schedules.get(key)
                } else {
                    state.webhooks.get(key)
                }
                .ok_or_else(|| invalid("trigger not found"))?;
                if config["enabled"] != true
                    || config["workflow_id"] != workflow_id
                    || (source == "schedule" && config["input"] != request["input"])
                {
                    return Err(invalid("trigger disabled or changed before commit"));
                }
            }
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
                .ok_or_else(|| invalid("workflow version not found"))?;
            if !definition.published {
                return Err(invalid("version not published"));
            }
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
        if let Some(receipt) =
            deduped(self, &format!("run.signal:{run_id}"), request_id, &request).await?
        {
            return Ok(receipt);
        }
        let output =
            flow_journal::value::store_json(&self.journal, payload.clone(), 8 * 1024 * 1024)
                .await?;
        let adjudicated_output = flow_journal::value::store_json(
            &self.journal,
            payload.get("output").cloned().unwrap_or(Value::Null),
            8 * 1024 * 1024,
        )
        .await?;
        self.command(
            &format!("run.signal:{run_id}"),
            request_id,
            &request,
            |state| {
                let run = state
                    .runs
                    .get(run_id)
                    .ok_or_else(|| invalid("run not found"))?;
                if run.terminal() {
                    return Err(flow_journal::Error::Conflict("run is terminal; cannot deliver signal".into()));
                }
                let node = run
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| invalid("node not waiting"))?;
                if !run.terminal() && node.wait.as_ref().is_some_and(|w| w.kind == "uncertain") {
                    let operation = node.operation.as_ref()
                        .filter(|op| op.outcome.is_none())
                        .ok_or_else(|| invalid("current uncertain operation required"))?;
                    let (decision, reason) = match payload["action"].as_str().unwrap_or("succeeded") {
                        "retry" => ("retry", "manual retry via run.signal"),
                        "failed" => ("failed", payload["error"].as_str().unwrap_or("manual failure via run.signal")),
                        "succeeded" => ("accept_output", "manual acceptance via run.signal"),
                        _ => return Err(invalid("action must be succeeded | failed | retry")),
                    };
                    if reason.trim().is_empty() || reason.len() > 4096 {
                        return Err(invalid("manual resolution requires a reason of at most 4096 bytes"));
                    }
                    let event = node_event(run, node_id, EventKind::Adjudicated,
                        json!({"operation_id":operation.operation_id,"reason":reason,"decision":decision,"output":adjudicated_output}), true);
                    return Ok((vec![event], json!({"delivered":true,"status":"applied","signal_id":request_id})));
                }
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
        if key.is_empty() || key.len() > 128 {
            return Err(invalid("invalid config key").into());
        }
        self.command(
            scope,
            request_id,
            &json!({"key":key,"patch":patch}),
            |state| trigger_decision(state, kind, key, &patch),
        )
        .await
    }

    /// 可复用节点模板的落账命令（TemplateChanged 事实）。patch 合并语义与
    /// config_change 一致；`deleted=true` 删除。非删除模板要求 name 唯一、
    /// nodes/edges 为数组（逐节点结构校验在 RPC 边缘，这里只做形状与唯一性）。
    pub async fn template_change(
        &self,
        key: &str,
        patch: Value,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        if key.is_empty() || key.len() > 128 {
            return Err(invalid("invalid template id").into());
        }
        self.command(
            "template.change",
            request_id,
            &json!({"key":key,"patch":patch}),
            |state| template_decision(state, key, &patch),
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

fn trigger_decision(
    state: &State,
    kind: EventKind,
    key: &str,
    patch: &Value,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let map = if kind == EventKind::ScheduleChanged {
        &state.schedules
    } else {
        &state.webhooks
    };
    let mut value = map
        .get(key)
        .cloned()
        .unwrap_or_else(|| json!({"created_at":now()}));
    if !map.contains_key(key) && map.len() >= 1000 {
        return Err(flow_journal::Error::Limit(
            "maximum 1000 trigger configurations".into(),
        ));
    }
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
        if kind == EventKind::ScheduleChanged {
            let expr = value["cron_expr"]
                .as_str()
                .ok_or_else(|| invalid("cron_expr required"))?;
            expr.parse::<cron_parser::Schedule>()
                .map_err(|e| invalid(e.to_string()))?;
        }
    }
    Ok((vec![Event::new(kind, value.clone())], value))
}

fn template_decision(
    state: &State,
    key: &str,
    patch: &Value,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let mut value = state
        .templates
        .get(key)
        .cloned()
        .unwrap_or_else(|| json!({"created_at":now()}));
    if !state.templates.contains_key(key) && state.templates.len() >= 1000 {
        return Err(flow_journal::Error::Limit("maximum 1000 templates".into()));
    }
    let object = patch
        .as_object()
        .ok_or_else(|| invalid("template patch must be object"))?;
    for (k, v) in object {
        value[k] = v.clone();
    }
    value["id"] = json!(key);
    if value["deleted"] != true {
        let name = value["name"]
            .as_str()
            .ok_or_else(|| invalid("template name required"))?;
        if name.is_empty() || name.len() > 1024 {
            return Err(invalid("invalid template name"));
        }
        // name 唯一（UI 以名为键）：撞别的模板即拒绝，不静默改名。
        if state
            .templates
            .values()
            .any(|t| t.get("id") != Some(&json!(key)) && t["name"] == value["name"])
        {
            return Err(flow_journal::Error::Conflict(
                "template name already taken".into(),
            ));
        }
        for field in ["nodes", "edges"] {
            if !value[field].is_array() {
                return Err(invalid(format!("template {field} must be an array")));
            }
        }
        value["updated_at"] = json!(now());
    }
    Ok((
        vec![Event::new(EventKind::TemplateChanged, value.clone())],
        value,
    ))
}
