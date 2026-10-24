use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use flow_engine::journal_state::{DefinitionVersion, Run, RunStatus, State, WaitKind, Workflow};
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

fn validate_workflow_name(name: &str) -> flow_journal::Result<()> {
    if name.trim().is_empty() || name.len() > 1024 {
        return Err(invalid("invalid workflow name"));
    }
    Ok(())
}

/// Shared normalization, validation and checksum for preflight, import and save.
struct PreparedDefinition {
    value: Value,
    parsed: flow_engine::Definition,
    checksum: String,
}
impl PreparedDefinition {
    fn new(mut value: Value) -> flow_journal::Result<Self> {
        if let Some(object) = value.as_object_mut() {
            object.entry("semantics").or_insert(json!(2));
        }
        let parsed = flow_engine::journal_state::validate_definition(&value)?;
        let bytes = flow_journal::codec::bounded_json(&value, flow_journal::MAX_LINE_BYTES)?;
        Ok(Self {
            value,
            parsed,
            checksum: flow_journal::codec::digest(&bytes),
        })
    }
    /// Caller holds the metadata write guard through the eventual command commit.
    /// Reusing an equal version must not publish a second, unreferenced value.
    async fn store(
        self,
        backend: &JournalBackend,
        latest: Option<&DefinitionVersion>,
    ) -> Result<(
        String,
        flow_journal::StoredValue,
        Option<flow_journal::value::PreparedValue>,
    )> {
        self.require_secrets()?;
        if let Some(version) = latest.filter(|v| v.checksum == self.checksum) {
            return Ok((self.checksum, version.definition.clone(), None));
        }
        let prepared = backend
            .journal
            .prepare_json(self.value, flow_journal::MAX_LINE_BYTES)
            .await?;
        Ok((self.checksum, prepared.stored().clone(), Some(prepared)))
    }
    fn missing_secrets(&self) -> Vec<(String, String)> {
        flow_engine::secrets::missing_secrets(&self.parsed)
    }
    fn require_secrets(&self) -> flow_journal::Result<()> {
        let missing = self.missing_secrets();
        if missing.is_empty() {
            return Ok(());
        }
        Err(invalid(
            missing
                .iter()
                .map(|(node, name)| {
                    format!("node {node}: missing secret {name} (FLOW_SECRET_{name})")
                })
                .collect::<Vec<_>>()
                .join("; "),
        ))
    }
}

fn import_target<'a>(state: &'a State, name: &str) -> flow_journal::Result<Option<&'a Workflow>> {
    let mut matches = state
        .workflows
        .values()
        .filter(|w| !w.deleted && w.name == name);
    let target = matches.next();
    if matches.next().is_some() {
        return Err(flow_journal::Error::Conflict(format!(
            "multiple workflows named {name}; update by workflow_id or use a unique import name"
        )));
    }
    Ok(target)
}

/// Repeated saves/imports reuse the latest version and its publication state.
fn prepare_version(
    workflow_id: &str,
    latest: Option<&DefinitionVersion>,
    definition: flow_journal::StoredValue,
    checksum: String,
) -> (DefinitionVersion, Option<Event>) {
    if let Some(latest) = latest.filter(|v| v.checksum == checksum) {
        return (latest.clone(), None);
    }
    let value = DefinitionVersion {
        version: latest.map_or(1, |v| v.version + 1),
        definition,
        checksum,
        published: false,
        created_at: now(),
    };
    let event = Event::new(
        EventKind::WorkflowUpdated,
        json!({"workflow_id":workflow_id,"version":value}),
    );
    (value, Some(event))
}

/// Reuse committed receipts before preparing large input. Concurrent requests
/// only stage anonymous files; command admission deduplicates before publication.
async fn deduped(
    backend: &JournalBackend,
    scope: &str,
    request_id: Option<&str>,
    request: &Value,
) -> Result<Option<CommandReceipt>> {
    backend.deduped_command(scope, request_id, request).await
}
/// 产品面 CRUD 的强类型载荷：在 RPC 边缘经 [`ProductCommand::parse`] 一次
/// 解析，内部只传枚举。`input`/`nodes` 等纯透传的用户数据保留 `Value`；
/// 事件载荷与命令指纹的 wire 形状不变（协议冻结，只改 Rust 内部表示）。
#[derive(Debug, Clone)]
pub enum ProductCommand {
    ScheduleCreate {
        key: String,
        workflow_id: String,
        cron: String,
        enabled: bool,
        input: Value,
    },
    ScheduleUpdate {
        id: String,
        cron: Option<String>,
        input: Option<Value>,
        enabled: Option<bool>,
    },
    ScheduleDelete {
        id: String,
    },
    WebhookCreate {
        key: String,
        workflow_id: String,
    },
    WebhookSetEnabled {
        token: String,
        enabled: bool,
    },
    WebhookDelete {
        token: String,
    },
    TemplateUpsert {
        /// true = create（key 在解析时生成）；false = update（key 来自请求）。
        creating: bool,
        id: String,
        name: Option<String>,
        category: Option<Option<String>>,
        nodes: Option<Vec<Value>>,
        edges: Option<Vec<Value>>,
    },
    TemplateDelete {
        id: String,
    },
}

/// Missing fields remain distinct from present fields of the wrong type.
fn optional_field<T: serde::de::DeserializeOwned>(
    p: &Value,
    key: &str,
    message: &str,
) -> flow_journal::Result<Option<T>> {
    p.get(key)
        .map(|v| serde_json::from_value(v.clone()).map_err(|_| invalid(message)))
        .transpose()
}

impl ProductCommand {
    /// Parse control fields once at the RPC boundary; arbitrary user input stays JSON.
    pub fn parse(method: &str, p: &Value) -> flow_journal::Result<Self> {
        let field = |key: &str| {
            p[key]
                .as_str()
                .ok_or_else(|| invalid(format!("missing string {key}")))
        };
        Ok(match method {
            "schedule.create" => Self::ScheduleCreate {
                key: id(),
                workflow_id: field("workflow_id")?.to_owned(),
                cron: field("cron")?.to_owned(),
                enabled: optional_field(p, "enabled", "enabled flag required")?.unwrap_or(true),
                input: p["input"].clone(),
            },
            "schedule.update" => Self::ScheduleUpdate {
                id: field("id")?.to_owned(),
                cron: optional_field(p, "cron", "cron_expr required")?,
                input: p.get("input").cloned(),
                enabled: optional_field(p, "enabled", "enabled flag required")?,
            },
            "schedule.delete" => Self::ScheduleDelete {
                id: field("id")?.to_owned(),
            },
            "webhook.create" => Self::WebhookCreate {
                key: uuid::Uuid::now_v7().simple().to_string(),
                workflow_id: field("workflow_id")?.to_owned(),
            },
            "webhook.set_enabled" => Self::WebhookSetEnabled {
                token: field("token")?.to_owned(),
                enabled: p["enabled"]
                    .as_bool()
                    .ok_or_else(|| invalid("enabled flag required"))?,
            },
            "webhook.delete" => Self::WebhookDelete {
                token: field("token")?.to_owned(),
            },
            "template.create" | "template.update" => Self::TemplateUpsert {
                creating: method == "template.create",
                id: if method == "template.create" {
                    id()
                } else {
                    field("id")?.to_owned()
                },
                name: optional_field(p, "name", "template name required")?,
                category: optional_field(
                    p,
                    "category",
                    "template category must be a string or null",
                )?,
                nodes: optional_field(p, "nodes", "template nodes must be an array")?,
                edges: optional_field(p, "edges", "template edges must be an array")?,
            },
            "template.delete" => Self::TemplateDelete {
                id: field("id")?.to_owned(),
            },
            _ => return Err(invalid("invalid product command")),
        })
    }

    /// 命令 scope（幂等键的一部分）= 方法名，与 wire 方法名一致。
    pub fn method(&self) -> &'static str {
        match self {
            Self::ScheduleCreate { .. } => "schedule.create",
            Self::ScheduleUpdate { .. } => "schedule.update",
            Self::ScheduleDelete { .. } => "schedule.delete",
            Self::WebhookCreate { .. } => "webhook.create",
            Self::WebhookSetEnabled { .. } => "webhook.set_enabled",
            Self::WebhookDelete { .. } => "webhook.delete",
            Self::TemplateUpsert { creating: true, .. } => "template.create",
            Self::TemplateUpsert {
                creating: false, ..
            } => "template.update",
            Self::TemplateDelete { .. } => "template.delete",
        }
    }
    fn creating(&self) -> bool {
        matches!(
            self,
            Self::ScheduleCreate { .. }
                | Self::WebhookCreate { .. }
                | Self::TemplateUpsert { creating: true, .. }
        )
    }
    fn key(&self) -> &str {
        match self {
            Self::ScheduleCreate { key, .. } | Self::WebhookCreate { key, .. } => key,
            Self::ScheduleUpdate { id, .. }
            | Self::TemplateUpsert { id, .. }
            | Self::ScheduleDelete { id }
            | Self::TemplateDelete { id } => id,
            Self::WebhookSetEnabled { token, .. } | Self::WebhookDelete { token } => token,
        }
    }
}

impl JournalBackend {
    pub async fn invoke_start(
        &self,
        hook: &str,
        request_id: &str,
        input: Value,
        correlation: &str,
        key_hash: &str,
    ) -> Result<CommandReceipt> {
        let config = self
            .inspect(|s| s.webhooks.get(hook).cloned())
            .await
            .ok_or_else(|| flow_journal::Error::NotFound("webhook not found".into()))?;
        let workflow = config["workflow_id"]
            .as_str()
            .ok_or_else(|| invalid("workflow_id required"))?;
        let scope = format!("run.start:webhook:invocation:{hook}");
        let version = if let Some(receipt) = self.command_status(&scope, request_id).await? {
            let run = self
                .run_snapshot(
                    receipt.result["run_id"]
                        .as_str()
                        .ok_or_else(|| invalid("invalid invocation receipt"))?,
                )
                .await?
                .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
            run.workflow_version
        } else {
            config["invoke_version"]
                .as_u64()
                .ok_or_else(|| invalid("invocation version is not configured"))?
        };
        self.start_with_regression(
            workflow,
            Some(version),
            input,
            "webhook",
            Some(hook),
            Some(request_id),
            None,
            Some(correlation),
            Some(key_hash),
        )
        .await
    }
    /// Product CRUD uses the caller's identity and arguments for the durable
    /// command. IDs, timestamps, existence checks and the mutation are decided
    /// under the same lock. `fingerprint` keeps the raw wire params (minus
    /// request_id) for command dedup — the typed enum is the parsed view.
    pub async fn product_command(
        &self,
        command: ProductCommand,
        fingerprint: &Value,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        let method = command.method();
        self.command(method, Some(request_id), fingerprint, |state| {
            product_decision(state, &command)
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
                .run_snapshot(run_id)
                .await?
                .ok_or_else(|| invalid("missing committed trigger run"))?;
            let input = if source == "schedule" {
                flow_journal::value::materialize(
                    self.journal.root(),
                    self.journal.durable_lsn(),
                    &run.input,
                    flow_journal::VALUE_BUDGET,
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
        flow_engine::journal_state::validate_adjudication(decision, reason)?;
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
        let validate = |state: &State| {
            let run = state
                .runs
                .get(run_id)
                .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
            let node = run
                .nodes
                .get(node_id)
                .ok_or_else(|| invalid("node not found"))?;
            flow_engine::journal_state::adjudicate_guard(
                run.terminal(),
                node,
                operation_id,
                decision,
                reason,
            )
        };
        if let Some(receipt) = self
            .preflight_command(
                &format!("run.adjudicate:{run_id}"),
                Some(request_id),
                &request,
                Some(run_id),
                validate,
            )
            .await?
        {
            return Ok(receipt);
        }
        let prepared = self
            .journal
            .prepare_json(output, flow_journal::VALUE_BUDGET)
            .await?;
        let output = prepared.stored().clone();
        self.command_with_value(&format!("run.adjudicate:{run_id}"),Some(request_id),&request,Some(run_id),prepared,|state|{
            let run=state.runs.get(run_id).ok_or_else(||flow_journal::Error::NotFound("run not found".into()))?;
            // outcome 已提交(结果被捕获、输出推导未达)的 Uncertain 节点同样
            // 允许人工收口:accept_output/failed 可用已提交证据裁决;只有
            // retry 需要未决 outcome——结果已捕获后重发授权没有意义,重派发
            // 会撞「operation already authorized」。
            validate(state)?;
            let event=node_event(run,node_id,EventKind::Adjudicated,json!({"operation_id":operation_id,"reason":reason,"decision":decision,"output":output}),true);
            Ok((vec![event],json!({"resolved":true,"decision":decision})))
        }).await
    }

    /// Preflight has no journal writes. Missing secrets are repairable by the import form.
    pub fn workflow_validate(&self, definition: Value) -> Result<Value> {
        let prepared = PreparedDefinition::new(definition)?;
        let missing: Vec<Value> = prepared
            .missing_secrets()
            .into_iter()
            .map(|(node_id, name)| json!({"node_id":node_id,"name":name}))
            .collect();
        Ok(json!({"valid":missing.is_empty(),"missing_secrets":missing}))
    }

    /// Name upsert, version creation and publication commit as one transaction.
    pub async fn workflow_import(
        &self,
        name: &str,
        definition: Value,
        publish: bool,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        validate_workflow_name(name)?;
        let prepared = PreparedDefinition::new(definition)?;
        let request = json!({"name":name,"definition":prepared.value,"publish":publish});
        let _metadata = self.workflow_metadata.write().await;
        if let Some(receipt) = deduped(self, "workflow.import", request_id, &request).await? {
            return Ok(receipt);
        }
        let latest = self
            .inspect_committed(|state| {
                Ok::<_, flow_journal::Error>(
                    import_target(state, name)?
                        .and_then(|w| w.versions.last_key_value().map(|(_, v)| v.clone())),
                )
            })
            .await?;
        let (checksum, stored, value) = prepared.store(self, latest.as_ref()).await?;
        self.command_selected(
            "workflow.import",
            request_id,
            &request,
            None,
            value.into_iter().collect(),
            |state| {
                let existing = import_target(state, name)?;
                let created = existing.is_none();
                let workflow_id = existing.map(|w| w.workflow_id.clone()).unwrap_or_else(id);
                let mut events = Vec::new();
                if created {
                    events.push(Event::new(
                        EventKind::WorkflowCreated,
                        serde_json::to_value(Workflow {
                            workflow_id: workflow_id.clone(),
                            name: name.into(),
                            created_at: now(),
                            versions: BTreeMap::new(),
                            deleted: false,
                        })?,
                    ));
                }
                let latest = existing.and_then(|w| w.versions.last_key_value().map(|(_, v)| v));
                let (value, event) = prepare_version(&workflow_id, latest, stored, checksum);
                let version = value.version;
                events.extend(event);
                let published = value.published || publish;
                if publish && !value.published {
                    events.push(Event::new(
                        EventKind::WorkflowPublished,
                        json!({"workflow_id":workflow_id,"version":version.to_string()}),
                    ));
                }
                Ok((
                    events,
                    json!({"workflow_id":workflow_id,"name":name,"version":version,
                "created":created,"status":if published {"published"} else {"draft"}}),
                ))
            },
        )
        .await
    }

    pub async fn workflow_create(
        &self,
        name: &str,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        validate_workflow_name(name)?;
        let _metadata = self.workflow_metadata.write().await;
        self.command("workflow.create", request_id, &json!({"name":name}), |_| {
            let workflow_id = id();
            let w = Workflow {
                workflow_id: workflow_id.clone(),
                name: name.into(),
                created_at: now(),
                versions: BTreeMap::new(),
                deleted: false,
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
        let prepared = PreparedDefinition::new(definition)?;
        let fingerprint_request = json!({"workflow_id":workflow_id,"definition":prepared.value});
        let _metadata = self.workflow_metadata.write().await;
        if let Some(receipt) =
            deduped(self, "workflow.update", request_id, &fingerprint_request).await?
        {
            return Ok(receipt);
        }
        let latest = self
            .inspect_committed(|state| {
                let workflow = state
                    .workflows
                    .get(workflow_id)
                    .filter(|w| !w.deleted)
                    .ok_or_else(|| flow_journal::Error::NotFound("workflow not found".into()))?;
                Ok::<_, flow_journal::Error>(
                    workflow.versions.last_key_value().map(|(_, v)| v.clone()),
                )
            })
            .await?;
        let (checksum, stored, value) = prepared.store(self, latest.as_ref()).await?;
        self.command_selected(
            "workflow.update",
            request_id,
            &fingerprint_request,
            None,
            value.into_iter().collect(),
            |state| {
                let w = state
                    .workflows
                    .get(workflow_id)
                    .filter(|w| !w.deleted)
                    .ok_or_else(|| flow_journal::Error::NotFound("workflow not found".into()))?;
                let latest = w.versions.last_key_value().map(|(_, v)| v);
                let (value, event) = prepare_version(workflow_id, latest, stored, checksum);
                Ok((
                    event.into_iter().collect(),
                    json!({"version":value.version}),
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
        let _metadata = self.workflow_metadata.write().await;
        self.command(
            "workflow.publish",
            request_id,
            &json!({"workflow_id":workflow_id,"version":version}),
            |state| {
                state
                    .workflows
                    .get(workflow_id)
                    .filter(|w| !w.deleted)
                    .and_then(|w| w.versions.get(&version))
                    .ok_or_else(|| {
                        flow_journal::Error::NotFound("workflow version not found".into())
                    })?;
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
        let _metadata = self.workflow_metadata.write().await;
        self.command(
            "workflow.delete",
            request_id,
            &json!({"workflow_id":workflow_id}),
            |state| {
                let workflow = state
                    .workflows
                    .get(workflow_id)
                    .ok_or_else(|| flow_journal::Error::NotFound("workflow not found".into()))?;
                if workflow.deleted {
                    return Ok((vec![], json!({"ok":true})));
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
        self.start_with_regression(
            workflow_id,
            version,
            input,
            source,
            source_detail,
            request_id,
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start_with_regression(
        &self,
        workflow_id: &str,
        version: Option<u64>,
        input: Value,
        source: &str,
        source_detail: Option<&str>,
        request_id: Option<&str>,
        regression: Option<flow_engine::journal_state::ExecutionSnapshot>,
        correlation_id: Option<&str>,
        invocation_key_hash: Option<&str>,
    ) -> Result<CommandReceipt> {
        if !flow_dto::RunSource::is_valid_str(source) {
            return Err(invalid("invalid run source").into());
        }
        let mut request = json!({"workflow_id":workflow_id,"version":version,"input":input,"source":source,"source_detail":source_detail});
        if let Some(regression) = &regression {
            request["regression"] = serde_json::to_value(regression)?;
        }
        if let Some(correlation) = correlation_id {
            if correlation.is_empty() || correlation.len() > 128 {
                return Err(invalid("correlation_id must contain 1..=128 bytes").into());
            }
            request["correlation_id"] = json!(correlation);
        }
        if let Some(hash) = invocation_key_hash {
            request["invocation_key_hash"] = json!(hash);
        }
        let scope = format!(
            "run.start:{source}:{}{}",
            if correlation_id.is_some() {
                "invocation:"
            } else {
                ""
            },
            source_detail.unwrap_or("")
        );
        if let Some(receipt) = deduped(self, &scope, request_id, &request).await? {
            return Ok(receipt);
        }
        let (selected, frozen_definition, mut execution) =
            self.freeze_execution(workflow_id, version).await?;
        if correlation_id.is_some() && execution.is_none() {
            return Err(invalid("invocation requires semantics 2").into());
        }
        let definition = self
            .cached_definition(
                &format!("start:{workflow_id}@{selected}"),
                &frozen_definition,
            )
            .await?;
        validate_run_input(&definition, &input)?;
        if let Some(execution) = &mut execution {
            execution.correlation_id = correlation_id.map(str::to_owned);
        }
        if let Some(recorded) = regression {
            let snapshot = execution
                .as_mut()
                .ok_or_else(|| invalid("regression requires semantics 2"))?;
            snapshot.clock_ms = recorded.clock_ms;
            snapshot.seed = recorded.seed;
            snapshot.regression = recorded.regression;
            // 回归候选必须继承 baseline 冻结的 agent 配置：请求指纹里含
            // 配置，若按当前配置构造，baseline 后改过 [harness.agent] 的
            // 回归会因指纹漂移而失配——那正是快照要消除的漂移。
            snapshot.agent = recorded.agent.clone();
        }
        // agent 节点的配置前置校验：快照里没有冻结配置就没有可授权的
        // 请求。放在 run 提交前而不是 executor 派发后——用户立刻知道
        // 去哪修（回归分支上面已继承 baseline 配置，不受当前安装影响）。
        if execution
            .as_ref()
            .is_none_or(|snapshot| snapshot.agent.is_none())
            && definition.nodes.iter().any(|node| {
                node.kind() == Some(flow_engine::NodeType::Harness)
                    && node
                        .param_str("mode")
                        .is_some_and(|mode| mode.trim() == "agent")
            })
        {
            return Err(invalid(
                "工作流包含 mode=agent 的 harness 节点，但 [harness.agent] 未启用：\
                 请在设置页配置 base_url 与 model（成对填写）并重启 flow 后重试",
            )
            .into());
        }
        let validate = |state: &State| {
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
                if correlation_id.is_some()
                    && (config["invoke_version"].as_u64() != Some(selected)
                        || config["invoke_key_hash"].as_str() != invocation_key_hash)
                {
                    return Err(invalid("invocation configuration changed before commit"));
                }
                if source == "schedule" {
                    let fire =
                        request_id.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
                    if fire.is_some_and(|at| {
                        state
                            .schedule_fires
                            .get(key)
                            .is_some_and(|last| at <= last.at)
                    }) {
                        return Err(flow_journal::Error::Conflict(
                            "scheduled slot already committed".into(),
                        ));
                    }
                    let since = config["active_since"]
                        .as_str()
                        .or_else(|| config["created_at"].as_str())
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
                    if fire.zip(since).is_some_and(|(fire, since)| fire < since) {
                        return Err(invalid("scheduled fire predates trigger activation"));
                    }
                }
            }
            if state.runs.values().filter(|r| !r.terminal()).count() >= crate::journal::R_MAX {
                return Err(flow_journal::Error::Limit(format!(
                    "R_max={}",
                    crate::journal::R_MAX
                )));
            }
            state
                .workflows
                .get(workflow_id)
                .filter(|w| !w.deleted)
                .ok_or_else(|| flow_journal::Error::NotFound("workflow not found".into()))?;
            // freeze_execution validated this immutable version as published.
            // Publication is monotonic; a later publish can evict the selected
            // version from the hot cache before this command acquires the lock.
            // Only deletion/existence must be rechecked here, not cache residency.
            Ok(())
        };
        if let Some(receipt) = self
            .preflight_command(&scope, request_id, &request, None, &validate)
            .await?
        {
            return Ok(receipt);
        }
        let prepared = self
            .journal
            .prepare_json(input, flow_journal::VALUE_BUDGET)
            .await?;
        let stored = prepared.stored().clone();
        self.command_with_value(&scope, request_id, &request, None, prepared, |state| {
            validate(state)?;
            let run_id = id();
            let run = Run {
                run_id: run_id.clone(),
                workflow_id: workflow_id.into(),
                workflow_version: selected,
                definition: frozen_definition,
                input: stored,
                source: source.into(),
                source_detail: source_detail.map(str::to_owned),
                parent: None,
                depth: 0,
                created_at: now(),
                status: RunStatus::Running,
                output: None,
                error: None,
                last_run_seq: 0,
                nodes: BTreeMap::new(),
                signals: execution
                    .as_ref()
                    .map(|e| e.signal_inputs())
                    .unwrap_or_default(),
                execution,
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
        self.command_for_run(
            run_id,
            &format!("run.cancel:{run_id}"),
            request_id,
            &json!({"run_id":run_id}),
            |state| {
                let run = state
                    .runs
                    .get(run_id)
                    .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
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
        let snapshot = self
            .run_snapshot(run_id)
            .await?
            .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
        let definition = self.cached_definition(run_id, &snapshot.definition).await?;
        let accepts_inbox = definition.node_type(node_id).is_some_and(|k| {
            matches!(
                k,
                flow_engine::NodeType::HumanTask | flow_engine::NodeType::Input
            )
        });
        let mut adjudication = false;
        if let Some(receipt) = self
            .preflight_command(
                &format!("run.signal:{run_id}"),
                request_id,
                &request,
                Some(run_id),
                |state| {
                    let (events, _) = signal_decision(
                        state,
                        run_id,
                        node_id,
                        &payload,
                        request_id,
                        accepts_inbox,
                        &flow_journal::StoredValue::Inline(Value::Null),
                    )?;
                    adjudication = events
                        .first()
                        .is_some_and(|event| event.kind == EventKind::Adjudicated);
                    Ok(())
                },
            )
            .await?
        {
            return Ok(receipt);
        }
        let value = if adjudication {
            payload.get("output").cloned().unwrap_or(Value::Null)
        } else {
            payload.clone()
        };
        let prepared = self
            .journal
            .prepare_json(value, flow_journal::VALUE_BUDGET)
            .await?;
        let output = prepared.stored().clone();
        self.command_with_value(
            &format!("run.signal:{run_id}"),
            request_id,
            &request,
            Some(run_id),
            prepared,
            |state| {
                let result = signal_decision(
                    state,
                    run_id,
                    node_id,
                    &payload,
                    request_id,
                    accepts_inbox,
                    &output,
                )?;
                if result
                    .0
                    .first()
                    .is_some_and(|event| event.kind == EventKind::Adjudicated)
                    != adjudication
                {
                    return Err(flow_journal::Error::Conflict(
                        "signal target changed before commit".into(),
                    ));
                }
                Ok(result)
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
        let fields = patch
            .as_object()
            .ok_or_else(|| invalid("config must be object"))?;
        for field in fields.keys() {
            if !matches!(
                field.as_str(),
                "workflow_id"
                    | "enabled"
                    | "input"
                    | "cron_expr"
                    | "deleted"
                    | "invoke_version"
                    | "invoke_key_hash"
            ) {
                return Err(invalid(format!("config field is not writable: {field}")).into());
            }
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
    /// name/category/nodes/edges 共用引擎模板规则，合并后再验证完整片段。
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

/// Typed commands decide directly against committed state. JSON is produced only
/// for the persisted document; legacy patch endpoints share the final validators.
fn product_decision(
    state: &State,
    command: &ProductCommand,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let key = command.key();
    if key.is_empty() || key.len() > 128 {
        return Err(invalid("invalid entity key"));
    }
    let creating = command.creating();
    match command {
        ProductCommand::ScheduleCreate { .. }
        | ProductCommand::ScheduleUpdate { .. }
        | ProductCommand::ScheduleDelete { .. } => {
            let previous = state.schedules.get(key);
            if !creating && previous.is_none() {
                return Err(flow_journal::Error::NotFound("schedule not found".into()));
            }
            let mut value = previous
                .cloned()
                .unwrap_or_else(|| json!({"created_at":now()}));
            require_object(&value)?;
            match command {
                ProductCommand::ScheduleCreate {
                    workflow_id,
                    cron,
                    enabled,
                    input,
                    ..
                } => {
                    value["workflow_id"] = json!(workflow_id);
                    value["cron_expr"] = json!(cron);
                    value["enabled"] = json!(enabled);
                    value["input"] = input.clone();
                }
                ProductCommand::ScheduleUpdate {
                    cron,
                    enabled,
                    input,
                    ..
                } => {
                    if let Some(cron) = cron {
                        value["cron_expr"] = json!(cron);
                    }
                    if let Some(enabled) = enabled {
                        value["enabled"] = json!(enabled);
                    }
                    if let Some(input) = input {
                        value["input"] = input.clone();
                    }
                }
                ProductCommand::ScheduleDelete { .. } => value["deleted"] = json!(true),
                _ => {
                    return Err(invalid(
                        "command kind does not match schedule command family",
                    ))
                }
            }
            let (events, mut value) = trigger_event(state, EventKind::ScheduleChanged, key, value)?;
            let result = if matches!(command, ProductCommand::ScheduleDelete { .. }) {
                json!({"deleted":true})
            } else if creating {
                value["next_fire_at"] = json!(value["cron_expr"]
                    .as_str()
                    .and_then(|expr| expr.parse::<cron_parser::Schedule>().ok())
                    .and_then(|cron| cron.next_after(&chrono::Local::now()))
                    .map(|t| t.with_timezone(&chrono::Utc).to_rfc3339()));
                value
                    .as_object_mut()
                    .ok_or_else(|| invalid("malformed config row"))?
                    .remove("input");
                value
            } else {
                json!({"updated":true})
            };
            Ok((events, result))
        }
        ProductCommand::WebhookCreate { .. }
        | ProductCommand::WebhookSetEnabled { .. }
        | ProductCommand::WebhookDelete { .. } => {
            let previous = state.webhooks.get(key);
            if !creating && previous.is_none() {
                return Err(flow_journal::Error::NotFound("webhook not found".into()));
            }
            let mut value = previous
                .cloned()
                .unwrap_or_else(|| json!({"created_at":now()}));
            require_object(&value)?;
            match command {
                ProductCommand::WebhookCreate { workflow_id, .. } => {
                    value["workflow_id"] = json!(workflow_id);
                    value["enabled"] = json!(true);
                }
                ProductCommand::WebhookSetEnabled { enabled, .. } => {
                    value["enabled"] = json!(enabled)
                }
                ProductCommand::WebhookDelete { .. } => value["deleted"] = json!(true),
                _ => {
                    return Err(invalid(
                        "command kind does not match webhook command family",
                    ))
                }
            }
            let (events, value) = trigger_event(state, EventKind::WebhookChanged, key, value)?;
            let result = match command {
                ProductCommand::WebhookCreate { .. } => value,
                ProductCommand::WebhookDelete { .. } => json!({"deleted":true}),
                _ => json!({"updated":true}),
            };
            Ok((events, result))
        }
        ProductCommand::TemplateUpsert { .. } | ProductCommand::TemplateDelete { .. } => {
            let previous = state.templates.get(key);
            if !creating && previous.is_none() {
                return if matches!(command, ProductCommand::TemplateDelete { .. }) {
                    Ok((vec![], json!({"deleted":false})))
                } else {
                    Err(flow_journal::Error::NotFound("template not found".into()))
                };
            }
            let mut value = previous
                .cloned()
                .unwrap_or_else(|| json!({"created_at":now(),"category":null}));
            require_object(&value)?;
            match command {
                ProductCommand::TemplateUpsert {
                    name,
                    category,
                    nodes,
                    edges,
                    ..
                } => {
                    if let Some(name) = name {
                        value["name"] = json!(name);
                    }
                    if let Some(category) = category {
                        value["category"] = json!(category);
                    }
                    if let Some(nodes) = nodes {
                        value["nodes"] = json!(nodes);
                    }
                    if let Some(edges) = edges {
                        value["edges"] = json!(edges);
                    }
                }
                ProductCommand::TemplateDelete { .. } => value["deleted"] = json!(true),
                _ => {
                    return Err(invalid(
                        "command kind does not match template command family",
                    ))
                }
            }
            let (events, mut value) = template_event(state, key, value)?;
            if matches!(command, ProductCommand::TemplateDelete { .. }) {
                return Ok((events, json!({"deleted":true})));
            }
            value
                .as_object_mut()
                .ok_or_else(|| invalid("malformed config row"))?
                .remove("nodes");
            value
                .as_object_mut()
                .ok_or_else(|| invalid("malformed config row"))?
                .remove("edges");
            Ok((events, value))
        }
    }
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
    require_object(&value)?;
    let object = patch
        .as_object()
        .ok_or_else(|| invalid("config must be object"))?;
    for (k, v) in object {
        value[k] = v.clone();
    }
    trigger_event(state, kind, key, value)
}

fn trigger_event(
    state: &State,
    kind: EventKind,
    key: &str,
    mut value: Value,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let map = if kind == EventKind::ScheduleChanged {
        &state.schedules
    } else {
        &state.webhooks
    };
    require_object(&value)?;
    let previous = map.get(key);
    let previous_enabled = previous.is_some_and(|v| v["enabled"] == true);
    let previous_cron = previous
        .map(|v| v["cron_expr"].clone())
        .unwrap_or(Value::Null);
    if previous.is_none() && map.len() >= 1000 {
        return Err(flow_journal::Error::Limit(
            "maximum 1000 trigger configurations".into(),
        ));
    }
    value[if kind == EventKind::ScheduleChanged {
        "id"
    } else {
        "token"
    }] = json!(key);
    if value["deleted"] != true {
        let workflow = value["workflow_id"]
            .as_str()
            .ok_or_else(|| invalid("workflow_id required"))?
            .to_owned();
        if state.workflows.get(&workflow).is_none_or(|w| w.deleted) {
            return Err(flow_journal::Error::NotFound("workflow not found".into()));
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
            if value["enabled"] == true
                && (!previous_enabled || previous_cron != value["cron_expr"])
            {
                value["active_since"] = json!(now());
            }
        }
        if kind == EventKind::WebhookChanged
            && (!value["invoke_version"].is_null() || !value["invoke_key_hash"].is_null())
        {
            let version = value["invoke_version"]
                .as_u64()
                .ok_or_else(|| invalid("invoke_version must be an integer"))?;
            if !state.workflows[&workflow]
                .versions
                .get(&version)
                .is_some_and(|v| v.published)
            {
                return Err(invalid("invocation version must be published"));
            }
            let hash = value["invoke_key_hash"]
                .as_str()
                .ok_or_else(|| invalid("invoke_key_hash required"))?;
            if hash.len() != 64 || hash.bytes().any(|c| !c.is_ascii_hexdigit()) {
                return Err(invalid("invoke_key_hash must be a SHA-256 hex digest"));
            }
        }
    }
    Ok((vec![Event::new(kind, value.clone())], value))
}

// Raw patch entry point shares the final domain validation with typed commands.
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
    let object = patch
        .as_object()
        .ok_or_else(|| invalid("template patch must be object"))?;
    require_object(&value)?;
    for (k, v) in object {
        value[k] = v.clone();
    }
    template_event(state, key, value)
}

fn template_event(
    state: &State,
    key: &str,
    mut value: Value,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    if !state.templates.contains_key(key) && state.templates.len() >= 1000 {
        return Err(flow_journal::Error::Limit("maximum 1000 templates".into()));
    }
    require_object(&value)?;
    value["id"] = json!(key);
    if value["deleted"] != true {
        flow_engine::template::normalize(&mut value)?;
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
        value["updated_at"] = json!(now());
    }
    Ok((
        vec![Event::new(EventKind::TemplateChanged, value.clone())],
        value,
    ))
}

/// Every run creation path validates the same input contract before dispatch.
pub(crate) fn validate_run_input(
    definition: &flow_engine::Definition,
    input: &Value,
) -> flow_journal::Result<()> {
    if let Some(schema) = &definition.input_schema {
        flow_journal::codec::bounded_json(input, flow_journal::VALUE_BUDGET)?;
        flow_engine::schema::validate(schema, input)
            .map_err(|e| invalid(format!("input contract: {e}")))?;
    }
    Ok(())
}

/// 非终态 run 的记录 runtime 与当前执行器不符 → 永远无法恢复（启动隔离对象）。
pub(crate) fn runtime_mismatched(run: &Run) -> bool {
    !run.terminal()
        && run
            .execution
            .as_ref()
            .is_some_and(|e| e.runtime != flow_engine::RUNTIME_ID)
}

fn require_object(value: &Value) -> flow_journal::Result<()> {
    if value.is_object() {
        Ok(())
    } else {
        Err(invalid("malformed config row: expected object"))
    }
}

fn signal_decision(
    state: &State,
    run_id: &str,
    node_id: &str,
    payload: &Value,
    request_id: Option<&str>,
    accepts_inbox: bool,
    output: &flow_journal::StoredValue,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let run = state
        .runs
        .get(run_id)
        .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
    if run.terminal() {
        return Err(flow_journal::Error::Conflict(
            "run is terminal; cannot deliver signal".into(),
        ));
    }
    let waiting = run.nodes.get(node_id).is_some_and(|n| {
        n.wait
            .as_ref()
            .is_some_and(|w| matches!(w.kind(), WaitKind::Signal | WaitKind::Uncertain))
    });
    if !waiting {
        if !accepts_inbox
            || request_id.is_none()
            || run.nodes.get(node_id).is_some_and(|n| {
                matches!(
                    n.status,
                    flow_engine::journal_state::NodeStatus::Succeeded
                        | flow_engine::journal_state::NodeStatus::Failed
                        | flow_engine::journal_state::NodeStatus::Skipped
                )
            })
        {
            return Err(invalid("node does not accept a pending signal"));
        }
        if run.signals.contains_key(node_id) {
            return Err(flow_journal::Error::Conflict(
                "signal already queued for node".into(),
            ));
        }
        let mut event = Event::new(
            EventKind::SignalReceived,
            json!({"payload":output,"queued":true}),
        );
        event.run_id = Some(run_id.into());
        event.node_id = Some(node_id.into());
        event.run_seq = run.last_run_seq + 1;
        return Ok((
            vec![event],
            json!({"delivered":true,"status":"queued","signal_id":request_id}),
        ));
    }
    let node = &run.nodes[node_id];
    if !run.terminal()
        && node
            .wait
            .as_ref()
            .is_some_and(|w| w.kind() == WaitKind::Uncertain)
    {
        let operation = node
            .operation
            .as_ref()
            .filter(|op| op.outcome.is_none())
            .ok_or_else(|| invalid("current uncertain operation required"))?;
        let (decision, reason) = match payload["action"].as_str().unwrap_or("succeeded") {
            "retry" => ("retry", "manual retry via run.signal"),
            "failed" => (
                "failed",
                payload["error"]
                    .as_str()
                    .unwrap_or("manual failure via run.signal"),
            ),
            "succeeded" => ("accept_output", "manual acceptance via run.signal"),
            _ => return Err(invalid("action must be succeeded | failed | retry")),
        };
        flow_engine::journal_state::adjudicate_guard(
            run.terminal(),
            node,
            &operation.operation_id,
            decision,
            reason,
        )?;
        let event = node_event(
            run,
            node_id,
            EventKind::Adjudicated,
            json!({"operation_id":operation.operation_id,"reason":reason,"decision":decision,"output":output}),
            true,
        );
        return Ok((
            vec![event],
            json!({"delivered":true,"status":"applied","signal_id":request_id}),
        ));
    }
    if run.terminal()
        || node
            .wait
            .as_ref()
            .is_none_or(|w| w.kind() != WaitKind::Signal)
    {
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
}

#[cfg(test)]
mod runtime_mismatch_tests {
    use super::*;

    fn waiting_run(runtime: &str) -> Run {
        // 最小非终态 run：status=running + execution 快照（runtime 可注入）。
        // StoredValue 是 tag=type/content=value 的外部标记枚举；ValueRef 需
        // journal_id/output_id/codec/version/chunk_count/total_bytes/digest。
        let value_ref = json!({
            "type": "ref",
            "value": {
                "journal_id": "j",
                "output_id": "v1",
                "codec": "json",
                "version": 1,
                "chunk_count": "1",
                "total_bytes": "2",
                "digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }
        });
        serde_json::from_value(json!({
            "run_id": "01JTEST",
            "workflow_id": "wf",
            "workflow_version": 1,
            "definition": value_ref,
            "input": value_ref,
            "source": "manual",
            "source_detail": null,
            "parent": null,
            "depth": 0,
            "created_at": "2026-10-20T00:00:00Z",
            "status": "running",
            "output": null,
            "error": null,
            "last_run_seq": "0",
            "nodes": {},
            "execution": {
                "runtime": runtime,
                "clock_ms": 0,
                "seed": "s",
                "path": [],
                "definitions": {},
                "bindings": {}
            }
        }))
        .unwrap()
    }

    #[test]
    fn runtime_mismatch_targets_only_stale_non_terminal_runs() {
        // 历史构建指纹不自动升级为执行兼容版本。
        let stale = "0".repeat(64);
        let stale = if stale == flow_engine::RUNTIME_ID {
            "1".repeat(64)
        } else {
            stale
        };
        assert!(runtime_mismatched(&waiting_run(&stale)));
        // 当前 runtime 的在途 run 不隔离
        assert!(!runtime_mismatched(&waiting_run(flow_engine::RUNTIME_ID)));
        // 终态（failed）的陈旧 run 不隔离
        let mut failed = waiting_run(&stale);
        failed.status = RunStatus::Failed;
        assert!(!runtime_mismatched(&failed));
        // 无执行快照（semantics=1）不隔离
        let mut legacy = waiting_run(&stale);
        legacy.execution = None;
        assert!(!runtime_mismatched(&legacy));
    }
}

#[cfg(test)]
mod atomic_command_tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_between_preflight_and_signal_commit_publishes_no_chunks() {
        let dir = flow_test_support::io::TempDir::new("atomic-signal-rejection");
        let b = JournalBackend::open(dir.path(), Default::default())
            .await
            .unwrap();
        let wf=b.workflow_import("atomic",json!({"semantics":2,"nodes":[{"id":"s","type":"start"},{"id":"n","type":"human_task"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),true,None).await.unwrap();
        let started = b
            .run_start(
                wf.result["workflow_id"].as_str().unwrap(),
                None,
                Value::Null,
                "manual",
                None,
                None,
            )
            .await
            .unwrap();
        let run = started.result["run_id"].as_str().unwrap();
        let payload = json!({"large":"x".repeat(700_000)});
        let request = json!({"run_id":run,"node_id":"n","payload":payload});
        let validate = |state: &State| {
            signal_decision(
                state,
                run,
                "n",
                &payload,
                Some("race"),
                true,
                &flow_journal::StoredValue::Inline(Value::Null),
            )
            .map(|_| ())
        };
        assert!(b
            .preflight_command("run.signal", Some("race"), &request, Some(run), validate)
            .await
            .unwrap()
            .is_none());
        let prepared = b
            .journal
            .prepare_json(payload.clone(), flow_journal::VALUE_BUDGET)
            .await
            .unwrap();
        let output = prepared.stored().clone();
        b.run_cancel(run, None).await.unwrap();
        assert!(b
            .command_with_value(
                "run.signal",
                Some("race"),
                &request,
                Some(run),
                prepared,
                |state| signal_decision(state, run, "n", &payload, Some("race"), true, &output)
            )
            .await
            .is_err());
        let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM value_chunks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        b.close().await.unwrap();
    }
}
