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
/// 产品面 CRUD 的强类型载荷：在 RPC 边缘经 [`ProductCommand::parse`] 一次
/// 解析，内部只传枚举。`input`/`nodes` 等纯透传的用户数据保留 `Value`；
/// 事件载荷与命令指纹的 wire 形状不变（协议冻结，只改 Rust 内部表示）。
#[derive(Debug, Clone)]
pub enum ProductCommand {
    ScheduleCreate {
        key: String,
        workflow_id: String,
        cron: String,
        enabled: Option<bool>,
        input: Value,
    },
    ScheduleUpdate {
        id: String,
        /// 原样透传：非法形状由 trigger_decision 在锁内裁决（语义保持）。
        cron: Option<Value>,
        input: Option<Value>,
        enabled: Option<Value>,
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
        name: Option<Value>,
        category: Option<Value>,
        nodes: Option<Value>,
        edges: Option<Value>,
    },
    TemplateDelete {
        id: String,
    },
}

impl ProductCommand {
    /// RPC 边缘解析。错误文本是 wire 契约的一部分，与旧字段级获取器逐字相同。
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
                enabled: p["enabled"].as_bool(),
                input: p["input"].clone(),
            },
            "schedule.update" => Self::ScheduleUpdate {
                id: field("id")?.to_owned(),
                cron: p.get("cron").cloned(),
                input: p.get("input").cloned(),
                enabled: p.get("enabled").cloned(),
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
                name: p.get("name").cloned(),
                category: p.get("category").cloned(),
                nodes: p.get("nodes").cloned(),
                edges: p.get("edges").cloned(),
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
    fn namespace(&self) -> &'static str {
        match self {
            Self::ScheduleCreate { .. }
            | Self::ScheduleUpdate { .. }
            | Self::ScheduleDelete { .. } => "schedule",
            Self::WebhookCreate { .. }
            | Self::WebhookSetEnabled { .. }
            | Self::WebhookDelete { .. } => "webhook",
            Self::TemplateUpsert { .. } | Self::TemplateDelete { .. } => "template",
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
    fn deleting(&self) -> bool {
        matches!(
            self,
            Self::ScheduleDelete { .. } | Self::WebhookDelete { .. } | Self::TemplateDelete { .. }
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
            .ok_or_else(|| invalid("webhook not found"))?;
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
                .ok_or_else(|| invalid("run not found"))?;
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
        let output = self.store_json(output, 8 * 1024 * 1024).await?;
        self.command_for_run(run_id, &format!("run.adjudicate:{run_id}"),Some(request_id),&request,|state|{
            let run=state.runs.get(run_id).ok_or_else(||invalid("run not found"))?;
            let node=run.nodes.get(node_id).ok_or_else(||invalid("node not found"))?;
            // outcome 已提交(结果被捕获、输出推导未达)的 Uncertain 节点同样
            // 允许人工收口:accept_output/failed 可用已提交证据裁决;只有
            // retry 需要未决 outcome——结果已捕获后重发授权没有意义,重派发
            // 会撞「operation already authorized」。
            if run.terminal() || node.wait.as_ref().is_none_or(|w| w.kind() != WaitKind::Uncertain)
                || node.operation.as_ref().is_none_or(|o|o.operation_id!=operation_id
                    || (o.outcome.is_some() && decision == "retry")) {
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
        mut definition: Value,
        request_id: Option<&str>,
    ) -> Result<CommandReceipt> {
        // 先注入 semantics 缺省再取指纹：指纹必须与语义裁决同源——同一
        // 有效定义（显式 semantics:2 vs 缺省）不能因指纹口径不同而漏判
        // 幂等窗口（checksum 用注入后的定义，指纹也要）。
        if let Some(object) = definition.as_object_mut() {
            object.entry("semantics").or_insert(json!(2));
        }
        let fingerprint_request = json!({"workflow_id":workflow_id,"definition":definition});
        let parsed = flow_engine::journal_state::validate_definition(&definition)?;
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
        let stored = self
            .store_json(definition, flow_journal::MAX_LINE_BYTES)
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
                if state.run_workflows.contains(workflow_id) {
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
        if let Some(schema) = &definition.input_schema {
            flow_engine::schema::validate(schema, &input).map_err(invalid)?;
        }
        let stored = self.store_json(input, 8 * 1024 * 1024).await?;
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
        }
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
            let workflow = state
                .workflows
                .get(workflow_id)
                .ok_or_else(|| invalid("workflow not found"))?;
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
        let snapshot = self
            .run_snapshot(run_id)
            .await?
            .ok_or_else(|| invalid("run not found"))?;
        let definition = self.cached_definition(run_id, &snapshot.definition).await?;
        let accepts_inbox = definition.node_type(node_id).is_some_and(|k| {
            matches!(
                k,
                flow_engine::NodeType::HumanTask | flow_engine::NodeType::Input
            )
        });
        let output = self.store_json(payload.clone(), 8 * 1024 * 1024).await?;
        let adjudicated_output = self
            .store_json(
                payload.get("output").cloned().unwrap_or(Value::Null),
                8 * 1024 * 1024,
            )
            .await?;
        self.command_for_run(
            run_id,
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
                let waiting = run.nodes.get(node_id).is_some_and(|n| n.wait.as_ref().is_some_and(|w| matches!(w.kind(), WaitKind::Signal | WaitKind::Uncertain)));
                if !waiting {
                    if !accepts_inbox || request_id.is_none() || run.nodes.get(node_id).is_some_and(|n| matches!(n.status, flow_engine::journal_state::NodeStatus::Succeeded | flow_engine::journal_state::NodeStatus::Failed | flow_engine::journal_state::NodeStatus::Skipped)) {
                        return Err(invalid("node does not accept a pending signal"));
                    }
                    if run.signals.contains_key(node_id) { return Err(flow_journal::Error::Conflict("signal already queued for node".into())); }
                    let mut event = Event::new(EventKind::SignalReceived, json!({"payload":output,"queued":true}));
                    event.run_id = Some(run_id.into()); event.node_id = Some(node_id.into()); event.run_seq = run.last_run_seq + 1;
                    return Ok((vec![event], json!({"delivered":true,"status":"queued","signal_id":request_id})));
                }
                let node = &run.nodes[node_id];
                if !run.terminal() && node.wait.as_ref().is_some_and(|w| w.kind() == WaitKind::Uncertain) {
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
                if run.terminal() || node.wait.as_ref().is_none_or(|w| w.kind() != WaitKind::Signal) {
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

/// 锁内的产品命令决策：存在性检查、patch 组装与存储文档裁决都在同一把
/// 锁下完成。patch 字段是持久化文档的一部分，保持既有 JSON 形状。
fn product_decision(
    state: &State,
    command: &ProductCommand,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let namespace = command.namespace();
    let creating = command.creating();
    let deleting = command.deleting();
    let key = command.key();
    if key.is_empty() || key.len() > 128 {
        return Err(invalid("invalid entity key"));
    }
    let map = match namespace {
        "schedule" => &state.schedules,
        "webhook" => &state.webhooks,
        "template" => &state.templates,
        _ => return Err(invalid("invalid product command")),
    };
    if !creating && !map.contains_key(key) {
        if namespace == "template" && deleting {
            return Ok((vec![], json!({"deleted": false})));
        }
        return Err(invalid(format!("{namespace} not found")));
    }
    let mut patch = serde_json::Map::new();
    if deleting {
        patch.insert("deleted".into(), json!(true));
    } else {
        match command {
            ProductCommand::ScheduleCreate {
                workflow_id,
                enabled,
                input,
                cron,
                ..
            } => {
                patch.insert("workflow_id".into(), json!(workflow_id));
                patch.insert("enabled".into(), json!(enabled.unwrap_or(true)));
                patch.insert("input".into(), input.clone());
                patch.insert("cron_expr".into(), json!(cron));
            }
            ProductCommand::ScheduleUpdate {
                cron,
                input,
                enabled,
                ..
            } => {
                if let Some(value) = cron {
                    patch.insert("cron_expr".into(), value.clone());
                }
                if let Some(value) = input {
                    patch.insert("input".into(), value.clone());
                }
                if let Some(value) = enabled {
                    patch.insert("enabled".into(), value.clone());
                }
            }
            ProductCommand::WebhookCreate { workflow_id, .. } => {
                patch.insert("workflow_id".into(), json!(workflow_id));
                patch.insert("enabled".into(), json!(true));
            }
            ProductCommand::WebhookSetEnabled { enabled, .. } => {
                patch.insert("enabled".into(), json!(enabled));
            }
            ProductCommand::TemplateUpsert {
                name,
                category,
                nodes,
                edges,
                creating,
                ..
            } => {
                for (key, value) in [
                    ("name", name),
                    ("category", category),
                    ("nodes", nodes),
                    ("edges", edges),
                ] {
                    if let Some(value) = value {
                        patch.insert(key.into(), value.clone());
                    }
                }
                if *creating {
                    patch.entry("category").or_insert(Value::Null);
                }
            }
            _ => return Err(invalid("invalid product command")),
        }
    }
    let (events, mut value) = if namespace == "template" {
        template_decision(state, key, &Value::Object(patch))?
    } else {
        trigger_decision(
            state,
            if namespace == "schedule" {
                EventKind::ScheduleChanged
            } else {
                EventKind::WebhookChanged
            },
            key,
            &Value::Object(patch),
        )?
    };
    let result = if deleting {
        json!({"deleted": true})
    } else if namespace == "webhook" && !creating {
        json!({"updated": true})
    } else if namespace == "schedule" {
        // next_fire_at 是派生展示值，但会随 ScheduleChanged 事实持久化进
        // 权威配置——表示必须与全库其余时间戳同口径（UTC）。cron 字段
        // 本身按产品契约是本地时间解释（"0 9 * * *" = 本地 9 点），因此
        // 用 Local 参考时刻求值、再转 UTC 表示：同一时刻，可比的字符串。
        let next = value["cron_expr"]
            .as_str()
            .and_then(|expr| expr.parse::<cron_parser::Schedule>().ok())
            .and_then(|cron| cron.next_after(&chrono::Local::now()))
            .map(|t| t.with_timezone(&chrono::Utc).to_rfc3339());
        value["next_fire_at"] = json!(next);
        if creating {
            value.as_object_mut().unwrap().remove("input");
            value
        } else {
            json!({"updated": true})
        }
    } else {
        value.as_object_mut().unwrap().remove("nodes");
        value.as_object_mut().unwrap().remove("edges");
        value
    };
    Ok((events, result))
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
    let previous_enabled = value["enabled"] == true;
    let previous_cron = value["cron_expr"].clone();
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
            .ok_or_else(|| invalid("workflow_id required"))?
            .to_owned();
        if !state.workflows.contains_key(&workflow) {
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
