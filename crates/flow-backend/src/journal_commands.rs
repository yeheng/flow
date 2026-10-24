//! 产品面命令的裁决层：把 RPC 请求变成一次 journal 决策与一份永久回执。
//!
//! 本模块只决定「要写哪些事实」，不碰执行、不碰外部系统：每条命令在
//! 决策锁下算出事件，再由 `journal::command_selected` 与正文、当前行、
//! 回执一起提交。
//! 它守护的核心不变量是「幂等 + 同一提交决定」：`scope` + `request_id`
//! 唯一标识一次命令，同身份同参数原样返回旧回执，同身份不同参数报冲突。
//! 需要它，是因为产品面的重试、传输失败与并发重复都必须收敛到同一条
//! 命令身份上——否则一次重发就可能变成两次业务副作用。
use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use flow_engine::journal_state::{DefinitionVersion, Run, RunStatus, State, WaitKind, Workflow};
use flow_journal::{Event, EventKind};
use serde_json::{json, Value};
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, JournalError>;

/// 统一错误构造：参数/合同问题一律是 `Invalid`，与存储故障区分开。
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

/// 生成新实体的身份 ID（UUIDv7：时间有序，便于按创建顺序分页）。
pub(crate) fn id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// 生成 RFC3339 时间戳（统一 UTC，避免各处时区不一致）。
pub(crate) fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// 校验工作流名称：非空且长度有界。
///
/// 上限防止超长名称撑爆索引键与列表响应；trim 后为空视为非法，
/// 因为空白名在产品面无法区分。
fn validate_workflow_name(name: &str) -> flow_journal::Result<()> {
    if name.trim().is_empty() || name.len() > 1024 {
        return Err(invalid("invalid workflow name"));
    }
    Ok(())
}

/// Shared normalization, validation and checksum for preflight, import and save.
///
/// 保存/导入/预检共用的定义准备结果：归一化后的 JSON、解析后的定义
/// 与内容摘要。三条路径必须得出同一摘要，否则「重复保存复用同一版本」
/// 的规则会在不同入口上产生分歧。
struct PreparedDefinition {
    value: Value,
    parsed: flow_engine::Definition,
    checksum: String,
}
impl PreparedDefinition {
    /// 归一化并校验一份定义：补齐语义版本、校验结构、计算摘要。
    fn new(mut value: Value) -> flow_journal::Result<Self> {
        if let Some(object) = value.as_object_mut() {
            // 未显式指定语义时按 v2 解释；显式传 1 仍可创建兼容版本。
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
    ///
    /// 摘要相同的最新版本直接复用它的值引用：重复保存不应产生第二份
    /// 无人引用的正文，值的引用闭包必须与事实一一对应。
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

    /// 列出定义里引用但当前环境未提供的密钥（节点 ID + 密钥名）。
    fn missing_secrets(&self) -> Vec<(String, String)> {
        flow_engine::secrets::missing_secrets(&self.parsed)
    }

    /// 缺密钥时拒绝保存：定义引用的密钥必须在保存时就能被解析，
    /// 否则运行期才发现缺失，错误会落在一次真实执行上。
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

/// 按名称找导入目标：只允许命中唯一的未删除工作流。
///
/// 多个同名工作流必须报冲突而不是随便挑一个——按名导入写错对象的
/// 代价比拒绝大得多。
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
///
/// 摘要相同即复用旧版本（不产生新事件）；否则版本号 +1 并生成
/// WorkflowUpdated 事实。新版本一律是草稿，发布是独立动作。
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
///
/// 幂等前置检查：命中已提交命令就直接返回旧回执，调用方据此跳过
/// 大输入的重复 store。
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
///
/// 「字段缺失」返回 `None`，「字段存在但类型不对」返回错误——
/// 两者语义不同：前者允许沿用旧值，后者必须拒绝。
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
    ///
    /// 只解析控制字段（ID、cron、开关等）；`input`/`nodes`/`edges` 这类
    /// 用户数据保持 `Value` 原样透传——在边缘解析一次，内部只传枚举，
    /// 既省掉重复解析，又避免用户数据被误当成控制字段校验。
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

    /// 是否是「创建」类命令（创建与更新的存在性检查不同）。
    fn creating(&self) -> bool {
        matches!(
            self,
            Self::ScheduleCreate { .. }
                | Self::WebhookCreate { .. }
                | Self::TemplateUpsert { creating: true, .. }
        )
    }

    /// 命令作用的实体键（schedule id / webhook token / template id）。
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
    /// 业务调用入口：按 hook 配置启动一次 run（幂等键来自业务请求）。
    ///
    /// 固定已发布版本 + 业务密钥摘要共同决定「这次调用是否合法」；
    /// 配置在调用之间被改动时必须拒绝，而不是按新配置执行。
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
        // 重试时沿用原回执里的版本：调用方重试的是同一次业务调用，
        // 不该因为期间发布了新版本而换掉执行语义。
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

    /// 列出某类触发器（schedule / webhook）的配置快照。
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

    /// 由触发器（cron / webhook）启动一次 run。
    ///
    /// 幂等键 = scope（触发器身份）+ request_id；重试必须复用原键，
    /// 否则同一个时隙会被触发两次。
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
            // 定时触发的输入由配置固定，必须取回原 run 已提交的输入，
            // 不能用当前配置里的输入——配置可能已被改过。
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
    ///
    /// 裁决只记录人的判断，绝不代造外部操作结果——`OperationOutcome`
    /// 只能来自真实的外部返回，否则证据链就断了。
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
    ///
    /// 预检不写任何事实，只回报「定义是否可用、缺哪些密钥」——
    /// 编辑器需要即时反馈，而预检写库会污染事实序列。
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
    ///
    /// 按名导入：唯一命中则更新，未命中则新建，多命中报冲突且不发布任何值。
    /// 创建、版本与发布三条事实同一事务提交，读者不会看到「已创建但无版本」
    /// 的中间态。
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
        // 元数据写锁：定义写入与依赖冻结必须互斥，否则冻结可能读到
        // 半更新的元数据视图。
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

    /// 新建工作流（只有名字，定义由后续 update 写入）。
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

    /// 保存一个新版本定义（不自动发布）。
    ///
    /// 内容与最新版本摘要相同则复用旧版本，不产生新的正文与版本号——
    /// 重复保存不应让版本历史膨胀。
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
    /// 发布一个已保存的版本（发布是单调的，只前进不回退）。
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
    /// 删除工作流，并同时停用引用它的定时器与 webhook。
    ///
    /// 引用它的触发器必须一并停用——留着指向已删工作流的触发器，
    /// 下一次触发会在提交前校验处失败，错误现场还更难定位。
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
    ///
    /// 启动一次 run：冻结定义版本、冻结执行快照、校验输入，然后写入
    /// RunStarted 事实。触发器的存在性、启用状态与配置在提交前会再校验
    /// 一次——预检与提交之间配置可能已变。
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

    /// 启动一次 run 的统一实现（普通启动、回归候选与业务调用共用）。
    ///
    /// `regression` 非空表示这是一次回归候选：时间、随机种子、agent 配置
    /// 都继承 baseline 的快照，而不是取当前环境。
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
            // 业务调用额外加一段前缀：它的幂等身份来自业务密钥，
            // 与后台触发器不能共用同一个 scope 空间。
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
            // 业务调用需要执行快照（时间/种子/依赖）才可复现，
            // 语义 1 的定义没有这套快照。
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
                // 预检到提交之间触发器可能被改掉或停用；此时必须拒绝，
                // 而不是按旧快照执行一次已经不该发生的调用。
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
                    // request_id 即触发时隙（RFC3339）：同一时隙只允许提交一次。
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
                    // 早于触发器启用时间的时隙不予接受（补跑历史时隙）。
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
                // 在飞 run 数达上限：拒绝而不是排队，排队会让内存与
                // 调度表无界增长。
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
    /// 取消一个 run（并级联取消其子 run）。
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
                    // 只有非终态 run 才写取消事实；已终态时命令仍然成功，
                    // 但不产生新的事实——重复取消不应污染序列。
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
    /// 向节点投递一次信号（人工输入、裁决或业务 signal）。
    ///
    /// 三种落点：节点正在等信号 → 立即唤醒；节点等待裁决 → 写 Adjudicated；
    /// 节点尚未到达但属于 human_task/input → 存入持久 inbox 等待消费。
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
        // 预检阶段先判定这次信号是「裁决」还是普通信号，提交阶段再核对
        // 判定是否一致——节点状态在两次之间变了就必须报冲突。
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
            // 裁决事件负载的是「裁决后的输出」，不是原始信号载荷。
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
    /// 修改触发器配置（schedule / webhook）的原始 patch 入口。
    ///
    /// 允许写入的字段是白名单：只有白名单里的键才可能落库，
    /// 防止 patch 借机写入 `id`/`token` 之类的身份字段。
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

/// 构造一个节点事件（带 run/node/dispatch 归属与 run_seq）。
///
/// `public=true` 表示这条事实进入产品面的公开序列（run_seq 递增）；
/// 内部审计事实不递增，避免污染公开的运行序号。
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
    // dispatch_id 缺失时留空：它用于对账 attempt，没有 attempt 的
    // 事件（如跳过）本来就没有对应的 dispatch。
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
///
/// 产品面 CRUD 的决策：读旧值 → 合并新值 → 校验 → 生成事实。
/// 返回值里的 JSON 是命令回执（不是事实载荷）。
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

/// 原始 patch 入口的决策：把 patch 合并进旧配置，再走统一校验。
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
    // 逐键覆盖：patch 的语义是「合并」，未出现的字段保留旧值。
    for (k, v) in object {
        value[k] = v.clone();
    }
    trigger_event(state, kind, key, value)
}

/// 触发器配置的最终校验与事实生成（typed 与 patch 两条路径共用）。
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
        // 触发器数量有上界：配置是无主的数据，不设限会被慢慢撑爆。
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
            // 触发器必须指向存在的工作流，否则触发时才失败、难定位。
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
                // 新启用或改了 cron 表达式：重置 active_since，
                // 早于此刻的时隙不再补跑。
                value["active_since"] = json!(now());
            }
        }
        if kind == EventKind::WebhookChanged
            && (!value["invoke_version"].is_null() || !value["invoke_key_hash"].is_null())
        {
            // 业务调用配置必须成对且有效：固定已发布版本 + SHA-256 摘要。
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
/// 模板的原始 patch 入口：与触发器 patch 同构，合并后交给统一校验。
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

/// 模板配置的最终校验与事实生成。
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
        // 先归一化再校验：片段合并后必须是完整合法的模板。
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
///
/// 输入合同在派发前校验：不合法的输入若等到执行期才失败，会留下一条
/// 已经消耗掉外部资源的失败记录。
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
///
/// 只针对非终态 run：终态 run 只是历史记录，不需要隔离；没有执行快照
/// 的语义 1 run 也不涉及执行器兼容版本。
pub(crate) fn runtime_mismatched(run: &Run) -> bool {
    !run.terminal()
        && run
            .execution
            .as_ref()
            .is_some_and(|e| e.runtime != flow_engine::RUNTIME_ID)
}

/// 要求配置行是 JSON 对象（配置一律按字段合并，非对象无法安全合并）。
fn require_object(value: &Value) -> flow_journal::Result<()> {
    if value.is_object() {
        Ok(())
    } else {
        Err(invalid("malformed config row: expected object"))
    }
}

/// 信号的决策：判断落点并生成相应事实。
///
/// 三条路径互斥：等信号 → 唤醒；等裁决 → 人工裁决；两者都不是但节点
/// 接受 inbox → 排队等待。早到信号必须排队而不是丢弃。
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
        // 节点不在等待：只有「接受 inbox 且带幂等键」的信号可以先排队，
        // 否则无法保证不重复消费。
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
            // 每个节点只有一个输入槽：另一个信号竞争已占用的槽返回冲突。
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
        // 裁决动作的语义与 run.adjudicate 一致：retry 授权重发，
        // failed 判失败，succeeded（默认）接受产出。
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
    // 两条事实必须占两个连续序号：公开序列里的序号是连续的，
    // 跳号会让读面把中间状态误判为丢失。
    resolved.run_seq = run.last_run_seq + 2;
    Ok((
        vec![signal, resolved],
        json!({"delivered":true,"status":"applied","signal_id":request_id}),
    ))
}

#[cfg(test)]
mod runtime_mismatch_tests {
    use super::*;

    /// 构造一个最小的非终态 run，并注入指定的 runtime 指纹。
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

    /// runtime 不匹配的判定只针对「在飞且带执行快照」的 run。
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
    /// 预检通过、提交前 run 被取消时，命令必须整体失败且不发布任何正文分块。
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
