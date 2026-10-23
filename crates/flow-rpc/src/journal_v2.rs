//! Explicit single-user JSONL RPC endpoint — the one product surface. Every call
//! authenticates, including pages. Product form (`module_product`) additionally
//! carries the config/secrets assembly and the materialized product reads
//! (`workflow.*.view` / `run.*.view` / triggers / templates), so the browser
//! frontend, the embedded desktop service and flow-cli all speak this module.
//!
//! 分发结构：`module_product` 只做注册；每条请求先过统一的前置层（参数
//! 归一 → token 常时比较 → request_id 幂等键检查），再由 `dispatch_call`
//! 把方法路由到独立的命名 Handler。对外 JSON-RPC 2.0 契约（错误码、错误
//! 文本、回执形状）逐字保持，改动仅限内部组织。
use flow_backend::journal::{JournalBackend, JournalError};
use flow_journal::page::{Cursor, Filter};
use jsonrpsee::{types::ErrorObjectOwned, RpcModule};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc};

pub struct Context {
    backend: Arc<JournalBackend>,
    token: String,
    /// 产品装配（统一配置面 + 持久化密钥）。None = 未装配（开发模式），
    /// config/secrets 方法报「未启用」。
    product: Option<Arc<crate::AppState>>,
}
use crate::invalid;
fn failure(error: JournalError) -> ErrorObjectOwned {
    match error {
        JournalError::Journal(flow_journal::Error::Conflict(message)) => {
            ErrorObjectOwned::owned(-32012, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Limit(message)) => {
            ErrorObjectOwned::owned(-32021, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::NotFound(message)) => {
            ErrorObjectOwned::owned(-32011, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Invalid(message)) => invalid(message),
        _ => ErrorObjectOwned::owned(-32603, "journal service unavailable", None::<()>),
    }
}
fn view_failure(error: flow_backend::ViewError) -> ErrorObjectOwned {
    failure(error.into())
}
fn storage_failure(error: flow_backend::StorageError) -> ErrorObjectOwned {
    if matches!(error, flow_backend::StorageError::ResponseTooLarge) {
        ErrorObjectOwned::owned(-32021, "RESPONSE_TOO_LARGE", None::<()>)
    } else {
        ErrorObjectOwned::owned(-32603, "projection unavailable", None::<()>)
    }
}
fn decimal_param(p: &Value, key: &str) -> Result<u64, ErrorObjectOwned> {
    match p.get(key) {
        None => Ok(0),
        Some(Value::String(s))
            if !s.is_empty()
                && (s == "0" || !s.starts_with('0'))
                && s.bytes().all(|b| b.is_ascii_digit()) =>
        {
            s.parse().map_err(invalid)
        }
        _ => Err(invalid(format!("{key} must be a decimal string"))),
    }
}
fn text<'a>(params: &'a Value, key: &str) -> Result<&'a str, ErrorObjectOwned> {
    params[key]
        .as_str()
        .ok_or_else(|| invalid(format!("missing string {key}")))
}
fn optional_version(params: &Value) -> Result<Option<u64>, ErrorObjectOwned> {
    match params.get("version") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(v)) => v.parse().map(Some).map_err(invalid),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| invalid("invalid version")),
    }
}

// ---- 写命令的边缘解析：参数一次 struct 化，键名只在 parse 里出现一次 ----
// 错误文本与旧字段级获取器逐字相同（"missing string {key}" 是 wire 契约）。

struct WorkflowCreateReq<'a> {
    name: &'a str,
}
impl<'a> WorkflowCreateReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            name: text(p, "name")?,
        })
    }
}

struct WorkflowUpdateReq<'a> {
    workflow_id: &'a str,
    definition: Value,
}
impl<'a> WorkflowUpdateReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
            definition: p["definition"].clone(),
        })
    }
}

struct WorkflowPublishReq<'a> {
    workflow_id: &'a str,
    version: u64,
}
impl<'a> WorkflowPublishReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
            version: optional_version(p)?.ok_or_else(|| invalid("version required"))?,
        })
    }
}

struct WorkflowDeleteReq<'a> {
    workflow_id: &'a str,
}
impl<'a> WorkflowDeleteReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
        })
    }
}

struct RunStartReq<'a> {
    workflow_id: &'a str,
    version: Option<u64>,
    input: Value,
}
impl<'a> RunStartReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
            version: optional_version(p)?,
            input: p["input"].clone(),
        })
    }
}

struct RunCancelReq<'a> {
    run_id: &'a str,
}
impl<'a> RunCancelReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            run_id: text(p, "run_id")?,
        })
    }
}

struct RunSignalReq<'a> {
    run_id: &'a str,
    node_id: &'a str,
    payload: Value,
}
impl<'a> RunSignalReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            run_id: text(p, "run_id")?,
            node_id: text(p, "node_id")?,
            payload: p["payload"].clone(),
        })
    }
}

struct RunAdjudicateReq<'a> {
    run_id: &'a str,
    node_id: &'a str,
    operation_id: &'a str,
    reason: &'a str,
    output: Value,
    decision: &'a str,
}
impl<'a> RunAdjudicateReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            run_id: text(p, "run_id")?,
            node_id: text(p, "node_id")?,
            operation_id: text(p, "operation_id")?,
            reason: text(p, "reason")?,
            output: p["output"].clone(),
            decision: p["decision"].as_str().unwrap_or("accept_output"),
        })
    }
}

struct ConfigChangeReq<'a> {
    key: &'a str,
    patch: Value,
}
impl<'a> ConfigChangeReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            key: text(p, "key")?,
            patch: p["patch"].clone(),
        })
    }
}

struct CommandStatusReq<'a> {
    scope: &'a str,
    request_id: &'a str,
}
impl<'a> CommandStatusReq<'a> {
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            scope: text(p, "scope")?,
            request_id: text(p, "request_id")?,
        })
    }
}

use flow_dto::rpc::Method;
pub use flow_dto::rpc::{is_read_method, is_write_method, READ_METHODS, WRITE_METHODS};

/// 写命令的服务端命令 scope（`command.status` 查询键）。run.start 带来源
/// 归因、run 族按 run_id 分域；其余就是方法名。
pub fn command_scope(method: &str, params: &Value) -> String {
    flow_dto::command_scope(method, params)
}

pub fn module(
    backend: Arc<JournalBackend>,
    token: String,
) -> Result<RpcModule<Context>, Box<dyn std::error::Error>> {
    module_product(backend, token, None)
}

/// 产品形态：装配统一配置面和持久化密钥（journal-server / 桌面内嵌用）。
pub fn module_product(
    backend: Arc<JournalBackend>,
    token: String,
    product: Option<Arc<crate::AppState>>,
) -> Result<RpcModule<Context>, Box<dyn std::error::Error>> {
    if token.len() < 32 {
        return Err("FLOW_JOURNAL_TOKEN must contain at least 32 bytes".into());
    }
    let mut module = RpcModule::new(Context {
        backend,
        token,
        product,
    });
    for &method in Method::ALL {
        module.register_async_method(method.as_str(), move |params, ctx, _| async move {
            dispatch_call(&ctx, method, params).await
        })?;
    }
    module.register_subscription(
        "run.subscribe",
        "run.event",
        "run.unsubscribe",
        |params, pending, ctx, _| async move { subscribe_run_events(params, pending, &ctx).await },
    )?;
    Ok(module)
}

/// 全量 token 常时比较（不在第一个差异字节提前退出）。
fn constant_time_eq(supplied: &str, expected: &str) -> bool {
    supplied.len() == expected.len()
        && supplied
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

/// 统一前置层：token 校验 + `_token` 剥离 + request_id 幂等键检查。
/// 通过后返回 `(归一参数, request_id)`。
async fn admit_call(
    ctx: &Context,
    method: &str,
    params: jsonrpsee::types::Params<'_>,
) -> Result<(Value, Option<String>), ErrorObjectOwned> {
    // JSON-RPC 允许整体省略 params（到达的是 null）——归一为空对象。
    let mut p: Value = match params.parse::<Value>() {
        Ok(Value::Null) => json!({}),
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    let supplied = p.get("_token").and_then(Value::as_str).unwrap_or("");
    if !constant_time_eq(supplied, &ctx.token) {
        return Err(ErrorObjectOwned::owned(-32001, "unauthorized", None::<()>));
    }
    p.as_object_mut()
        .ok_or_else(|| invalid("object params required"))?
        .remove("_token");
    let request_id = p
        .get("request_id")
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("request_id must be a string"))
        })
        .transpose()?;
    let request = request_id.as_deref();
    // 写命令必须带稳定 request_id：无幂等键的客户端重试=双 run/双
    // workflow（服务端无 UUID 透传键可去重）。读面免检。
    if is_write_method(method) && request.is_none_or(|id| id.trim().is_empty()) {
        return Err(invalid("request_id required for write commands"));
    }
    Ok((p, request_id))
}

/// 单条方法分发：cluster 族与读面直接返回；写命令产出回执。
async fn dispatch_call(
    ctx: &Context,
    method: Method,
    params: jsonrpsee::types::Params<'_>,
) -> Result<Value, ErrorObjectOwned> {
    let (p, request_id) = admit_call(ctx, method.as_str(), params).await?;
    let request = request_id.as_deref();
    let b = &ctx.backend;
    let receipt = match method {
        // ---- 观测 / 读面（直接返回，不走回执解包） ----
        Method::RunObservationsPage => return observations_page(b, &p).await,
        Method::CommandStatus => {
            let req = CommandStatusReq::parse(&p)?;
            return serde_json::to_value(
                b.command_status(req.scope, req.request_id)
                    .await
                    .map_err(failure)?,
            )
            .map_err(invalid);
        }
        Method::WorkflowList | Method::RunList | Method::LegacyList => {
            return snapshot_list(b, method.as_str(), &p).await
        }
        Method::WorkflowGet | Method::RunGet | Method::LegacyGet => {
            return snapshot_get(b, method.as_str(), &p).await
        }
        Method::RunEventsPage | Method::RunAuditPage => {
            return events_page(b, method.as_str(), &p).await
        }

        // ---- 写命令（CommandReceipt 回执面）：边缘一次 struct 化 ----
        Method::WorkflowValidate => {
            return b
                .workflow_validate(p["definition"].clone())
                .map_err(failure)
        }
        Method::WorkflowImport => {
            let publish = p
                .get("publish")
                .map(|v| {
                    v.as_bool()
                        .ok_or_else(|| invalid("publish must be boolean"))
                })
                .transpose()?
                .unwrap_or(true);
            b.workflow_import(text(&p, "name")?, p["definition"].clone(), publish, request)
                .await
        }
        Method::WorkflowCreate => {
            let req = WorkflowCreateReq::parse(&p)?;
            b.workflow_create(req.name, request).await
        }
        Method::WorkflowUpdate => {
            let req = WorkflowUpdateReq::parse(&p)?;
            b.workflow_update(req.workflow_id, req.definition, request)
                .await
        }
        Method::WorkflowPublish => {
            let req = WorkflowPublishReq::parse(&p)?;
            b.workflow_publish(req.workflow_id, req.version, request)
                .await
        }
        Method::WorkflowDelete => {
            let req = WorkflowDeleteReq::parse(&p)?;
            b.workflow_delete(req.workflow_id, request).await
        }
        Method::RunStart => {
            let req = RunStartReq::parse(&p)?;
            b.run_start(
                req.workflow_id,
                req.version,
                req.input,
                "manual",
                None,
                request,
            )
            .await
        }
        Method::RunRegress => {
            b.run_regression_with_fixtures(
                text(&p, "baseline")?,
                p["workflow_id"].as_str(),
                optional_version(&p)?,
                p.get("fixtures")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(invalid)?
                    .unwrap_or_default(),
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::RunCompare => {
            return b
                .compare_runs(text(&p, "baseline")?, text(&p, "candidate")?)
                .await
                .map_err(failure)
        }
        Method::RunCancel => {
            let req = RunCancelReq::parse(&p)?;
            b.run_cancel(req.run_id, request).await
        }
        Method::RunSignal => {
            let req = RunSignalReq::parse(&p)?;
            b.run_signal(req.run_id, req.node_id, req.payload, request)
                .await
        }
        Method::RunAdjudicate => {
            let req = RunAdjudicateReq::parse(&p)?;
            b.run_adjudicate(
                req.run_id,
                req.node_id,
                req.operation_id,
                req.reason,
                req.output,
                req.decision,
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::ScheduleChange | Method::WebhookChange => {
            let kind = if method == Method::ScheduleChange {
                flow_journal::EventKind::ScheduleChanged
            } else {
                flow_journal::EventKind::WebhookChanged
            };
            let req = ConfigChangeReq::parse(&p)?;
            b.config_change(method.as_str(), kind, req.key, req.patch, request)
                .await
        }
        Method::ScheduleCreate
        | Method::ScheduleUpdate
        | Method::ScheduleDelete
        | Method::WebhookCreate
        | Method::WebhookSetEnabled
        | Method::WebhookDelete
        | Method::TemplateCreate
        | Method::TemplateUpdate
        | Method::TemplateDelete => return product_command_route(ctx, method.as_str(), &p).await,

        // ---- 产品面：V2 product views（journal_views 物化，直接返回） ----
        Method::WorkflowListView => return workflow_list_view(b).await,
        Method::WorkflowGetView => return workflow_get_view(b, &p).await,
        Method::WorkflowVersions => return workflow_versions(b, &p).await,
        Method::RunGetView => return run_get_view(b, &p).await,
        Method::RunListView => return run_list_view(b, &p).await,
        Method::RunStats => {
            let stats = flow_backend::journal_views::run_stats(b, p["workflow_id"].as_str())
                .await
                .map_err(view_failure)?;
            return serde_json::to_value(stats).map_err(invalid);
        }
        Method::RunTimeline => {
            return flow_backend::journal_views::timeline(b, text(&p, "run_id")?)
                .await
                .map_err(view_failure);
        }
        Method::RunEventsView => return events_view(b, &p).await,
        Method::ScheduleList => return schedule_list(b, &p).await,
        Method::WebhookList => {
            let webhooks = flow_backend::journal_views::list_webhooks(b, p["workflow_id"].as_str())
                .await
                .map_err(view_failure)?;
            return serde_json::to_value(json!({"webhooks": webhooks})).map_err(invalid);
        }
        Method::TemplateList => {
            let templates = flow_backend::journal_views::template_list(b)
                .await
                .map_err(view_failure)?;
            return Ok(json!({"templates": templates}));
        }
        Method::TemplateGet => {
            let template = flow_backend::journal_views::template_get(b, text(&p, "id")?)
                .await
                .map_err(view_failure)?;
            return serde_json::to_value(template).map_err(invalid);
        }
        Method::ConfigGet => {
            let product = ctx_product(ctx)?;
            return Ok(crate::config_view(&product));
        }
        Method::ConfigUpdate => return config_update(ctx, &p).await,
        Method::SecretsList => {
            let secrets: Vec<Value> = flow_backend::secrets::list_secrets()
                .into_iter()
                .map(|(name, source)| json!({"name": name, "source": source}))
                .collect();
            return Ok(json!({"secrets": secrets}));
        }
        Method::SecretsSet => return secrets_set(ctx, &p).await,
        Method::SecretsDelete => return secrets_delete(ctx, &p).await,
        Method::NodetypesList => {
            return Ok(json!({"node_types": crate::node_types()}));
        }
    }
    .map_err(failure)?;
    serde_json::to_value(receipt).map_err(invalid)
}

/// 观测分页：run 存在性以 projection 为准；store 未启用时返回空页 + 丢失
/// 标记（history_incomplete）。
async fn observations_page(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?.to_owned();
    if b.store
        .get_summary("run", &run_id)
        .await
        .map_err(invalid)?
        .1
        .is_none()
    {
        return Err(ErrorObjectOwned::owned(-32011, "run not found", None::<()>));
    }
    let Some(store) = b.observations.clone() else {
        return Ok(json!({
            "records": [],
            "loss": {"history_incomplete": true},
            "loss_scope": "store",
        }));
    };
    let from = decimal_param(p, "from_seq")?;
    let dispatch = p["dispatch_id"].as_str().map(str::to_owned);
    let limit = p["limit"].as_u64().unwrap_or(100).min(256) as usize;
    tokio::task::spawn_blocking(move || {
        let records = store
            .page(&run_id, dispatch.as_deref(), from, limit)
            .map_err(invalid)?;
        Ok(json!({
            "records": records,
            "loss": store.scoped_loss(&run_id, dispatch.as_deref()),
            "loss_scope": if dispatch.is_some() { "dispatch" } else { "run" },
            "store_loss": store.loss(),
        }))
    })
    .await
    .map_err(invalid)?
}

/// workflow.list / run.list / legacy.list：projection 快照 + 游标。
async fn snapshot_list(
    b: &JournalBackend,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    let kind = method.split('.').next().unwrap();
    let after = p["after"].as_str().unwrap_or("");
    let (lsn, values) = b
        .store
        .list(
            kind,
            after,
            p["limit"].as_u64().unwrap_or(64).min(256) as usize,
        )
        .await
        .map_err(storage_failure)?;
    Ok(json!({
        "snapshot_cursor": {"journal_id": b.journal.id(), "lsn": lsn.to_string()},
        "values": values,
    }))
}

/// workflow.get / run.get / legacy.get：单键快照。
async fn snapshot_get(
    b: &JournalBackend,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    let kind = method.split('.').next().unwrap();
    let key = text(
        p,
        match kind {
            "run" => "run_id",
            "legacy" => "key",
            _ => "workflow_id",
        },
    )?;
    let (lsn, value) = b.store.get(kind, key).await.map_err(storage_failure)?;
    Ok(json!({
        "snapshot_cursor": {"journal_id": b.journal.id(), "lsn": lsn.to_string()},
        "value": value,
    }))
}

/// run.events.page / run.audit.page：游标化 journal 事件/审计分页。
async fn events_page(
    b: &JournalBackend,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?.to_owned();
    let (upper, run) = b
        .store
        .get_summary("run", &run_id)
        .await
        .map_err(storage_failure)?;
    if run.is_none() {
        return Err(ErrorObjectOwned::owned(-32011, "run not found", None::<()>));
    }
    let filter = if method == "run.events.page" {
        Filter::Events
    } else {
        Filter::Audit
    };
    let identity = b.journal.id().to_owned();
    let cursor = match p.get("cursor").filter(|v| !v.is_null()) {
        Some(v) => serde_json::from_value::<Cursor>(v.clone()).map_err(invalid)?,
        None => Cursor::first(identity.clone(), run_id.clone(), filter, upper),
    };
    let limit = p
        .get("limit")
        .map(|v| v.as_u64().ok_or_else(|| invalid("invalid limit")))
        .transpose()?
        .unwrap_or(256)
        .min(256) as usize;
    let root = b.journal.root().to_path_buf();
    let page = tokio::task::spawn_blocking(move || {
        flow_journal::page::page(&root, &identity, &run_id, filter, cursor, upper, limit)
    })
    .await
    .map_err(invalid)?
    .map_err(|e| failure(e.into()))?;
    serde_json::to_value(page).map_err(invalid)
}

/// 产品面：工作流清单视图。
async fn workflow_list_view(b: &JournalBackend) -> Result<Value, ErrorObjectOwned> {
    let workflows = flow_backend::journal_views::list_workflows(b)
        .await
        .map_err(view_failure)?;
    Ok(json!({"workflows": workflows}))
}

/// 产品面：工作流版本视图（附 published_version 供编辑器渲染徽标）。
async fn workflow_get_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let version = flow_backend::journal_views::get_version(
        b,
        text(p, "workflow_id")?,
        optional_version(p)?
            .map(|v| i64::try_from(v).map_err(invalid))
            .transpose()?,
    )
    .await
    .map_err(view_failure)?;
    let published = flow_backend::journal_views::latest_published(b, text(p, "workflow_id")?)
        .await
        .map_err(view_failure)?;
    let mut value = serde_json::to_value(version).map_err(invalid)?;
    value["published_version"] = json!(published);
    Ok(value)
}

/// 产品面：工作流版本列表。
async fn workflow_versions(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let versions = flow_backend::journal_views::version_metadata(b, text(p, "workflow_id")?)
        .await
        .map_err(view_failure)?;
    Ok(json!({"versions": versions}))
}

/// 产品面：run 详情 + live 标记。
async fn run_get_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?;
    let run = flow_backend::journal_views::get_run(b, run_id)
        .await
        .map_err(view_failure)?;
    let live = flow_backend::journal_views::is_live(b, run_id).await;
    Ok(json!({"run": run, "live": live}))
}

/// 产品面：run 清单（status/source 过滤 + 分页）。
async fn run_list_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    if let Some(status) = p.get("status") {
        if !status
            .as_str()
            .is_some_and(flow_backend::RunStatus::is_valid_str)
        {
            return Err(invalid("invalid run status"));
        }
    }
    if let Some(source) = p.get("source") {
        if !source
            .as_str()
            .is_some_and(flow_backend::RunSource::is_valid_str)
        {
            return Err(invalid("invalid run source"));
        }
    }
    let runs = flow_backend::journal_views::list_runs(
        b,
        p["workflow_id"].as_str(),
        p["status"].as_str(),
        p["source"].as_str(),
        p["before_run_id"].as_str(),
        p["limit"].as_i64().unwrap_or(50).clamp(1, 500),
    )
    .await
    .map_err(view_failure)?;
    Ok(json!({"runs": runs}))
}

/// 产品面：run 事件视图（from_seq 起的事件流）。
async fn events_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?.to_owned();
    let (upper, run) = b
        .store
        .get_summary("run", &run_id)
        .await
        .map_err(storage_failure)?;
    if run.is_none() {
        return Err(ErrorObjectOwned::owned(-32011, "run not found", None::<()>));
    }
    // from_seq 与平台序号参数规范同口径（十进制字符串）：传 JSON 数字
    // 必须显式报错，静默当 0 会从头发送全部事件。
    let from = decimal_param(p, "from_seq")?;
    let identity = b.journal.id().to_owned();
    let root = b.journal.root().to_path_buf();
    let cursor = match p.get("cursor").filter(|v| !v.is_null()) {
        Some(v) => serde_json::from_value::<Cursor>(v.clone()).map_err(invalid)?,
        None => Cursor::first(identity.clone(), run_id.clone(), Filter::Events, upper),
    };
    let mut page = tokio::task::spawn_blocking(move || {
        flow_journal::page::page(
            &root,
            &identity,
            &run_id,
            Filter::Events,
            cursor,
            upper,
            256,
        )
    })
    .await
    .map_err(invalid)?
    .map_err(|e| failure(e.into()))?;
    page.events.retain(|e| e.event.run_seq >= from);
    serde_json::to_value(page).map_err(invalid)
}

/// schedule/webhook/template CRUD：cron 与模板片段校验后走统一产品命令
/// （回执序列化与 journal 错误分层在此完成）。
async fn product_command_route(
    ctx: &Context,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    if method.starts_with("schedule.") {
        if let Some(cron) = p.get("cron") {
            crate::validate_cron(
                cron.as_str()
                    .ok_or_else(|| invalid("cron must be a string"))?,
            )?;
        }
    }
    // Preserve the existing normalized wire fingerprint when a complete fragment
    // is supplied. Partial updates are merged and validated by the backend.
    let fragment = if method.starts_with("template.")
        && method != "template.delete"
        && p.get("nodes").is_some()
        && p.get("edges").is_some()
    {
        Some(
            flow_engine::template::normalize_fragment(&p["nodes"], &p["edges"])
                .map_err(|e| failure(e.into()))?,
        )
    } else {
        None
    };
    let mut args = p.clone();
    let map = args.as_object_mut().unwrap();
    map.remove("request_id");
    if let Some((nodes, edges)) = fragment {
        map.insert("nodes".into(), nodes);
        map.insert("edges".into(), edges);
    }
    // 边缘一次 struct 化；args 保留原始 wire 形状作为命令幂等指纹。
    let command =
        flow_backend::ProductCommand::parse(method, &args).map_err(|e| failure(e.into()))?;
    let receipt = ctx
        .backend
        .product_command(command, &args, text(p, "request_id")?)
        .await
        .map_err(failure)?;
    serde_json::to_value(receipt).map_err(invalid)
}

/// 产品面：schedule 清单。
async fn schedule_list(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let schedules = flow_backend::journal_views::list_schedules(b, p["workflow_id"].as_str())
        .await
        .map_err(view_failure)?;
    let list: Vec<Value> = schedules.iter().map(crate::schedule_value).collect();
    Ok(json!({"schedules": list}))
}

/// config.update：读改写统一配置面（merge-patch → 落盘 → 热更新）。
async fn config_update(ctx: &Context, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let product = ctx_product(ctx)?;
    let patch = p["patch"].clone();
    let cs = product
        .config
        .as_ref()
        .ok_or_else(|| invalid("此实例未启用配置文件读写，无法修改配置"))?;
    let mut current = cs.config.write().unwrap_or_else(|p| p.into_inner());
    let base = current.clone();
    let merged =
        flow_config::Config::merge_patch(&base, &patch).map_err(|e| invalid(e.to_string()))?;
    // 落点只认启动时发现的配置文件，其次默认落点（FLOW_CONFIG 声明值或
    // 平台配置目录）；绝不用进程 CWD——换目录启动会丢配置。
    let existing = cs.path.read().unwrap_or_else(|p| p.into_inner()).clone();
    let (write_path, first_write) = match existing {
        Some(path) => (path, false),
        None => (
            cs.default_path.clone().ok_or_else(|| {
                invalid("无法确定配置文件落点：请通过 --config 或 FLOW_CONFIG 指定路径")
            })?,
            true,
        ),
    };
    if let Some(parent) = write_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| invalid(format!("配置写入失败：{e}")))?;
        }
    }
    merged
        .save(&write_path)
        .map_err(|e| invalid(format!("配置写入失败：{e}")))?;
    if first_write {
        *cs.path.write().unwrap_or_else(|p| p.into_inner()) = Some(write_path);
    }
    *current = merged;
    drop(current);
    Ok(crate::config_view(&product))
}

/// secrets.set：名称/长度校验后写持久密钥存储。
async fn secrets_set(ctx: &Context, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let name = text(p, "name")?.to_owned();
    let value = text(p, "value")?.to_owned();
    crate::validate_secret_name(&name)?;
    if value.is_empty() {
        return Err(invalid("密钥值不能为空"));
    }
    if value.len() > 8192 {
        return Err(invalid("密钥值过长（上限 8 KiB）"));
    }
    let product = ctx_product(ctx)?;
    let store = product
        .secrets
        .as_ref()
        .ok_or_else(|| invalid("密钥存储未启用（需要 storage.data_dir 可写）"))?;
    store
        .set(&name, &value)
        .map_err(|e| ErrorObjectOwned::owned(-32603, format!("密钥写入失败：{e}"), None::<()>))?;
    Ok(json!({"name": name, "source": "stored"}))
}

/// secrets.delete：删除持久密钥。
async fn secrets_delete(ctx: &Context, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let name = text(p, "name")?.to_owned();
    crate::validate_secret_name(&name)?;
    let product = ctx_product(ctx)?;
    let Some(store) = &product.secrets else {
        return Err(invalid("密钥存储未启用（需要 storage.data_dir 可写）"));
    };
    let deleted = store
        .delete(&name)
        .map_err(|e| ErrorObjectOwned::owned(-32603, format!("密钥删除失败：{e}"), None::<()>))?;
    Ok(json!({"name": name, "deleted": deleted}))
}

/// 产品装配取用（config/secrets 方法用）。未装配时报「未启用」。
fn ctx_product(ctx: &Context) -> Result<Arc<crate::AppState>, ErrorObjectOwned> {
    ctx.product
        .clone()
        .ok_or_else(|| invalid("此实例未装配产品面（config/secrets 不可用）"))
}

/// run.subscribe：tail journal 按 run 推 v2 原形事件（PositionedEvent）。
/// 事件只发 journal v2 原形；/journal 工作区与主工作台 monitor 都直接
/// 消费这一格式。
async fn subscribe_run_events(
    params: jsonrpsee::types::Params<'_>,
    pending: jsonrpsee::server::PendingSubscriptionSink,
    ctx: &Context,
) {
    let p: Value = match params.parse() {
        Ok(p) => p,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let supplied = p["_token"].as_str().unwrap_or("");
    if !constant_time_eq(supplied, &ctx.token) {
        pending
            .reject(ErrorObjectOwned::owned(-32001, "unauthorized", None::<()>))
            .await;
        return;
    }
    if p["event_format"] != "v2" {
        pending.reject(invalid("event_format must be v2")).await;
        return;
    }
    let run_id = match text(&p, "run_id") {
        Ok(id) => id.to_owned(),
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let from_seq = match decimal_param(&p, "from_seq") {
        Ok(n) => n,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let audit = p["mode"] == "audit";
    if p.get("mode").is_some() && !matches!(p["mode"].as_str(), Some("audit" | "control")) {
        pending.reject(invalid("invalid subscription mode")).await;
        return;
    }
    let from_lsn = match decimal_param(&p, "from_lsn") {
        Ok(n) => n,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let sink = match pending.accept().await {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut tail = flow_journal::tail::TailReader::for_run(
        ctx.backend.journal.root(),
        ctx.backend.journal.id(),
        &run_id,
    );
    let mut scanned = 0;
    loop {
        if sink.is_closed() {
            return;
        }
        let (upper, row) = match ctx.backend.store.get_summary("run", &run_id).await {
            Ok(v) => v,
            Err(_) => return,
        };
        let Some(row) = row else { return };
        let terminal = matches!(
            row["status"].as_str(),
            Some("succeeded" | "failed" | "cancelled")
        );
        let id = run_id.clone();
        let batch = tokio::task::spawn_blocking(move || -> flow_journal::Result<_> {
            let mut records = Vec::new();
            let mut bytes = 0;
            let mut latest = scanned;
            while bytes < flow_journal::MAX_LINE_BYTES {
                let Some((tx, location)) = tail.next(upper)? else {
                    // A filtered reader may exhaust this snapshot without returning
                    // a transaction at upper. Progress is scan work, not event delivery.
                    latest = upper;
                    break;
                };
                latest = tx.lsn;
                bytes += location.bytes as usize;
                for (index, event) in tx.events.into_iter().enumerate() {
                    if event.run_id.as_deref() == Some(&id)
                        && (if audit {
                            tx.lsn > from_lsn && event.is_audit()
                        } else {
                            event.run_seq > from_seq
                        })
                    {
                        records.push(flow_journal::page::PositionedEvent {
                            lsn: tx.lsn,
                            event_index: index,
                            event,
                        });
                    }
                }
            }
            Ok((tail, records, latest))
        })
        .await;
        let (reader, records, latest) = match batch {
            Ok(Ok(v)) => v,
            _ => return,
        };
        tail = reader;
        scanned = latest;
        for record in records {
            let payload: Value = match serde_json::to_value(&record) {
                Ok(v) => v,
                Err(_) => return,
            };
            if flow_journal::codec::bounded_json(&payload, flow_journal::MAX_LINE_BYTES - 1024)
                .is_err()
            {
                if let Ok(message) = jsonrpsee::server::SubscriptionMessage::new(
                    "run.event",
                    sink.subscription_id(),
                    &json!({"type": "stream_error", "code": "RESPONSE_TOO_LARGE"}),
                ) {
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_secs(2), sink.send(message))
                            .await;
                }
                return;
            }
            let message = match jsonrpsee::server::SubscriptionMessage::new(
                "run.event",
                sink.subscription_id(),
                &payload,
            ) {
                Ok(m) => m,
                Err(_) => return,
            };
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_secs(2), sink.send(message)).await,
                Ok(Ok(()))
            ) {
                return;
            }
        }
        if scanned >= upper {
            if terminal && !audit {
                return;
            }
            tokio::select! {
                _ = sink.closed() => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }
}

pub async fn serve(
    backend: Arc<JournalBackend>,
    token: String,
    addr: SocketAddr,
) -> Result<(jsonrpsee::server::ServerHandle, SocketAddr), Box<dyn std::error::Error>> {
    if !addr.ip().is_loopback() {
        return Err("development JSONL RPC requires a loopback address".into());
    }
    let module = module(backend, token)?;
    serve_module(module, addr).await
}

/// 产品形态服务：非 loopback 绑定由调用方决定（token 认证仍在）。
pub async fn serve_product(
    module: RpcModule<Context>,
    addr: SocketAddr,
) -> Result<(jsonrpsee::server::ServerHandle, SocketAddr), Box<dyn std::error::Error>> {
    serve_module(module, addr).await
}

async fn serve_module(
    module: RpcModule<Context>,
    addr: SocketAddr,
) -> Result<(jsonrpsee::server::ServerHandle, SocketAddr), Box<dyn std::error::Error>> {
    let server = bind_product_server(addr).await?;
    let addr = server.local_addr()?;
    Ok((server.start(module), addr))
}

pub(crate) async fn bind_product_server(
    addr: SocketAddr,
) -> Result<jsonrpsee::server::Server, Box<dyn std::error::Error>> {
    let config = jsonrpsee::server::ServerConfig::builder()
        .max_request_body_size(9 * 1024 * 1024)
        .max_response_body_size(9 * 1024 * 1024)
        .max_connections(64)
        .build();
    let server = jsonrpsee::server::Server::builder()
        .set_config(config)
        .build(addr)
        .await?;
    Ok(server)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn error_codes_follow_types_instead_of_message_text() {
        assert_eq!(
            failure(flow_journal::Error::NotFound("任意文案".into()).into()).code(),
            -32011
        );
        assert_eq!(
            failure(flow_journal::Error::Invalid("workflow not found".into()).into()).code(),
            -32602
        );
        assert_eq!(
            view_failure(flow_backend::ViewError::NotFound("new wording".into())).code(),
            -32011
        );
        assert_eq!(
            view_failure(flow_backend::ViewError::Limit("budget".into())).code(),
            -32021
        );
    }

    #[test]
    fn storage_limit_mapping_does_not_depend_on_database_messages() {
        assert_eq!(
            storage_failure(flow_backend::StorageError::ResponseTooLarge).code(),
            -32021
        );
    }

    #[test]
    fn sequence_parameters_are_lossless_canonical_decimal_strings() {
        assert_eq!(decimal_param(&json!({}), "from_lsn").unwrap(), 0);
        assert_eq!(
            decimal_param(&json!({"from_lsn":"18446744073709551615"}), "from_lsn").unwrap(),
            u64::MAX
        );
        for value in [
            json!(1),
            json!(null),
            json!("+1"),
            json!("01"),
            json!("-1"),
            json!("18446744073709551616"),
        ] {
            assert!(decimal_param(&json!({"from_lsn":value}), "from_lsn").is_err());
        }
    }
}
