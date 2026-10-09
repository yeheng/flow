//! Explicit single-user JSONL RPC endpoint — the one product surface. Every call
//! authenticates, including pages. Product form (`module_product`) additionally
//! carries the config/secrets assembly and the v1-shaped product reads
//! (`workflow.*.full` / `run.*.full` / triggers / templates), so the browser
//! frontend, the embedded desktop service and flow-cli all speak this module.
use flow_backend::journal::{JournalBackend, JournalError};
use flow_journal::page::{Cursor, Filter};
use jsonrpsee::{types::ErrorObjectOwned, RpcModule};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc};

pub const COMMITTED_NOT_VISIBLE: i32 = -32020;
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
        JournalError::CommittedNotVisible(receipt) => ErrorObjectOwned::owned(
            COMMITTED_NOT_VISIBLE,
            "COMMITTED_NOT_VISIBLE",
            Some(receipt),
        ),
        JournalError::Journal(flow_journal::Error::Conflict(message)) => {
            ErrorObjectOwned::owned(-32012, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Limit(message)) => {
            ErrorObjectOwned::owned(-32021, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Invalid(message)) => invalid(message),
        _ => ErrorObjectOwned::owned(-32603, "journal service unavailable", None::<()>),
    }
}
/// v1 公共面错误 → v2 错误码（与 failure() 的 journal 错误同语义分层）。
fn arm_failure(error: flow_backend::BackendError) -> ErrorObjectOwned {
    match error {
        flow_backend::BackendError::WorkflowNotFound(m) => {
            ErrorObjectOwned::owned(-32011, format!("工作流不存在：{m}"), None::<()>)
        }
        flow_backend::BackendError::VersionNotFound(w, v) => ErrorObjectOwned::owned(
            -32011,
            format!("版本不存在：{w} v{v}"),
            None::<()>,
        ),
        flow_backend::BackendError::RunNotFound(m) => {
            ErrorObjectOwned::owned(-32011, format!("run 不存在：{m}"), None::<()>)
        }
        flow_backend::BackendError::ScheduleNotFound(m) => {
            ErrorObjectOwned::owned(-32011, format!("schedule 不存在：{m}"), None::<()>)
        }
        flow_backend::BackendError::WebhookNotFound(m) => {
            ErrorObjectOwned::owned(-32011, format!("webhook 不存在：{m}"), None::<()>)
        }
        flow_backend::BackendError::TemplateNotFound(m) => {
            ErrorObjectOwned::owned(-32011, format!("节点模板不存在：{m}"), None::<()>)
        }
        flow_backend::BackendError::TemplateNameTaken(m) => {
            ErrorObjectOwned::owned(-32012, format!("模板名已存在：{m}"), None::<()>)
        }
        flow_backend::BackendError::Invalid(m) => invalid(m),
        flow_backend::BackendError::Conflict(m) => {
            ErrorObjectOwned::owned(-32012, m, None::<()>)
        }
        other => ErrorObjectOwned::owned(-32603, other.to_string(), None::<()>),
    }
}
fn projection_failure(error: impl ToString) -> ErrorObjectOwned {
    if error.to_string().contains("RESPONSE_TOO_LARGE") {
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

pub fn module(
    backend: Arc<JournalBackend>,
    token: String,
) -> Result<RpcModule<Context>, Box<dyn std::error::Error>> {
    module_product(backend, token, None)
}

/// 产品形态：装配统一配置面与持久化密钥（journal-server / 桌面内嵌用）。
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
    for method in [
        "workflow.create",
        "workflow.update",
        "workflow.publish",
        "workflow.delete",
        "run.start",
        "run.cancel",
        "run.signal",
        "run.adjudicate",
        "command.status",
        "workflow.get",
        "run.get",
        "run.events.page",
        "run.audit.page",
        "schedule.change",
        "webhook.change",
        "workflow.list",
        "run.list",
        "legacy.get",
        "legacy.list",
        "run.observations.page",
        // ---- 产品面（v2 协议 + v1 物化数据形状；前端主工作台用） ----
        "workflow.get.full",
        "workflow.list.full",
        "workflow.versions",
        "run.get.full",
        "run.list.full",
        "run.stats",
        "run.timeline",
        "run.events.full",
        "schedule.create",
        "schedule.update",
        "schedule.delete",
        "schedule.list",
        "webhook.create",
        "webhook.set_enabled",
        "webhook.delete",
        "webhook.list",
        "template.create",
        "template.update",
        "template.delete",
        "template.list",
        "template.get",
        "config.get",
        "config.update",
        "secrets.list",
        "secrets.set",
        "secrets.delete",
        "nodetypes.list",
    ] {
        module.register_async_method(method,move |params,ctx,_|async move {
            // JSON-RPC 允许整体省略 params（到达的是 null）——归一为空对象，
            // 与 v1 契约的「省略 = {}」同一语义。
            let mut p:Value=match params.parse::<Value>(){Ok(Value::Null)=>json!({}),Ok(v)=>v,Err(e)=>return Err(e)};
            let supplied=p.get("_token").and_then(Value::as_str).unwrap_or("");
            // Full-token comparison without early exit on the first differing byte.
            let mismatch=supplied.len()!=ctx.token.len() || supplied.bytes().zip(ctx.token.bytes()).fold(0u8,|d,(a,b)|d|(a^b))!=0;
            if mismatch {return Err(ErrorObjectOwned::owned(-32001,"unauthorized",None::<()>));}
            p.as_object_mut().ok_or_else(||invalid("object params required"))?.remove("_token");
            let request=p.get("request_id").map(|v|v.as_str().ok_or_else(||invalid("request_id must be a string"))).transpose()?;
            // 写命令必须带稳定 request_id：无幂等键的客户端重试=双 run/双
            // workflow（服务端无 UUID 透传键可去重）。读面免检。
            if !matches!(method,"run.observations.page"|"command.status"|"workflow.list"|"run.list"|"legacy.list"|"workflow.get"|"run.get"|"legacy.get"|"run.events.page"|"run.audit.page"|"workflow.get.full"|"workflow.list.full"|"workflow.versions"|"run.get.full"|"run.list.full"|"run.stats"|"run.timeline"|"run.events.full"|"schedule.list"|"webhook.list"|"template.list"|"template.get"|"config.get"|"secrets.list"|"nodetypes.list")
                && request.is_none_or(|id| id.trim().is_empty()) {
                return Err(invalid("request_id required for write commands"));
            }
            let b=&ctx.backend;
            let receipt=match method {
                "run.observations.page"=>{
                    let run_id=text(&p,"run_id")?.to_owned();
                    if b.projection.get("run",&run_id).await.map_err(invalid)?.1.is_none(){return Err(ErrorObjectOwned::owned(-32011,"run not found",None::<()>));}
                    let Some(store)=b.observations.clone() else{return Ok(json!({"records":[],"loss":{"history_incomplete":true},"loss_scope":"store"}));};
                    let from=decimal_param(&p,"from_seq")?;
                    let dispatch=p["dispatch_id"].as_str().map(str::to_owned);
                    let limit=p["limit"].as_u64().unwrap_or(100).min(256) as usize;
                    return tokio::task::spawn_blocking(move||{let records=store.page(&run_id,dispatch.as_deref(),from,limit).map_err(invalid)?;Ok(json!({"records":records,"loss":store.scoped_loss(&run_id,dispatch.as_deref()),"loss_scope":if dispatch.is_some(){"dispatch"}else{"run"},"store_loss":store.loss()}))}).await.map_err(invalid)?;
                },
                "workflow.create"=>b.workflow_create(text(&p,"name")?,request).await,
                "workflow.update"=>b.workflow_update(text(&p,"workflow_id")?,p["definition"].clone(),request).await,
                "workflow.publish"=>b.workflow_publish(text(&p,"workflow_id")?,optional_version(&p)?.ok_or_else(||invalid("version required"))?,request).await,
                "workflow.delete"=>b.workflow_delete(text(&p,"workflow_id")?,request).await,
                "run.start"=>b.run_start(text(&p,"workflow_id")?,optional_version(&p)?,p["input"].clone(),"manual",None,request).await,
                "run.cancel"=>b.run_cancel(text(&p,"run_id")?,request).await,
                "run.signal"=>b.run_signal(text(&p,"run_id")?,text(&p,"node_id")?,p["payload"].clone(),request).await,
                "run.adjudicate"=>b.run_adjudicate(text(&p,"run_id")?,text(&p,"node_id")?,text(&p,"operation_id")?,text(&p,"reason")?,p["output"].clone(),p["decision"].as_str().unwrap_or("accept_output"),request.ok_or_else(||invalid("request_id required"))?).await,
                "schedule.change"|"webhook.change"=>b.config_change(method,if method=="schedule.change"{flow_journal::EventKind::ScheduleChanged}else{flow_journal::EventKind::WebhookChanged},text(&p,"key")?,p["patch"].clone(),request).await,
                "command.status"=>return serde_json::to_value(b.command_status(text(&p,"scope")?,text(&p,"request_id")?).await.map_err(failure)?).map_err(invalid),
                "workflow.list"|"run.list"|"legacy.list"=>{
                    let kind=method.split('.').next().unwrap();let after=p["after"].as_str().unwrap_or("");
                    let (lsn,values)=b.projection.list(kind,after,p["limit"].as_u64().unwrap_or(64).min(256) as usize).await.map_err(projection_failure)?;
                    return Ok(json!({"snapshot_cursor":{"journal_id":b.journal.id(),"lsn":lsn.to_string()},"values":values}));
                },
                "workflow.get"|"run.get"|"legacy.get"=>{
                    let kind=method.split('.').next().unwrap();
                    let key=text(&p,match kind {"run"=>"run_id","legacy"=>"key",_=>"workflow_id"})?;
                    let (lsn,value)=b.projection.get(kind,key).await.map_err(projection_failure)?;
                    return Ok(json!({"snapshot_cursor":{"journal_id":b.journal.id(),"lsn":lsn.to_string()},"value":value}));
                },
                "run.events.page"|"run.audit.page"=>{
                    let run_id=text(&p,"run_id")?.to_owned();
                    let (upper,run)=b.projection.get("run",&run_id).await.map_err(projection_failure)?;
                    if run.is_none(){return Err(ErrorObjectOwned::owned(-32011,"run not found",None::<()>));}
                    let filter=if method=="run.events.page"{Filter::Events}else{Filter::Audit};
                    let identity=b.journal.id().to_owned();
                    let cursor=match p.get("cursor").filter(|v|!v.is_null()) {Some(v)=>serde_json::from_value::<Cursor>(v.clone()).map_err(invalid)?,None=>Cursor::first(identity.clone(),run_id.clone(),filter,upper)};
                    let limit=p.get("limit").map(|v|v.as_u64().ok_or_else(||invalid("invalid limit"))).transpose()?.unwrap_or(256).min(256) as usize;
                    let root=b.journal.root().to_path_buf();
                    let page=tokio::task::spawn_blocking(move||flow_journal::page::page(&root,&identity,&run_id,filter,cursor,upper,limit)).await.map_err(invalid)?.map_err(|e|failure(e.into()))?;
                    return serde_json::to_value(page).map_err(invalid);
                },
                // ---- 产品面：v1 数据形状（journal_arm 物化）----
                "workflow.list.full"=>{
                    let workflows=flow_backend::journal_arm::list_workflows(b).await.map_err(arm_failure)?;
                    return Ok(json!({"workflows":workflows}));
                },
                "workflow.get.full"=>{
                    let version=flow_backend::journal_arm::get_version(
                        b,text(&p,"workflow_id")?,p["version"].as_i64(),
                    ).await.map_err(arm_failure)?;
                    // v1 workflow.get 形状：version 之外还带 published_version
                    // （编辑器用它渲染「已发布 vN」徽标），单独补上。
                    let published=flow_backend::journal_arm::latest_published(b,text(&p,"workflow_id")?).await.map_err(arm_failure)?;
                    let mut value=serde_json::to_value(version).map_err(invalid)?;
                    value["published_version"]=json!(published);
                    return Ok(value);
                },
                "workflow.versions"=>{
                    let versions=flow_backend::journal_arm::list_versions(b,text(&p,"workflow_id")?).await.map_err(arm_failure)?;
                    return Ok(json!({"versions":versions}));
                },
                "run.get.full"=>{
                    let run_id=text(&p,"run_id")?;
                    let run=flow_backend::journal_arm::get_run(b,run_id).await.map_err(arm_failure)?;
                    let live=flow_backend::journal_arm::is_live(b,run_id).await;
                    return Ok(json!({"run":run,"live":live}));
                },
                "run.list.full"=>{
                    let runs=flow_backend::journal_arm::list_runs(
                        b,
                        p["workflow_id"].as_str(),
                        p["status"].as_str(),
                        p["source"].as_str(),
                        p["before_run_id"].as_str(),
                        p["limit"].as_i64().unwrap_or(50).clamp(1,500),
                    ).await.map_err(arm_failure)?;
                    return Ok(json!({"runs":runs}));
                },
                "run.stats"=>{
                    let stats=flow_backend::journal_arm::run_stats(b,p["workflow_id"].as_str()).await.map_err(arm_failure)?;
                    return serde_json::to_value(stats).map_err(invalid);
                },
                "run.timeline"=>{
                    let run_id=text(&p,"run_id")?;
                    let run=flow_backend::journal_arm::get_run(b,run_id).await.map_err(arm_failure)?;
                    let stored=flow_backend::journal_arm::get_version(b,&run.workflow_id,Some(run.workflow_version)).await.map_err(arm_failure)?;
                    let definition:flow_engine::Definition=serde_json::from_value(stored.definition).map_err(|e|invalid(format!("定义结构非法：{e}")))?;
                    let snapshot=flow_backend::journal_arm::snapshot(b,run_id).await.map_err(arm_failure)?;
                    return Ok(crate::timeline_value(run_id,&run.status,&run.workflow_id,run.workflow_version,&definition,&snapshot));
                },
                "run.events.full"=>{
                    let events=flow_backend::journal_arm::read_events(b,text(&p,"run_id")?,p["from_seq"].as_u64()).await.map_err(arm_failure)?;
                    return Ok(json!({"events":events}));
                },
                "schedule.create"=>{
                    crate::validate_cron(text(&p,"cron")?)?;
                    let input=p.get("input").filter(|v|!v.is_null());
                    let schedule=flow_backend::journal_arm::create_schedule(
                        b,text(&p,"workflow_id")?,text(&p,"cron")?,
                        input,p["enabled"].as_bool().unwrap_or(true),
                    ).await.map_err(arm_failure)?;
                    return Ok(crate::schedule_value(&schedule));
                },
                "schedule.update"=>{
                    let input=match p.get("input"){None=>None,Some(v)=>Some(Some(v.clone()))};
                    flow_backend::journal_arm::update_schedule(
                        b,text(&p,"id")?,p["cron"].as_str(),input,p["enabled"].as_bool(),
                    ).await.map_err(arm_failure)?;
                    // 回读完整实体（按 v1 形状返回）
                    let list=flow_backend::journal_arm::list_schedules(b,Some(text(&p,"id")?)).await.map_err(arm_failure)?;
                    return Ok(json!({"updated":true,"schedule":list.first().map(crate::schedule_value)}));
                },
                "schedule.delete"=>{
                    flow_backend::journal_arm::delete_schedule(b,text(&p,"id")?).await.map_err(arm_failure)?;
                    return Ok(json!({"deleted":true}));
                },
                "schedule.list"=>{
                    let schedules=flow_backend::journal_arm::list_schedules(b,p["workflow_id"].as_str()).await.map_err(arm_failure)?;
                    let list:Vec<Value>=schedules.iter().map(crate::schedule_value).collect();
                    return Ok(json!({"schedules":list}));
                },
                "webhook.create"=>{
                    let webhook=flow_backend::journal_arm::create_webhook(b,text(&p,"workflow_id")?).await.map_err(arm_failure)?;
                    return serde_json::to_value(webhook).map_err(invalid);
                },
                "webhook.set_enabled"=>{
                    flow_backend::journal_arm::set_webhook_enabled(b,text(&p,"token")?,p["enabled"].as_bool().unwrap_or(false)).await.map_err(arm_failure)?;
                    return Ok(json!({"updated":true}));
                },
                "webhook.delete"=>{
                    flow_backend::journal_arm::delete_webhook(b,text(&p,"token")?).await.map_err(arm_failure)?;
                    return Ok(json!({"deleted":true}));
                },
                "webhook.list"=>{
                    let webhooks=flow_backend::journal_arm::list_webhooks(b,p["workflow_id"].as_str()).await.map_err(arm_failure)?;
                    return serde_json::to_value(json!({"webhooks":webhooks})).map_err(invalid);
                },
                "template.create"=>{
                    crate::validate_template_name(text(&p,"name")?)?;
                    let (nodes,edges)=crate::validate_fragment(&p["nodes"],&p["edges"])?;
                    let template=flow_backend::journal_arm::template_create(b,text(&p,"name")?,p["category"].as_str(),&nodes,&edges).await.map_err(arm_failure)?;
                    return serde_json::to_value(template).map_err(invalid);
                },
                "template.update"=>{
                    crate::validate_template_name(text(&p,"name").unwrap_or(""))?;
                    let (nodes,edges)=if p["nodes"].is_null()&&(p["edges"].is_null()){
                        (Value::Null,Value::Null)
                    }else{
                        crate::validate_fragment(&p["nodes"],&p["edges"])?
                    };
                    let category=match p.get("category"){None=>None,Some(v)=>Some(v.as_str())};
                    let template=flow_backend::journal_arm::template_update(
                        b,text(&p,"id")?,p["name"].as_str(),category,
                        if nodes.is_null(){None}else{Some(&nodes)},
                        if edges.is_null(){None}else{Some(&edges)},
                    ).await.map_err(arm_failure)?;
                    return serde_json::to_value(template).map_err(invalid);
                },
                "template.delete"=>{
                    let deleted=flow_backend::journal_arm::template_delete(b,text(&p,"id")?).await.map_err(arm_failure)?;
                    return Ok(json!({"deleted":deleted}));
                },
                "template.list"=>{
                    let templates=flow_backend::journal_arm::template_list(b).await.map_err(arm_failure)?;
                    return Ok(json!({"templates":templates}));
                },
                "template.get"=>{
                    let template=flow_backend::journal_arm::template_get(b,text(&p,"id")?).await.map_err(arm_failure)?;
                    return serde_json::to_value(template).map_err(invalid);
                },
                "config.get"=>{
                    let product=ctx_product(&ctx)?;
                    return Ok(crate::config_view(&product));
                },
                "config.update"=>{
                    let product=ctx_product(&ctx)?;
                    let patch=p["patch"].clone();
                    let cs=product.config.as_ref().ok_or_else(||invalid("此实例未启用配置文件读写，无法修改配置"))?;
                    let base=cs.config.read().unwrap().clone();
                    let mut patch=patch;
                    if patch.get("storage").and_then(|s|s.get("database_url"))==Some(&json!("<set>")){
                        if let Some(storage)=patch.get_mut("storage").and_then(|s|s.as_object_mut()){
                            storage.insert("database_url".into(),json!(base.storage.database_url));
                        }
                    }
                    let merged=flow_config::Config::merge_patch(&base,&patch).map_err(|e|invalid(e.to_string()))?;
                    let write_path=cs.path.clone().unwrap_or_else(||std::path::PathBuf::from("flow.toml"));
                    merged.save(&write_path).map_err(|e|invalid(format!("配置写入失败：{e}")))?;
                    *cs.config.write().unwrap()=merged;
                    return Ok(crate::config_view(&product));
                },
                "secrets.list"=>{
                    let secrets:Vec<Value>=flow_backend::secrets::list_secrets()
                        .into_iter()
                        .map(|(name,source)|json!({"name":name,"source":source}))
                        .collect();
                    return Ok(json!({"secrets":secrets}));
                },
                "secrets.set"=>{
                    let name=text(&p,"name")?.to_owned();
                    let value=text(&p,"value")?.to_owned();
                    crate::validate_secret_name(&name)?;
                    if value.is_empty(){return Err(invalid("密钥值不能为空"));}
                    if value.len()>8192{return Err(invalid("密钥值过长（上限 8 KiB）"));}
                    let product=ctx_product(&ctx)?;
                    let store=product.secrets.as_ref().ok_or_else(||invalid("密钥存储未启用（需要 storage.data_dir 可写）"))?;
                    store.set(&name,&value).map_err(|e|ErrorObjectOwned::owned(-32603,format!("密钥写入失败：{e}"),None::<()>))?;
                    return Ok(json!({"name":name,"source":"stored"}));
                },
                "secrets.delete"=>{
                    let name=text(&p,"name")?.to_owned();
                    crate::validate_secret_name(&name)?;
                    let product=ctx_product(&ctx)?;
                    let Some(store)=&product.secrets else{return Err(invalid("密钥存储未启用（需要 storage.data_dir 可写）"));};
                    let deleted=store.delete(&name).map_err(|e|ErrorObjectOwned::owned(-32603,format!("密钥删除失败：{e}"),None::<()>))?;
                    return Ok(json!({"name":name,"deleted":deleted}));
                },
                "nodetypes.list"=>{
                    return Ok(json!({"node_types":crate::node_types()}));
                },
                _=>unreachable!(),
            }.map_err(failure)?;
            serde_json::to_value(receipt).map_err(invalid)
        })?;
    }
    module.register_subscription("run.subscribe","run.event","run.unsubscribe",|params,pending,ctx,_|async move {
        let p:Value=match params.parse(){Ok(p)=>p,Err(e)=>{pending.reject(e).await;return;}};
        let supplied=p["_token"].as_str().unwrap_or("");
        if supplied.len()!=ctx.token.len() || supplied.bytes().zip(ctx.token.bytes()).fold(0u8,|d,(a,b)|d|(a^b))!=0 {
            pending.reject(ErrorObjectOwned::owned(-32001,"unauthorized",None::<()>)).await;return;
        }
        // event_format：v2 = journal 事件原形（/journal 工作区）；
        // envelope = v1 Envelope 形状（物化 StoredValue；主工作台 monitor 用）
        let envelope_format=p["event_format"]=="envelope";
        if !envelope_format && p["event_format"]!="v2" {pending.reject(invalid("event_format must be v2 or envelope")).await;return;}
        let run_id=match text(&p,"run_id"){Ok(id)=>id.to_owned(),Err(e)=>{pending.reject(e).await;return;}};
        let from_seq=match decimal_param(&p,"from_seq"){Ok(n)=>n,Err(e)=>{pending.reject(e).await;return;}};
        let audit=p["mode"]=="audit";
        if p.get("mode").is_some() && !matches!(p["mode"].as_str(),Some("audit"|"control")){pending.reject(invalid("invalid subscription mode")).await;return;}
        let from_lsn=match decimal_param(&p,"from_lsn"){Ok(n)=>n,Err(e)=>{pending.reject(e).await;return;}};
        let sink=match pending.accept().await {Ok(s)=>s,Err(_)=>return};
        let mut tail=flow_journal::tail::TailReader::new(ctx.backend.journal.root(),ctx.backend.journal.id());
        let mut scanned=0;
        let root=ctx.backend.journal.root().to_path_buf();
        // envelope 模式的 attempt 追踪（DispatchStarted 增量维护，跨批保持）
        let mut attempts=std::collections::HashMap::<String,u32>::new();
        loop {
            let (upper,row)=match ctx.backend.projection.get("run",&run_id).await {Ok(v)=>v,Err(_)=>return};
            let Some(row)=row else{return};
            let terminal=matches!(row["status"].as_str(),Some("succeeded"|"failed"|"cancelled"));
            let id=run_id.clone();
            let want_envelope=envelope_format;
            let scan_root=root.clone();
            let mut attempt_state=std::mem::take(&mut attempts);
            let batch=tokio::task::spawn_blocking(move||->flow_journal::Result<_>{
                let mut records=Vec::new();let mut bytes=0;let mut latest=scanned;
                while bytes<flow_journal::MAX_LINE_BYTES {
                    let Some((tx,location))=tail.next(upper)? else{break};latest=tx.lsn;bytes+=location.bytes as usize;
                    for (index,event) in tx.events.into_iter().enumerate(){
                        if event.run_id.as_deref()==Some(&id) && (if audit {tx.lsn>from_lsn && (event.audit_seq>0 || event.kind==flow_journal::EventKind::LateAudit)}else{event.run_seq>from_seq}) {
                            if want_envelope {
                                // 物化为 v1 Envelope（StoredValue 解析；Ref 值读文件）
                                if let Some(envelope)=flow_backend::journal_arm::envelope(&event,&scan_root,upper,&mut attempt_state){
                                    // 展示出口脱敏：与 timeline 同一规则（node_completed
                                    // 的 output）。不脱的话前端 applyEvent 会用原始值
                                    // 覆盖 timeline 的脱敏值——脱敏被订阅绕过。
                                    records.push((flow_journal::page::PositionedEvent{lsn:tx.lsn,event_index:index,event},Some(crate::redact_display_envelope(envelope))));
                                }
                            } else {
                                records.push((flow_journal::page::PositionedEvent{lsn:tx.lsn,event_index:index,event},None));
                            }
                        }
                    }
                }
                Ok((tail,records,latest,attempt_state))
            }).await;
            let (reader,records,latest,state)=match batch {Ok(Ok(v))=>v,_=>return};tail=reader;scanned=latest;attempts=state;
            for (record,envelope) in records {
                let payload:Value=match envelope{
                    Some(e)=>match serde_json::to_value(e){Ok(v)=>v,Err(_)=>return},
                    None=>match serde_json::to_value(&record){Ok(v)=>v,Err(_)=>return},
                };
                if flow_journal::codec::bounded_json(&payload,flow_journal::MAX_LINE_BYTES-1024).is_err(){
                    if let Ok(message)=jsonrpsee::server::SubscriptionMessage::new("run.event",sink.subscription_id(),&json!({"type":"stream_error","code":"RESPONSE_TOO_LARGE"})){let _=tokio::time::timeout(std::time::Duration::from_secs(2),sink.send(message)).await;}
                    return;
                }
                let message=match jsonrpsee::server::SubscriptionMessage::new("run.event",sink.subscription_id(),&payload){Ok(m)=>m,Err(_)=>return};
                if !matches!(tokio::time::timeout(std::time::Duration::from_secs(2),sink.send(message)).await,Ok(Ok(()))) {return;}
            }
            if scanned>=upper {
                if terminal && !audit {return;}
                tokio::select!{_=sink.closed()=>return,_=tokio::time::sleep(std::time::Duration::from_millis(50))=>{}}
            }
        }
    })?;
    Ok(module)
}

/// 产品装配取用（config/secrets 方法用）。未装配时报「未启用」。
fn ctx_product(ctx: &Context) -> Result<Arc<crate::AppState>, ErrorObjectOwned> {
    ctx.product
        .clone()
        .ok_or_else(|| invalid("此实例未装配产品面（config/secrets 不可用）"))
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
    let config = jsonrpsee::server::ServerConfig::builder()
        .max_request_body_size(9 * 1024 * 1024)
        .max_response_body_size(9 * 1024 * 1024)
        .max_connections(64)
        .build();
    let server = jsonrpsee::server::Server::builder()
        .set_config(config)
        .build(addr)
        .await?;
    let addr = server.local_addr()?;
    Ok((server.start(module), addr))
}

#[cfg(test)]
mod tests {
    use super::*;
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
