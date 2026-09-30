//! Explicit single-user JSONL RPC endpoint. Every call authenticates, including pages.
//! It is separate from the legacy/PG server so those authorities cannot be mixed.
use flow_backend::journal::{JournalBackend, JournalError};
use flow_journal::page::{Cursor, Filter};
use jsonrpsee::{types::ErrorObjectOwned, RpcModule};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc};

pub const COMMITTED_NOT_VISIBLE: i32 = -32020;
pub struct Context {
    backend: Arc<JournalBackend>,
    token: String,
}
fn invalid(message: impl ToString) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32602, message.to_string(), None::<()>)
}
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
    if token.len() < 32 {
        return Err("FLOW_JOURNAL_TOKEN must contain at least 32 bytes".into());
    }
    let mut module = RpcModule::new(Context { backend, token });
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
    ] {
        module.register_async_method(method,move |params,ctx,_|async move {
            let mut p:Value=params.parse()?;
            let supplied=p.get("_token").and_then(Value::as_str).unwrap_or("");
            // Full-token comparison without early exit on the first differing byte.
            let mismatch=supplied.len()!=ctx.token.len() || supplied.bytes().zip(ctx.token.bytes()).fold(0u8,|d,(a,b)|d|(a^b))!=0;
            if mismatch {return Err(ErrorObjectOwned::owned(-32001,"unauthorized",None::<()>));}
            p.as_object_mut().ok_or_else(||invalid("object params required"))?.remove("_token");
            let request=p.get("request_id").map(|v|v.as_str().ok_or_else(||invalid("request_id must be a string"))).transpose()?;
            let b=&ctx.backend;
            let receipt=match method {
                "workflow.create"=>b.workflow_create(text(&p,"name")?,request).await,
                "workflow.update"=>b.workflow_update(text(&p,"workflow_id")?,p["definition"].clone(),request).await,
                "workflow.publish"=>b.workflow_publish(text(&p,"workflow_id")?,optional_version(&p)?.ok_or_else(||invalid("version required"))?,request).await,
                "workflow.delete"=>b.workflow_delete(text(&p,"workflow_id")?,request).await,
                "run.start"=>b.run_start(text(&p,"workflow_id")?,optional_version(&p)?,p["input"].clone(),"manual",None,request).await,
                "run.cancel"=>b.run_cancel(text(&p,"run_id")?,request).await,
                "run.signal"=>b.run_signal(text(&p,"run_id")?,text(&p,"node_id")?,p["payload"].clone(),request).await,
                "run.adjudicate"=>b.run_adjudicate(text(&p,"run_id")?,text(&p,"node_id")?,text(&p,"operation_id")?,text(&p,"reason")?,p["output"].clone(),request.ok_or_else(||invalid("request_id required"))?).await,
                "schedule.change"|"webhook.change"=>b.config_change(method,if method=="schedule.change"{flow_journal::EventKind::ScheduleChanged}else{flow_journal::EventKind::WebhookChanged},text(&p,"key")?,p["patch"].clone(),request).await,
                "command.status"=>return serde_json::to_value(b.command_status(text(&p,"scope")?,text(&p,"request_id")?).await.map_err(failure)?).map_err(invalid),
                "workflow.get"|"run.get"=>{
                    let kind=if method=="run.get"{"run"}else{"workflow"};
                    let key=text(&p,if kind=="run"{"run_id"}else{"workflow_id"})?;
                    let (lsn,value)=b.projection.get(kind,key).await.map_err(|_|invalid("projection unavailable"))?;
                    return Ok(json!({"snapshot_cursor":{"journal_id":b.journal.id(),"lsn":lsn.to_string()},"value":value}));
                },
                "run.events.page"|"run.audit.page"=>{
                    let run_id=text(&p,"run_id")?.to_owned();
                    let (upper,run)=b.projection.get("run",&run_id).await.map_err(|_|invalid("projection unavailable"))?;
                    if run.is_none(){return Err(ErrorObjectOwned::owned(-32011,"run not found",None::<()>));}
                    let filter=if method=="run.events.page"{Filter::Events}else{Filter::Audit};
                    let identity=b.journal.id().to_owned();
                    let cursor=match p.get("cursor").filter(|v|!v.is_null()) {Some(v)=>serde_json::from_value::<Cursor>(v.clone()).map_err(invalid)?,None=>Cursor::first(identity.clone(),run_id.clone(),filter,upper)};
                    let limit=p.get("limit").map(|v|v.as_u64().ok_or_else(||invalid("invalid limit"))).transpose()?.unwrap_or(256).min(256) as usize;
                    let root=b.journal.root().to_path_buf();
                    let page=tokio::task::spawn_blocking(move||flow_journal::page::page(&root,&identity,&run_id,filter,cursor,upper,limit)).await.map_err(invalid)?.map_err(|e|failure(e.into()))?;
                    return serde_json::to_value(page).map_err(invalid);
                },
                _=>unreachable!(),
            }.map_err(failure)?;
            serde_json::to_value(receipt).map_err(invalid)
        })?;
    }
    Ok(module)
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
