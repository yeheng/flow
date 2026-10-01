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
        "workflow.list",
        "run.list",
        "legacy.get",
        "legacy.list",
        "run.observations.page",
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
                "run.adjudicate"=>b.run_adjudicate(text(&p,"run_id")?,text(&p,"node_id")?,text(&p,"operation_id")?,text(&p,"reason")?,p["output"].clone(),request.ok_or_else(||invalid("request_id required"))?).await,
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
        if p["event_format"]!="v2" {pending.reject(invalid("event_format=v2 required")).await;return;}
        let run_id=match text(&p,"run_id"){Ok(id)=>id.to_owned(),Err(e)=>{pending.reject(e).await;return;}};
        let from_seq=match decimal_param(&p,"from_seq"){Ok(n)=>n,Err(e)=>{pending.reject(e).await;return;}};
        let audit=p["mode"]=="audit";
        if p.get("mode").is_some() && !matches!(p["mode"].as_str(),Some("audit"|"control")){pending.reject(invalid("invalid subscription mode")).await;return;}
        let from_lsn=match decimal_param(&p,"from_lsn"){Ok(n)=>n,Err(e)=>{pending.reject(e).await;return;}};
        let sink=match pending.accept().await {Ok(s)=>s,Err(_)=>return};
        let mut tail=flow_journal::tail::TailReader::new(ctx.backend.journal.root(),ctx.backend.journal.id());
        let mut scanned=0;
        loop {
            let (upper,row)=match ctx.backend.projection.get("run",&run_id).await {Ok(v)=>v,Err(_)=>return};
            let Some(row)=row else{return};
            let terminal=matches!(row["status"].as_str(),Some("succeeded"|"failed"|"cancelled"));
            let id=run_id.clone();
            let batch=tokio::task::spawn_blocking(move||->flow_journal::Result<_>{
                let mut records=Vec::new();let mut bytes=0;let mut latest=scanned;
                while bytes<flow_journal::MAX_LINE_BYTES {
                    let Some((tx,location))=tail.next(upper)? else{break};latest=tx.lsn;bytes+=location.bytes as usize;
                    for (index,event) in tx.events.into_iter().enumerate(){
                        if event.run_id.as_deref()==Some(&id) && (if audit {tx.lsn>from_lsn && (event.audit_seq>0 || event.kind==flow_journal::EventKind::LateAudit)}else{event.run_seq>from_seq}) {
                            records.push(flow_journal::page::PositionedEvent{lsn:tx.lsn,event_index:index,event});
                        }
                    }
                }
                Ok((tail,records,latest))
            }).await;
            let (reader,records,latest)=match batch {Ok(Ok(v))=>v,_=>return};tail=reader;scanned=latest;
            for record in records {
                if flow_journal::codec::bounded_json(&record,flow_journal::MAX_LINE_BYTES-1024).is_err(){
                    if let Ok(message)=jsonrpsee::server::SubscriptionMessage::new("run.event",sink.subscription_id(),&json!({"type":"stream_error","code":"RESPONSE_TOO_LARGE"})){let _=tokio::time::timeout(std::time::Duration::from_secs(2),sink.send(message)).await;}
                    return;
                }
                let message=match jsonrpsee::server::SubscriptionMessage::new("run.event",sink.subscription_id(),&record){Ok(m)=>m,Err(_)=>return};
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
