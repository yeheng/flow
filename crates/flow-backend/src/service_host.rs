//! Host callbacks for resident services; identities and grants come from startup, not messages.
use crate::{journal::JournalBackend, resident_service::HostHandler};
use serde_json::json;
use std::sync::Arc;
pub fn handler(
    backend: &Arc<JournalBackend>,
    source_plugin: String,
    allowed: Vec<String>,
    binding: Option<(String, String, String)>,
) -> HostHandler {
    let backend = Arc::downgrade(backend);
    Arc::new(move |method, params| {
        let backend = backend.clone();
        let allowed = allowed.clone();
        let binding = binding.clone();
        let source_plugin = source_plugin.clone();
        Box::pin(async move {
            let backend = backend.upgrade().ok_or("host unavailable")?;
            if method.starts_with("flow/node.") {
                let (run_id, node_id, dispatch) = binding.ok_or("node binding required")?;
                let run = backend
                    .run_snapshot(&run_id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or("bound run missing")?;
                let node = run.nodes.get(&node_id).ok_or("bound node missing")?;
                if node.dispatch_id != dispatch {
                    return Err("stale node binding".into());
                }
                return match method.as_str() {
                    "flow/node.get" => Ok(
                        json!({"run_id":run_id,"node_id":node_id,"sequence":run.last_run_seq.to_string(),"node":node}),
                    ),
                    "flow/node.events" => {
                        use flow_journal::page::{Cursor, Filter};
                        let upper = backend.journal.durable_lsn();
                        let identity = backend.journal.id().to_owned();
                        let cursor = match params.get("cursor").filter(|v| !v.is_null()) {
                            Some(v) => serde_json::from_value::<Cursor>(v.clone())
                                .map_err(|e| e.to_string())?,
                            None => {
                                let since = params
                                    .get("since_lsn")
                                    .map(|v| v.as_str().ok_or("invalid since_lsn"))
                                    .transpose()?
                                    .unwrap_or("0")
                                    .parse::<u64>()
                                    .map_err(|_| "invalid since_lsn")?;
                                Cursor::first(
                                    identity.clone(),
                                    run_id.clone(),
                                    Filter::Events,
                                    upper,
                                )
                                .with_next_lsn(since.checked_add(1).ok_or("cursor overflow")?)
                            }
                        };
                        let root = backend.journal.root().to_path_buf();
                        let mut page = tokio::task::spawn_blocking(move || {
                            flow_journal::page::page(
                                &root,
                                &identity,
                                &run_id,
                                Filter::Events,
                                cursor,
                                upper,
                                32,
                            )
                        })
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?;
                        page.events
                            .retain(|e| e.event.node_id.as_deref() == Some(node_id.as_str()));
                        let mut page = serde_json::to_value(page).map_err(|e| e.to_string())?;
                        page["terminal"] = json!(
                            run.terminal()
                                && page["snapshot_cursor"]["lsn"] == json!(upper.to_string())
                        );
                        Ok(page)
                    }
                    "flow/node.signal" => {
                        let request = params["request_id"].as_str().ok_or("request_id required")?;
                        if request.is_empty() || request.len() > 128 {
                            return Err("invalid request_id".into());
                        }
                        use sha2::Digest;
                        let identity = serde_json::to_vec(&(
                            source_plugin,
                            run_id.clone(),
                            node_id.clone(),
                            request,
                        ))
                        .map_err(|e| e.to_string())?;
                        let identity = hex::encode(sha2::Sha256::digest(identity));
                        let receipt = backend
                            .run_signal_bound(
                                &run_id,
                                &node_id,
                                params["payload"].clone(),
                                Some(&identity),
                                Some(&dispatch),
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                        serde_json::to_value(receipt).map_err(|e| e.to_string())
                    }
                    _ => Err("node method forbidden".into()),
                };
            }
            if method == "flow/services" {
                let services = backend.services.lock().await;
                return Ok(
                    json!({"services":services.iter().filter(|(id,_)|allowed.contains(id)).map(|(id,s)|json!({"plugin_id":id,"instance":s.instance,"alive":s.alive()})).collect::<Vec<_>>()}),
                );
            }
            let target = params["plugin_id"].as_str().ok_or("plugin_id required")?;
            if !allowed.iter().any(|id| id == target) {
                return Err("peer forbidden".into());
            }
            let service = backend
                .services
                .lock()
                .await
                .get(target)
                .cloned()
                .ok_or("peer unavailable")?;
            let instance = params["instance"].as_str().ok_or("instance required")?;
            match method.as_str() {
                "flow/call" => {
                    service
                        .call(
                            instance,
                            params["method"].as_str().ok_or("method required")?,
                            params["params"].clone(),
                        )
                        .await
                }
                "flow/events" => service.events_since(
                    instance,
                    params["after"]
                        .as_str()
                        .ok_or("cursor required")?
                        .parse()
                        .map_err(|_| "invalid cursor")?,
                ),
                _ => Err("host method forbidden".into()),
            }
        })
    })
}

pub fn with_subscriptions(
    base: HostHandler,
    instance: Arc<std::sync::OnceLock<std::sync::Weak<crate::resident_service::ResidentService>>>,
) -> HostHandler {
    Arc::new(move |method, params| {
        let base = base.clone();
        let instance = instance.clone();
        Box::pin(async move {
            if method == "flow/subscribe" {
                let service = instance
                    .get()
                    .and_then(|w| w.upgrade())
                    .ok_or("service not ready")?;
                let query = match params["stream"].as_str() {
                    Some("peer") => "flow/events",
                    Some("node") => "flow/node.events",
                    _ => return Err("invalid stream".into()),
                };
                let mut args = params.get("params").cloned().unwrap_or(json!({}));
                if !args.is_object() {
                    return Err("subscription params must be an object".into());
                }
                if query == "flow/events" && args.get("after").is_none() {
                    args["after"] = json!("0");
                }
                // Authorize before accepting the subscription. The worker repeats authorization per page.
                base(query.into(), args.clone()).await?;
                Ok(
                    json!({"subscription":service.follow(params["subscription"].as_str().ok_or("subscription required")?.to_owned(),base,query.into(),args)?}),
                )
            } else if method == "flow/unsubscribe" {
                let service = instance
                    .get()
                    .and_then(|w| w.upgrade())
                    .ok_or("service not ready")?;
                Ok(
                    json!({"removed":service.unfollow(params["subscription"].as_str().ok_or("subscription required")?)}),
                )
            } else {
                base(method, params).await
            }
        })
    })
}
