//! Resident IPC fixture: initialize, repeated echo calls, notifications, explicit crash.
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame};
use flow_plugin_sdk::{InitializeResult, Message, Response, PROTOCOL_VERSION};
use serde_json::json;
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    let mut channel = flow_plugin_sdk::ipc::channel::from_env()?;
    #[cfg(not(unix))]
    let mut channel = flow_plugin_sdk::ipc::channel::from_env().await?;
    let Message::Request(request) = read_frame(&mut channel).await?.as_rpc()? else {
        return Err("initialize required".into());
    };
    let result = InitializeResult {
        protocol_version: PROTOCOL_VERSION,
        capabilities: vec![flow_plugin_sdk::Capability::InteractionEventsV1],
        server_info: json!({"name":"resident-echo","interaction_protocol":1}),
        session_token: std::env::var("PROC_IPC_TOKEN").ok(),
    };
    let response = Message::Response(Response {
        jsonrpc: "2.0",
        id: request.id,
        result: Some(serde_json::to_value(result)?),
        error: None,
    });
    write_frame(
        &mut channel,
        &flow_plugin_sdk::Frame {
            kind: flow_plugin_sdk::FrameKind::Rpc,
            payload: response.to_vec()?,
        },
    )
    .await?;
    let (sender, outgoing) = tokio::sync::mpsc::channel(64);
    let (incoming, mut messages) = tokio::sync::mpsc::channel(64);
    let dispatch = async {
        let mut relays = std::collections::HashMap::new();
        let mut next = 0u64;
        while let Some(message) = messages.recv().await {
            match message {
                Message::Request(request) => {
                    if request.method == "crash" {
                        std::process::exit(7);
                    }
                    if request.method == "relay" {
                        next += 1;
                        let id = flow_plugin_sdk::Id::String(format!("reverse-{next}"));
                        relays.insert(id.clone(), request.id);
                        sender
                            .send(Message::Request(flow_plugin_sdk::Request {
                                jsonrpc: "2.0",
                                id,
                                method: "flow/call".into(),
                                params: request.params,
                            }))
                            .await?;
                        continue;
                    }
                    if request.method == "host" {
                        next += 1;
                        let id = flow_plugin_sdk::Id::String(format!("reverse-{next}"));
                        relays.insert(id.clone(), request.id);
                        let p = request.params.unwrap_or(serde_json::Value::Null);
                        sender
                            .send(Message::Request(flow_plugin_sdk::Request {
                                jsonrpc: "2.0",
                                id,
                                method: p["method"].as_str().unwrap_or("").into(),
                                params: Some(p["params"].clone()),
                            }))
                            .await?;
                        continue;
                    }
                    let event = Message::Notification(flow_plugin_sdk::Notification {
                        jsonrpc: "2.0",
                        method: "echo.received".into(),
                        params: request.params.clone(),
                    });
                    sender.send(event).await?;
                    sender
                        .send(Message::Response(Response {
                            jsonrpc: "2.0",
                            id: request.id,
                            result: request.params,
                            error: None,
                        }))
                        .await?;
                }
                Message::Response(mut response) => {
                    if let Some(id) = relays.remove(&response.id) {
                        response.id = id;
                        sender.send(Message::Response(response)).await?;
                    }
                }
                Message::Notification(notification) => {
                    if notification.method=="flow/event" || notification.method=="flow/stream_closed" {
                        sender.send(Message::Notification(flow_plugin_sdk::Notification{jsonrpc:"2.0",method:"host.event".into(),params:notification.params})).await?;
                    }
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::select! {r=flow_plugin_sdk::duplex::pump(channel,outgoing,incoming)=>r?,r=dispatch=>r?}
    Ok(())
}
