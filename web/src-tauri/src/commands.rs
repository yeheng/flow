use crate::service::{Host, Result, Service};
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tauri::{ipc::Channel, State, WebviewWindow};
use tauri_plugin_dialog::DialogExt;
use tokio::io::AsyncWriteExt;
use tower::ServiceExt;

type Backend<'a> = State<'a, Arc<Host>>;

#[tauri::command]
pub fn flow_info(host: Backend<'_>) -> Value {
    json!({"httpUrl":host.services.http_url})
}
#[tauri::command]
pub fn flow_open_client(host: Backend<'_>, window: WebviewWindow) -> Result<String> {
    if host
        .services
        .closing
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Err("local service is stopping".into());
    }
    Ok(host.sessions.open(window.label().into()))
}
#[tauri::command]
pub fn flow_close_client(host: Backend<'_>, window: WebviewWindow, session: String) -> Result<()> {
    host.sessions.check(&session, window.label())?;
    host.sessions.close(&session);
    Ok(())
}
#[tauri::command]
pub async fn flow_call(
    host: Backend<'_>,
    window: WebviewWindow,
    session: String,
    service: Service,
    method: String,
    params: Value,
) -> Result<Value> {
    host.sessions.check(&session, window.label())?;
    let services = host.services.clone();
    host.runtime
        .spawn(async move {
            services
                .request(service, method, params)
                .await
                .map(|(reply, _)| reply)
        })
        .await
        .map_err(|e| e.to_string())?
}
#[tauri::command]
// Tauri injects host/window; the remaining arguments form the subscription IPC contract.
#[allow(clippy::too_many_arguments)]
pub async fn flow_subscribe(
    host: Backend<'_>,
    window: WebviewWindow,
    session: String,
    subscription: String,
    service: Service,
    method: String,
    params: Value,
    channel: Channel<Value>,
) -> Result<Value> {
    host.sessions.check(&session, window.label())?;
    let services = host.services.clone();
    let sessions = host.sessions.clone();
    let sid = session.clone();
    let sub = subscription.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let (reply, response) = tokio::sync::oneshot::channel();
    let task = host.runtime.spawn(async move {
        if started.await.is_err() {
            return;
        }
        match services.request(service, method, params).await {
            Ok((value, mut events)) => {
                let success = value.get("error").is_none();
                if reply.send(Ok(value)).is_ok() && success {
                    while let Some(event) = events.recv().await {
                        let Ok(event) = serde_json::from_str::<Value>(event.get()) else {
                            break;
                        };
                        if channel.send(event["params"]["result"].clone()).is_err() {
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
        sessions.remove(&sid, &sub);
    });
    if let Err(error) =
        host.sessions
            .insert(&session, window.label(), subscription, task.abort_handle())
    {
        task.abort();
        return Err(error);
    }
    let _ = ready.send(());
    response
        .await
        .map_err(|_| "RPC client closed".to_string())?
}
#[tauri::command]
pub fn flow_unsubscribe(
    host: Backend<'_>,
    window: WebviewWindow,
    session: String,
    subscription: String,
) -> Result<()> {
    host.sessions.check(&session, window.label())?;
    host.sessions.remove(&session, &subscription);
    Ok(())
}

#[tauri::command]
pub async fn flow_download(
    host: Backend<'_>,
    app: tauri::AppHandle,
    run: String,
    output: String,
) -> Result<()> {
    let request = host.services.download_request(&run, &output)?;
    let (chosen, path) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .set_file_name(format!("{output}.bin"))
        .save_file(move |file| {
            let _ = chosen.send(file);
        });
    let Some(path) = path.await.map_err(|e| e.to_string())? else {
        return Ok(());
    };
    let path = path.into_path().map_err(|e| e.to_string())?;
    let services = host.services.clone();
    host.runtime
        .spawn(async move {
            let _gate = services.gate.read().await;
            if services.closing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("local service is stopping".into());
            }
            let response = services
                .downloads
                .clone()
                .oneshot(request)
                .await
                .map_err(|e| e.to_string())?;
            if !response.status().is_success() {
                return Err(format!("下载失败：{}", response.status()));
            }
            let parent = path.parent().ok_or("invalid destination")?;
            // Keep an existing destination intact if a chunk is missing/corrupt or the app exits.
            let temporary = tempfile::NamedTempFile::new_in(parent).map_err(|e| e.to_string())?;
            let mut file =
                tokio::fs::File::from_std(temporary.reopen().map_err(|e| e.to_string())?);
            let mut stream = response.into_body().into_data_stream();
            while let Some(bytes) = stream.next().await {
                file.write_all(&bytes.map_err(|e| e.to_string())?)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            file.sync_all().await.map_err(|e| e.to_string())?;
            drop(file);
            temporary.persist(path).map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())?
}
