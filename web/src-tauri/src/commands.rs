//! Tauri 命令层：渲染层可调用的 IPC 命令。
//!
//! 这一层只做三件事：**校验会话归属**、把调用转交给内嵌后端、把后端
//! 结果翻译成渲染层能消费的形状。它不包含任何业务语义——业务语义一律
//! 在 `flow_rpc` 的方法模块里，因此桌面端与 Web 端行为完全一致。
//!
//! 关键约定：
//!
//! - 每个命令都先 `sessions.check` 校验会话属于当前窗口，防止跨窗口
//!   操作他人会话。
//! - 长耗时调用在宿主专用 runtime 上 `spawn`，不占用 Tauri 主 runtime。
//! - 订阅推送在渲染层消失（channel 发送失败）时主动结束并注销订阅，
//!   否则会留下一条永远不消费却仍在跑的推送任务。
use crate::service::{Host, Result, Service};
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tauri::{ipc::Channel, State, WebviewWindow};
use tauri_plugin_dialog::DialogExt;
use tokio::io::AsyncWriteExt;
use tower::ServiceExt;

/// 宿主状态的注入类型。
type Backend<'a> = State<'a, Arc<Host>>;

/// 返回后端的基础信息（webhook HTTP 入口地址）。
#[tauri::command]
pub fn flow_info(host: Backend<'_>) -> Value {
    json!({"httpUrl":host.services.http_url})
}

/// 为当前窗口开启一个后端会话，返回会话 id。
#[tauri::command]
pub fn flow_open_client(host: Backend<'_>, window: WebviewWindow) -> Result<String> {
    // 停机中不再开新会话：此时开出的会话立刻就会被关闭。
    if host
        .services
        .closing
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Err("local service is stopping".into());
    }
    Ok(host.sessions.open(window.label().into()))
}

/// 关闭当前窗口的某个会话。
#[tauri::command]
pub fn flow_close_client(host: Backend<'_>, window: WebviewWindow, session: String) -> Result<()> {
    host.sessions.check(&session, window.label())?;
    host.sessions.close(&session);
    Ok(())
}

/// 执行一次 RPC 调用并返回结果。
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
    // 在宿主 runtime 上执行：后端调用可能长时间阻塞（执行器派发、
    // 磁盘 IO），不能占着 Tauri 的主 runtime。
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

/// 建立一条订阅，把后端推送事件经 Tauri channel 转发给渲染层。
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
        // 等到订阅已在会话表里登记后再发起调用：否则渲染层可能在
        // 订阅就绪前就收到事件，顺序反了会让前端漏掉首个事件。
        if started.await.is_err() {
            return;
        }
        match services.request(service, method, params).await {
            Ok((value, mut events)) => {
                // 先把首个响应发回渲染层，再开始推事件——渲染层据此
                // 知道订阅已建立。
                let success = value.get("error").is_none();
                if reply.send(Ok(value)).is_ok() && success {
                    while let Some(event) = events.recv().await {
                        let Ok(event) = serde_json::from_str::<Value>(event.get()) else {
                            break;
                        };
                        // 发送失败说明渲染层已消失：立即结束推送，
                        // 否则会一直空转到后端关闭。
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
        // 无论正常结束还是出错，都注销订阅，避免会话表里留下死订阅。
        sessions.remove(&sid, &sub);
    });
    if let Err(error) =
        host.sessions
            .insert(&session, window.label(), subscription, task.abort_handle())
    {
        // 登记失败（会话已关或订阅 id 重复）：中止刚起的任务，避免
        // 出现一个不在会话表里却仍在推送的孤儿订阅。
        task.abort();
        return Err(error);
    }
    let _ = ready.send(());
    response
        .await
        .map_err(|_| "RPC client closed".to_string())?
}
/// 取消一条订阅。
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

/// 下载一次运行的输出到用户选择的位置。
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
    // 用户取消保存：不视为错误，直接返回。
    let Some(path) = path.await.map_err(|e| e.to_string())? else {
        return Ok(());
    };
    let path = path.into_path().map_err(|e| e.to_string())?;
    let services = host.services.clone();
    host.runtime
        .spawn(async move {
            // 持读锁：下载期间不允许停机把库关掉，否则会读到半个文件。
            let _gate = services.gate.read().await;
            if services.closing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("local service is stopping".into());
            }
            // 走内部下载路由：请求已由 download_request 注入令牌并
            // 校验过标识符，这里不再重复拼 URL。
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
            // 先写同目录临时文件，全部写完且 fsync 后再 rename：
            // 中途失败或进程被杀时，目标文件保持原样，不会留下半个文件。
            let temporary = tempfile::NamedTempFile::new_in(parent).map_err(|e| e.to_string())?;
            let mut file =
                tokio::fs::File::from_std(temporary.reopen().map_err(|e| e.to_string())?);
            let mut stream = response.into_body().into_data_stream();
            while let Some(bytes) = stream.next().await {
                file.write_all(&bytes.map_err(|e| e.to_string())?)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            // fsync 后再落位：否则崩溃可能留下一个内容不完整的文件。
            file.sync_all().await.map_err(|e| e.to_string())?;
            drop(file);
            temporary.persist(path).map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())?
}
