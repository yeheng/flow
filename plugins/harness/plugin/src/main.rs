//! flow-agent-plugin：harness agent 插件服务进程入口。
//!
//! 启动契约（proc-ipc，见根仓库 docs/protocol/wire.md §0）：
//! - unix：`PROC_IPC_FD`（固定 3）接管的 socketpair fd；
//! - 其余平台：`PROC_IPC_ADDR` 回连 + `PROC_IPC_TOKEN` 握手回验。
//!
//! 进程生命周期归宿主（flow-executor）管：宿主杀进程组即整体回收，
//! 本进程不装信号处理、不自行 daemon 化。

use flow_harness_plugin::{
    app_error, classify, initialize_result, project_event, summarize, EventClocks, Incoming,
};
use flow_harness_protocol::{
    RunTurnParams, ASSEMBLY_ERROR, EVENT_METHOD, RUN_TURN_METHOD, TURN_ERROR,
};
use flow_plugin_sdk::ipc::Channel;
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame, Frame, FrameKind};
use flow_plugin_sdk::protocol::handshake::{
    parse_initialize_params, PermissionRequestParams, PermissionRequestResult, INITIALIZE_METHOD,
    PERMISSION_REQUEST_METHOD,
};
use flow_plugin_sdk::protocol::jsonrpc::{Id, Message, Notification, Request, Response};

const CANCEL_METHOD: &str = "$/cancelRequest";

fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .without_time()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("plugin runtime");
    let code = runtime.block_on(serve_from_env());
    std::process::exit(code);
}

/// 按环境变量接管通道并进入服务循环。
async fn serve_from_env() -> i32 {
    let token = flow_plugin_sdk::ipc::env::token_from_env();
    let channel = match channel_from_env().await {
        Ok(channel) => channel,
        Err(error) => {
            eprintln!("flow-agent-plugin: channel open failed: {error}");
            return 2;
        }
    };
    match serve(channel, token).await {
        Ok(()) => 0,
        Err(error) => {
            // 帧级错误 = 会话分叉，直接退出（wire.md：未知即明确）。
            eprintln!("flow-agent-plugin: session end: {error}");
            0
        }
    }
}

/// unix 与非 unix 承载的统一入口。
async fn channel_from_env() -> std::io::Result<Channel> {
    #[cfg(unix)]
    {
        flow_plugin_sdk::ipc::channel::from_env()
    }
    #[cfg(not(unix))]
    {
        flow_plugin_sdk::ipc::channel::from_env().await
    }
}

/// 会话错误：EOF（宿主关闭）是正常收口，其余为协议错误。
#[derive(Debug, thiserror::Error)]
enum SessionError {
    #[error("channel closed")]
    Closed,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] flow_plugin_sdk::protocol::ProtoError),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

async fn serve(channel: Channel, token: Option<String>) -> Result<(), SessionError> {
    let (mut reader, writer) = tokio::io::split(channel);
    // writer 全程 Arc<Mutex<>>：runTurn 的反向权限请求与事件转发共享写侧。
    let writer = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    // --- initialize 握手：版本门 + 令牌回验 ---
    let first = read_message(&mut reader).await?;
    let request = match first {
        Message::Request(request) if request.method == INITIALIZE_METHOD => request,
        other => {
            let error = flow_plugin_sdk::protocol::ProtoError::InvalidMessage(format!(
                "first message must be initialize, got {other:?}"
            ));
            return Err(SessionError::Protocol(error));
        }
    };
    let params = parse_initialize_params(&request).map_err(|error| {
        SessionError::Protocol(flow_plugin_sdk::protocol::ProtoError::InvalidMessage(
            error.message.clone(),
        ))
    })?;
    if let Err(message) =
        flow_plugin_sdk::protocol::handshake::check_version(params.protocol_version)
    {
        // 版本不等：回 -32001（含双方主版本）再退出，不降级（wire.md §3）。
        let response = flow_plugin_sdk::protocol::jsonrpc::Response {
            jsonrpc: "2.0",
            id: request.id,
            result: None,
            error: Some(app_error(-32001, message)),
        };
        let _ = try_write_message(&mut *writer.lock().await, &Message::Response(response)).await;
        return Err(SessionError::Closed);
    }
    let result = initialize_result(token);
    let response = flow_plugin_sdk::protocol::handshake::initialize_response(request.id, &result);
    write_message(&mut *writer.lock().await, &Message::Response(response)).await?;

    // --- 会话循环：一次至多一个在飞回合（宿主按任务串行派发） ---
    while let Some(message) = read_message_opt(&mut reader).await? {
        match classify(message).map_err(|e| {
            SessionError::Protocol(flow_plugin_sdk::protocol::ProtoError::InvalidMessage(
                e.message.clone(),
            ))
        })? {
            Incoming::Request(request) => match request.method.as_str() {
                RUN_TURN_METHOD => {
                    run_turn(&mut reader, writer.clone(), request).await?;
                }
                _ => {
                    let response = flow_plugin_sdk::protocol::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id: request.id,
                        result: None,
                        error: Some(flow_plugin_sdk::protocol::jsonrpc::Error::new(
                            flow_plugin_sdk::protocol::jsonrpc::ErrorCode::MethodNotFound,
                            format!("method not found: {}", request.method),
                        )),
                    };
                    write_message(&mut *writer.lock().await, &Message::Response(response)).await?;
                }
            },
            // 无在飞回合时的取消/其余通知：按规范忽略（不回响应）。
            Incoming::Notification(_) | Incoming::Response(_) => {}
        }
    }
    Ok(())
}

/// 驱动一次 runTurn：装配 → 事件转发 → 摘要/错误响应；期间处理取消。
async fn run_turn<R, W>(
    reader: &mut R,
    writer: std::sync::Arc<tokio::sync::Mutex<W>>,
    request: Request,
) -> Result<(), SessionError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let id = request.id.clone();
    let params: RunTurnParams = request
        .params
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| {
            SessionError::Protocol(flow_plugin_sdk::protocol::ProtoError::InvalidMessage(
                "agent/runTurn requires RunTurnParams".into(),
            ))
        })?;

    // --- 装配（确定性失败 → -32010）---
    let spec = agent_host::headless::HeadlessSpec {
        base_url: params.spec.base_url.clone(),
        model: params.spec.model.clone(),
        api: params.spec.api.clone(),
        proxy: params.spec.proxy.clone(),
        max_turns: params.spec.max_turns,
        max_tool_calls_per_turn: params.spec.max_tool_calls_per_turn,
        max_tokens: params.spec.max_tokens,
        tool_timeout_secs: params.spec.tool_timeout_secs,
    };
    let roots = std::path::PathBuf::from(&params.roots);
    if let Err(error) = std::fs::create_dir_all(roots.join("tmp")) {
        let response = flow_plugin_sdk::protocol::jsonrpc::Response {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(app_error(
                ASSEMBLY_ERROR,
                format!("harness agent 临时目录创建失败：{error}"),
            )),
        };
        return write_shared(&writer, &Message::Response(response)).await;
    }
    // 受管回合（TurnPermission）：工具调用全部经 permission/request 反向
    // 裁决；未受管（legacy 回合，仅测试钩子路径）保留本地 AutoEdit + 恒拒
    // approver——没有任何路径本地放行（09 §8.4，FullAuto 硬编码已删）。
    let managed = params.permission.as_ref().is_some_and(|p| p.managed);
    let plan = params
        .permission
        .as_ref()
        .is_some_and(|p| p.execution_mode == "plan");
    tracing::info!(managed, plan, "run_turn assembling (permission mode)");
    // 反向请求登记表：request id → oneshot；select 循环按 id 投递响应。
    let pending_reverse: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<Id, tokio::sync::oneshot::Sender<Message>>>,
    > = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    let pending_for_loop = pending_reverse.clone();
    let reverse_seq: std::sync::Arc<std::sync::atomic::AtomicU64> =
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));
    let assembled = if managed {
        let channel: agent_host::permission::PermissionChannel = {
            let writer_ref = writer.clone();
            let pending_reverse = pending_reverse.clone();
            let reverse_seq = reverse_seq.clone();
            std::sync::Arc::new(
                move |tool_call_id: &str, tool: &str, args: &serde_json::Value| {
                    let writer_ref = writer_ref.clone();
                    let pending_reverse = pending_reverse.clone();
                    let reverse_seq = reverse_seq.clone();
                    let tool_call_id = tool_call_id.to_string();
                    let tool = tool.to_string();
                    let args = args.clone();
                    Box::pin(async move {
                        let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
                        let id = Id::String(format!(
                            "perm-{}",
                            reverse_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        ));
                        let params = PermissionRequestParams {
                            tool_call_id: tool_call_id.clone(),
                            tool: tool.clone(),
                            params: args,
                            cwd: None,
                        };
                        let request = Message::Request(Request {
                            jsonrpc: "2.0",
                            id: id.clone(),
                            method: PERMISSION_REQUEST_METHOD.into(),
                            params: Some(serde_json::to_value(&params).map_err(|e| e.to_string())?),
                        });
                        write_message(&mut *writer_ref.lock().await, &request)
                            .await
                            .map_err(|e| format!("permission channel write failed: {e}"))?;
                        pending_reverse.lock().await.insert(id, tx);
                        let response = tokio::select! {
                            response = rx => response.map_err(|_| "permission channel closed".to_string())?,
                            _ = tokio::time::sleep(std::time::Duration::from_secs(600)) => {
                                return Err("permission decision timeout (failing closed)".into());
                            }
                        };
                        match response {
                            Message::Response(Response {
                                result: Some(value),
                                ..
                            }) => {
                                let verdict: PermissionRequestResult =
                                    serde_json::from_value(value).map_err(|e| e.to_string())?;
                                if verdict.allowed {
                                    Ok(agent_core::ToolCallVerdict::Allow)
                                } else {
                                    Ok(agent_core::ToolCallVerdict::Block(format!(
                                        "permission denied: {}",
                                        verdict.reason.unwrap_or_default()
                                    )))
                                }
                            }
                            Message::Response(Response {
                                error: Some(error), ..
                            }) => Err(format!("permission request rejected: {}", error.message)),
                            _ => Err("unexpected permission response shape".into()),
                        }
                    })
                },
            )
        };
        agent_host::headless::build_headless_managed(
            &params.api_key,
            &spec,
            &roots,
            std::path::Path::new(&params.workdir),
            channel,
            plan,
        )
        .await
    } else {
        agent_host::headless::build_headless_with(
            &params.api_key,
            &spec,
            &roots,
            std::path::Path::new(&params.workdir),
            agent_host::permission::Mode::AutoEdit,
        )
        .await
    };
    let session = match assembled {
        Ok(session) => session,
        Err(error) => {
            let response = flow_plugin_sdk::protocol::jsonrpc::Response {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(app_error(
                    ASSEMBLY_ERROR,
                    format!("harness agent assembly failed: {error}"),
                )),
            };
            return write_shared(&writer, &Message::Response(response)).await;
        }
    };
    let session_id = session.host.session().current_id();

    // --- 回合：事件回调 → channel 泵 → 响应 ---
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<agent_core::AgentEvent>();
    let cancel = agent_core::CancelSignal::new();
    let cancel_for_task = cancel.clone();
    let prompt = params.prompt.clone();
    let turn = tokio::spawn(async move {
        let result = session
            .host
            .run_turn(&prompt, move |event| {
                let _ = event_tx.send(event);
            })
            .await;
        agent_host::session_cleanup::cleanup_session_resources(&session_id).await;
        result
    });

    let clocks = EventClocks::default();
    let mut turn = std::pin::pin!(turn);
    loop {
        tokio::select! {
            event = event_rx.recv() => {
                let Some(event) = event else { continue };
                if let Some(projected) = project_event(&event, &clocks) {
                    let notification = Message::Notification(Notification {
                        jsonrpc: "2.0",
                        method: EVENT_METHOD.into(),
                        params: Some(serde_json::to_value(&projected)?),
                    });
                    write_shared(&writer, &notification).await?;
                }
            }
            incoming = read_message_opt(reader) => {
                match incoming? {
                    Some(Message::Notification(note)) => {
                        if note.method == CANCEL_METHOD {
                            let target = note.params.as_ref()
                                .and_then(|p| p.get("id")).cloned();
                            if target == Some(serde_json::to_value(&id)?) {
                                cancel_for_task.cancel();
                            }
                        }
                    }
                    // 反向权限请求的响应：按 id 投递给等待方。
                    Some(Message::Response(response)) => {
                        if let Some(tx) =
                            pending_for_loop.lock().await.remove(&response.id)
                        {
                            let _ = tx.send(Message::Response(response));
                        }
                    }
                    _ => {}
                }
            }
            result = &mut turn => {
                let response = match result {
                    Ok(Ok(summary)) => flow_plugin_sdk::protocol::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id,
                        result: Some(serde_json::to_value(summarize(&summary))?),
                        error: None,
                    },
                    Ok(Err(error)) => flow_plugin_sdk::protocol::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id,
                        result: None,
                        error: Some(app_error(TURN_ERROR, format!("harness agent turn failed: {error}"))),
                    },
                    Err(join) => flow_plugin_sdk::protocol::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id,
                        result: None,
                        error: Some(app_error(TURN_ERROR, format!("agent task panicked: {join}"))),
                    },
                };
                return write_shared(&writer, &Message::Response(response)).await;
            }
        }
    }
}

async fn read_message<S>(stream: &mut S) -> Result<Message, SessionError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    read_message_opt(stream).await?.ok_or(SessionError::Closed)
}

async fn read_message_opt<S>(stream: &mut S) -> Result<Option<Message>, SessionError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let frame = match read_frame(stream).await {
        Ok(frame) => frame,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(frame.as_rpc()?))
}

async fn write_shared<W>(
    writer: &std::sync::Arc<tokio::sync::Mutex<W>>,
    message: &Message,
) -> Result<(), SessionError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    write_message(&mut *writer.lock().await, message).await
}

async fn write_message<S>(stream: &mut S, message: &Message) -> Result<(), SessionError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let frame = Frame {
        kind: FrameKind::Rpc,
        payload: message.to_vec()?,
    };
    write_frame(stream, &frame).await?;
    Ok(())
}

async fn try_write_message<S>(stream: &mut S, message: &Message) -> Result<(), SessionError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    write_message(stream, message).await
}
