//! flow-agent-plugin：harness agent 插件服务进程入口。
//!
//! 启动契约（proc-ipc，见根仓库 docs/protocol/wire.md §0）：
//! - unix：`PROC_IPC_FD`（固定 3）接管的 socketpair fd；
//! - 其余平台：`PROC_IPC_ADDR` 回连 + `PROC_IPC_TOKEN` 握手回验。
//!
//! 进程生命周期归宿主（flow-executor）管：宿主杀进程组即整体回收，
//! 本进程不装信号处理、不自行 daemon 化。

use agent_plugin::{app_error, classify, initialize_result, project_event, summarize};
use agent_plugin::{EventClocks, Incoming, RunTurnParams, ASSEMBLY_ERROR, TURN_ERROR};
use plugin_proto::envelope::{read_frame, write_frame, Frame, FrameKind};
use plugin_proto::handshake::{parse_initialize_params, INITIALIZE_METHOD};
use plugin_proto::jsonrpc::{Message, Notification, Request};
use proc_ipc::Channel;

const RUN_TURN_METHOD: &str = "agent/runTurn";
const EVENT_METHOD: &str = "agent/event";
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
    let token = proc_ipc::env::token_from_env();
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
        proc_ipc::channel::from_env()
    }
    #[cfg(not(unix))]
    {
        proc_ipc::channel::from_env().await
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
    Protocol(#[from] plugin_proto::ProtoError),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

async fn serve(channel: Channel, token: Option<String>) -> Result<(), SessionError> {
    let (mut reader, mut writer) = tokio::io::split(channel);

    // --- initialize 握手：版本门 + 令牌回验 ---
    let first = read_message(&mut reader).await?;
    let request = match first {
        Message::Request(request) if request.method == INITIALIZE_METHOD => request,
        other => {
            let error = plugin_proto::ProtoError::InvalidMessage(format!(
                "first message must be initialize, got {other:?}"
            ));
            return Err(SessionError::Protocol(error));
        }
    };
    let params = parse_initialize_params(&request)
        .map_err(|error| SessionError::Protocol(plugin_proto::ProtoError::InvalidMessage(error.message.clone())))?;
    if let Err(message) = plugin_proto::handshake::check_version(params.protocol_version) {
        // 版本不等：回 -32001（含双方主版本）再退出，不降级（wire.md §3）。
        let response = plugin_proto::jsonrpc::Response {
            jsonrpc: "2.0",
            id: request.id,
            result: None,
            error: Some(app_error(-32001, message)),
        };
        let _ = try_write_message(&mut writer, &Message::Response(response)).await;
        return Err(SessionError::Closed);
    }
    let result = initialize_result(token);
    let response = plugin_proto::handshake::initialize_response(request.id, &result);
    write_message(&mut writer, &Message::Response(response)).await?;

    // --- 会话循环：一次至多一个在飞回合（宿主按任务串行派发） ---
    while let Some(message) = read_message_opt(&mut reader).await? {
        match classify(message).map_err(|e| SessionError::Protocol(plugin_proto::ProtoError::InvalidMessage(e.message.clone())))? {
            Incoming::Request(request) => match request.method.as_str() {
                RUN_TURN_METHOD => {
                    run_turn(&mut reader, &mut writer, request).await?;
                }
                _ => {
                    let response = plugin_proto::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id: request.id,
                        result: None,
                        error: Some(plugin_proto::jsonrpc::Error::new(
                            plugin_proto::jsonrpc::ErrorCode::MethodNotFound,
                            format!("method not found: {}", request.method),
                        )),
                    };
                    write_message(&mut writer, &Message::Response(response)).await?;
                }
            },
            // 无在飞回合时的取消/其余通知：按规范忽略（不回响应）。
            Incoming::Notification(_) | Incoming::Response(_) => {}
        }
    }
    Ok(())
}

/// 驱动一次 runTurn：装配 → 事件转发 → 摘要/错误响应；期间处理取消。
async fn run_turn<R, W>(reader: &mut R, writer: &mut W, request: Request) -> Result<(), SessionError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let id = request.id.clone();
    let params: RunTurnParams = request
        .params
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| {
            SessionError::Protocol(plugin_proto::ProtoError::InvalidMessage(
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
        let response = plugin_proto::jsonrpc::Response {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(app_error(
                ASSEMBLY_ERROR,
                format!("harness agent 临时目录创建失败：{error}"),
            )),
        };
        return write_message(writer, &Message::Response(response)).await;
    }
    let assembled = agent_host::headless::build_headless_with(
        &params.api_key,
        &spec,
        &roots,
        std::path::Path::new(&params.workdir),
        agent_host::permission::Mode::FullAuto,
    )
    .await;
    let session = match assembled {
        Ok(session) => session,
        Err(error) => {
            let response = plugin_proto::jsonrpc::Response {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(app_error(ASSEMBLY_ERROR, format!("harness agent assembly failed: {error}"))),
            };
            return write_message(writer, &Message::Response(response)).await;
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
                    write_message(writer, &notification).await?;
                }
            }
            incoming = read_message_opt(reader) => {
                if let Some(message) = incoming? {
                    if let Message::Notification(note) = message {
                        if note.method == CANCEL_METHOD {
                            let target = note.params.as_ref()
                                .and_then(|p| p.get("id")).cloned();
                            if target == Some(serde_json::to_value(&id)?) {
                                cancel_for_task.cancel();
                            }
                        }
                    }
                }
            }
            result = &mut turn => {
                let response = match result {
                    Ok(Ok(summary)) => plugin_proto::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id,
                        result: Some(serde_json::to_value(summarize(&summary))?),
                        error: None,
                    },
                    Ok(Err(error)) => plugin_proto::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id,
                        result: None,
                        error: Some(app_error(TURN_ERROR, format!("harness agent turn failed: {error}"))),
                    },
                    Err(join) => plugin_proto::jsonrpc::Response {
                        jsonrpc: "2.0",
                        id,
                        result: None,
                        error: Some(app_error(TURN_ERROR, format!("agent task panicked: {join}"))),
                    },
                };
                return write_message(writer, &Message::Response(response)).await;
            }
        }
    }
}

async fn read_message<S>(stream: &mut S) -> Result<Message, SessionError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    read_message_opt(stream)
        .await?
        .ok_or(SessionError::Closed)
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
