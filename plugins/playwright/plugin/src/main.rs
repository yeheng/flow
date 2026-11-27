//! flow-playwright-plugin：playwright 节点的执行进程（壳）。
//!
//! 通道引导 + initialize 握手 + 会话循环（与 flow-llm-plugin 同一模式）。
//! 执行核心在 lib（`run_playwright`）：spawn `node runtime/wrapper.js`，
//! stdin 喂节点参数，stdout 收哨兵信封。
//!
//! 会话模型：与派发生命周期 1:1——runtime 每次 invoke 开新会话（spawn →
//! 握手 → invoke → shutdown），本进程循环只是防御性地多接几轮。
//!
//! 环境事实：插件进程环境是宿主白名单注入的（PATH/HOME/TMPDIR 在内，
//! 见 flow-plugin-runtime `plugin_env`）——node 从 PATH 解析，playwright
//! 浏览器缓存落在 HOME 下的默认位置。runtime 目录不经环境变量，按
//! `current_exe() → ../runtime` 定位（部署布局由 install-plugins.sh 钉死）。

use serde_json::Value;

use flow_playwright_plugin::{
    fit_output_budget, plugin_root_near, run_playwright, InvokeError, InvokeParams, INVOKE_METHOD,
};
use flow_plugin_sdk::ipc::Channel;
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame, Frame, FrameKind};
use flow_plugin_sdk::protocol::handshake::{
    initialize_response, parse_initialize_params, Capability, InitializeResult, INITIALIZE_METHOD,
};
use flow_plugin_sdk::protocol::jsonrpc::{Error, ErrorCode, Id, Message, Request};

fn app_error(code: i64, message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Application(code), message)
}

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

async fn serve_from_env() -> i32 {
    let token = flow_plugin_sdk::ipc::env::token_from_env();
    let channel = match channel_from_env().await {
        Ok(channel) => channel,
        Err(error) => {
            eprintln!("flow-playwright-plugin: channel open failed: {error}");
            return 2;
        }
    };
    match serve(channel, token).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("flow-playwright-plugin: session end: {error}");
            0
        }
    }
}

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

#[derive(Debug, thiserror::Error)]
enum SessionError {
    #[error("channel closed")]
    Closed,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] flow_plugin_sdk::protocol::ProtoError),
}

async fn serve(channel: Channel, token: Option<String>) -> Result<(), SessionError> {
    let (mut reader, mut writer) = tokio::io::split(channel);

    // --- initialize：版本门 + 令牌回验 ---
    let first = read_message(&mut reader).await?;
    let request = match first {
        Message::Request(request) if request.method == INITIALIZE_METHOD => request,
        _ => {
            return Err(SessionError::Protocol(
                flow_plugin_sdk::protocol::ProtoError::InvalidMessage(
                    "first message must be initialize".into(),
                ),
            ))
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
        let response = flow_plugin_sdk::protocol::jsonrpc::Response {
            jsonrpc: "2.0",
            id: request.id,
            result: None,
            error: Some(app_error(-32001, message)),
        };
        let _ = try_write(&mut writer, &Message::Response(response)).await;
        return Err(SessionError::Closed);
    }
    // master_permit_v1：受管插件（manifest 声明了 [permissions]）必须协商；
    // 本插件不发反向 permission/request（授权在 runtime 派发前完成），
    // 但能力目录按 wire.md §3.3 如实声明。
    let result = InitializeResult {
        protocol_version: flow_plugin_sdk::protocol::PROTOCOL_VERSION,
        capabilities: vec![Capability::MasterPermitV1],
        server_info: serde_json::json!({
            "name": "flow-playwright-plugin",
            "version": env!("CARGO_PKG_VERSION"),
        }),
        session_token: token,
    };
    let response = initialize_response(request.id, &result);
    write_message(&mut writer, &Message::Response(response)).await?;

    // --- 会话循环：invoke 请求 → {output} 响应 ---
    while let Some(message) = read_message_opt(&mut reader).await? {
        let request = match message {
            Message::Request(request) => request,
            // 通知（取消等）与迟到响应：忽略——执行由插件侧超时兜底。
            _ => continue,
        };
        match request.method.as_str() {
            INVOKE_METHOD => {
                invoke_round(&mut writer, request).await?;
            }
            other => {
                let response = flow_plugin_sdk::protocol::jsonrpc::Response {
                    jsonrpc: "2.0",
                    id: request.id,
                    result: None,
                    error: Some(Error::new(
                        ErrorCode::MethodNotFound,
                        format!("method not found: {other}"),
                    )),
                };
                write_message(&mut writer, &Message::Response(response)).await?;
            }
        }
    }
    Ok(())
}

/// 一次 playwright 执行：畸形参数回 `-32022`；执行失败按错误码透传；
/// 成功 = result `{output: <信封>}`。
async fn invoke_round<W>(writer: &mut W, request: Request) -> Result<(), SessionError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    // 请求 params 形状：{"params": <节点参数>}（plugin_node 派发契约）。
    let node_params: Value = request
        .params
        .as_ref()
        .and_then(|params| params.get("params"))
        .cloned()
        .unwrap_or(Value::Null);
    let respond_error = |code: i64, message: String| {
        let response = flow_plugin_sdk::protocol::jsonrpc::Response {
            jsonrpc: "2.0",
            id: request.id.clone(),
            result: None,
            error: Some(app_error(code, message)),
        };
        Message::Response(response)
    };
    let validated = match InvokeParams::validate(&node_params) {
        Ok(validated) => validated,
        Err(message) => {
            let error = respond_error(
                flow_playwright_plugin::MALFORMED_ERROR,
                format!("playwright/invoke 参数非法：{message}"),
            );
            return write_message(writer, &error).await;
        }
    };
    // 插件根按部署布局定位（current_exe → 上两级）。
    let plugin_root = plugin_root_near(&std::env::current_exe().unwrap_or_default())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    tracing::info!(
        "playwright/invoke：browser={} headless={} timeout={}ms",
        validated.browser,
        validated.headless,
        validated.timeout.as_millis()
    );
    let outcome = run_playwright(&validated, &plugin_root, "node").await;
    let envelope = match outcome {
        Ok(envelope) => match fit_output_budget(envelope) {
            Ok(envelope) => envelope,
            Err(error) => {
                let error = respond_error(error.code, error.message);
                return write_message(writer, &error).await;
            }
        },
        Err(InvokeError { code, message }) => {
            tracing::warn!("playwright 执行失败（{code}）：{message}");
            let error = respond_error(code, message);
            return write_message(writer, &error).await;
        }
    };
    let result = serde_json::json!({ "output": envelope });
    let response = flow_plugin_sdk::protocol::jsonrpc::Response {
        jsonrpc: "2.0",
        id: request.id,
        result: Some(result),
        error: None,
    };
    write_message(writer, &Message::Response(response)).await
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

async fn try_write<S>(stream: &mut S, message: &Message) -> Result<(), SessionError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    write_message(stream, message).await
}

// Id 导入占位（invoke_round 里 request.id 直接使用）。
#[allow(dead_code)]
fn _id_type_anchor(_id: &Id) {}
