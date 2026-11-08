//! flow-llm-plugin：llm 节点的出站执行进程。
//!
//! 方法层契约（`llm/invoke`）：
//!
//! - 请求 params：`{method, url, headers{}, body, proxy?, timeout_ms,
//!   credential?}`——runtime 已完成 permit 授权（指纹/journal intent），
//!   credential 是许可携带的凭据值（bearer），只经通道过线；
//! - 响应：**先**若干 `kind=binary` 帧（body 字节按序分块，每块 ≤512KiB），
//!   **后** JSON-RPC result `{status, headers{}, body_bytes, body_digest}`；
//!   宿主按长度+摘要校验重组；
//! - 应用错误码：`-32020` 出站失败（不确定类：副作用可能已发生）；
//!   `-32021` 响应超过捕获上限（不确定类，部分字节已丢弃）；
//!   `-32022` 请求畸形（防御纵深：runtime 授权前已校验过一次）。
//!
//! 出站纪律（与原 runtime 内联路径逐条一致）：
//! - **不跟随重定向、不自动重试**：两者都会在授权之外多发请求，让
//!   「授权 = 一次副作用」的记账失真；
//! - **限额流读**：Content-Length 缺失或撒谎时，只有边读边判才能拦住
//!   超限响应；
//! - 响应头按 RFC 7230 合并多值（Set-Cookie 等），不覆盖。

use serde::{Deserialize, Serialize};
use sha2::Digest;

use flow_plugin_sdk::ipc::Channel;
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame, Frame, FrameKind};
use flow_plugin_sdk::protocol::handshake::{
    parse_initialize_params, Capability, INITIALIZE_METHOD,
};
use flow_plugin_sdk::protocol::jsonrpc::{Error, ErrorCode, Id, Message, Notification, Request};

/// 出站失败（不确定类）。
pub const EGRESS_ERROR: i64 = -32020;
/// 响应超过捕获上限（不确定类）。
pub const CAPTURE_LIMIT_ERROR: i64 = -32021;
/// 请求畸形（防御纵深；正常不应出现——runtime 授权前已校验）。
pub const MALFORMED_ERROR: i64 = -32022;

/// 响应捕获上限：与 runtime 的 `flow_journal::MAX_VALUE_BYTES` 同值。
/// 这是**跨进程契约值**（两侧各自声明，改动必须双侧同步 + 协议版本）。
pub const MAX_CAPTURE_BYTES: u64 = 256 * 1024 * 1024;
/// 二进制帧分块上限（wire.md §1 建议 chunk ≤ 256KiB；留帧头余量）。
pub const BINARY_CHUNK_BYTES: usize = 256 * 1024;
/// 单请求超时上限（runtime 侧已按 10min 夹紧；这里再守一次）。
pub const TIMEOUT_MAX_MS: u64 = 10 * 60 * 1000;

/// `llm/invoke` 请求参数。
#[derive(Debug, Deserialize)]
pub struct InvokeParams {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub body: serde_json::Value,
    #[serde(default)]
    pub proxy: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub credential: Option<String>,
}

/// `llm/invoke` 响应摘要（result 体；body 字节走二进制帧先行到达）。
#[derive(Debug, Serialize)]
pub struct InvokeResult {
    pub status: u16,
    /// 合并多值后的响应头（逗号分隔，RFC 7230）。
    pub headers: serde_json::Map<String, serde_json::Value>,
    /// body 总字节数（宿主与二进制块重组长度互验）。
    pub body_bytes: u64,
    /// body 的 SHA-256 摘要（hex）。
    pub body_digest: String,
}

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
            eprintln!("flow-llm-plugin: channel open failed: {error}");
            return 2;
        }
    };
    match serve(channel, token).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("flow-llm-plugin: session end: {error}");
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

    // --- initialize：版本门 + 令牌回验 + binary_blocks 能力 ---
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
    let result = flow_plugin_sdk::protocol::handshake::InitializeResult {
        protocol_version: flow_plugin_sdk::protocol::PROTOCOL_VERSION,
        capabilities: vec![Capability::BinaryBlocks],
        server_info: serde_json::json!({"name": "flow-llm-plugin", "version": "0.1.0"}),
        session_token: token,
    };
    let response = flow_plugin_sdk::protocol::handshake::initialize_response(request.id, &result);
    write_message(&mut writer, &Message::Response(response)).await?;

    // --- 会话循环：单请求单响应（与派发 1:1） ---
    while let Some(message) = read_message_opt(&mut reader).await? {
        let request = match message {
            Message::Request(request) => request,
            // 通知（取消等）与迟到响应：忽略——egress 是单发单收。
            _ => continue,
        };
        match request.method.as_str() {
            "llm/invoke" => {
                invoke_round(&mut reader, &mut writer, request).await?;
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

/// 一次 egress：畸形请求回 `-32022`；出站失败回 `-32020`；超限 `-32021`；
/// 成功 = 二进制帧（body 分块）→ result 摘要。
async fn invoke_round<R, W>(
    _reader: &mut R,
    writer: &mut W,
    request: Request,
) -> Result<(), SessionError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let respond_error = |code: i64, message: String| {
        let response = flow_plugin_sdk::protocol::jsonrpc::Response {
            jsonrpc: "2.0",
            id: request.id.clone(),
            result: None,
            error: Some(app_error(code, message)),
        };
        Message::Response(response)
    };
    let params: InvokeParams = match request
        .params
        .and_then(|value| serde_json::from_value(value).ok())
    {
        Some(params) => params,
        None => {
            let error = respond_error(MALFORMED_ERROR, "llm/invoke requires InvokeParams".into());
            return write_message(writer, &error).await;
        }
    };

    let method = reqwest::Method::from_bytes(params.method.to_uppercase().as_bytes());
    let url = reqwest::Url::parse(&params.url);
    let (method, url) = match (method, url) {
        (Ok(method), Ok(url)) => (method, url),
        _ => {
            let error = respond_error(
                MALFORMED_ERROR,
                format!("malformed method/url: {} {}", params.method, params.url),
            );
            return write_message(writer, &error).await;
        }
    };
    let mut builder = reqwest::Client::builder()
        // 不跟随重定向、不自动重试：授权 = 一次副作用的记账不能被放大。
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never());
    if let Some(proxy) = params.proxy.as_deref().filter(|s| !s.trim().is_empty()) {
        match reqwest::Proxy::all(proxy) {
            Ok(proxy) => builder = builder.proxy(proxy),
            Err(error) => {
                let error = respond_error(MALFORMED_ERROR, format!("invalid proxy URL: {error}"));
                return write_message(writer, &error).await;
            }
        }
    }
    let client = match builder.build() {
        Ok(client) => client,
        Err(error) => {
            let error = respond_error(EGRESS_ERROR, format!("client build failed: {error}"));
            return write_message(writer, &error).await;
        }
    };
    let timeout = params.timeout_ms.unwrap_or(30_000).clamp(1, TIMEOUT_MAX_MS);
    let mut outbound = client
        .request(method, url)
        .timeout(std::time::Duration::from_millis(timeout));
    for (key, value) in &params.headers {
        outbound = outbound.header(
            key,
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string()),
        );
    }
    if !params.body.is_null() {
        outbound = outbound.json(&params.body);
    }
    if let Some(credential) = &params.credential {
        outbound = outbound.bearer_auth(credential);
    }
    let outbound = match outbound.build() {
        Ok(outbound) => outbound,
        Err(error) => {
            let error = respond_error(MALFORMED_ERROR, format!("request build failed: {error}"));
            return write_message(writer, &error).await;
        }
    };

    let mut response = match client.execute(outbound).await {
        Ok(response) => response,
        Err(error) => {
            // 出站失败（DNS/连接/超时）：副作用状态未知，不确定类。
            let message = error.without_url().to_string();
            let error = respond_error(EGRESS_ERROR, message);
            return write_message(writer, &error).await;
        }
    };
    if response
        .content_length()
        .is_some_and(|length| length > MAX_CAPTURE_BYTES)
    {
        let error = respond_error(
            CAPTURE_LIMIT_ERROR,
            "response exceeds capture limit; no complete outcome retained".into(),
        );
        return write_message(writer, &error).await;
    }
    let status = response.status().as_u16();
    // 多值头合并（逗号分隔，RFC 7230）：headers 是审计证据的保真面。
    let mut headers = serde_json::Map::new();
    for (name, value) in response.headers() {
        let value = value.to_str().unwrap_or("");
        headers
            .entry(name.to_string())
            .and_modify(|existing| {
                if let serde_json::Value::String(joined) = existing {
                    joined.push_str(", ");
                    joined.push_str(value);
                }
            })
            .or_insert_with(|| serde_json::Value::String(value.to_string()));
    }
    // 限额流读：边读边判，Content-Length 撒谎也拦得住。
    let mut raw: Vec<u8> = Vec::new();
    let exceeded = loop {
        match response.chunk().await {
            Ok(Some(bytes)) => {
                if raw.len() as u64 + bytes.len() as u64 > MAX_CAPTURE_BYTES {
                    break true;
                }
                raw.extend_from_slice(&bytes);
            }
            Ok(None) => break false,
            Err(error) => {
                let error = respond_error(EGRESS_ERROR, error.without_url().to_string());
                return write_message(writer, &error).await;
            }
        }
    };
    if exceeded {
        let error = respond_error(
            CAPTURE_LIMIT_ERROR,
            "response exceeds capture limit; no complete outcome retained".into(),
        );
        return write_message(writer, &error).await;
    }

    // body 字节：二进制帧先行（分块），后跟摘要 result。
    let digest = hex::encode(sha2::Sha256::digest(&raw));
    for chunk in raw.chunks(BINARY_CHUNK_BYTES) {
        write_frame(
            writer,
            &Frame {
                kind: FrameKind::Binary,
                payload: chunk.to_vec(),
            },
        )
        .await?;
    }
    let result = InvokeResult {
        status,
        headers,
        body_bytes: raw.len() as u64,
        body_digest: digest,
    };
    let response = flow_plugin_sdk::protocol::jsonrpc::Response {
        jsonrpc: "2.0",
        id: request.id,
        result: Some(serde_json::to_value(&result).expect("InvokeResult serializable")),
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

// 通知导入占位（会话循环忽略通知；保留类型引用供后续取消语义）。
#[allow(dead_code)]
fn _notification_type_anchor(_note: &Notification) {}

// Id 导入占位（invoke_round 里 request.id 直接使用）。
#[allow(dead_code)]
fn _id_type_anchor(_id: &Id) {}
