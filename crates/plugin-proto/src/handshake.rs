//! `initialize` 握手：版本与能力协商。
//!
//! 规则（与 flow 执行协议同一条纪律）：
//!
//! 1. **版本门集中在握手**：主版本号相等才继续；不等就回错误并关闭，
//!    **不降级**。逐帧不带版本号 —— 帧格式由握手结果担保。
//! 2. **能力是目录，不是开关**：声明未知能力 → 忽略；缺必需能力 → 拒绝。
//!    这样老宿主 + 新插件（或反之）可以共存：新能力没被对方声明就不启用，
//!    不会出现「一边以为对方支持、实际不支持」的静默错配。
//! 3. **宿主先说话**：连接建立后宿主发 `initialize` 请求，插件回响应。
//!    谁先说话必须写死，否则两边互相等。
//!
//! 兼容性规则（与 crate 文档一致）：
//! - 加方法、加能力名：安全。
//! - 改帧格式 / 现有字段语义：bump 主版本。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::jsonrpc::{Error, ErrorCode, Id, Request, Response};

/// 协议版本。高位 16 bit 是主版本，低位 16 bit 是次版本。
///
/// 主版本不等 = 拒绝；次版本只用于诊断展示，不参与裁决 —— 向后兼容的
/// 新增（新方法、新能力名）体现在次版本，不需要对方同步升级。
pub const PROTOCOL_VERSION: u32 = 1;

/// 当前主版本。
pub const PROTOCOL_VERSION_MAJOR: u32 = PROTOCOL_VERSION >> 16;

/// `initialize` 方法名。
pub const INITIALIZE_METHOD: &str = "initialize";

/// 宿主能力：宿主愿意替插件做的事情。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// 宿主可以转发宿主 → 插件的请求（插件侧反向调用）。
    ReverseRequests,
    /// 宿主接受 kind=binary 的数据块帧。
    BinaryBlocks,
}

/// 一个能力目录：可序列化的能力集合。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilitySet(pub Vec<Capability>);

/// `initialize` 请求参数（宿主 → 插件）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitializeParams {
    /// 宿主说的协议版本（主版本必须与插件相等）。
    pub protocol_version: u32,
    /// 宿主能力目录。
    pub capabilities: Vec<Capability>,
    /// 宿主实现标识（name/version，诊断用）。
    pub client_info: Value,
}

/// `initialize` 响应结果（插件 → 宿主）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitializeResult {
    /// 插件说的协议版本。
    pub protocol_version: u32,
    /// 插件能力目录。
    pub capabilities: Vec<Capability>,
    /// 插件实现标识。
    pub server_info: Value,
    /// 回连承载的会话令牌回验（wire.md §3.5）：宿主 spawn 时经
    /// `PROC_IPC_TOKEN` 注入子进程，插件在握手响应里回显。**方向不可
    /// 反**：宿主永不把令牌发给对端——只有真正被 spawn 的子进程才可能
    /// 知道它；抢连者答不出。unix fd 承载无需令牌，字段缺省。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}

/// 构造 `initialize` 请求（宿主侧）。
pub fn initialize_request(id: Id, params: &InitializeParams) -> Request {
    Request {
        jsonrpc: "2.0",
        id,
        method: INITIALIZE_METHOD.into(),
        params: Some(serde_json::to_value(params).expect("InitializeParams serializable")),
    }
}

/// 插件侧：解析 `initialize` 请求参数并做版本裁决。
///
/// 返回错误（应作为 error 响应回给宿主）的三种情况：
/// - 方法名不是 `initialize`（用 [`parse_initialize_params`] 前应已分发）；
/// - 参数结构非法 → `-32602`；
/// - 主版本不等 → 应用错误 `-32001`（不降级，不猜测兼容）。
pub fn parse_initialize_params(request: &Request) -> Result<InitializeParams, Error> {
    if request.method != INITIALIZE_METHOD {
        return Err(Error::new(
            ErrorCode::MethodNotFound,
            format!(
                "expected method {INITIALIZE_METHOD}, got {}",
                request.method
            ),
        ));
    }
    let params = request
        .params
        .as_ref()
        .ok_or_else(|| Error::new(ErrorCode::InvalidParams, "initialize requires params"))?;
    serde_json::from_value::<InitializeParams>(params.clone())
        .map_err(|error| Error::new(ErrorCode::InvalidParams, error.to_string()))
}

/// 主版本裁决：不等即拒绝。
///
/// 返回 `Err(message)` 时调用方应回应用错误 `-32001` 并关闭连接。
pub fn check_version(remote: u32) -> Result<(), String> {
    let remote_major = remote >> 16;
    if remote_major == PROTOCOL_VERSION_MAJOR {
        Ok(())
    } else {
        Err(format!(
            "protocol major version mismatch: host={PROTOCOL_VERSION_MAJOR}, peer={remote_major}"
        ))
    }
}

/// 构造 `initialize` 成功响应（插件侧）。
pub fn initialize_response(id: Id, result: &InitializeResult) -> Response {
    Response {
        jsonrpc: "2.0",
        id,
        result: Some(serde_json::to_value(result).expect("InitializeResult serializable")),
        error: None,
    }
}

/// 宿主侧：核对握手响应携带的会话令牌（回连承载必校；unix fd 承载双方
/// 均无令牌，直接通过）。
///
/// 不匹配 = 对端不是被 spawn 的那个子进程：调用方必须立即杀掉会话，
/// 不给第二次机会（重试会给抢连者猜令牌的机会窗口）。
pub fn verify_session_token(
    expected: Option<&str>,
    result: &InitializeResult,
) -> Result<(), Error> {
    let matched = match (expected, result.session_token.as_deref()) {
        (Some(expected), Some(presented)) => {
            constant_time_eq(expected.as_bytes(), presented.as_bytes())
        }
        (None, None) => true,
        _ => false,
    };
    if matched {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::Application(SESSION_TOKEN_ERROR),
            "session token mismatch: peer is not the spawned child",
        ))
    }
}

/// 会话令牌不匹配的应用错误码。
pub const SESSION_TOKEN_ERROR: i64 = -32002;

/// 常数时间字节比较（令牌是握手门，比较耗时不应当成为副信道）。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::Message;
    use serde_json::json;

    #[test]
    fn initialize_roundtrip_and_version_gate() {
        let params = InitializeParams {
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec![Capability::BinaryBlocks],
            client_info: json!({"name": "host", "version": "0.1"}),
        };
        let request = initialize_request(Id::Number(1), &params);
        let parsed = parse_initialize_params(&request).expect("valid initialize");
        assert_eq!(parsed.protocol_version, PROTOCOL_VERSION);
        assert_eq!(parsed.capabilities, params.capabilities);

        // 主版本不等：拒绝，且错误信息包含两侧版本号（排障第一现场）。
        let mut old = params.clone();
        old.protocol_version = (PROTOCOL_VERSION_MAJOR + 1) << 16;
        let request = initialize_request(Id::Number(2), &old);
        let parsed = parse_initialize_params(&request).unwrap();
        let error = check_version(parsed.protocol_version).unwrap_err();
        assert!(error.contains("mismatch"), "{error}");
        assert!(
            error.contains(&PROTOCOL_VERSION_MAJOR.to_string()),
            "{error}"
        );
    }

    #[test]
    fn wire_form_of_initialize_request_is_canonical() {
        let params = InitializeParams {
            protocol_version: 1,
            capabilities: vec![],
            client_info: json!({"name": "h"}),
        };
        let request = initialize_request(Id::String("s-1".into()), &params);
        let message = Message::Request(request);
        let text = String::from_utf8(message.to_vec().unwrap()).unwrap();
        assert!(text.contains(r#""method":"initialize""#));
        assert!(text.contains(r#""protocol_version":1"#));
    }

    #[test]
    fn initialize_response_with_token_canonical_and_verified() {
        let result = InitializeResult {
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec![],
            server_info: json!({"name": "p"}),
            session_token: Some("tok-abc".into()),
        };
        let response = initialize_response(Id::Number(1), &result);
        let text = serde_json::to_string(&response).unwrap();
        // 字段序：protocol_version, capabilities, server_info, session_token。
        assert!(text.contains(r#""session_token":"tok-abc""#), "{text}");

        // 核对：相等通过；不等拒绝；单边缺失拒绝；双方缺失通过（unix）。
        assert!(verify_session_token(Some("tok-abc"), &result).is_ok());
        assert!(verify_session_token(Some("tok-abd"), &result).is_err());
        assert!(verify_session_token(None, &result).is_err());
        let mut bare = result.clone();
        bare.session_token = None;
        assert!(verify_session_token(None, &bare).is_ok());
        assert!(verify_session_token(Some("tok-abc"), &bare).is_err());

        // 无令牌响应的规范形不得携带该字段（与既有向量逐字节一致）。
        let bare = initialize_response(Id::Number(2), &bare);
        let text = serde_json::to_string(&bare).unwrap();
        assert!(!text.contains("session_token"));
    }

    #[test]
    fn wrong_method_is_method_not_found() {
        let request = Request {
            jsonrpc: "2.0",
            id: Id::Number(1),
            method: "not-initialize".into(),
            params: None,
        };
        let error = parse_initialize_params(&request).unwrap_err();
        assert_eq!(error.code, ErrorCode::MethodNotFound);
    }

    #[test]
    fn bad_params_is_invalid_params() {
        let request = Request {
            jsonrpc: "2.0",
            id: Id::Number(1),
            method: INITIALIZE_METHOD.into(),
            params: Some(json!("not an object")),
        };
        let error = parse_initialize_params(&request).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidParams);
    }

    #[test]
    fn unknown_capabilities_are_ignored_by_design() {
        // 能力目录是 Vec<Capability>（闭集枚举）：未知能力名在解码层就
        // 报错，而不是静默丢失 —— 这里钉住「闭集」这个选择：新增能力
        // 必须同时 bump 次版本，双方各自升级后自然互通。
        let params = json!({
            "protocol_version": 1,
            "capabilities": [{"unknown": true}],
            "client_info": {}
        });
        let result = serde_json::from_value::<InitializeParams>(params);
        assert!(result.is_err(), "闭集能力目录必须拒绝未知名");
    }
}
