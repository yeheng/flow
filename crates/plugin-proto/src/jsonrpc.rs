//! JSON-RPC 2.0 子集：请求 / 响应 / 通知，明确禁用 batch。
//!
//! 为什么是子集而不是全量实现：
//!
//! - **禁用 batch**（顶层数组）：规范允许，但各实现的批次语义分歧大
//!   （部分失败怎么回、乱序允不允许），而插件协议的场景里没有收益。
//!   顶层数组按非法消息处理（`-32600`）。
//! - **id 只允许 string | number**（响应的 error 分支允许 null，对齐规范
//!   的「无法解析请求 id」场景）。布尔、对象做 id 直接拒绝。
//! - **未知方法回 `-32601`**，不静默丢弃 —— 静默丢弃会让「方法名拼错」
//!   变成「插件没反应」，两边各查半天。
//!
//! 序列化是**规范形**（canonical）：字段顺序 `jsonrpc, id, method, params`
//! / `jsonrpc, id, result` / `jsonrpc, id, error`。解析侧接受任意合法 JSON
//! （字段顺序无关），编码侧恒出规范形 —— golden vectors 钉的就是规范形，
//! 其他语言的实现「必须能解析任意合法 JSON、应当输出规范形」。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ProtoError;

/// 应用错误码段的起点（宿主与插件自定义错误用 `-32001..=-32099`）。
///
/// 规范保留 `-32000..=-32099` 给实现定义；`-32000` 留空不用，避免与
/// 某些服务器错误「server error」惯例撞车。
pub const APP_ERROR_RANGE_START: i64 = -32001;

/// JSON-RPC 2.0 标准错误码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// -32700：JSON 语法非法。
    ParseError,
    /// -32600：结构非法（含 batch、未知字段组合、非法 id 类型）。
    InvalidRequest,
    /// -32601：方法不存在。
    MethodNotFound,
    /// -32602：参数非法。
    InvalidParams,
    /// -32603：内部错误。
    InternalError,
    /// 应用自定义码（`-32001..=-32099`）。
    Application(i64),
}

impl ErrorCode {
    /// 数值码。
    pub fn code(&self) -> i64 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
            Self::Application(code) => *code,
        }
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.code())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let code = i64::deserialize(deserializer)?;
        match code {
            -32700 => Ok(Self::ParseError),
            -32600 => Ok(Self::InvalidRequest),
            -32601 => Ok(Self::MethodNotFound),
            -32602 => Ok(Self::InvalidParams),
            -32603 => Ok(Self::InternalError),
            -32099..=-32001 => Ok(Self::Application(code)),
            _ => Err(serde::de::Error::custom(format!(
                "reserved or unknown error code {code}"
            ))),
        }
    }
}

/// 请求 id：string 或 number（error 响应里可为 null）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    /// 字符串 id。
    String(String),
    /// 数值 id。
    Number(i64),
    /// 空 id：只允许出现在 error 响应（请求无法解析时的兜底）。
    Null,
}

/// JSON-RPC 错误对象。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    /// 错误码。
    pub code: ErrorCode,
    /// 人读信息。
    pub message: String,
    /// 可选结构化附加上下文。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Error {
    /// 构造错误对象。
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// 附上结构化数据。
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

/// 请求（期待响应）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// 恒为 `"2.0"`。
    pub jsonrpc: &'static str,
    /// 请求 id。
    pub id: Id,
    /// 方法名。
    pub method: String,
    /// 参数（对象或数组；可省）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// 通知（不期待响应）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notification {
    /// 恒为 `"2.0"`。
    pub jsonrpc: &'static str,
    /// 方法名。
    pub method: String,
    /// 参数。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// 响应（result 与 error 恰居其一）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    /// 恒为 `"2.0"`。
    pub jsonrpc: &'static str,
    /// 对应请求的 id（无法解析请求时为 null 且必有 error）。
    pub id: Id,
    /// 成功结果。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// 错误。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

/// 一条 JSON-RPC 消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// 请求。
    Request(Request),
    /// 通知。
    Notification(Notification),
    /// 响应。
    Response(Response),
}

impl Message {
    /// 从字节切片解析（拒绝 batch 与非法结构）。
    pub fn from_slice(payload: &[u8]) -> Result<Self, ProtoError> {
        let value = serde_json::from_slice::<Value>(payload)?;
        Self::from_value(value)
    }

    /// 从 JSON 值解析并做结构校验。
    pub fn from_value(value: Value) -> Result<Self, ProtoError> {
        let invalid = |detail: &str| ProtoError::InvalidMessage(detail.to_string());
        let object = value
            .as_object()
            .ok_or_else(|| invalid("top level must be an object (batch is disabled)"))?;
        match object.get("jsonrpc").and_then(Value::as_str) {
            Some("2.0") => {}
            _ => return Err(invalid(r#"jsonrpc must be "2.0""#)),
        }
        if object.contains_key("method") {
            let method = object["method"]
                .as_str()
                .filter(|m| !m.is_empty())
                .ok_or_else(|| invalid("method must be a non-empty string"))?
                .to_string();
            let params = object.get("params").cloned();
            if let Some(id) = object.get("id") {
                let id =
                    Id::from_value(id).ok_or_else(|| invalid("id must be string or number"))?;
                return Ok(Self::Request(Request {
                    jsonrpc: "2.0",
                    id,
                    method,
                    params,
                }));
            }
            return Ok(Self::Notification(Notification {
                jsonrpc: "2.0",
                method,
                params,
            }));
        }
        let has_result = object.contains_key("result");
        let has_error = object.contains_key("error");
        match (has_result, has_error) {
            // result 与 error 恰居其一；两个都给 = 结构非法。
            (true, false) | (false, true) => {}
            _ => return Err(invalid("response must carry exactly one of result / error")),
        }
        let id = match object.get("id") {
            Some(id) => {
                Id::from_value(id).ok_or_else(|| invalid("id must be string, number, or null"))?
            }
            None => return Err(invalid("response must carry id")),
        };
        if has_error && id == Id::Null {
            // 合法：请求无法解析时的规范兜底形态。
        } else if id == Id::Null {
            return Err(invalid("null id is only valid on error responses"));
        }
        Ok(Self::Response(Response {
            jsonrpc: "2.0",
            id,
            result: if has_result {
                object.get("result").cloned()
            } else {
                None
            },
            error: if has_error {
                let error =
                    serde_json::from_value(object["error"].clone()).map_err(ProtoError::from)?;
                Some(error)
            } else {
                None
            },
        }))
    }

    /// 序列化为规范形 JSON 字节。
    pub fn to_vec(&self) -> Result<Vec<u8>, ProtoError> {
        match self {
            Self::Request(request) => serde_json::to_vec(request),
            Self::Notification(notification) => serde_json::to_vec(notification),
            Self::Response(response) => serde_json::to_vec(response),
        }
        .map_err(ProtoError::from)
    }
}

impl Id {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::String(text) => Some(Self::String(text.clone())),
            Value::Number(number) => number.as_i64().map(Self::Number),
            Value::Null => Some(Self::Null),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_request_notification_response() {
        let request = Message::from_value(json!({
            "jsonrpc": "2.0", "id": 7, "method": "ping", "params": {"a": 1}
        }))
        .unwrap();
        assert!(matches!(request, Message::Request(_)));

        let notification = Message::from_value(json!({
            "jsonrpc": "2.0", "method": "cancelled"
        }))
        .unwrap();
        assert!(matches!(notification, Message::Notification(_)));

        let response = Message::from_value(json!({
            "jsonrpc": "2.0", "id": 7, "result": "pong"
        }))
        .unwrap();
        assert!(matches!(response, Message::Response(_)));
    }

    #[test]
    fn rejects_batch_missing_version_and_bad_ids() {
        assert!(Message::from_value(json!([{"jsonrpc": "2.0"}])).is_err());
        assert!(Message::from_value(json!({"id": 1, "method": "x"})).is_err());
        assert!(Message::from_value(json!({"jsonrpc": "2.0", "id": true, "method": "x"})).is_err());
        assert!(Message::from_value(json!({"jsonrpc": "2.0", "id": 1})).is_err());
    }

    #[test]
    fn result_and_error_are_exclusive() {
        assert!(Message::from_value(json!({
            "jsonrpc": "2.0", "id": 1, "result": 1, "error": {"code": -32603, "message": "x"}
        }))
        .is_err());
        assert!(Message::from_value(json!({"jsonrpc": "2.0", "id": 1})).is_err());
        // null id 只在 error 响应上合法。
        assert!(Message::from_value(json!({
            "jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "x"}
        }))
        .is_ok());
        assert!(Message::from_value(json!({"jsonrpc": "2.0", "id": null, "result": 1})).is_err());
    }

    #[test]
    fn error_codes_roundtrip_and_reject_reserved() {
        for code in [
            ErrorCode::ParseError,
            ErrorCode::InvalidRequest,
            ErrorCode::MethodNotFound,
            ErrorCode::InvalidParams,
            ErrorCode::InternalError,
            ErrorCode::Application(APP_ERROR_RANGE_START),
        ] {
            let json = serde_json::to_value(code).unwrap();
            assert_eq!(json, serde_json::json!(code.code()));
            assert_eq!(serde_json::from_value::<ErrorCode>(json).unwrap(), code);
        }
        assert!(serde_json::from_value::<ErrorCode>(json!(-31999)).is_err());
        assert!(serde_json::from_value::<ErrorCode>(json!(-32100)).is_err());
    }

    #[test]
    fn canonical_field_order_is_stable() {
        let request = Message::Request(Request {
            jsonrpc: "2.0",
            id: Id::Number(7),
            method: "ping".into(),
            params: Some(json!({"a": 1})),
        });
        let text = String::from_utf8(request.to_vec().unwrap()).unwrap();
        assert!(text.starts_with(r#"{"jsonrpc":"2.0","id":7,"method":"ping""#));
    }
}
