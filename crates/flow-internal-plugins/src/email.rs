//! email 节点请求构造的纯实现（Resend 兼容形态 → 统一 HTTP 请求）。
//!
//! 自 flow-engine::exec 迁入（T4 第二批）：构造与校验在这里，授权与
//! 出站执行在 flow-plugin-runtime。

use serde_json::Value;

use crate::command::NodeFailure;

/// 从节点 params 构造 Resend 兼容的邮件请求（端点 + 请求体）。
///
/// 请求体只含 `from`/`to`/`subject`/`text` 四个字段——密钥不进 body，
/// 它走 Authorization 头，由执行器在授权后注入。
///
/// # Errors
/// 缺少 `from`/`to`/`subject`/`body` 时返回 `NodeFailure::fatal`。
pub fn email_request(params: &Value) -> Result<(String, Value), NodeFailure> {
    let need = |key: &str| {
        params
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| NodeFailure::fatal(format!("email 节点缺少参数 {key}")))
    };
    // 端点可覆盖（自建 Resend 兼容服务），缺省用官方端点。
    let endpoint = params
        .get("endpoint")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("https://api.resend.com/emails")
        .to_string();
    let body = serde_json::json!({
        "from": need("from")?,
        "to": need("to")?,
        "subject": need("subject")?,
        "text": need("body")?,
    });
    Ok((endpoint, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn email_request_builds_resend_body() {
        let (url, body) = email_request(&json!({
            "api_key": "k", "from": "a@b.c", "to": "d@e.f", "subject": "s", "body": "正文"
        }))
        .unwrap();
        assert_eq!(url, "https://api.resend.com/emails");
        assert_eq!(
            body,
            json!({"from": "a@b.c", "to": "d@e.f", "subject": "s", "text": "正文"})
        );

        let err = email_request(&json!({"api_key": "k", "from": "a@b.c"})).unwrap_err();
        assert!(
            !err.retryable && err.message.contains("to"),
            "{}",
            err.message
        );
    }
}
