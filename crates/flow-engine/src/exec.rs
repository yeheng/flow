//! Shared pure node request/output helpers used by V2 execution ports.
pub const DEFAULT_JS_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_HARNESS_TIMEOUT_MS: u64 = 300_000;
use serde_json::Value;
/// 节点执行失败。`retryable` 决定引擎是重试还是把 run 判为失败；
/// `platform` 表示基础设施故障（IO/Backend/日志损坏）——不是工作流失败，
/// 引擎遇此标志挂起 run（awaiting_resume）而不是写 run_failed（DESIGN §7）。
#[derive(Debug, Clone, PartialEq)]
pub struct NodeFailure {
    pub message: String,
    pub retryable: bool,
    pub platform: bool,
}

impl NodeFailure {
    pub fn retryable(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: true,
            platform: false,
        }
    }

    pub fn fatal(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: false,
            platform: false,
        }
    }

    /// 基础设施故障：不重试、不判死 run，交回引擎挂起等待恢复。
    pub fn platform(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: false,
            platform: true,
        }
    }
}

pub fn singular_or_map(mut entries: Vec<(String, Value)>) -> Value {
    if entries.len() == 1 {
        let (_, value) = entries.pop().unwrap();
        return value;
    }
    Value::Object(entries.into_iter().collect())
}

pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

pub type HarnessRequest = (String, Vec<String>, Option<String>, u64, String);
pub fn harness_request(params: &Value) -> Result<HarnessRequest, NodeFailure> {
    let need = |key: &str| {
        params
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| NodeFailure::fatal(format!("harness 节点缺少参数 {key}")))
    };
    let command = need("command")?;
    let prompt = params
        .get("prompt")
        .and_then(Value::as_str)
        .ok_or_else(|| NodeFailure::fatal("harness 节点缺少参数 prompt"))?
        .to_string();
    let args = params
        .get("args")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    let workdir = params
        .get("workdir")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    let timeout_ms = params
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_HARNESS_TIMEOUT_MS);
    Ok((command, args, workdir, timeout_ms, prompt))
}

pub fn harness_output(
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
) -> Result<Value, NodeFailure> {
    if exit_code != Some(0) {
        let code = exit_code.map_or_else(|| "signal".to_string(), |c| c.to_string());
        return Err(NodeFailure::fatal(format!(
            "harness 退出码 {code}，stderr 尾部：{}",
            stderr_tail(&stderr)
        )));
    }
    // stdout 去空白后恰为 JSON 对象/数组时附上解析结果，方便下游直接取数
    let trimmed = stdout.trim();
    let result = if trimmed.starts_with('{') || trimmed.starts_with('[') {
        serde_json::from_str::<Value>(trimmed)
            .ok()
            .filter(|v| v.is_object() || v.is_array())
    } else {
        None
    };
    let mut output = serde_json::json!({
        "exit_code": 0,
        "stdout": stdout,
        "stderr": stderr,
    });
    if let Some(result) = result {
        output["result"] = result;
    }
    Ok(output)
}

pub fn stderr_tail(stderr: &str) -> String {
    if stderr.len() <= 512 {
        return stderr.to_string();
    }
    let mut start = stderr.len() - 512;
    while !stderr.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &stderr[start..])
}

pub fn harness_command(
    command: &str,
    args: &[String],
    workdir: Option<&str>,
) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(command);
    cmd.args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(dir) = workdir {
        cmd.current_dir(dir);
    }
    cmd
}

pub fn email_request(params: &Value) -> Result<(String, Value), NodeFailure> {
    let need = |key: &str| {
        params
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| NodeFailure::fatal(format!("email 节点缺少参数 {key}")))
    };
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
    use crate::expr;
    use serde_json::json;
    use std::time::Duration;
    #[test]
    fn json_truthiness_matches_javascript() {
        for value in [
            json!(null),
            json!(false),
            json!(true),
            json!(0),
            json!(-1),
            json!(""),
            json!("false"),
            json!("0"),
            json!([]),
            json!({}),
            json!([0]),
            json!({"x": false}),
        ] {
            let expected = expr::eval_expr(
                "Boolean(input)",
                &value,
                &Value::Null,
                Duration::from_secs(1),
            )
            .unwrap();
            assert_eq!(json!(truthy(&value)), expected, "{value}");
        }
    }
    #[test]
    fn harness_request_builds_spawn_spec() {
        let (command, args, workdir, timeout_ms, prompt) = harness_request(&json!({
            "command": "kimi", "prompt": "你好"
        }))
        .unwrap();
        assert_eq!(command, "kimi");
        assert_eq!(args, Vec::<String>::new());
        assert_eq!(workdir, None);
        assert_eq!(timeout_ms, DEFAULT_HARNESS_TIMEOUT_MS);
        assert_eq!(prompt, "你好");

        let (command, args, workdir, timeout_ms, _) = harness_request(&json!({
            "command": "claude", "prompt": "p",
            "args": ["--model", "m", 42],
            "workdir": "/tmp", "timeout_ms": 1000
        }))
        .unwrap();
        assert_eq!(command, "claude");
        assert_eq!(
            args,
            vec!["--model".to_string(), "m".to_string(), "42".to_string()]
        );
        assert_eq!(workdir, Some("/tmp".to_string()));
        assert_eq!(timeout_ms, 1000);

        // 缺 command / prompt：fatal
        for params in [json!({"prompt": "p"}), json!({"command": "kimi"})] {
            let err = harness_request(&params).unwrap_err();
            assert!(!err.retryable, "{}", err.message);
        }
    }
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
    #[test]
    fn passes_single_and_maps_multiple() {
        assert_eq!(
            singular_or_map(vec![("e1".into(), json!(7))]),
            json!(7),
            "单来源必须透传其值"
        );
        assert_eq!(
            singular_or_map(vec![("e1".into(), json!(7)), ("e2".into(), Value::Null)],),
            json!({"e1": 7, "e2": null}),
            "多来源必须组成映射，缺失补 null"
        );
    }
}
