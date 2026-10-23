//! Shared pure node request/output helpers used by V2 execution ports.
pub const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_HARNESS_TIMEOUT_MS: u64 = 300_000;

/// 节点级超时的 clamp 上界。`timeout_ms` 是用户可配参数，不设上限时一个
/// 巨大值会让执行槽位被无限占用（本地/远程都没有对应的 wall-clock 预算，
/// 只有取消能救）。上界取「远超任何合理集成调用」的量级：正常用例应远
/// 低于它，配超了说明是配置错误，按配置失败比静默挂死可诊断。
pub const HTTP_TIMEOUT_MAX_MS: u64 = 10 * 60 * 1000;
pub const HARNESS_TIMEOUT_MAX_MS: u64 = 60 * 60 * 1000;
use serde_json::Value;
/// 节点执行失败。`retryable` 决定引擎是重试还是把 run 判为失败；
/// `platform` 表示基础设施故障（IO/Backend/日志损坏）——不是工作流失败，
/// 引擎遇此标志挂起 run（awaiting_resume）而不是写 run_failed（DESIGN.md §13.3）。
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
    // 空输入返回 null（DESIGN §9.2：无 end 或全无输出 → null），不是空
    // 对象——`{}` 是「有一个空对象输出」，语义不同。
    if entries.is_empty() {
        return Value::Null;
    }
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
    // 附上解析结果，方便下游直接取数
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

// ---- harness agent 模式（mode=agent，内置 flow-harness-agent runner）----

/// agent 模式的节点参数（模板展开后）。`workdir` 可选：配置了才是 agent
/// 工作根（保存期校验字面量，存在性由 executor 授权前在本机验证——remote
/// 模式下 master 看不到远端路径）；缺省时 executor 为本次 run 建随机临时
/// 沙箱。缺省沙箱是 executor 本地事实，**不进请求/指纹**（请求记 null），
/// 与 runner 路径同一归属原则。
pub struct AgentParams {
    pub prompt: String,
    pub workdir: Option<String>,
    pub timeout_ms: u64,
    /// 密钥**名称**（`x-secret`）；真值经 permit → runner stdin。
    pub api_key: String,
}

pub fn agent_params(params: &Value) -> Result<AgentParams, NodeFailure> {
    let need = |key: &str| {
        params
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| NodeFailure::fatal(format!("harness agent 节点缺少参数 {key}")))
    };
    Ok(AgentParams {
        prompt: need("prompt")?,
        workdir: params
            .get("workdir")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string),
        api_key: need("api_key")?,
        timeout_ms: params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_HARNESS_TIMEOUT_MS),
    })
}

/// operation 请求（授权证据 + 回归指纹）。冻结配置整包进请求：改任何一项
/// 都让 fixture 失配——这正是快照要钉住的执行身份。只有密钥名；`workdir`
/// 未配置时记 null（缺省沙箱路径不进 run 身份，指纹保持确定）。
pub fn agent_request(params: &AgentParams, config: &flow_dto::AgentRunConfig) -> Value {
    serde_json::json!({
        "mode": "agent",
        "prompt": params.prompt,
        "workdir": params.workdir,
        "timeout_ms": params.timeout_ms,
        "agent": serde_json::to_value(config).unwrap_or(Value::Null),
        "credential": {"scheme": "bearer", "secret_ref": params.api_key},
    })
}

/// 冻结配置从 `_flow_execution.agent` 反序列化；缺失 = 未配置/旧 master。
pub fn frozen_agent_config(node: &Value) -> Result<Option<flow_dto::AgentRunConfig>, String> {
    match node.pointer("/_flow_execution/agent") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|e| format!("frozen agent config malformed: {e}")),
    }
}

/// agent 模式的 Outcome → 节点输出（恢复路径重跑同一派生）。与
/// [`harness_output`] 同构：非 0 退出码 = 业务失败（stderr 尾部带 runner
/// 的 error 行）；成功时附 `agent` 元数据。
pub fn agent_output(
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    agent: Value,
) -> Result<Value, NodeFailure> {
    if exit_code != Some(0) {
        let code = exit_code.map_or_else(|| "signal".to_string(), |c| c.to_string());
        return Err(NodeFailure::fatal(format!(
            "harness agent 退出码 {code}，stderr 尾部：{}",
            stderr_tail(&stderr)
        )));
    }
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
        "agent": agent,
    });
    if let Some(result) = result {
        output["result"] = result;
    }
    Ok(output)
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

#[cfg(test)]
mod agent_tests {
    use super::*;
    use serde_json::json;

    fn config() -> flow_dto::AgentRunConfig {
        flow_dto::AgentRunConfig {
            base_url: "https://api.example/v1".into(),
            model: "test-model".into(),
            api: "openai".into(),
            proxy: None,
            max_turns: Some(50),
            max_tool_calls_per_turn: None,
            max_tokens: Some(4096),
            tool_timeout_secs: None,
        }
    }

    fn params() -> AgentParams {
        AgentParams {
            prompt: "do the thing".into(),
            workdir: Some("/tmp/workdir".into()),
            api_key: "DEEPSEEK_KEY".into(),
            timeout_ms: 300_000,
        }
    }

    fn default_params() -> AgentParams {
        AgentParams {
            workdir: None,
            ..params()
        }
    }

    /// 回归指纹的稳定性契约：同一配置/参数序列化逐字节相等；改任何一项
    /// （模型/限额/prompt/workdir/密钥名/超时）都必须失配。缺省 workdir
    /// 记 null（临时沙箱路径不进 run 身份），指纹保持确定。
    #[test]
    fn agent_request_is_fingerprint_stable_and_sensitive() {
        let request = agent_request(&params(), &config());
        let baseline = serde_json::to_string(&request).unwrap();
        assert_eq!(
            serde_json::to_string(&agent_request(&params(), &config())).unwrap(),
            baseline
        );
        // 只有密钥名，绝无真值字段
        assert_eq!(request["credential"]["secret_ref"], "DEEPSEEK_KEY");
        let mut changed = config();
        changed.model = "other-model".into();
        assert_ne!(
            serde_json::to_string(&agent_request(&params(), &changed)).unwrap(),
            baseline
        );
        let mut changed = config();
        changed.max_tokens = Some(8192);
        assert_ne!(
            serde_json::to_string(&agent_request(&params(), &changed)).unwrap(),
            baseline
        );
        let mut changed = params();
        changed.prompt = "different".into();
        assert_ne!(
            serde_json::to_string(&agent_request(&changed, &config())).unwrap(),
            baseline
        );
        assert_eq!(
            agent_request(&default_params(), &config())["workdir"],
            Value::Null
        );
    }

    #[test]
    fn frozen_agent_config_reads_dispatch_injection() {
        let node = json!({"params": {"mode": "agent"},
                          "_flow_execution": {"agent": config()}});
        assert_eq!(frozen_agent_config(&node).unwrap(), Some(config()));
        assert_eq!(frozen_agent_config(&json!({"params": {}})).unwrap(), None);
        assert!(frozen_agent_config(&json!({"_flow_execution": {"agent": "garbage"}})).is_err());
    }

    /// agent_output 与 harness_output 同构：非 0 退出码 = 业务失败；成功附
    /// agent 元数据与 result 解析。
    #[test]
    fn agent_output_maps_exit_codes_and_metadata() {
        let meta = json!({"model": "m", "stop_reason": "end_turn", "elapsed_ms": 5});
        let ok = agent_output(Some(0), "{\"a\":1}".into(), String::new(), meta.clone()).unwrap();
        assert_eq!(ok["exit_code"], 0);
        assert_eq!(ok["stdout"], "{\"a\":1}");
        assert_eq!(ok["result"]["a"], 1);
        assert_eq!(ok["agent"], meta);

        let err = agent_output(Some(1), String::new(), "boom".into(), meta).unwrap_err();
        assert!(err.message.contains("1"), "{}", err.message);
        assert!(err.message.contains("boom"), "{}", err.message);
    }
}
