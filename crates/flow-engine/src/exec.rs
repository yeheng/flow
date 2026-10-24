//! Shared pure node request/output helpers used by V2 execution ports.
//!
//! 本模块提供节点执行器之间的**纯函数**工具：把节点 params 翻译成外部
//! 请求（HTTP / harness / email），把外部进程结果翻译成节点输出，并统一
//! 失败语义（`NodeFailure`）。
//!
//! 它守护的核心不变量是**失败分类**：每个失败都必须标明「可重试」「不可
//! 重试」还是「平台故障」。平台故障不是工作流失败——引擎遇此应挂起 run
//! 等恢复，而不是写 run_failed（DESIGN.md §13.3）。分类漏掉或错分，后果
//! 是把一次基础设施抖动永久记成业务失败。
//!
//! 不这样做的代价：同样的判断会在调度层、执行层各写一份，两份一旦漂移，
//! 「该重试的不重试（卡死）」与「不该重试的重试（重复副作用）」都会出现。
/// HTTP 节点的默认超时：多数集成调用在秒级完成，30s 足够覆盖慢接口，
/// 又不至于让一次挂死的调用长期占住执行槽位。
pub const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;
/// harness 节点的默认超时：外部 agent CLI 的冷启动可能到秒级，
/// 比 HTTP 宽裕得多。
pub const DEFAULT_HARNESS_TIMEOUT_MS: u64 = 300_000;

/// 节点级超时的 clamp 上界。`timeout_ms` 是用户可配参数，不设上限时一个
/// 巨大值会让执行槽位被无限占用（本地/远程都没有对应的 wall-clock 预算，
/// 只有取消能救）。上界取「远超任何合理集成调用」的量级：正常用例应远
/// 低于它，配超了说明是配置错误，按配置失败比静默挂死可诊断。
pub const HTTP_TIMEOUT_MAX_MS: u64 = 10 * 60 * 1000;
/// harness 超时的 clamp 上界：比 HTTP 宽一个量级（agent 可能长时间
/// 跑工具），但同样有上界——「无上限」只会把配置错误变成静默挂死。
pub const HARNESS_TIMEOUT_MAX_MS: u64 = 60 * 60 * 1000;
use serde_json::Value;
/// 节点执行失败。`retryable` 决定引擎是重试还是把 run 判为失败；
/// `platform` 表示基础设施故障（IO/Backend/日志损坏）——不是工作流失败，
/// 引擎遇此标志挂起 run（awaiting_resume）而不是写 run_failed（DESIGN.md §13.3）。
///
/// 三个字段是**互斥的判定轴**，构造时必须用 `retryable` / `fatal` /
/// `platform` 三个工厂之一，不要直接手写字面量：手写容易漏填 `platform`，
/// 而漏填的后果是平台故障被记成业务失败，run 永久判死。
#[derive(Debug, Clone, PartialEq)]
pub struct NodeFailure {
    /// 失败原因文本（直接展示给用户/写入节点失败证据）。
    pub message: String,
    /// 是否属于可自动重试的失败（如临时网络抖动）。
    pub retryable: bool,
    /// 是否属于平台故障面（应挂起 run 等恢复，而非判死）。
    pub platform: bool,
}

impl NodeFailure {
    /// 构造一个可重试的业务失败。
    ///
    /// 只用于「重做一次大概率成功」的场景（如临时网络错误）。对有外部
    /// 副作用的节点，重试必须先取得显式 repeatable 授权，不能靠这个标志。
    pub fn retryable(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: true,
            platform: false,
        }
    }

    /// 构造一个不可重试的业务失败：run 应被判为失败。
    ///
    /// 参数缺失、目标不存在、用户脚本语法错误等都属于此类——重试不会
    /// 改变结果，只会浪费时间并制造噪声日志。
    pub fn fatal(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: false,
            platform: false,
        }
    }

    /// 基础设施故障：不重试、不判死 run，交回引擎挂起等待恢复。
    ///
    /// 用于 IO 错误、后端不可达、日志损坏这类「与用户定义无关」的故障。
    /// 把它错标成 `fatal` 就等于把一次可恢复的抖动永久记成业务失败。
    pub fn platform(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: false,
            platform: true,
        }
    }
}

/// 把若干来源的输出合成一个节点输出。
///
/// 形状规则（DESIGN §9.2）：**恰好一个来源直接透传其值**，多个来源组成
/// `map[来源 id → 值]`，空输入返回 `null`。
///
/// 关键在于形状恒定：多来源时的 map 不会因为某个来源缺失就退化成标量，
/// 下游读到的结构在整个 run 生命周期里保持稳定。
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

/// 按 JavaScript 真值语义判定一个 JSON 值。
///
/// 与 JS 的 `Boolean(x)` 严格对齐：空串为假、非空串（包括 `"false"`、
/// `"0"`）为真、数组与对象恒为真、空数组也真。
///
/// 必须与 JS 一致：条件节点的表达式在沙箱里求值，若宿主侧判定口径不同，
/// 同一份数据会在两处得到相反结论，且这种分歧只在边界值上出现。
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// harness（command 模式）的进程规格：命令、参数、工作目录、超时、提示词。
///
/// 用元组而非结构体：它只在「拼装 → 发起」之间传递一次，且 executor
/// 侧有自己的进程结构，不值得引入跨 crate 的类型耦合。
pub type HarnessRequest = (String, Vec<String>, Option<String>, u64, String);

/// 从节点 params 解析出 harness command 模式的进程规格。
///
/// # Errors
/// 缺少 `command` 或 `prompt`（或为空白）时返回 `NodeFailure::fatal`：
/// 这是定义缺参，重试无意义。
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
                    // 非字符串参数按其 JSON 字面量转字符串：命令行参数
                    // 本就只有字符串形态，给出数字 42 时用户想要的是 "42"。
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

/// 把 harness 进程的执行结果翻译成节点输出。
///
/// 非 0 退出码一律是业务失败：外部 CLI 的退出码语义由它自己定义，
/// 引擎不替它猜「哪些退出码可以忽略」。
///
/// # Errors
/// 退出码非 0（或进程被信号杀死）时返回 `NodeFailure::fatal`，错误文本
/// 附带 stderr 尾部，便于定位。
pub fn harness_output(
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
) -> Result<Value, NodeFailure> {
    if exit_code != Some(0) {
        // exit_code 为 None 表示被信号杀死（没有正常退出码），用
        // "signal" 标注，区别于具体退出码。
        let code = exit_code.map_or_else(|| "signal".to_string(), |c| c.to_string());
        return Err(NodeFailure::fatal(format!(
            "harness 退出码 {code}，stderr 尾部：{}",
            stderr_tail(&stderr)
        )));
    }
    // 附上解析结果，方便下游直接取数
    let trimmed = stdout.trim();
    // 只在 stdout 是 JSON 对象/数组时才解析：agent CLI 常在 JSON 前后
    // 混入日志行，强行解析会得到误导性的部分结果。
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

/// 取 stderr 末尾最多 512 字节用于报错展示。
///
/// 截尾而非截头：崩溃现场的诊断信息通常在最后（panic 栈、错误摘要）。
/// 边界处理保证不切在 UTF-8 字符中间——否则会产生非法字符串，
/// 一路带到日志与 JSON 响应里。
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

/// 构造 harness 子进程命令。
///
/// 三个管道全开：`prompt` 经 stdin 传入、stdout 作为节点输出、stderr 用于
/// 失败诊断。`kill_on_drop(true)` 保证执行器崩溃或任务被取消时子进程
/// 不会变成孤儿继续占用机器。
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

// ---- harness agent 模式（mode=agent，内置 flow-harness-agent runner）----

/// agent 模式的节点参数（模板展开后）。`workdir` 可选：配置了才是 agent
/// 工作根（保存期校验字面量，存在性由 executor 授权前在本机验证——remote
/// 模式下 master 看不到远端路径）；缺省时 executor 为本次 run 建随机临时
/// 沙箱。缺省沙箱是 executor 本地事实，**不进请求/指纹**（请求记 null），
/// 与 runner 路径同一归属原则。
pub struct AgentParams {
    /// 提示词正文（模板展开后的最终值）。
    pub prompt: String,
    /// agent 工作根；`None` 表示「由 executor 建临时沙箱」。
    pub workdir: Option<String>,
    /// 操作截止时间（毫秒）。
    pub timeout_ms: u64,
    /// 密钥**名称**（`x-secret`）；真值经 permit → runner stdin。
    pub api_key: String,
}

/// 从节点 params 解析 agent 模式的参数。
///
/// # Errors
/// 缺少 `prompt` 或 `api_key`（或为空白）时返回 `NodeFailure::fatal`。
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
///
/// 指纹必须**逐字节稳定**：回归比对靠它把新请求与录制基线对上，
/// 请求里多一个无意义的字段（如临时目录路径）就会让每次回归都失配。
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
///
/// # Errors
/// 字段存在但结构不合法时返回错误文本：静默忽略会让节点在运行期拿不到
/// 模型配置，报错信息还指向别处（误导性 Business 失败）。
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
///
/// # Errors
/// 退出码非 0（或被信号杀死）时返回 `NodeFailure::fatal`。
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
    /// 宿主侧真值判定必须与 JS 的 `Boolean(x)` 逐值一致，
    /// 否则同一份数据在沙箱内外会得到相反结论。
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
    /// harness command 模式的参数装配：缺省值、类型强转与缺参失败。
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
    /// 邮件请求体必须是 Resend 兼容的四字段形状，且密钥不进 body。
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
    /// 单来源透传、多来源成映射：输出形状必须恒定。
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

    /// 构造一份基准 agent 配置：模型、限额等回归身份字段。
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

    /// 基准 agent 参数：显式 workdir、密钥名与超时。
    fn params() -> AgentParams {
        AgentParams {
            prompt: "do the thing".into(),
            workdir: Some("/tmp/workdir".into()),
            api_key: "DEEPSEEK_KEY".into(),
            timeout_ms: 300_000,
        }
    }

    /// 缺省 workdir 的参数形态：请求里记 null，指纹保持确定。
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

    /// 冻结配置从派发注入里读取：缺失为 None，结构非法必须报错。
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
