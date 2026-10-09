use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::child_run::{ChildRunLauncher, ChildRunOutcome, MAX_SUB_WORKFLOW_DEPTH};
use crate::error::EngineError;
use crate::expr;
use crate::model::{Node, NodeType, HTTP_METHODS};
use crate::nodelog::{redact_url, redact_value, NodeLogger};
use crate::secrets;

pub const DEFAULT_JS_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_HARNESS_TIMEOUT_MS: u64 = 300_000;

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

impl From<EngineError> for NodeFailure {
    fn from(err: EngineError) -> NodeFailure {
        if err.is_platform_fault() {
            NodeFailure::platform(err.to_string())
        } else {
            NodeFailure::fatal(err.to_string())
        }
    }
}

#[derive(Clone)]
pub struct NodeExecContext {
    pub node: Node,
    pub input: Value,
    /// 前驱节点输出快照（决定论输入）
    pub outputs: HashMap<String, Value>,
    pub preds: Vec<String>,
    /// 本 run 的嵌套深度（根 run 为 0）；仅 sub_workflow 使用
    pub depth: u32,
    /// sub_workflow：已随 node_started 落盘的确定性子 run id 与启动器
    pub child: Option<ChildRunSpec>,
    /// 节点日志发射器（发射即忘，不阻塞执行）
    pub logger: NodeLogger,
}

#[derive(Clone)]
pub struct ChildRunSpec {
    pub child_run_id: String,
    pub launcher: Arc<dyn ChildRunLauncher>,
}

/// 前驱输出 → `nodes` 输入面（决定论快照，DESIGN §6、§10）。
fn outputs_value(outputs: &HashMap<String, Value>) -> Value {
    Value::Object(
        outputs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Map<String, Value>>(),
    )
}

/// params 里是否存在待展开的 `${`。决定论快速路径：无模板的节点原样
/// 返回，不为展开付一次 JS 运行时的成本。
fn contains_template(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains("${"),
        Value::Array(items) => items.iter().any(contains_template),
        Value::Object(map) => map.values().any(contains_template),
        _ => false,
    }
}

/// 执行前统一展开节点 params 的 `${}` 模板。返回 `None` 表示无模板、
/// 调用方直接沿用原 node（零克隆）。
///
/// 规则只有一条：除 `NodeType::opaque_params`（script.code / condition.expr
/// 等用户 JS 字段，其中的 `${}` 是 JS 模板字面量而非 flow 模板）外全部展开，
/// 与 http_call 历来共用 `expr::expand_templates`。展开的数据面与 script 的
/// `nodes` 完全一致——只读 run 输入与直接前驱输出快照，不扩大决定论边界。
///
/// 由 driver 的 start_node 在写 node_started 前调用（输入面快照随事件落盘），
/// 展开**恰好一次**：exec 层不再展开。数据里合法的 `${` 经二次展开会被损坏
/// （回归测试 `http_url_template_is_expanded_before_request`）。
pub(crate) async fn expand_params(
    node: &Node,
    input: &Value,
    outputs: &HashMap<String, Value>,
) -> Result<Option<Node>, NodeFailure> {
    if !contains_template(&node.params) {
        return Ok(None);
    }
    // 代码字段整体摘出、展开后原样放回：字节级不动
    let mut params = node.params.clone();
    let mut opaque: Vec<(String, Value)> = Vec::new();
    if let Value::Object(ref mut map) = params {
        for key in node.kind().map(NodeType::opaque_params).unwrap_or_default() {
            if let Some(value) = map.remove(key) {
                opaque.push((key.to_string(), value));
            }
        }
    }
    if !contains_template(&params) {
        // 模板只出现在代码字段里：还原后原样返回
        if let Value::Object(ref mut map) = params {
            map.extend(opaque);
        }
        return Ok(None);
    }
    let nodes = outputs_value(outputs);
    let expanded = tokio::task::spawn_blocking({
        let params = params.clone();
        let input = input.clone();
        move || {
            expr::expand_templates_bounded(
                &params,
                &input,
                &nodes,
                // 展开恒用默认脚本超时：timeout_ms 在各类型上只表示该类型
                // 自己的执行超时（http_call 的 HTTP 超时等），不承担第二语义
                Duration::from_millis(DEFAULT_JS_TIMEOUT_MS),
            )
        }
    })
    .await
    .map_err(|e| NodeFailure::fatal(format!("参数展开任务异常：{e}")))?
    .map_err(NodeFailure::from)?;
    let mut expanded = expanded;
    if let Value::Object(ref mut map) = expanded {
        map.extend(opaque);
    }
    Ok(Some(Node {
        params: expanded,
        ..node.clone()
    }))
}

impl NodeExecContext {
    fn nodes_value(&self) -> Value {
        outputs_value(&self.outputs)
    }

    fn js_timeout(&self) -> Duration {
        Duration::from_millis(
            self.node
                .param_u64("timeout_ms")
                .unwrap_or(DEFAULT_JS_TIMEOUT_MS),
        )
    }
}

pub async fn execute(
    ctx: &NodeExecContext,
    cancel: &CancellationToken,
) -> Result<Value, NodeFailure> {
    let kind = ctx
        .node
        .kind()
        .ok_or_else(|| NodeFailure::fatal(format!("未知节点类型：{}", ctx.node.node_type)))?;

    // params 已由 driver 的 start_node 统一展开（DESIGN §10，展开恰好一次）：
    // 双展开会把数据里合法的 ${ 再 evaluate 一遍，见
    // http_url_template_is_expanded_before_request。
    // x-secret 参数：dispatch 之前由名称解析为真值。真值不参与模板展开，也
    // 不进事件——node_started 在 driver 侧早已落盘，这里的 node 只是执行期
    // 的内存副本。
    if let Some(node) = secrets::resolve_node_secrets(&ctx.node).map_err(NodeFailure::fatal)? {
        let owned = NodeExecContext {
            node,
            ..ctx.clone()
        };
        dispatch(kind, &owned, cancel).await
    } else {
        dispatch(kind, ctx, cancel).await
    }
}

async fn dispatch(
    kind: NodeType,
    ctx: &NodeExecContext,
    cancel: &CancellationToken,
) -> Result<Value, NodeFailure> {
    match kind {
        NodeType::Start => Ok(ctx.input.clone()),
        NodeType::End => Ok(collect_end_output(ctx)),
        NodeType::Script => run_script(ctx).await,
        NodeType::Condition => run_condition(ctx).await,
        NodeType::Delay => run_delay(ctx, cancel).await,
        NodeType::HttpCall => run_http(ctx).await,
        NodeType::Harness => run_harness(ctx).await,
        NodeType::Email => run_email(ctx).await,
        NodeType::SubWorkflow => run_sub_workflow(ctx, cancel).await,
        NodeType::HumanTask => Err(NodeFailure::fatal(
            "human_task 由引擎等待信号驱动，不应直接执行",
        )),
    }
}

/// sub_workflow：启动子 run（幂等）并等待其终态，输出透传为节点输出。
/// 写序协议已由 Driver 保证：执行到这里时带 child_run_id 的 node_started 已落盘。
async fn run_sub_workflow(
    ctx: &NodeExecContext,
    cancel: &CancellationToken,
) -> Result<Value, NodeFailure> {
    let child = ctx
        .child
        .as_ref()
        .ok_or_else(|| NodeFailure::fatal("sub_workflow 未配置子 run 启动器"))?;
    let workflow_id = ctx
        .node
        .param_str("workflow_id")
        .ok_or_else(|| NodeFailure::fatal("sub_workflow 节点缺少 workflow_id 参数"))?
        .to_string();
    if ctx.depth >= MAX_SUB_WORKFLOW_DEPTH {
        return Err(NodeFailure::fatal(format!(
            "子工作流嵌套超过 {MAX_SUB_WORKFLOW_DEPTH} 层（疑似循环引用）"
        )));
    }
    // 子 run 输入：input_mapping（执行前已展开）优先；缺省时保持旧语义
    // ——父 run 输入快照（DESIGN §6.8，存量定义零行为变化）。
    // 映射的结果整体作为子 run 输入，不是与父输入合并。
    let child_input = match ctx.node.params.get("input_mapping") {
        Some(mapped) if !mapped.is_null() => mapped.clone(),
        _ => ctx.input.clone(),
    };
    // 子 run 输入 = 父 run 输入快照（或 input_mapping 展开结果）
    match child
        .launcher
        .start(
            &child.child_run_id,
            &workflow_id,
            child_input,
            ctx.depth + 1,
        )
        .await
    {
        Ok(()) => {}
        // 崩溃重放：子 run 已由崩溃前的同一 attempt 创建，附着等待即可
        Err(EngineError::RunExists(_)) => {}
        Err(err) if err.is_platform_fault() => {
            // 基础设施故障不当作工作流失败：挂起等恢复，不写 run_failed
            return Err(NodeFailure::platform(format!("启动子 run 失败：{err}")));
        }
        Err(err) => return Err(NodeFailure::retryable(format!("启动子 run 失败：{err}"))),
    }
    tokio::select! {
        _ = cancel.cancelled() => {
            child.launcher.cancel(&child.child_run_id).await;
            Err(NodeFailure::fatal("已取消"))
        }
        outcome = child.launcher.await_terminal(&child.child_run_id, cancel.clone()) => {
            match outcome {
                Ok(ChildRunOutcome::Succeeded(output)) => Ok(output),
                Ok(ChildRunOutcome::Failed(error)) => Err(NodeFailure::fatal(format!(
                    "子 run {} 失败：{error}",
                    child.child_run_id
                ))),
                Ok(ChildRunOutcome::Cancelled) => Err(NodeFailure::fatal(format!(
                    "子 run {} 已取消",
                    child.child_run_id
                ))),
                Err(err) if err.is_platform_fault() => Err(NodeFailure::platform(format!(
                    "等待子 run {} 终态失败：{err}",
                    child.child_run_id
                ))),
                Err(err) => Err(NodeFailure::retryable(format!(
                    "等待子 run {} 终态失败：{err}",
                    child.child_run_id
                ))),
            }
        }
    }
}

/// 单数透传、复数映射：end 节点输出与 run 最终输出共用同一条规则。
/// 单个来源直接透传其值；多个来源组成 {id: value} 映射，缺失的补 null。
pub fn singular_or_map(mut entries: Vec<(String, Value)>) -> Value {
    if entries.len() == 1 {
        let (_, value) = entries.pop().unwrap();
        return value;
    }
    Value::Object(entries.into_iter().collect())
}

fn collect_end_output(ctx: &NodeExecContext) -> Value {
    singular_or_map(
        ctx.preds
            .iter()
            .map(|p| {
                (
                    p.clone(),
                    ctx.outputs.get(p).cloned().unwrap_or(Value::Null),
                )
            })
            .collect(),
    )
}

async fn run_script(ctx: &NodeExecContext) -> Result<Value, NodeFailure> {
    let code = ctx
        .node
        .param_str("code")
        .ok_or_else(|| NodeFailure::fatal("script 节点缺少 code 参数"))?
        .to_string();
    let input = ctx.input.clone();
    let nodes = ctx.nodes_value();
    let timeout = ctx.js_timeout();
    let logger = ctx.logger.clone();
    // rquickjs 是同步 CPU 执行，必须放到阻塞线程池，避免占死 tokio worker
    let out = tokio::task::spawn_blocking(move || {
        expr::eval_body_bounded(&code, &input, &nodes, timeout, &logger)
    })
    .await
    .map_err(|e| NodeFailure::fatal(format!("脚本任务异常：{e}")));
    match out {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => {
            // 事件里只有 NodeFailed 的单行 error；日志里给完整异常细节
            ctx.logger.error(format!("脚本执行失败：{err}"));
            Err(NodeFailure::from(err))
        }
        Err(err) => Err(err),
    }
}

async fn run_condition(ctx: &NodeExecContext) -> Result<Value, NodeFailure> {
    let expression = ctx
        .node
        .param_str("expr")
        .ok_or_else(|| NodeFailure::fatal("condition 节点缺少 expr 参数"))?
        .to_string();
    let input = ctx.input.clone();
    let nodes = ctx.nodes_value();
    let timeout = ctx.js_timeout();
    let result = tokio::task::spawn_blocking(move || {
        expr::eval_expr_bounded(&expression, &input, &nodes, timeout)
    })
    .await
    .map_err(|e| NodeFailure::fatal(format!("条件求值任务异常：{e}")))?
    .map_err(NodeFailure::from);
    match &result {
        // 分支走向从日志一目了然：求值结果 + 真值判定
        Ok(value) => ctx.logger.debug(format!(
            "条件求值：{} => {}（取 {} 出口）",
            ctx.node.param_str("expr").unwrap_or(""),
            value,
            if truthy(value) { "true" } else { "false" }
        )),
        Err(err) => ctx.logger.error(format!(
            "条件求值失败：{}（{}）",
            err.message,
            ctx.node.param_str("expr").unwrap_or("")
        )),
    }
    result
}

/// 条件节点的真值判定。引擎用它选出口端口，节点输出保持为求值结果本身。
/// 对 JSON 值与 JS Boolean() 一致（空数组、空对象、"false"、"0" 都是真）。
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

async fn run_delay(
    ctx: &NodeExecContext,
    cancel: &CancellationToken,
) -> Result<Value, NodeFailure> {
    let ms = parse_ms(&ctx.node)?;
    ctx.logger.debug(format!("等待 {ms}ms"));
    tokio::select! {
        _ = cancel.cancelled() => Err(NodeFailure::fatal("已取消")),
        _ = tokio::time::sleep(Duration::from_millis(ms)) => Ok(serde_json::json!({ "slept_ms": ms })),
    }
}

/// delay 的 ms：数字，或 `${}` 模板展开出的数字串（展开是字符串插值语义，
/// 标量结果被 JSON.stringify 成字符串，DESIGN §10）。裸数字串（无模板）在建图
/// 校验期已被拒绝，这里只兜展开产物。
fn parse_ms(node: &Node) -> Result<u64, NodeFailure> {
    if let Some(ms) = node.param_u64("ms") {
        return Ok(ms);
    }
    match node.param_str("ms") {
        Some(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| NodeFailure::fatal(format!("delay 参数 ms 展开后不是非负整数：{s:?}"))),
        None => Err(NodeFailure::fatal("delay 节点缺少 ms 参数")),
    }
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

async fn run_http(ctx: &NodeExecContext) -> Result<Value, NodeFailure> {
    // params 已在 execute 统一展开（DESIGN §10）——此处直接读展开结果
    let expanded = &ctx.node.params;

    let method_str = expanded
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET")
        .to_uppercase();
    let url = expanded
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| NodeFailure::fatal("http_call 节点缺少 url 参数"))?
        .to_string();
    // 展示面用的 URL（日志行 + 失败消息）：query 里的敏感参数值脱敏，
    // 发起请求用的永远是原始 `url`。两者分开，别为了脱敏改请求目标。
    let url_label = redact_url(&url);
    // 定义层校验的是原始参数，${...} 模板展开后的 method 可能绕过白名单——
    // 执行层按同一份 HTTP_METHODS 再验一次，两层共用一个词汇表
    if !HTTP_METHODS.contains(&method_str.as_str()) {
        return Err(NodeFailure::fatal(format!("非法 HTTP 方法：{method_str}")));
    }
    let method = reqwest::Method::from_bytes(method_str.as_bytes())
        .map_err(|_| NodeFailure::fatal(format!("非法 HTTP 方法：{method_str}")))?;

    let timeout_ms = ctx
        .node
        .param_u64("timeout_ms")
        .unwrap_or(DEFAULT_HTTP_TIMEOUT_MS);

    // proxy 参数存在时按请求建独立 client（共享的 HTTP_CLIENT 不挂代理）；
    // 代理 URL 非法是配置错误：请求从未发出，重试也不会变好
    let proxy = expanded
        .get("proxy")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty());
    let owned_client = match proxy {
        Some(proxy) => Some(
            reqwest::Client::builder()
                .proxy(
                    reqwest::Proxy::all(proxy)
                        .map_err(|e| NodeFailure::fatal(format!("非法代理地址：{e}")))?,
                )
                .build()
                .map_err(|e| NodeFailure::fatal(format!("非法代理配置：{e}")))?,
        ),
        None => None,
    };
    let client = owned_client.as_ref().unwrap_or(&HTTP_CLIENT);

    let started = Instant::now();
    let mut request = client
        .request(method, &url)
        .timeout(Duration::from_millis(timeout_ms));

    if let Some(headers) = expanded.get("headers").and_then(Value::as_object) {
        for (key, value) in headers {
            let value = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            request = request.header(key, value);
        }
    }
    if let Some(body) = expanded.get("body") {
        if !body.is_null() {
            request = request.json(body);
        }
    }

    ctx.logger.info(format!("→ {method_str} {url_label}"));
    let response = match request.send().await {
        Ok(response) => response,
        Err(err) if err.is_builder() => {
            // URL/请求头非法：请求从未发出，参数错误重试也不会变好（DESIGN §6.5）。
            // reqwest 错误 Display 自带 ` for url (…)` 原样回显查询串，必须剥离；
            // 脱敏 URL 只走 url_label 这一份。
            let err = err.without_url();
            ctx.logger.error(format!("请求参数非法：{err}（未发出）"));
            return Err(NodeFailure::fatal(format!("请求参数非法：{err}")));
        }
        Err(err) => {
            // 连接失败/超时：副作用不明确或未发生，交给重试策略
            let err = err.without_url();
            ctx.logger.error(format!(
                "请求 {url_label} 失败（{}ms）：{err}",
                started.elapsed().as_millis()
            ));
            return Err(NodeFailure::retryable(format!(
                "请求 {url_label} 失败（{}ms）：{err}",
                started.elapsed().as_millis()
            )));
        }
    };

    let status = response.status();
    let headers: Map<String, Value> = response
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                Value::String(v.to_str().unwrap_or("").to_string()),
            )
        })
        .collect();
    let text = read_response_text(response).await?;
    let body = serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text.clone()));

    let output = serde_json::json!({
        "status": status.as_u16(),
        "headers": Value::Object(headers),
        "body": body,
    });

    // 响应元信息一行：状态码、耗时、体量；非 2xx 升级 warn
    let response_line = format!(
        "← HTTP {}（{}ms，body {}B）",
        status.as_u16(),
        started.elapsed().as_millis(),
        text.len()
    );
    if status.is_success() {
        ctx.logger.info(response_line);
    } else {
        ctx.logger.warn(response_line);
    }

    if status.is_server_error() || status.is_client_error() {
        // 5xx 可能已被下游处理了一部分，标为可重试；4xx 是请求本身的问题
        let mut failure =
            if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                NodeFailure::retryable(format!("HTTP {status}"))
            } else {
                NodeFailure::fatal(format!("HTTP {status}"))
            };
        failure.message = format!(
            "{}，响应体：{}",
            failure.message,
            truncate(&redact_value(&output))
        );
        return Err(failure);
    }

    Ok(output)
}

/// Bound response allocation even when Content-Length is missing or dishonest.
async fn read_response_text(mut response: reqwest::Response) -> Result<String, NodeFailure> {
    const LIMIT: usize = 8 * 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|length| length > LIMIT as u64)
    {
        return Err(NodeFailure::fatal("HTTP response exceeds 8 MiB"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        NodeFailure::retryable(format!("读取响应体失败：{}", error.without_url()))
    })? {
        if chunk.len() > LIMIT.saturating_sub(body.len()) {
            return Err(NodeFailure::fatal("HTTP response exceeds 8 MiB"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// email 的 POST JSON；与 http_call 一致，429/5xx 可重试。
async fn post_json_bearer(
    url: &str,
    api_key: &str,
    body: &Value,
    timeout_ms: u64,
) -> Result<(reqwest::StatusCode, Value), NodeFailure> {
    let started = Instant::now();
    // 失败消息里回显的 URL 走脱敏副本：原值只用于发请求
    let url_label = redact_url(url);
    let response = match HTTP_CLIENT
        .post(url)
        .bearer_auth(api_key)
        .json(body)
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) if err.is_builder() => {
            // URL 非法：请求从未发出，参数错误重试也不会变好
            return Err(NodeFailure::fatal(format!(
                "请求参数非法：{}",
                err.without_url()
            )));
        }
        Err(err) => {
            return Err(NodeFailure::retryable(format!(
                "请求 {url_label} 失败（{}ms）：{}",
                started.elapsed().as_millis(),
                err.without_url()
            )));
        }
    };
    let status = response.status();
    let text = read_response_text(response).await?;
    let body = serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text));
    if status.is_server_error()
        || status.is_client_error()
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        // 429/5xx 稍后可能好转；其余 4xx 是请求本身的问题（key 无效、模型名错）
        let retryable =
            status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
        let mut failure = if retryable {
            NodeFailure::retryable(format!("HTTP {status}"))
        } else {
            NodeFailure::fatal(format!("HTTP {status}"))
        };
        failure.message = format!(
            "{}，响应体：{}",
            failure.message,
            truncate(&redact_value(&body))
        );
        return Err(failure);
    }
    Ok((status, body))
}

/// harness：外部 agent CLI 的请求构造（纯函数，与进程无关便于测试）。
/// 返回 `(command, args, workdir, timeout_ms, prompt)`。
pub fn harness_request(params: &Value) -> Result<(String, Vec<String>, Option<String>, u64, String), NodeFailure> {
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

/// harness 执行：spawn → prompt 写 stdin → 收 stdout/stderr。
/// 退出码 0 → `{exit_code, stdout, stderr, result?}`（stdout 去空白后能解析为
/// JSON 对象/数组时带 `result`）；非 0 → fatal 带 stderr 尾部；超时 → 杀进程、
/// 可重试；spawn 失败（命令不存在等）→ fatal（重试无意义）。
async fn run_harness(ctx: &NodeExecContext) -> Result<Value, NodeFailure> {
    let (command, args, workdir, timeout_ms, prompt) = harness_request(&ctx.node.params)?;
    let mut command_line = command.clone();
    if !args.is_empty() {
        command_line = format!("{command_line} {}", args.join(" "));
    }
    ctx.logger.info(format!("→ {command_line}"));
    let started = Instant::now();
    let output = harness_spawn(&command, &args, workdir.as_deref(), &prompt, timeout_ms)
        .await
        .map_err(|err| {
            ctx.logger.error(format!("harness 失败：{}", err.message));
            err
        })?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    ctx.logger.info(format!(
        "← exit {}（{}ms，stdout {}B，stderr {}B）",
        output.status.code().unwrap_or(-1),
        started.elapsed().as_millis(),
        stdout.len(),
        stderr.len()
    ));
    harness_output(output.status.code(), stdout, stderr)
}

/// 三条执行路径（engine / journal / executor）共用的 harness 输出派生：
/// 退出码 0 → 输出对象；否则 fatal 错误（消息带 stderr 尾部）。
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

/// stderr 尾部（错误消息用）：最多保留末尾 512 字节，按字符边界切。
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

/// harness 子进程命令构造：stdio 全部管道，kill_on_drop 保证超时/取消路径上
/// 进程被带走（engine / journal / executor 三条执行路径共用）。
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

/// spawn 并等待 harness 子进程。kill_on_drop 保证超时/取消路径上进程被带走。
async fn harness_spawn(
    command: &str,
    args: &[String],
    workdir: Option<&str>,
    prompt: &str,
    timeout_ms: u64,
) -> Result<std::process::Output, NodeFailure> {
    use tokio::io::AsyncWriteExt;
    let mut child = harness_command(command, args, workdir)
        .spawn()
        .map_err(|e| NodeFailure::fatal(format!("harness 启动失败（{command}）：{e}")))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| NodeFailure::fatal("harness 子进程 stdin 不可用"))?;
    // 对端不读 stdin 提前退出时 EPIPE 不算失败——退出码会说明一切
    match stdin.write_all(prompt.as_bytes()).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
        Err(e) => return Err(NodeFailure::retryable(format!("harness 写 stdin 失败：{e}"))),
    }
    drop(stdin);
    match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait_with_output()).await
    {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(NodeFailure::retryable(format!(
            "harness 等待子进程失败（{command}）：{e}"
        ))),
        // 超时：wait_with_output 被 drop，kill_on_drop 杀掉子进程
        Err(_) => Err(NodeFailure::retryable(format!(
            "harness 超时（{timeout_ms}ms，已终止）：{command}"
        ))),
    }
}

/// Resend 兼容发信接口的请求构造（纯函数）。to 原样透传字符串。
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

async fn run_email(ctx: &NodeExecContext) -> Result<Value, NodeFailure> {
    let api_key = ctx
        .node
        .param_str("api_key")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| NodeFailure::fatal("email 节点缺少参数 api_key"))?
        .to_string();
    let (endpoint, body) = email_request(&ctx.node.params)?;
    let timeout_ms = ctx
        .node
        .param_u64("timeout_ms")
        .unwrap_or(DEFAULT_HTTP_TIMEOUT_MS);
    let (status, response) = post_json_bearer(&endpoint, &api_key, &body, timeout_ms).await?;
    Ok(serde_json::json!({
        "status": status.as_u16(),
        "id": response.get("id").cloned().unwrap_or(Value::Null),
    }))
}

fn truncate(value: &Value) -> String {
    let text = value.to_string();
    if text.len() <= 512 {
        return text;
    }
    // 512 字节可能落在多字节 UTF-8 字符中间，回退到最近的字符边界再切
    let mut end = 512;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    fn node(node_type: &str, params: Value) -> Node {
        Node {
            id: "n".into(),
            node_type: node_type.into(),
            name: String::new(),
            position: None,
            params,
        }
    }

    fn ctx(node: Node, input: Value, outputs: HashMap<String, Value>) -> NodeExecContext {
        NodeExecContext {
            node,
            input,
            outputs,
            preds: vec![],
            depth: 0,
            child: None,
            logger: NodeLogger::disabled(),
        }
    }

    /// 统一参数展开（DESIGN §10）：无模板零成本（None）；有模板则按类型展开。
    #[tokio::test]
    async fn expand_params_applies_outside_code_fields_only() {
        // 无模板：None，调用方零克隆沿用原 node
        let plain = node("delay", json!({"ms": 5}));
        assert!(expand_params(&plain, &json!({}), &HashMap::new())
            .await
            .unwrap()
            .is_none());

        // delay 的 ms 走模板：整值规则保留数字类型
        let delay = node("delay", json!({"ms": "${input.amount * 10}"}));
        let expanded = expand_params(&delay, &json!({"amount": 5}), &HashMap::new())
            .await
            .unwrap()
            .expect("有模板应返回展开后的 node");
        assert_eq!(expanded.params["ms"], json!(50));

        // script.code 是 opaque：含 JS 模板字面量也不展开（None = 原样）
        let script = node(
            "script",
            json!({"code": "return `${input.amount}`;", "note": "${input.amount}"}),
        );
        let expanded = expand_params(&script, &json!({"amount": 5}), &HashMap::new())
            .await
            .unwrap()
            .expect("note 有模板应返回展开后的 node");
        assert_eq!(
            expanded.params["code"],
            json!("return `${input.amount}`;"),
            "code 必须字节级不动"
        );
        // 整值模板保留类型：数字 5 不再是字符串 "5"
        assert_eq!(expanded.params["note"], json!(5));

        // 只读直接前驱快照：引用非前驱节点 = 属性访问抛错进 fatal
        // （与 script 节点同一条决定论边界，DESIGN §10）
        let http = node("http_call", json!({"url": "http://x/${nodes.n1.hit}"}));
        let err = expand_params(&http, &json!({}), &HashMap::new())
            .await
            .unwrap_err();
        assert!(!err.retryable, "{}", err.message);
        assert!(err.message.contains("表达式求值失败"), "{}", err.message);
    }

    /// delay 的 ms 接受模板产物；执行路径端到端（展开 → parse → sleep）。
    /// 展开现在由 driver 的 start_node 负责，测试预展开后进 execute。
    #[tokio::test]
    async fn delay_ms_template_drives_the_sleep() {
        let ok = node("delay", json!({"ms": "${input.tick}"}));
        let ok = expand_params(&ok, &json!({"tick": 40}), &HashMap::new())
            .await
            .unwrap()
            .unwrap_or(ok);
        let out = execute(
            &ctx(ok, json!({"tick": 40}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(out, json!({"slept_ms": 40}));

        // 展开产物不是整数：fatal，不猜
        let bad = node("delay", json!({"ms": "${input.tick}"}));
        let bad = expand_params(&bad, &json!({"tick": "soon"}), &HashMap::new())
            .await
            .unwrap()
            .unwrap_or(bad);
        let err = execute(
            &ctx(bad, json!({"tick": "soon"}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(!err.retryable, "参数错误不可重试：{}", err.message);
        assert!(err.message.contains("非负整数"), "{}", err.message);
    }

    /// http_call 的 url 模板经统一入口展开后再发请求（回归：run_http 不自行
    /// 展开，且双展开不得改变结果）。
    #[tokio::test]
    async fn http_url_template_is_expanded_before_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let n = sock.read(&mut buf).await.unwrap();
            *seen2.lock().await = String::from_utf8_lossy(&buf[..n]).to_string();
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n{\"ok\":1}")
                .await
                .unwrap();
        });

        let node = node(
            "http_call",
            json!({"url": format!("http://{addr}/items/${{input.order_id}}")}),
        );
        // 展开已上移到 driver 的 start_node：测试预展开后进 execute
        let node = expand_params(&node, &json!({"order_id": "o-9"}), &HashMap::new())
            .await
            .unwrap()
            .unwrap_or(node);
        let out = execute(
            &ctx(node, json!({"order_id": "o-9"}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(out["status"], json!(200));
        assert_eq!(out["body"], json!({"ok": 1}));
        let request = seen.lock().await.clone();
        assert!(request.starts_with("GET /items/o-9 "), "{request}");
    }

    /// 双展开守卫：用户数据里合法的 `${` 在 driver 展开后被嵌入 params，
    /// execute 不得再展开一次——否则数据会被当模板 evaluate（数据损坏 +
    /// 模板注入）。展开恰好一次，见 expand_params 文档。
    #[tokio::test]
    async fn execute_does_not_expand_user_data_again() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let n = sock.read(&mut buf).await.unwrap();
            *seen2.lock().await = String::from_utf8_lossy(&buf[..n]).to_string();
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n{\"ok\":1}")
                .await
                .unwrap();
        });

        // 模拟 driver 的那一次展开：用户数据 order_id 本身是模板样子的字符串
        let input = json!({"order_id": "${input.evil}", "evil": "PWNED"});
        let http = node(
            "http_call",
            json!({"url": format!("http://{addr}/items/${{input.order_id}}")}),
        );
        let http = expand_params(&http, &input, &HashMap::new())
            .await
            .unwrap()
            .unwrap_or(http);
        execute(&ctx(http, input, HashMap::new()), &CancellationToken::new())
            .await
            .unwrap();
        let request = seen.lock().await.clone();
        assert!(
            !request.contains("PWNED"),
            "用户数据不得被二次展开 evaluate：{request}"
        );
    }

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
    fn truncate_cuts_on_utf8_char_boundary() {
        // 300 个三字节字符：512 字节处落在字符中间，截断必须回退到字符边界
        let value = json!("宁".repeat(300));
        let cut = truncate(&value);
        assert!(cut.ends_with('…'), "{cut}");
    }

    #[tokio::test]
    async fn http_body_truncated_mid_stream_is_retryable_failure() {
        // 回归：服务器声明 Content-Length: 100 却只发 10 字节就断开。
        // text() 的 Err 若被吞成空字符串，节点会带着 200 + 空 body 记成功。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let _ = sock.read(&mut buf).await; // 丢弃请求
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                .await
                .unwrap();
            sock.write_all(b"0123456789").await.unwrap();
            // 不足 Content-Length 就关闭，让响应体确定性地断流
        });

        let node = crate::model::Node {
            id: "h".into(),
            node_type: "http_call".into(),
            name: String::new(),
            position: None,
            params: json!({ "url": format!("http://{addr}/pay"), "method": "POST" }),
        };
        let ctx = NodeExecContext {
            node,
            input: json!({"amount": 1}),
            outputs: HashMap::new(),
            preds: vec![],
            depth: 0,
            child: None,
            logger: NodeLogger::disabled(),
        };
        let failure = execute(&ctx, &CancellationToken::new()).await.unwrap_err();
        assert!(
            failure.retryable,
            "响应体断流必须判为可重试失败：{}",
            failure.message
        );
    }

    /// 一次性 mock HTTP 服务器：收下请求原文，回给定状态行与 JSON body。
    fn mock_server(
        status_line: &'static str,
        body: &'static str,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<tokio::sync::Mutex<String>>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            // 读满 请求头 + Content-Length 声明的 body（POST body 可能分片到达）
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf);
                if let Some(head_end) = text.find("\r\n\r\n") {
                    let len = text[..head_end]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if buf.len() >= head_end + 4 + len {
                        break;
                    }
                }
            }
            *seen2.lock().await = String::from_utf8_lossy(&buf).to_string();
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(response.as_bytes()).await.unwrap();
        });
        (addr, seen)
    }

    /// harness 请求构造：必填、args 规整、workdir/timeout 默认值。
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

    /// harness 端到端：prompt 经 stdin 进，stdout 出；stdout 是 JSON 时带 result。
    #[tokio::test]
    async fn harness_pipes_prompt_and_captures_output() {
        // cat：stdin 原样回显
        let cat = node("harness", json!({"command": "cat", "prompt": "提示词正文"}));
        let out = execute(
            &ctx(cat, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            json!({"exit_code": 0, "stdout": "提示词正文", "stderr": ""}),
            "stdout 恰为 prompt 时不带 result（非 JSON）"
        );

        // stdout 是 JSON 对象：附 result
        let json_out = node(
            "harness",
            json!({"command": "printf", "args": ["{\"a\":1}"], "prompt": "p"}),
        );
        let out = execute(
            &ctx(json_out, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(out["result"], json!({"a": 1}));
        assert_eq!(out["stdout"], json!("{\"a\":1}"));
    }

    /// harness 失败分类：非 0 退出 → fatal 带 stderr 尾部；命令不存在 → fatal；
    /// 超时 → 可重试。
    #[tokio::test]
    async fn harness_failure_classification() {
        let fail = node(
            "harness",
            json!({"command": "sh", "args": ["-c", "echo boom >&2; exit 3"], "prompt": "p"}),
        );
        let err = execute(
            &ctx(fail, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(!err.retryable, "非 0 退出不可重试：{}", err.message);
        assert!(
            err.message.contains("3") && err.message.contains("boom"),
            "错误消息带退出码与 stderr 尾部：{}",
            err.message
        );

        let missing = node(
            "harness",
            json!({"command": "flow-test-no-such-command", "prompt": "p"}),
        );
        let err = execute(
            &ctx(missing, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(!err.retryable, "spawn 失败不可重试：{}", err.message);

        let slow = node(
            "harness",
            json!({"command": "sleep", "args": ["30"], "prompt": "p", "timeout_ms": 100}),
        );
        let err = execute(
            &ctx(slow, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(err.retryable, "超时必须可重试：{}", err.message);
        assert!(err.message.contains("超时"), "{}", err.message);
    }

    /// email 请求构造：Resend 格式（body → text），endpoint 默认值。
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

    /// x-secret 注入：名称对应环境变量缺失 → fatal 且消息带提示。
    /// （注入成功的端到端由 email_posts_resend_format_and_reports_id 覆盖。）
    #[tokio::test]
    async fn missing_secret_name_is_fatal_with_env_hint() {
        let email = node(
            "email",
            json!({"api_key": "TEST_EMAIL_MISSING", "from": "a@b.c", "to": "d@e.f",
                   "subject": "s", "body": "b"}),
        );
        let err = execute(
            &ctx(email, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(!err.retryable, "{}", err.message);
        assert!(
            err.message.contains("FLOW_SECRET_TEST_EMAIL_MISSING"),
            "错误消息必须提示环境变量名：{}",
            err.message
        );
    }

    #[tokio::test]
    async fn http_rate_limit_is_retryable() {
        let (addr, _) = mock_server("429 Too Many Requests", "limited");
        let err = execute(
            &ctx(
                node("http_call", json!({"url": format!("http://{addr}")})),
                json!({}),
                HashMap::new(),
            ),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(err.retryable);
        assert!(err.message.contains("429"));
    }

    #[tokio::test]
    async fn http_declared_oversize_is_rejected_without_reading_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8388609\r\n\r\n")
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            execute(
                &ctx(
                    node("http_call", json!({"url": format!("http://{addr}")})),
                    json!({}),
                    HashMap::new(),
                ),
                &CancellationToken::new(),
            ),
        )
        .await;
        server.abort();
        let err = result
            .expect("must reject before waiting for body")
            .unwrap_err();
        assert!(!err.retryable);
        assert!(err.message.contains("8 MiB"), "{}", err.message);
    }

    /// email（post_json_bearer）的错误分类：429/5xx 可重试，其余 4xx fatal。
    #[tokio::test]
    async fn email_error_classification_matches_http_habits() {
        // api_key 是密钥名称（x-secret）：字面量也要经 FLOW_SECRET_<名称> 解析
        std::env::set_var("FLOW_SECRET_TEST_CLS_KEY", "sk-x");
        for (status_line, retryable) in [
            ("429 Too Many Requests", true),
            ("500 Internal Server Error", true),
            ("400 Bad Request", false),
            ("401 Unauthorized", false),
        ] {
            let (addr, _seen) = mock_server(status_line, r#"{"error":"x"}"#);
            let email = node(
                "email",
                json!({
                    "endpoint": format!("http://{addr}/emails"),
                    "api_key": "TEST_CLS_KEY",
                    "from": "a@b.c", "to": "d@e.f", "subject": "s", "body": "b"
                }),
            );
            let err = execute(
                &ctx(email, json!({}), HashMap::new()),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.retryable, retryable, "{status_line}: {}", err.message);
            assert!(err.message.contains(status_line.split(' ').next().unwrap()));
        }
        std::env::remove_var("FLOW_SECRET_TEST_CLS_KEY");
    }

    /// email 端到端（mock）：请求体是 Resend 格式，Bearer 头是注入的真值，
    /// 输出 {status, id}。
    #[tokio::test]
    async fn email_posts_resend_format_and_reports_id() {
        let (addr, seen) = mock_server("200 OK", r#"{"id":"em_123"}"#);
        std::env::set_var("FLOW_SECRET_TEST_EMAIL_KEY", "re-live-value");
        let email = node(
            "email",
            json!({
                "endpoint": format!("http://{addr}/emails"),
                "api_key": "TEST_EMAIL_KEY",
                "from": "a@b.c", "to": "d@e.f",
                "subject": "告警 ${input.what}", "body": "明细 ${input.what}"
            }),
        );
        // 展开已上移到 driver 的 start_node：测试预展开后进 execute
        let email = expand_params(&email, &json!({"what": "磁盘"}), &HashMap::new())
            .await
            .unwrap()
            .unwrap_or(email);
        let out = execute(
            &ctx(email, json!({"what": "磁盘"}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(out, json!({"status": 200, "id": "em_123"}));
        let request = seen.lock().await.clone();
        assert!(
            request.contains("authorization: Bearer re-live-value"),
            "{request}"
        );
        assert!(
            request.contains("\"subject\":\"告警 磁盘\""),
            "subject 模板应展开：{request}"
        );
        assert!(
            request.contains("\"text\":\"明细 磁盘\""),
            "body → text：{request}"
        );

        // 响应没有 id 字段：id 为 null
        let (addr, _seen) = mock_server("202 Accepted", r#"{"queued":true}"#);
        let email = node(
            "email",
            json!({
                "endpoint": format!("http://{addr}/emails"),
                "api_key": "TEST_EMAIL_KEY", "from": "a@b.c", "to": "d@e.f", "subject": "s", "body": "b"
            }),
        );
        let out = execute(
            &ctx(email, json!({}), HashMap::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(out, json!({"status": 202, "id": null}));
        std::env::remove_var("FLOW_SECRET_TEST_EMAIL_KEY");
    }
}

/// `singular_or_map`：单来源透传其值、多来源组成映射、缺失补 null。
/// 与 `mod tests` 里的 json truthiness 用例合一处——一个文件里两个同名
/// `mod tests` 是非法的，合并不必为此再起个怪名字。
#[cfg(test)]
mod singular_or_map_tests {
    use super::*;
    use serde_json::json;

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
