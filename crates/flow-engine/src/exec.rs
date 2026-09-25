use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::child_run::{ChildRunLauncher, ChildRunOutcome, MAX_SUB_WORKFLOW_DEPTH};
use crate::error::EngineError;
use crate::expr;
use crate::model::{Node, NodeType};

pub const DEFAULT_JS_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;

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
}

pub struct ChildRunSpec {
    pub child_run_id: String,
    pub launcher: Arc<dyn ChildRunLauncher>,
}

impl NodeExecContext {
    fn nodes_value(&self) -> Value {
        Value::Object(
            self.outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Map<String, Value>>(),
        )
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

    match kind {
        NodeType::Start => Ok(ctx.input.clone()),
        NodeType::End => Ok(collect_end_output(ctx)),
        NodeType::Script => run_script(ctx).await,
        NodeType::Condition => run_condition(ctx).await,
        NodeType::Delay => run_delay(ctx, cancel).await,
        NodeType::HttpCall => run_http(ctx).await,
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
    // 子 run 输入 = 父 run 输入快照
    match child
        .launcher
        .start(
            &child.child_run_id,
            &workflow_id,
            ctx.input.clone(),
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
    // rquickjs 是同步 CPU 执行，必须放到阻塞线程池，避免占死 tokio worker
    let out = tokio::task::spawn_blocking(move || expr::eval_body(&code, &input, &nodes, timeout))
        .await
        .map_err(|e| NodeFailure::fatal(format!("脚本任务异常：{e}")))?;
    out.map_err(NodeFailure::from)
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
    tokio::task::spawn_blocking(move || expr::eval_expr(&expression, &input, &nodes, timeout))
        .await
        .map_err(|e| NodeFailure::fatal(format!("条件求值任务异常：{e}")))?
        .map_err(NodeFailure::from)
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
    let ms = ctx
        .node
        .param_u64("ms")
        .ok_or_else(|| NodeFailure::fatal("delay 节点缺少 ms 参数"))?;
    tokio::select! {
        _ = cancel.cancelled() => Err(NodeFailure::fatal("已取消")),
        _ = tokio::time::sleep(Duration::from_millis(ms)) => Ok(serde_json::json!({ "slept_ms": ms })),
    }
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

async fn run_http(ctx: &NodeExecContext) -> Result<Value, NodeFailure> {
    let input = ctx.input.clone();
    let nodes = ctx.nodes_value();
    // timeout_ms 在 http_call 上只表示 HTTP 超时（见 nodetypes 清单）；
    // 模板展开的 JS 超时与它无关，固定用默认脚本超时——一个参数不承担两个语义
    let timeout = Duration::from_millis(DEFAULT_JS_TIMEOUT_MS);
    let params = ctx.node.params.clone();
    let expanded = tokio::task::spawn_blocking(move || {
        expr::expand_templates(&params, &input, &nodes, timeout)
    })
    .await
    .map_err(|e| NodeFailure::fatal(format!("参数展开任务异常：{e}")))?
    .map_err(NodeFailure::from)?;

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
    let method = reqwest::Method::from_bytes(method_str.as_bytes())
        .map_err(|_| NodeFailure::fatal(format!("非法 HTTP 方法：{method_str}")))?;

    let timeout_ms = ctx
        .node
        .param_u64("timeout_ms")
        .unwrap_or(DEFAULT_HTTP_TIMEOUT_MS);

    let started = Instant::now();
    let mut request = HTTP_CLIENT
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

    let response = match request.send().await {
        Ok(response) => response,
        Err(err) if err.is_builder() => {
            // URL/请求头非法：请求从未发出，参数错误重试也不会变好（DESIGN §6.5）
            return Err(NodeFailure::fatal(format!("请求参数非法：{err}")));
        }
        Err(err) => {
            // 连接失败/超时：副作用不明确或未发生，交给重试策略
            return Err(NodeFailure::retryable(format!(
                "请求 {url} 失败（{}ms）：{err}",
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
    let text = match response.text().await {
        Ok(text) => text,
        // 连接在读完响应头之后断开：body 不完整。不能带着 200 + 空 body 记成功
        Err(err) => {
            return Err(NodeFailure::retryable(format!(
                "读取 {url} 响应体失败（{}ms）：{err}",
                started.elapsed().as_millis()
            )));
        }
    };
    let body = serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text));

    let output = serde_json::json!({
        "status": status.as_u16(),
        "headers": Value::Object(headers),
        "body": body,
    });

    if status.is_server_error() || status.is_client_error() {
        // 5xx 可能已被下游处理了一部分，标为可重试；4xx 是请求本身的问题
        let mut failure = if status.is_server_error() {
            NodeFailure::retryable(format!("HTTP {status}"))
        } else {
            NodeFailure::fatal(format!("HTTP {status}"))
        };
        failure.message = format!("{}，响应体：{}", failure.message, truncate(&output));
        return Err(failure);
    }

    Ok(output)
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
        // 300 个三字节字符：512 字节处落在字符中间，修复前这里直接 panic
        let value = json!("宁".repeat(300));
        let cut = truncate(&value);
        assert!(cut.ends_with('…'), "{cut}");
    }

    #[tokio::test]
    async fn http_body_truncated_mid_stream_is_retryable_failure() {
        // 回归：服务器声明 Content-Length: 100 却只发 10 字节就断开。
        // 修复前 text() 的 Err 被吞成空字符串，节点带着 200 + 空 body 记成功。
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
        };
        let failure = execute(&ctx, &CancellationToken::new()).await.unwrap_err();
        assert!(
            failure.retryable,
            "响应体断流必须判为可重试失败：{}",
            failure.message
        );
    }
}

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
            singular_or_map(vec![("e1".into(), json!(7)), ("e2".into(), Value::Null),]),
            json!({"e1": 7, "e2": null}),
            "多来源必须组成映射，缺失补 null"
        );
    }
}
