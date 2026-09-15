use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::error::EngineError;
use crate::expr;
use crate::model::{Node, NodeType};

pub const DEFAULT_JS_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;

/// 节点执行失败。`retryable` 决定引擎是重试还是把 run 判为失败。
#[derive(Debug, Clone, PartialEq)]
pub struct NodeFailure {
    pub message: String,
    pub retryable: bool,
}

impl NodeFailure {
    pub fn retryable(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: true,
        }
    }

    pub fn fatal(message: impl Into<String>) -> NodeFailure {
        NodeFailure {
            message: message.into(),
            retryable: false,
        }
    }
}

impl From<EngineError> for NodeFailure {
    fn from(err: EngineError) -> NodeFailure {
        NodeFailure::fatal(err.to_string())
    }
}

pub struct NodeExecContext {
    pub node: Node,
    pub input: Value,
    /// 前驱节点输出快照（决定论输入）
    pub outputs: Arc<HashMap<String, Value>>,
    pub preds: Vec<String>,
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
            self.node.param_u64("timeout_ms").unwrap_or(DEFAULT_JS_TIMEOUT_MS),
        )
    }
}

pub async fn execute(ctx: &NodeExecContext, cancel: &CancellationToken) -> Result<Value, NodeFailure> {
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
        NodeType::HumanTask => Err(NodeFailure::fatal(
            "human_task 由引擎等待信号驱动，不应直接执行",
        )),
    }
}

fn collect_end_output(ctx: &NodeExecContext) -> Value {
    if ctx.preds.len() == 1 {
        return ctx
            .outputs
            .get(&ctx.preds[0])
            .cloned()
            .unwrap_or(Value::Null);
    }
    Value::Object(
        ctx.preds
            .iter()
            .map(|p| (p.clone(), ctx.outputs.get(p).cloned().unwrap_or(Value::Null)))
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
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

async fn run_delay(ctx: &NodeExecContext, cancel: &CancellationToken) -> Result<Value, NodeFailure> {
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
    let timeout = ctx.js_timeout();
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
        .map(|(k, v)| (k.to_string(), Value::String(v.to_str().unwrap_or("").to_string())))
        .collect();
    let text = response.text().await.unwrap_or_default();
    let body = serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text));

    let output = serde_json::json!({
        "status": status.as_u16(),
        "headers": Value::Object(headers),
        "body": body,
    });

    if status.is_server_error() || status.is_client_error() {
        // 5xx 可能已被下游处理了一部分，标为可重试；4xx 是请求本身的问题
        let failure = if status.is_server_error() {
            NodeFailure::retryable(format!("HTTP {status}"))
        } else {
            NodeFailure::fatal(format!("HTTP {status}"))
        };
        return Err(NodeFailure {
            message: format!("{}，响应体：{}", failure.message, truncate(&output)),
            retryable: failure.retryable,
        });
    }

    Ok(output)
}

fn truncate(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 512 {
        format!("{}…", &text[..512])
    } else {
        text
    }
}
