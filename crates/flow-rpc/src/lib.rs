//! flow-rpc：jsonrpsee WebSocket 服务（bin: flow-server）。
//!
//! 适配层重构后的职责边界（DESIGN.md §2）：
//! - 本 crate **只依赖 flow-backend 的 `Backend` trait**，不感知
//!   flow-store / flow-pg，也不读取 FLOW_BACKEND——后端选择在 main +
//!   `flow_backend::open_from_env()` 完成一次，之后对 RPC 层完全透明；
//! - SQLite + event.jsonl（canonical）与 Postgres（可替代）的语义差异
//!   （初始化协议、信号落账、订阅推送）由 flow-backend 吸收，这里的每个
//!   RPC 方法只有一份实现。

use std::net::SocketAddr;
use std::sync::Arc;

use flow_backend::{AnyBackend, BackendError, SignalAck};
use flow_engine::{Definition, RunState, HTTP_METHODS};
use futures::StreamExt;
use jsonrpsee::core::RegisterMethodError;
use jsonrpsee::server::{Server, ServerHandle, SubscriptionMessage};
use jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned};
use jsonrpsee::types::Params;
use jsonrpsee::RpcModule;
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

const CODE_INVALID: i32 = -32010;
const CODE_NOT_FOUND: i32 = -32011;
const CODE_CONFLICT: i32 = -32012;
const CODE_INTERNAL: i32 = -32603;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("注册 RPC 方法失败：{0}")]
    Register(#[from] RegisterMethodError),
    #[error("监听失败：{0}")]
    Io(#[from] std::io::Error),
    #[error("后端错误：{0}")]
    Backend(#[from] BackendError),
}

/// 唯一的 RPC 层状态：闭集后端枚举。SQLite / Postgres 在这里不可区分，
/// pg 专属能力在对应方法的注册点单点 match。
pub struct AppState {
    pub backend: AnyBackend,
}

pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
) -> Result<(ServerHandle, SocketAddr), RpcError> {
    let module = build_module(state)?;
    let server = Server::builder().build(addr).await?;
    let local_addr = server.local_addr()?;
    let handle = server.start(module);
    Ok((handle, local_addr))
}

pub fn build_module(state: Arc<AppState>) -> Result<RpcModule<Arc<AppState>>, RpcError> {
    let mut module: RpcModule<Arc<AppState>> = RpcModule::new(state);

    // ---- 工作流定义 ----
    module.register_async_method("workflow.create", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            name: String,
        }
        let p: P = parse(&params)?;
        let workflow_id = state
            .backend
            .create_workflow(&p.name)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflow_id": workflow_id }))
    })?;

    module.register_async_method("workflow.update", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
            definition: Value,
        }
        let p: P = parse(&params)?;
        // 拖拽产出的图在落库前就校验，别等到发布
        let definition: Definition = serde_json::from_value(p.definition.clone())
            .map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;

        let version = state
            .backend
            .update_workflow(&p.workflow_id, &p.definition)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflow_id": p.workflow_id, "version": version }))
    })?;

    module.register_async_method("workflow.publish", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
            version: i64,
        }
        let p: P = parse(&params)?;
        let stored = state
            .backend
            .get_version(&p.workflow_id, Some(p.version))
            .await
            .map_err(backend_err)?;
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;

        state
            .backend
            .publish(&p.workflow_id, p.version)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(
            json!({ "workflow_id": p.workflow_id, "version": p.version, "status": "published" }),
        )
    })?;

    module.register_async_method("workflow.get", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
            #[serde(default)]
            version: Option<i64>,
        }
        let p: P = parse(&params)?;
        let version = state
            .backend
            .get_version(&p.workflow_id, p.version)
            .await
            .map_err(backend_err)?;
        let published_version = state
            .backend
            .latest_published(&p.workflow_id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({
            "workflow_id": version.workflow_id,
            "version": version.version,
            "status": version.status,
            "definition": version.definition,
            "published_version": published_version,
        }))
    })?;

    module.register_async_method("workflow.list", |params, state, _| async move {
        let _: Value = parse(&params)?;
        let list = state.backend.list_workflows().await.map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflows": list }))
    })?;

    module.register_async_method("workflow.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
        }
        let p: P = parse(&params)?;
        state
            .backend
            .delete_workflow(&p.workflow_id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "deleted": true }))
    })?;

    // ---- 前端画布 ----
    module.register_method("nodetypes.list", |params, _state, _| {
        let _: Value = parse(&params)?;
        Ok::<_, ErrorObjectOwned>(json!({ "node_types": node_types() }))
    })?;

    // ---- 执行 ----
    module.register_async_method("run.start", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
            #[serde(default)]
            version: Option<i64>,
            #[serde(default)]
            input: Option<Value>,
        }
        let p: P = parse(&params)?;
        let created = state
            .backend
            .create_run(flow_backend::CreateRun {
                workflow_id: p.workflow_id,
                version: p.version,
                input: p.input.unwrap_or(Value::Null),
            })
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({
            "run_id": created.run_id,
            "workflow_version": created.workflow_version,
        }))
    })?;

    module.register_async_method("run.get", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
        }
        let p: P = parse(&params)?;
        let run = state
            .backend
            .get_run(&p.run_id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({
            "run": run,
            "live": state.backend.is_live(&p.run_id),
        }))
    })?;

    module.register_async_method("run.list", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            #[serde(default)]
            workflow_id: Option<String>,
            #[serde(default)]
            limit: Option<i64>,
        }
        let p: P = parse(&params)?;
        let runs = state
            .backend
            .list_runs(
                p.workflow_id.as_deref(),
                p.limit.unwrap_or(50).clamp(1, 500),
            )
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "runs": runs }))
    })?;

    // 只读时间线：定义顺序 + 折叠后的节点状态，前端直接画列表
    module.register_async_method("run.timeline", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
        }
        let p: P = parse(&params)?;
        let run = state
            .backend
            .get_run(&p.run_id)
            .await
            .map_err(backend_err)?;
        let stored = state
            .backend
            .get_version(&run.workflow_id, Some(run.workflow_version))
            .await
            .map_err(backend_err)?;
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| internal(format!("定义结构非法：{e}")))?;
        let snapshot = state
            .backend
            .snapshot(&p.run_id)
            .await
            .map_err(|e| internal(format!("读取事件日志失败：{e}")))?;

        Ok::<_, ErrorObjectOwned>(timeline_value(
            &run.id,
            &run.status,
            &run.workflow_id,
            run.workflow_version,
            &definition,
            &snapshot,
        ))
    })?;

    module.register_async_method("run.events", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            #[serde(default)]
            from_seq: Option<u64>,
        }
        let p: P = parse(&params)?;
        let events = state
            .backend
            .read_events(&p.run_id, p.from_seq)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "events": events }))
    })?;

    // 取消：SQLite 仅对活着的 run 生效（conflict）；Postgres 经持久 inbox 消费。
    module.register_async_method("run.cancel", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            #[serde(default)]
            signal_id: Option<String>,
        }
        let p: P = parse(&params)?;
        let ack = state
            .backend
            .cancel(&p.run_id, p.signal_id)
            .await
            .map_err(backend_err)?;
        signal_ack_value(ack)
    })?;

    // human_task 交付信号；崩溃残留的副作用节点用 payload.action = retry/succeeded/failed 裁决。
    // signal_id 在 Postgres 后端必填且重试复用；SQLite 后端可省略（同步交付）。
    module.register_async_method("run.signal", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            #[serde(default)]
            signal_id: Option<String>,
            node_id: String,
            #[serde(default)]
            payload: Value,
        }
        let p: P = parse(&params)?;
        let ack = state
            .backend
            .signal(flow_backend::SignalRequest {
                run_id: p.run_id,
                signal_id: p.signal_id,
                node_id: p.node_id,
                payload: p.payload,
            })
            .await
            .map_err(backend_err)?;
        signal_ack_value(ack)
    })?;

    // 信号落账查询：pg 专属能力，在唯一的注册点 match 暴露。
    // SQLite 信号是进程内同步交付，没有账可查——不进 AnyBackend 公共面，
    // 也不会伪造一个查不到的 id。
    module.register_async_method("run.signal_status", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            signal_id: String,
        }
        let p: P = parse(&params)?;
        let ack = match &state.backend {
            AnyBackend::Postgres(b) => {
                b.signal_status(&p.run_id, &p.signal_id)
                    .await
                    .map_err(backend_err)?
            }
            AnyBackend::Sqlite(_) => {
                return Err(invalid(
                    "run.signal_status 仅 Postgres 后端提供；SQLite 后端的信号在进程内同步交付，无持久 inbox 可查",
                ))
            }
        };
        Ok::<_, ErrorObjectOwned>(json!({
            "signal_id": ack.signal_id,
            "status": ack.status,
            "delivered": ack.delivered,
            "event_seq": ack.event_seq,
            "error": ack.error,
        }))
    })?;

    // 执行进度推送（JSON-RPC 2.0 订阅通知）。推送机制由后端吸收：
    // SQLite 是进程内 broadcast；Postgres 按 run_id 维护 last_seq 轮询共享日志。
    module.register_subscription(
        "run.subscribe",
        "run.event",
        "run.unsubscribe",
        |params, pending, state, _| async move {
            #[derive(Deserialize)]
            struct P {
                #[serde(default)]
                run_id: Option<String>,
            }
            let filter = match parse::<P>(&params) {
                Ok(p) => p.run_id,
                Err(err) => {
                    pending.reject(err).await;
                    return;
                }
            };
            let mut events = state.backend.subscribe(filter);
            let sink = match pending.accept().await {
                Ok(sink) => sink,
                Err(_) => return,
            };
            loop {
                tokio::select! {
                    _ = sink.closed() => break,
                    received = events.next() => match received {
                        Some(envelope) => {
                            // jsonrpsee 0.26：通知消息需显式携带方法名与订阅 id
                            match SubscriptionMessage::new("run.event", sink.subscription_id(), &envelope)
                                .map_err(|e| internal(format!("事件序列化失败：{e}")))
                            {
                                Ok(message) => {
                                    if sink.send(message).await.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        // 流结束：指定 run 已终结追平（Postgres），或引擎通道关闭
                        None => break,
                    },
                }
            }
        },
    )?;

    Ok(module)
}

pub(crate) fn timeline_value(
    run_id: &str,
    run_status: &str,
    workflow_id: &str,
    workflow_version: i64,
    definition: &Definition,
    snapshot: &RunState,
) -> Value {
    let nodes: Vec<Value> = definition
        .nodes
        .iter()
        .map(|node| {
            let record = snapshot.record(&node.id);
            let mut entry = json!({
                "id": node.id,
                "name": node.name,
                "type": node.node_type,
                "state": record.state.label(),
                "attempts": record.attempts,
                "started_at": record.started_at,
                "ended_at": record.ended_at,
                "duration_ms": record.duration_ms,
                "output": record.output,
                "error": record.error,
                "child_run_id": record.child_run_id,
            });
            if let flow_engine::NodeState::Skipped { reason } = &record.state {
                entry["reason"] = json!(reason);
            }
            entry
        })
        .collect();

    json!({
        "run_id": run_id,
        "status": run_status,
        "phase": snapshot.phase,
        "workflow_id": workflow_id,
        "workflow_version": workflow_version,
        "started_at": snapshot.started_at,
        "ended_at": snapshot.ended_at,
        "output": snapshot.output,
        "fatal_error": snapshot.fatal_error,
        "last_seq": snapshot.last_seq,
        "nodes": nodes,
    })
}

/// 信号/取消请求的响应语义（DISTRIBUTED.md §6.1，两种后端共用）：
/// delivered=true 才是交付；rejected 返回 invalid/conflict 错误体系；
/// pending 返回明确的 pending 结果和 signal_id，客户端用 run.signal_status 查询。
fn signal_ack_value(ack: SignalAck) -> Result<Value, ErrorObjectOwned> {
    if ack.delivered {
        let mut body = json!({ "delivered": true });
        // signal_id 只在真有一个可查询的 id 时出现（Postgres inbox 落账）；
        // SQLite 同步交付只回显客户端提供的 id，不伪造
        if let Some(id) = ack.signal_id {
            body["signal_id"] = json!(id);
        }
        if let Some(seq) = ack.event_seq {
            body["event_seq"] = json!(seq);
        }
        return Ok(body);
    }
    if ack.status == "rejected" {
        let error = ack.error.clone().unwrap_or_else(|| json!({}));
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("invalid");
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("信号被拒绝")
            .to_string();
        return Err(if code == "conflict" {
            conflict(message)
        } else {
            invalid(message)
        });
    }
    let mut body = json!({
        "delivered": false,
        "pending": true,
        "status": ack.status,
    });
    if let Some(id) = ack.signal_id {
        body["signal_id"] = json!(id);
    }
    Ok(body)
}

/// 前端拖拽面板 + 参数表单所需的能力清单。
///
/// `params_schema` 是 JSON Schema draft-07 子集（type/required/properties/enum/default），
/// 另带 `x-widget`（code/json/workflow-picker）、`x-label`、`x-help` 扩展，
/// 前端据此递归渲染参数表单，后端 validate 仍以 model.rs 为准。
pub(crate) fn node_types() -> Value {
    json!([
        {
            "type": "start",
            "label": "开始",
            "category": "control",
            "max_instances": 1,
            "ports": [{"id": "out", "label": "出"}],
            "params_schema": {"type": "object", "properties": {}}
        },
        {
            "type": "end",
            "label": "结束",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}],
            "params_schema": {"type": "object", "properties": {}}
        },
        {
            "type": "script",
            "label": "脚本",
            "category": "compute",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params_schema": {
                "type": "object",
                "required": ["code"],
                "properties": {
                    "code": {"type": "string", "x-widget": "code", "x-label": "JS 函数体",
                             "x-help": "可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果"},
                    "timeout_ms": {"type": "integer", "default": 2000, "x-label": "脚本超时（毫秒）"}
                }
            },
            "supports_retry": true
        },
        {
            "type": "condition",
            "label": "条件分支",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}, {"id": "true", "label": "真"}, {"id": "false", "label": "假"}],
            "params_schema": {
                "type": "object",
                "required": ["expr"],
                "properties": {
                    "expr": {"type": "string", "x-widget": "code", "x-label": "条件表达式",
                             "x-help": "表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes"},
                    "timeout_ms": {"type": "integer", "default": 2000, "x-label": "求值超时（毫秒）"}
                }
            }
        },
        {
            "type": "delay",
            "label": "等待",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params_schema": {
                "type": "object",
                "required": ["ms"],
                "properties": {
                    "ms": {"type": "integer", "x-label": "时长（毫秒）"}
                }
            }
        },
        {
            "type": "http_call",
            "label": "HTTP 请求",
            "category": "integration",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params_schema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                    "method": {"type": "string", "enum": HTTP_METHODS, "default": "GET", "x-label": "方法"},
                    "url": {"type": "string", "x-label": "URL",
                            "x-help": "支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）"},
                    "headers": {"x-widget": "json", "default": {}, "x-label": "请求头"},
                    "body": {"x-widget": "json", "x-label": "请求体"},
                    "timeout_ms": {"type": "integer", "default": 30000, "x-label": "HTTP 超时（毫秒）"}
                }
            },
            "supports_retry": true,
            "side_effect": true
        },
        {
            "type": "human_task",
            "label": "人工节点",
            "category": "human",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params_schema": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "x-label": "提示"}
                }
            }
        },
        {
            "type": "sub_workflow",
            "label": "子工作流",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params_schema": {
                "type": "object",
                "required": ["workflow_id"],
                "properties": {
                    "workflow_id": {"type": "string", "x-widget": "workflow-picker", "x-label": "目标工作流",
                                    "x-help": "调用其最新已发布版本作为子 run；输入为父 run 输入，子 run 输出透传为本节点输出"}
                }
            },
            "supports_retry": true
        }
    ])
}

/// 具名参数解析。JSON-RPC 允许整个省略 params，此时到达的是 null，等价于空对象。
pub(crate) fn parse<T: serde::de::DeserializeOwned>(
    params: &Params<'_>,
) -> Result<T, ErrorObjectOwned> {
    let raw: Value = params
        .parse::<Value>()
        .map_err(|e| invalid(format!("参数非法：{}", e.message())))?;
    let raw = if raw.is_null() { json!({}) } else { raw };
    serde_json::from_value(raw).map_err(|e| invalid(format!("参数非法：{e}")))
}

pub(crate) fn invalid(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned(CODE_INVALID, message.into(), None::<()>)
}

pub(crate) fn conflict(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned(CODE_CONFLICT, message.into(), None::<()>)
}

pub(crate) fn internal(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned(CODE_INTERNAL, message.into(), None::<()>)
}

/// 适配层错误 → JSON-RPC 错误码。唯一映射点：RPC 层不再分叉处理具体后端错误。
/// 没有 Unsupported 分支——公共面上的方法两个后端都会做；
/// 后端专属能力在方法注册点 match 时已经处理。
pub(crate) fn backend_err(err: BackendError) -> ErrorObjectOwned {
    match err {
        BackendError::WorkflowNotFound(_)
        | BackendError::VersionNotFound(..)
        | BackendError::RunNotFound(_)
        | BackendError::SignalNotFound(_) => {
            ErrorObject::owned(CODE_NOT_FOUND, err.to_string(), None::<()>)
        }
        BackendError::VersionNotPublished(..) | BackendError::Conflict(_) => {
            conflict(err.to_string())
        }
        BackendError::Invalid(_) => invalid(err.to_string()),
        BackendError::Internal(_) => internal(err.to_string()),
    }
}
