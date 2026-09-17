use std::net::SocketAddr;
use std::sync::Arc;

use flow_engine::{
    DbRunStatus, Definition, Engine, EngineError, Envelope, HTTP_METHODS, ResumeOutcome,
    RunObserver, RunPhase, RunState, Signal, StartRun, StatusUpdate,
};
use flow_store::{Store, StoreError};
use futures::future::BoxFuture;
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
    #[error("存储错误：{0}")]
    Store(#[from] StoreError),
    #[error("引擎错误：{0}")]
    Engine(#[from] EngineError),
}

pub struct AppState {
    pub store: Arc<Store>,
    pub engine: Arc<Engine>,
}

/// 把引擎的 run 状态变化落到 runs 表。引擎本身不依赖存储实现，适配在 RPC 层做。
pub struct StoreObserver {
    store: Arc<Store>,
}

impl StoreObserver {
    pub fn new(store: Arc<Store>) -> StoreObserver {
        StoreObserver { store }
    }
}

impl RunObserver for StoreObserver {
    fn on_status<'a>(&'a self, update: StatusUpdate<'a>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Err(err) = self
                .store
                .set_run_status(
                    update.run_id,
                    update.status.as_str(),
                    update.output,
                    update.error,
                )
                .await
            {
                tracing::error!(run_id = %update.run_id, error = %err, "回写 run 状态失败");
            }
        })
    }
}

/// 启动恢复：把 runs 表里未结束的 run 从 event.jsonl 折叠回来继续跑。
///
/// 事件日志是权威：日志已终结但 DB 未回填的情况在这里补齐。
pub async fn recover_unfinished(state: &AppState) -> Result<Vec<(String, String)>, RpcError> {
    let mut failures = Vec::new();
    for run in state.store.unfinished_runs().await? {
        let version = match state
            .store
            .get_version(&run.workflow_id, Some(run.workflow_version))
            .await
        {
            Ok(version) => version,
            Err(err) => {
                failures.push((run.id.clone(), err.to_string()));
                continue;
            }
        };
        let definition: Definition = match serde_json::from_value(version.definition.clone()) {
            Ok(definition) => definition,
            Err(err) => {
                failures.push((run.id.clone(), format!("定义无法解析：{err}")));
                continue;
            }
        };

        let spec = StartRun {
            run_id: run.id.clone(),
            workflow_id: run.workflow_id.clone(),
            workflow_version: run.workflow_version,
            definition,
            input: run.input.clone(),
        };

        match state.engine.resume_run(spec).await {
            Ok(ResumeOutcome::Resumed) => {
                tracing::info!(run_id = %run.id, "恢复未完成的 run");
            }
            Ok(ResumeOutcome::AlreadyTerminal(phase)) => {
                // 崩溃发生在「事件已落盘、DB 未回填」之间：以事件为准修正 DB
                let snapshot = match state.engine.snapshot(&run.id).await {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        // 读不了权威日志就不能编造状态回填
                        failures.push((run.id.clone(), format!("读取事件日志失败：{err}")));
                        continue;
                    }
                };
                let status = match phase {
                    RunPhase::Succeeded => DbRunStatus::Succeeded,
                    RunPhase::Failed => DbRunStatus::Failed,
                    RunPhase::Cancelled => DbRunStatus::Cancelled,
                    RunPhase::Running => DbRunStatus::Running,
                };
                state
                    .store
                    .set_run_status(
                        &run.id,
                        status.as_str(),
                        snapshot.output.as_ref(),
                        snapshot.fatal_error.as_deref(),
                    )
                    .await?;
                tracing::info!(run_id = %run.id, "事件日志已终结，回填 DB 状态");
            }
            Err(err) => {
                let message = err.to_string();
                if matches!(err, EngineError::RunNotFound(_) | EngineError::LogCorrupted(_)) {
                    // A missing log is never evidence that replaying side effects is safe.
                    let status = if run.status == DbRunStatus::Initializing.as_str() {
                        DbRunStatus::Failed
                    } else {
                        DbRunStatus::AwaitingResume
                    };
                    state.store.set_run_status(&run.id, status.as_str(), None, Some(&message)).await?;
                }
                failures.push((run.id.clone(), message));
            }
        }
    }
    Ok(failures)
}

pub async fn serve(state: Arc<AppState>, addr: SocketAddr) -> Result<(ServerHandle, SocketAddr), RpcError> {
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
            .store
            .create_workflow(&p.name)
            .await
            .map_err(store_err)?;
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
        let definition: Definition =
            serde_json::from_value(p.definition.clone()).map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;

        let version = state
            .store
            .update_workflow(&p.workflow_id, &p.definition)
            .await
            .map_err(store_err)?;
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
            .store
            .get_version(&p.workflow_id, Some(p.version))
            .await
            .map_err(store_err)?;
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;

        state
            .store
            .publish(&p.workflow_id, p.version)
            .await
            .map_err(store_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflow_id": p.workflow_id, "version": p.version, "status": "published" }))
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
            .store
            .get_version(&p.workflow_id, p.version)
            .await
            .map_err(store_err)?;
        let published_version = state
            .store
            .latest_published(&p.workflow_id)
            .await
            .map_err(store_err)?;
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
        let list = state.store.list_workflows().await.map_err(store_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflows": list }))
    })?;

    module.register_async_method("workflow.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
        }
        let p: P = parse(&params)?;
        state
            .store
            .delete_workflow(&p.workflow_id)
            .await
            .map_err(store_err)?;
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

        let version = match p.version {
            Some(version) => version,
            None => state
                .store
                .latest_published(&p.workflow_id)
                .await
                .map_err(store_err)?
                .ok_or_else(|| {
                    invalid(format!(
                        "工作流 {} 没有已发布版本，先 publish 再执行",
                        p.workflow_id
                    ))
                })?,
        };
        let stored = state
            .store
            .get_version(&p.workflow_id, Some(version))
            .await
            .map_err(store_err)?;
        if !stored.is_published() {
            return Err(store_err(StoreError::VersionNotPublished(p.workflow_id, version)));
        }
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;

        let input = p.input.unwrap_or(Value::Null);
        let run_id = uuid::Uuid::now_v7().to_string();
        state
            .store
            .insert_run(&run_id, &p.workflow_id, version, &input, DbRunStatus::Initializing.as_str())
            .await
            .map_err(store_err)?;

        let spec = StartRun {
            run_id: run_id.clone(),
            workflow_id: p.workflow_id.clone(),
            workflow_version: version,
            definition,
            input,
        };
        if let Err(err) = state.engine.start_run(spec).await {
            let message = err.to_string();
            if let Err(write_err) = state
                .store
                .set_run_status(&run_id, DbRunStatus::Failed.as_str(), None, Some(&message))
                .await
            {
                tracing::error!(run_id = %run_id, error = %write_err, "回写 run failed 状态失败");
            }
            return Err(engine_err(err));
        }
        Ok::<_, ErrorObjectOwned>(json!({ "run_id": run_id, "workflow_version": version }))
    })?;

    module.register_async_method("run.get", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
        }
        let p: P = parse(&params)?;
        let run = state.store.get_run(&p.run_id).await.map_err(store_err)?;
        Ok::<_, ErrorObjectOwned>(json!({
            "run": run,
            "live": state.engine.is_live(&p.run_id),
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
            .store
            .list_runs(p.workflow_id.as_deref(), p.limit.unwrap_or(50).clamp(1, 500))
            .await
            .map_err(store_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "runs": runs }))
    })?;

    // 只读时间线：定义顺序 + 折叠后的节点状态，前端直接画列表
    module.register_async_method("run.timeline", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
        }
        let p: P = parse(&params)?;
        let run = state.store.get_run(&p.run_id).await.map_err(store_err)?;
        let stored = state
            .store
            .get_version(&run.workflow_id, Some(run.workflow_version))
            .await
            .map_err(store_err)?;
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| internal(format!("定义结构非法：{e}")))?;
        let snapshot = state
            .engine
            .snapshot(&p.run_id)
            .await
            .map_err(|e| internal(format!("读取事件日志失败：{e}")))?;

        Ok::<_, ErrorObjectOwned>(timeline_value(&run, &definition, &snapshot))
    })?;

    module.register_async_method("run.events", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            #[serde(default)]
            from_seq: Option<u64>,
        }
        let p: P = parse(&params)?;
        let events: Vec<Envelope> = state
            .engine
            .read_events(&p.run_id, p.from_seq)
            .await
            .map_err(engine_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "events": events }))
    })?;

    module.register_async_method("run.cancel", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
        }
        let p: P = parse(&params)?;
        if state.engine.cancel(&p.run_id).await {
            return Ok::<_, ErrorObjectOwned>(json!({ "cancelling": true }));
        }
        let run = state.store.get_run(&p.run_id).await.map_err(store_err)?;
        Err(conflict(format!(
            "run {} 当前不在运行中（状态 {}）",
            p.run_id, run.status
        )))
    })?;

    // human_task 交付信号；崩溃残留的副作用节点用 payload.action = retry/succeeded/failed 裁决
    module.register_async_method("run.signal", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            node_id: String,
            #[serde(default)]
            payload: Value,
        }
        let p: P = parse(&params)?;
        state
            .engine
            .signal(
                &p.run_id,
                Signal {
                    node_id: p.node_id,
                    payload: p.payload,
                },
            )
            .await
            .map_err(|e| conflict(e.to_string()))?;
        Ok::<_, ErrorObjectOwned>(json!({ "delivered": true }))
    })?;

    // 执行进度推送（JSON-RPC 2.0 订阅通知）
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
            let mut events = state.engine.subscribe();
            let sink = match pending.accept().await {
                Ok(sink) => sink,
                Err(_) => return,
            };
            loop {
                tokio::select! {
                    _ = sink.closed() => break,
                    received = events.recv() => match received {
                        Ok(envelope) => {
                            if let Some(filter) = &filter {
                                if envelope.run_id != *filter {
                                    continue;
                                }
                            }
                            match SubscriptionMessage::from_json(&envelope) {
                                Ok(message) => {
                                    if sink.send(message).await.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    },
                }
            }
        },
    )?;

    Ok(module)
}

fn timeline_value(
    run: &flow_store::RunRecord,
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
            });
            if let flow_engine::NodeState::Skipped { reason } = &record.state {
                entry["reason"] = json!(reason);
            }
            entry
        })
        .collect();

    json!({
        "run_id": run.id,
        "status": run.status,
        "phase": snapshot.phase,
        "workflow_id": run.workflow_id,
        "workflow_version": run.workflow_version,
        "started_at": snapshot.started_at,
        "ended_at": snapshot.ended_at,
        "output": snapshot.output,
        "fatal_error": snapshot.fatal_error,
        "last_seq": snapshot.last_seq,
        "nodes": nodes,
    })
}

/// 前端拖拽面板 + 参数表单所需的能力清单。
fn node_types() -> Value {
    json!([
        {
            "type": "start",
            "label": "开始",
            "category": "control",
            "max_instances": 1,
            "ports": [{"id": "out", "label": "出"}],
            "params": []
        },
        {
            "type": "end",
            "label": "结束",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}],
            "params": []
        },
        {
            "type": "script",
            "label": "脚本",
            "category": "compute",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params": [
                {"name": "code", "label": "JS 函数体", "kind": "code", "required": true,
                 "help": "可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果"},
                {"name": "timeout_ms", "label": "超时（毫秒）", "kind": "number", "default": 2000}
            ],
            "supports_retry": true
        },
        {
            "type": "condition",
            "label": "条件分支",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}, {"id": "true", "label": "真"}, {"id": "false", "label": "假"}],
            "params": [
                {"name": "expr", "label": "条件表达式", "kind": "code", "required": true,
                 "help": "表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes"},
                {"name": "timeout_ms", "label": "超时（毫秒）", "kind": "number", "default": 2000}
            ]
        },
        {
            "type": "delay",
            "label": "等待",
            "category": "control",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params": [{"name": "ms", "label": "时长（毫秒）", "kind": "number", "required": true}]
        },
        {
            "type": "http_call",
            "label": "HTTP 请求",
            "category": "integration",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params": [
                {"name": "method", "label": "方法", "kind": "select", "options": HTTP_METHODS, "default": "GET"},
                {"name": "url", "label": "URL", "kind": "text", "required": true, "help": "支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）"},
                {"name": "headers", "label": "请求头", "kind": "json", "default": {}},
                {"name": "body", "label": "请求体", "kind": "json"},
                {"name": "timeout_ms", "label": "超时（毫秒）", "kind": "number", "default": 30000}
            ],
            "supports_retry": true,
            "side_effect": true
        },
        {
            "type": "human_task",
            "label": "人工节点",
            "category": "human",
            "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
            "params": [{"name": "prompt", "label": "提示", "kind": "text"}]
        }
    ])
}

/// 具名参数解析。JSON-RPC 允许整个省略 params，此时到达的是 null，等价于空对象。
fn parse<T: serde::de::DeserializeOwned>(params: &Params<'_>) -> Result<T, ErrorObjectOwned> {
    let raw: Value = params
        .parse::<Value>()
        .map_err(|e| invalid(format!("参数非法：{}", e.message())))?;
    let raw = if raw.is_null() { json!({}) } else { raw };
    serde_json::from_value(raw).map_err(|e| invalid(format!("参数非法：{e}")))
}

fn invalid(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned(CODE_INVALID, message.into(), None::<()>)
}

fn conflict(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned(CODE_CONFLICT, message.into(), None::<()>)
}

fn internal(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned(CODE_INTERNAL, message.into(), None::<()>)
}

fn store_err(err: StoreError) -> ErrorObjectOwned {
    match err {
        StoreError::WorkflowNotFound(_)
        | StoreError::VersionNotFound(..)
        | StoreError::RunNotFound(_) => ErrorObject::owned(CODE_NOT_FOUND, err.to_string(), None::<()>),
        StoreError::VersionNotPublished(..) => conflict(err.to_string()),
        other => internal(other.to_string()),
    }
}

fn engine_err(err: EngineError) -> ErrorObjectOwned {
    match err {
        EngineError::RunNotFound(_) => ErrorObject::owned(CODE_NOT_FOUND, err.to_string(), None::<()>),
        EngineError::RunExists(_) => conflict(err.to_string()),
        EngineError::InvalidDefinition(_) | EngineError::Node(_) | EngineError::Expr(_) => {
            invalid(err.to_string())
        }
        other => internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 状态词汇表契约：引擎 DbRunStatus 的每个取值都必须被 store 接受；
    /// 词汇表外的字符串必须在写入时被拒绝——否则 run 会从崩溃恢复扫描里静默消失。
    #[tokio::test]
    async fn engine_run_statuses_are_the_only_statuses_store_accepts() {
        let root = std::env::temp_dir()
            .join(format!("flow-rpc-status-contract-{}", uuid::Uuid::now_v7()));
        let store = Store::open(root.join("flow.db")).await.unwrap();

        let statuses = [
            DbRunStatus::Initializing,
            DbRunStatus::Running,
            DbRunStatus::AwaitingResume,
            DbRunStatus::Succeeded,
            DbRunStatus::Failed,
            DbRunStatus::Cancelled,
        ];
        for (i, status) in statuses.iter().enumerate() {
            let run_id = format!("r-{i}");
            store
                .insert_run(&run_id, "wf", 1, &Value::Null, status.as_str())
                .await
                .unwrap();
            store
                .set_run_status(&run_id, status.as_str(), None, None)
                .await
                .unwrap();
        }

        // 非终态必须进入未完成扫描（这是恢复的输入）
        assert_eq!(store.unfinished_runs().await.unwrap().len(), 3);

        // 词汇表外的状态在写入时当场报错
        let err = store
            .insert_run("r-unknown", "wf", 1, &Value::Null, "paused")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("paused"), "{err}");

        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
