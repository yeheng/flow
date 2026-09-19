//! Postgres 模式的 RPC 层：gateway 职责（DISTRIBUTED.md §3、§6、§8）。
//!
//! 与 SQLite 模式的差异：
//! - `run.start` 是单事务原子创建，run 直接进入 running（已入队，不保证容量）；
//! - `run.signal` 必须带稳定 `signal_id`，入队后等待落账；pending 不是错误；
//! - `run.signal_status` 查询 inbox 落账状态；
//! - `run.cancel` 经持久 inbox 消费，不由 gateway 直接清租约；
//! - `run.subscribe` 按 run_id 维护 last_seq 轮询增量（§8）。

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use flow_engine::{Definition, RunState};
use flow_pg::gateway::SignalAck;
use flow_pg::{PgEngine, PgError};
use jsonrpsee::core::RegisterMethodError;
use jsonrpsee::server::{Server, ServerHandle, SubscriptionMessage};
use jsonrpsee::types::error::ErrorObjectOwned;
use jsonrpsee::RpcModule;
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

use crate::{conflict, internal, invalid, node_types, parse, timeline_value};

#[derive(Debug, Error)]
pub enum PgRpcError {
    #[error("注册 RPC 方法失败：{0}")]
    Register(#[from] RegisterMethodError),
    #[error("监听失败：{0}")]
    Io(#[from] std::io::Error),
    #[error("后端错误：{0}")]
    Engine(#[from] PgError),
}

pub struct PgState {
    pub engine: Arc<PgEngine>,
}

pub async fn serve(
    state: Arc<PgState>,
    addr: SocketAddr,
) -> Result<(ServerHandle, SocketAddr), PgRpcError> {
    let module = build_module(state)?;
    let server = Server::builder().build(addr).await?;
    let local_addr = server.local_addr()?;
    let handle = server.start(module);
    Ok((handle, local_addr))
}

pub fn build_module(state: Arc<PgState>) -> Result<RpcModule<Arc<PgState>>, PgRpcError> {
    let mut module: RpcModule<Arc<PgState>> = RpcModule::new(state);

    // ---- 工作流定义 ----
    module.register_async_method("workflow.create", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            name: String,
        }
        let p: P = parse(&params)?;
        let workflow_id = state
            .engine
            .store()
            .create_workflow(&p.name)
            .await
            .map_err(PgErr)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflow_id": workflow_id }))
    })?;

    module.register_async_method("workflow.update", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
            definition: Value,
        }
        let p: P = parse(&params)?;
        let definition: Definition = serde_json::from_value(p.definition.clone())
            .map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;
        let version = state
            .engine
            .store()
            .update_workflow(&p.workflow_id, &p.definition)
            .await
            .map_err(PgErr)?;
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
            .engine
            .store()
            .get_version(&p.workflow_id, Some(p.version))
            .await
            .map_err(PgErr)?;
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| invalid(format!("定义结构非法：{e}")))?;
        definition.validate().map_err(invalid)?;
        state
            .engine
            .store()
            .publish(&p.workflow_id, p.version)
            .await
            .map_err(PgErr)?;
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
            .engine
            .store()
            .get_version(&p.workflow_id, p.version)
            .await
            .map_err(PgErr)?;
        let published_version = state
            .engine
            .store()
            .latest_published(&p.workflow_id)
            .await
            .map_err(PgErr)?;
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
        let list = state.engine.store().list_workflows().await.map_err(PgErr)?;
        Ok::<_, ErrorObjectOwned>(json!({ "workflows": list }))
    })?;

    module.register_async_method("workflow.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
        }
        let p: P = parse(&params)?;
        state
            .engine
            .store()
            .delete_workflow(&p.workflow_id)
            .await
            .map_err(PgErr)?;
        Ok::<_, ErrorObjectOwned>(json!({ "deleted": true }))
    })?;

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
            .engine
            .create_run(flow_pg::CreateRun {
                workflow_id: p.workflow_id,
                version: p.version,
                input: p.input.unwrap_or(Value::Null),
            })
            .await
            .map_err(PgErr)?;
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
            .engine
            .store()
            .get_run(&p.run_id)
            .await
            .map_err(PgErr)?;
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
            .engine
            .store()
            .list_runs(
                p.workflow_id.as_deref(),
                p.limit.unwrap_or(50).clamp(1, 500),
            )
            .await
            .map_err(PgErr)?;
        Ok::<_, ErrorObjectOwned>(json!({ "runs": runs }))
    })?;

    module.register_async_method("run.timeline", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
        }
        let p: P = parse(&params)?;
        let run = state
            .engine
            .store()
            .get_run(&p.run_id)
            .await
            .map_err(PgErr)?;
        let stored = state
            .engine
            .store()
            .get_version(&run.workflow_id, Some(run.workflow_version))
            .await
            .map_err(PgErr)?;
        let definition: Definition = serde_json::from_value(stored.definition)
            .map_err(|e| internal(format!("定义结构非法：{e}")))?;
        let snapshot: RunState = state.engine.snapshot(&p.run_id).await.map_err(PgErr)?;
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
            .engine
            .read_events(&p.run_id, p.from_seq)
            .await
            .map_err(PgErr)?;
        Ok::<_, ErrorObjectOwned>(json!({ "events": events }))
    })?;

    // 取消经持久 inbox：消费事务写 RunCancelled + 终态投影 + 释放租约（§6.2）
    module.register_async_method("run.cancel", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            #[serde(default)]
            signal_id: Option<String>,
        }
        let p: P = parse(&params)?;
        let ack = state
            .engine
            .cancel(&p.run_id, p.signal_id)
            .await
            .map_err(PgErr)?;
        signal_ack_value(ack)
    })?;

    // 信号入队 + 等待落账（§6.1）。Postgres 模式必须带稳定 signal_id。
    module.register_async_method("run.signal", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            signal_id: String,
            node_id: String,
            #[serde(default)]
            payload: Value,
        }
        let p: P = parse(&params)?;
        let ack = state
            .engine
            .signal(&p.run_id, &p.signal_id, &p.node_id, &p.payload)
            .await
            .map_err(PgErr)?;
        signal_ack_value(ack)
    })?;

    module.register_async_method("run.signal_status", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            run_id: String,
            signal_id: String,
        }
        let p: P = parse(&params)?;
        let ack = state
            .engine
            .signal_status(&p.run_id, &p.signal_id)
            .await
            .map_err(PgErr)?;
        Ok::<_, ErrorObjectOwned>(json!({
            "signal_id": ack.signal_id,
            "status": ack.status,
            "delivered": ack.delivered,
            "event_seq": ack.event_seq,
            "error": ack.error,
        }))
    })?;

    // 订阅：按 run_id 维护 last_seq 轮询增量（§8）。游标只在确认转发后推进，
    // 重复消息按 seq 去重（游标本体），缺口由 run.events 补齐。
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
            let Ok(sink) = pending.accept().await else {
                return;
            };
            let poll = state.engine.config().subscribe_poll;
            // 时钟偏差留 5s 余量：捕获订阅开始前后的新 run
            let sub_start = chrono::Utc::now() - chrono::Duration::seconds(5);
            // run_id -> 已确认转发的 last_seq
            let mut cursors: HashMap<String, u64> = HashMap::new();
            // 已终结且追平、不必再查的 run
            let mut drained: HashSet<String> = HashSet::new();
            loop {
                if sink.is_closed() {
                    tracing::debug!("pg subscribe: sink closed, exit");
                    break;
                }
                // 候选：过滤指定的 run，或（活跃 ∪ 订阅后创建）∪ 尚在游标中的 run
                // 短 run 可能在两次轮询之间走完一生：按 started_at 捕获订阅开始后的新 run。
                let mut candidates: Vec<(String, bool)> = Vec::new();
                if let Some(run_id) = &filter {
                    let terminal = match state.engine.store().get_run(run_id).await {
                        Ok(run) => !matches!(run.status.as_str(), "running" | "awaiting_resume"),
                        Err(_) => true,
                    };
                    candidates.push((run_id.clone(), terminal));
                } else {
                    let watched = state.engine.watch_runs(sub_start).await.unwrap_or_default();
                    for (run_id, terminal) in watched {
                        candidates.push((run_id, terminal));
                    }
                    for run_id in cursors.keys() {
                        if !candidates.iter().any(|(id, _)| id == run_id) {
                            candidates.push((run_id.clone(), true));
                        }
                    }
                }
                for (run_id, terminal) in candidates {
                    if drained.contains(&run_id) {
                        continue;
                    }
                    let head = cursors.entry(run_id.clone()).or_insert(0);
                    let events = match state.engine.read_events(&run_id, Some(*head + 1)).await {
                        Ok(events) => events,
                        Err(_) => continue,
                    };
                    for envelope in &events {
                        // 只推进已确认转发的游标；转发失败即断开
                        match SubscriptionMessage::from_json(envelope) {
                            Ok(message) => {
                                if sink.send(message).await.is_err() {
                                    return;
                                }
                                *head = envelope.seq;
                            }
                            Err(_) => return,
                        }
                    }
                    if terminal && events.is_empty() {
                        // 终结且已追平：丢弃游标
                        cursors.remove(&run_id);
                        drained.insert(run_id.clone());
                        // 已终结 run 无限累积会撑爆订阅生命周期内的内存；
                        // 超阈值整体清空只损失一点轮询冗余（终态 run 重查一次即空）
                        if drained.len() > 4096 {
                            drained.clear();
                            tracing::warn!("订阅 drained 集超过 4096，整体清空");
                        }
                        if filter.is_some() {
                            return; // 指定 run 已终结：订阅自然结束
                        }
                    }
                }
                tokio::time::sleep(poll).await;
            }
        },
    )?;

    Ok(module)
}

/// 信号/取消请求的响应语义（§6.1）：
/// delivered=true 才是交付；rejected 返回 invalid/conflict 错误体系；
/// pending 返回明确的 pending 结果和 signal_id，客户端用 run.signal_status 查询。
fn signal_ack_value(ack: SignalAck) -> Result<Value, ErrorObjectOwned> {
    if ack.delivered {
        return Ok(json!({
            "delivered": true,
            "signal_id": ack.signal_id,
            "event_seq": ack.event_seq,
        }));
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
    Ok(json!({
        "delivered": false,
        "pending": true,
        "signal_id": ack.signal_id,
        "status": ack.status,
    }))
}

/// PgError → JSON-RPC 错误。
fn pg_err(err: PgError) -> ErrorObjectOwned {
    use jsonrpsee::types::error::ErrorObject;
    const CODE_NOT_FOUND: i32 = -32011;
    match &err {
        PgError::RunNotFound(_) => ErrorObject::owned(CODE_NOT_FOUND, err.to_string(), None::<()>),
        PgError::Conflict(_) => conflict(err.to_string()),
        PgError::Invalid(_) => invalid(err.to_string()),
        _ => internal(err.to_string()),
    }
}

/// 本地包装以绕过 orphan rule：`?` 在闭包里直接把 PgError 转成 RPC 错误。
pub(crate) struct PgErr(pub PgError);

impl From<PgErr> for ErrorObjectOwned {
    fn from(err: PgErr) -> Self {
        pg_err(err.0)
    }
}
