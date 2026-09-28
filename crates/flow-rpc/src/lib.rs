//! flow-rpc：jsonrpsee WebSocket 服务（bin: flow-server）。
//!
//! 适配层重构后的职责边界（DESIGN.md §2）：
//! - 本 crate **只依赖 flow-backend 的 `AnyBackend` 闭集枚举**，不感知
//!   flow-store / flow-pg，也不读取 FLOW_BACKEND——后端选择在 main +
//!   `flow_backend::open_from_env()` 完成一次，之后对 RPC 层完全透明；
//! - SQLite + event.jsonl（canonical）与 Postgres（可替代）的语义差异
//!   （初始化协议、信号落账、订阅推送）由 flow-backend 吸收，这里的每个
//!   RPC 方法只有一份实现。

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Local;
use flow_backend::{
    open_from_env, secrets, AnyBackend, BackendError, Definition, NodeState, NodeType, RunState,
    SignalAck,
};
use futures::StreamExt;
use jsonrpsee::core::RegisterMethodError;

/// 进程入口逃逸口：backend-perf 的自举服务模式与 flow-server 二进制共用的
/// runtime 形态提示（sqlite = current_thread）。见 flow_backend 同名函数。
pub use flow_backend::prefer_current_thread_runtime;
use jsonrpsee::server::{Server, ServerHandle, SubscriptionMessage};
use jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned};
use jsonrpsee::types::Params;
use jsonrpsee::RpcModule;
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

pub mod scheduler;
pub mod webhook;

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

/// 进程入口三件套：从环境变量装配后端 → JSON-RPC WebSocket → cron 调度器 →
/// webhook HTTP，运行到中断信号为止。
///
/// `flow-server` 二进制、backend-e2e 的被测进程、backend-perf 的自举服务模式
/// 共用这一份实现，保证「被测的就是生产进程」。tracing 在这里初始化（进程入口
/// 只有一个调用点，不会重复 init）。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,flow_engine=debug,flow_rpc=debug,flow_backend=debug".into()
            }),
        )
        .init();

    let addr: SocketAddr = std::env::var("FLOW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9800".into())
        .parse()?;

    // 后端选择只发生在这里一次：FLOW_BACKEND=sqlite（缺省，canonical：
    // SQLite + event.jsonl）| postgres（可替代：共享日志 + 租约 + inbox）。
    // 之后整条 RPC 链路只看 AnyBackend 枚举。
    let backend = open_from_env().await?;
    backend.start().await?;

    let state = Arc::new(AppState {
        backend: backend.clone(),
    });
    let (handle, local_addr) = serve(state.clone(), addr).await?;
    tracing::info!(
        %local_addr,
        backend = backend.name(),
        detail = backend.describe(),
        "flow-server 已启动 (JSON-RPC 2.0 over WebSocket)"
    );

    // cron 调度器：默认开启，FLOW_SCHEDULER=off 禁用
    let scheduler_task = if std::env::var("FLOW_SCHEDULER").as_deref() == Ok("off") {
        tracing::info!("FLOW_SCHEDULER=off，cron 调度器未启动");
        None
    } else {
        let backend = backend.clone();
        Some(tokio::spawn(async move { scheduler::run(backend).await }))
    };

    // webhook HTTP 入口：FLOW_HTTP_ADDR，默认 127.0.0.1:9801
    let http_addr: SocketAddr = std::env::var("FLOW_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9801".into())
        .parse()?;
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!(%http_addr, "webhook HTTP 监听已启动 (POST /hook/:token)");
    let http_task = tokio::spawn(async move {
        if let Err(err) = axum::serve(http_listener, webhook::router(state)).await {
            tracing::error!(error = %err, "webhook HTTP 服务退出");
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
    if let Some(task) = scheduler_task {
        task.abort();
    }
    http_task.abort();
    // 停机错误只能记日志：进程即将退出，没有重试的意义
    if let Err(err) = backend.shutdown().await {
        tracing::error!(error = %err, "后端停机失败");
    }
    handle.stop()?;
    handle.stopped().await;
    Ok(())
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

        // x-secret 参数只存名称：落库前确认每个名称都有对应的
        // FLOW_SECRET_<名称> 环境变量，把配置错误挡在发布前
        let missing = secrets::missing_secrets(&definition);
        if !missing.is_empty() {
            let detail = missing
                .iter()
                .map(|(node_id, name)| {
                    format!(
                        "节点 {node_id} 引用 {name}（请设置环境变量 {}{name}）",
                        secrets::SECRET_ENV_PREFIX
                    )
                })
                .collect::<Vec<_>>()
                .join("；");
            return Err(invalid(format!("引用的密钥未配置：{detail}")));
        }

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

    // 版本历史（倒序）：只回元数据列，definition 走 workflow.get 按需拉取
    module.register_async_method("workflow.versions", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
        }
        let p: P = parse(&params)?;
        let versions = state
            .backend
            .list_versions(&p.workflow_id)
            .await
            .map_err(backend_err)?;
        let versions: Vec<Value> = versions
            .into_iter()
            .map(|v| {
                json!({
                    "workflow_id": v.workflow_id,
                    "version": v.version,
                    "status": v.status,
                    "checksum": v.checksum,
                    "created_at": v.created_at,
                })
            })
            .collect();
        Ok::<_, ErrorObjectOwned>(json!({ "versions": versions }))
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

    // 密钥名称清单（永远不含值）：前端给 x-secret 参数渲染可选名称
    module.register_method("secrets.list", |params, _state, _| {
        let _: Value = parse(&params)?;
        Ok::<_, ErrorObjectOwned>(json!({ "secrets": secrets::list_secret_names() }))
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
                source: flow_backend::DbRunSource::Manual.as_str().to_string(),
                source_detail: None,
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
            status: Option<String>,
            /// 触发来源过滤（manual / schedule / webhook / sub_workflow）
            #[serde(default)]
            source: Option<String>,
            /// 游标分页：返回该 run 之前更旧的记录
            #[serde(default)]
            before_run_id: Option<String>,
            #[serde(default)]
            limit: Option<i64>,
        }
        let p: P = parse(&params)?;
        // 客户端给的 status/source 过滤词必须在对应词汇表内（-32010）
        if let Some(status) = &p.status {
            if !flow_backend::DbRunStatus::is_valid_str(status) {
                return Err(invalid(format!("非法的 run 状态：{status}")));
            }
        }
        if let Some(source) = &p.source {
            if !flow_backend::DbRunSource::is_valid_str(source) {
                return Err(invalid(format!("非法的 run 来源：{source}")));
            }
        }
        let runs = state
            .backend
            .list_runs(
                p.workflow_id.as_deref(),
                p.status.as_deref(),
                p.source.as_deref(),
                p.before_run_id.as_deref(),
                p.limit.unwrap_or(50).clamp(1, 500),
            )
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "runs": runs }))
    })?;

    // 精确统计（GROUP BY）：仪表盘全局与单工作流共用
    module.register_async_method("run.stats", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            #[serde(default)]
            workflow_id: Option<String>,
        }
        let p: P = parse(&params)?;
        let stats = state
            .backend
            .run_stats(p.workflow_id.as_deref())
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!(stats))
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

    // ---- 触发器：cron 调度与 webhook ----
    // cron 合法性在本层校验（-32010）；next_fire_at 服务端算好（本地时间 cron →
    // RFC3339），前端不解析 cron。

    module.register_async_method("schedule.create", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
            cron: String,
            #[serde(default)]
            input: Option<Value>,
            #[serde(default)]
            enabled: Option<bool>,
        }
        let p: P = parse(&params)?;
        validate_cron(&p.cron)?;
        let schedule = state
            .backend
            .create_schedule(
                &p.workflow_id,
                &p.cron,
                p.input.as_ref(),
                p.enabled.unwrap_or(true),
            )
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(schedule_value(&schedule))
    })?;

    module.register_async_method("schedule.list", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            #[serde(default)]
            workflow_id: Option<String>,
        }
        let p: P = parse(&params)?;
        let schedules = state
            .backend
            .list_schedules(p.workflow_id.as_deref())
            .await
            .map_err(backend_err)?;
        let schedules: Vec<Value> = schedules.iter().map(schedule_value).collect();
        Ok::<_, ErrorObjectOwned>(json!({ "schedules": schedules }))
    })?;

    // 部分更新：cron/enabled 缺省不动；input 用双 Option——缺省=不改，null=清空。
    module.register_async_method("schedule.update", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            id: String,
            #[serde(default)]
            cron: Option<String>,
            #[serde(default, deserialize_with = "double_option")]
            input: Option<Option<Value>>,
            #[serde(default)]
            enabled: Option<bool>,
        }
        let p: P = parse(&params)?;
        if let Some(cron) = &p.cron {
            validate_cron(cron)?;
        }
        state
            .backend
            .update_schedule(&p.id, p.cron.as_deref(), p.input, p.enabled)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "updated": true }))
    })?;

    module.register_async_method("schedule.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            id: String,
        }
        let p: P = parse(&params)?;
        state
            .backend
            .delete_schedule(&p.id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "deleted": true }))
    })?;

    module.register_async_method("webhook.create", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            workflow_id: String,
        }
        let p: P = parse(&params)?;
        let webhook = state
            .backend
            .create_webhook(&p.workflow_id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!(webhook))
    })?;

    module.register_async_method("webhook.list", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            #[serde(default)]
            workflow_id: Option<String>,
        }
        let p: P = parse(&params)?;
        let webhooks = state
            .backend
            .list_webhooks(p.workflow_id.as_deref())
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "webhooks": webhooks }))
    })?;

    module.register_async_method("webhook.set_enabled", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            token: String,
            enabled: bool,
        }
        let p: P = parse(&params)?;
        state
            .backend
            .set_webhook_enabled(&p.token, p.enabled)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "updated": true }))
    })?;

    module.register_async_method("webhook.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            token: String,
        }
        let p: P = parse(&params)?;
        state
            .backend
            .delete_webhook(&p.token)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "deleted": true }))
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
            // output 展示出口脱敏（固定敏感键）：事件日志里的原始 output 是
            // 下游节点的数据面，不能动；这里只脱时间线的展示值。
            // input 在 node_started 写入时已脱敏，直接透传。
            let output = record.output.as_ref().map(flow_backend::redact_value);
            let mut entry = json!({
                "id": node.id,
                "name": node.name,
                "type": node.node_type,
                "state": record.state.label(),
                "attempts": record.attempts,
                "started_at": record.started_at,
                "ended_at": record.ended_at,
                "duration_ms": record.duration_ms,
                "input": record.input,
                "output": output,
                "error": record.error,
                "child_run_id": record.child_run_id,
            });
            if let NodeState::Skipped { reason } = &record.state {
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
        // run 级输出与节点输出同一脱敏标准：timeline 是展示面，
        // 不能节点输出是 *** 而十行之下 run 输出就是明文（run.get 是数据面，另论）
        "output": snapshot.output.as_ref().map(flow_backend::redact_value),
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
/// 单条描述的唯一来源是 `flow_engine::NodeType::descriptor`（与引擎共用一份
/// 定义），这里只负责按序组装；schema 约定见 descriptor 的文档注释。
pub(crate) fn node_types() -> Value {
    json!(NodeType::ALL
        .iter()
        .map(|kind| kind.descriptor())
        .collect::<Vec<Value>>())
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

/// schedule.update 的 input 用双 Option 区分「字段缺失=不改」与「显式 null=清空」。
/// serde 原生会把两者都收成 None，所以需要自定义反序列化把外层 Some 钉上。
fn double_option<'de, D>(deserializer: D) -> Result<Option<Option<Value>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::<Value>::deserialize(deserializer)?))
}

/// cron 合法性校验（标准 5 字段，分 时 日 月 周，本地时间）。非法表达式 -32010。
fn validate_cron(expr: &str) -> Result<(), ErrorObjectOwned> {
    expr.parse::<cron_parser::Schedule>()
        .map(|_| ())
        .map_err(|e| invalid(format!("非法的 cron 表达式：{e}")))
}

/// schedule 响应：实体字段 + 服务端算好的 next_fire_at（RFC3339，本地时区偏移）。
/// 存量数据 cron 损坏时 next_fire_at 为 null，不让 list 整个失败。
fn schedule_value(schedule: &flow_backend::Schedule) -> Value {
    let next_fire_at = schedule
        .cron_expr
        .parse::<cron_parser::Schedule>()
        .ok()
        .and_then(|cron| cron.next_after(&Local::now()))
        .map(|t| t.to_rfc3339());
    let mut value = json!(schedule);
    value["next_fire_at"] = json!(next_fire_at);
    value
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
        | BackendError::SignalNotFound(_)
        | BackendError::ScheduleNotFound(_)
        | BackendError::WebhookNotFound(_) => {
            ErrorObject::owned(CODE_NOT_FOUND, err.to_string(), None::<()>)
        }
        BackendError::VersionNotPublished(..) | BackendError::Conflict(_) => {
            conflict(err.to_string())
        }
        BackendError::Invalid(_) => invalid(err.to_string()),
        BackendError::Internal(_) => internal(err.to_string()),
    }
}

/// `nodetypes.list` 响应快照：descriptor 注册表收敛（model.rs `NodeType::descriptor`）
/// 是纯重构，响应必须逐字节不变。新增/修改节点类型时同步更新本快照。
#[cfg(test)]
mod tests {
    #[test]
    fn node_types_snapshot_is_stable() {
        const SNAPSHOT: &str = r#"[{"category":"control","label":"开始","max_instances":1,"params_schema":{"properties":{},"type":"object"},"ports":[{"id":"out","label":"出"}],"type":"start"},{"category":"control","label":"结束","params_schema":{"properties":{},"type":"object"},"ports":[{"id":"in","label":"入"}],"type":"end"},{"category":"compute","label":"脚本","params_schema":{"properties":{"code":{"type":"string","x-help":"可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果","x-label":"JS 函数体","x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"脚本超时（毫秒）"}},"required":["code"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"script"},{"category":"control","label":"条件分支","params_schema":{"properties":{"expr":{"type":"string","x-help":"表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes","x-label":"条件表达式","x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"求值超时（毫秒）"}},"required":["expr"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"true","label":"真"},{"id":"false","label":"假"}],"type":"condition"},{"category":"control","label":"等待","params_schema":{"properties":{"ms":{"type":"integer","x-help":"数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）","x-label":"时长（毫秒）"}},"required":["ms"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"delay"},{"category":"integration","label":"HTTP 请求","params_schema":{"properties":{"body":{"x-label":"请求体","x-widget":"json"},"headers":{"default":{},"x-label":"请求头","x-widget":"json"},"method":{"default":"GET","enum":["GET","POST","PUT","PATCH","DELETE"],"type":"string","x-label":"方法"},"timeout_ms":{"default":30000,"type":"integer","x-label":"HTTP 超时（毫秒）"},"url":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）","x-label":"URL"}},"required":["url"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"http_call"},{"category":"human","label":"人工节点","params_schema":{"properties":{"prompt":{"type":"string","x-label":"提示"}},"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"human_task"},{"category":"control","label":"子工作流","params_schema":{"properties":{"input_mapping":{"x-help":"JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入","x-label":"子 run 输入映射","x-widget":"json"},"workflow_id":{"type":"string","x-help":"调用其最新已发布版本作为子 run；子 run 输出透传为本节点输出；子 run 失败传导为本节点 fatal（DESIGN §6.8），重试策略只覆盖启动/等待类错误","x-label":"目标工作流","x-widget":"workflow-picker"}},"required":["workflow_id"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"sub_workflow"},{"category":"ai","label":"LLM 调用","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 OPENAI_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"base_url":{"default":"https://api.openai.com/v1","type":"string","x-help":"OpenAI 兼容端点，请求发往 {base_url}/chat/completions","x-label":"API 地址"},"json_mode":{"default":false,"type":"boolean","x-help":"开启后请求带 response_format: {\"type\":\"json_object\"}","x-label":"JSON 模式"},"max_tokens":{"type":"integer","x-label":"最大 token 数"},"model":{"type":"string","x-label":"模型"},"prompt":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"提示词","x-widget":"code"},"system":{"type":"string","x-label":"系统提示"},"temperature":{"type":"number","x-label":"温度"}},"required":["api_key","model","prompt"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"llm"},{"category":"notify","label":"邮件","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"body":{"type":"string","x-help":"纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板","x-label":"正文","x-widget":"code"},"endpoint":{"default":"https://api.resend.com/emails","type":"string","x-help":"Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证","x-label":"API 端点"},"from":{"type":"string","x-label":"发件人"},"subject":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"主题"},"to":{"type":"string","x-label":"收件人"}},"required":["api_key","from","to","subject","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"email"}]"#;
        assert_eq!(
            serde_json::to_string(&crate::node_types()).unwrap(),
            SNAPSHOT,
            "nodetypes.list 响应变了：若是有意变更，更新本快照"
        );
    }
}
