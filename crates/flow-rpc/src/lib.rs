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
    secrets, AnyBackend, BackendError, Definition, Envelope, Event, NodeState, NodeType, RunState,
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

pub mod journal_download;
pub mod journal_triggers;
pub mod journal_v2;
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
    /// 统一配置面（config.get / config.update 的工作副本）。
    /// `Some` 时 update 可写回文件；`None`（测试简装）只回默认值。
    /// 这里的 config 是**文件级**视图（默认值 + 文件，不含 env 覆盖）——
    /// 文件才是用户可编辑的真相，env 覆盖名单单列展示。
    pub config: Option<ConfigState>,
    /// 持久化密钥存储（secrets.set/delete 落这里）；None 时这两个方法报
    /// 「未启用」。引擎侧取值经 secrets::get_secret（stored 优先，env 兜底）。
    pub secrets: Option<Arc<flow_engine::secrets_store::SecretFileStore>>,
}

/// 进程启动时装配的配置工作副本。
pub struct ConfigState {
    pub config: std::sync::RwLock<flow_config::Config>,
    /// 配置文件路径；None 时 update 会落到 `./flow.toml`。
    pub path: Option<std::path::PathBuf>,
    /// 启动时生效的 env 覆盖名单（仅名称，值可能含凭据不回传）。
    pub env_overrides: Vec<String>,
}

impl AppState {
    /// 测试与嵌入式简装入口：只有后端，无配置文件与密钥存储。
    pub fn new(backend: AnyBackend) -> AppState {
        AppState {
            backend,
            config: None,
            secrets: None,
        }
    }

    /// 产品入口：配置 + 密钥存储全量装配。密钥文件在 data_dir 下
    /// （secret.key + secrets.json）；安装进程级 SecretSource 失败（重复）
    /// 只告警——先装先得，第二个入口不该发生。
    pub async fn for_production(
        backend: AnyBackend,
        config_state: ConfigState,
        data_dir: &std::path::Path,
    ) -> (
        Arc<AppState>,
        Option<Arc<flow_engine::secrets_store::SecretFileStore>>,
    ) {
        let store = match flow_engine::secrets_store::SecretFileStore::open(data_dir) {
            Ok(store) => {
                let store = Arc::new(store);
                if let Err(err) = flow_engine::secrets::install_secret_source(store.clone()) {
                    tracing::warn!(%err, "持久化密钥来源安装失败，仅环境变量密钥生效");
                    None
                } else {
                    Some(store)
                }
            }
            Err(err) => {
                tracing::warn!(%err, "密钥存储初始化失败，secrets.set/delete 不可用");
                None
            }
        };
        (
            Arc::new(AppState {
                backend,
                config: Some(config_state),
                secrets: store.clone(),
            }),
            store,
        )
    }
}

/// config.get / config.update 的公共响应体。
/// `config.storage.database_url` 含凭据：一律脱敏为 `"<set>"` / null；
/// update 侧收到 `"<set>"` 表示保持原值不变。
fn config_view(state: &AppState) -> Value {
    let (config, path, env_overrides) = match &state.config {
        Some(cs) => (
            Some(cs.config.read().unwrap().clone()),
            cs.path.clone(),
            cs.env_overrides.clone(),
        ),
        None => (None, None, Vec::new()),
    };
    let mut config = config.unwrap_or_default();
    if config
        .storage
        .database_url
        .as_deref()
        .is_some_and(|v| !v.is_empty())
    {
        config.storage.database_url = Some("<set>".into());
    }
    json!({
        "config": config,
        "config_path": path,
        "env_overrides": env_overrides,
    })
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

/// 进程入口三件套（配置文件形态）：统一配置装配后端 → JSON-RPC WebSocket →
/// cron 调度器 → webhook HTTP，运行到中断信号为止。
///
/// `flow server` 二进制、backend-e2e 的被测进程、backend-perf 的自举服务模式
/// 共用这一份实现，保证「被测的就是生产进程」。tracing 在这里初始化（进程入口
/// 只有一个调用点，不会重复 init）。
pub async fn run(loaded: flow_config::Loaded) -> Result<(), Box<dyn std::error::Error>> {
    let config = loaded.config;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,flow_engine=debug,flow_rpc=debug,flow_backend=debug".into()
            }),
        )
        .init();

    let addr: SocketAddr = config.server.rpc_addr.parse()?;

    // 后端选择只发生在这里一次（storage.backend）。之后整条 RPC 链路只看
    // AnyBackend 枚举。
    let backend = flow_backend::open(&config).await?;
    backend.start().await?;

    // config.get/update 的工作副本：文件级视图（默认 + 文件，env 已在上层
    // 合并进运行值，不进这份副本）。
    let file_config = match &loaded.path {
        Some(path) => {
            let text = std::fs::read_to_string(path)?;
            toml_config_from_str(&text)?
        }
        None => flow_config::Config::default(),
    };
    let data_dir = std::path::PathBuf::from(&config.storage.data_dir);
    let config_path = loaded
        .path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "<none>".into());
    let config_state = ConfigState {
        config: std::sync::RwLock::new(file_config),
        path: loaded.path,
        env_overrides: loaded.env_overrides,
    };
    let (state, _secrets) =
        AppState::for_production(backend.clone(), config_state, &data_dir).await;
    let (handle, local_addr) = serve(state.clone(), addr).await?;
    tracing::info!(
        %local_addr,
        backend = backend.name(),
        detail = backend.describe(),
        config = %config_path,
        "flow-server 已启动 (JSON-RPC 2.0 over WebSocket)"
    );

    // cron 调度器：config.server.scheduler_enabled（env FLOW_SCHEDULER=off 已
    // 在加载时合并为 false）禁用
    let scheduler_task = if config.server.scheduler_enabled {
        let backend = backend.clone();
        let tick = config.scheduler_tick();
        Some(tokio::spawn(
            async move { scheduler::run(backend, tick).await },
        ))
    } else {
        tracing::info!("scheduler_enabled=false，cron 调度器未启动");
        None
    };

    // webhook HTTP 入口：server.http_addr
    let http_addr: SocketAddr = config.server.http_addr.parse()?;
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    // 记**实际绑到的**地址而非请求值：传 `127.0.0.1:0` 让内核分配端口时，
    // 记请求值会得到一条 `http_addr=127.0.0.1:0` 的假日志（端口 0 意味着
    // 「由内核选」，没人能从日志里知道实际端口）。测试 harness 靠这行的
    // `http_addr=` 字段名拿端口（flow-test-support::io 的 HTTP_MARKER），
    // 字段名不能改。
    let bound_http = http_listener.local_addr()?;
    tracing::info!(http_addr = %bound_http, "webhook HTTP 监听已启动 (POST /hook/:token)");
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

/// 环境变量形态的进程入口（backend-e2e / backend-perf 等测试脚手架用）：
/// 加载默认位置的配置文件（缺省即默认值）+ env 覆盖，然后走同一份 [`run`]。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    run(flow_config::Config::load(None)?).await
}

fn toml_config_from_str(text: &str) -> Result<flow_config::Config, Box<dyn std::error::Error>> {
    Ok(flow_config::Config::from_toml_str(text)?)
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

    // 密钥清单（永远不含值）：{name, source}，source=stored（界面管理）| env。
    // 前端给 x-secret 参数渲染可选名称，设置页按来源决定能否删除。
    module.register_method("secrets.list", |params, state, _| {
        let _: Value = parse(&params)?;
        let secrets: Vec<Value> = secrets::list_secrets()
            .into_iter()
            .map(|(name, source)| json!({ "name": name, "source": source }))
            .collect();
        let _ = state; // 读取走进程级 SecretSource，与 state.secrets 解耦
        Ok::<_, ErrorObjectOwned>(json!({ "secrets": secrets }))
    })?;

    // 写入（或覆盖）一个持久化密钥。真值只进内存与加密落盘，永不回显。
    module.register_async_method("secrets.set", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            name: String,
            value: String,
        }
        let p: P = parse(&params)?;
        validate_secret_name(&p.name)?;
        if p.value.is_empty() {
            return Err(invalid("密钥值不能为空"));
        }
        if p.value.len() > 8192 {
            return Err(invalid("密钥值过长（上限 8 KiB）"));
        }
        let store = state
            .secrets
            .as_ref()
            .ok_or_else(|| invalid("密钥存储未启用（需要 storage.data_dir 可写）"))?;
        store
            .set(&p.name, &p.value)
            .map_err(|e| internal(format!("密钥写入失败：{e}")))?;
        Ok::<_, ErrorObjectOwned>(json!({ "name": p.name, "source": "stored" }))
    })?;

    // 删除一个持久化密钥。env 来源的名字删不了（进程环境不可写），
    // 回 deleted=false 让前端提示。
    module.register_async_method("secrets.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            name: String,
        }
        let p: P = parse(&params)?;
        validate_secret_name(&p.name)?;
        let Some(store) = &state.secrets else {
            return Err(invalid("密钥存储未启用（需要 storage.data_dir 可写）"));
        };
        let deleted = store
            .delete(&p.name)
            .map_err(|e| internal(format!("密钥删除失败：{e}")))?;
        Ok::<_, ErrorObjectOwned>(json!({ "name": p.name, "deleted": deleted }))
    })?;

    // ---- 统一配置（重启生效；文件为可编辑真相，env 覆盖单列） ----
    module.register_method("config.get", |params, state, _| {
        let _: Value = parse(&params)?;
        Ok::<_, ErrorObjectOwned>(config_view(state))
    })?;

    module.register_async_method("config.update", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            patch: Value,
        }
        let p: P = parse(&params)?;
        let Some(cs) = &state.config else {
            return Err(invalid("此实例未启用配置文件读写，无法修改配置"));
        };
        // 合并基准是**文件级**配置（默认 + 文件），不是运行值——env 覆盖不
        // 烘焙进文件，重启后 env 仍然优先。
        let base = cs.config.read().unwrap().clone();
        // database_url 的 "<set>" 哨兵（config.get 脱敏产物）表示「保持原值」：
        // 替换回基准值再合并，否则哨兵字面量会落盘。
        let mut patch = p.patch;
        if patch.get("storage").and_then(|s| s.get("database_url")) == Some(&json!("<set>")) {
            if let Some(storage) = patch.get_mut("storage").and_then(|s| s.as_object_mut()) {
                storage.insert("database_url".into(), json!(base.storage.database_url));
            }
        }
        let merged =
            flow_config::Config::merge_patch(&base, &patch).map_err(|e| invalid(e.to_string()))?;
        let write_path = cs
            .path
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("flow.toml"));
        merged
            .save(&write_path)
            .map_err(|e| invalid(format!("配置写入失败：{e}")))?;
        *cs.config.write().unwrap() = merged;
        Ok::<_, ErrorObjectOwned>(config_view(&state))
    })?;

    // ---- 可复用节点模板（画布片段：节点 + 内部边，非完整 Definition） ----
    // 校验是**逐节点**的（类型已知 + 参数过 validate_params + 边端点在片段内），
    // 不跑 Definition::validate——片段本就没有 start/end 约束。
    module.register_async_method("template.create", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            name: String,
            #[serde(default)]
            category: Option<String>,
            nodes: Value,
            edges: Value,
        }
        let p: P = parse(&params)?;
        validate_template_name(&p.name)?;
        let (nodes, edges) = validate_fragment(&p.nodes, &p.edges)?;
        let template = state
            .backend
            .template_create(&p.name, p.category.as_deref(), &nodes, &edges)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!(template))
    })?;

    module.register_async_method("template.list", |params, state, _| async move {
        let _: Value = parse(&params)?;
        let templates = state.backend.template_list().await.map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "templates": templates }))
    })?;

    module.register_async_method("template.get", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            id: String,
        }
        let p: P = parse(&params)?;
        let template = state
            .backend
            .template_get(&p.id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!(template))
    })?;

    module.register_async_method("template.update", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            id: String,
            #[serde(default)]
            name: Option<String>,
            /// 双 Option：缺失 = 不改；null = 清空分组；字符串 = 设置
            #[serde(default, deserialize_with = "double_option")]
            category: Option<Option<String>>,
            #[serde(default)]
            nodes: Option<Value>,
            #[serde(default)]
            edges: Option<Value>,
        }
        let p: P = parse(&params)?;
        if let Some(name) = &p.name {
            validate_template_name(name)?;
        }
        let (nodes, edges) = match (&p.nodes, &p.edges) {
            (Some(nodes), Some(edges)) => {
                let (n, e) = validate_fragment(nodes, edges)?;
                (Some(n), Some(e))
            }
            // 只改信封不改片段：仍要保证存量片段成对一致
            (None, None) => (None, None),
            _ => return Err(invalid("nodes 与 edges 必须成对提供或不提供")),
        };
        let template = state
            .backend
            .template_update(
                &p.id,
                p.name.as_deref(),
                p.category.as_ref().map(|c| c.as_deref()),
                nodes.as_ref(),
                edges.as_ref(),
            )
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!(template))
    })?;

    module.register_async_method("template.delete", |params, state, _| async move {
        #[derive(Deserialize)]
        struct P {
            id: String,
        }
        let p: P = parse(&params)?;
        let deleted = state
            .backend
            .template_delete(&p.id)
            .await
            .map_err(backend_err)?;
        Ok::<_, ErrorObjectOwned>(json!({ "id": p.id, "deleted": deleted }))
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
                            // 展示出口脱敏：与 timeline 同一规则（见 redact_display_envelope）。
                            // 少了这一步，实时段推送的 node_completed 会带原始 output，
                            // 前端 applyEvent 用它覆盖 timeline 的脱敏值，脱敏被绕过
                            let envelope = redact_display_envelope(envelope);
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

/// 订阅通知的展示脱敏：`node_completed.output` 按与 [`timeline_value`] 同一
/// 规则脱敏后下发。
///
/// **为什么订阅也要脱敏**：事件日志里存的是原始 output（数据面，下游节点要
/// 消费，不能动），而展示面有两处——timeline 投影与订阅通知。只脱一处时，
/// 前端 `applyEvent(node_completed)` 会用事件里的原始值**覆盖** timeline 的
/// 脱敏值（monitor-logic.ts），实时观看的 run 于是把敏感值原样显示出来，
/// 而同一个 run 事后查看反而是脱敏的——同一个字段两种命运。
///
/// 与 timeline 的差异只有一处：run 级 output（`run_completed`）两面都不脱敏
/// （它就是 run.get 那个数据面值，同名字段必须同值），见 timeline_value 注释。
fn redact_display_envelope(mut envelope: Envelope) -> Envelope {
    if let Event::NodeCompleted { output, .. } = &mut envelope.event {
        *output = flow_backend::redact_value(output);
    }
    envelope
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
            // 节点输出的唯一所有者是 NodeRecord.output（见 fold.rs）
            //
            // 同一规则在订阅通知上还有一份（`redact_display_envelope`）——
            // 两个展示面必须同规则，否则前端会用一个覆盖另一个。
            let output = record
                .output()
                .cloned()
                .map(|v| flow_backend::redact_value(&v));
            let mut entry = json!({
                "id": node.id,
                "name": node.name,
                "type": node.node_type,
                "state": record.state.label(),
                // attempts / error 从 state 派生（NodeRecord 不存副本，见
                // NodeState::attempt / ::error）——wire 形状逐字不变。
                "attempts": record.state.attempt(),
                "started_at": record.started_at,
                "ended_at": record.ended_at,
                "duration_ms": record.duration_ms,
                "input": record.input,
                "output": output,
                "error": record.state.error(),
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
        // run 级 output 是**数据面**：与 run.get / run_completed 事件逐字一致。
        // 脱敏只发生在节点级展示值（上面 nodes[].output）——节点的输出可能是
        // 上游 HTTP 响应（会回显凭据），而 run 级 output 就是 run.get 那个值。
        // 同名字段必须同值：否则客户端从 timeline 重建输出会拿到污染数据。
        "output": snapshot.output,
        "fatal_error": snapshot.fatal_error,
        "last_seq": snapshot.last_seq,
        "nodes": nodes,
    })
}

/// 信号/取消请求的响应语义（`flow-pg/src/gateway.rs`，两种后端共用）：
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
        .map(|kind| kind.descriptor().clone())
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

/// schedule.update 的 input 与 template.update 的 category 用双 Option 区分
/// 「字段缺失=不改」与「显式 null=清空」。serde 原生会把两者都收成 None，
/// 所以需要自定义反序列化把外层 Some 钉上。
fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
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

/// 持久化密钥名称约束：环境变量后缀的合法子集（字母数字下划线），
/// 1..=64 字符。名称会拼进 FLOW_SECRET_<名称> 展示，不能引入歧义字符。
fn validate_secret_name(name: &str) -> Result<(), ErrorObjectOwned> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if valid {
        Ok(())
    } else {
        Err(invalid(
            "密钥名称须为 1..=64 个字母/数字/下划线（如 OPENAI_KEY）",
        ))
    }
}

/// 模板名约束：非空、去首尾空白后 1..=100 字符。
fn validate_template_name(name: &str) -> Result<(), ErrorObjectOwned> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.len() > 100 {
        return Err(invalid("模板名须为 1..=100 个字符"));
    }
    Ok(())
}

/// 画布片段校验（template.create / template.update 共用）：
/// - nodes 非空数组；每项 `{id, type, name, position?, params}`——类型已知，
///   params 过 [`flow_engine::model::validate_params`]（与整图同源的参数规则）；
/// - edges 数组（可为空）；端点必须在片段内；带 sourceHandle 的边只允许
///   condition 节点且端口为 "true"/"false"（与整图规则一致）。
///
/// 返回**归一化**后的片段：只保留白名单字段，杜绝面板存进任意 JSON。
fn validate_fragment(nodes: &Value, edges: &Value) -> Result<(Value, Value), ErrorObjectOwned> {
    let nodes_arr = nodes
        .as_array()
        .ok_or_else(|| invalid("nodes 必须是数组"))?;
    if nodes_arr.is_empty() {
        return Err(invalid("片段至少要有一个节点"));
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut normalized_nodes = Vec::with_capacity(nodes_arr.len());
    for raw in nodes_arr {
        let id = raw["id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("片段节点缺少非空 id"))?;
        if !ids.insert(id.to_string()) {
            return Err(invalid(format!("片段节点 id 重复：{id}")));
        }
        let type_str = raw["type"]
            .as_str()
            .ok_or_else(|| invalid(format!("片段节点 {id} 缺少 type")))?;
        let kind = NodeType::parse(type_str)
            .ok_or_else(|| invalid(format!("片段节点 {id} 的类型未知：{type_str}")))?;
        let name = raw["name"].as_str().unwrap_or(id);
        let params = raw.get("params").cloned().unwrap_or(json!({}));
        if !params.is_object() {
            return Err(invalid(format!("片段节点 {id} 的 params 必须是对象")));
        }
        let node = flow_engine::Node {
            id: id.to_string(),
            node_type: type_str.to_string(),
            name: name.to_string(),
            position: None,
            params,
        };
        flow_engine::model::validate_params(&node, kind)
            .map_err(|e| invalid(format!("片段节点 {id} 参数非法：{e}")))?;
        let position = raw.get("position").filter(|p| p.is_object()).cloned();
        let mut normalized = json!({
            "id": node.id,
            "type": type_str,
            "name": node.name,
            "params": node.params,
        });
        if let Some(position) = position {
            normalized["position"] = position;
        }
        normalized_nodes.push(normalized);
    }

    let edges_arr = edges
        .as_array()
        .ok_or_else(|| invalid("edges 必须是数组"))?;
    let mut normalized_edges = Vec::with_capacity(edges_arr.len());
    for raw in edges_arr {
        let source = raw["source"].as_str().unwrap_or_default();
        let target = raw["target"].as_str().unwrap_or_default();
        if !ids.contains(source) || !ids.contains(target) {
            return Err(invalid(format!(
                "片段边的端点不在片段内：{source} → {target}"
            )));
        }
        if source == target {
            return Err(invalid(format!("片段不允许自环：{source}")));
        }
        let mut normalized = json!({ "source": source, "target": target });
        if let Some(handle) = raw["sourceHandle"].as_str() {
            if handle != "out" {
                let source_kind = NodeType::parse(
                    normalized_nodes
                        .iter()
                        .find(|n| n["id"] == json!(source))
                        .and_then(|n| n["type"].as_str())
                        .unwrap_or_default(),
                );
                if source_kind != Some(NodeType::Condition) || !matches!(handle, "true" | "false") {
                    return Err(invalid(format!(
                        "片段边 {source}→{target} 的 sourceHandle 非法：{handle}（仅 condition 节点可用 true/false）"
                    )));
                }
            }
            normalized["sourceHandle"] = json!(handle);
        }
        if let Some(handle) = raw["targetHandle"].as_str() {
            normalized["targetHandle"] = json!(handle);
        }
        normalized_edges.push(normalized);
    }
    Ok((
        Value::Array(normalized_nodes),
        Value::Array(normalized_edges),
    ))
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
        | BackendError::WebhookNotFound(_)
        | BackendError::TemplateNotFound(_) => {
            ErrorObject::owned(CODE_NOT_FOUND, err.to_string(), None::<()>)
        }
        BackendError::VersionNotPublished(..)
        | BackendError::Conflict(_)
        | BackendError::TemplateNameTaken(_) => conflict(err.to_string()),
        BackendError::Invalid(_) => invalid(err.to_string()),
        BackendError::Internal(_) => internal(err.to_string()),
    }
}

/// `nodetypes.list` 响应快照：descriptor 注册表收敛（model.rs `NodeType::descriptor`）
/// 是纯重构，响应必须逐字节不变。新增/修改节点类型时同步更新本快照。
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// 订阅通知的展示脱敏：node_completed 的 output 脱敏，其余事件与字段原样。
    /// 回归的是「只脱 timeline 不脱订阅 → 前端用事件原始值覆盖脱敏值」。
    #[test]
    fn subscription_notice_redacts_node_output_like_timeline() {
        let envelope = Envelope {
            seq: 7,
            ts: Utc::now(),
            run_id: "r".into(),
            event: Event::NodeCompleted {
                node_id: "n".into(),
                attempt: 1,
                output: serde_json::json!({
                    "status": 200,
                    "headers": {"set-cookie": "sid=secret-value", "content-type": "application/json"},
                    "body": {"ok": true}
                }),
                duration_ms: 12,
            },
        };
        let redacted = redact_display_envelope(envelope);
        let Event::NodeCompleted { output, .. } = &redacted.event else {
            panic!("事件类型不该被改写");
        };
        assert_eq!(output["headers"]["content-type"], json!("application/json"));
        assert_eq!(output["headers"]["set-cookie"], json!("***"));
        assert_eq!(output["body"]["ok"], json!(true));
    }

    /// run 级 output 是数据面（= run.get 的值），两个展示面都不动它。
    #[test]
    fn subscription_notice_keeps_run_level_output_verbatim() {
        let envelope = Envelope {
            seq: 9,
            ts: Utc::now(),
            run_id: "r".into(),
            event: Event::RunCompleted {
                output: serde_json::json!({"api_key": "sk-live"}),
            },
        };
        let redacted = redact_display_envelope(envelope);
        let Event::RunCompleted { output } = &redacted.event else {
            panic!("事件类型不该被改写");
        };
        assert_eq!(output["api_key"], json!("sk-live"));
    }

    #[test]
    fn node_types_snapshot_is_stable() {
        const SNAPSHOT: &str = r#"[{"category":"control","label":"开始","max_instances":1,"params_schema":{"properties":{},"type":"object"},"ports":[{"id":"out","label":"出"}],"type":"start"},{"category":"control","label":"结束","params_schema":{"properties":{},"type":"object"},"ports":[{"id":"in","label":"入"}],"type":"end"},{"category":"compute","label":"脚本","params_schema":{"properties":{"code":{"type":"string","x-help":"可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果","x-label":"JS 函数体","x-opaque":true,"x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"脚本超时（毫秒）"}},"required":["code"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"script"},{"category":"control","label":"条件分支","params_schema":{"properties":{"expr":{"type":"string","x-help":"表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes","x-label":"条件表达式","x-opaque":true,"x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"求值超时（毫秒）"}},"required":["expr"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"true","label":"真"},{"id":"false","label":"假"}],"type":"condition"},{"category":"control","label":"等待","params_schema":{"properties":{"ms":{"type":"integer","x-help":"数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）","x-label":"时长（毫秒）"}},"required":["ms"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"delay"},{"category":"integration","label":"HTTP 请求","params_schema":{"properties":{"body":{"x-label":"请求体","x-widget":"json"},"headers":{"default":{},"x-label":"请求头","x-widget":"json"},"method":{"default":"GET","enum":["GET","POST","PUT","PATCH","DELETE"],"type":"string","x-label":"方法"},"timeout_ms":{"default":30000,"type":"integer","x-label":"HTTP 超时（毫秒）"},"url":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）","x-label":"URL"}},"required":["url"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"http_call"},{"category":"human","label":"人工节点","params_schema":{"properties":{"prompt":{"type":"string","x-label":"提示"}},"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"human_task"},{"category":"control","label":"子工作流","params_schema":{"properties":{"input_mapping":{"x-help":"JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入","x-label":"子 run 输入映射","x-widget":"json"},"workflow_id":{"type":"string","x-help":"调用其最新已发布版本作为子 run；子 run 输出透传为本节点输出；子 run 失败传导为本节点 fatal（DESIGN §6.8），重试策略只覆盖启动/等待类错误","x-label":"目标工作流","x-widget":"workflow-picker"}},"required":["workflow_id"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"sub_workflow"},{"category":"ai","label":"LLM 调用","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 OPENAI_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"base_url":{"default":"https://api.openai.com/v1","type":"string","x-help":"OpenAI 兼容端点，请求发往 {base_url}/chat/completions","x-label":"API 地址"},"json_mode":{"default":false,"type":"boolean","x-help":"开启后请求带 response_format: {\"type\":\"json_object\"}","x-label":"JSON 模式"},"max_tokens":{"type":"integer","x-label":"最大 token 数"},"model":{"type":"string","x-label":"模型"},"prompt":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"提示词","x-widget":"code"},"system":{"type":"string","x-label":"系统提示"},"temperature":{"type":"number","x-label":"温度"}},"required":["api_key","model","prompt"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"llm"},{"category":"notify","label":"邮件","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"body":{"type":"string","x-help":"纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板","x-label":"正文","x-widget":"code"},"endpoint":{"default":"https://api.resend.com/emails","type":"string","x-help":"Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证","x-label":"API 端点"},"from":{"type":"string","x-label":"发件人"},"subject":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"主题"},"to":{"type":"string","x-label":"收件人"}},"required":["api_key","from","to","subject","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"email"}]"#;
        assert_eq!(
            serde_json::to_string(&crate::node_types()).unwrap(),
            SNAPSHOT,
            "nodetypes.list 响应变了：若是有意变更，更新本快照"
        );
    }
}
