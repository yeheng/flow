//! flow-rpc：jsonrpsee WebSocket 服务（bin: flow-journal-server）。
//!
//! v1 RPC 面（`flow-server` 的无认证契约，35 个方法直连 `AnyBackend`）已随
//! flow-server 退役。本 crate 现在只有一个产品面：
//! - [`journal_v2`]：journal v2 协议（token 认证、写命令幂等键、回执语义、
//!   `journal_views` 物化产品视图）——浏览器前端、桌面内嵌服务、flow-cli
//!   全部走这一份方法集；
//! - [`journal_triggers`]：cron 扫描与 `POST /hooks/<key>` HTTP 入口；
//! - [`journal_download`]：有界值下载 HTTP。
//!
//! 本文件的职责只剩产品装配与共享助手：`AppState` / `ConfigState`
//! （统一配置面 + 持久化密钥，挂在 v2 模块的 config.*/secrets.* 上）、
//! timeline 投影（`timeline_value`）、展示脱敏（`redact_display_envelope`）、
//! 节点类型目录与产品面校验（cron / 模板片段 / 密钥名）。
//!
//! 错误码词汇（v2 单一来源）：`-32001` unauthorized、`-32011` not found、
//! `-32012` conflict、`-32020` COMMITTED_NOT_VISIBLE、`-32021` limit/超限、
//! `-32602` invalid params、`-32603` internal。

use std::sync::Arc;

use flow_backend::journal::JournalBackend;
use flow_backend::NodeType;
use jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned};
use serde_json::{json, Value};

pub mod journal_download;
pub mod journal_triggers;
pub mod journal_v2;

/// RPC 层状态：v2 产品装配的工作副本（config.get/update、secrets.set/delete）。
pub struct AppState {
    pub backend: Arc<JournalBackend>,
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
    /// 产品入口：配置 + 密钥存储全量装配。密钥文件在 data_dir 下
    /// （secret.key + secrets.json）；安装进程级 SecretSource 失败（重复）
    /// 只告警——先装先得，第二个入口不该发生。
    pub async fn for_production(
        backend: Arc<JournalBackend>,
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
pub(crate) fn config_view(state: &AppState) -> Value {
    let (config, path, env_overrides) = match &state.config {
        Some(cs) => (
            Some(cs.config.read().unwrap().clone()),
            cs.path.clone(),
            cs.env_overrides.clone(),
        ),
        None => (None, None, Vec::new()),
    };
    json!({
        "config": config.unwrap_or_default(),
        "config_path": path,
        "env_overrides": env_overrides,
    })
}

pub fn toml_config_from_str(text: &str) -> Result<flow_config::Config, Box<dyn std::error::Error>> {
    Ok(flow_config::Config::from_toml_str(text)?)
}

/// journal v2 产品服务的完整装配与运行（`flow-journal-server` 产品 bin 与
/// backend-e2e / backend-perf 的被测进程共用同一份实现——「被测的就是
/// 生产进程」）。
///
/// 组成：下载/webhook HTTP（`[journal].http_addr`，Bearer token）+ WS RPC
/// （`module_product`：v2 协议 + journal_views 物化产品视图 + 统一配置/密钥）+ 执行驱动
/// + cron 触发器（`[server].scheduler_enabled`）。
///
/// 环境变量（覆盖配置文件）：`FLOW_JOURNAL_DATA_DIR` / `FLOW_JOURNAL_TOKEN`
/// （≥32 字节，必填）/ `FLOW_JOURNAL_ADDR` / `FLOW_JOURNAL_HTTP_ADDR`。
///
/// 日志：`tracing_subscriber` 初始化（RUST_LOG env-filter；已初始化时跳过），
/// 启动行带 `local_addr=` / `http_addr=` 字段——这是测试 harness 的就绪
/// 标记（`flow_test_support::io::spawn_reporting_ports`），不能改。
pub async fn serve_journal_product(
    config: flow_config::Config,
    config_path: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    use flow_backend::journal::JournalBackend;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,flow_engine=debug,flow_rpc=debug,flow_backend=debug".into()
            }),
        )
        .try_init();

    let root = std::env::var("FLOW_JOURNAL_DATA_DIR")
        .ok()
        .or_else(|| config.journal.data_dir.clone())
        .map(std::path::PathBuf::from)
        .ok_or("journal 数据目录缺失：配置 [journal].data_dir 或环境变量 FLOW_JOURNAL_DATA_DIR")?;
    let token = std::env::var("FLOW_JOURNAL_TOKEN")?;
    let addr: std::net::SocketAddr = config.journal.addr.parse()?;
    let backend = JournalBackend::open(&root, Default::default()).await?;
    let download_addr: std::net::SocketAddr = config.journal.http_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(download_addr).await?;
    let bound_http = listener.local_addr()?;
    let router = journal_download::router(backend.clone(), token.clone())?
        .merge(journal_triggers::router(backend.clone(), token.clone()));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut downloads = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    // 产品装配：统一配置面 + 持久化密钥（密钥在 data_dir 下，与桌面同规则）
    let data_dir = std::path::PathBuf::from(&config.storage.data_dir);
    let file_config = match &config_path {
        Some(path) => {
            let text = std::fs::read_to_string(path)?;
            toml_config_from_str(&text)?
        }
        None => flow_config::Config::default(),
    };
    let config_state = ConfigState {
        config: std::sync::RwLock::new(file_config),
        path: config_path.clone(),
        env_overrides: flow_config::Config::load(config_path.as_deref())?.env_overrides,
    };
    let (state, _secrets) =
        AppState::for_production(backend.clone(), config_state, &data_dir).await;
    let module = journal_v2::module_product(backend.clone(), token, Some(state))?;
    let (server, addr) = journal_v2::serve_product(module, addr).await?;
    tracing::info!(
        local_addr = %addr,
        http_addr = %bound_http,
        data_dir = %root.display(),
        "flow-journal-server 已启动 (JSONL v2 WS RPC + 下载/触发器 HTTP)"
    );
    flow_backend::start_execution(&config.execution, &backend).await?;
    // cron 触发器：[server].scheduler_enabled=false 时不启动（与桌面同一开关）
    let scheduler = if config.server.scheduler_enabled {
        journal_triggers::start(backend.clone(), config.journal_trigger_tick())
    } else {
        tracing::info!("scheduler_enabled=false，cron 触发器未启动");
        tokio::spawn(async {})
    };
    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
    scheduler.abort();
    let _ = scheduler.await;
    server.stop()?;
    server.stopped().await;
    let _ = stop.send(());
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut downloads)
        .await
        .is_err()
    {
        downloads.abort();
        let _ = downloads.await;
    }
    backend.close().await?;
    Ok(())
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

/// cron 合法性校验（标准 5 字段，分 时 日 月 周，本地时间）。非法表达式
/// -32602（与 v2 参数错误同一层）。
pub(crate) fn validate_cron(expr: &str) -> Result<(), ErrorObjectOwned> {
    expr.parse::<cron_parser::Schedule>()
        .map(|_| ())
        .map_err(|e| invalid(format!("非法的 cron 表达式：{e}")))
}

/// schedule 响应：实体字段 + 服务端算好的 next_fire_at（RFC3339，本地时区偏移）。
/// 存量数据 cron 损坏时 next_fire_at 为 null，不让 list 整个失败。
pub(crate) fn schedule_value(schedule: &flow_dto::Schedule) -> Value {
    let next_fire_at = schedule
        .cron_expr
        .parse::<cron_parser::Schedule>()
        .ok()
        .and_then(|cron| cron.next_after(&chrono::Local::now()))
        .map(|t| t.to_rfc3339());
    let mut value = json!(schedule);
    value["next_fire_at"] = json!(next_fire_at);
    value
}

pub(crate) fn invalid(message: impl ToString) -> ErrorObjectOwned {
    ErrorObject::owned(-32602, message.to_string(), None::<()>)
}

/// 持久化密钥名称约束：环境变量后缀的合法子集（字母数字下划线），
/// 1..=64 字符。名称会拼进 FLOW_SECRET_<名称> 展示，不能引入歧义字符。
pub(crate) fn validate_secret_name(name: &str) -> Result<(), ErrorObjectOwned> {
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
pub(crate) fn validate_template_name(name: &str) -> Result<(), ErrorObjectOwned> {
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
pub(crate) fn validate_fragment(
    nodes: &Value,
    edges: &Value,
) -> Result<(Value, Value), ErrorObjectOwned> {
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

/// `nodetypes.list` 响应快照：descriptor 注册表收敛（model.rs `NodeType::descriptor`）
/// 是纯重构，响应必须逐字节不变。新增/修改节点类型时同步更新本快照。
#[cfg(test)]
mod tests {
    #[test]
    fn node_types_snapshot_is_stable() {
        const SNAPSHOT: &str = r#"[{"category":"control","label":"开始","max_instances":1,"params_schema":{"properties":{},"type":"object"},"ports":[{"id":"out","label":"出"}],"type":"start"},{"category":"control","label":"结束","params_schema":{"properties":{},"type":"object"},"ports":[{"id":"in","label":"入"}],"type":"end"},{"category":"compute","label":"脚本","params_schema":{"properties":{"code":{"type":"string","x-help":"可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果","x-label":"JS 函数体","x-opaque":true,"x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"脚本超时（毫秒）"}},"required":["code"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"script"},{"category":"control","label":"条件分支","params_schema":{"properties":{"expr":{"type":"string","x-help":"表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes","x-label":"条件表达式","x-opaque":true,"x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"求值超时（毫秒）"}},"required":["expr"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"true","label":"真"},{"id":"false","label":"假"}],"type":"condition"},{"category":"control","label":"等待","params_schema":{"properties":{"ms":{"type":"integer","x-help":"数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）","x-label":"时长（毫秒）"}},"required":["ms"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"delay"},{"category":"integration","label":"HTTP 请求","params_schema":{"properties":{"body":{"x-label":"请求体","x-widget":"json"},"headers":{"default":{},"x-label":"请求头","x-widget":"key-value"},"method":{"default":"GET","enum":["GET","POST","PUT","PATCH","DELETE"],"type":"string","x-label":"方法"},"proxy":{"type":"string","x-help":"http(s)://[user:pass@]host:port，留空直连","x-label":"代理"},"timeout_ms":{"default":30000,"type":"integer","x-label":"HTTP 超时（毫秒）"},"url":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）","x-label":"URL"}},"required":["url"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"http_call"},{"category":"human","label":"人工节点","params_schema":{"properties":{"prompt":{"type":"string","x-label":"提示"}},"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"human_task"},{"category":"control","label":"子工作流","params_schema":{"properties":{"input_mapping":{"x-help":"JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入","x-label":"子 run 输入映射","x-widget":"json"},"workflow_id":{"type":"string","x-help":"调用其最新已发布版本作为子 run；子 run 输出透传为本节点输出；子 run 失败传导为本节点 fatal（DESIGN §6.8），重试策略只覆盖启动/等待类错误","x-label":"目标工作流","x-widget":"workflow-picker"}},"required":["workflow_id"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"sub_workflow"},{"category":"ai","label":"Harness 代理","params_schema":{"properties":{"args":{"items":{"type":"string"},"type":"array","x-label":"额外参数","x-widget":"json"},"command":{"type":"string","x-help":"要执行的 harness CLI（如 kimi / claude / codex）；prompt 经 stdin 传入，stdout 作为输出","x-label":"命令"},"prompt":{"type":"string","x-label":"提示词","x-widget":"code"},"timeout_ms":{"default":300000,"type":"number","x-label":"超时 (ms)"},"workdir":{"type":"string","x-label":"工作目录"}},"required":["command","prompt"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"harness"},{"category":"notify","label":"邮件","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"body":{"type":"string","x-help":"纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板","x-label":"正文","x-widget":"code"},"endpoint":{"default":"https://api.resend.com/emails","type":"string","x-help":"Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证","x-label":"API 端点"},"from":{"type":"string","x-label":"发件人"},"subject":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"主题"},"to":{"type":"string","x-label":"收件人"}},"required":["api_key","from","to","subject","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"email"}]"#;
        assert_eq!(
            serde_json::to_string(&crate::node_types()).unwrap(),
            SNAPSHOT,
            "nodetypes.list 响应变了：若是有意变更，更新本快照"
        );
    }
}
