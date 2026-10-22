//! flow-rpc：jsonrpsee WebSocket 服务（bin: flow-journal-server）。
//!
//! v1 RPC 面（`flow-server` 的无认证契约）已随
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
//! `-32012` conflict、`-32021` limit/超限、
//! `-32602` invalid params、`-32603` internal。
//!
//! `-32020` 是投影病理阀：服务端在写响应前等到 `applied_lsn >= commit_lsn`
//! （默认 30s 上限），健康路径永不触发。到达即投影故障——客户端按终态错误
//! 处理；已发布旧客户端的 command.status 轮询仍然正确，只是不再必要。

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
    /// 启动时发现的配置文件路径（内层 None = 启动无配置文件）。config.update
    /// 首次落盘后回填实际写入路径，config.get 的 config_path 立即可见。
    pub path: std::sync::RwLock<Option<std::path::PathBuf>>,
    /// 启动无配置文件时 config.update 的落点
    /// （[`flow_config::Config::default_write_path`]）；None 时 update 拒绝写入。
    pub default_path: Option<std::path::PathBuf>,
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
            cs.path.read().unwrap().clone(),
            cs.env_overrides.clone(),
        ),
        None => (None, None, Vec::new()),
    };
    // WSS agent 凭据不回显（与 cluster 连接串「stays in this environment
    // variable, never config.get」同一标准）：多 agent 运维下，任一持
    // master token 的调用方不该借此拿到全部 agent 的接入令牌。
    let config = config.map(|mut config: flow_config::Config| {
        if let Some(remote) = config.execution.remote.as_mut() {
            for agent in &mut remote.agents {
                agent.token = "***".into();
            }
        }
        config
    });
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
    if token.len() < 32 {
        return Err("FLOW_JOURNAL_TOKEN must contain at least 32 bytes".into());
    }
    config.validate()?;
    #[cfg(not(feature = "journal-cluster"))]
    if config.journal.cluster.is_some() {
        return Err("journal.cluster requires a build with --features journal-cluster".into());
    }
    // Finish fallible setup before exposing RPC/HTTP or starting cron. Any failure
    // after registration closes the backend and releases its PG membership.
    let data_dir = std::path::PathBuf::from(&config.storage.data_dir);
    let file_config = match &config_path {
        Some(path) => toml_config_from_str(&std::fs::read_to_string(path)?)?,
        None => flow_config::Config::default(),
    };
    let config_state = ConfigState {
        config: std::sync::RwLock::new(file_config),
        path: std::sync::RwLock::new(config_path.clone()),
        default_path: flow_config::Config::default_write_path(),
        env_overrides: flow_config::Config::load(config_path.as_deref())?.env_overrides,
    };
    let backend = JournalBackend::open(&root, Default::default()).await?;
    let prepared = async {
        let listener = tokio::net::TcpListener::bind(&config.journal.http_addr).await?;
        let bound_http = listener.local_addr()?;
        let rpc_server = journal_v2::bind_product_server(config.journal.addr.parse()?).await?;
        let bound_rpc = rpc_server.local_addr()?;
        let router = journal_download::router(backend.clone(), token.clone())?
            .merge(journal_triggers::router(backend.clone(), token.clone()));
        let (state, _secrets) =
            AppState::for_production(backend.clone(), config_state, &data_dir).await;
        #[cfg(feature = "journal-cluster")]
        if let Some(cluster_config) = &config.journal.cluster {
            let cluster = flow_backend::journal_cluster::Membership::join(
                &backend,
                cluster_config,
                &format!("ws://{bound_rpc}"),
                &format!("http://{bound_http}"),
            )
            .await?;
            assert!(backend.install_cluster(cluster));
        }
        let module = journal_v2::module_product(backend.clone(), token, Some(state))?;
        let harness_agent = flow_backend::harness_agent_run_config(&config.harness.agent);
        flow_backend::start_execution(&config.execution, harness_agent, &backend).await?;
        #[cfg(feature = "journal-cluster")]
        if backend
            .cluster()
            .is_some_and(|cluster| cluster.lost().is_cancelled())
        {
            return Err("journal membership lost during startup".into());
        }
        Ok::<_, Box<dyn std::error::Error>>((
            listener, bound_http, rpc_server, bound_rpc, router, module,
        ))
    }
    .await;
    let (listener, bound_http, rpc_server, bound_rpc, router, module) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = backend.close().await;
            return Err(error);
        }
    };
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut downloads = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let server = rpc_server.start(module);
    tracing::info!(local_addr=%bound_rpc,http_addr=%bound_http,data_dir=%root.display(),
        "flow-journal-server 已启动 (JSONL v2 WS RPC + 下载/触发器 HTTP)");
    // cron 触发器：[server].scheduler_enabled=false 时不启动（与桌面同一开关）
    let scheduler = if config.server.scheduler_enabled {
        journal_triggers::start(backend.clone(), config.journal_trigger_tick())
    } else {
        tracing::info!("scheduler_enabled=false，cron 触发器未启动");
        tokio::spawn(async {})
    };
    #[cfg(feature = "journal-cluster")]
    let interrupted = if let Some(cluster) = backend.cluster() {
        let lost = cluster.lost();
        tokio::select! {
            result = shutdown_signal() => result,
            _ = lost.cancelled() => { tracing::error!("PG 注册租约失效，停止节点服务"); Ok(()) },
        }
    } else {
        shutdown_signal().await
    };
    #[cfg(not(feature = "journal-cluster"))]
    let interrupted = shutdown_signal().await;
    tracing::info!("正在停止 journal 节点");
    scheduler.abort();
    let _ = scheduler.await;
    let server_stopped = server.stop();
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
    interrupted?;
    server_stopped?;
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
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

/// schedule 响应：实体字段 + 服务端算好的 next_fire_at（RFC3339）。
/// cron 字段按本地时间解释（产品契约），参考时刻取 Local、输出统一转
/// UTC 表示——与 create 路径持久化的口径一致（同一时刻、可比的字符串）。
/// 存量数据 cron 损坏时 next_fire_at 为 null，不让 list 整个失败。
pub(crate) fn schedule_value(schedule: &flow_dto::Schedule) -> Value {
    let next_fire_at = schedule
        .cron_expr
        .parse::<cron_parser::Schedule>()
        .ok()
        .and_then(|cron| cron.next_after(&chrono::Local::now()))
        .map(|t| t.with_timezone(&chrono::Utc).to_rfc3339());
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

/// `nodetypes.list` 响应快照：
/// 响应必须逐字节不变。新增/修改节点类型时同步更新本快照。
#[cfg(test)]
mod tests {
    #[test]
    fn node_types_snapshot_is_stable() {
        const SNAPSHOT: &str = r#"[{"category":"control","label":"开始","max_instances":1,"params_schema":{"properties":{},"type":"object"},"ports":[{"id":"out","label":"出"}],"trace_granularity":"node","type":"start"},{"category":"control","label":"结束","params_schema":{"properties":{"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"}},"type":"object"},"ports":[{"id":"in","label":"入"}],"trace_granularity":"node","type":"end"},{"category":"compute","label":"脚本","params_schema":{"properties":{"code":{"type":"string","x-help":"可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果","x-label":"JS 函数体","x-opaque":true,"x-widget":"code"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"timeout_ms":{"default":2000,"type":"integer","x-label":"脚本超时（毫秒）"}},"required":["code"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"trace_granularity":"node","type":"script"},{"category":"control","label":"条件分支","params_schema":{"properties":{"expr":{"type":"string","x-help":"表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes","x-label":"条件表达式","x-opaque":true,"x-widget":"code"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"timeout_ms":{"default":2000,"type":"integer","x-label":"求值超时（毫秒）"}},"required":["expr"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"true","label":"真"},{"id":"false","label":"假"}],"trace_granularity":"node","type":"condition"},{"category":"control","label":"等待","params_schema":{"properties":{"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"ms":{"type":"integer","x-help":"数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）","x-label":"时长（毫秒）"}},"required":["ms"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"trace_granularity":"node","type":"delay"},{"category":"integration","label":"HTTP 请求","params_schema":{"properties":{"body":{"x-label":"请求体","x-widget":"json"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"headers":{"default":{},"x-label":"请求头","x-widget":"key-value"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"method":{"default":"GET","enum":["GET","POST","PUT","PATCH","DELETE"],"type":"string","x-label":"方法"},"proxy":{"type":"string","x-help":"http(s)://[user:pass@]host:port，留空直连","x-label":"代理"},"timeout_ms":{"default":30000,"type":"integer","x-label":"HTTP 超时（毫秒）"},"url":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）","x-label":"URL"}},"required":["url"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"external_call","type":"http_call"},{"category":"human","label":"人工节点","params_schema":{"properties":{"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"prompt":{"type":"string","x-label":"提示"}},"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"trace_granularity":"node","type":"human_task"},{"category":"human","label":"输入","params_schema":{"properties":{"fields":{"type":"array","x-help":"JSON 数组，每项 {\"key\":\"token\",\"label\":\"显示名\",\"default\":…}（label/default 可省）。run 输入已覆盖全部字段（默认值可兜底）时直接放行，输出这些字段的值；否则挂起等用户填写（run.signal 交付的对象成为本节点输出）","x-label":"输入字段","x-widget":"json"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"}},"required":["fields"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"trace_granularity":"node","type":"input"},{"category":"control","label":"子工作流","params_schema":{"properties":{"input_mapping":{"x-help":"JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入","x-label":"子 run 输入映射","x-widget":"json"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"version":{"type":"integer","x-help":"省略时在运行启动时固定最新已发布版本","x-label":"子流程版本"},"workflow_id":{"type":"string","x-help":"v2 在运行启动时固定已发布子流程；输出透传，失败向上传导","x-label":"目标工作流","x-widget":"workflow-picker"}},"required":["workflow_id"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"trace_granularity":"node","type":"sub_workflow"},{"category":"ai","label":"Harness 代理","params_schema":{"properties":{"api_key":{"type":"string","x-help":"仅 mode=agent：只存密钥名称，真值来自密钥管理（secrets.list 提供候选）","x-label":"API 密钥名称","x-secret":true},"args":{"items":{"type":"string"},"type":"array","x-help":"仅 mode=command 可用","x-label":"额外参数","x-widget":"json"},"command":{"type":"string","x-help":"mode=command 时必填；进程级记录：prompt 经 stdin，最终 stdout 作为输出；逐模型/工具记录使用模型调用和 HTTP 节点","x-label":"命令"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"mode":{"default":"command","enum":["command","agent"],"type":"string","x-help":"command 运行外部 CLI（需 command 参数）；agent 使用内置 flow-harness-agent（需 workdir 与 api_key，模型配置见设置页 [harness.agent]）","x-label":"模式"},"prompt":{"type":"string","x-label":"提示词","x-widget":"code"},"timeout_ms":{"default":300000,"type":"number","x-label":"超时 (ms)"},"workdir":{"type":"string","x-help":"仅 mode=agent；可选。缺省 = 每次 run 的临时沙箱（结束后删除）；配置后为 agent 工作根（绝对路径且须存在，agent 可读写其中文件）","x-label":"工作目录"}},"required":["prompt"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"process","type":"harness"},{"category":"notify","label":"邮件","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"body":{"type":"string","x-help":"纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板","x-label":"正文","x-widget":"code"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"endpoint":{"default":"https://api.resend.com/emails","type":"string","x-help":"Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证","x-label":"API 端点"},"from":{"type":"string","x-label":"发件人"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"subject":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"主题"},"to":{"type":"string","x-label":"收件人"}},"required":["api_key","from","to","subject","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"external_call","type":"email"},{"category":"ai","label":"模型调用","params_schema":{"properties":{"body":{"type":"object","x-label":"模型请求","x-widget":"json"},"credential":{"type":"object","x-help":"{scheme: bearer, secret_ref: 密钥名称}","x-label":"凭据引用","x-widget":"json"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"headers":{"default":{},"x-label":"请求头","x-widget":"key-value"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"response_schema":{"type":"object","x-label":"响应约束","x-widget":"json"},"timeout_ms":{"default":30000,"type":"integer","x-label":"超时（毫秒）"},"url":{"type":"string","x-label":"模型 API 地址"}},"required":["url","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"model_call","type":"llm"}]"#;
        assert_eq!(
            serde_json::to_string(&crate::node_types()).unwrap(),
            SNAPSHOT,
            "nodetypes.list 响应变了：若是有意变更，更新本快照"
        );
    }
}
