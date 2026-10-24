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
//! 产品视图由 flow-backend::journal_views 投影并执行展示脱敏；
//! 节点类型目录与产品面校验（cron / 模板片段 / 密钥名）。
//!
//! 错误码词汇（v2 单一来源）：`-32001` unauthorized、`-32011` not found、
//! `-32012` conflict、`-32021` limit/超限、
//! `-32602` invalid params、`-32603` internal。
//!
//! `-32020` 是投影病理阀：服务端在写响应前等到 `applied_lsn >= commit_lsn`
//! （默认 30s 上限），健康路径永不触发。到达即投影故障——客户端按终态错误
//! 处理；已发布旧客户端的 command.status 轮询仍然正确，只是不再必要。
//!
//! 本文件守住的装配不变量：**全部可失败的准备都先于端口暴露完成**——配置
//! 校验、journal 打开、监听绑定任一失败时进程直接退出，绝不出现「端口已开
//! 但服务不可用」的半可用状态；关闭时按反序收束（cron → RPC → HTTP → journal），
//! 让每一层在被依赖方之前停止接收新工作。
//!
//! 之所以把装配集中在这一个文件：产品 bin、桌面内嵌服务与测试 harness 必须
//! 跑同一份实现，否则「被测的不是生产进程」会让所有回归结论失效。

use std::sync::Arc;

use flow_backend::journal::JournalBackend;
use flow_backend::NodeType;
use jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned};
use serde_json::{json, Value};

pub mod journal_download;
pub mod journal_triggers;
pub mod journal_v2;

/// RPC 层状态：v2 产品装配的工作副本（config.get/update、secrets.set/delete）。
///
/// 它只承载「可被 RPC 读写的产品面状态」，不放任何请求级数据——请求级数据
/// 必须随请求结束而消失，否则一次请求的失败会污染后续请求（典型：把未校验
/// 的客户端参数留在共享状态里）。
pub struct AppState {
    /// 权威 journal 后端：所有读写命令的最终落点。
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
///
/// 之所以把「配置文件内容」与「配置文件路径」分成两把锁：读侧（config.get）
/// 要同时拿到两者，写侧（config.update）要先把内容 merge 好再落盘，两者都
/// 需要各自独立的一致性视图，但路径只在首次写入后变化一次。
pub struct ConfigState {
    /// 当前生效的**文件级**配置（不含 env 覆盖）。
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
    ///
    /// 密钥存储初始化失败**不阻断服务启动**：这台机器仍可用 env 密钥工作，
    /// 只是 secrets.set/delete 不可用。若改成硬失败，一个坏掉的 data_dir
    /// 就会让整台机器彻底无法配置，凭据环境变量这条退路也会被一起掐掉。
    ///
    /// 返回值第一项是 RPC 用的共享状态，第二项是独立的密钥存储句柄——调用方
    /// 可能需要它做进程级安装之外的用途（如自举），因此不隐藏在 AppState 里。
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
                // 进程级 SecretSource 是单例：第二个入口（如桌面内嵌 + 本进程）
                // 装不上属预期情况，记告警即可，不能因此让调用方拿到 None。
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
///
/// 三段式形状：当前文件级配置、配置文件实际路径、生效的 env 覆盖**名单**。
/// env 覆盖只回名字不回值——值里可能有凭据，回传即等于把密钥写进日志与前端
/// 的本地存储。
pub(crate) fn config_view(state: &AppState) -> Value {
    let (config, path, env_overrides) = match &state.config {
        Some(cs) => (
            // RwLock 毒化时仍然取回内容：配置锁只保护一份可重建的工作副本，
            // 因一次 panic 就让整个配置面不可读，比带着毒化的锁继续服务更糟。
            Some(cs.config.read().unwrap_or_else(|p| p.into_inner()).clone()),
            cs.path.read().unwrap_or_else(|p| p.into_inner()).clone(),
            cs.env_overrides.clone(),
        ),
        None => (None, None, Vec::new()),
    };
    // WSS agent tokens stay out of config.get responses.
    // 远端 agent 的 token 是凭据：config.get 是最容易被前端/日志顺手带出去的
    // 读接口，用占位符而非删除字段，保留「这里配过 token」的结构信息。
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

/// 把 TOML 文本解析成统一配置对象。
///
/// 单独成函数而不是内联，是为了让「读文件」与「解析文本」两种失败在调用方
/// 各自可见：读文件失败是 IO 问题，解析失败是配置内容问题，运维排障时要能
/// 从错误信息里区分二者。
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
///
/// # Errors
/// 配置缺失/校验失败、token 不足 32 字节、journal 目录不可打开、端口绑定
/// 失败都会以 Err 返回；此时进程不应继续服务半成品状态。
pub async fn serve_journal_product(
    config: flow_config::Config,
    config_path: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    use flow_backend::journal::JournalBackend;

    // 日志初始化用 try_init：进程内可能已有别的组件装过 subscriber，
    // 重复初始化会 panic——这里退化为「沿用已有配置」，不夺走控制权。
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,flow_engine=debug,flow_rpc=debug,flow_backend=debug".into()
            }),
        )
        .try_init();

    // journal 权威目录：FLOW_JOURNAL_DATA_DIR env > [journal].data_dir >
    // 缺省 flow 主目录下 `journal`（`~/.flow/journal`）。相对路径相对
    // flow 主目录解析，不按 CWD——换目录启动必须看到同一份数据。
    let root = std::env::var("FLOW_JOURNAL_DATA_DIR")
        .ok()
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| config.journal_data_dir())
        .ok_or("journal 数据目录缺失：配置 [journal].data_dir、环境变量 FLOW_JOURNAL_DATA_DIR 或可用的用户主目录（FLOW_HOME/~）")?;
    let token = std::env::var("FLOW_JOURNAL_TOKEN")?;
    // 32 字节是 token 强度的下限：它是单用户凭据，但仍是唯一屏障，
    // 短 token 等价于无认证。缺失/过短都在开端口前拒绝。
    if token.len() < 32 {
        return Err("FLOW_JOURNAL_TOKEN must contain at least 32 bytes".into());
    }
    config.validate()?;
    // Finish fallible setup before exposing RPC/HTTP or starting cron.
    let data_dir = config.storage_data_dir();
    // 工作副本取「文件原文解析结果」而不是内存里的 config：内存 config 已经
    // 叠过 env 覆盖，落盘时必须写回纯文件视图，否则 env 值会被固化进文件。
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
    // 所有会失败的准备（绑定端口、装配模块、启动执行器）收在一个 async 块里：
    // 任一失败都必须先关掉已打开的 journal 再返回错误，否则会留下一个占着
    // 数据目录 flock 的僵尸句柄，让下一次启动以「目录被占用」失败。
    let prepared = async {
        let listener = tokio::net::TcpListener::bind(&config.journal.http_addr).await?;
        let bound_http = listener.local_addr()?;
        let rpc_server = journal_v2::bind_product_server(config.journal.addr.parse()?).await?;
        let bound_rpc = rpc_server.local_addr()?;
        let router = journal_download::router(backend.clone(), token.clone())?
            .merge(journal_triggers::router(backend.clone(), token.clone()));
        let (state, _secrets) =
            AppState::for_production(backend.clone(), config_state, &data_dir).await;
        let module = journal_v2::module_product(backend.clone(), token, Some(state))?;
        let harness_agent = flow_backend::harness_agent_run_config(&config.harness.agent);
        flow_backend::start_execution(&config.execution, harness_agent, &backend).await?;
        Ok::<_, Box<dyn std::error::Error>>((
            listener, bound_http, rpc_server, bound_rpc, router, module,
        ))
    }
    .await;
    let (listener, bound_http, rpc_server, bound_rpc, router, module) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            // 准备失败：先释放 journal 再把错误抛出去，保证下次启动能拿到
            // 数据目录锁。
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
    let interrupted = shutdown_signal().await;
    tracing::info!("正在停止 journal 节点");
    // 收束顺序：先停 cron（不再产生新工作），再停 RPC（不再接新命令），
    // 再停 HTTP（下载与 webhook 收尾），最后关 journal——让被依赖方先停。
    scheduler.abort();
    let _ = scheduler.await;
    let server_stopped = server.stop();
    server.stopped().await;
    let _ = stop.send(());
    // HTTP 是优雅关停：给在途下载 5s；超时则强制 abort，避免进程挂着不退出。
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

/// 等待进程收到中断信号，返回触发该信号的 IO 结果。
///
/// unix 上同时监听 SIGINT 与 SIGTERM：容器编排停机发的是 SIGTERM，只等
/// ctrl_c 会让进程被 SIGKILL 强杀，journal 得不到正常收尾。
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
///
/// 顺序固定且响应逐字节稳定：前端按数组下标渲染表单与能力，插入新节点类型
/// 若改变顺序会让旧前端与后端能力错位（快照测试守着这条线）。
pub(crate) fn node_types() -> Value {
    json!(NodeType::ALL
        .iter()
        .map(|kind| kind.descriptor().clone())
        .collect::<Vec<Value>>())
}

/// cron 合法性校验（标准 5 字段，分 时 日 月 周，本地时间）。非法表达式
/// -32602（与 v2 参数错误同一层）。
///
/// 在写命令入口就拦下坏 cron：坏表达式若流到触发器扫描，只会表现为
/// 「schedule 永不触发且无迹可查」——那是最难排查的一类故障。
pub(crate) fn validate_cron(expr: &str) -> Result<(), ErrorObjectOwned> {
    expr.parse::<cron_parser::Schedule>()
        .map(|_| ())
        .map_err(|e| invalid(format!("非法的 cron 表达式：{e}")))
}

/// schedule 响应：实体字段 + 服务端算好的 next_fire_at（RFC3339）。
/// cron 字段按本地时间解释（产品契约），参考时刻取 Local、输出统一转
/// UTC 表示——与 create 路径持久化的口径一致（同一时刻、可比的字符串）。
/// 存量数据 cron 损坏时 next_fire_at 为 null，不让 list 整个失败。
///
/// next_fire_at 为 null 是「这条 schedule 算不出下次触发」的显式信号，而不是
/// 把整条 schedule 隐藏掉——用户需要看见这条配置存在但已失效。
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

/// 构造 `-32602 invalid params` 错误对象。
///
/// 整个 v2 面共用这一个构造器：invalid params 的错误码必须是单一来源，
/// 否则客户端按码分支时会漏掉某一处自造的 `-32603`。
pub(crate) fn invalid(message: impl ToString) -> ErrorObjectOwned {
    ErrorObject::owned(-32602, message.to_string(), None::<()>)
}

/// 持久化密钥名称约束：环境变量后缀的合法子集（字母数字下划线），
/// 1..=64 字符。名称会拼进 FLOW_SECRET_<名称> 展示，不能引入歧义字符。
///
/// 白名单而非黑名单：名称会直接参与环境变量名构造，含分隔符或空白会让
/// 「配置了这个密钥」与「引擎实际读到的密钥」对不上。
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
    /// 快照守的是「前端按数组下标渲染」的契约：节点类型描述一旦字节级变化，
    /// 已发布的前端与后端就可能对同一能力产生不同解释。
    #[test]
    fn node_types_snapshot_is_stable() {
        const SNAPSHOT: &str = r#"[{"category":"control","label":"开始","max_instances":1,"params_schema":{"properties":{},"type":"object"},"ports":[{"id":"out","label":"出"}],"trace_granularity":"node","type":"start"},{"category":"control","label":"结束","params_schema":{"properties":{"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"}},"type":"object"},"ports":[{"id":"in","label":"入"}],"trace_granularity":"node","type":"end"},{"category":"compute","label":"脚本","params_schema":{"properties":{"code":{"type":"string","x-help":"可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果","x-label":"JS 函数体","x-opaque":true,"x-widget":"code"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"timeout_ms":{"default":2000,"type":"integer","x-label":"脚本超时（毫秒）"}},"required":["code"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"trace_granularity":"node","type":"script"},{"category":"control","label":"条件分支","params_schema":{"properties":{"expr":{"type":"string","x-help":"表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes","x-label":"条件表达式","x-opaque":true,"x-widget":"code"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"timeout_ms":{"default":2000,"type":"integer","x-label":"求值超时（毫秒）"}},"required":["expr"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"true","label":"真"},{"id":"false","label":"假"}],"trace_granularity":"node","type":"condition"},{"category":"control","label":"等待","params_schema":{"properties":{"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"ms":{"type":"integer","x-help":"数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）","x-label":"时长（毫秒）"}},"required":["ms"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"trace_granularity":"node","type":"delay"},{"category":"integration","label":"HTTP 请求","params_schema":{"properties":{"body":{"x-label":"请求体","x-widget":"json"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"headers":{"default":{},"x-label":"请求头","x-widget":"key-value"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"method":{"default":"GET","enum":["GET","POST","PUT","PATCH","DELETE"],"type":"string","x-label":"方法"},"proxy":{"type":"string","x-help":"http(s)://[user:pass@]host:port，留空直连","x-label":"代理"},"timeout_ms":{"default":30000,"type":"integer","x-label":"HTTP 超时（毫秒）"},"url":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）","x-label":"URL"}},"required":["url"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"external_call","type":"http_call"},{"category":"human","label":"人工节点","params_schema":{"properties":{"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"prompt":{"type":"string","x-label":"提示"}},"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"trace_granularity":"node","type":"human_task"},{"category":"human","label":"输入","params_schema":{"properties":{"fields":{"type":"array","x-help":"JSON 数组，每项 {\"key\":\"token\",\"label\":\"显示名\",\"default\":…}（label/default 可省）。run 输入已覆盖全部字段（默认值可兜底）时直接放行，输出这些字段的值；否则挂起等用户填写（run.signal 交付的对象成为本节点输出）","x-label":"输入字段","x-widget":"json"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"}},"required":["fields"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"trace_granularity":"node","type":"input"},{"category":"control","label":"子工作流","params_schema":{"properties":{"input_mapping":{"x-help":"JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入","x-label":"子 run 输入映射","x-widget":"json"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"version":{"type":"integer","x-help":"省略时在运行启动时固定最新已发布版本","x-label":"子流程版本"},"workflow_id":{"type":"string","x-help":"v2 在运行启动时固定已发布子流程；输出透传，失败向上传导","x-label":"目标工作流","x-widget":"workflow-picker"}},"required":["workflow_id"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"trace_granularity":"node","type":"sub_workflow"},{"category":"ai","label":"Harness 代理","params_schema":{"properties":{"api_key":{"type":"string","x-help":"仅 mode=agent：只存密钥名称，真值来自密钥管理（secrets.list 提供候选）","x-label":"API 密钥名称","x-secret":true},"args":{"items":{"type":"string"},"type":"array","x-help":"仅 mode=command 可用","x-label":"额外参数","x-widget":"json"},"command":{"type":"string","x-help":"mode=command 时必填；进程级记录：prompt 经 stdin，最终 stdout 作为输出；逐模型/工具记录使用模型调用和 HTTP 节点","x-label":"命令"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"mode":{"default":"command","enum":["command","agent"],"type":"string","x-help":"command 运行外部 CLI（需 command 参数）；agent 使用内置 flow-harness-agent（需 api_key，workdir 可留空，模型配置见设置页 [harness.agent]）","x-label":"模式"},"prompt":{"type":"string","x-label":"提示词","x-widget":"code"},"timeout_ms":{"default":300000,"type":"number","x-label":"超时 (ms)"},"workdir":{"type":"string","x-help":"仅 mode=agent；可选。缺省 = 每次 run 的临时沙箱（结束后删除）；配置后为 agent 工作根（绝对路径且须存在，agent 可读写其中文件）","x-label":"工作目录"}},"required":["prompt"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"process","type":"harness"},{"category":"notify","label":"邮件","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"body":{"type":"string","x-help":"纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板","x-label":"正文","x-widget":"code"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"endpoint":{"default":"https://api.resend.com/emails","type":"string","x-help":"Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证","x-label":"API 端点"},"from":{"type":"string","x-label":"发件人"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"subject":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"主题"},"to":{"type":"string","x-label":"收件人"}},"required":["api_key","from","to","subject","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"external_call","type":"email"},{"category":"ai","label":"模型调用","params_schema":{"properties":{"body":{"type":"object","x-label":"模型请求","x-widget":"json"},"credential":{"type":"object","x-help":"{scheme: bearer, secret_ref: 密钥名称}","x-label":"凭据引用","x-widget":"json"},"effect_policy":{"default":"manual","enum":["manual","repeatable"],"type":"string","x-help":"manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制","x-label":"未知结果恢复"},"headers":{"default":{},"x-label":"请求头","x-widget":"key-value"},"join":{"enum":["selected","all"],"type":"string","x-help":"selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则","x-label":"合流规则"},"response_schema":{"type":"object","x-label":"响应约束","x-widget":"json"},"timeout_ms":{"default":30000,"type":"integer","x-label":"超时（毫秒）"},"url":{"type":"string","x-label":"模型 API 地址"}},"required":["url","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"retry_scope":"declared_repeatable_only","side_effect":true,"supports_retry":true,"trace_granularity":"model_call","type":"llm"}]"#;
        assert_eq!(
            serde_json::to_string(&crate::node_types()).unwrap(),
            SNAPSHOT,
            "nodetypes.list 响应变了：若是有意变更，更新本快照"
        );
    }
}
