//! flow-config：统一配置模型（叶子 crate）。
//!
//! 此前进程配置全部散落在环境变量 + CLI 参数里（30+ 个 `FLOW_*`），本 crate
//! 把**部署/运行形态**收敛为一份 TOML（`flow.toml`）：
//!
//! ```toml
//! [server]   cron 调度器开关与 journal 触发器 tick
//! [storage]  backend（journal）、data_dir
//! [execution] 执行模式（in_process|ipc|remote）、executor_bin、x_max、[remote]
//! [agent]    flow-agent 的上联地址与证书
//! [journal]  journal-server 的监听地址与数据目录（token 仍是环境变量——凭据）
//! ```
//!
//! 分层加载（高覆盖低）：**CLI `--config`（显式路径）> 环境变量 > 配置文件 >
//! 内置默认值**。所有 `FLOW_*` 环境变量保持原语义（向后兼容：既有部署脚本
//! 与测试不受影响）；配置文件缺省时行为与从前逐字一致。flow-server 退役后
//! 旧 `[server].rpc_addr / http_addr / scheduler_tick_secs` 与 `FLOW_ADDR /
//! FLOW_HTTP_ADDR` 不再生效（deny_unknown_fields 会让存量配置显式报错）。
//!
//! # 统一目录布局（flow 主目录）
//!
//! 运行数据统一住在 flow 主目录（[`flow_home`]：`FLOW_HOME` 环境变量 →
//! `~/.flow`），不散落在进程 CWD 或平台应用目录：
//!
//! ```text
//! ~/.flow/flow.toml     配置文件（检索序末位与 config.update 默认落点）
//! ~/.flow/journal/      journal 权威数据库（flow.sqlite + observations/）
//! ~/.flow/data/         storage 数据目录（密钥存储等；[storage].data_dir 相对此解析）
//! ~/.flow/harness/      harness agent 每次运行的临时沙箱（运行结束即删）
//! ```
//!
//! 明确**不收敛**的配置（保持现状）：`RUST_LOG`（env-filter 惯例）、
//! `FLOW_SECRET_*`（凭据走密钥机制，见 flow-engine secrets）、journal token
//! （每工作区凭据，仅 env）、执行协议冻结常量（帧上限/超时族，改即协议变更）、
//! executor 通道 FD 引导（argv `--channel-fd` 契约）、`FLOW_RUN_LOG_BUDGET` 等
//! engine 内部预算（读取点在 driver 深处，维持 env）。
//!
//! 加载必须在**构造 tokio runtime 之前**同步完成。

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// 配置文件路径的显式覆盖环境变量（优先级最高的文件来源）。
pub const CONFIG_FILE_ENV: &str = "FLOW_CONFIG";

/// flow 主目录的显式覆盖环境变量（测试/多实例隔离用）。
/// 缺省为 `~/.flow`：配置、journal 数据库与 harness 沙箱统一住在这里。
pub const HOME_ENV: &str = "FLOW_HOME";

/// 默认配置文件名（搜索顺序：`./flow.toml` → flow 主目录 `flow.toml`）。
pub const CONFIG_FILE_NAME: &str = "flow.toml";

/// flow 主目录：`FLOW_HOME` 环境变量（非空）→ `~/.flow`。
/// 配置文件、journal 数据库（`journal/`）、storage 数据目录与 harness
/// agent 沙箱（`harness/`）的统一根。返回 None 仅发生在无 home 目录的
/// 退化环境（此时各消费方按各自缺省降级）。
pub fn flow_home() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(HOME_ENV) {
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }
    dirs::home_dir().map(|home| home.join(".flow"))
}

/// 数据目录解析：绝对路径原样返回；相对路径相对 flow 主目录
/// （`data` → `~/.flow/data`），不再按进程 CWD 解释——换目录启动的
/// 进程必须看到同一份数据。无 home 的退化环境回退到 CWD 相对（旧行为）。
pub fn resolve_data_dir(spec: &str) -> PathBuf {
    let path = PathBuf::from(spec);
    if path.is_absolute() {
        return path;
    }
    match flow_home() {
        Some(home) => home.join(path),
        None => path,
    }
}

/// 配置加载与校验过程中可能失败的原因。
///
/// 刻意区分「读文件失败」「解析失败」「语义非法」三类：运维看到这三类
/// 错误时该做的事完全不同——分别是修路径、修语法、修取值。
#[derive(Debug, Error)]
pub enum ConfigError {
    /// 配置文件读不到（路径错、权限不足）。
    #[error("读取配置文件 {path} 失败：{source}")]
    Io {
        /// 出问题的文件路径。
        path: PathBuf,
        /// 底层 IO 错误。
        source: std::io::Error,
    },
    /// TOML 语法错误（含未知字段）。
    #[error("解析配置失败：{0}")]
    Parse(String),
    /// 语法没问题但取值非法（跨字段约束不成立）。
    #[error("配置非法：{0}")]
    Invalid(String),
}

/// 配置文件的顶层结构：每个分区一个子结构。
///
/// 所有分区都带 `serde(default)` 与 `deny_unknown_fields`：前者让缺省分区
/// 可省略，后者让**写错的键显式报错**——静默忽略拼错的键是配置类问题里
/// 最难排查的一种。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub struct Config {
    /// `[server]`：调度器与触发器扫描。
    pub server: ServerConfig,
    /// `[storage]`：后端选择与数据目录。
    pub storage: StorageConfig,
    /// `[execution]`：执行模式与执行器参数。
    pub execution: ExecutionConfig,
    /// `[agent]`：flow-agent 的部署参数。
    pub agent: AgentConfig,
    /// `[journal]`：journal-server 的监听地址与数据目录。
    pub journal: JournalConfig,
    /// `[harness]`：harness 节点的内置 agent 配置。
    pub harness: HarnessConfig,
}

/// `[server]` 分区：与「谁在什么时候扫描触发器」有关。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// cron 调度器开关（原 FLOW_SCHEDULER=off 关闭）。
    /// journal-server 与桌面内嵌服务共用（journal 触发器扫描）。
    pub scheduler_enabled: bool,
    /// journal 触发器扫描 tick（秒）。
    pub journal_trigger_tick_secs: u64,
}

impl Default for ServerConfig {
    /// 调度器默认开启、扫描间隔 20 秒。
    ///
    /// 间隔取 20 秒是权衡：太短会让空转消耗持续 CPU，太长会让定时任务
    /// 的启动延迟肉眼可见。
    fn default() -> Self {
        ServerConfig {
            scheduler_enabled: true,
            journal_trigger_tick_secs: 20,
        }
    }
}

/// 存储后端选择。
///
/// 只剩一个变体，但保留枚举而不是裸字符串：它是**契约的一部分**
/// （配置/迁移文档都按名字引用它），未来若新增后端，枚举能保证所有
/// `match` 点被编译器强制更新。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// 本地 SQLite 业务权威，配置名称保留 journal；不导入旧数据格式。
    Journal,
}

impl StorageBackend {
    /// 序列化成配置与文档中使用的名称。
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageBackend::Journal => "journal",
        }
    }
}

/// `[storage]` 分区：权威后端与非权威数据目录。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// 业务权威后端。
    pub backend: StorageBackend,
    /// 数据目录（原 FLOW_DATA_DIR，默认 flow 主目录下 `data`）：密钥存储与
    /// 桌面历史数据在这里；journal 权威在 [journal].data_dir。
    /// 相对路径相对 flow 主目录解析（见 [`resolve_data_dir`]）。
    pub data_dir: String,
}

impl Default for StorageConfig {
    /// 默认 journal 后端 + 主目录下的 `data` 子目录。
    fn default() -> Self {
        StorageConfig {
            backend: StorageBackend::Journal,
            data_dir: "data".into(),
        }
    }
}

/// 执行模式：节点语义在哪里被执行。
///
/// 这是**安全边界**的开关，不是性能选项：进程内执行意味着用户定义的
/// 脚本与权威写者同处一个故障域，因此该模式已在配置期被拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionModeKind {
    /// 进程内执行（已全平台删除，配置期即拒绝）。
    InProcess,
    /// 通过 IPC 把任务派发给 `flow-executor` 子进程（当前唯一本地模式）。
    Ipc,
    /// 通过 TLS/WSS 派发给远端 agent 托管的执行器。
    Remote,
}

impl ExecutionModeKind {
    /// 序列化成配置与 CLI 使用的名称。
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionModeKind::InProcess => "in_process",
            ExecutionModeKind::Ipc => "ipc",
            ExecutionModeKind::Remote => "remote",
        }
    }
}

/// `[execution]` 分区：执行模式、执行器二进制与槽位。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    /// 执行模式（ipc / remote）。
    pub mode: ExecutionModeKind,
    /// 执行器二进制（原 FLOW_EXECUTOR_BIN）；缺省定位同目录兄弟 flow-executor。
    pub executor_bin: Option<String>,
    /// IPC 执行槽位（1..=16；协议常量 X_MAX_DEFAULT 的可配置面）。
    pub x_max: u32,
    /// 启动时预热一个空闲执行器（省去首次派发的进程启动 + 握手延迟）。
    /// 预热进程占用一个 x_max 槽位；失败降级为首次派发时懒加载。
    /// 桌面 app 默认开启（见 web/src-tauri），服务端默认关闭。
    pub prewarm: bool,
    /// remote 模式的上联与证书；mode=remote 时必填。
    pub remote: Option<RemoteExecutionConfig>,
}

impl Default for ExecutionConfig {
    /// 默认 4 个 IPC 槽位、不预热。
    ///
    /// 槽位取 4 是默认值而非上限：`x_max` 的合法范围是 1..=16，但槽位
    /// 越多常驻的执行器进程越多，空闲时也在占内存。
    fn default() -> Self {
        ExecutionConfig {
            // 默认 ipc（全平台）：unix 走 socketpair，其余走 loopback 回连；
            // in_process 已全平台删除（配置期即拒绝）。
            mode: ExecutionModeKind::Ipc,
            executor_bin: None,
            x_max: 4,
            prewarm: false,
            remote: None,
        }
    }
}

/// `[execution.remote]` 分区：master 侧连接远端 agent 的参数。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RemoteExecutionConfig {
    /// 控制通道地址（master 侧监听，agent 上联）。
    pub control_addr: String,
    /// 已废弃（v3 上联收敛为单条 TLS 连接）：仅为存量配置兼容保留，
    /// 值被忽略（T10 约束：旧配置不应突然被拒绝）。
    #[serde(default)]
    pub data_addr: Option<String>,
    /// 用于校验 agent 证书的 CA。
    pub ca_cert: PathBuf,
    /// master 侧证书。
    pub cert: PathBuf,
    /// master 侧私钥。
    pub key: PathBuf,
    /// 等待 agent 上联/接入的超时（毫秒）。
    pub attach_timeout_ms: u64,
    /// WSS 接入监听地址（T13，代理终止 TLS 场景的附加承载）；缺省关闭。
    pub wss_addr: Option<String>,
    /// WSS 接入的 agent 凭据映射表（`[[execution.remote.agents]]`）：
    /// token → agent_id 显式映射，身份由凭证反查，客户端自报头只做
    /// 一致性断言。与 wss_addr 成对配置（认证不可留空）。
    #[serde(default)]
    pub agents: Vec<AgentCredential>,
}

/// WSS 接入的 agent 凭据（映射表一项）。任何单一凭证只能取得一个
/// agent 身份；id 与 token 在表内都不得重复。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub struct AgentCredential {
    /// agent 身份（会话注册键，对应 x-flow-agent-id 一致性断言）。
    pub id: String,
    /// 该 agent 的接入令牌（不共享）。
    pub token: String,
}

impl Default for RemoteExecutionConfig {
    /// 空的默认配置：地址与证书路径都为空，由 `Config::validate` 拒绝。
    ///
    /// 刻意不给「像样的」默认值——凭空造一个证书路径只会让错误延后到
    /// TLS 握手时才暴露成网络故障。
    fn default() -> Self {
        RemoteExecutionConfig {
            control_addr: String::new(),
            data_addr: None,
            ca_cert: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            // 2 分钟：远端 agent 可能要下载/拉起执行器镜像，给足时间。
            attach_timeout_ms: 120_000,
            wss_addr: None,
            agents: Vec::new(),
        }
    }
}

/// `flow-agent` 的部署参数。CLI 参数仍可逐项覆盖（flag > env > 本节 > 报错）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    /// master 控制通道地址（agent 上联目标）。
    pub control_addr: Option<String>,
    /// 已废弃（v3 上联收敛为单条 TLS 连接）：仅为存量配置兼容保留，
    /// 值被忽略。
    #[serde(default)]
    pub data_addr: Option<String>,
    /// 本 agent 的身份标识；远端侧用它做会话注册键。
    pub agent_id: Option<String>,
    /// 用于校验 master 证书的 CA。
    pub ca_cert: Option<PathBuf>,
    /// agent 侧证书。
    pub cert: Option<PathBuf>,
    /// agent 侧私钥。
    pub key: Option<PathBuf>,
    /// mTLS 服务端证书校验名/SNI（缺省 `flow-server`，与服务端证书 SAN
    /// 对应；使用自有 PKI 且 SAN 不同时在此指定）。
    #[serde(default)]
    pub sni: Option<String>,
    /// agent 本机的执行器二进制路径。
    pub executor_bin: Option<String>,
    /// 本 agent 可提供的执行槽位。
    pub slots: u32,
    /// WSS 上联地址（T13，代理终止 TLS 场景）；设置后优先于 mTLS 直连。
    pub wss_url: Option<String>,
    /// WSS 接入令牌：本 agent 的凭证，master 侧按 token→agent_id 映射表
    /// 反查身份（`x-flow-agent-id` 头只做一致性断言）。
    pub token: Option<String>,
}

impl Default for AgentConfig {
    /// 默认 4 个槽位，其余字段留空（未配置）。
    fn default() -> Self {
        AgentConfig {
            control_addr: None,
            data_addr: None,
            agent_id: None,
            ca_cert: None,
            cert: None,
            key: None,
            sni: None,
            executor_bin: None,
            slots: 4,
            wss_url: None,
            token: None,
        }
    }
}

/// `[harness]` 分区：harness 节点的内置 agent 行为。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HarnessConfig {
    /// harness 节点 agent 模式的模型配置（run 启动时冻结进执行快照）。
    pub agent: HarnessAgentConfig,
}

/// `[harness.agent]`：内置 agent 的模型配置。`base_url` 与 `model` 成对
/// 配置才启用（都留空 = 关闭，agent 模式运行期报清晰错误）。
/// 真值校验（URL 形态/数值边界）在 [`HarnessAgentConfig::validate`]。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HarnessAgentConfig {
    pub base_url: Option<String>,
    pub model: Option<String>,
    /// `openai`（默认，OpenAI 兼容）| `anthropic`
    pub api: Option<String>,
    /// LLM 出口代理；禁止 userinfo/query（代理凭据不进请求/journal）。
    pub proxy: Option<String>,
    /// 1..=200（省略 = conga 默认 50）
    pub max_turns: Option<u64>,
    /// 1..=100（省略 = conga 默认）
    pub max_tool_calls_per_turn: Option<u64>,
    /// 1..=1_000_000
    pub max_tokens: Option<u64>,
    /// 省略 = conga 默认；总生命周期仍受节点 timeout_ms 与一次性 executor 回收约束
    pub tool_timeout_secs: Option<u64>,
}

impl HarnessAgentConfig {
    /// 把「有值」与「非空白」统一成一处判断。
    ///
    /// TOML 里写 `base_url = ""` 与不写是两种语义，前者是显式关闭。
    fn non_empty(value: &Option<String>) -> Option<&str> {
        value.as_deref().filter(|v| !v.trim().is_empty())
    }

    /// 启用判定与跨字段校验。返回 Err 的文案直接面向配置编辑者。
    pub fn validate(&self) -> Result<(), String> {
        let base_url = Self::non_empty(&self.base_url);
        let model = Self::non_empty(&self.model);
        match (base_url, model) {
            (Some(_), None) => {
                return Err("harness.agent.model 与 base_url 必须成对配置".into());
            }
            (None, Some(_)) => {
                return Err("harness.agent.base_url 与 model 必须成对配置".into());
            }
            _ => {}
        }
        if let Some(url) = base_url {
            check_endpoint(url, "harness.agent.base_url")?;
        }
        if let Some(api) = self.api.as_deref().filter(|v| !v.trim().is_empty()) {
            if !matches!(api, "openai" | "anthropic") {
                return Err(format!(
                    "harness.agent.api 非法：{api:?}（允许 openai | anthropic）"
                ));
            }
        }
        if let Some(proxy) = Self::non_empty(&self.proxy) {
            check_endpoint(proxy, "harness.agent.proxy")?;
        }
        for (name, value, max) in [
            ("max_turns", self.max_turns, 200u64),
            ("max_tool_calls_per_turn", self.max_tool_calls_per_turn, 100),
            ("max_tokens", self.max_tokens, 1_000_000),
            ("tool_timeout_secs", self.tool_timeout_secs, 86_400),
        ] {
            if let Some(v) = value {
                if v == 0 || v > max {
                    return Err(format!("harness.agent.{name} 必须在 1..={max}"));
                }
            }
        }
        Ok(())
    }
}

/// 端点 URL 校验：http(s)、无 userinfo、无 query。
fn check_endpoint(url: &str, field: &str) -> Result<(), String> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .ok_or_else(|| format!("{field} 必须是 http(s):// URL（当前 {url:?}）"))?;
    // scheme 之后、首个路径/query/fragment 之前是 authority；userinfo 以 @ 分隔
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.contains('@') {
        return Err(format!(
            "{field} 禁止携带 userinfo（凭据不进配置/请求/journal）：{url:?}"
        ));
    }
    if url.contains('?') {
        return Err(format!("{field} 禁止携带 query：{url:?}"));
    }
    if authority.is_empty() {
        return Err(format!("{field} 缺少主机名：{url:?}"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JournalConfig {
    /// journal v2 WS RPC 监听地址（原 FLOW_JOURNAL_ADDR）。
    pub addr: String,
    /// 下载/webhook HTTP 监听地址（原 FLOW_JOURNAL_HTTP_ADDR；Bearer token 认证）。
    pub http_addr: String,
    /// journal 数据目录（原 FLOW_JOURNAL_DATA_DIR；journal-server 用）。
    /// 缺省 = flow 主目录下 `journal`（`~/.flow/journal`）；相对路径相对
    /// flow 主目录解析（见 [`resolve_data_dir`]）。
    pub data_dir: Option<String>,
}

impl Default for JournalConfig {
    fn default() -> Self {
        JournalConfig {
            addr: "127.0.0.1:9802".into(),
            http_addr: "127.0.0.1:9803".into(),
            data_dir: None,
        }
    }
}

impl Config {
    /// 分层加载：显式路径（CLI `--config`）→ `FLOW_CONFIG` → `./flow.toml`
    /// → 平台配置目录。返回配置、实际读取的文件路径与生效的 env 覆盖名单。
    ///
    /// 显式路径不存在是错误（用户点名了文件）；默认搜索路径都缺失时回落
    /// 纯默认值 + env（与旧行为一致）。
    pub fn load(explicit: Option<&Path>) -> Result<Loaded, ConfigError> {
        let (path, text) = match explicit {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
                (Some(path.to_path_buf()), Some(text))
            }
            None => match Self::find_default_file() {
                Some(path) => {
                    let text =
                        std::fs::read_to_string(&path).map_err(|source| ConfigError::Io {
                            path: path.clone(),
                            source,
                        })?;
                    (Some(path), Some(text))
                }
                None => (None, None),
            },
        };
        let mut config = match &text {
            Some(text) => toml::from_str(text)
                .map_err(|e| ConfigError::Parse(format!("{}：{e}", path_display(&path))))?,
            None => Config::default(),
        };
        let env_overrides = apply_env(&mut config)
            .map_err(|e| ConfigError::Invalid(format!("FLOW_BACKEND：{e}")))?;
        config.validate().map_err(ConfigError::Invalid)?;
        Ok(Loaded {
            config,
            path,
            env_overrides,
        })
    }

    /// 桌面端/嵌入式入口：配置文件可选（缺省即默认值），路径固定。
    pub fn load_or_default(path: &Path) -> Result<Loaded, ConfigError> {
        match path.exists() {
            true => Self::load(Some(path)),
            false => Ok(Loaded {
                config: Config::default(),
                path: Some(path.to_path_buf()),
                env_overrides: Vec::new(),
            }),
        }
    }

    /// 搜索默认配置文件：`FLOW_CONFIG` → `./flow.toml` → flow 主目录。
    ///
    /// 顺序即优先级：环境变量显式指定优先于当前目录，当前目录优先于
    /// 主目录。找不到就返回 None（回落纯默认值 + env）。
    fn find_default_file() -> Option<PathBuf> {
        if let Ok(path) = std::env::var(CONFIG_FILE_ENV) {
            let path = PathBuf::from(path);
            if path.exists() {
                return Some(path);
            }
        }
        let local = PathBuf::from(CONFIG_FILE_NAME);
        if local.exists() {
            return Some(local);
        }
        flow_home()
            .map(|home| home.join(CONFIG_FILE_NAME))
            .filter(|p| p.exists())
    }

    /// config.update 在启动时无配置文件情况下的落点：运维声明的
    /// `FLOW_CONFIG`（即使启动时文件尚不存在）→ flow 主目录
    /// （与 [`Config::find_default_file`] 检索序末位一致，写下去下次启动
    /// 一定找得到）。绝不落到进程 CWD——换目录启动会丢配置。
    /// 返回 `None` 只可能发生在无 home 目录的环境，调用方应拒绝写入。
    pub fn default_write_path() -> Option<PathBuf> {
        if let Ok(path) = std::env::var(CONFIG_FILE_ENV) {
            if !path.is_empty() {
                return Some(PathBuf::from(path));
            }
        }
        flow_home().map(|home| home.join(CONFIG_FILE_NAME))
    }

    /// 写入口统一校验。所有监听地址必须可解析；枚举值在 deserialize 层已把关，
    /// 这里管跨字段约束。
    pub fn validate(&self) -> Result<(), String> {
        if self.server.journal_trigger_tick_secs == 0 {
            return Err("server.journal_trigger_tick_secs 必须 > 0".into());
        }
        if !(1..=16).contains(&self.execution.x_max) {
            return Err("execution.x_max 必须在 1..=16".into());
        }
        // 进程内执行路径已全平台删除（节点执行语义只在 flow-executor），
        // 配置期就明确拒绝——比等到执行装配时才报错更早、更贴近用户输入。
        if self.execution.mode == ExecutionModeKind::InProcess {
            return Err("execution.mode=in_process 已删除：本地执行只有 ipc".into());
        }
        if self.execution.mode == ExecutionModeKind::Remote && self.execution.remote.is_none() {
            return Err("execution.mode=remote 时必须配置 [execution.remote]".into());
        }
        if let Some(remote) = &self.execution.remote {
            if remote.control_addr.is_empty() {
                return Err("execution.remote.control_addr 不能为空".into());
            }
            // T13：WSS 监听地址与凭据映射表必须成对配置（认证不能留空）。
            if remote.wss_addr.is_some() != !remote.agents.is_empty() {
                return Err(
                    "execution.remote.wss_addr 与 agents 必须同时配置（WSS 认证不可留空）".into(),
                );
            }
            // 凭据表的硬约束：id/token 非空、各自唯一——token→身份必须
            // 一对一，否则同一凭证能取得多个身份。
            let mut ids = std::collections::BTreeSet::new();
            let mut tokens = std::collections::BTreeSet::new();
            for agent in &remote.agents {
                if agent.id.is_empty() || agent.id.len() > 128 {
                    return Err("execution.remote.agents[].id 必须非空且不超过 128 字节".into());
                }
                if agent.token.is_empty() {
                    return Err(format!(
                        "execution.remote.agents[].token 不能为空（agent {}）",
                        agent.id
                    ));
                }
                if !ids.insert(agent.id.clone()) {
                    return Err(format!(
                        "execution.remote.agents 重复的 agent id：{}",
                        agent.id
                    ));
                }
                if !tokens.insert(agent.token.clone()) {
                    return Err(format!(
                        "execution.remote.agents 重复的 token（agent {}）：token→身份必须一对一",
                        agent.id
                    ));
                }
            }
            if remote.ca_cert.as_os_str().is_empty()
                || remote.cert.as_os_str().is_empty()
                || remote.key.as_os_str().is_empty()
            {
                return Err("execution.remote 的 ca_cert / cert / key 不能为空".into());
            }
        }
        if self.agent.slots == 0 {
            return Err("agent.slots 必须 > 0".into());
        }
        self.harness.agent.validate()?;
        let _ = parse_addr(&self.journal.addr, "journal.addr")?;
        let _ = parse_addr(&self.journal.http_addr, "journal.http_addr")?;
        Ok(())
    }

    /// 从 TOML 文本解析（不叠加 env）。config.update 的合并基准与
    /// 嵌入式启动读文件共用。
    pub fn from_toml_str(text: &str) -> Result<Config, ConfigError> {
        toml::from_str(text).map_err(|e| ConfigError::Parse(format!("配置解析失败：{e}")))
    }

    /// 原子写回（tmp + rename）。写前校验，坏配置永远不落盘。
    pub fn save(&self, path: &Path) -> Result<(), ConfigError> {
        self.validate().map_err(ConfigError::Invalid)?;
        let mut body = toml::to_string_pretty(self)
            .map_err(|e| ConfigError::Parse(format!("配置序列化失败：{e}")))?;
        body.insert_str(
            0,
            "# flow 统一配置（flow config update 会整文件重写，手工注释不会保留）\n\
             # 分层：CLI --config > 环境变量 > 本文件 > 内置默认值\n\n",
        );
        durable_write(path, body.as_bytes()).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(())
    }

    /// config.update 的合并语义：patch 是部分 JSON（分区可选，给了就整节替换）。
    /// 合并后必须通过 validate，否则原样报错、不产生半成品。
    pub fn merge_patch(base: &Config, patch: &Value) -> Result<Config, ConfigError> {
        let obj = patch
            .as_object()
            .ok_or_else(|| ConfigError::Invalid("config 补丁必须是 JSON 对象".into()))?;
        let mut out = base.clone();
        macro_rules! replace_section {
            ($field:expr, $key:literal, $value:expr) => {{
                $field = serde_json::from_value($value.clone())
                    .map_err(|e| ConfigError::Invalid(format!("分区 {} 非法：{e}", $key)))?;
            }};
        }
        for (key, value) in obj {
            match key.as_str() {
                "server" => replace_section!(out.server, "server", value),
                "storage" => replace_section!(out.storage, "storage", value),
                "execution" => replace_section!(out.execution, "execution", value),
                "agent" => replace_section!(out.agent, "agent", value),
                "journal" => replace_section!(out.journal, "journal", value),
                "harness" => replace_section!(out.harness, "harness", value),
                other => {
                    return Err(ConfigError::Invalid(format!(
                        "未知配置分区 {other:?}（合法：server/storage/execution/agent/journal/harness）"
                    )))
                }
            }
        }
        out.validate().map_err(ConfigError::Invalid)?;
        Ok(out)
    }

    /// 覆盖该配置的 env 名单（不含值——FLOW_JOURNAL_TOKEN 等凭据不进配置）。
    pub fn env_override_names(&self) -> Vec<String> {
        let mut probe = self.clone();
        apply_env(&mut probe).unwrap_or_default()
    }
}

/// load 的返回：合并结果 + 文件路径 + env 覆盖名单。
#[derive(Debug, Clone)]
pub struct Loaded {
    /// 合并后的最终配置。
    pub config: Config,
    /// 实际读取（或将写入）的配置文件路径；None = 未找到文件。
    pub path: Option<PathBuf>,
    /// 生效的环境变量覆盖（仅名称）。
    pub env_overrides: Vec<String>,
}

/// 把可选路径渲染成可读文本（用于错误信息）。
///
/// 没有路径时用占位符 `<config>`，让错误文案始终指明「是哪个来源」。
fn path_display(path: &Option<PathBuf>) -> &str {
    path.as_deref().and_then(Path::to_str).unwrap_or("<config>")
}

/// 解析监听地址，并在失败时给出可直接照抄的示例。
fn parse_addr(addr: &str, field: &str) -> Result<std::net::SocketAddr, String> {
    addr.parse()
        .map_err(|_| format!("{field} 不是合法地址：{addr:?}（形如 127.0.0.1:9800）"))
}

/// 把进程环境变量覆盖到配置上（手写映射，显式可 grep）。返回生效的变量名。
///
/// 语义与旧读取点逐字对齐（特别是 FLOW_SCHEDULER：仅 `off` 关闭，其他值
/// 一律开启——旧代码对未知值不报错，这里保持）。
fn apply_env(config: &mut Config) -> Result<Vec<String>, String> {
    let mut applied = Vec::new();
    let env_str =
        |name: &str| -> Option<String> { std::env::var(name).ok().filter(|v| !v.is_empty()) };
    let env_u64 = |name: &str| -> Option<u64> { env_str(name).and_then(|v| v.parse().ok()) };

    // ---- [server] ----
    if let Ok(v) = std::env::var("FLOW_SCHEDULER") {
        config.server.scheduler_enabled = v != "off";
        applied.push("FLOW_SCHEDULER".into());
    }

    // ---- [storage] ----
    if let Some(v) = env_str("FLOW_BACKEND") {
        // 不静默回落：非法值走 Err，与 toml 未知值同语义。
        match v.as_str() {
            "journal" | "jsonl" => config.storage.backend = StorageBackend::Journal,
            other => {
                return Err(format!(
                    "FLOW_BACKEND={other} 不可用（合法：journal；\
                     sqlite / postgres（v1）已删除，历史数据不迁移，\
                     见 docs/SQLITE_V1_TO_V2_MIGRATION.md）"
                ));
            }
        }
        applied.push("FLOW_BACKEND".into());
    }
    if let Some(v) = env_str("FLOW_DATA_DIR") {
        config.storage.data_dir = v;
        applied.push("FLOW_DATA_DIR".into());
    }

    // ---- [execution] ----
    if let Some(v) = env_str("FLOW_EXECUTION_MODE") {
        // 空值视为未设置（不覆盖平台默认）；显式值覆盖；非法值必须报错
        // 而不是静默回落文件值——拼写错误（ipcc 之类）静默换执行模式比
        // 启动失败危险得多（同变量另一入口 execution::mode_from_env
        // 也是这个语义）。
        config.execution.mode = match v.as_str() {
            "in_process" => ExecutionModeKind::InProcess,
            "ipc" => ExecutionModeKind::Ipc,
            "remote" => ExecutionModeKind::Remote,
            other => {
                return Err(format!(
                    "FLOW_EXECUTION_MODE 非法值 {other:?}（可选 ipc | remote；in_process 已删除）"
                ))
            }
        };
        applied.push("FLOW_EXECUTION_MODE".into());
    }
    if let Some(v) = env_str("FLOW_EXECUTOR_BIN") {
        config.execution.executor_bin = Some(v);
        applied.push("FLOW_EXECUTOR_BIN".into());
    }
    if let Some(v) = env_str("FLOW_REMOTE_CONTROL_ADDR") {
        config
            .execution
            .remote
            .get_or_insert_with(Default::default)
            .control_addr = v;
        applied.push("FLOW_REMOTE_CONTROL_ADDR".into());
    }
    if let Some(v) = env_str("FLOW_REMOTE_CA") {
        config
            .execution
            .remote
            .get_or_insert_with(Default::default)
            .ca_cert = v.into();
        applied.push("FLOW_REMOTE_CA".into());
    }
    if let Some(v) = env_str("FLOW_REMOTE_CERT") {
        config
            .execution
            .remote
            .get_or_insert_with(Default::default)
            .cert = v.into();
        applied.push("FLOW_REMOTE_CERT".into());
    }
    if let Some(v) = env_str("FLOW_REMOTE_KEY") {
        config
            .execution
            .remote
            .get_or_insert_with(Default::default)
            .key = v.into();
        applied.push("FLOW_REMOTE_KEY".into());
    }
    if let Some(v) = env_u64("FLOW_REMOTE_ATTACH_TIMEOUT_MS") {
        config
            .execution
            .remote
            .get_or_insert_with(Default::default)
            .attach_timeout_ms = v;
        applied.push("FLOW_REMOTE_ATTACH_TIMEOUT_MS".into());
    }

    // ---- [agent] ----
    if let Some(v) = env_str("FLOW_AGENT_CONTROL_ADDR") {
        config.agent.control_addr = Some(v);
        applied.push("FLOW_AGENT_CONTROL_ADDR".into());
    }
    if let Some(v) = env_str("FLOW_AGENT_ID") {
        config.agent.agent_id = Some(v);
        applied.push("FLOW_AGENT_ID".into());
    }
    if let Some(v) = env_str("FLOW_AGENT_CA") {
        config.agent.ca_cert = Some(v.into());
        applied.push("FLOW_AGENT_CA".into());
    }
    if let Some(v) = env_str("FLOW_AGENT_CERT") {
        config.agent.cert = Some(v.into());
        applied.push("FLOW_AGENT_CERT".into());
    }
    if let Some(v) = env_str("FLOW_AGENT_KEY") {
        config.agent.key = Some(v.into());
        applied.push("FLOW_AGENT_KEY".into());
    }
    if let Some(v) = env_u64("FLOW_AGENT_SLOTS") {
        // u64→u32 截断会把 2^32+8 静默变 8；显式报错。
        config.agent.slots =
            u32::try_from(v).map_err(|_| format!("FLOW_AGENT_SLOTS 超出 u32 范围：{v}"))?;
        applied.push("FLOW_AGENT_SLOTS".into());
    }
    if let Some(v) = env_str("FLOW_AGENT_SNI") {
        config.agent.sni = Some(v);
        applied.push("FLOW_AGENT_SNI".into());
    }

    // ---- [journal] ----
    if let Some(v) = env_str("FLOW_JOURNAL_ADDR") {
        config.journal.addr = v;
        applied.push("FLOW_JOURNAL_ADDR".into());
    }
    if let Some(v) = env_str("FLOW_JOURNAL_HTTP_ADDR") {
        config.journal.http_addr = v;
        applied.push("FLOW_JOURNAL_HTTP_ADDR".into());
    }
    if let Some(v) = env_str("FLOW_JOURNAL_DATA_DIR") {
        config.journal.data_dir = Some(v);
        applied.push("FLOW_JOURNAL_DATA_DIR".into());
    }

    applied.sort();
    applied.dedup();
    Ok(applied)
}

impl Config {
    /// journal 权威数据目录（含 env 叠加后的值）：显式配置绝对路径原样、
    /// 相对路径相对 flow 主目录；缺省 = flow 主目录下 `journal`。
    /// home 不可用的退化环境 + 未显式配置时返回 None（调用方报错）。
    pub fn journal_data_dir(&self) -> Option<PathBuf> {
        match self.journal.data_dir.as_deref() {
            Some(spec) => Some(resolve_data_dir(spec)),
            None => flow_home().map(|home| home.join("journal")),
        }
    }

    /// storage 数据目录（密钥存储等）：相对路径相对 flow 主目录解析。
    pub fn storage_data_dir(&self) -> PathBuf {
        resolve_data_dir(&self.storage.data_dir)
    }

    /// journal 触发器扫描间隔（秒 → Duration）。
    pub fn journal_trigger_tick(&self) -> Duration {
        Duration::from_secs(self.server.journal_trigger_tick_secs)
    }
}

/// Atomic replacement; Unix success includes syncing the file and parent directory.
///
/// 原子替换的实现：先写同目录下的临时文件并 fsync，再 `rename` 覆盖目标。
/// `rename` 在同一文件系统内是原子的，因此读者要么看到旧文件、要么看到
/// 新文件，**不会看到写了一半的配置文件**。
///
/// Unix 上还要 fsync 父目录：否则 rename 本身可能只存在于 page cache 里，
/// 崩溃后目录项可能回退——数据看起来「丢了一次写入」。
fn durable_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    // 临时文件名带进程号 + 计数器并用 create_new 独占创建：
    // 避免两个进程写同一个配置目录时互相覆盖临时文件。
    let (tmp, mut file) = loop {
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = parent.join(format!(".flow-config-{}-{id}.tmp", std::process::id()));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => break (tmp, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    let result = (|| {
        file.write_all(bytes)?;
        // 先把内容刷盘再 rename：否则崩溃可能留下一个已改名但内容为空的文件。
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        // 失败时清掉临时文件，避免目录里堆积半成品。
        let _ = std::fs::remove_file(tmp);
    }
    result
}

#[cfg(test)]
/// 配置模型的单元测试。
///
/// 覆盖三类风险：默认值必须与旧 env 语义一致、跨字段约束必须能拒绝
/// 非法组合、已删除的配置段必须显式报错而不是被静默忽略。
mod tests {
    #[test]
    fn harness_agent_section_validates_pairing_urls_and_bounds() {
        // 旧 toml（无 section）可解析，默认关闭
        let legacy: Config = toml::from_str("[server]\n").unwrap();
        assert_eq!(legacy.harness, HarnessConfig::default());
        legacy.validate().unwrap();

        // 成对 + 合法配置
        let ok: Config = toml::from_str(
            "[harness.agent]\nbase_url='https://api.deepseek.com/v1'\nmodel='deepseek-chat'\napi='anthropic'\nmax_turns=50\n",
        )
        .unwrap();
        ok.validate().unwrap();

        // 成对校验：只配一半拒绝
        for half in ["base_url='https://x/v1'", "model='m'"] {
            let config: Config = toml::from_str(&format!("[harness.agent]\n{half}\n")).unwrap();
            assert!(config.validate().is_err(), "{half} 不该单独出现");
        }

        // URL 形态：非 http(s) / userinfo / query 拒绝（base_url 与 proxy 同规则）
        for bad_url in [
            "ftp://x",
            "https://user:pass@x/v1",
            "https://x/v1?token=1",
            "not-a-url",
        ] {
            for field in ["base_url", "proxy"] {
                let body = if field == "base_url" {
                    format!("base_url='{bad_url}'\nmodel='m'\n")
                } else {
                    format!("base_url='https://x/v1'\nmodel='m'\n{field}='{bad_url}'\n")
                };
                let config: Config = toml::from_str(&format!("[harness.agent]\n{body}")).unwrap();
                assert!(config.validate().is_err(), "{field}={bad_url} 应被拒绝");
            }
        }

        // api 枚举与数值边界
        for bad in [
            "api='bedrock'",
            "max_turns=0",
            "max_turns=201",
            "max_tool_calls_per_turn=0",
            "max_tool_calls_per_turn=101",
            "max_tokens=0",
            "max_tokens=1000001",
            "tool_timeout_secs=0",
        ] {
            let config: Config = toml::from_str(&format!(
                "[harness.agent]\nbase_url='https://x/v1'\nmodel='m'\n{bad}\n"
            ))
            .unwrap();
            assert!(config.validate().is_err(), "{bad} 应被拒绝");
        }

        // merge_patch 整节替换 + 未知分区仍拒绝
        let patched = Config::merge_patch(
            &Config::default(),
            &serde_json::json!({"harness": {"agent": {"base_url": "http://127.0.0.1:1/v1", "model": "m"}}}),
        )
        .unwrap();
        assert_eq!(
            patched.harness.agent.base_url.as_deref(),
            Some("http://127.0.0.1:1/v1")
        );
        assert!(Config::merge_patch(
            &patched,
            &serde_json::json!({"harness": {"agent": {"proxy": "socks5://127.0.0.1:1080"}}})
        )
        .is_err());

        // 序列化往返
        let encoded = toml::to_string(&ok).unwrap();
        assert_eq!(toml::from_str::<Config>(&encoded).unwrap(), ok);
    }

    #[test]
    fn removed_cluster_configuration_is_rejected_instead_of_silently_ignored() {
        let text = "[journal.cluster]\ncluster_id='production'\nnode_id='journal-a'\n";
        let error = toml::from_str::<super::Config>(text)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field `cluster`"), "{error}");
        assert!(super::Config::merge_patch(
            &super::Config::default(),
            &serde_json::json!({"journal":{"cluster":{}}}),
        )
        .is_err());
    }
    use super::*;

    fn clean_env(names: &[&str]) -> Vec<(String, Option<std::ffi::OsString>)> {
        names
            .iter()
            .map(|n| {
                let old = std::env::var_os(n);
                std::env::remove_var(n);
                (n.to_string(), old)
            })
            .collect()
    }

    fn restore_env(saved: Vec<(String, Option<std::ffi::OsString>)>) {
        for (name, old) in saved {
            match old {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
    }

    #[test]
    fn defaults_match_legacy_env_defaults() {
        let config = Config::default();
        config.validate().expect("默认值必须合法");
        assert_eq!(config.storage.backend, StorageBackend::Journal);
        assert_eq!(config.storage.data_dir, "data");
        // 默认 ipc（全平台；进程内执行已删除）。
        assert_eq!(config.execution.mode, ExecutionModeKind::Ipc);
        assert_eq!(config.execution.x_max, 4);
        // 服务端默认不预热；桌面 app 在无配置文件时显式开启。
        assert!(!config.execution.prewarm);
        assert!(config.server.scheduler_enabled);
        assert_eq!(config.server.journal_trigger_tick_secs, 20);
    }

    #[test]
    fn parses_full_toml_and_roundtrips() {
        let text = r#"
[server]
scheduler_enabled = false
journal_trigger_tick_secs = 5

[storage]
backend = "journal"
data_dir = "/var/lib/flow"

[execution]
mode = "ipc"
x_max = 8
prewarm = true
executor_bin = "/usr/local/bin/flow-executor"

[agent]
control_addr = "main.example:9700"
slots = 8

[journal]
addr = "127.0.0.1:9802"
http_addr = "127.0.0.1:9803"
data_dir = "/var/lib/flow/journal"
"#;
        let config: Config = toml::from_str(text).expect("解析失败");
        config.validate().expect("校验失败");
        assert!(!config.server.scheduler_enabled);
        assert_eq!(config.server.journal_trigger_tick_secs, 5);
        assert_eq!(config.storage.backend, StorageBackend::Journal);
        assert_eq!(config.execution.mode, ExecutionModeKind::Ipc);
        assert_eq!(config.execution.x_max, 8);
        assert!(config.execution.prewarm);
        assert_eq!(config.agent.slots, 8);
        assert_eq!(
            config.journal.data_dir.as_deref(),
            Some("/var/lib/flow/journal")
        );

        let path =
            std::env::temp_dir().join(format!("flow-config-test-{}.toml", std::process::id()));
        config.save(&path).expect("save 失败");
        // 直接解析落盘文本做 roundtrip 对拍（不走 Config::load——它会叠加进程
        // env，测试并行时其他用例的变量会串进来）。
        let text = std::fs::read_to_string(&path).expect("save 的文件读不回来");
        let reloaded: Config = toml::from_str(&text).expect("重新解析失败");
        assert_eq!(reloaded, config);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unknown_keys_and_sections_are_rejected() {
        let result = toml::from_str::<Config>("[server]\nnope = 1\n");
        assert!(result.is_err(), "未知键必须被拒绝");
        // v1 时代的 [server].rpc_addr / http_addr 已随 flow-server 退役：
        // 存量配置里的旧键必须显式报错（deny_unknown_fields），不能静默忽略。
        let stale = toml::from_str::<Config>("[server]\nrpc_addr = \"127.0.0.1:9800\"\n");
        assert!(stale.is_err(), "退役的 [server] 键必须被拒绝");
        let result = Config::merge_patch(&Config::default(), &serde_json::json!({"bogus": {}}));
        assert!(result.is_err());
    }

    #[test]
    fn wss_requires_addr_and_credentials_together() {
        // T13：WSS 监听与凭据映射表成对；单边配置必须报错。
        let base = "[execution.remote]\ncontrol_addr = \"127.0.0.1:1\"\nca_cert = \"a\"\ncert = \"b\"\nkey = \"c\"\n";
        let only_addr =
            toml::from_str::<Config>(&format!("{base}wss_addr = \"127.0.0.1:2\"\n")).unwrap();
        assert!(only_addr.validate().is_err());
        let only_credential = toml::from_str::<Config>(&format!(
            "{base}[[execution.remote.agents]]\nid = \"agent-1\"\ntoken = \"t\"\n"
        ))
        .unwrap();
        assert!(only_credential.validate().is_err());
        let both = toml::from_str::<Config>(&format!(
            "{base}wss_addr = \"127.0.0.1:2\"\n[[execution.remote.agents]]\nid = \"agent-1\"\ntoken = \"t\"\n"
        ))
        .unwrap();
        assert!(both.validate().is_ok());
        // token→身份必须一对一：重复 token / 重复 id 都在配置期报错。
        let duplicate_token = toml::from_str::<Config>(&format!(
            "{base}wss_addr = \"127.0.0.1:2\"\n\
             [[execution.remote.agents]]\nid = \"agent-1\"\ntoken = \"t\"\n\
             [[execution.remote.agents]]\nid = \"agent-2\"\ntoken = \"t\"\n"
        ))
        .unwrap();
        assert!(duplicate_token.validate().is_err());
        let duplicate_id = toml::from_str::<Config>(&format!(
            "{base}wss_addr = \"127.0.0.1:2\"\n\
             [[execution.remote.agents]]\nid = \"agent-1\"\ntoken = \"t1\"\n\
             [[execution.remote.agents]]\nid = \"agent-1\"\ntoken = \"t2\"\n"
        ))
        .unwrap();
        assert!(duplicate_id.validate().is_err());
    }

    #[test]
    fn deprecated_data_addr_is_accepted_and_ignored() {
        // T10：v2 双流时代的 data_addr 在存量配置里广泛存在，v3 单流后
        // 字段废弃但必须继续解析（值被忽略），不能因 deny_unknown_fields
        // 突然拒绝旧配置。
        let config = toml::from_str::<Config>(
            "[execution.remote]\ncontrol_addr = \"127.0.0.1:9801\"\
             \ndata_addr = \"127.0.0.1:9802\"\
             \nca_cert = \"ca.pem\"\ncert = \"c.pem\"\nkey = \"k.pem\"\n\n\
             [agent]\ndata_addr = \"127.0.0.1:9803\"\n",
        )
        .expect("存量 data_addr 配置必须可解析");
        config.validate().expect("废弃字段不影响校验");
        let remote = config.execution.remote.expect("remote section");
        assert_eq!(remote.control_addr, "127.0.0.1:9801");
        assert_eq!(remote.data_addr.as_deref(), Some("127.0.0.1:9802"));
        assert_eq!(config.agent.data_addr.as_deref(), Some("127.0.0.1:9803"));
    }

    #[test]
    fn env_overrides_win_and_are_reported() {
        let saved = clean_env(&["FLOW_BACKEND", "FLOW_SCHEDULER", "FLOW_DATA_DIR"]);
        std::env::set_var("FLOW_BACKEND", "journal");
        std::env::set_var("FLOW_SCHEDULER", "off");
        std::env::set_var("FLOW_DATA_DIR", "/tmp/flow-env-override");

        let mut config = Config::default();
        let applied = apply_env(&mut config).unwrap();
        assert_eq!(config.storage.backend, StorageBackend::Journal);
        assert_eq!(config.storage.data_dir, "/tmp/flow-env-override");
        assert!(!config.server.scheduler_enabled);
        assert!(applied.contains(&"FLOW_SCHEDULER".to_string()));

        // v1 后端值不再可用：postgres 与 sqlite 同样显式报错
        std::env::set_var("FLOW_BACKEND", "postgres");
        let mut config = Config::default();
        assert!(apply_env(&mut config).is_err());

        restore_env(saved);
    }

    #[test]
    fn merge_patch_replaces_whole_sections_and_validates() {
        let patch = serde_json::json!({
            "server": {"scheduler_enabled": true, "journal_trigger_tick_secs": 10}
        });
        let merged = Config::merge_patch(&Config::default(), &patch).expect("合并失败");
        assert_eq!(merged.server.journal_trigger_tick_secs, 10);
        // 未给的分区保持 base
        assert_eq!(merged.storage.data_dir, "data");

        // 非法合并被拦下：v1 的 postgres backend 在 deserialize 层即拒绝
        let bad = serde_json::json!({"storage": {"backend": "postgres", "data_dir": "data"}});
        assert!(Config::merge_patch(&Config::default(), &bad).is_err());
        // 跨字段约束由 validate 把关
        let bad = serde_json::json!({"execution": {"x_max": 0}});
        assert!(Config::merge_patch(&Config::default(), &bad).is_err());
    }

    #[test]
    fn explicit_missing_file_is_an_error() {
        let result = Config::load(Some(Path::new("/nonexistent/flow-config-test.toml")));
        assert!(matches!(result, Err(ConfigError::Io { .. })));
    }

    /// FLOW_HOME/FLOW_CONFIG env 手术的测试共用锁：并行 set/remove
    /// 同名变量会互相践踏（flow-engine spawn.rs 的 ENV_LOCK 同款）。
    static HOME_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn flow_home_env_override_and_defaults() {
        let _guard = HOME_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // 未设 FLOW_HOME：~/.flow（home 总是存在；仅验证后缀拼装）
        let saved = clean_env(&["FLOW_HOME", "FLOW_CONFIG"]);
        assert!(
            flow_home().is_some_and(|home| home.ends_with(".flow")),
            "缺省 flow home 应是 ~/.flow：{:?}",
            flow_home()
        );

        // FLOW_HOME 覆盖（测试/多实例隔离）
        std::env::set_var("FLOW_HOME", "/tmp/flow-home-override");
        assert_eq!(flow_home(), Some(PathBuf::from("/tmp/flow-home-override")));
        // 空值视为未设置
        std::env::set_var("FLOW_HOME", "");
        assert!(
            flow_home().is_some_and(|home| home.ends_with(".flow")),
            "空 FLOW_HOME 应忽略"
        );

        // default_write_path：FLOW_CONFIG 优先，否则 flow 主目录下的 flow.toml
        std::env::set_var("FLOW_CONFIG", "/tmp/declared.toml");
        assert_eq!(
            Config::default_write_path(),
            Some(PathBuf::from("/tmp/declared.toml"))
        );
        std::env::remove_var("FLOW_CONFIG");
        std::env::set_var("FLOW_HOME", "/tmp/flow-home-override");
        assert_eq!(
            Config::default_write_path(),
            Some(PathBuf::from("/tmp/flow-home-override/flow.toml"))
        );
        restore_env(saved);
    }

    #[test]
    fn data_dirs_resolve_against_flow_home() {
        let _guard = HOME_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved = clean_env(&["FLOW_HOME"]);
        std::env::set_var("FLOW_HOME", "/tmp/flow-home-resolve");

        // 绝对路径原样；相对路径相对 flow 主目录（不再按 CWD 解释）
        assert_eq!(
            resolve_data_dir("/var/lib/flow"),
            PathBuf::from("/var/lib/flow")
        );
        assert_eq!(
            resolve_data_dir("data"),
            PathBuf::from("/tmp/flow-home-resolve/data")
        );

        let config = Config::default();
        assert_eq!(
            config.storage_data_dir(),
            PathBuf::from("/tmp/flow-home-resolve/data")
        );
        assert_eq!(
            config.journal_data_dir(),
            Some(PathBuf::from("/tmp/flow-home-resolve/journal"))
        );

        let configured: Config = toml::from_str(
            "[storage]\ndata_dir = \"custom\"\n\n[journal]\ndata_dir = \"journal-x\"\n",
        )
        .unwrap();
        assert_eq!(
            configured.storage_data_dir(),
            PathBuf::from("/tmp/flow-home-resolve/custom")
        );
        assert_eq!(
            configured.journal_data_dir(),
            Some(PathBuf::from("/tmp/flow-home-resolve/journal-x"))
        );

        // 绝对配置不受 flow home 影响
        let absolute: Config = toml::from_str("[journal]\ndata_dir = \"/srv/journal\"\n").unwrap();
        assert_eq!(
            absolute.journal_data_dir(),
            Some(PathBuf::from("/srv/journal"))
        );

        restore_env(saved);
    }
}
