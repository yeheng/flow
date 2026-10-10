//! flow-config：统一配置模型（叶子 crate）。
//!
//! 此前进程配置全部散落在环境变量 + CLI 参数里（30+ 个 `FLOW_*`），本 crate
//! 把**部署/运行形态**收敛为一份 TOML（`flow.toml`）：
//!
//! ```toml
//! [server]   cron 调度器开关与 journal 触发器 tick
//! [storage]  backend（journal）、data_dir
//! [execution] 执行模式（in_process|ipc|remote）、executor_bin、x_max、[remote]
//! [agent]    flow-agent 的上联地址与证书（原先只有 CLI 参数）
//! [journal]  journal-server 的监听地址与数据目录（token 仍是环境变量——凭据）
//! ```
//!
//! 分层加载（高覆盖低）：**CLI `--config`（显式路径）> 环境变量 > 配置文件 >
//! 内置默认值**。所有 `FLOW_*` 环境变量保持原语义（向后兼容：既有部署脚本
//! 与测试不受影响）；配置文件缺省时行为与从前逐字一致。flow-server 退役后
//! 旧 `[server].rpc_addr / http_addr / scheduler_tick_secs` 与 `FLOW_ADDR /
//! FLOW_HTTP_ADDR` 不再生效（deny_unknown_fields 会让存量配置显式报错）。
//!
//! 明确**不收敛**的配置（保持现状）：`RUST_LOG`（env-filter 惯例）、
//! `FLOW_SECRET_*`（凭据走密钥机制，见 flow-engine secrets）、journal token
//! （每工作区凭据，仅 env）、执行协议冻结常量（帧上限/超时族，改即协议变更）、
//! executor FD 槽位（父进程 pre_exec 契约）、`FLOW_RUN_LOG_BUDGET` 等
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

/// 默认配置文件名（搜索顺序：`./flow.toml` → 平台配置目录 `flow/flow.toml`）。
pub const CONFIG_FILE_NAME: &str = "flow.toml";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("读取配置文件 {path} 失败：{source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("解析配置失败：{0}")]
    Parse(String),
    #[error("配置非法：{0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub execution: ExecutionConfig,
    pub agent: AgentConfig,
    pub journal: JournalConfig,
}

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
    fn default() -> Self {
        ServerConfig {
            scheduler_enabled: true,
            journal_trigger_tick_secs: 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// JSONL v2 journal 权威（`<data_dir>/` 即 journal 根，SQLite 只是
    /// 可重建投影）。唯一后端；v1 的 sqlite/postgres 已删除，历史数据
    /// 不做迁移，见 docs/SQLITE_V1_TO_V2_MIGRATION.md。
    Journal,
}

impl StorageBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageBackend::Journal => "journal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    /// 数据目录（原 FLOW_DATA_DIR，默认 CWD 下 `data`）：密钥存储与
    /// 桌面历史数据在这里；journal 权威在 [journal].data_dir。
    pub data_dir: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            // 默认 = journal（JSONL v2 权威）。sqlite（v1）已删除；历史数据
            // 不迁移，见 docs/SQLITE_V1_TO_V2_MIGRATION.md。
            backend: StorageBackend::Journal,
            data_dir: "data".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionModeKind {
    InProcess,
    Ipc,
    Remote,
}

impl ExecutionModeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionModeKind::InProcess => "in_process",
            ExecutionModeKind::Ipc => "ipc",
            ExecutionModeKind::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    pub mode: ExecutionModeKind,
    /// 执行器二进制（原 FLOW_EXECUTOR_BIN）；缺省定位同目录兄弟 flow-executor。
    pub executor_bin: Option<String>,
    /// IPC 执行槽位（1..=16；协议常量 X_MAX_DEFAULT 的可配置面）。
    pub x_max: u32,
    /// remote 模式的上联与证书；mode=remote 时必填。
    pub remote: Option<RemoteExecutionConfig>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig {
            mode: ExecutionModeKind::InProcess,
            executor_bin: None,
            x_max: 4,
            remote: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RemoteExecutionConfig {
    pub control_addr: String,
    pub data_addr: String,
    pub ca_cert: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub attach_timeout_ms: u64,
}

impl Default for RemoteExecutionConfig {
    fn default() -> Self {
        RemoteExecutionConfig {
            control_addr: String::new(),
            data_addr: String::new(),
            ca_cert: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            attach_timeout_ms: 120_000,
        }
    }
}

/// `flow-agent` 的部署参数。CLI 参数仍可逐项覆盖（flag > env > 本节 > 报错）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    pub control_addr: Option<String>,
    pub data_addr: Option<String>,
    pub agent_id: Option<String>,
    pub ca_cert: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub executor_bin: Option<String>,
    pub slots: u32,
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            control_addr: None,
            data_addr: None,
            agent_id: None,
            ca_cert: None,
            cert: None,
            key: None,
            executor_bin: None,
            slots: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JournalConfig {
    /// journal v2 WS RPC 监听地址（原 FLOW_JOURNAL_ADDR）。
    pub addr: String,
    /// 下载/webhook HTTP 监听地址（原 FLOW_JOURNAL_HTTP_ADDR；Bearer token 认证）。
    pub http_addr: String,
    /// journal 数据目录（原 FLOW_JOURNAL_DATA_DIR；journal-server 用）。
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
        dirs::config_dir()
            .map(|dir| dir.join("flow").join(CONFIG_FILE_NAME))
            .filter(|p| p.exists())
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
        if self.execution.mode == ExecutionModeKind::Remote && self.execution.remote.is_none() {
            return Err("execution.mode=remote 时必须配置 [execution.remote]".into());
        }
        if let Some(remote) = &self.execution.remote {
            if remote.control_addr.is_empty() || remote.data_addr.is_empty() {
                return Err("execution.remote.control_addr / data_addr 不能为空".into());
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
        let tmp = path.with_extension("toml.tmp");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConfigError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(&tmp, body).map_err(|source| ConfigError::Io {
            path: tmp.clone(),
            source,
        })?;
        std::fs::rename(&tmp, path).map_err(|source| ConfigError::Io {
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
                other => {
                    return Err(ConfigError::Invalid(format!(
                        "未知配置分区 {other:?}（合法：server/storage/execution/agent/journal）"
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
    pub config: Config,
    /// 实际读取（或将写入）的配置文件路径；None = 未找到文件。
    pub path: Option<PathBuf>,
    /// 生效的环境变量覆盖（仅名称）。
    pub env_overrides: Vec<String>,
}

fn path_display(path: &Option<PathBuf>) -> &str {
    path.as_deref().and_then(Path::to_str).unwrap_or("<config>")
}

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
        // v1 后端（sqlite/postgres）已删除（历史数据不迁移，见
        // docs/SQLITE_V1_TO_V2_MIGRATION.md）。不静默回落：非法值走 Err，
        // 与 toml 未知值同语义。
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
        match v.as_str() {
            "" | "in_process" => config.execution.mode = ExecutionModeKind::InProcess,
            "ipc" => config.execution.mode = ExecutionModeKind::Ipc,
            "remote" => config.execution.mode = ExecutionModeKind::Remote,
            _ => {}
        }
        if !v.is_empty() {
            applied.push("FLOW_EXECUTION_MODE".into());
        }
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
    if let Some(v) = env_str("FLOW_REMOTE_DATA_ADDR") {
        config
            .execution
            .remote
            .get_or_insert_with(Default::default)
            .data_addr = v;
        applied.push("FLOW_REMOTE_DATA_ADDR".into());
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
    if let Some(v) = env_str("FLOW_AGENT_DATA_ADDR") {
        config.agent.data_addr = Some(v);
        applied.push("FLOW_AGENT_DATA_ADDR".into());
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
        config.agent.slots = v as u32;
        applied.push("FLOW_AGENT_SLOTS".into());
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
    /// journal 触发器扫描间隔（秒 → Duration）。
    pub fn journal_trigger_tick(&self) -> Duration {
        Duration::from_secs(self.server.journal_trigger_tick_secs)
    }
}

#[cfg(test)]
mod tests {
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
        // 默认后端 = journal（JSONL v2 权威）
        assert_eq!(config.storage.backend, StorageBackend::Journal);
        assert_eq!(config.storage.data_dir, "data");
        assert_eq!(config.execution.mode, ExecutionModeKind::InProcess);
        assert_eq!(config.execution.x_max, 4);
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
}
