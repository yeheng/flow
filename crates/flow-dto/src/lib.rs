//! 产品视图的 V2 数据传输对象与运行词汇表。
//!
//! 本 crate 的定位是**跨 crate 共享的契约层**：运行状态、触发来源这些
//! 词汇表被 engine、backend、rpc、web 同时消费，如果每个 crate 各写一份
//! `matches!`，很快就会出现「三处对同一状态给出三种判断」的漂移。
//! 因此这里的原则是：
//!
//! - **单一词汇表**：状态与来源都只有一处枚举定义，其余谓词从 `ALL` 派生，
//!   新增一个变体只需要改一处。
//! - **形状与存储解耦**：DTO 只描述「产品面看到什么」，不承担持久化语义；
//!   存储侧的合法值校验在写入口调用本 crate 的 `is_valid_str`。
//! - **命令作用域可推导**：`command_scope` 把一次 RPC 调用映射到可重放的
//!   幂等作用域，使「回复丢失」的写可以被安全重试。
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// workflow_versions.status 的合法取值。
pub const STATUS_DRAFT: &str = "draft";
pub const STATUS_PUBLISHED: &str = "published";

/// runs.status 的合法词汇表（全工作区唯一来源；engine 直接 re-export）。
///
/// 之所以放在 DTO 层而不是 engine：web 端也要判断「这个 run 还能不能接受
/// 信号」，两端的判断必须来自同一份定义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// 正在执行中。
    Running,
    /// 停在「等人工裁决」状态：外部结果未知，等待 `run.adjudicate`。
    AwaitingResume,
    /// 已成功终结。
    Succeeded,
    /// 已失败终结。
    Failed,
    /// 被取消终结。
    Cancelled,
}

impl RunStatus {
    /// **变体的唯一清单。** 数组顺序 = `as_str` 逐项对拍的顺序；
    /// 其余谓词（`is_valid_str` / `is_terminal_str`）由它派生，
    /// 改这里一处即全链路生效。
    pub const ALL: [RunStatus; 5] = [
        RunStatus::Running,
        RunStatus::AwaitingResume,
        RunStatus::Succeeded,
        RunStatus::Failed,
        RunStatus::Cancelled,
    ];

    /// 序列化成存储与 RPC 使用的字符串形式。
    ///
    /// 这里手写 `match` 而不是 `Display`/serde 自动派生：序列化形式是
    /// **持久化与协议契约**，一旦发版就不能改，用显式映射可以让改动
    /// 在 diff 里一眼可见。
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::AwaitingResume => "awaiting_resume",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }

    /// 字符串是否属于词汇表。存储写入口的校验函数用它。
    pub fn is_valid_str(status: &str) -> bool {
        Self::ALL.iter().any(|s| s.as_str() == status)
    }

    /// 字符串是否表示一个已终结的状态。
    ///
    /// 与 `is_valid_str` 分开是因为「非法值」和「合法但未终结」是两种不同
    /// 的错误：前者是数据损坏，后者是正常业务状态。
    pub fn is_terminal_str(status: &str) -> bool {
        Self::ALL
            .iter()
            .any(|s| s.is_terminal() && s.as_str() == status)
    }

    /// 该状态是否已终结（不可再接受信号）。
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        )
    }

    /// 与 `is_terminal` 同义的历史名称，保留现有调用点语义。
    pub fn terminal(self) -> bool {
        self.is_terminal()
    }
}

impl std::fmt::Display for RunStatus {
    /// 与 `as_str` 保持一致：日志与错误信息里出现的状态名必须唯一，
    /// 否则按状态做 grep 排障时会漏。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// runs.source 的合法取值（run 触发来源词汇表，单一来源）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunSource {
    /// run.start 手动触发
    Manual,
    /// cron 调度器触发（source_detail = schedule id）
    Schedule,
    /// webhook HTTP 触发（source_detail = webhook token）
    Webhook,
    /// sub_workflow 节点启动的子 run
    SubWorkflow,
}

impl RunSource {
    /// 变体的唯一清单（与 `RunStatus::ALL` 同理）。
    pub const ALL: [RunSource; 4] = [
        RunSource::Manual,
        RunSource::Schedule,
        RunSource::Webhook,
        RunSource::SubWorkflow,
    ];

    /// 序列化成存储使用的字符串形式。
    pub fn as_str(self) -> &'static str {
        match self {
            RunSource::Manual => "manual",
            RunSource::Schedule => "schedule",
            RunSource::Webhook => "webhook",
            RunSource::SubWorkflow => "sub_workflow",
        }
    }

    /// 字符串是否属于词汇表（存储写入口与 RPC 过滤参数校验共用）。
    pub fn is_valid_str(source: &str) -> bool {
        Self::ALL.iter().any(|s| s.as_str() == source)
    }
}

/// 定义版本。definition 是不可变快照，run 钉死某一版。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowVersion {
    /// 所属工作流。
    pub workflow_id: String,
    /// 版本号，单调递增；run 引用的是这个数字而不是定义内容。
    pub version: i64,
    /// 冻结的定义快照。不可变——修改必须产生新版本。
    pub definition: Value,
    /// 定义内容的摘要，用于校验读取到的版本与写入时一致。
    pub checksum: String,
    /// 版本状态：`draft` 或 `published`。
    pub status: String,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

impl WorkflowVersion {
    /// 该版本是否已发布（可被 run.start 使用）。
    ///
    /// 草稿版本可以被编辑与查看，但不能被运行引用——否则一次运行可能
    /// 引用到一份随后就被改掉的定义。
    pub fn is_published(&self) -> bool {
        self.status == STATUS_PUBLISHED
    }
}

/// 工作流摘要：列表页需要的最小信息，不含定义内容。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSummary {
    /// 工作流 id。
    pub workflow_id: String,
    /// 显示名。
    pub name: String,
    /// 最新版本号（可能是草稿）。
    pub latest_version: i64,
    /// 已发布版本号；尚未发布时为 None。
    pub published_version: Option<i64>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

/// cron 定时调度（schedules 表）。cron_expr 是标准 5 字段（分 时 日 月 周），本地时间。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    /// 调度 id。
    pub id: String,
    /// 被调度的工作流。
    pub workflow_id: String,
    /// 5 字段 cron 表达式，按本地时间解释。
    pub cron_expr: String,
    /// 触发时注入的输入载荷。
    pub input: Option<Value>,
    /// 是否启用；停用的调度不会产生 run。
    pub enabled: bool,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

/// webhook 触发器（webhooks 表，token 即主键，随机生成不可猜）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    /// 触发 token，同时是主键；不可猜测，因为它本身就是凭据。
    pub token: String,
    /// 被触发的工作流。
    pub workflow_id: String,
    /// 是否启用。
    pub enabled: bool,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

/// 一次运行的完整记录（runs 表的投影）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    /// 运行 id。
    pub id: String,
    /// 所属工作流。
    pub workflow_id: String,
    /// 钉死的定义版本号。
    pub workflow_version: i64,
    /// 当前状态。
    pub status: RunStatus,
    /// 输入载荷。
    pub input: Value,
    /// 成功时的输出；未结束时为 None。
    pub output: Option<Value>,
    /// 失败时的错误信息；未失败时为 None。
    pub error: Option<String>,
    /// 触发来源（RunSource 词汇表）；存量数据迁移后为 manual
    pub source: String,
    /// 来源细节：schedule id / webhook token；manual 与 sub_workflow 为 None
    pub source_detail: Option<String>,
    /// 开始时间。
    pub started_at: DateTime<Utc>,
    /// 结束时间；仍在进行时为 None。
    pub ended_at: Option<DateTime<Utc>>,
}

/// run.stats 的返回：按状态分组的精确计数（GROUP BY，不走采样）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStats {
    /// 运行总数。
    pub total: i64,
    /// status → count，只含实际出现的状态
    pub by_status: std::collections::BTreeMap<String, i64>,
    /// 仅在不带 workflow_id 过滤时返回（否则为空数组）
    pub by_workflow: Vec<WorkflowRunStats>,
}

/// 单个工作流维度的运行统计。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRunStats {
    /// 工作流 id。
    pub workflow_id: String,
    /// 该工作流的运行总数。
    pub total: i64,
    /// status → count。
    pub by_status: std::collections::BTreeMap<String, i64>,
}

/// 可复用节点模板（node_templates 表）：画布片段（节点 + 内部边）+ 命名信封。
/// 片段**不是完整 Definition**——不走整图校验（`Definition::validate`），只在
/// 保存/读取时做逐节点参数校验；因此 start/end 节点可含可不含。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTemplate {
    /// 模板 id。
    pub id: String,
    /// 人类可读名，唯一（UI 里的键）。
    pub name: String,
    /// 面板分组标签（可空；空 = 通用）。
    pub category: Option<String>,
    /// 片段节点：[{id, type, name, position, params}]
    pub nodes: Value,
    /// 片段内部边：[{source, target, sourceHandle, targetHandle}]
    pub edges: Value,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

/// template.list 的条目：不含节点/边载荷（payload 走 template.get 按需取）。
///
/// 列表页只需要知道「有哪些模板」，带上完整片段会让响应体积随模板数线性膨胀。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTemplateSummary {
    /// 模板 id。
    pub id: String,
    /// 显示名。
    pub name: String,
    /// 面板分组标签。
    pub category: Option<String>,
    /// 片段内的节点数量。
    pub node_count: i64,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

/// Durable command namespace used to recover a write after a lost reply.
///
/// 把一次写调用映射到一个**可重放的幂等作用域**：当 RPC 回复在途中丢失时，
/// 客户端用同一个 scope 重发，服务端就能认出「这是同一次写」而不是新写一次。
///
/// scope 的构造规则：
/// - `run.start` 与 `run.regress` 共享 `run.start:manual:` 前缀，因为回归
///   本质上就是一次手动启动；regress 额外带上 baseline 以区分不同基线。
/// - `run.cancel` / `run.signal` / `run.adjudicate` 按 `run_id` 定位，
///   同一 run 上的同类操作天然幂等。
/// - 其余方法用方法名本身作为作用域。
pub fn command_scope(method: &str, params: &Value) -> String {
    match method {
        "run.start" => "run.start:manual:".into(),
        "run.regress" => format!(
            "run.start:manual:regression:{}",
            params["baseline"].as_str().unwrap_or("")
        ),
        "run.cancel" | "run.signal" | "run.adjudicate" => {
            format!("{method}:{}", params["run_id"].as_str().unwrap_or(""))
        }
        other => other.into(),
    }
}

/// `[harness.agent]` 的运行期形态：run 启动时冻结进 `ExecutionSnapshot`，
/// 派发时随 `_flow_execution.agent` 下发，并进 operation 请求（回归指纹）。
///
/// 只含非敏感标量——API key 只以密钥名出现在节点参数（`x-secret`），真值经
/// `OperationPermit` → executor 内存 → conga provider 参数，任何环境变量都不携带。
/// 非敏感模型配置通过 typed HeadlessSpec 传入固定版本 conga-host。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentRunConfig {
    /// OpenAI 兼容 / Anthropic 端点；与 model 成对出现（空 = 未启用 agent 模式）。
    pub base_url: String,
    /// 模型名。
    pub model: String,
    /// `openai`（默认，OpenAI 兼容）| `anthropic`。
    pub api: String,
    /// LLM 出口代理（http(s)://host:port；禁 userinfo/query）。
    pub proxy: Option<String>,
    /// 单次运行的最大对话轮数。
    pub max_turns: Option<u64>,
    /// 单轮内最大工具调用次数。
    pub max_tool_calls_per_turn: Option<u64>,
    /// 单次请求的最大生成 token 数。
    pub max_tokens: Option<u64>,
    /// 单次工具调用的超时秒数。
    pub tool_timeout_secs: Option<u64>,
}

impl Default for AgentRunConfig {
    /// 默认值表示「未启用 agent 模式」：端点与模型为空。
    ///
    /// 刻意不填默认值去「猜」一个可用端点——猜错的端点会让每次运行都
    /// 以一个看起来像网络故障的错误失败。
    fn default() -> Self {
        AgentRunConfig {
            base_url: String::new(),
            model: String::new(),
            api: "openai".into(),
            proxy: None,
            max_turns: None,
            max_tool_calls_per_turn: None,
            max_tokens: None,
            tool_timeout_secs: None,
        }
    }
}

impl AgentRunConfig {
    /// base_url 与 model 都非空才算启用；任一为空 = `[harness.agent]` 未配置。
    pub fn enabled(&self) -> bool {
        !self.base_url.trim().is_empty() && !self.model.trim().is_empty()
    }
}

pub mod rpc;
