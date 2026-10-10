//! V2 product view DTOs and run vocabulary.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// workflow_versions.status 的合法取值。
pub const STATUS_DRAFT: &str = "draft";
pub const STATUS_PUBLISHED: &str = "published";

/// runs.status 的合法词汇表（DESIGN.md §8：单一来源，写入口校验）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    AwaitingResume,
    Succeeded,
    Failed,
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
    pub workflow_id: String,
    pub version: i64,
    pub definition: Value,
    pub checksum: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

impl WorkflowVersion {
    pub fn is_published(&self) -> bool {
        self.status == STATUS_PUBLISHED
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSummary {
    pub workflow_id: String,
    pub name: String,
    pub latest_version: i64,
    pub published_version: Option<i64>,
    pub created_at: DateTime<Utc>,
}

/// cron 定时调度（schedules 表）。cron_expr 是标准 5 字段（分 时 日 月 周），本地时间。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub workflow_id: String,
    pub cron_expr: String,
    pub input: Option<Value>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

/// webhook 触发器（webhooks 表，token 即主键，随机生成不可猜）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub token: String,
    pub workflow_id: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub workflow_id: String,
    pub workflow_version: i64,
    pub status: String,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<String>,
    /// 触发来源（RunSource 词汇表）；存量数据迁移后为 manual
    pub source: String,
    /// 来源细节：schedule id / webhook token；manual 与 sub_workflow 为 None
    pub source_detail: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

/// run.stats 的返回：按状态分组的精确计数（GROUP BY，不走采样）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStats {
    pub total: i64,
    /// status → count，只含实际出现的状态
    pub by_status: std::collections::BTreeMap<String, i64>,
    /// 仅在不带 workflow_id 过滤时返回（否则为空数组）
    pub by_workflow: Vec<WorkflowRunStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRunStats {
    pub workflow_id: String,
    pub total: i64,
    pub by_status: std::collections::BTreeMap<String, i64>,
}

/// 可复用节点模板（node_templates 表）：画布片段（节点 + 内部边）+ 命名信封。
/// 片段**不是完整 Definition**——不走整图校验（`Definition::validate`），只在
/// 保存/读取时做逐节点参数校验；因此 start/end 节点可含可不含。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTemplate {
    pub id: String,
    /// 人类可读名，唯一（UI 里的键）。
    pub name: String,
    /// 面板分组标签（可空；空 = 通用）。
    pub category: Option<String>,
    /// 片段节点：[{id, type, name, position, params}]
    pub nodes: Value,
    /// 片段内部边：[{source, target, sourceHandle, targetHandle}]
    pub edges: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// template.list 的条目：不含节点/边载荷（payload 走 template.get 按需取）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTemplateSummary {
    pub id: String,
    pub name: String,
    pub category: Option<String>,
    pub node_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
