//! flow-dto：领域 DTO 与状态词汇表的**单一来源**（零依赖叶子 crate）。
//!
//! 存在理由（消灭三份拷贝）：flow-store 与 flow-pg 持久化的本来就是引擎域的
//! 数据——`WorkflowVersion / WorkflowSummary / RunRecord` 与 run 状态词汇表
//! 在两个存储里字段逐字相同。语言里同一个结构写三遍，`From` 样板换字段名，
//! 是用复制换"互不依赖"的教条。本 crate 是叶子，store / pg / engine / backend
//! 都可以依赖它，谁也不依赖谁。
//!
//! `DbRunStatus` 的字符串形式是 runs.status 列的唯一合法词汇表：
//! 各存储写入口用它校验，不维护第二份私有常量。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// workflow_versions.status 的合法取值。
pub const STATUS_DRAFT: &str = "draft";
pub const STATUS_PUBLISHED: &str = "published";

/// runs.status 中仍在执行、接受输入与续租的取值（可写权准入判定用）。
pub const STATUS_ACTIVE: [&str; 2] = ["running", "awaiting_resume"];

/// runs.status 的合法词汇表（DESIGN.md §8：单一来源，写入口校验）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbRunStatus {
    Initializing,
    Running,
    AwaitingResume,
    Succeeded,
    Failed,
    Cancelled,
}

impl DbRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DbRunStatus::Initializing => "initializing",
            DbRunStatus::Running => "running",
            DbRunStatus::AwaitingResume => "awaiting_resume",
            DbRunStatus::Succeeded => "succeeded",
            DbRunStatus::Failed => "failed",
            DbRunStatus::Cancelled => "cancelled",
        }
    }

    /// 字符串是否属于词汇表。存储写入口的校验函数用它，不复制常量列表。
    pub fn is_valid_str(status: &str) -> bool {
        matches!(
            status,
            "initializing" | "running" | "awaiting_resume" | "succeeded" | "failed" | "cancelled"
        )
    }

    pub fn is_terminal_str(status: &str) -> bool {
        matches!(status, "succeeded" | "failed" | "cancelled")
    }

    /// 仍在执行、接受输入与续租的状态（runs 行可写权的准入判定用）。
    pub fn is_active_str(status: &str) -> bool {
        STATUS_ACTIVE.contains(&status)
    }
}

/// runs.source 的合法取值（run 触发来源词汇表，单一来源）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbRunSource {
    /// run.start 手动触发
    Manual,
    /// cron 调度器触发（source_detail = schedule id）
    Schedule,
    /// webhook HTTP 触发（source_detail = webhook token）
    Webhook,
    /// sub_workflow 节点启动的子 run
    SubWorkflow,
}

impl DbRunSource {
    pub fn as_str(self) -> &'static str {
        match self {
            DbRunSource::Manual => "manual",
            DbRunSource::Schedule => "schedule",
            DbRunSource::Webhook => "webhook",
            DbRunSource::SubWorkflow => "sub_workflow",
        }
    }

    /// 字符串是否属于词汇表（存储写入口与 RPC 过滤参数校验共用）。
    pub fn is_valid_str(source: &str) -> bool {
        matches!(source, "manual" | "schedule" | "webhook" | "sub_workflow")
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
    /// 触发来源（DbRunSource 词汇表）；存量数据迁移后为 manual
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

/// run.start 的创建结果。
#[derive(Debug, Clone)]
pub struct CreatedRun {
    pub run_id: String,
    pub workflow_version: i64,
}

/// 信号/取消请求的落账结果（DISTRIBUTED.md §6.1）：
/// - `delivered=true`：已写入事件并生效；
/// - `status="pending"`：已入队尚未处理（仅 Postgres 持久 inbox），
///   客户端用 run.signal_status 查询；
/// - `status="rejected"`：非法请求被拒，`error` 携带原因。
///
/// `signal_id` 只在真有一个可查询的 id 时出现（Postgres inbox 主键）；
/// SQLite 同步交付没有账可查，回显客户端提供的 id 或不带——不伪造查不到的 id。
#[derive(Debug, Clone)]
pub struct SignalAck {
    pub signal_id: Option<String>,
    pub status: String,
    pub delivered: bool,
    pub event_seq: Option<u64>,
    pub error: Option<Value>,
}
