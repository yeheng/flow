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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub workflow_id: String,
    pub workflow_version: i64,
    pub status: String,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
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
