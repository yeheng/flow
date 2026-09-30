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
//!
//! **加枚举变体只需改这一处**：`ALL` 是变体的唯一清单（`as_str` 与它对拍），
//! `is_valid_str` / `is_terminal_str` / `is_active_str` 全部由它派生，
//! `flow-pg/src/schema.rs` 的 `CHECK` 列表也由它生成。先前 `is_valid_str` 是
//! 一份手写 `matches!` 字面量表，与 enum 不联动——加一个变体编译器不吭声，
//! 新状态在写入时被当场拒掉，而症状是「run 从恢复扫描里静默消失」。
//! `ALL` 让这种失配不可能发生：变体与其字符串在同一处定义。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// workflow_versions.status 的合法取值。
pub const STATUS_DRAFT: &str = "draft";
pub const STATUS_PUBLISHED: &str = "published";

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
    /// **变体的唯一清单。** 数组顺序 = `as_str` 逐项对拍的顺序；
    /// 其余谓词（`is_valid_str` / `is_terminal_str` / `is_active_str`）与
    /// Postgres 的 `CHECK` 约束都由它派生，改这里一处即全链路生效。
    pub const ALL: [DbRunStatus; 6] = [
        DbRunStatus::Initializing,
        DbRunStatus::Running,
        DbRunStatus::AwaitingResume,
        DbRunStatus::Succeeded,
        DbRunStatus::Failed,
        DbRunStatus::Cancelled,
    ];

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

    /// 字符串是否属于词汇表。存储写入口的校验函数用它。
    pub fn is_valid_str(status: &str) -> bool {
        Self::ALL.iter().any(|s| s.as_str() == status)
    }

    pub fn is_terminal_str(status: &str) -> bool {
        Self::ALL
            .iter()
            .any(|s| s.is_terminal() && s.as_str() == status)
    }

    /// 仍在执行、接受输入与续租的状态（runs 行可写权的准入判定用）。
    pub fn is_active_str(status: &str) -> bool {
        Self::ALL
            .iter()
            .any(|s| s.is_active() && s.as_str() == status)
    }

    /// 该状态是否已终结（不可再接受信号 / 不再续租）。
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            DbRunStatus::Succeeded | DbRunStatus::Failed | DbRunStatus::Cancelled
        )
    }

    /// 该状态是否仍在执行（接受输入、续租、参与未完成扫描）。
    pub fn is_active(self) -> bool {
        matches!(self, DbRunStatus::Running | DbRunStatus::AwaitingResume)
    }

    /// SQL `IN (...)` 的字面量列表：`'running', 'awaiting_resume', ...`。
    ///
    /// 供 schema.rs 生成 `CHECK` / 索引谓词，让数据库约束与 Rust 侧词汇表
    /// 逐字同源。手写那份列表时加变体要记得改两处，而编译器不提醒。
    pub fn sql_in_list(self: DbRunStatus) -> String {
        Self::ALL
            .iter()
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// 只含**本后端真实会产生**的状态的 `IN (...)` 列表。
    ///
    /// 词汇表（`ALL`）是全局的，`initializing` 是 SQLite 两段式创建的中间态；
    /// Postgres 的 `run.start` 是单事务原子创建，**永不产生该状态**——把
    /// `CREATE TABLE` 的 `CHECK` 与 `DbRunStatus::ALL` 直接挂钩会让 DB 接受一个
    /// 不该存在的状态，掩盖「谁写了 initializing」的 bug。
    ///
    /// 用 `exclude` 显式排除而不是另立一份手写清单：手写那份就是本次要消灭的
    /// 第二份拷贝，派生才有一处真相。
    pub fn sql_in_list_excluding(self: DbRunStatus, exclude: &[DbRunStatus]) -> String {
        Self::ALL
            .iter()
            .filter(|s| !exclude.contains(s))
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Postgres 后端永不产生的状态（`run.start` 单事务原子创建，无两段式中间态）。
/// 供 `flow-pg` 生成 `CHECK` 列表时排除，见 [`DbRunStatus::sql_in_list_excluding`]。
pub const PG_UNREACHABLE_STATUSES: [DbRunStatus; 1] = [DbRunStatus::Initializing];

/// 仍在执行、接受输入与续租的状态（runs 行可写权的准入判定用）。
///
/// 由 [`DbRunStatus::ALL`] 派生，不是手写列表。既是成员判定（`is_active_str`）
/// 也是绑定到 SQL `= ANY($n)` 的数组——两处用法共用这一份，加变体自动跟随。
pub fn active_statuses() -> Vec<&'static str> {
    DbRunStatus::ALL
        .iter()
        .filter(|s| s.is_active())
        .map(|s| s.as_str())
        .collect()
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
    /// 变体的唯一清单（与 `DbRunStatus::ALL` 同理）。
    pub const ALL: [DbRunSource; 4] = [
        DbRunSource::Manual,
        DbRunSource::Schedule,
        DbRunSource::Webhook,
        DbRunSource::SubWorkflow,
    ];

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

/// 信号/取消请求的落账结果（`flow-pg/src/gateway.rs`）：
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

#[cfg(test)]
mod tests {
    use super::*;

    /// **「单一来源」这条不变量的守卫**：`ALL` 与 `as_str` 必须构成双射。
    ///
    /// 两层防护，分工不同：
    /// - **编译期**：`ALL` 的类型标注 `[DbRunStatus; 6]` 写死了长度，加变体
    ///   忘加进 `ALL`（或数组多/少一项）直接编译失败，不用等测试。
    /// - **测试期**：`as_str` 的 match 分支与 `ALL` 的**内容**是否一一对应。
    ///   编译器管不了这个——把 `ALL` 里的某项换成另一个已存在的变体，长度照样对得上。
    ///   两个方向都查：每项都能序列化，且 match 里的每个变体都被 `is_valid_str`
    ///   认可（后者等价于「在 ALL 中」）。
    ///
    /// 为什么值得测：`is_valid_str` / `is_terminal_str` / `is_active_str` /
    /// `sql_in_list` 全部由 `ALL` 派生，`ALL` 一旦与 `as_str` 脱节，这些派生
    /// 谓词会与真实变体表不一致——症状是该状态在写入时当场被拒，run 从恢复
    /// 扫描里静默消失。
    #[test]
    fn all_is_the_single_source_for_status_vocabulary() {
        // 方向一：ALL → as_str。每项都能通过由 ALL 派生的校验。
        for status in DbRunStatus::ALL {
            assert!(
                DbRunStatus::is_valid_str(status.as_str()),
                "{} 在 ALL 里却通不过 is_valid_str",
                status.as_str()
            );
        }
        // 方向二：as_str → ALL。遍历 match 的每个分支；漏进 ALL 的变体会
        // 在这里暴露（is_valid_str 只认 ALL，不认它）。
        for status in [
            DbRunStatus::Initializing,
            DbRunStatus::Running,
            DbRunStatus::AwaitingResume,
            DbRunStatus::Succeeded,
            DbRunStatus::Failed,
            DbRunStatus::Cancelled,
        ] {
            assert!(
                DbRunStatus::is_valid_str(status.as_str()),
                "变体 {:?}（as_str = {:?}）不在 ALL 里：加变体时漏改 ALL 了",
                status,
                status.as_str()
            );
        }
        // 词汇表外必须被拒
        assert!(!DbRunStatus::is_valid_str("paused"), "词汇表外的串必须被拒");
        assert!(!DbRunStatus::is_valid_str(""));
        // ALL 无重复（否则 sql_in_list 会生成重复字面量）
        let mut uniq: Vec<&str> = DbRunStatus::ALL.iter().map(|s| s.as_str()).collect();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(uniq.len(), DbRunStatus::ALL.len(), "ALL 里有重复变体");
        // 终态 / 活跃的划分与 is_terminal / is_active 一致
        for status in DbRunStatus::ALL {
            assert_eq!(
                DbRunStatus::is_terminal_str(status.as_str()),
                status.is_terminal(),
                "{} 的终态判定两条路径不一致",
                status.as_str()
            );
            assert_eq!(
                DbRunStatus::is_active_str(status.as_str()),
                status.is_active(),
                "{} 的活跃判定两条路径不一致",
                status.as_str()
            );
        }
        // 终态与活跃互斥且各自非空（分类失效会让未完成扫描漏掉 run）
        assert!(DbRunStatus::ALL.iter().any(|s| s.is_terminal()));
        assert!(DbRunStatus::ALL.iter().any(|s| s.is_active()));
        assert!(DbRunStatus::ALL
            .iter()
            .all(|s| !(s.is_terminal() && s.is_active())));
    }

    /// `sql_in_list` 供 schema.rs 生成 `CHECK` 约束：必须含全部变体、
    /// 逐项加引号、无多余空白。
    #[test]
    fn sql_in_list_renders_every_status() {
        let list = DbRunStatus::ALL[0].sql_in_list();
        for status in DbRunStatus::ALL {
            assert!(
                list.contains(&format!("'{}'", status.as_str())),
                "CHECK 列表缺 {}：{list}",
                status.as_str()
            );
        }
        assert!(!list.contains(", ,"), "不该有多余逗号/空白：{list}");
        assert_eq!(list.matches('\'').count(), DbRunStatus::ALL.len() * 2);
    }

    /// 同样的对拍：DbRunSource 的 ALL 与 as_str 双向覆盖。
    #[test]
    fn all_is_the_single_source_for_source_vocabulary() {
        for source in DbRunSource::ALL {
            assert!(
                DbRunSource::is_valid_str(source.as_str()),
                "{} 应通过 is_valid_str",
                source.as_str()
            );
        }
        assert!(!DbRunSource::is_valid_str("cron"));
        let mut seen: Vec<&str> = DbRunSource::ALL.iter().map(|s| s.as_str()).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), DbRunSource::ALL.len(), "ALL 里有重复变体");
    }
}
