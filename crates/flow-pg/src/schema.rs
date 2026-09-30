//! Postgres schema。幂等建表；与 DESIGN.md 的 SQLite 是两个独立后端，
//! 不是迁移关系。
//!
//! 关键不变量：
//! - `runs.last_seq` 与 run_events 尾部一致，seq 唯一 + 连续由持锁事务共同保证；
//! - `lease_pair` 约束：lease_owner 与 lease_expires_at 必须同时为空或同时非空；
//! - `run_signals` 是持久 inbox：signal/cancel 入队、applied/rejected 落账；
//! - 状态词汇表不含 initializing——Postgres 后端的 run.start 是单事务原子创建，
//!   不存在「已插 run 但没有首事件」的合法状态。

use flow_dto::{DbRunStatus, PG_UNREACHABLE_STATUSES};
use sqlx::AssertSqlSafe;
use sqlx::PgPool;

/// `runs.status` 的 `CHECK` 列表，由 `DbRunStatus::ALL` 派生并排除本后端
/// 永不产生的状态。
///
/// **不手写**：手写列表与 flow-dto 的 enum 是两份必须同步的拷贝，加变体时
/// 编译器不提醒，症状是该状态在写入时当场被 DB 拒掉。`DbRunStatus::ALL[0]`
/// 只是取一个实例来调方法（该方法不依赖具体变体）。
///
/// 排除 `initializing` 很重要：PG 的 `run.start` 单事务原子创建，该状态不可达；
/// 直接用 `ALL` 会让 DB 接受一个不该存在的状态，掩盖「谁写了 initializing」。
fn run_status_check() -> String {
    DbRunStatus::ALL[0].sql_in_list_excluding(&PG_UNREACHABLE_STATUSES)
}

/// `runs.status IN (...)` 的谓词片段（部分索引用），同样由 `ALL` 生成。
fn run_status_active_in() -> String {
    DbRunStatus::ALL
        .iter()
        .filter(|s| s.is_active())
        .map(|s| format!("'{}'", s.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// **关于下面两处 `AssertSqlSafe(format!(...))`**：DDL（`CREATE TABLE` /
/// `CREATE INDEX`）不接受绑定参数，状态列表必须格式化进 SQL 文本。片段内容
/// 全部来自 `DbRunStatus::ALL` 的 `as_str()`——一组编译期字面量，**不含任何
/// 外部输入**，故无注入面。这处 `AssertSqlSafe` 是对上述事实的显式背书，不是
/// 绕过检查：若将来 `sql_in_list` 改去拼接用户输入，它会立刻变成真实注入点。
pub async fn init(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS workflows (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS workflow_versions (
            workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
            version BIGINT NOT NULL,
            definition JSONB NOT NULL,
            checksum TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (workflow_id, version)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL REFERENCES workflows(id),
            workflow_version BIGINT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ({})),
            input JSONB NOT NULL,
            output JSONB,
            error TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            ended_at TIMESTAMPTZ,
            source TEXT NOT NULL DEFAULT 'manual',
            source_detail TEXT,
            lease_owner TEXT,
            lease_epoch BIGINT NOT NULL DEFAULT 0,
            lease_expires_at TIMESTAMPTZ,
            last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
            CONSTRAINT lease_pair CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
            FOREIGN KEY (workflow_id, workflow_version)
                REFERENCES workflow_versions(workflow_id, version)
        )",
        run_status_check()
    )))
    .execute(pool)
    .await?;

    // 存量库迁移：runs 加触发来源列（ADD COLUMN IF NOT EXISTS，幂等）。
    // 旧行由 DEFAULT 'manual' 归因——迁移前没有自动触发入口，语义正确。
    sqlx::query("ALTER TABLE runs ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE runs ADD COLUMN IF NOT EXISTS source_detail TEXT")
        .execute(pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS run_events (
            run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            seq BIGINT NOT NULL CHECK (seq > 0),
            ts TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL,
            PRIMARY KEY (run_id, seq)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS run_signals (
            run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            signal_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('signal', 'cancel')),
            node_id TEXT,
            payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'applied', 'rejected')),
            event_seq BIGINT,
            error JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (run_id, signal_id),
            CHECK ((kind = 'signal' AND node_id IS NOT NULL)
                OR (kind = 'cancel' AND node_id IS NULL))
        )",
    )
    .execute(pool)
    .await?;

    // 集群模式共享表：一个集群只启用一种模式。本版只落 peer 模式
    // （中心指派模式未实现，见 DESIGN.md §14 未做清单）。
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS cluster_settings (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            scheduling_mode TEXT NOT NULL CHECK (scheduling_mode IN ('peer', 'scheduled'))
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO cluster_settings (singleton, scheduling_mode) VALUES (TRUE, 'peer')
         ON CONFLICT (singleton) DO NOTHING",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS pending_run_signals ON run_signals (run_id, created_at, signal_id)
         WHERE status = 'pending'",
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, started_at)")
        .execute(pool)
        .await?;

    // 活跃租约的部分索引：谓词同样由 DbRunStatus::ALL 派生
    sqlx::query(AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS idx_runs_active_lease ON runs (lease_owner, lease_expires_at)
         WHERE status IN ({})",
        run_status_active_in()
    )))
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schedules (
            id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
            cron_expr TEXT NOT NULL,
            input JSONB,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )",
    )
    .execute(pool)
    .await?;

    // 去重表：同一 (schedule_id, fire_at) 只允许插入一次——多节点下谁先插入谁触发
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schedule_fires (
            schedule_id TEXT NOT NULL,
            fire_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (schedule_id, fire_at)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS webhooks (
            token TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )",
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_dto::active_statuses;

    /// CHECK 列表必须与 flow-dto 的词汇表逐字一致，且**排除 initializing**
    /// （Postgres 的 run.start 是单事务原子创建，不存在 initializing 状态）。
    ///
    /// 列表由 `ALL` 生成，但「生成结果对不对」仍需断言：把 `initializing`
    /// 误加进 `ALL` 会让 PG 接受一个该后端永远不产生的状态。
    #[test]
    fn generated_run_status_check_excludes_initializing() {
        let list = run_status_check();
        for status in DbRunStatus::ALL {
            if PG_UNREACHABLE_STATUSES.contains(&status) {
                continue;
            }
            assert!(
                list.contains(&format!("'{}'", status.as_str())),
                "CHECK 列表缺 {}：{list}",
                status.as_str()
            );
        }
        for excluded in PG_UNREACHABLE_STATUSES {
            assert!(
                !list.contains(&format!("'{}'", excluded.as_str())),
                "{} 必须被排除：PG 后端的 run.start 单事务原子创建，\
                 不存在「已插 run 但没有首事件」的中间态：{list}",
                excluded.as_str()
            );
        }
    }

    /// 生成的 CHECK 列表必须与存量库里已建表的那份逐字一致——派生逻辑一旦改变
    /// 列表内容，存量库与新建库就会分叉（存量库的 CHECK 不会因代码变化而更新）。
    #[test]
    fn generated_check_list_matches_the_previous_handwritten_one() {
        assert_eq!(
            run_status_check(),
            "'running', 'awaiting_resume', 'succeeded', 'failed', 'cancelled'"
        );
    }

    /// 部分索引谓词 = 活跃状态，与 `active_statuses()` 同一份来源。
    #[test]
    fn generated_active_index_predicate_matches_active_statuses() {
        let list = run_status_active_in();
        let expected: Vec<String> = active_statuses().iter().map(|s| format!("'{s}'")).collect();
        assert_eq!(list, expected.join(", "));
        assert!(list.contains("'running'") && list.contains("'awaiting_resume'"));
        for status in DbRunStatus::ALL {
            if !status.is_active() {
                assert!(
                    !list.contains(&format!("'{}'", status.as_str())),
                    "{} 非活跃，不该进活跃租约索引：{list}",
                    status.as_str()
                );
            }
        }
    }
}
