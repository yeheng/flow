//! Postgres schema。幂等建表；与 DESIGN.md 的 SQLite 是两个独立后端，
//! 不是迁移关系（DISTRIBUTED.md §4）。
//!
//! 关键不变量：
//! - `runs.last_seq` 与 run_events 尾部一致，seq 唯一 + 连续由持锁事务共同保证；
//! - `lease_pair` 约束：lease_owner 与 lease_expires_at 必须同时为空或同时非空；
//! - `run_signals` 是持久 inbox：signal/cancel 入队、applied/rejected 落账；
//! - 状态词汇表不含 initializing——Postgres 后端的 run.start 是单事务原子创建，
//!   不存在「已插 run 但没有首事件」的合法状态。

use sqlx::PgPool;

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

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL REFERENCES workflows(id),
            workflow_version BIGINT NOT NULL,
            status TEXT NOT NULL
                CHECK (status IN ('running', 'awaiting_resume', 'succeeded', 'failed', 'cancelled')),
            input JSONB NOT NULL,
            output JSONB,
            error TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            ended_at TIMESTAMPTZ,
            lease_owner TEXT,
            lease_epoch BIGINT NOT NULL DEFAULT 0,
            lease_expires_at TIMESTAMPTZ,
            last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
            CONSTRAINT lease_pair CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
            FOREIGN KEY (workflow_id, workflow_version)
                REFERENCES workflow_versions(workflow_id, version)
        )",
    )
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

    // 集群模式共享表：一个集群只启用一种模式（DISTRIBUTED.md §1）。
    // scheduler 模式由 SCHEDULER.md 定义，本版只落 peer。
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

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_runs_active_lease ON runs (lease_owner, lease_expires_at)
         WHERE status IN ('running', 'awaiting_resume')",
    )
    .execute(pool)
    .await?;

    Ok(())
}
