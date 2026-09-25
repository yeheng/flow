//! 所有权协议（DISTRIBUTED.md §5）：行锁、准入检查、租约获取/续期/释放、
//! 原子 run 创建。
//!
//! 约定：
//! - 隔离级别 READ COMMITTED；所有受保护操作先锁同一个 runs 行，锁持有至提交；
//! - 过期检查在取得行锁后用数据库 `clock_timestamp()`，不用客户端时钟，
//!   也不用事务开始时间的 `now()`；
//! - 每次获取都递增 `lease_epoch`，即使目标实例恰好相同；释放/清空不重置 epoch。

use std::time::Duration;

use serde_json::Value;
use sqlx::postgres::PgConnection;
use sqlx::{PgPool, Row};

use crate::error::PgError;

pub const MODE_PEER: &str = "peer";
// 状态词汇表的单一来源在 flow-dto；此处重导出保持旧导入路径可用。
pub use flow_dto::{DbRunStatus, STATUS_ACTIVE, STATUS_DRAFT, STATUS_PUBLISHED};

/// 持锁后读到的 run 行快照。`expired` 用 SQL 表达式在锁内计算。
#[derive(Debug)]
pub(crate) struct RunRow {
    pub status: String,
    pub lease_owner: Option<String>,
    pub lease_epoch: i64,
    pub last_seq: i64,
    pub expired: bool,
}

/// 锁定 runs 行并读取快照。锁持有至事务提交/回滚。
pub(crate) async fn lock_run_row(
    tx: &mut PgConnection,
    run_id: &str,
) -> Result<Option<RunRow>, sqlx::Error> {
    let rec = sqlx::query(
        "SELECT status, lease_owner, lease_epoch, last_seq,
                (lease_expires_at IS NOT NULL AND lease_expires_at <= clock_timestamp()) AS expired
         FROM runs WHERE id = $1 FOR UPDATE",
    )
    .bind(run_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(rec) = rec else { return Ok(None) };
    Ok(Some(RunRow {
        status: rec.try_get("status")?,
        lease_owner: rec.try_get("lease_owner")?,
        lease_epoch: rec.try_get("lease_epoch")?,
        last_seq: rec.try_get("last_seq")?,
        expired: rec.try_get("expired")?,
    }))
}

/// 持锁后的准入点检查（§5.1）。任何不匹配都视为 LeaseLost：
/// 停止本地派发、取消本地任务并丢弃未提交结果。
pub(crate) fn check_writable(
    row: &RunRow,
    instance_id: &str,
    epoch: i64,
    expected_last_seq: Option<u64>,
) -> Result<(), flow_engine::EngineError> {
    use flow_engine::EngineError;
    if !STATUS_ACTIVE.contains(&row.status.as_str()) {
        // run 已终结：写权已随终态消失，按所有权丢失处理（静默退出）
        return Err(EngineError::LeaseLost);
    }
    if row.lease_owner.as_deref() != Some(instance_id) || row.lease_epoch != epoch {
        return Err(EngineError::LeaseLost);
    }
    if row.expired {
        return Err(EngineError::LeaseLost);
    }
    if let Some(expected) = expected_last_seq {
        if row.last_seq < 0 || row.last_seq as u64 != expected {
            // 内存 fold 与已提交日志不一致：拒绝写入，接管流程会以日志为准
            return Err(EngineError::LogCorrupted(format!(
                "last_seq 不一致：内存 fold 为 {expected}，数据库为 {}",
                row.last_seq
            )));
        }
    }
    Ok(())
}

pub(crate) fn status_is_active(status: &str) -> bool {
    STATUS_ACTIVE.contains(&status)
}

#[derive(Debug)]
pub enum AcquireOutcome {
    Acquired { epoch: i64 },
    NotEligible(String),
}

/// 获取租约（§5.2）：锁行 → 检查状态与租约空闲 → 写入新 owner、epoch+1、
/// expires_at=clock_timestamp()+TTL → 提交。事件读取在提交之后进行。
pub async fn acquire(
    pool: &PgPool,
    run_id: &str,
    instance_id: &str,
    ttl: Duration,
) -> Result<AcquireOutcome, PgError> {
    let mut tx = pool.begin().await?;
    // 集群模式入口检查：peer 模式只允许 executor 自行获取（§5.2 步骤 2）
    let mode: Option<String> =
        sqlx::query_scalar("SELECT scheduling_mode FROM cluster_settings WHERE singleton = TRUE")
            .fetch_optional(&mut *tx)
            .await?;
    match mode.as_deref() {
        Some(MODE_PEER) => {}
        Some(other) => {
            return Err(PgError::ModeMismatch(format!(
                "集群模式为 {other}，executor 不得自行获取租约"
            )))
        }
        None => return Err(PgError::ModeMismatch("cluster_settings 缺失".into())),
    }

    let Some(row) = lock_run_row(&mut tx, run_id).await? else {
        return Err(PgError::RunNotFound(run_id.to_string()));
    };
    if !status_is_active(&row.status) {
        return Ok(AcquireOutcome::NotEligible(format!(
            "run 状态为 {}",
            row.status
        )));
    }
    let free = row.lease_owner.is_none() || row.expired;
    if !free {
        return Ok(AcquireOutcome::NotEligible("租约仍被有效持有".into()));
    }
    sqlx::query(
        "UPDATE runs
         SET lease_owner = $1, lease_epoch = lease_epoch + 1,
             lease_expires_at = clock_timestamp() + make_interval(secs => $2)
         WHERE id = $3",
    )
    .bind(instance_id)
    .bind(ttl.as_secs_f64())
    .bind(run_id)
    .execute(&mut *tx)
    .await?;
    let epoch: i64 = sqlx::query_scalar("SELECT lease_epoch FROM runs WHERE id = $1")
        .bind(run_id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(AcquireOutcome::Acquired { epoch })
}

/// 续期：只允许 owner、epoch 都匹配且当前未过期的持有者延长 TTL。
/// 已过期的租约不能靠迟到心跳复活，必须重新获取并递增 epoch。
pub async fn renew(
    pool: &PgPool,
    run_id: &str,
    instance_id: &str,
    epoch: i64,
    ttl: Duration,
) -> Result<(), flow_engine::EngineError> {
    let affected = sqlx::query(
        "UPDATE runs SET lease_expires_at = clock_timestamp() + make_interval(secs => $1)
         WHERE id = $2 AND lease_owner = $3 AND lease_epoch = $4
           AND lease_expires_at > clock_timestamp()
           AND status = ANY($5)",
    )
    .bind(ttl.as_secs_f64())
    .bind(run_id)
    .bind(instance_id)
    .bind(epoch)
    .bind(&STATUS_ACTIVE[..])
    .execute(pool)
    .await
    .map_err(|e| flow_engine::EngineError::Backend(e.to_string()))?
    .rows_affected();
    if affected == 0 {
        return Err(flow_engine::EngineError::LeaseLost);
    }
    Ok(())
}

/// 正常释放（§5.2）：校验 owner/epoch 和有效期后清空。
/// 崩溃或已失效时不再尝试清空新持有者的租约。
pub async fn release(
    pool: &PgPool,
    run_id: &str,
    instance_id: &str,
    epoch: i64,
) -> Result<(), flow_engine::EngineError> {
    let affected = sqlx::query(
        "UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL
         WHERE id = $1 AND lease_owner = $2 AND lease_epoch = $3
           AND lease_expires_at > clock_timestamp()",
    )
    .bind(run_id)
    .bind(instance_id)
    .bind(epoch)
    .execute(pool)
    .await
    .map_err(|e| flow_engine::EngineError::Backend(e.to_string()))?
    .rows_affected();
    if affected == 0 {
        return Err(flow_engine::EngineError::LeaseLost);
    }
    Ok(())
}

/// gateway 的 run.start（§3）：在一个事务内校验 published 版本、插入 run
/// 和 seq=1 的 RunStarted。提交后 run.status=running、lease 为空、last_seq=1，
/// 表示已入队——running 不保证已获得执行容量。
pub async fn create_run(
    pool: &PgPool,
    run_id: &str,
    workflow_id: &str,
    workflow_version: i64,
    input: &Value,
    depth: u32,
) -> Result<(), PgError> {
    let mut tx = pool.begin().await?;
    // 锁 workflow 行：与 delete_workflow（拒绝已有 run 的 workflow）串行化
    let exists: Option<String> =
        sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1 FOR SHARE")
            .bind(workflow_id)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        return Err(PgError::WorkflowNotFound(workflow_id.to_string()));
    }
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM workflow_versions WHERE workflow_id = $1 AND version = $2 FOR SHARE",
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .fetch_optional(&mut *tx)
    .await?;
    match status.as_deref() {
        None => {
            return Err(PgError::VersionNotFound(
                workflow_id.to_string(),
                workflow_version,
            ))
        }
        Some(s) if s != STATUS_PUBLISHED => {
            return Err(PgError::VersionNotPublished(
                workflow_id.to_string(),
                workflow_version,
            ));
        }
        _ => {}
    }

    let started = payload_of(&flow_engine::Event::RunStarted {
        workflow_id: workflow_id.to_string(),
        workflow_version,
        input: input.clone(),
        depth,
    })?;
    sqlx::query(
        "INSERT INTO runs (id, workflow_id, workflow_version, status, input, started_at, last_seq)
         VALUES ($1, $2, $3, $4, $5, clock_timestamp(), 1)",
    )
    .bind(run_id)
    .bind(workflow_id)
    .bind(workflow_version)
    .bind(DbRunStatus::Running.as_str())
    .bind(input)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO run_events (run_id, seq, ts, payload) VALUES ($1, 1, clock_timestamp(), $2)",
    )
    .bind(run_id)
    .bind(started)
    .execute(&mut *tx)
    .await?;
    queue_event_notify(&mut tx, run_id).await?;
    tx.commit().await?;
    Ok(())
}

/// 事件提交后唤醒订阅者（DISTRIBUTED.md §8）：与事件插入同一事务，提交时才投递。
/// 载荷只带 run_id（NOTIFY 载荷上限 8000 字节），事件本体始终由 read_events
/// 按游标读取；通知只是低延迟提示，丢失由订阅方兜底轮询兜住。
pub(crate) async fn queue_event_notify(
    tx: &mut PgConnection,
    run_id: &str,
) -> Result<(), sqlx::Error> {
    // 频道名是编译期常量，无注入面（sqlx 0.9 的 SqlSafeStr 要求）
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT pg_notify('{}', $1)",
        crate::EVENTS_CHANNEL
    )))
    .bind(run_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// Event 的 JSONB 载荷编码。
pub(crate) fn payload_of(event: &flow_engine::Event) -> Result<Value, flow_engine::EngineError> {
    serde_json::to_value(event).map_err(flow_engine::EngineError::Json)
}
