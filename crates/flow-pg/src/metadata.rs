//! Postgres 元数据存储：workflows / workflow_versions / runs。
//! 语义与单机 flow-store 对齐：版本不可变、checksum 去重复用版本号、
//! 有 run 时拒删 workflow、只有 published 版本可执行。
//! 领域 DTO 的单一来源在 flow-dto，本模块不再维护第二份拷贝。

use std::time::Duration;

use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::PgError;
pub use flow_dto::{RunRecord, WorkflowSummary, WorkflowVersion, STATUS_DRAFT, STATUS_PUBLISHED};

/// 定义与 run 元数据的存储。执行事件在 run_events，租约在 runs 行内。
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> PgStore {
        PgStore { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn create_workflow(&self, name: &str) -> Result<String, PgError> {
        let id = Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO workflows (id, name, created_at) VALUES ($1, $2, clock_timestamp())",
        )
        .bind(&id)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// 保存定义：生成新的 draft 版本。与最新版本完全相同则复用该版本。
    /// 用 workflow 行锁串行化版本分配（对应 SQLite 的 BEGIN IMMEDIATE）。
    pub async fn update_workflow(
        &self,
        workflow_id: &str,
        definition: &Value,
    ) -> Result<i64, PgError> {
        let checksum = definition_checksum(definition)?;
        let mut tx = self.pool.begin().await?;
        let exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1 FOR UPDATE")
                .bind(workflow_id)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_none() {
            return Err(PgError::WorkflowNotFound(workflow_id.to_string()));
        }
        let latest: Option<(i64, String)> = sqlx::query_as(
            "SELECT version, checksum FROM workflow_versions
             WHERE workflow_id = $1 ORDER BY version DESC LIMIT 1",
        )
        .bind(workflow_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((version, latest_checksum)) = latest {
            if latest_checksum == checksum {
                tx.commit().await?;
                return Ok(version);
            }
        }
        let version: i64 = sqlx::query_scalar(
            "INSERT INTO workflow_versions (workflow_id, version, definition, checksum, status, created_at)
             SELECT $1, COALESCE(MAX(version), 0) + 1, $2, $3, $4, clock_timestamp()
             FROM workflow_versions WHERE workflow_id = $1 RETURNING version",
        )
        .bind(workflow_id)
        .bind(definition)
        .bind(&checksum)
        .bind(STATUS_DRAFT)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn publish(&self, workflow_id: &str, version: i64) -> Result<(), PgError> {
        let affected = sqlx::query(
            "UPDATE workflow_versions SET status = $1 WHERE workflow_id = $2 AND version = $3",
        )
        .bind(STATUS_PUBLISHED)
        .bind(workflow_id)
        .bind(version)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(PgError::VersionNotFound(workflow_id.to_string(), version));
        }
        Ok(())
    }

    /// 取指定版本；version 为 None 时取最新版本。
    pub async fn get_version(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, PgError> {
        match version {
            Some(version) => {
                let rec = sqlx::query(
                    "SELECT workflow_id, version, definition, checksum, status, created_at
                     FROM workflow_versions WHERE workflow_id = $1 AND version = $2",
                )
                .bind(workflow_id)
                .bind(version)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| PgError::VersionNotFound(workflow_id.to_string(), version))?;
                version_from_row(rec)
            }
            None => {
                let rec = sqlx::query(
                    "SELECT workflow_id, version, definition, checksum, status, created_at
                     FROM workflow_versions WHERE workflow_id = $1 ORDER BY version DESC LIMIT 1",
                )
                .bind(workflow_id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| PgError::WorkflowNotFound(workflow_id.to_string()))?;
                version_from_row(rec)
            }
        }
    }

    /// 最新已发布版本：run.start 不带 version 时用它。
    pub async fn latest_published(&self, workflow_id: &str) -> Result<Option<i64>, PgError> {
        let version: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM workflow_versions WHERE workflow_id = $1 AND status = $2
             ORDER BY version DESC LIMIT 1",
        )
        .bind(workflow_id)
        .bind(STATUS_PUBLISHED)
        .fetch_optional(&self.pool)
        .await?;
        Ok(version)
    }

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, PgError> {
        let rows = sqlx::query(
            "SELECT w.id, w.name, w.created_at,
                    COALESCE(MAX(v.version), 0) AS latest_version,
                    MAX(CASE WHEN v.status = $1 THEN v.version END) AS published_version
             FROM workflows w LEFT JOIN workflow_versions v ON v.workflow_id = w.id
             GROUP BY w.id, w.name, w.created_at
             ORDER BY w.created_at DESC",
        )
        .bind(STATUS_PUBLISHED)
        .fetch_all(&self.pool)
        .await?;
        let mut list = Vec::with_capacity(rows.len());
        for row in rows {
            list.push(WorkflowSummary {
                workflow_id: row.try_get("id")?,
                name: row.try_get("name")?,
                latest_version: row.try_get("latest_version")?,
                published_version: row.try_get("published_version")?,
                created_at: row.try_get("created_at")?,
            });
        }
        Ok(list)
    }

    /// 删除工作流。已有 run 记录时拒绝，避免事件日志变成无法解释的孤儿。
    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), PgError> {
        let mut tx = self.pool.begin().await?;
        // 行锁与 run.start 的 FOR SHARE 串行化
        let exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1 FOR UPDATE")
                .bind(workflow_id)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_none() {
            return Err(PgError::WorkflowNotFound(workflow_id.to_string()));
        }
        let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_one(&mut *tx)
            .await?;
        if runs > 0 {
            return Err(PgError::Conflict(format!(
                "workflow {workflow_id} 已有 {runs} 条 run 记录，拒绝删除"
            )));
        }
        sqlx::query("DELETE FROM workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, PgError> {
        let rec = sqlx::query(
            "SELECT id, workflow_id, workflow_version, status, input, output, error,
                    started_at, ended_at
             FROM runs WHERE id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| PgError::RunNotFound(run_id.to_string()))?;
        run_from_row(rec)
    }

    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, PgError> {
        let rows = match workflow_id {
            Some(id) => {
                sqlx::query(
                    "SELECT id, workflow_id, workflow_version, status, input, output, error,
                            started_at, ended_at
                     FROM runs WHERE workflow_id = $1 ORDER BY started_at DESC LIMIT $2",
                )
                .bind(id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT id, workflow_id, workflow_version, status, input, output, error,
                            started_at, ended_at
                     FROM runs ORDER BY started_at DESC LIMIT $1",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };
        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(run_from_row(row)?);
        }
        Ok(runs)
    }

    /// executor 扫描（§5.4）：running/awaiting_resume 且无租约或租约过期的 run。
    /// 扫描结果是候选，不是授权。
    pub async fn takeover_candidates(&self, limit: i64) -> Result<Vec<String>, PgError> {
        let rows = sqlx::query(
            "SELECT id FROM runs
             WHERE status IN ('running', 'awaiting_resume')
               AND (lease_owner IS NULL
                    OR lease_expires_at <= clock_timestamp())
             ORDER BY started_at ASC
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| r.try_get::<String, _>(0))
            .collect::<Result<_, _>>()?)
    }

    /// 订阅轮询的候选 run（§8）：活跃 run + 最近 ended_at 窗口内终结的 run。
    /// 短 run 可能在两次轮询之间走完一生，只有按 ended_at 回看最近窗口才不漏其终态；
    /// 窗口过后自动退出视野，订阅游标随之回收——不需要单独的去重集合。
    /// LIMIT 256 是有界性权衡：活跃 run 超过 256 时按 started_at 取最早的，
    /// 最新 run 可能延迟若干轮才进入订阅候选（DISTRIBUTED.md §8）。
    /// 返回 (run_id, 是否已终结)。
    pub async fn watch_candidates(
        &self,
        ended_within: Duration,
    ) -> Result<Vec<(String, bool)>, PgError> {
        let rows = sqlx::query(
            "SELECT id, status FROM runs
             WHERE status IN ('running', 'awaiting_resume')
                OR ended_at >= clock_timestamp() - make_interval(secs => $1)
             ORDER BY started_at ASC
             LIMIT 256",
        )
        .bind(ended_within.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let status: String = row.try_get("status")?;
            let terminal = !flow_dto::DbRunStatus::is_active_str(&status);
            out.push((id, terminal));
        }
        Ok(out)
    }
}

fn version_from_row(rec: sqlx::postgres::PgRow) -> Result<WorkflowVersion, PgError> {
    Ok(WorkflowVersion {
        workflow_id: rec.try_get("workflow_id")?,
        version: rec.try_get("version")?,
        definition: rec.try_get("definition")?,
        checksum: rec.try_get("checksum")?,
        status: rec.try_get("status")?,
        created_at: rec.try_get("created_at")?,
    })
}

fn run_from_row(rec: sqlx::postgres::PgRow) -> Result<RunRecord, PgError> {
    Ok(RunRecord {
        id: rec.try_get("id")?,
        workflow_id: rec.try_get("workflow_id")?,
        workflow_version: rec.try_get("workflow_version")?,
        status: rec.try_get("status")?,
        input: rec.try_get("input")?,
        output: rec.try_get("output")?,
        error: rec.try_get("error")?,
        started_at: rec.try_get("started_at")?,
        ended_at: rec.try_get("ended_at")?,
    })
}

pub fn definition_checksum(definition: &Value) -> Result<String, PgError> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(definition)?);
    Ok(hex::encode(hasher.finalize()))
}
