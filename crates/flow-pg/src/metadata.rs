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
pub use flow_dto::{
    NodeTemplate, NodeTemplateSummary, RunRecord, RunStats, Schedule, Webhook, WorkflowRunStats,
    WorkflowSummary, WorkflowVersion, STATUS_DRAFT, STATUS_PUBLISHED,
};

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

    /// 全部版本（按 version 倒序）；workflow 不存在时报 WorkflowNotFound，与 update/get 语义一致。
    pub async fn list_versions(&self, workflow_id: &str) -> Result<Vec<WorkflowVersion>, PgError> {
        let exists: Option<String> = sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_none() {
            return Err(PgError::WorkflowNotFound(workflow_id.to_string()));
        }
        let rows = sqlx::query(
            "SELECT workflow_id, version, definition, checksum, status, created_at
             FROM workflow_versions WHERE workflow_id = $1 ORDER BY version DESC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(version_from_row).collect()
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

    // ---- schedules / webhooks（触发器，P3；与 sqlite 臂同契约） ----

    /// 创建 cron 调度。cron 合法性校验在 RPC 边缘（-32010），这里只做持久化。
    pub async fn create_schedule(
        &self,
        workflow_id: &str,
        cron_expr: &str,
        input: Option<&Value>,
        enabled: bool,
    ) -> Result<Schedule, PgError> {
        let exists: Option<String> = sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_none() {
            return Err(PgError::WorkflowNotFound(workflow_id.to_string()));
        }
        let id = Uuid::now_v7().to_string();
        let created_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "INSERT INTO schedules (id, workflow_id, cron_expr, input, enabled)
             VALUES ($1, $2, $3, $4, $5) RETURNING created_at",
        )
        .bind(&id)
        .bind(workflow_id)
        .bind(cron_expr)
        .bind(input)
        .bind(enabled)
        .fetch_one(&self.pool)
        .await?;
        Ok(Schedule {
            id,
            workflow_id: workflow_id.to_string(),
            cron_expr: cron_expr.to_string(),
            input: input.cloned(),
            enabled,
            created_at,
        })
    }

    pub async fn list_schedules(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<Schedule>, PgError> {
        let rows = match workflow_id {
            Some(id) => {
                sqlx::query(
                    "SELECT id, workflow_id, cron_expr, input, enabled, created_at
                     FROM schedules WHERE workflow_id = $1 ORDER BY created_at DESC",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT id, workflow_id, cron_expr, input, enabled, created_at
                     FROM schedules ORDER BY created_at DESC",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        rows.into_iter().map(schedule_from_row).collect()
    }

    /// 部分更新：None 字段不动；input 用 Option<Option<Value>> 区分「不改」与「清空」。
    pub async fn update_schedule(
        &self,
        id: &str,
        cron_expr: Option<&str>,
        input: Option<Option<Value>>,
        enabled: Option<bool>,
    ) -> Result<(), PgError> {
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new("UPDATE schedules SET ");
        let mut first = true;
        let mut sep = |qb: &mut sqlx::QueryBuilder<sqlx::Postgres>| {
            if !std::mem::take(&mut first) {
                qb.push(", ");
            }
        };
        if let Some(cron_expr) = cron_expr {
            sep(&mut qb);
            qb.push("cron_expr = ").push_bind(cron_expr);
        }
        if let Some(input) = input {
            sep(&mut qb);
            qb.push("input = ").push_bind(input);
        }
        if let Some(enabled) = enabled {
            sep(&mut qb);
            qb.push("enabled = ").push_bind(enabled);
        }
        if first {
            // 没有要更新的字段：无操作可以，但必须确认 schedule 存在——
            // 静默返回成功等于把「更新了一个不存在的 id」伪装成已更新
            let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM schedules WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
            if exists.is_none() {
                return Err(PgError::ScheduleNotFound(id.to_string()));
            }
            return Ok(());
        }
        let affected = qb
            .push(" WHERE id = ")
            .push_bind(id)
            .build()
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(PgError::ScheduleNotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<(), PgError> {
        let affected = sqlx::query("DELETE FROM schedules WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(PgError::ScheduleNotFound(id.to_string()));
        }
        Ok(())
    }

    /// 触发去重：同一 (schedule_id, fire_at) 只插入成功一次。
    /// 多节点下谁先插入谁触发，天然分布式锁。
    pub async fn try_insert_fire(
        &self,
        schedule_id: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, PgError> {
        let affected = sqlx::query(
            "INSERT INTO schedule_fires (schedule_id, fire_at) VALUES ($1, $2)
             ON CONFLICT (schedule_id, fire_at) DO NOTHING",
        )
        .bind(schedule_id)
        .bind(fire_at)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected == 1)
    }

    /// 撤销一次触发去重（调度器 `create_run` 遇瞬时故障时调用，让下个 tick 重试）。
    /// 去重键含 fire_at，只删自己刚插入的那一行；不存在返回 Ok（幂等）。
    pub async fn delete_fire(
        &self,
        schedule_id: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), PgError> {
        sqlx::query("DELETE FROM schedule_fires WHERE schedule_id = $1 AND fire_at = $2")
            .bind(schedule_id)
            .bind(fire_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn create_webhook(&self, workflow_id: &str) -> Result<Webhook, PgError> {
        let exists: Option<String> = sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_none() {
            return Err(PgError::WorkflowNotFound(workflow_id.to_string()));
        }
        let token = Uuid::now_v7().simple().to_string();
        let created_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "INSERT INTO webhooks (token, workflow_id) VALUES ($1, $2) RETURNING created_at",
        )
        .bind(&token)
        .bind(workflow_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(Webhook {
            token,
            workflow_id: workflow_id.to_string(),
            enabled: true,
            created_at,
        })
    }

    pub async fn list_webhooks(&self, workflow_id: Option<&str>) -> Result<Vec<Webhook>, PgError> {
        let rows = match workflow_id {
            Some(id) => {
                sqlx::query(
                    "SELECT token, workflow_id, enabled, created_at
                     FROM webhooks WHERE workflow_id = $1 ORDER BY created_at DESC",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT token, workflow_id, enabled, created_at
                     FROM webhooks ORDER BY created_at DESC",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        rows.into_iter().map(webhook_from_row).collect()
    }

    pub async fn get_webhook(&self, token: &str) -> Result<Option<Webhook>, PgError> {
        let rec = sqlx::query(
            "SELECT token, workflow_id, enabled, created_at FROM webhooks WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        rec.map(webhook_from_row).transpose()
    }

    pub async fn set_webhook_enabled(&self, token: &str, enabled: bool) -> Result<(), PgError> {
        let affected = sqlx::query("UPDATE webhooks SET enabled = $1 WHERE token = $2")
            .bind(enabled)
            .bind(token)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(PgError::WebhookNotFound(token.to_string()));
        }
        Ok(())
    }

    pub async fn delete_webhook(&self, token: &str) -> Result<(), PgError> {
        let affected = sqlx::query("DELETE FROM webhooks WHERE token = $1")
            .bind(token)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(PgError::WebhookNotFound(token.to_string()));
        }
        Ok(())
    }

    // ---- 可复用节点模板（node_templates 表，与 SQLite 后端同构） ----

    pub async fn create_template(
        &self,
        name: &str,
        category: Option<&str>,
        nodes: &serde_json::Value,
        edges: &serde_json::Value,
    ) -> Result<NodeTemplate, PgError> {
        let id = Uuid::now_v7().to_string();
        let row =
            sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
                "INSERT INTO node_templates (id, name, category, nodes, edges)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING created_at, updated_at",
            )
            .bind(&id)
            .bind(name)
            .bind(category)
            .bind(nodes)
            .bind(edges)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| map_template_conflict(err, name))?;
        Ok(NodeTemplate {
            id,
            name: name.to_string(),
            category: category.map(str::to_string),
            nodes: nodes.clone(),
            edges: edges.clone(),
            created_at: row.0,
            updated_at: row.1,
        })
    }

    pub async fn list_template_summaries(&self) -> Result<Vec<NodeTemplateSummary>, PgError> {
        let rows = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                i64,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            "SELECT id, name, category, jsonb_array_length(nodes) AS node_count,
                    created_at, updated_at
             FROM node_templates ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, name, category, node_count, created_at, updated_at)| NodeTemplateSummary {
                    id,
                    name,
                    category,
                    node_count,
                    created_at,
                    updated_at,
                },
            )
            .collect())
    }

    pub async fn get_template(&self, id: &str) -> Result<NodeTemplate, PgError> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                serde_json::Value,
                serde_json::Value,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            "SELECT id, name, category, nodes, edges, created_at, updated_at
             FROM node_templates WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| PgError::TemplateNotFound(id.to_string()))?;
        Ok(NodeTemplate {
            id: row.0,
            name: row.1,
            category: row.2,
            nodes: row.3,
            edges: row.4,
            created_at: row.5,
            updated_at: row.6,
        })
    }

    pub async fn update_template(
        &self,
        id: &str,
        name: Option<&str>,
        category: Option<Option<&str>>,
        nodes: Option<&serde_json::Value>,
        edges: Option<&serde_json::Value>,
    ) -> Result<NodeTemplate, PgError> {
        let existing = self.get_template(id).await?;
        let name = name.unwrap_or(&existing.name);
        let category = match category {
            Some(Some(c)) => Some(c),
            Some(None) => None,
            None => existing.category.as_deref(),
        };
        let nodes = nodes.unwrap_or(&existing.nodes);
        let edges = edges.unwrap_or(&existing.edges);
        let updated_at = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "UPDATE node_templates
             SET name = $1, category = $2, nodes = $3, edges = $4, updated_at = clock_timestamp()
             WHERE id = $5
             RETURNING updated_at",
        )
        .bind(name)
        .bind(category)
        .bind(nodes)
        .bind(edges)
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| map_template_conflict(err, name))?;
        Ok(NodeTemplate {
            id: id.to_string(),
            name: name.to_string(),
            category: category.map(str::to_string),
            nodes: nodes.clone(),
            edges: edges.clone(),
            created_at: existing.created_at,
            updated_at,
        })
    }

    pub async fn delete_template(&self, id: &str) -> Result<bool, PgError> {
        let affected = sqlx::query("DELETE FROM node_templates WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected > 0)
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, PgError> {
        let rec = sqlx::query(
            "SELECT id, workflow_id, workflow_version, status, input, output, error,
                    source, source_detail, started_at, ended_at
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
        status: Option<&str>,
        source: Option<&str>,
        before_run_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, PgError> {
        // 与 sqlite 臂同契约：status/source 过滤 + before_run_id 游标（更旧的记录）。
        // status/source 词汇表校验在 RPC 边缘（-32010）；这里作为读过滤，未知值自然查空
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "SELECT id, workflow_id, workflow_version, status, input, output, error,
                    source, source_detail, started_at, ended_at
             FROM runs WHERE TRUE",
        );
        if let Some(id) = workflow_id {
            qb.push(" AND workflow_id = ").push_bind(id);
        }
        if let Some(status) = status {
            qb.push(" AND status = ").push_bind(status);
        }
        if let Some(source) = source {
            qb.push(" AND source = ").push_bind(source);
        }
        if let Some(before) = before_run_id {
            qb.push(" AND started_at < (SELECT started_at FROM runs WHERE id = ")
                .push_bind(before)
                .push(")");
        }
        qb.push(" ORDER BY started_at DESC LIMIT ").push_bind(limit);
        let rows = qb.build().fetch_all(&self.pool).await?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(run_from_row(row)?);
        }
        Ok(runs)
    }

    /// run.stats：GROUP BY 精确计数。workflow_id 为 None 时附带按工作流分组。
    pub async fn run_stats(&self, workflow_id: Option<&str>) -> Result<RunStats, PgError> {
        let rows: Vec<(String, i64)> =
            match workflow_id {
                Some(id) => sqlx::query_as(
                    "SELECT status, COUNT(*) AS c FROM runs WHERE workflow_id = $1 GROUP BY status",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?,
                None => {
                    sqlx::query_as("SELECT status, COUNT(*) AS c FROM runs GROUP BY status")
                        .fetch_all(&self.pool)
                        .await?
                }
            };
        let mut stats = RunStats {
            total: 0,
            by_status: std::collections::BTreeMap::new(),
            by_workflow: Vec::new(),
        };
        for (status, count) in rows {
            stats.total += count;
            stats.by_status.insert(status, count);
        }
        if workflow_id.is_none() {
            let rows: Vec<(String, String, i64)> = sqlx::query_as(
                "SELECT workflow_id, status, COUNT(*) AS c FROM runs GROUP BY workflow_id, status",
            )
            .fetch_all(&self.pool)
            .await?;
            let mut grouped: std::collections::BTreeMap<String, WorkflowRunStats> =
                std::collections::BTreeMap::new();
            for (wf, status, count) in rows {
                let entry = grouped.entry(wf.clone()).or_insert(WorkflowRunStats {
                    workflow_id: wf,
                    total: 0,
                    by_status: std::collections::BTreeMap::new(),
                });
                entry.total += count;
                entry.by_status.insert(status, count);
            }
            stats.by_workflow = grouped.into_values().collect();
        }
        Ok(stats)
    }

    /// executor 扫描（§5.4）：running/awaiting_resume 且无租约或租约过期的 run。
    /// 扫描结果是候选，不是授权。
    pub async fn takeover_candidates(&self, limit: i64) -> Result<Vec<String>, PgError> {
        let rows = sqlx::query(
            "SELECT id FROM runs
             WHERE status = ANY($2)
               AND (lease_owner IS NULL
                    OR lease_expires_at <= clock_timestamp())
             ORDER BY started_at ASC
             LIMIT $1",
        )
        .bind(limit)
        .bind(flow_dto::active_statuses())
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
    /// 返回 (run_id, 是否已终结)。
    ///
    /// 两个集合各有自己的 LIMIT 256 配额，互不挤占（backend-perf 的
    /// subscribe_latency 实测钉住的坑）：若共用一个 `ORDER BY started_at` 的
    /// 限额，30s 回看窗口里的老 run（刚批量终结的）会把名额占满，**新 run 一个
    /// 都进不了扫描**——它的事件不被任何游标追平，订阅端要等老 run 退出窗口
    /// （最多 30s）才恢复推送，p95 从亚秒劣化到 ~25s。所以：活跃集合永远优先
    /// （还有活跃 run 时终态补漏不得占其名额），ended 集合单独限额且优先最近
    /// 终结的（最可能漏终态的）。残余风险只剩「同时活跃超过 256 个」：那批最
    /// 新 run 由下一轮扫描接住，且已有游标的 run 由 scan 的「视野 ∪ 游标」
    /// 并集兜住，不受本查询限额影响。
    pub async fn watch_candidates(
        &self,
        ended_within: Duration,
    ) -> Result<Vec<(String, bool)>, PgError> {
        let rows = sqlx::query(
            "(SELECT id, status FROM runs
               WHERE status = ANY($2)
               ORDER BY started_at ASC
               LIMIT 256)
             UNION ALL
             (SELECT id, status FROM runs
               WHERE NOT (status = ANY($2))
                 AND ended_at >= clock_timestamp() - make_interval(secs => $1)
               ORDER BY ended_at DESC
               LIMIT 256)",
        )
        .bind(ended_within.as_secs_f64())
        .bind(flow_dto::active_statuses())
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

fn schedule_from_row(rec: sqlx::postgres::PgRow) -> Result<Schedule, PgError> {
    Ok(Schedule {
        id: rec.try_get("id")?,
        workflow_id: rec.try_get("workflow_id")?,
        cron_expr: rec.try_get("cron_expr")?,
        input: rec.try_get("input")?,
        enabled: rec.try_get("enabled")?,
        created_at: rec.try_get("created_at")?,
    })
}

fn webhook_from_row(rec: sqlx::postgres::PgRow) -> Result<Webhook, PgError> {
    Ok(Webhook {
        token: rec.try_get("token")?,
        workflow_id: rec.try_get("workflow_id")?,
        enabled: rec.try_get("enabled")?,
        created_at: rec.try_get("created_at")?,
    })
}

/// UNIQUE 冲突（模板重名）映射为专用错误，其余原样。
fn map_template_conflict(err: sqlx::Error, name: &str) -> PgError {
    if err
        .as_database_error()
        .is_some_and(|e| e.is_unique_violation())
    {
        PgError::TemplateNameTaken(name.to_string())
    } else {
        err.into()
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
        source: rec.try_get("source")?,
        source_detail: rec.try_get("source_detail")?,
        started_at: rec.try_get("started_at")?,
        ended_at: rec.try_get("ended_at")?,
    })
}

pub fn definition_checksum(definition: &Value) -> Result<String, PgError> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(definition)?);
    Ok(hex::encode(hasher.finalize()))
}
