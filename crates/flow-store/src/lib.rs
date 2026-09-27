use std::path::Path;

use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use chrono::{DateTime, Utc};

// 领域 DTO 与状态词汇表的单一来源在 flow-dto；本 crate 不再维护第二份拷贝。
// 写入口经 ensure_run_status 用 DbRunStatus 校验，不复制常量列表。
pub use flow_dto::{
    DbRunStatus, RunRecord, Schedule, Webhook, WorkflowSummary, WorkflowVersion, STATUS_DRAFT,
    STATUS_PUBLISHED,
};

fn ensure_run_status(status: &str) -> Result<(), StoreError> {
    if DbRunStatus::is_valid_str(status) {
        Ok(())
    } else {
        Err(StoreError::InvalidStatus(status.to_string()))
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("数据库错误：{0}")]
    Sql(#[from] sqlx::Error),
    #[error("io 错误：{0}")]
    Io(#[from] std::io::Error),
    #[error("工作流不存在：{0}")]
    WorkflowNotFound(String),
    #[error("版本不存在：{0} v{1}")]
    VersionNotFound(String, i64),
    #[error("版本尚未发布：{0} v{1}")]
    VersionNotPublished(String, i64),
    #[error("run 不存在：{0}")]
    RunNotFound(String),
    #[error("schedule 不存在：{0}")]
    ScheduleNotFound(String),
    #[error("webhook 不存在：{0}")]
    WebhookNotFound(String),
    #[error("冲突：{0}")]
    Conflict(String),
    #[error("非法的 run 状态：{0}")]
    InvalidStatus(String),
    #[error("时间戳无法解析：{0}")]
    InvalidTimestamp(String),
    #[error("json 错误：{0}")]
    Json(#[from] serde_json::Error),
}

/// 定义与 run 元数据的存储。执行状态不在这里——那是 event.jsonl 的事。
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    pub async fn open(path: impl AsRef<Path>) -> Result<Store, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        let store = Store { pool };
        store.init().await?;
        Ok(store)
    }

    async fn init(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS workflow_versions (
                workflow_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                definition TEXT NOT NULL,
                checksum TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (workflow_id, version),
                FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER NOT NULL,
                status TEXT NOT NULL,
                input TEXT NOT NULL,
                output TEXT,
                error TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_workflow ON runs(workflow_id, started_at DESC)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                cron_expr TEXT NOT NULL,
                input TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
            )",
        )
        .execute(&self.pool)
        .await?;

        // 去重表：同一 (schedule_id, fire_at) 只允许插入一次——多节点下谁先插入谁触发
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schedule_fires (
                schedule_id TEXT NOT NULL,
                fire_at TEXT NOT NULL,
                PRIMARY KEY (schedule_id, fire_at)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS webhooks (
                token TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_workflow(&self, name: &str) -> Result<String, StoreError> {
        let id = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO workflows (id, name, created_at) VALUES (?, ?, ?)")
            .bind(&id)
            .bind(name)
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(id)
    }

    /// 保存定义：生成新的 draft 版本。与最新版本完全相同则复用该版本，避免编辑器重复保存刷版本号。
    pub async fn update_workflow(
        &self,
        workflow_id: &str,
        definition: &Value,
    ) -> Result<i64, StoreError> {
        let checksum = definition_checksum(definition)?;
        let definition_json = serde_json::to_string(definition)?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let exists = sqlx::query("SELECT 1 FROM workflows WHERE id = ?")
            .bind(workflow_id)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_none() {
            return Err(StoreError::WorkflowNotFound(workflow_id.to_string()));
        }

        let latest = sqlx::query(
            "SELECT version, checksum FROM workflow_versions WHERE workflow_id = ? ORDER BY version DESC LIMIT 1",
        )
        .bind(workflow_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(latest) = latest {
            if latest.try_get::<String, _>("checksum")? == checksum {
                let version = latest.try_get("version")?;
                tx.commit().await?;
                return Ok(version);
            }
        }

        let version = sqlx::query_scalar(
            "INSERT INTO workflow_versions (workflow_id, version, definition, checksum, status, created_at)
             SELECT ?, COALESCE(MAX(version), 0) + 1, ?, ?, ?, ?
             FROM workflow_versions WHERE workflow_id = ? RETURNING version",
        )
        .bind(workflow_id)
        .bind(&definition_json)
        .bind(&checksum)
        .bind(STATUS_DRAFT)
        .bind(Utc::now().to_rfc3339())
        .bind(workflow_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn publish(&self, workflow_id: &str, version: i64) -> Result<(), StoreError> {
        let affected = sqlx::query(
            "UPDATE workflow_versions SET status = ? WHERE workflow_id = ? AND version = ?",
        )
        .bind(STATUS_PUBLISHED)
        .bind(workflow_id)
        .bind(version)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(StoreError::VersionNotFound(
                workflow_id.to_string(),
                version,
            ));
        }
        Ok(())
    }

    /// 取指定版本；version 为 None 时取最新版本。
    pub async fn get_version(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, StoreError> {
        match version {
            Some(version) => {
                let row = sqlx::query(
                    "SELECT workflow_id, version, definition, checksum, status, created_at FROM workflow_versions WHERE workflow_id = ? AND version = ?",
                )
                .bind(workflow_id)
                .bind(version)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::VersionNotFound(workflow_id.to_string(), version))?;
                Self::version_from_row(row)
            }
            None => self
                .latest_version(workflow_id)
                .await?
                .ok_or_else(|| StoreError::WorkflowNotFound(workflow_id.to_string())),
        }
    }

    pub async fn latest_version(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowVersion>, StoreError> {
        let row = sqlx::query(
            "SELECT workflow_id, version, definition, checksum, status, created_at FROM workflow_versions WHERE workflow_id = ? ORDER BY version DESC LIMIT 1",
        )
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Self::version_from_row).transpose()
    }

    /// 全部版本（按 version 倒序）；workflow 不存在时报 WorkflowNotFound，与 update/get 语义一致。
    pub async fn list_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowVersion>, StoreError> {
        let exists = sqlx::query("SELECT 1 FROM workflows WHERE id = ?")
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_none() {
            return Err(StoreError::WorkflowNotFound(workflow_id.to_string()));
        }
        let rows = sqlx::query(
            "SELECT workflow_id, version, definition, checksum, status, created_at FROM workflow_versions WHERE workflow_id = ? ORDER BY version DESC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Self::version_from_row).collect()
    }

    /// 最新已发布版本：run.start 不带 version 时用它。
    pub async fn latest_published(&self, workflow_id: &str) -> Result<Option<i64>, StoreError> {
        let row = sqlx::query(
            "SELECT version FROM workflow_versions WHERE workflow_id = ? AND status = ?
             ORDER BY version DESC LIMIT 1",
        )
        .bind(workflow_id)
        .bind(STATUS_PUBLISHED)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get::<i64, _>("version")))
    }

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, StoreError> {
        let rows = sqlx::query(
            "SELECT w.id, w.name, w.created_at,
                    COALESCE(MAX(v.version), 0) AS latest_version,
                    MAX(CASE WHEN v.status = ? THEN v.version END) AS published_version
             FROM workflows w LEFT JOIN workflow_versions v ON v.workflow_id = w.id
             GROUP BY w.id, w.name, w.created_at
             ORDER BY w.created_at DESC",
        )
        .bind(STATUS_PUBLISHED)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(WorkflowSummary {
                    workflow_id: row.try_get("id")?,
                    name: row.try_get("name")?,
                    latest_version: row.try_get("latest_version")?,
                    published_version: row.try_get("published_version")?,
                    created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
                })
            })
            .collect()
    }

    /// 删除工作流。已有 run 记录时拒绝，避免事件日志变成无法解释的孤儿。
    /// 计数与删除同处一个 BEGIN IMMEDIATE 事务（写锁），与 insert_run 的版本
    /// 存在性守卫互斥——否则 run.start 能插进 COUNT 与 DELETE 之间，造出引用
    /// 已删版本的孤儿 run（PG 臂用 FOR UPDATE / FOR SHARE 达成同一互斥）。
    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let runs: i64 = sqlx::query("SELECT COUNT(*) AS c FROM runs WHERE workflow_id = ?")
            .bind(workflow_id)
            .fetch_one(&mut *tx)
            .await?
            .try_get("c")?;
        if runs > 0 {
            return Err(StoreError::Conflict(format!(
                "workflow {workflow_id} 已有 {runs} 条 run 记录，拒绝删除"
            )));
        }
        let affected = sqlx::query("DELETE FROM workflows WHERE id = ?")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(StoreError::WorkflowNotFound(workflow_id.to_string()));
        }
        tx.commit().await?;
        Ok(())
    }

    // ---- schedules / webhooks（触发器，P3） ----

    /// 创建 cron 调度。cron 合法性校验在 RPC 边缘（-32010），这里只做持久化。
    pub async fn create_schedule(
        &self,
        workflow_id: &str,
        cron_expr: &str,
        input: Option<&Value>,
        enabled: bool,
    ) -> Result<Schedule, StoreError> {
        let exists = sqlx::query("SELECT 1 FROM workflows WHERE id = ?")
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_none() {
            return Err(StoreError::WorkflowNotFound(workflow_id.to_string()));
        }
        let id = Uuid::now_v7().to_string();
        let created_at = Utc::now();
        sqlx::query(
            "INSERT INTO schedules (id, workflow_id, cron_expr, input, enabled, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(workflow_id)
        .bind(cron_expr)
        .bind(input.map(serde_json::to_string).transpose()?)
        .bind(enabled)
        .bind(created_at.to_rfc3339())
        .execute(&self.pool)
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
    ) -> Result<Vec<Schedule>, StoreError> {
        let rows = match workflow_id {
            Some(id) => {
                sqlx::query(
                    "SELECT * FROM schedules WHERE workflow_id = ? ORDER BY created_at DESC",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query("SELECT * FROM schedules ORDER BY created_at DESC")
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.into_iter().map(Self::schedule_from_row).collect()
    }

    /// 部分更新：None 字段不动；input 用 Option<Option<Value>> 区分「不改」与「清空」。
    pub async fn update_schedule(
        &self,
        id: &str,
        cron_expr: Option<&str>,
        input: Option<Option<Value>>,
        enabled: Option<bool>,
    ) -> Result<(), StoreError> {
        let mut qb: sqlx::QueryBuilder<sqlx::Sqlite> =
            sqlx::QueryBuilder::new("UPDATE schedules SET ");
        let mut first = true;
        let mut sep = |qb: &mut sqlx::QueryBuilder<sqlx::Sqlite>| {
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
            qb.push("input = ")
                .push_bind(input.map(|v| serde_json::to_string(&v)).transpose()?);
        }
        if let Some(enabled) = enabled {
            sep(&mut qb);
            qb.push("enabled = ").push_bind(enabled);
        }
        if first {
            return Ok(()); // 没有要更新的字段
        }
        let affected = qb
            .push(" WHERE id = ")
            .push_bind(id)
            .build()
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(StoreError::ScheduleNotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<(), StoreError> {
        let affected = sqlx::query("DELETE FROM schedules WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(StoreError::ScheduleNotFound(id.to_string()));
        }
        Ok(())
    }

    /// 触发去重：同一 (schedule_id, fire_at) 只插入成功一次。
    /// 多节点（pg）下谁先插入谁触发，天然分布式锁。
    pub async fn try_insert_fire(
        &self,
        schedule_id: &str,
        fire_at: &str,
    ) -> Result<bool, StoreError> {
        let affected = sqlx::query(
            "INSERT OR IGNORE INTO schedule_fires (schedule_id, fire_at) VALUES (?, ?)",
        )
        .bind(schedule_id)
        .bind(fire_at)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected == 1)
    }

    pub async fn create_webhook(&self, workflow_id: &str) -> Result<Webhook, StoreError> {
        let exists = sqlx::query("SELECT 1 FROM workflows WHERE id = ?")
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_none() {
            return Err(StoreError::WorkflowNotFound(workflow_id.to_string()));
        }
        let token = Uuid::now_v7().simple().to_string();
        let created_at = Utc::now();
        sqlx::query(
            "INSERT INTO webhooks (token, workflow_id, enabled, created_at) VALUES (?, ?, 1, ?)",
        )
        .bind(&token)
        .bind(workflow_id)
        .bind(created_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(Webhook {
            token,
            workflow_id: workflow_id.to_string(),
            enabled: true,
            created_at,
        })
    }

    pub async fn list_webhooks(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<Webhook>, StoreError> {
        let rows = match workflow_id {
            Some(id) => {
                sqlx::query("SELECT * FROM webhooks WHERE workflow_id = ? ORDER BY created_at DESC")
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("SELECT * FROM webhooks ORDER BY created_at DESC")
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.into_iter().map(Self::webhook_from_row).collect()
    }

    pub async fn get_webhook(&self, token: &str) -> Result<Option<Webhook>, StoreError> {
        let row = sqlx::query("SELECT * FROM webhooks WHERE token = ?")
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        row.map(Self::webhook_from_row).transpose()
    }

    pub async fn set_webhook_enabled(&self, token: &str, enabled: bool) -> Result<(), StoreError> {
        let affected = sqlx::query("UPDATE webhooks SET enabled = ? WHERE token = ?")
            .bind(enabled)
            .bind(token)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(StoreError::WebhookNotFound(token.to_string()));
        }
        Ok(())
    }

    pub async fn delete_webhook(&self, token: &str) -> Result<(), StoreError> {
        let affected = sqlx::query("DELETE FROM webhooks WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(StoreError::WebhookNotFound(token.to_string()));
        }
        Ok(())
    }

    fn schedule_from_row(row: sqlx::sqlite::SqliteRow) -> Result<Schedule, StoreError> {
        let input: Option<String> = row.try_get("input")?;
        Ok(Schedule {
            id: row.try_get("id")?,
            workflow_id: row.try_get("workflow_id")?,
            cron_expr: row.try_get("cron_expr")?,
            input: input.map(|s| serde_json::from_str(&s)).transpose()?,
            enabled: row.try_get::<i64, _>("enabled")? != 0,
            created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
        })
    }

    fn webhook_from_row(row: sqlx::sqlite::SqliteRow) -> Result<Webhook, StoreError> {
        Ok(Webhook {
            token: row.try_get("token")?,
            workflow_id: row.try_get("workflow_id")?,
            enabled: row.try_get::<i64, _>("enabled")? != 0,
            created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
        })
    }

    fn version_from_row(row: sqlx::sqlite::SqliteRow) -> Result<WorkflowVersion, StoreError> {
        let definition: String = row.try_get("definition")?;
        Ok(WorkflowVersion {
            workflow_id: row.try_get("workflow_id")?,
            version: row.try_get("version")?,
            definition: serde_json::from_str(&definition)?,
            checksum: row.try_get("checksum")?,
            status: row.try_get("status")?,
            created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
        })
    }

    /// 创建 run 行。BEGIN IMMEDIATE 写锁内先验证版本仍存在：与
    /// delete_workflow 的事务互斥，杜绝「版本已删、run 行照样插入」的竞态
    ///（runs 表无外键，数据库不会替我们拦）。
    pub async fn insert_run(
        &self,
        run_id: &str,
        workflow_id: &str,
        workflow_version: i64,
        input: &Value,
        status: &str,
    ) -> Result<(), StoreError> {
        ensure_run_status(status)?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let version_exists =
            sqlx::query("SELECT 1 FROM workflow_versions WHERE workflow_id = ? AND version = ?")
                .bind(workflow_id)
                .bind(workflow_version)
                .fetch_optional(&mut *tx)
                .await?;
        if version_exists.is_none() {
            return Err(StoreError::VersionNotFound(
                workflow_id.to_string(),
                workflow_version,
            ));
        }
        sqlx::query(
            "INSERT INTO runs (id, workflow_id, workflow_version, status, input, started_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(run_id)
        .bind(workflow_id)
        .bind(workflow_version)
        .bind(status)
        .bind(serde_json::to_string(input)?)
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 终态时同时写 output/error 与 ended_at。ended_at 为 None 用当前时间；
    /// 恢复回填要传事件里的结束时间——别把「重启时刻」伪装成「结束时刻」。
    pub async fn set_run_status(
        &self,
        run_id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
        ended_at: Option<&DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        ensure_run_status(status)?;
        let terminal = DbRunStatus::is_terminal_str(status);
        let affected = sqlx::query(
            "UPDATE runs SET status = ?, output = ?, error = ?,
                    ended_at = CASE WHEN ? THEN ? ELSE ended_at END
             WHERE id = ?",
        )
        .bind(status)
        .bind(output.map(serde_json::to_string).transpose()?)
        .bind(error)
        .bind(terminal)
        .bind(ended_at.copied().unwrap_or_else(Utc::now).to_rfc3339())
        .bind(run_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            // 静默成功会掩盖「run 行根本没插进去」这类问题
            return Err(StoreError::RunNotFound(run_id.to_string()));
        }
        Ok(())
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, StoreError> {
        let row = sqlx::query("SELECT id, workflow_id, workflow_version, status, input, output, error, started_at, ended_at FROM runs WHERE id = ?")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::RunNotFound(run_id.to_string()))?;
        Self::run_from_row(row)
    }

    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        status: Option<&str>,
        before_run_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, StoreError> {
        // status 词汇表校验在 RPC 边缘（-32010）；这里作为读过滤，未知值自然查空。
        // before_run_id 游标：返回该 run 之前更旧的记录（started_at 严格更小）；
        // 游标 run 已被删时子查询为 NULL，整页为空——翻页到此为止，语义可接受
        let mut qb: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, workflow_id, workflow_version, status, input, output, error, started_at, ended_at FROM runs WHERE 1=1",
        );
        if let Some(id) = workflow_id {
            qb.push(" AND workflow_id = ").push_bind(id);
        }
        if let Some(status) = status {
            qb.push(" AND status = ").push_bind(status);
        }
        if let Some(before) = before_run_id {
            qb.push(" AND started_at < (SELECT started_at FROM runs WHERE id = ")
                .push_bind(before)
                .push(")");
        }
        qb.push(" ORDER BY started_at DESC LIMIT ").push_bind(limit);
        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(Self::run_from_row).collect()
    }

    /// 崩溃恢复的输入：进程重启后需要续跑的 run。
    pub async fn unfinished_runs(&self) -> Result<Vec<RunRecord>, StoreError> {
        let rows =
            sqlx::query("SELECT id, workflow_id, workflow_version, status, input, output, error, started_at, ended_at FROM runs WHERE status IN (?, ?, ?) ORDER BY started_at ASC")
                .bind(DbRunStatus::Initializing.as_str())
                .bind(DbRunStatus::Running.as_str())
                .bind(DbRunStatus::AwaitingResume.as_str())
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(Self::run_from_row).collect()
    }

    fn run_from_row(row: sqlx::sqlite::SqliteRow) -> Result<RunRecord, StoreError> {
        let input: String = row.try_get("input")?;
        let output: Option<String> = row.try_get("output")?;
        let ended_at: Option<String> = row.try_get("ended_at")?;
        Ok(RunRecord {
            id: row.try_get("id")?,
            workflow_id: row.try_get("workflow_id")?,
            workflow_version: row.try_get("workflow_version")?,
            status: row.try_get("status")?,
            input: serde_json::from_str(&input)?,
            output: output.map(|o| serde_json::from_str(&o)).transpose()?,
            error: row.try_get("error")?,
            started_at: parse_ts(&row.try_get::<String, _>("started_at")?)?,
            ended_at: ended_at.map(|ts| parse_ts(&ts)).transpose()?,
        })
    }
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>, StoreError> {
    Ok(DateTime::parse_from_rfc3339(raw)
        .map_err(|e| StoreError::InvalidTimestamp(format!("{raw}：{e}")))?
        .with_timezone(&Utc))
}

pub fn definition_checksum(definition: &Value) -> Result<String, StoreError> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(definition)?);
    Ok(hex::encode(hasher.finalize()))
}
