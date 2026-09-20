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
    DbRunStatus, RunRecord, WorkflowSummary, WorkflowVersion, STATUS_DRAFT, STATUS_PUBLISHED,
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
    #[error("非法的 run 状态：{0}")]
    InvalidStatus(String),
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
                    "SELECT * FROM workflow_versions WHERE workflow_id = ? AND version = ?",
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
            "SELECT * FROM workflow_versions WHERE workflow_id = ? ORDER BY version DESC LIMIT 1",
        )
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Self::version_from_row).transpose()
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
    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), StoreError> {
        let runs: i64 = sqlx::query("SELECT COUNT(*) AS c FROM runs WHERE workflow_id = ?")
            .bind(workflow_id)
            .fetch_one(&self.pool)
            .await?
            .try_get("c")?;
        if runs > 0 {
            return Err(StoreError::WorkflowNotFound(format!(
                "{workflow_id}（已有 {runs} 条 run 记录，拒绝删除）"
            )));
        }
        let affected = sqlx::query("DELETE FROM workflows WHERE id = ?")
            .bind(workflow_id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(StoreError::WorkflowNotFound(workflow_id.to_string()));
        }
        Ok(())
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

    pub async fn insert_run(
        &self,
        run_id: &str,
        workflow_id: &str,
        workflow_version: i64,
        input: &Value,
        status: &str,
    ) -> Result<(), StoreError> {
        ensure_run_status(status)?;
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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 终态时同时写 output/error 与 ended_at。
    pub async fn set_run_status(
        &self,
        run_id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
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
        .bind(Utc::now().to_rfc3339())
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
        let row = sqlx::query("SELECT * FROM runs WHERE id = ?")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::RunNotFound(run_id.to_string()))?;
        Self::run_from_row(row)
    }

    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let rows = match workflow_id {
            Some(id) => {
                sqlx::query(
                    "SELECT * FROM runs WHERE workflow_id = ? ORDER BY started_at DESC LIMIT ?",
                )
                .bind(id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?")
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.into_iter().map(Self::run_from_row).collect()
    }

    /// 崩溃恢复的输入：进程重启后需要续跑的 run。
    pub async fn unfinished_runs(&self) -> Result<Vec<RunRecord>, StoreError> {
        let rows =
            sqlx::query("SELECT * FROM runs WHERE status IN (?, ?, ?) ORDER BY started_at ASC")
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
        .map_err(|e| {
            StoreError::Json(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )))
        })?
        .with_timezone(&Utc))
}

pub fn definition_checksum(definition: &Value) -> Result<String, StoreError> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(definition)?);
    Ok(hex::encode(hasher.finalize()))
}
