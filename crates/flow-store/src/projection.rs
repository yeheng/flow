//! Disposable v2 projection. Business mutations enter as complete journal transactions only.
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::path::Path;

pub struct Projector {
    pool: SqlitePool,
    journal_id: String,
}
pub struct ProjectedRow {
    pub kind: String,
    pub key: String,
    pub value: Option<Value>,
}

impl Projector {
    /// Startup-only replacement from an already verified complete reducer snapshot.
    /// Rows and cursor are installed atomically; the caller still owns the journal lock.
    pub async fn restore_snapshot(
        &self,
        journal_id: &str,
        lsn: u64,
        rows: Vec<ProjectedRow>,
    ) -> Result<(), sqlx::Error> {
        if journal_id != self.journal_id {
            return Err(sqlx::Error::Protocol("foreign snapshot".into()));
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM journal_entities")
            .execute(&mut *tx)
            .await?;
        for row in rows {
            if let Some(value) = row.value {
                sqlx::query("INSERT INTO journal_entities(kind,key,body) VALUES(?,?,?)")
                    .bind(row.kind)
                    .bind(row.key)
                    .bind(value.to_string())
                    .execute(&mut *tx)
                    .await?;
            }
        }
        sqlx::query("UPDATE journal_cursor SET applied_lsn=? WHERE singleton=1")
            .bind(lsn.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
    pub async fn open(path: &Path, journal_id: &str) -> Result<Self, sqlx::Error> {
        use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS journal_cursor (singleton INTEGER PRIMARY KEY CHECK(singleton=1), journal_id TEXT NOT NULL, applied_lsn TEXT NOT NULL)").execute(&pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS journal_entities (kind TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(kind,key))").execute(&pool).await?;
        sqlx::query("INSERT OR IGNORE INTO journal_cursor VALUES(1,?, '0')")
            .bind(journal_id)
            .execute(&pool)
            .await?;
        let identity: String =
            sqlx::query_scalar("SELECT journal_id FROM journal_cursor WHERE singleton=1")
                .fetch_one(&pool)
                .await?;
        if identity != journal_id {
            return Err(sqlx::Error::Protocol(
                "projection belongs to another journal".into(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(sqlx::Error::Io)?;
        }
        Ok(Self {
            pool,
            journal_id: journal_id.into(),
        })
    }
    pub async fn applied_lsn(&self) -> Result<u64, sqlx::Error> {
        let value: String =
            sqlx::query_scalar("SELECT applied_lsn FROM journal_cursor WHERE singleton=1")
                .fetch_one(&self.pool)
                .await?;
        value
            .parse()
            .map_err(|_| sqlx::Error::Protocol("invalid projection cursor".into()))
    }
    pub async fn apply(
        &self,
        journal_id: &str,
        lsn: u64,
        rows: Vec<ProjectedRow>,
    ) -> Result<(), sqlx::Error> {
        if journal_id != self.journal_id {
            return Err(sqlx::Error::Protocol("foreign journal transaction".into()));
        }
        let mut tx = self.pool.begin().await?;
        let previous: String =
            sqlx::query_scalar("SELECT applied_lsn FROM journal_cursor WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        let previous: u64 = previous
            .parse()
            .map_err(|_| sqlx::Error::Protocol("invalid projection cursor".into()))?;
        if lsn <= previous {
            return Ok(());
        }
        if lsn != previous + 1 {
            return Err(sqlx::Error::Protocol("projection transaction gap".into()));
        }
        for row in rows {
            if let Some(value) = row.value {
                sqlx::query("INSERT INTO journal_entities(kind,key,body) VALUES(?,?,?) ON CONFLICT(kind,key) DO UPDATE SET body=excluded.body")
                    .bind(row.kind).bind(row.key).bind(value.to_string()).execute(&mut *tx).await?;
            } else {
                sqlx::query("DELETE FROM journal_entities WHERE kind=? AND key=?")
                    .bind(row.kind)
                    .bind(row.key)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        sqlx::query("UPDATE journal_cursor SET applied_lsn=? WHERE singleton=1")
            .bind(lsn.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
    /// State and its cursor share one SQLite read transaction.
    pub async fn get(&self, kind: &str, key: &str) -> Result<(u64, Option<Value>), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let lsn: String =
            sqlx::query_scalar("SELECT applied_lsn FROM journal_cursor WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        let body: Option<String> =
            sqlx::query_scalar("SELECT CASE WHEN length(CAST(body AS BLOB))<=8388608 THEN body ELSE 'RESPONSE_TOO_LARGE' END FROM journal_entities WHERE kind=? AND key=?")
                .bind(kind)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?;
        if body.as_deref() == Some("RESPONSE_TOO_LARGE") {
            return Err(sqlx::Error::Protocol("RESPONSE_TOO_LARGE".into()));
        }
        let value = body
            .map(|s| serde_json::from_str(&s).map_err(|e| sqlx::Error::Decode(Box::new(e))))
            .transpose()?;
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            value,
        ))
    }
    pub async fn list(
        &self,
        kind: &str,
        after: &str,
        limit: usize,
    ) -> Result<(u64, Vec<Value>), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let lsn: String =
            sqlx::query_scalar("SELECT applied_lsn FROM journal_cursor WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        let rows = sqlx::query(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM journal_entities WHERE kind=? AND key>? ORDER BY key LIMIT ?",
        )
        .bind(kind)
        .bind(after)
        .bind(limit.min(256) as i64)
        .fetch_all(&mut *tx)
        .await?;
        let mut values = Vec::new();
        let mut bytes = 0i64;
        for row in rows {
            let n: i64 = row.get("bytes");
            if bytes + n > 4 * 1024 * 1024 {
                if values.is_empty() {
                    return Err(sqlx::Error::Protocol("RESPONSE_TOO_LARGE".into()));
                }
                break;
            }
            let body: String =
                sqlx::query_scalar("SELECT body FROM journal_entities WHERE kind=? AND key=?")
                    .bind(kind)
                    .bind(row.get::<&str, _>("key"))
                    .fetch_one(&mut *tx)
                    .await?;
            values.push(serde_json::from_str(&body).map_err(|e| sqlx::Error::Decode(Box::new(e)))?);
            bytes += n;
        }
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            values,
        ))
    }
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
