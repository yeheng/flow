//! Read-only queries over the same SQLite authority that commits domain transitions.
use serde_json::Value;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RESPONSE_TOO_LARGE")]
    ResponseTooLarge,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub struct Store {
    pool: SqlitePool,
}

// Startup must scale with live runs even before SQLite has collected statistics.
const ACTIVE_RUNS_SQL:&str="SELECT key FROM current_entities INDEXED BY entity_active_runs WHERE kind='run' AND status IN ('running','awaiting_resume') AND key>? ORDER BY key LIMIT 256";

pub use flow_journal::sqlite::EntityChange;

#[derive(Default)]
pub struct RunsQuery<'a> {
    pub workflow_id: Option<&'a str>,
    pub status: Option<&'a str>,
    pub source: Option<&'a str>,
    pub before_run_id: Option<&'a str>,
    pub limit: usize,
}

fn attempt_filter(q: &mut QueryBuilder<Sqlite>, run: &str, all: bool) {
    q.push("parent=").push_bind(run);
    if !all {
        // Only the attempt that currently owns the node, not its historical attempts.
        q.push(" AND extra IN (SELECT dispatch_id FROM current_entities WHERE kind='run_node' AND parent=")
            .push_bind(run)
            .push(")");
    }
}

fn decode_body(kind: &str, key: &str, body: &str, digest: &str) -> Result<Value, Error> {
    if flow_journal::sqlite::entity_digest(kind, key, body) != digest {
        return Err(sqlx::Error::Protocol("current entity checksum mismatch".into()).into());
    }
    serde_json::from_str(body).map_err(|e| Error::Database(sqlx::Error::Decode(Box::new(e))))
}

impl Store {
    /// Opens only an existing authoritative database. This query handle cannot write it.
    pub async fn open(path: &Path, journal_id: &str) -> Result<Self, Error> {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(SqliteConnectOptions::new().filename(path).read_only(true))
            .await?;
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        if version != flow_journal::sqlite::FORMAT {
            return Err(sqlx::Error::Protocol("unsupported SQLite storage format".into()).into());
        }
        let identity: String =
            sqlx::query_scalar("SELECT journal_id FROM store_meta WHERE singleton=1")
                .fetch_one(&pool)
                .await?;
        if identity != journal_id {
            return Err(sqlx::Error::Protocol("foreign SQLite authority".into()).into());
        }
        Ok(Self { pool })
    }
    pub async fn applied_lsn(&self) -> Result<u64, Error> {
        let raw: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&self.pool)
                .await?;
        raw.parse()
            .map_err(|_| Error::Database(sqlx::Error::Protocol("invalid commit cursor".into())))
    }
    pub async fn domain_mode(&self) -> Result<bool, Error> {
        sqlx::query_scalar("SELECT domain_mode FROM store_meta WHERE singleton=1")
            .fetch_one(&self.pool)
            .await
            .map_err(Error::from)
    }
    /// State and its cursor share one SQLite read transaction.
    pub async fn get(&self, kind: &str, key: &str) -> Result<(u64, Option<Value>), Error> {
        self.get_entity(kind, key, true, true).await
    }
    /// Read run metadata without assembling its node ledger.
    pub async fn get_summary(&self, kind: &str, key: &str) -> Result<(u64, Option<Value>), Error> {
        self.get_entity(kind, key, false, false).await
    }
    pub async fn get_execution(&self, key: &str) -> Result<(u64, Option<Value>), Error> {
        self.get_entity("run", key, true, false).await
    }
    async fn get_entity(
        &self,
        kind: &str,
        key: &str,
        with_nodes: bool,
        all_attempts: bool,
    ) -> Result<(u64, Option<Value>), Error> {
        let mut tx = self.pool.begin().await?;
        let lsn: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        let body_row =
            sqlx::query("SELECT CASE WHEN length(CAST(body AS BLOB))<=8388608 THEN body ELSE 'RESPONSE_TOO_LARGE' END,digest FROM current_entities WHERE kind=? AND key=?")
                .bind(kind)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?;
        let body: Option<String> = body_row.as_ref().map(|r| r.get(0));
        if body.as_deref() == Some("RESPONSE_TOO_LARGE") {
            return Err(Error::ResponseTooLarge);
        }
        let mut value: Option<Value> = body
            .as_ref()
            .map(|s| decode_body(kind, key, s, body_row.as_ref().unwrap().get("digest")))
            .transpose()?;
        if kind == "workflow" && with_nodes {
            if let Some(workflow) = value.as_mut() {
                let size:i64=sqlx::query_scalar("SELECT COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM current_entities WHERE kind='workflow_version' AND parent=?").bind(key).fetch_one(&mut *tx).await?;
                if size > 8 * 1024 * 1024 {
                    return Err(Error::ResponseTooLarge);
                }
                let rows=sqlx::query("SELECT key,body,digest FROM current_entities WHERE kind='workflow_version' AND parent=? ORDER BY key").bind(key).fetch_all(&mut *tx).await?;
                for row in rows {
                    let version = decode_body(
                        "workflow_version",
                        row.get("key"),
                        row.get("body"),
                        row.get("digest"),
                    )?;
                    let key = version["version"]
                        .as_u64()
                        .ok_or_else(|| sqlx::Error::Protocol("invalid definition version".into()))?
                        .to_string();
                    workflow["versions"][key] = version;
                }
            }
        }
        if kind == "run" && with_nodes {
            if let Some(run) = value.as_mut() {
                let node_size: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(key AS BLOB))+4),0) FROM current_entities WHERE kind='run_node' AND parent=?")
                    .bind(key).fetch_one(&mut *tx).await?;
                let mut size_query = QueryBuilder::<Sqlite>::new(
                    "SELECT COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(key AS BLOB))+4),0) FROM current_entities WHERE kind='run_attempt' AND ",
                );
                attempt_filter(&mut size_query, key, all_attempts);
                let size = node_size
                    + size_query
                        .build_query_scalar::<i64>()
                        .fetch_one(&mut *tx)
                        .await?;
                if size > (8 * 1024 * 1024 - body.as_ref().map_or(0, String::len)) as i64 {
                    return Err(Error::ResponseTooLarge);
                }
                let nodes = sqlx::query("SELECT slot AS node_id,key,body,digest FROM current_entities WHERE kind='run_node' AND parent=?")
                    .bind(key).fetch_all(&mut *tx).await?;
                let mut map = serde_json::Map::new();
                for node in nodes {
                    let body: String = node.get("body");
                    map.insert(
                        node.get("node_id"),
                        decode_body("run_node", node.get("key"), &body, node.get("digest"))?,
                    );
                }
                let mut attempt_query = QueryBuilder::<Sqlite>::new(
                    "SELECT slot AS node_id,extra AS dispatch_id,key,body,digest FROM current_entities WHERE kind='run_attempt' AND ",
                );
                attempt_filter(&mut attempt_query, key, all_attempts);
                let attempts = attempt_query.build().fetch_all(&mut *tx).await?;
                for attempt in attempts {
                    let node_id: String = attempt.get("node_id");
                    let dispatch_id: String = attempt.get("dispatch_id");
                    let body: String = attempt.get("body");
                    if let Some(node) = map.get_mut(&node_id) {
                        node["attempts"][dispatch_id] = decode_body(
                            "run_attempt",
                            attempt.get("key"),
                            &body,
                            attempt.get("digest"),
                        )?;
                    }
                }
                run["nodes"] = Value::Object(map);
            }
        }
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            value,
        ))
    }
    /// Only current state is retained. An older revision cannot be reconstructed by
    /// silently reading a later one; callers serialize decisions against the writer.
    pub async fn current_at(
        &self,
        kind: &str,
        key: &str,
        upper: u64,
    ) -> Result<Option<Value>, Error> {
        let row =
            sqlx::query("SELECT body,digest,last_lsn FROM current_entities WHERE kind=? AND key=?")
                .bind(kind)
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|row| {
            let revision: String = row.get("last_lsn");
            if revision > format!("{upper:020}") {
                return Err(sqlx::Error::Protocol(
                    "current state is newer than decision boundary".into(),
                )
                .into());
            }
            decode_body(kind, key, row.get("body"), row.get("digest"))
        })
        .transpose()
    }
    pub async fn current_keys(
        &self,
        kind: &str,
        parent: Option<&str>,
        after: &str,
        upper: u64,
        limit: usize,
    ) -> Result<Vec<String>, Error> {
        let mut q = QueryBuilder::<Sqlite>::new("SELECT key FROM current_entities WHERE kind=");
        q.push_bind(kind)
            .push(" AND key>")
            .push_bind(after)
            .push(" AND last_lsn<=")
            .push_bind(format!("{upper:020}"));
        if let Some(parent) = parent {
            q.push(" AND parent=").push_bind(parent);
        }
        q.push(" ORDER BY key LIMIT ")
            .push_bind(limit.min(256) as i64);
        q.build_query_scalar()
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)
    }
    pub async fn children(&self, parent: &str, upper: u64) -> Result<Vec<(String, String)>, Error> {
        sqlx::query_as("SELECT slot,MIN(extra) FROM current_entities WHERE kind='child' AND parent=? AND last_lsn<=? GROUP BY slot")
            .bind(parent).bind(format!("{upper:020}")).fetch_all(&self.pool).await.map_err(Error::from)
    }
    pub async fn dispatch_owner(&self, dispatch: &str) -> Result<Option<Value>, Error> {
        Ok(self.get("dispatch", dispatch).await?.1)
    }
    pub async fn active_run_ids(&self, after: &str) -> Result<Vec<String>, Error> {
        sqlx::query_scalar(ACTIVE_RUNS_SQL)
            .bind(after)
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)
    }
    pub async fn list(
        &self,
        kind: &str,
        after: &str,
        limit: usize,
    ) -> Result<(u64, Vec<Value>), Error> {
        let mut tx = self.pool.begin().await?;
        let lsn: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        let rows = sqlx::query(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities WHERE kind=? AND key>? ORDER BY key LIMIT ?",
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
                    return Err(Error::ResponseTooLarge);
                }
                break;
            }
            let body_row =
                sqlx::query("SELECT body,digest FROM current_entities WHERE kind=? AND key=?")
                    .bind(kind)
                    .bind(row.get::<&str, _>("key"))
                    .fetch_one(&mut *tx)
                    .await?;
            values.push(decode_body(
                kind,
                row.get("key"),
                body_row.get("body"),
                body_row.get("digest"),
            )?);
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

    /// Filter, keyset ordering and LIMIT execute in the same SQLite snapshot as the cursor.
    pub async fn runs_page(&self, filter: RunsQuery<'_>) -> Result<(u64, Vec<Value>), Error> {
        let mut tx = self.pool.begin().await?;
        let cursor: Option<String> = match filter.before_run_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT created_order FROM current_entities WHERE kind='run' AND key=?",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
            }
            None => None,
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities WHERE kind='run'",
        );
        for (field, value) in [
            ("workflow_id", filter.workflow_id),
            ("status", filter.status),
            ("source", filter.source),
        ] {
            if let Some(value) = value {
                query.push(format!(" AND {field}=")).push_bind(value);
            }
        }
        if let Some(created) = &cursor {
            query
                .push(" AND (created_order,key)<(")
                .push_bind(created)
                .push(",")
                .push_bind(filter.before_run_id.unwrap())
                .push(")");
        }
        query
            .push(" ORDER BY created_order DESC,key DESC LIMIT ")
            .push_bind(filter.limit.min(500) as i64);
        let rows = query.build().fetch_all(&mut *tx).await?;
        Self::bodies(&mut tx, "run", rows).await
    }

    /// Product collections retain their existing order without cloning reducer history.
    pub async fn ordered(
        &self,
        kind: &str,
        workflow_id: Option<&str>,
        by_name: bool,
    ) -> Result<Vec<Value>, Error> {
        let mut tx = self.pool.begin().await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities WHERE kind=",
        );
        query.push_bind(kind);
        if let Some(id) = workflow_id {
            query.push(" AND workflow_id=").push_bind(id);
        }
        query.push(if by_name {
            " ORDER BY name,key"
        } else {
            " ORDER BY created_order DESC,key DESC"
        });
        query.push(" LIMIT 10001");
        let rows = query.build().fetch_all(&mut *tx).await?;
        if rows.len() > 10000 {
            return Err(Error::ResponseTooLarge);
        }
        Ok(Self::bodies(&mut tx, kind, rows).await?.1)
    }

    async fn bodies(
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        kind: &str,
        rows: Vec<sqlx::sqlite::SqliteRow>,
    ) -> Result<(u64, Vec<Value>), Error> {
        let bytes: i64 = rows.iter().map(|r| r.get::<i64, _>("bytes")).sum();
        if bytes > 8 * 1024 * 1024 {
            return Err(Error::ResponseTooLarge);
        }
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let body_row =
                sqlx::query("SELECT body,digest FROM current_entities WHERE kind=? AND key=?")
                    .bind(kind)
                    .bind(row.get::<&str, _>("key"))
                    .fetch_one(&mut **tx)
                    .await?;
            values.push(decode_body(
                kind,
                row.get("key"),
                body_row.get("body"),
                body_row.get("digest"),
            )?);
        }
        let lsn: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&mut **tx)
                .await?;
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            values,
        ))
    }

    pub async fn run_counts(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<(String, String, i64)>, Error> {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT workflow_id AS workflow,status AS status,COUNT(*) AS count FROM current_entities WHERE kind='run'",
        );
        if let Some(id) = workflow_id {
            query.push(" AND workflow_id=").push_bind(id);
        }
        query.push(" GROUP BY workflow_id,status");
        Ok(query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| (r.get("workflow"), r.get("status"), r.get("count")))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn startup_seeks_the_active_index_without_scanning_completed_history() {
        let dir = flow_test_support::io::TempDir::new("active-query-plan");
        let writer = flow_journal::Journal::open(dir.path(), Default::default())
            .await
            .unwrap();
        let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
        let plan: String = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {ACTIVE_RUNS_SQL}"))
            .unwrap()
            .query_map([""], |r| r.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(plan.contains("entity_active_runs"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        writer.close().await.unwrap();
    }
}
