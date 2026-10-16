//! Disposable v2 projection. Business mutations enter as complete journal transactions only.
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Connection, QueryBuilder, Row, Sqlite, SqlitePool};
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

#[derive(Default)]
pub struct RunsQuery<'a> {
    pub workflow_id: Option<&'a str>,
    pub status: Option<&'a str>,
    pub source: Option<&'a str>,
    pub before_run_id: Option<&'a str>,
    pub limit: usize,
}

fn created_order(kind: &str, value: &Value) -> Result<String, sqlx::Error> {
    if kind == "history" {
        return created_order("run", value);
    }
    if !matches!(
        kind,
        "run" | "workflow" | "schedule" | "webhook" | "template"
    ) {
        return Ok(String::new());
    }
    value
        .get("created_at")
        .and_then(Value::as_str)
        .map(|raw| {
            chrono::DateTime::parse_from_rfc3339(raw)
                .map(|ts| {
                    ts.with_timezone(&chrono::Utc)
                        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
                })
                // Old journals may contain invalid display timestamps. Preserve the
                // original fact, sorting it last instead of making replay impossible.
                .or_else(|_| Ok::<_, sqlx::Error>(String::new()))
        })
        .transpose()
        .map(|v| v.unwrap_or_default())
}

fn attempt_filter(q: &mut QueryBuilder<Sqlite>, run: &str, all: bool) {
    if all {
        q.push("json_extract(key,'$[0]')=").push_bind(run);
    } else {
        q.push("key IN (SELECT json_array(").push_bind(run)
            .push(",json_extract(key,'$[1]'),json_extract(body,'$.dispatch_id')) FROM journal_entities WHERE kind='run_node' AND json_extract(key,'$[0]')=")
            .push_bind(run).push(")");
    }
}

fn decode_history(row: &sqlx::sqlite::SqliteRow) -> Result<Option<Value>, sqlx::Error> {
    let key: String = row.get("key");
    let body: String = row.get("body");
    let key: Value = serde_json::from_str(&key).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
    let digest = hex::encode(Sha256::digest(body.as_bytes()));
    if key[3].as_str() != Some(digest.as_str()) {
        return Err(sqlx::Error::Protocol(
            "execution index checksum mismatch; rebuild derived index".into(),
        ));
    }
    serde_json::from_str(&body).map_err(|e| sqlx::Error::Decode(Box::new(e)))
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
                sqlx::query(
                    "INSERT INTO journal_entities(kind,key,body,created_order) VALUES(?,?,?,?)",
                )
                .bind(&row.kind)
                .bind(row.key)
                .bind(value.to_string())
                .bind(created_order(&row.kind, &value)?)
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
        sqlx::query("CREATE INDEX IF NOT EXISTS journal_run_nodes ON journal_entities(json_extract(key,'$[0]')) WHERE kind='run_node'")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS journal_history_versions ON journal_entities(json_extract(key,'$[0]'),json_extract(key,'$[1]'),json_extract(key,'$[2]') DESC) WHERE kind='history'").execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS journal_history_lsn ON journal_entities(json_extract(key,'$[2]')) WHERE kind='history'").execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS journal_attempt_nodes ON journal_entities(json_extract(key,'$[0]'),json_extract(key,'$[1]')) WHERE kind='run_attempt'").execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS journal_child_parent ON journal_entities(json_extract(key,'$[0]')) WHERE kind='child'").execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS journal_history_parent ON journal_entities(json_extract(key,'$[0]'),json_extract(json_extract(key,'$[1]'),'$[0]'),json_extract(key,'$[1]'),json_extract(key,'$[2]')) WHERE kind='history' AND json_valid(json_extract(key,'$[1]'))").execute(&pool).await?;
        let columns = sqlx::query("PRAGMA table_info(journal_entities)")
            .fetch_all(&pool)
            .await?;
        if !columns
            .iter()
            .any(|r| r.get::<&str, _>("name") == "created_order")
        {
            sqlx::query("ALTER TABLE journal_entities ADD COLUMN created_order TEXT")
                .execute(&pool)
                .await?;
        }
        // Migrate old disposable projections one body at a time, preserving nanoseconds
        // and timezone equivalence rather than relying on raw RFC3339 lexical order.
        loop {
            let keys = sqlx::query(
                "SELECT kind,key FROM journal_entities WHERE created_order IS NULL LIMIT 128",
            )
            .fetch_all(&pool)
            .await?;
            if keys.is_empty() {
                break;
            }
            let mut tx = pool.begin().await?;
            for key in keys {
                let kind = key.get::<&str, _>("kind");
                let key = key.get::<&str, _>("key");
                let body: String =
                    sqlx::query_scalar("SELECT body FROM journal_entities WHERE kind=? AND key=?")
                        .bind(kind)
                        .bind(key)
                        .fetch_one(&mut *tx)
                        .await?;
                let value: Value =
                    serde_json::from_str(&body).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
                sqlx::query("UPDATE journal_entities SET created_order=? WHERE kind=? AND key=?")
                    .bind(created_order(kind, &value)?)
                    .bind(kind)
                    .bind(key)
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
        }
        for statement in [
            "CREATE INDEX IF NOT EXISTS journal_created_order_v2 ON journal_entities(kind,created_order DESC,key DESC)",
            "CREATE INDEX IF NOT EXISTS journal_workflow_order_v2 ON journal_entities(kind,json_extract(body,'$.workflow_id'),created_order DESC,key DESC)",
            "CREATE INDEX IF NOT EXISTS journal_status_order_v2 ON journal_entities(kind,json_extract(body,'$.status'),created_order DESC,key DESC)",
            "CREATE INDEX IF NOT EXISTS journal_source_order_v2 ON journal_entities(kind,json_extract(body,'$.source'),created_order DESC,key DESC)",
            "CREATE INDEX IF NOT EXISTS journal_name_order ON journal_entities(kind,json_extract(body,'$.name'),key)",
        ] {
            sqlx::query(statement).execute(&pool).await?;
        }
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
                let statement = if row.kind == "command" {
                    "INSERT INTO journal_entities(kind,key,body,created_order) VALUES(?,?,?,?) ON CONFLICT(kind,key) DO NOTHING"
                } else {
                    "INSERT INTO journal_entities(kind,key,body,created_order) VALUES(?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET body=excluded.body,created_order=excluded.created_order"
                };
                sqlx::query(statement)
                    .bind(&row.kind)
                    .bind(row.key)
                    .bind(value.to_string())
                    .bind(created_order(&row.kind, &value)?)
                    .execute(&mut *tx)
                    .await?;
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
        self.get_entity(kind, key, true, true).await
    }
    /// Read run metadata without assembling its node ledger.
    pub async fn get_summary(
        &self,
        kind: &str,
        key: &str,
    ) -> Result<(u64, Option<Value>), sqlx::Error> {
        self.get_entity(kind, key, false, false).await
    }
    pub async fn get_execution(&self, key: &str) -> Result<(u64, Option<Value>), sqlx::Error> {
        self.get_entity("run", key, true, false).await
    }
    async fn get_entity(
        &self,
        kind: &str,
        key: &str,
        with_nodes: bool,
        all_attempts: bool,
    ) -> Result<(u64, Option<Value>), sqlx::Error> {
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
        let mut value: Option<Value> = body
            .as_ref()
            .map(|s| serde_json::from_str(s).map_err(|e| sqlx::Error::Decode(Box::new(e))))
            .transpose()?;
        if kind == "run" && with_nodes {
            if let Some(run) = value.as_mut().filter(|r| r["_node_layout"] == "split-v1") {
                let node_size: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(key AS BLOB))+4),0) FROM journal_entities WHERE kind='run_node' AND json_extract(key,'$[0]')=?")
                    .bind(key).fetch_one(&mut *tx).await?;
                let mut size_query = QueryBuilder::<Sqlite>::new(
                    "SELECT COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(key AS BLOB))+4),0) FROM journal_entities WHERE kind='run_attempt' AND ",
                );
                attempt_filter(&mut size_query, key, all_attempts);
                let size = node_size
                    + size_query
                        .build_query_scalar::<i64>()
                        .fetch_one(&mut *tx)
                        .await?;
                if size > (8 * 1024 * 1024 - body.as_ref().map_or(0, String::len)) as i64 {
                    return Err(sqlx::Error::Protocol("RESPONSE_TOO_LARGE".into()));
                }
                let nodes = sqlx::query("SELECT json_extract(key,'$[1]') AS node_id,body FROM journal_entities WHERE kind='run_node' AND json_extract(key,'$[0]')=?")
                    .bind(key).fetch_all(&mut *tx).await?;
                let mut map = serde_json::Map::new();
                for node in nodes {
                    let body: String = node.get("body");
                    map.insert(
                        node.get("node_id"),
                        serde_json::from_str(&body)
                            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
                    );
                }
                let mut attempt_query = QueryBuilder::<Sqlite>::new(
                    "SELECT json_extract(key,'$[1]') AS node_id,json_extract(key,'$[2]') AS dispatch_id,body FROM journal_entities WHERE kind='run_attempt' AND ",
                );
                attempt_filter(&mut attempt_query, key, all_attempts);
                let attempts = attempt_query.build().fetch_all(&mut *tx).await?;
                for attempt in attempts {
                    let node_id: String = attempt.get("node_id");
                    let dispatch_id: String = attempt.get("dispatch_id");
                    let body: String = attempt.get("body");
                    if let Some(node) = map.get_mut(&node_id) {
                        if node["_attempt_layout"] == "split-v1" {
                            node["attempts"][dispatch_id] = serde_json::from_str(&body)
                                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
                        }
                    }
                }
                for node in map.values_mut() {
                    if let Some(node) = node.as_object_mut() {
                        node.remove("_attempt_layout");
                    }
                }
                run["nodes"] = Value::Object(map);
                run.as_object_mut()
                    .expect("run object")
                    .remove("_node_layout");
            }
        }
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            value,
        ))
    }
    /// Versioned derived entities use fixed-width decimal LSNs, preserving u64 order.
    /// Atomic, disk-to-disk rebuild. SQLite's temporary grouping spills to disk;
    /// Rust never collects the terminal execution tree during projection recovery.
    pub async fn restore_history(&self, source: &Path, upper: u64) -> Result<(), sqlx::Error> {
        let mut conn = self.pool.acquire().await?.detach();
        sqlx::query("PRAGMA temp_store=FILE")
            .execute(&mut conn)
            .await?;
        sqlx::query("ATTACH DATABASE ? AS history_source")
            .bind(source.to_string_lossy().as_ref())
            .execute(&mut conn)
            .await?;
        let mut tx = conn.begin().await?;
        // ATTACH reads and installation share this transaction's source snapshot.
        // Check bounded pages, including tombstones, before replacing the target.
        let mut after = String::new();
        loop {
            let rows = sqlx::query("SELECT key,body FROM history_source.journal_entities WHERE kind='history' AND key>? AND json_extract(key,'$[2]')<=? ORDER BY key LIMIT 128")
                .bind(&after).bind(format!("{upper:020}")).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                decode_history(row)?;
            }
            after = rows.last().unwrap().get("key");
        }
        sqlx::query("DELETE FROM journal_entities")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO journal_entities(kind,key,body,created_order) SELECT json_extract(v.key,'$[0]'),json_extract(v.key,'$[1]'),v.body,v.created_order FROM history_source.journal_entities v JOIN (SELECT json_extract(key,'$[0]') AS k,json_extract(key,'$[1]') AS id,MAX(json_extract(key,'$[2]')) AS revision FROM history_source.journal_entities WHERE kind='history' AND json_extract(key,'$[2]')<=? GROUP BY json_extract(key,'$[0]'),json_extract(key,'$[1]')) latest ON json_extract(v.key,'$[0]')=latest.k AND json_extract(v.key,'$[1]')=latest.id AND json_extract(v.key,'$[2]')=latest.revision WHERE v.kind='history' AND v.body!='null'")
            .bind(format!("{upper:020}")).execute(&mut *tx).await?;
        sqlx::query("UPDATE journal_cursor SET applied_lsn=? WHERE singleton=1")
            .bind(upper.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        conn.close().await
    }
    pub async fn history_at(
        &self,
        kind: &str,
        key: &str,
        upper: u64,
    ) -> Result<Option<Value>, sqlx::Error> {
        let row = sqlx::query("SELECT key,body FROM journal_entities WHERE kind='history' AND json_extract(key,'$[0]')=? AND json_extract(key,'$[1]')=? AND json_extract(key,'$[2]')<=? ORDER BY json_extract(key,'$[2]') DESC LIMIT 1")
            .bind(kind).bind(key).bind(format!("{upper:020}")).fetch_optional(&self.pool).await?;
        row.map(|row| decode_history(&row))
            .transpose()
            .map(Option::flatten)
    }
    /// Entity keys at a verified journal prefix. Callers page whole-history diagnostics;
    /// execution reads restrict the prefix to one run or node.
    pub async fn history_keys(
        &self,
        kind: &str,
        parent: Option<&str>,
        after: &str,
        upper: u64,
        limit: usize,
    ) -> Result<Vec<String>, sqlx::Error> {
        let mut q = QueryBuilder::<Sqlite>::new(
            "SELECT DISTINCT json_extract(key,'$[1]') AS entity FROM journal_entities WHERE kind='history' AND json_extract(key,'$[0]')=",
        );
        q.push_bind(kind)
            .push(" AND json_extract(key,'$[2]')<=")
            .push_bind(format!("{upper:020}"));
        q.push(" AND json_extract(key,'$[1]')>").push_bind(after);
        if let Some(parent) = parent {
            q.push(" AND json_valid(json_extract(key,'$[1]')) AND json_extract(json_extract(key,'$[1]'),'$[0]')=").push_bind(parent);
        }
        q.push(" ORDER BY entity LIMIT ")
            .push_bind(limit.min(256) as i64);
        q.build_query_scalar().fetch_all(&self.pool).await
    }
    pub async fn history_changes(&self, lsn: u64) -> Result<Vec<ProjectedRow>, sqlx::Error> {
        let rows = sqlx::query("SELECT key,json_extract(key,'$[0]') AS entity_kind,json_extract(key,'$[1]') AS entity_key,body FROM journal_entities WHERE kind='history' AND json_extract(key,'$[2]')=?")
            .bind(format!("{lsn:020}")).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectedRow {
                    kind: row.get("entity_kind"),
                    key: row.get("entity_key"),
                    value: decode_history(&row)?,
                })
            })
            .collect()
    }
    pub async fn history_children(
        &self,
        parent: &str,
        upper: u64,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        sqlx::query_as("SELECT json_extract(key,'$[1]'),MIN(json_extract(key,'$[2]')) FROM journal_entities WHERE kind='child' AND json_extract(key,'$[0]')=? AND json_extract(body,'$.lsn')<=? GROUP BY json_extract(key,'$[1]')")
            .bind(parent).bind(format!("{upper:020}")).fetch_all(&self.pool).await
    }
    pub async fn dispatch_owner(&self, dispatch: &str) -> Result<Option<Value>, sqlx::Error> {
        Ok(self.get("dispatch", dispatch).await?.1)
    }

    /// Old projections are disposable: the coordinator installs one verified snapshot
    /// before serving reads, so no mixed whole-run/split-node layout is exposed.
    pub async fn requires_split_runs(&self) -> Result<bool, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, i64>("SELECT EXISTS(SELECT 1 FROM journal_entities WHERE kind='run' AND json_extract(body,'$._node_layout') IS NULL OR (kind='run_node' AND json_extract(body,'$._attempt_layout') IS NULL))")
            .fetch_one(&self.pool).await? != 0)
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

    /// Filter, keyset ordering and LIMIT execute in the same SQLite snapshot as the cursor.
    pub async fn runs_page(&self, filter: RunsQuery<'_>) -> Result<(u64, Vec<Value>), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let cursor: Option<String> = match filter.before_run_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT created_order FROM journal_entities WHERE kind='run' AND key=?",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
            }
            None => None,
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM journal_entities WHERE kind='run'",
        );
        for (field, value) in [
            ("workflow_id", filter.workflow_id),
            ("status", filter.status),
            ("source", filter.source),
        ] {
            if let Some(value) = value {
                query
                    .push(format!(" AND json_extract(body,'$.{field}')="))
                    .push_bind(value);
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
    ) -> Result<Vec<Value>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM journal_entities WHERE kind=",
        );
        query.push_bind(kind);
        if let Some(id) = workflow_id {
            query
                .push(" AND json_extract(body,'$.workflow_id')=")
                .push_bind(id);
        }
        query.push(if by_name {
            " ORDER BY json_extract(body,'$.name'),key"
        } else {
            " ORDER BY created_order DESC,key DESC"
        });
        query.push(" LIMIT 10001");
        let rows = query.build().fetch_all(&mut *tx).await?;
        if rows.len() > 10000 {
            return Err(sqlx::Error::Protocol("RESPONSE_TOO_LARGE".into()));
        }
        Ok(Self::bodies(&mut tx, kind, rows).await?.1)
    }

    async fn bodies(
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        kind: &str,
        rows: Vec<sqlx::sqlite::SqliteRow>,
    ) -> Result<(u64, Vec<Value>), sqlx::Error> {
        let bytes: i64 = rows.iter().map(|r| r.get::<i64, _>("bytes")).sum();
        if bytes > 8 * 1024 * 1024 {
            return Err(sqlx::Error::Protocol("RESPONSE_TOO_LARGE".into()));
        }
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let body: String =
                sqlx::query_scalar("SELECT body FROM journal_entities WHERE kind=? AND key=?")
                    .bind(kind)
                    .bind(row.get::<&str, _>("key"))
                    .fetch_one(&mut **tx)
                    .await?;
            values.push(serde_json::from_str(&body).map_err(|e| sqlx::Error::Decode(Box::new(e)))?);
        }
        let lsn: String =
            sqlx::query_scalar("SELECT applied_lsn FROM journal_cursor WHERE singleton=1")
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
    ) -> Result<Vec<(String, String, i64)>, sqlx::Error> {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT json_extract(body,'$.workflow_id') AS workflow,json_extract(body,'$.status') AS status,COUNT(*) AS count FROM journal_entities WHERE kind='run'",
        );
        if let Some(id) = workflow_id {
            query
                .push(" AND json_extract(body,'$.workflow_id')=")
                .push_bind(id);
        }
        query.push(" GROUP BY json_extract(body,'$.workflow_id'),json_extract(body,'$.status')");
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
    use flow_test_support::io::TempDir;
    use serde_json::json;

    fn row(id: &str, timestamp: &str, workflow: &str, status: &str) -> ProjectedRow {
        ProjectedRow {
            kind: "run".into(),
            key: id.into(),
            value: Some(json!({
                "run_id": id, "created_at": timestamp, "workflow_id": workflow,
                "status": status, "source": "manual"
            })),
        }
    }
    fn ids(values: Vec<Value>) -> Vec<String> {
        values
            .into_iter()
            .map(|v| v["run_id"].as_str().unwrap().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn keyset_pages_normalize_time_and_push_filters_before_limit() {
        let dir = TempDir::new("projection-pages");
        let p = Projector::open(&dir.join("view.db"), "j").await.unwrap();
        p.apply(
            "j",
            1,
            vec![
                row("a", "2026-01-01T00:00:00Z", "w", "completed"),
                row("b", "2026-01-01T00:00:00.000000001Z", "w", "completed"),
                row("c", "2026-01-01T08:00:00.000000001+08:00", "w", "completed"),
                row("d", "2026-01-02T00:00:00Z", "other", "completed"),
                row("e", "2026-01-03T00:00:00Z", "w", "failed"),
            ],
        )
        .await
        .unwrap();
        let query = RunsQuery {
            workflow_id: Some("w"),
            status: Some("completed"),
            source: Some("manual"),
            limit: 2,
            ..Default::default()
        };
        let (lsn, page) = p.runs_page(query).await.unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(ids(page), ["c", "b"]);
        p.apply(
            "j",
            2,
            vec![row("new", "2026-01-04T00:00:00Z", "w", "completed")],
        )
        .await
        .unwrap();
        let (_, page) = p
            .runs_page(RunsQuery {
                workflow_id: Some("w"),
                status: Some("completed"),
                before_run_id: Some("b"),
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(ids(page), ["a"]);
        assert_eq!(
            p.run_counts(Some("w"))
                .await
                .unwrap()
                .iter()
                .map(|r| r.2)
                .sum::<i64>(),
            5
        );
        assert!(
            p.runs_page(RunsQuery::default())
                .await
                .unwrap()
                .1
                .is_empty()
        );
        let plan=sqlx::query("EXPLAIN QUERY PLAN SELECT key FROM journal_entities WHERE kind='run' AND json_extract(body,'$.workflow_id')='w' AND (created_order,key)<('2026','b') ORDER BY created_order DESC,key DESC LIMIT 2").fetch_all(&p.pool).await.unwrap();
        let plan = plan
            .iter()
            .map(|r| r.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(plan.contains("journal_workflow_order_v2"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        p.close().await;
    }

    #[tokio::test]
    async fn only_page_bodies_are_loaded_and_aggregate_response_is_bounded() {
        let dir = TempDir::new("projection-budget");
        let p = Projector::open(&dir.join("view.db"), "j").await.unwrap();
        let mut large = row("old", "2026-01-01T00:00:00Z", "w", "completed");
        large.value.as_mut().unwrap()["padding"] = json!("x".repeat(8 * 1024 * 1024));
        p.apply(
            "j",
            1,
            vec![large, row("new", "2026-01-02T00:00:00Z", "w", "completed")],
        )
        .await
        .unwrap();
        assert_eq!(
            ids(p
                .runs_page(RunsQuery {
                    limit: 1,
                    ..Default::default()
                })
                .await
                .unwrap()
                .1),
            ["new"]
        );
        assert!(
            p.runs_page(RunsQuery {
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("RESPONSE_TOO_LARGE")
        );
        assert_eq!(p.run_counts(None).await.unwrap()[0].2, 2);
        p.close().await;
    }

    #[tokio::test]
    async fn old_projection_is_migrated_without_replaying_journal() {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        let dir = TempDir::new("projection-migration");
        let path = dir.join("view.db");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query("CREATE TABLE journal_entities(kind TEXT NOT NULL,key TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(kind,key))").execute(&pool).await.unwrap();
        let old = row("old", "2026-01-01T00:00:00Z", "w", "completed");
        sqlx::query("INSERT INTO journal_entities VALUES('run','old',?)")
            .bind(old.value.unwrap().to_string())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let p = Projector::open(&path, "j").await.unwrap();
        assert_eq!(
            ids(p
                .runs_page(RunsQuery {
                    limit: 1,
                    ..Default::default()
                })
                .await
                .unwrap()
                .1),
            ["old"]
        );
        let normalized: String =
            sqlx::query_scalar("SELECT created_order FROM journal_entities WHERE key='old'")
                .fetch_one(&p.pool)
                .await
                .unwrap();
        assert_eq!(normalized, "2026-01-01T00:00:00.000000000Z");
        p.close().await;
    }

    #[tokio::test]
    async fn opaque_entities_do_not_acquire_timestamp_validation() {
        let dir = TempDir::new("projection-opaque");
        let p = Projector::open(&dir.join("view.db"), "j").await.unwrap();
        let value = json!({"created_at":"business data, not a timestamp"});
        p.apply(
            "j",
            1,
            vec![ProjectedRow {
                kind: "legacy".into(),
                key: "row".into(),
                value: Some(value.clone()),
            }],
        )
        .await
        .unwrap();
        assert_eq!(p.get("legacy", "row").await.unwrap().1, Some(value));
        p.close().await;
    }
    #[tokio::test]
    async fn split_run_nodes_are_atomic_and_summary_reads_skip_the_ledger() {
        let dir = TempDir::new("projection-split");
        let p = Projector::open(&dir.join("view.db"), "j").await.unwrap();
        let run_id = "run/with quotes\"";
        let node = "node/\"";
        let mut summary = row(run_id, "2026-01-01T00:00:00Z", "w", "running");
        summary.value.as_mut().unwrap()["_node_layout"] = json!("split-v1");
        summary.value.as_mut().unwrap()["nodes"] = json!({});
        let node_row = |state| ProjectedRow {
            kind: "run_node".into(),
            key: serde_json::to_string(&(run_id, node)).unwrap(),
            value: Some(json!({"status":state,"attempts":{},"_attempt_layout":"split-v1"})),
        };
        p.apply("j", 1, vec![summary, node_row("running")])
            .await
            .unwrap();
        assert!(!p.requires_split_runs().await.unwrap());
        assert_eq!(
            p.get_summary("run", run_id).await.unwrap().1.unwrap()["nodes"],
            json!({})
        );
        let (lsn, full) = p.get("run", run_id).await.unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(full.unwrap()["nodes"][node]["status"], "running");
        // A later invalid row must roll back the preceding node update and cursor.
        let invalid = ProjectedRow {
            kind: "run_node".into(),
            key: "not-json".into(),
            value: Some(json!({"status":"running"})),
        };
        assert!(
            p.apply("j", 2, vec![node_row("succeeded"), invalid])
                .await
                .is_err()
        );
        let (lsn, full) = p.get("run", run_id).await.unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(full.unwrap()["nodes"][node]["status"], "running");
        p.apply("j", 2, vec![node_row("succeeded")]).await.unwrap();
        assert_eq!(
            p.get("run", run_id).await.unwrap().1.unwrap()["nodes"][node]["status"],
            "succeeded"
        );
        p.close().await;
    }
    #[tokio::test]
    async fn historical_reads_obey_prefix_and_reject_corrupt_derived_rows() {
        let dir = TempDir::new("history-prefix");
        let p = Projector::open(&dir.join("index.db"), "j").await.unwrap();
        let row = |lsn: u64, value: Value| {
            let digest = hex::encode(Sha256::digest(value.to_string().as_bytes()));
            ProjectedRow {
                kind: "history".into(),
                key: serde_json::to_string(&("run", "r", format!("{lsn:020}"), digest)).unwrap(),
                value: Some(value),
            }
        };
        p.apply("j", 1, vec![row(1, json!({"state":"before"}))])
            .await
            .unwrap();
        p.apply("j", 2, vec![row(2, json!({"state":"after"}))])
            .await
            .unwrap();
        assert_eq!(
            p.history_at("run", "r", 1).await.unwrap(),
            Some(json!({"state":"before"}))
        );
        assert_eq!(
            p.history_at("run", "r", 2).await.unwrap(),
            Some(json!({"state":"after"}))
        );
        assert_eq!(
            p.history_changes(1).await.unwrap()[0].value,
            Some(json!({"state":"before"}))
        );
        sqlx::query("UPDATE journal_entities SET body='{}' WHERE kind='history' AND json_extract(key,'$[2]')=?").bind(format!("{:020}",2)).execute(&p.pool).await.unwrap();
        assert!(
            p.history_at("run", "r", 2)
                .await
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        assert!(p.history_changes(2).await.is_err());
        assert!(p.history_at("run", "r", 1).await.is_ok());
        p.close().await;
    }
}
