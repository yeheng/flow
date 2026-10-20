//! One SQLite authority: accepted facts and current entities share the same commit.
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD, Engine};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::codec::{self, Event, EventKind, Transaction};
use crate::storage::{private_dir, sync_dir, FaultInjection, Location, SyncLatency};
use crate::value::{Chunk, ValueRef};
use crate::{invalid, Error, Result, MAX_VALUE_BYTES};

pub const DATABASE: &str = "flow.sqlite";
pub const FORMAT: i64 = 4;
pub const MAX_ENTITY_BYTES: usize = 2 * 1024 * 1024;

/// A current object, not a historical copy of every revision. None is a deletion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityChange {
    pub kind: String,
    pub key: String,
    pub value: Option<Value>,
}

pub(crate) fn db(error: rusqlite::Error) -> Error {
    // 约束违例（UNIQUE tx_id / CHECK event_count 等）意味着被拒的是这笔
    // 事务的内容，不是存储平台故障——按 Invalid 分类，不冻结唯一写者
    // （writer 的失败分类只放行 Invalid/Limit/Conflict）。误冻结会把一条
    // 坏输入升级成「权威不可用直到进程重启」。
    if matches!(
        error,
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ErrorCode::ConstraintViolation
    ) {
        return Error::Invalid(format!("SQLite constraint violation: {error}"));
    }
    Error::Unavailable(format!("SQLite: {error}"))
}

/// Bind checksums to identity as well as content, including arbitrary user keys.
pub fn entity_digest(kind: &str, key: &str, body: &str) -> String {
    let mut hash = Sha256::new();
    for bytes in [kind.as_bytes(), key.as_bytes(), body.as_bytes()] {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    hex::encode(hash.finalize())
}

pub fn read_connection(root: &Path) -> Result<Connection> {
    let path = root.join(DATABASE);
    if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
        return Err(invalid("symlink database"));
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(db)?;
    conn.busy_timeout(Duration::from_secs(5)).map_err(db)?;
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(db)?;
    if version != FORMAT {
        return Err(invalid("unsupported SQLite storage format; use flow-journal-tool upgrade <source> <new-directory> for format 3"));
    }
    Ok(conn)
}

pub fn metadata(conn: &Connection) -> Result<(String, u64)> {
    let (identity, lsn): (String, String) = conn
        .query_row(
            "SELECT journal_id,committed_lsn FROM store_meta WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(db)?;
    let lsn = lsn
        .parse()
        .map_err(|_| invalid("invalid SQLite commit sequence"))?;
    Ok((identity, lsn))
}

pub(crate) fn entity(conn: &Connection, kind: &str, key: &str) -> Result<Option<Value>> {
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT body,digest FROM current_entities WHERE kind=? AND key=?",
            params![kind, key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(db)?;
    row.map(|(body, digest)| {
        if entity_digest(kind, key, &body) != digest {
            return Err(invalid("current entity checksum mismatch"));
        }
        Ok(serde_json::from_str(&body)?)
    })
    .transpose()
}

fn created_order(kind: &str, value: &Value) -> String {
    if !matches!(
        kind,
        "run" | "workflow" | "schedule" | "webhook" | "template"
    ) {
        return String::new();
    }
    value["created_at"]
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|ts| {
            ts.with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        })
        .unwrap_or_default()
}

/// Decode the canonical key tuple once at write time into typed columns. Reads then
/// use plain indexed columns instead of `json_extract(key, ...)`.
fn key_parts(kind: &str, key: &str) -> (String, String, String) {
    let parts: Vec<String> = serde_json::from_str(key).unwrap_or_default();
    let get = |index: usize| parts.get(index).cloned().unwrap_or_default();
    match kind {
        "run_node" | "command" => (get(0), get(1), String::new()),
        "run_attempt" | "child" | "audit" => (get(0), get(1), get(2)),
        "workflow_version" => (get(0), get(1), String::new()),
        _ => (String::new(), String::new(), String::new()),
    }
}

pub(crate) fn apply_entity(conn: &Connection, lsn: u64, row: &EntityChange) -> Result<()> {
    if let Some(value) = &row.value {
        let body =
            String::from_utf8(codec::bounded_json(value, MAX_ENTITY_BYTES)?).expect("JSON UTF-8");
        // Receipts are immutable. A conflicting duplicate is an error, never an overwrite.
        if matches!(row.kind.as_str(), "command" | "audit") {
            if let Some(previous) = entity(conn, &row.kind, &row.key)? {
                if previous != *value {
                    return Err(Error::Conflict("conflicting permanent receipt".into()));
                }
                return Ok(());
            }
        }
        let (parent, slot, extra) = key_parts(&row.kind, &row.key);
        conn.execute("INSERT INTO current_entities(kind,key,parent,slot,extra,body,digest,created_order,last_lsn) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET parent=excluded.parent,slot=excluded.slot,extra=excluded.extra,body=excluded.body,digest=excluded.digest,created_order=excluded.created_order,last_lsn=excluded.last_lsn",
            params![row.kind,row.key,parent,slot,extra,body,entity_digest(&row.kind,&row.key,&body),created_order(&row.kind,value),format!("{lsn:020}")]).map_err(db)?;
    } else {
        conn.execute(
            "DELETE FROM current_entities WHERE kind=? AND key=?",
            params![row.kind, row.key],
        )
        .map_err(db)?;
    }
    Ok(())
}

/// Reconstruct a bounded accepted transaction; raw chunk bytes exist only in value_chunks.
pub fn transaction(conn: &Connection, identity: &str, lsn: u64) -> Result<Transaction> {
    transaction_with_size(conn, identity, lsn).map(|(tx, _)| tx)
}

/// Same reconstruction, plus the recorded encoded size used by tail/subscription budgets.
pub fn transaction_with_size(
    conn: &Connection,
    identity: &str,
    lsn: u64,
) -> Result<(Transaction, u64)> {
    let (tx_id, count, digest, recorded_bytes): (String, i64, String, i64) = conn
        .query_row(
            "SELECT tx_id,event_count,digest,bytes FROM transactions WHERE lsn=?",
            [format!("{lsn:020}")],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(db)?;
    // rusqlite 0.38 dropped u64/usize FromSql/ToSql; SQLite integers are i64.
    let count: usize =
        usize::try_from(count).map_err(|_| invalid("invalid stored transaction metadata"))?;
    let recorded_bytes = u64::try_from(recorded_bytes)
        .map_err(|_| invalid("invalid stored transaction metadata"))?;
    let mut statement = conn
        .prepare("SELECT event_index,body,run_id,dispatch_id,run_event,audit_event FROM events WHERE lsn=? ORDER BY event_index")
        .map_err(db)?;
    let mut rows = statement.query([format!("{lsn:020}")]).map_err(db)?;
    let mut events = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.next().map_err(db)? {
        let index: i64 = row.get(0).map_err(db)?;
        let body: String = row.get(1).map_err(db)?;
        bytes = bytes.saturating_add(body.len());
        if index != events.len() as i64 || bytes > crate::MAX_LINE_BYTES {
            return Err(invalid("invalid stored transaction size/sequence"));
        }
        let mut event: Event = serde_json::from_str(&body)?;
        let run_id: Option<String> = row.get(2).map_err(db)?;
        let dispatch_id: Option<String> = row.get(3).map_err(db)?;
        let run_event: bool = row.get(4).map_err(db)?;
        let audit_event: bool = row.get(5).map_err(db)?;
        if run_id != event.run_id
            || dispatch_id != event.dispatch_id
            || run_event != (event.run_seq > 0)
            || audit_event != event.is_audit()
        {
            return Err(invalid("accepted event index identity mismatch"));
        }
        if event.kind == EventKind::ValueChunk {
            let (output, indexed_chunk, raw, hash): (String, i64, Vec<u8>, String) = conn
                .query_row(
                    "SELECT output_id,chunk_index,bytes,digest FROM value_chunks WHERE lsn=? AND event_index=?",
                    params![format!("{lsn:020}"), index],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .map_err(db)?;
            let indexed_chunk =
                u64::try_from(indexed_chunk).map_err(|_| invalid("invalid stored chunk index"))?;
            if Some(output.as_str()) != event.payload["output_id"].as_str()
                || Some(indexed_chunk)
                    != event.payload["chunk_index"]
                        .as_str()
                        .and_then(|s| s.parse::<u64>().ok())
                || raw.is_empty()
                || raw.len() > crate::CHUNK_BYTES
                || codec::digest(&raw) != hash
            {
                return Err(invalid("stored chunk checksum mismatch"));
            }
            let encoded = STANDARD.encode(raw);
            // The stored null (4 bytes) becomes a quoted base64 string (n+2 bytes).
            bytes = bytes.saturating_add(encoded.len().saturating_sub(2));
            if bytes > crate::MAX_LINE_BYTES {
                return Err(invalid("invalid stored transaction size"));
            }
            event.payload["data"] = Value::String(encoded);
        }
        events.push(event);
    }
    let tx = Transaction {
        v: 2,
        journal_id: identity.into(),
        lsn,
        tx_id,
        events,
    };
    let body = codec::bounded_json(&tx, crate::MAX_LINE_BYTES)?;
    if tx.events.len() != count
        || body.len() as u64 != recorded_bytes
        || codec::digest(&body) != digest
    {
        return Err(invalid("accepted transaction checksum/size mismatch"));
    }
    tx.validate()?;
    Ok((tx, recorded_bytes))
}

pub(crate) struct Disk {
    pub conn: Connection,
    pub id: String,
    pub last_lsn: u64,
    pub data_syncs: u64,
    pub data_latency: SyncLatency,
    pub directory_latency: SyncLatency,
    faults: FaultInjection,
    written: u64,
    before_batch: u64,
    _lock: File,
}

impl Disk {
    pub fn open(root: &Path, faults: FaultInjection) -> Result<Self> {
        private_dir(root)?;
        let lock = File::open(root)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|e| Error::Conflict(format!("data directory is locked: {e}")))?;
        let path = root.join(DATABASE);
        let fresh = !path.exists();
        if fresh && std::fs::read_dir(root)?.next().is_some() {
            return Err(invalid(
                "new SQLite storage requires an empty data directory; old formats are not imported",
            ));
        }
        if std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(invalid("symlink database"));
        }
        if fresh {
            drop(crate::storage::private_file(&path, true)?);
        }
        let mut conn = Connection::open(&path).map_err(db)?;
        conn.busy_timeout(Duration::from_secs(5)).map_err(db)?;
        let mut initialize = fresh;
        if !fresh {
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .map_err(db)?;
            if version == 0 {
                // A crash before the schema transaction commits leaves an empty SQLite
                // file. It has no accepted facts; resume only this bootstrap state.
                let tables: i64 = conn
                    .query_row("SELECT COUNT(*) FROM sqlite_schema", [], |r| r.get(0))
                    .map_err(db)?;
                if tables != 0 {
                    return Err(invalid("unsupported SQLite storage format"));
                }
                initialize = true;
            } else if version != FORMAT {
                return Err(invalid("unsupported SQLite storage format; format 3 can be copied with flow-journal-tool upgrade <source> <new-directory>"));
            } else {
                metadata(&conn)?;
            }
        }
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA foreign_keys=ON; PRAGMA temp_store=FILE;").map_err(db)?;
        if initialize {
            if faults.fail_dir_sync == Some(1) {
                return Err(std::io::Error::other("injected directory sync failure").into());
            }
            let tx = conn.transaction().map_err(db)?;
            tx.execute_batch(include_str!("schema.sql")).map_err(db)?;
            tx.execute(
                "INSERT INTO store_meta(singleton,journal_id,committed_lsn) VALUES(1,?,'0')",
                [uuid::Uuid::now_v7().to_string()],
            )
            .map_err(db)?;
            tx.commit().map_err(db)?;
            sync_dir(root)?;
        }
        let (id, last_lsn) = metadata(&conn)?;
        let last: Option<String> = conn
            .query_row("SELECT MAX(lsn) FROM transactions", [], |r| r.get(0))
            .map_err(db)?;
        if last != ((last_lsn > 0).then(|| format!("{last_lsn:020}"))) {
            return Err(invalid("SQLite commit watermark mismatch"));
        }
        Ok(Self {
            conn,
            id,
            last_lsn,
            data_syncs: 0,
            data_latency: SyncLatency::default(),
            directory_latency: SyncLatency::default(),
            faults,
            written: 0,
            before_batch: last_lsn,
            _lock: lock,
        })
    }
    pub fn begin(&mut self) -> Result<()> {
        self.before_batch = self.last_lsn;
        self.conn.execute_batch("BEGIN IMMEDIATE").map_err(db)
    }
    /// `bytes`/`digest` are the canonical encoded transaction computed once by the
    /// writer, never recomputed here. `changes` are the derived current rows.
    pub fn append(
        &mut self,
        tx: &Transaction,
        changes: &[EntityChange],
        prepared: bool,
        bytes: u64,
        digest: &str,
    ) -> Result<Location> {
        let domain: bool = self
            .conn
            .query_row(
                "SELECT domain_mode FROM store_meta WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        if domain && !prepared {
            return Err(invalid("domain database requires a prepared transition"));
        }
        if prepared {
            if !domain && self.last_lsn > 0 {
                return Err(invalid("cannot mix fact-only and domain databases"));
            }
            self.conn
                .execute("UPDATE store_meta SET domain_mode=1 WHERE singleton=1", [])
                .map_err(db)?;
        }
        if self
            .faults
            .fail_after_bytes
            .is_some_and(|n| self.written.saturating_add(bytes) > n)
        {
            return Err(std::io::Error::other("injected SQLite write failure").into());
        }
        self.conn
            .execute(
                "INSERT INTO transactions VALUES(?,?,?,?,?)",
                params![
                    format!("{:020}", tx.lsn),
                    tx.tx_id,
                    tx.events.len() as i64,
                    digest,
                    bytes as i64
                ],
            )
            .map_err(db)?;
        for (index, event) in tx.events.iter().enumerate() {
            let mut stored = event.clone();
            let chunk = if event.kind == EventKind::ValueChunk {
                // 载荷形状错误是调用方输入问题（Invalid 不冻结），不是平台
                // 故障——Err(Json) 会被 writer 当存储故障冻结唯一写者。
                let chunk: Chunk = serde_json::from_value(event.payload.clone())
                    .map_err(|e| invalid(format!("malformed value chunk payload: {e}")))?;
                let raw = chunk.decode()?;
                stored.payload["data"] = Value::Null;
                Some((chunk, raw))
            } else {
                None
            };
            self.conn
                .execute(
                    "INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                    params![
                        format!("{:020}", tx.lsn),
                        index as i64,
                        event.run_id,
                        event.dispatch_id,
                        event.run_seq > 0,
                        event.is_audit(),
                        serde_json::to_string(&stored)?
                    ],
                )
                .map_err(db)?;
            if let Some((chunk, raw)) = chunk {
                self.conn
                    .execute(
                        "INSERT INTO value_chunks VALUES(?,?,?,?,?,?)",
                        params![
                            chunk.output_id,
                            chunk.chunk_index as i64,
                            format!("{:020}", tx.lsn),
                            index as i64,
                            raw,
                            codec::digest(&raw)
                        ],
                    )
                    .map_err(db)?;
            }
            if matches!(
                event.kind,
                EventKind::ValueChunk | EventKind::ValuePublished
            ) {
                self.apply_value(event, tx.lsn)?;
            }
        }
        for change in changes {
            apply_entity(&self.conn, tx.lsn, change)?;
        }
        self.conn
            .execute(
                "UPDATE store_meta SET committed_lsn=? WHERE singleton=1",
                [tx.lsn.to_string()],
            )
            .map_err(db)?;
        self.written += bytes;
        self.last_lsn = tx.lsn;
        Ok(Location { bytes, lsn: tx.lsn })
    }
    /// Value validation happens once, here, while the bytes are in hand. The reducer
    /// only keeps verified publication descriptors; it never re-hashes a stream.
    fn apply_value(&self, event: &Event, lsn: u64) -> Result<()> {
        let id = event.payload["output_id"]
            .as_str()
            .ok_or_else(|| invalid("missing value identity"))?;
        let published: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM current_entities WHERE kind='published' AND key=?)",
                [id],
                |r| r.get(0),
            )
            .map_err(db)?;
        if published {
            return Err(invalid("value chunk/publication after publication"));
        }
        if event.kind == EventKind::ValueChunk {
            // The current row is already inserted, so its index must be the last one.
            let chunk: Chunk = serde_json::from_value(event.payload.clone())
                .map_err(|e| invalid(format!("malformed value chunk payload: {e}")))?;
            chunk.decode()?;
            let (count, total): (i64, i64) = self
                .conn
                .query_row(
                    "SELECT COUNT(*),COALESCE(SUM(length(bytes)),0) FROM value_chunks WHERE output_id=?",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(db)?;
            let (count, total) = (count as u64, total as u64);
            if chunk.chunk_index.checked_add(1) != Some(count) || total > MAX_VALUE_BYTES {
                return Err(invalid("value chunk gap/duplicate/limit"));
            }
            return Ok(());
        }
        let value: ValueRef = serde_json::from_value(event.payload.clone())
            .map_err(|e| invalid(format!("malformed value ref payload: {e}")))?;
        value.validate()?;
        if value.journal_id != self.id {
            return Err(invalid("published value belongs to another journal"));
        }
        let (count, total): (i64, i64) = self
            .conn
            .query_row(
                "SELECT COUNT(*),COALESCE(SUM(length(bytes)),0) FROM value_chunks WHERE output_id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(db)?;
        let (count, total) = (count as u64, total as u64);
        if value.chunk_count != count || value.total_bytes != total {
            return Err(invalid("publication requires complete matching chunks"));
        }
        let mut hash = Sha256::new();
        let mut statement = self
            .conn
            .prepare(
                "SELECT chunk_index,bytes FROM value_chunks WHERE output_id=? ORDER BY chunk_index",
            )
            .map_err(db)?;
        let mut rows = statement.query([id]).map_err(db)?;
        let mut expected = 0i64;
        while let Some(row) = rows.next().map_err(db)? {
            let index: i64 = row.get(0).map_err(db)?;
            let bytes: Vec<u8> = row.get(1).map_err(db)?;
            if index != expected {
                return Err(invalid("non-contiguous stored value chunk"));
            }
            hash.update(&bytes);
            expected += 1;
        }
        if hex::encode(hash.finalize()) != value.digest {
            return Err(invalid("publication digest mismatch"));
        }
        apply_entity(
            &self.conn,
            lsn,
            &EntityChange {
                kind: "published".into(),
                key: id.into(),
                value: Some(serde_json::json!({"lsn":lsn.to_string(),"value":event.payload})),
            },
        )?;
        Ok(())
    }
    pub fn sync(&mut self) -> Result<()> {
        self.data_syncs += 1;
        if self.faults.sync_delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.faults.sync_delay_ms));
        }
        if self.faults.fail_data_sync == Some(self.data_syncs) {
            return Err(std::io::Error::other("injected SQLite commit failure").into());
        }
        let start = Instant::now();
        self.conn.execute_batch("COMMIT").map_err(db)?;
        self.data_latency.record(start.elapsed());
        Ok(())
    }
    pub fn rollback(&mut self) {
        let _ = self.conn.execute_batch("ROLLBACK");
        self.last_lsn = self.before_batch;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sqlite_full_rolls_back_facts_entities_and_commit_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut disk = Disk::open(dir.path(), FaultInjection::default()).unwrap();
        let tx = Transaction {
            v: 2,
            journal_id: disk.id.clone(),
            lsn: 1,
            tx_id: "confirmed".into(),
            events: vec![Event::new(EventKind::Command, serde_json::json!({}))],
        };
        disk.begin().unwrap();
        let body = codec::bounded_json(&tx, crate::MAX_LINE_BYTES).unwrap();
        disk.append(
            &tx,
            &[EntityChange {
                kind: "run".into(),
                key: "r".into(),
                value: Some(serde_json::json!({"status":"before"})),
            }],
            true,
            body.len() as u64,
            &codec::digest(&body),
        )
        .unwrap();
        disk.sync().unwrap();
        let pages: i64 = disk
            .conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        disk.conn
            .pragma_update(None, "max_page_count", pages)
            .unwrap();
        let mut candidate = tx.clone();
        candidate.lsn = 2;
        candidate.tx_id = "too-large".into();
        candidate.events[0].payload = Value::String("x".repeat(512 * 1024));
        disk.begin().unwrap();
        let body = codec::bounded_json(&candidate, crate::MAX_LINE_BYTES).unwrap();
        let error = disk
            .append(
                &candidate,
                &[],
                true,
                body.len() as u64,
                &codec::digest(&body),
            )
            .unwrap_err();
        assert!(error.to_string().contains("full"), "{error}");
        disk.rollback();
        drop(disk);
        let disk = Disk::open(dir.path(), FaultInjection::default()).unwrap();
        assert_eq!(disk.last_lsn, 1);
        assert_eq!(
            entity(&disk.conn, "run", "r").unwrap().unwrap()["status"],
            "before"
        );
        let count: i64 = disk
            .conn
            .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
