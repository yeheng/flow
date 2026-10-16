//! Disposable value locations, incrementally indexed at verified journal boundaries.
use crate::codec::{EventKind, Transaction};
use crate::storage::{Location, private_dir};
use crate::tail::TailReader;
use crate::{Result, invalid};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{BTreeSet, VecDeque};
use std::path::Path;

fn db(error: rusqlite::Error) -> crate::Error {
    invalid(format!("value address cache: {error}"))
}
fn open(root: &Path) -> Result<Connection> {
    let dir = root.join("indexes/value-addresses");
    private_dir(&dir)?;
    let conn = Connection::open(dir.join("locations.sqlite")).map_err(db)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(db)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS cursor(singleton INTEGER PRIMARY KEY CHECK(singleton=1),journal_id TEXT NOT NULL,location TEXT NOT NULL,digest TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS locations(output_id TEXT NOT NULL,lsn TEXT NOT NULL,location TEXT NOT NULL,PRIMARY KEY(output_id,lsn));").map_err(db)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            dir.join("locations.sqlite"),
            std::fs::Permissions::from_mode(0o600),
        )?;
    }
    Ok(conn)
}
fn append(conn: &Connection, fact: &Transaction, location: &Location) -> Result<()> {
    let ids: BTreeSet<_> = fact
        .events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ValueChunk | EventKind::ValuePublished))
        .filter_map(|e| e.payload["output_id"].as_str())
        .collect();
    let encoded = serde_json::to_string(location)?;
    for id in ids {
        conn.execute(
            "INSERT OR REPLACE INTO locations VALUES(?,?,?)",
            params![id, format!("{:020}", fact.lsn), encoded],
        )
        .map_err(db)?;
    }
    Ok(())
}
fn cursor(conn: &Connection, root: &Path) -> Result<Option<(String, Location)>> {
    let row: Option<(String, String, String)> = conn
        .query_row(
            "SELECT journal_id,location,digest FROM cursor WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(db)?;
    row.map(|(id, encoded, digest)| {
        let location: Location = serde_json::from_str(&encoded)?;
        let bytes = crate::maintenance::read_location(root, &location)?;
        if crate::codec::digest(&bytes) != digest || crate::codec::decode(&bytes)?.journal_id != id
        {
            return Err(invalid("value cache boundary mismatch"));
        }
        Ok((id, location))
    })
    .transpose()
}
fn save_cursor(conn: &Connection, root: &Path, id: &str, location: &Location) -> Result<()> {
    let digest = crate::codec::digest(&crate::maintenance::read_location(root, location)?);
    conn.execute(
        "INSERT OR REPLACE INTO cursor VALUES(1,?,?,?)",
        params![id, serde_json::to_string(location)?, digest],
    )
    .map_err(db)?;
    Ok(())
}
fn advance(conn: &mut Connection, root: &Path, upper: u64, expected: &str) -> Result<()> {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(db)?;
    let mut reader = match cursor(&tx, root).ok().flatten() {
        Some((id, location)) if id == expected => {
            if location.lsn >= upper {
                return Ok(());
            }
            TailReader::after(root, expected, &location)?
        }
        _ => {
            tx.execute_batch("DELETE FROM locations; DELETE FROM cursor;")
                .map_err(db)?;
            TailReader::new(root, expected)
        }
    };
    let mut last = None;
    while let Some((fact, location)) = reader.next(upper)? {
        append(&tx, &fact, &location)?;
        last = Some(location);
    }
    if let Some(location) = last {
        save_cursor(&tx, root, expected, &location)?;
    }
    tx.commit().map_err(db)
}
pub(crate) fn initialize(root: &Path, upper: u64, journal_id: &str) -> Result<()> {
    advance(&mut open(root)?, root, upper, journal_id)
}
/// Runs only after journal sync. Failure never changes the authoritative commit.
pub(crate) fn committed(root: &Path, commits: &[crate::Commit]) {
    if let Some(last) = commits.last() {
        let _ = initialize(root, last.transaction.lsn, &last.transaction.journal_id);
    }
}
pub(crate) fn locations(root: &Path, upper: u64, journal_id: &str, id: &str) -> Result<Locations> {
    let mut conn = open(root)?;
    // The client's descriptor cannot establish the cache's dataset identity.
    TailReader::new(root, journal_id).next(1)?;
    advance(&mut conn, root, upper, journal_id)?;
    conn.execute_batch("BEGIN").map_err(db)?;
    Ok(Locations {
        conn,
        id: id.into(),
        upper: format!("{upper:020}"),
        after: String::new(),
        pending: VecDeque::new(),
        done: false,
    })
}
pub(crate) struct Locations {
    conn: Connection,
    id: String,
    upper: String,
    after: String,
    pending: VecDeque<Location>,
    done: bool,
}
impl Iterator for Locations {
    type Item = Result<Location>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(location) = self.pending.pop_front() {
            return Some(Ok(location));
        }
        if self.done {
            return None;
        }
        let result = (|| -> Result<()> {
            let mut statement = self.conn.prepare("SELECT lsn,location FROM locations WHERE output_id=? AND lsn>? AND lsn<=? ORDER BY lsn LIMIT 128").map_err(db)?;
            let rows = statement
                .query_map(params![self.id, self.after, self.upper], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(db)?;
            for row in rows {
                let (lsn, encoded) = row.map_err(db)?;
                let location: Location = serde_json::from_str(&encoded)?;
                if lsn != format!("{:020}", location.lsn) {
                    return Err(invalid("value location LSN mismatch"));
                }
                self.after = lsn;
                self.pending.push_back(location);
            }
            self.done = self.pending.is_empty();
            Ok(())
        })();
        if let Err(error) = result {
            self.done = true;
            return Some(Err(error));
        }
        self.pending.pop_front().map(Ok)
    }
}
