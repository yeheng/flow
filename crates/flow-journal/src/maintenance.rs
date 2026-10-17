//! SQLite verification, consistent backups and explicit offline maintenance.
use crate::storage::{private_dir, private_file, sync_dir, Recovery};
use crate::{invalid, Error, Result};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("cache path without parent"))?;
    let temp = parent.join(format!(".{}.tmp", uuid::Uuid::now_v7()));
    let mut file = private_file(&temp, true)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    sync_dir(parent)
}

/// Full database structure, application checksums and published-value verification.
pub fn verify(root: &Path) -> Result<Recovery> {
    let conn = crate::sqlite::read_connection(root)?;
    conn.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .map_err(crate::sqlite::db)?;
    if result != "ok" {
        return Err(invalid(format!("SQLite integrity check: {result}")));
    }
    let foreign: bool = conn
        .prepare("PRAGMA foreign_key_check")
        .map_err(crate::sqlite::db)?
        .exists([])
        .map_err(crate::sqlite::db)?;
    if foreign {
        return Err(invalid("SQLite foreign key violation"));
    }
    let (identity, upper) = crate::sqlite::metadata(&conn)?;
    let count: u64 = conn
        .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
        .map_err(crate::sqlite::db)?;
    if count != upper {
        return Err(invalid("SQLite fact count and commit watermark mismatch"));
    }
    let mut report = Recovery {
        journal_id: identity,
        last_lsn: 0,
        ..Default::default()
    };
    for lsn in 1..=upper {
        match crate::sqlite::transaction(&conn, &report.journal_id, lsn) {
            Ok(_) => report.last_lsn = lsn,
            Err(error) => {
                report.fault = Some(crate::ScanFault {
                    lsn,
                    reason: error.to_string(),
                });
                return Ok(report);
            }
        }
    }
    let mut rows = conn
        .prepare("SELECT kind,key,body,digest FROM current_entities")
        .map_err(crate::sqlite::db)?;
    let mut rows = rows.query([]).map_err(crate::sqlite::db)?;
    while let Some(row) = rows.next().map_err(crate::sqlite::db)? {
        let kind: String = row.get(0).map_err(crate::sqlite::db)?;
        let key: String = row.get(1).map_err(crate::sqlite::db)?;
        let body: String = row.get(2).map_err(crate::sqlite::db)?;
        let digest: String = row.get(3).map_err(crate::sqlite::db)?;
        if crate::sqlite::entity_digest(&kind, &key, &body) != digest {
            return Err(invalid("current entity checksum mismatch"));
        }
    }
    let mut publications = conn
        .prepare("SELECT body FROM current_entities WHERE kind='published'")
        .map_err(crate::sqlite::db)?;
    let mut publications = publications.query([]).map_err(crate::sqlite::db)?;
    while let Some(row) = publications.next().map_err(crate::sqlite::db)? {
        let body: String = row.get(0).map_err(crate::sqlite::db)?;
        let record: serde_json::Value = serde_json::from_str(&body)?;
        let value = serde_json::from_value(record["value"].clone())?;
        crate::value::read_value(root, upper, &value, true, &mut std::io::sink())?;
    }
    Ok(report)
}

/// Back up one SQLite read snapshot, including state, receipts, facts and BLOBs.
/// A requested old prefix is rejected: its corresponding current state is not retained.
/// u64::MAX selects the current snapshot. No destination is overwritten.
pub fn backup(root: &Path, destination: &Path, upper_lsn: u64) -> Result<Recovery> {
    if fs::symlink_metadata(destination).is_ok() {
        return Err(Error::Conflict("backup destination already exists".into()));
    }
    let source = crate::sqlite::read_connection(root)?;
    source.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let (journal_id, last_lsn) = crate::sqlite::metadata(&source)?;
    if upper_lsn != u64::MAX && upper_lsn != last_lsn {
        return Err(Error::Conflict(
            "backup boundary changed or is historical; select a current snapshot".into(),
        ));
    }
    private_dir(destination)?;
    // Publish only a fully verified standalone file. Failed copies remain staging
    // evidence, and cannot be opened as a fresh coordinator database.
    let staging = destination.join(format!(".backup-{}", uuid::Uuid::now_v7()));
    private_dir(&staging)?;
    let path = staging.join(crate::sqlite::DATABASE);
    drop(private_file(&path, true)?);
    let mut target = rusqlite::Connection::open(&path).map_err(crate::sqlite::db)?;
    {
        let copy =
            rusqlite::backup::Backup::new(&source, &mut target).map_err(crate::sqlite::db)?;
        copy.run_to_completion(128, std::time::Duration::from_millis(1), None)
            .map_err(|e| Error::Unavailable(format!("copy SQLite snapshot: {e}")))?;
    }
    target
        .execute_batch("PRAGMA journal_mode=DELETE;")
        .map_err(|e| Error::Unavailable(format!("seal SQLite backup: {e}")))?;
    drop(target);
    File::open(&path)?.sync_all()?;
    sync_dir(destination)?;
    sync_dir(destination.parent().unwrap_or(Path::new(".")))?;
    let report = verify(&staging)?;
    if report.fault.is_some() || report.journal_id != journal_id || report.last_lsn != last_lsn {
        return Err(invalid("backup verification failed"));
    }
    // hard_link gives no-replace publication on the same filesystem.
    fs::hard_link(&path, destination.join(crate::sqlite::DATABASE))?;
    sync_dir(destination)?;
    fs::remove_file(&path)?;
    fs::remove_dir(&staging)?;
    sync_dir(destination)?;
    Ok(report)
}

/// Indexes are native SQLite indexes; rebuilding never creates an application index file.
pub fn rebuild_index(root: &Path, upper_lsn: u64) -> Result<Recovery> {
    let report = verify(root)?;
    if report.fault.is_some() || (upper_lsn != u64::MAX && upper_lsn != report.last_lsn) {
        return Err(invalid("cannot rebuild indexes at this boundary"));
    }
    let conn = rusqlite::Connection::open(root.join(crate::sqlite::DATABASE))
        .map_err(crate::sqlite::db)?;
    conn.execute_batch("PRAGMA synchronous=FULL; REINDEX;")
        .map_err(crate::sqlite::db)?;
    Ok(report)
}

pub fn checkpoint(root: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open_with_flags(
        root.join(crate::sqlite::DATABASE),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .map_err(crate::sqlite::db)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(crate::sqlite::db)?;
    conn.execute_batch(
        "PRAGMA synchronous=FULL; PRAGMA checkpoint_fullfsync=ON; PRAGMA wal_checkpoint(PASSIVE);",
    )
    .map_err(crate::sqlite::db)?;
    Ok(())
}

pub fn offline_lock(root: &Path) -> Result<File> {
    let file = File::open(root)?;
    fs2::FileExt::try_lock_exclusive(&file)
        .map_err(|e| Error::Conflict(format!("offline tool requires stopped writer: {e}")))?;
    Ok(file)
}
