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

/// 发布目标前置检查（backup / upgrade 共用）。不变量：目标要么空、要么已含
/// 发布成品——失败残骸不算占用。已发布主文件或外来内容 → Conflict 且现场
/// 原样不动；仅自家 staging 残骸（`.backup-*` / `.upgrade-*`，上次失败或进程
/// 崩溃留下）→ 清理后视为空目标，同路径直接重试，无需人工清理。
/// 返回 true 表示目录由本次调用创建。
fn prepare_destination(destination: &Path, staging_prefix: &str, label: &str) -> Result<bool> {
    let meta = match fs::symlink_metadata(destination) {
        Ok(meta) => meta,
        Err(_) => {
            private_dir(destination)?;
            return Ok(true);
        }
    };
    if !meta.is_dir() || fs::symlink_metadata(destination.join(crate::sqlite::DATABASE)).is_ok() {
        return Err(Error::Conflict(format!(
            "{label} destination already exists"
        )));
    }
    let mut leftovers = Vec::new();
    for entry in fs::read_dir(destination)? {
        let path = entry?.path();
        let ours = path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(staging_prefix));
        if !ours {
            // 有任何外来内容就整体拒绝：自家残骸也原样保留，不动现场。
            return Err(Error::Conflict(format!("{label} destination not empty")));
        }
        leftovers.push(path);
    }
    for leftover in leftovers {
        if fs::symlink_metadata(&leftover)?.file_type().is_dir() {
            fs::remove_dir_all(leftover)?;
        } else {
            fs::remove_file(leftover)?;
        }
    }
    Ok(false)
}

/// staging 清理守卫：发布成功后解除；任何中途失败 best-effort 清掉 staging
/// 与本次创建的空目标目录——目标要么是验证过的成品，要么什么都不是。
struct StagingGuard<'a> {
    destination: &'a Path,
    staging: &'a Path,
    created: bool,
    armed: bool,
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _ = fs::remove_dir_all(self.staging);
        if self.created {
            let _ = fs::remove_dir(self.destination);
        }
    }
}

/// Back up one SQLite read snapshot, including state, receipts, facts and BLOBs.
/// A requested old prefix is rejected: its corresponding current state is not retained.
/// u64::MAX selects the current snapshot. No destination is overwritten, and a
/// failed attempt leaves nothing behind: the same destination can be retried.
pub fn backup(root: &Path, destination: &Path, upper_lsn: u64) -> Result<Recovery> {
    let source = crate::sqlite::read_connection(root)?;
    source.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let (journal_id, last_lsn) = crate::sqlite::metadata(&source)?;
    if upper_lsn != u64::MAX && upper_lsn != last_lsn {
        return Err(Error::Conflict(
            "backup boundary changed or is historical; select a current snapshot".into(),
        ));
    }
    let created = prepare_destination(destination, ".backup-", "backup")?;
    // Publish only a fully verified standalone file. StagingGuard discards a
    // failed attempt, so a half-copied database never survives at the target.
    let staging = destination.join(format!(".backup-{}", uuid::Uuid::now_v7()));
    let mut guard = StagingGuard {
        destination,
        staging: &staging,
        created,
        armed: true,
    };
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
    // The sealed backup may still carry a -shm left by the WAL to DELETE switch.
    // Only the main database file is published; drop the whole staging directory.
    fs::remove_dir_all(&staging)?;
    sync_dir(destination)?;
    guard.armed = false;
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

/// Format 4 fences older executors from interpreting semantics-2 runs as legacy
/// DAGs. The SQL schema is unchanged. Upgrade a consistent COPY; never edit or
/// overwrite the source. The caller must hold the source's offline lock.
pub fn upgrade(source: &Path, destination: &Path) -> Result<Recovery> {
    let database = source.join(crate::sqlite::DATABASE);
    if fs::symlink_metadata(&database)?.file_type().is_symlink() {
        return Err(invalid("symlink source database"));
    }
    let source =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(crate::sqlite::db)?;
    let version: i64 = source
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(crate::sqlite::db)?;
    if version != 3 {
        return Err(invalid("upgrade supports only SQLite format 3 -> 4"));
    }
    let created = prepare_destination(destination, ".upgrade-", "upgrade")?;
    let staging = destination.join(format!(".upgrade-{}", uuid::Uuid::now_v7()));
    let mut guard = StagingGuard {
        destination,
        staging: &staging,
        created,
        armed: true,
    };
    private_dir(&staging)?;
    let path = staging.join(crate::sqlite::DATABASE);
    drop(private_file(&path, true)?);
    let mut target = rusqlite::Connection::open(&path).map_err(crate::sqlite::db)?;
    rusqlite::backup::Backup::new(&source, &mut target)
        .map_err(crate::sqlite::db)?
        .run_to_completion(128, std::time::Duration::from_millis(1), None)
        .map_err(crate::sqlite::db)?;
    target
        .execute_batch(
            "PRAGMA synchronous=FULL; PRAGMA user_version=4; PRAGMA journal_mode=DELETE;",
        )
        .map_err(crate::sqlite::db)?;
    drop(target);
    File::open(&path)?.sync_all()?;
    let report = verify(&staging)?;
    if report.fault.is_some() {
        return Err(invalid(
            "upgraded copy failed verification; source unchanged",
        ));
    }
    fs::hard_link(&path, destination.join(crate::sqlite::DATABASE))?;
    sync_dir(destination)?;
    fs::remove_dir_all(&staging)?;
    sync_dir(destination)?;
    sync_dir(destination.parent().unwrap_or(Path::new(".")))?;
    guard.armed = false;
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
