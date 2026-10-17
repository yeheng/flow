//! SQLite inspection and filesystem initialization. Locations identify logical facts.
use crate::codec::{self, Transaction};
use crate::{invalid, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Location {
    #[serde(with = "crate::decimal")]
    pub bytes: u64,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanFault {
    pub lsn: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Recovery {
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub last_lsn: u64,
    pub fault: Option<ScanFault>,
}

pub fn scan(
    root: &Path,
    apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    scan_until(root, u64::MAX, apply)
}

pub fn scan_until(
    root: &Path,
    upper_lsn: u64,
    mut apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    let conn = crate::sqlite::read_connection(root)?;
    conn.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let (identity, durable) = crate::sqlite::metadata(&conn)?;
    if upper_lsn != u64::MAX && upper_lsn > durable {
        return Err(invalid("cursor ahead of database"));
    }
    let upper = upper_lsn.min(durable);
    let mut report = Recovery {
        journal_id: identity.clone(),
        ..Default::default()
    };
    for lsn in 1..=upper {
        let result = (|| {
            let tx = crate::sqlite::transaction(&conn, &identity, lsn)?;
            let bytes = codec::encode(&tx)?.len() as u64;
            apply(&tx, &Location { bytes, lsn })
        })();
        if let Err(error) = result {
            report.fault = Some(ScanFault {
                lsn,
                reason: error.to_string(),
            });
            return Ok(report);
        }
        report.last_lsn = lsn;
    }
    Ok(report)
}

/// Startup reads only the database's committed metadata, not its accepted history.
pub fn scan_fast(root: &Path) -> Result<Recovery> {
    let conn = crate::sqlite::read_connection(root)?;
    let (journal_id, last_lsn) = crate::sqlite::metadata(&conn)?;
    Ok(Recovery {
        journal_id,
        last_lsn,
        ..Default::default()
    })
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn private_file(path: &Path, create_new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).append(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Do not accept symlink destinations (including a replaced active segment).
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(invalid("symlink journal file"));
    }
    Ok(options.open(path)?)
}

pub(crate) fn private_dir(path: &Path) -> Result<()> {
    private_dir_recorded(path, &mut SyncLatency::default())
}
fn private_dir_recorded(path: &Path, latency: &mut SyncLatency) -> Result<()> {
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(invalid("symlink data directory"));
    }
    // Persist each new ancestor's directory entry, including nested fresh roots.
    // create_dir_all followed by syncing only the immediate parent is insufficient.
    if !path.exists() {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if !parent.exists() {
            private_dir_recorded(parent, latency)?;
        }
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::symlink_metadata(path)?.file_type().is_symlink() || !path.is_dir() {
                    return Err(invalid("invalid data directory"));
                }
            }
            Err(e) => return Err(e.into()),
        }
        let started = std::time::Instant::now();
        sync_dir(parent)?;
        latency.record(started.elapsed());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Fault hooks are opt-in and deterministic; all paths use the same production writer.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultInjection {
    pub fail_after_bytes: Option<u64>,
    pub fail_data_sync: Option<u64>,
    pub fail_dir_sync: Option<u64>,
    pub sync_delay_ms: u64,
}

/// Fixed logarithmic microsecond buckets: bucket i contains durations <= 2^i us,
/// with the last bucket also collecting longer calls. Includes directory initialization.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SyncLatency {
    pub samples: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub buckets: [u64; 24],
}
impl SyncLatency {
    pub(crate) fn record(&mut self, elapsed: std::time::Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = (64 - us.max(1).saturating_sub(1).leading_zeros() as usize).min(23);
        self.samples += 1;
        self.total_us = self.total_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
        self.buckets[bucket] += 1;
    }
}
