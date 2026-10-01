use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::codec::{self, Event, EventKind, Transaction};
use crate::{invalid, Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Location {
    #[serde(with = "crate::decimal")]
    pub segment: u64,
    #[serde(with = "crate::decimal")]
    pub offset: u64,
    #[serde(with = "crate::decimal")]
    pub bytes: u64,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanFault {
    pub segment: u64,
    pub offset: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Recovery {
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub last_lsn: u64,
    pub segments: Vec<Segment>,
    pub fault: Option<ScanFault>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub number: u64,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub valid_bytes: u64,
    pub sealed: bool,
    pub digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    #[serde(with = "crate::decimal")]
    segment: u64,
    previous_digest: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seal {
    #[serde(with = "crate::decimal")]
    segment: u64,
    #[serde(with = "crate::decimal")]
    first_lsn: u64,
    #[serde(with = "crate::decimal")]
    last_lsn: u64,
    digest: String,
}

pub fn segment_path(root: &Path, number: u64) -> PathBuf {
    root.join("journal").join(format!("{number:08}.jsonl"))
}

/// Strict scan, bounded by one transaction plus the segment inventory. Callbacks only see
/// complete verified transactions; fault reports preserve the last unambiguous prefix.
pub fn scan(
    root: &Path,
    mut apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    scan_until(root, u64::MAX, &mut apply)
}

pub fn scan_until(
    root: &Path,
    upper_lsn: u64,
    mut apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    scan_using(root, upper_lsn, None, &mut apply)
}

/// Fast inventory recovery trusts checkpoint-verified sealed bytes, but still checks every
/// known segment name, identity and length via load_checkpoint. Full verify always uses scan.
pub fn scan_fast(root: &Path) -> Result<Recovery> {
    let checkpoint = crate::maintenance::load_checkpoint(root, 2)?
        .or(crate::maintenance::load_checkpoint(root, 1)?);
    scan_using(
        root,
        u64::MAX,
        checkpoint.as_ref().map(|cp| &cp.inventory),
        |_, _| Ok(()),
    )
}

fn scan_using(
    root: &Path,
    upper_lsn: u64,
    trusted: Option<&Recovery>,
    mut apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    let dir = root.join("journal");
    let mut numbers = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let number = name
            .strip_suffix(".jsonl")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| invalid(format!("unexpected journal entry {name}")))?;
        if name != format!("{number:08}.jsonl") || !entry.file_type()?.is_file() {
            return Err(invalid(format!("invalid segment name/type {name}")));
        }
        numbers.push(number);
    }
    numbers.sort_unstable();
    let mut recovery = Recovery::default();
    for (i, number) in numbers.iter().copied().enumerate() {
        if number != i as u64 + 1 || recovery.segments.last().is_some_and(|s| !s.sealed) {
            recovery.fault = Some(ScanFault {
                segment: number,
                offset: 0,
                reason: "missing segment or unsealed predecessor".into(),
            });
            return Ok(recovery);
        }
        if let Some(previous) = trusted
            .and_then(|r| r.segments.get(i))
            .filter(|s| s.sealed && s.last_lsn < upper_lsn)
        {
            recovery.journal_id = trusted.unwrap().journal_id.clone();
            recovery.last_lsn = previous.last_lsn;
            recovery.segments.push(previous.clone());
            continue;
        }
        let previous_digest = recovery.segments.last().map(|s| s.digest.clone());
        let mut seg = Segment {
            number,
            first_lsn: recovery.last_lsn + 1,
            last_lsn: recovery.last_lsn,
            valid_bytes: 0,
            sealed: false,
            digest: String::new(),
        };
        let mut hash = Sha256::new();
        let mut reader = BufReader::new(File::open(segment_path(root, number))?);
        let result: Result<()> = (|| {
            while let Some(line) = codec::read_line(&mut reader)? {
                let tx = codec::decode(&line)?;
                if seg.sealed {
                    return Err(invalid("data after segment seal"));
                }
                if tx.lsn != recovery.last_lsn + 1 {
                    return Err(invalid("non-contiguous LSN"));
                }
                if recovery.journal_id.is_empty() {
                    recovery.journal_id = tx.journal_id.clone();
                }
                if tx.journal_id != recovery.journal_id {
                    return Err(invalid("foreign journal identity"));
                }
                let structural = tx.events.iter().any(|e| {
                    matches!(e.kind, EventKind::SegmentStarted | EventKind::SegmentSealed)
                });
                if structural && tx.events.len() != 1 {
                    return Err(invalid("mixed structural transaction"));
                }
                let first = &tx.events[0];
                if structural
                    && (first.run_id.is_some()
                        || first.dispatch_id.is_some()
                        || first.run_seq != 0
                        || first.audit_seq != 0)
                {
                    return Err(invalid("structural transaction has business identity"));
                }
                if seg.valid_bytes == 0 {
                    if first.kind != EventKind::SegmentStarted {
                        return Err(invalid("missing segment start"));
                    }
                    let start: Start = serde_json::from_value(first.payload.clone())?;
                    if start.segment != number || start.previous_digest != previous_digest {
                        return Err(invalid("segment chain mismatch"));
                    }
                } else if first.kind == EventKind::SegmentStarted {
                    return Err(invalid("unexpected segment start"));
                } else if first.kind == EventKind::SegmentSealed {
                    let seal: Seal = serde_json::from_value(first.payload.clone())?;
                    if seal.segment != number
                        || seal.first_lsn != seg.first_lsn
                        || seal.last_lsn != recovery.last_lsn
                        || seal.digest != hex::encode(hash.clone().finalize())
                    {
                        return Err(invalid("segment seal mismatch"));
                    }
                    seg.sealed = true;
                }
                let location = Location {
                    segment: number,
                    offset: seg.valid_bytes,
                    bytes: line.len() as u64,
                    lsn: tx.lsn,
                };
                apply(&tx, &location)?;
                hash.update(&line);
                seg.valid_bytes += line.len() as u64;
                seg.last_lsn = tx.lsn;
                recovery.last_lsn = tx.lsn;
                if recovery.last_lsn == upper_lsn {
                    break;
                }
            }
            if seg.valid_bytes == 0 {
                return Err(invalid("empty segment (ambiguous creation)"));
            }
            Ok(())
        })();
        seg.digest = hex::encode(hash.finalize());
        recovery.segments.push(seg.clone());
        if let Err(error) = result {
            recovery.fault = Some(ScanFault {
                segment: number,
                offset: seg.valid_bytes,
                reason: error.to_string(),
            });
            return Ok(recovery);
        }
        if recovery.last_lsn == upper_lsn {
            return Ok(recovery);
        }
    }
    if numbers.is_empty() {
        return Err(invalid("journal contains no segments"));
    }
    if upper_lsn != u64::MAX && recovery.last_lsn < upper_lsn {
        return Err(invalid("cursor ahead of journal"));
    }
    Ok(recovery)
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
    pub short_write_bytes: Option<usize>,
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
    fn record(&mut self, elapsed: std::time::Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = (64 - us.max(1).saturating_sub(1).leading_zeros() as usize).min(23);
        self.samples += 1;
        self.total_us = self.total_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
        self.buckets[bucket] += 1;
    }
}

pub(crate) struct Disk {
    pub file: File,
    pub root: PathBuf,
    pub id: String,
    pub segment: u64,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub bytes: u64,
    pub hash: Sha256,
    pub data_syncs: u64,
    pub dir_syncs: u64,
    pub data_latency: SyncLatency,
    pub directory_latency: SyncLatency,
    pub written: u64,
    pub faults: FaultInjection,
    pub _lock: File,
}

impl Disk {
    pub fn open(root: &Path, faults: FaultInjection) -> Result<Self> {
        let mut directory_latency = SyncLatency::default();
        private_dir_recorded(root, &mut directory_latency)?;
        let lock = File::open(root)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|e| Error::Conflict(format!("data directory is locked: {e}")))?;
        let fresh = !root.join("journal").exists();
        if fresh {
            if fs::read_dir(root)?.next().is_some() {
                return Err(invalid(
                    "new journal requires an empty data directory; import legacy data offline",
                ));
            }
            private_dir_recorded(&root.join("journal"), &mut directory_latency)?;
            let started = std::time::Instant::now();
            sync_dir(root)?;
            directory_latency.record(started.elapsed());
            let file = private_file(&segment_path(root, 1), true)?;
            let mut disk = Self {
                file,
                root: root.into(),
                id: uuid::Uuid::now_v7().to_string(),
                segment: 1,
                first_lsn: 1,
                last_lsn: 0,
                bytes: 0,
                hash: Sha256::new(),
                data_syncs: 0,
                dir_syncs: 0,
                data_latency: SyncLatency::default(),
                directory_latency,
                written: 0,
                faults,
                _lock: lock,
            };
            disk.structure(
                EventKind::SegmentStarted,
                json!({"segment":"1", "previous_digest": null}),
            )?;
            disk.sync()?;
            disk.sync_directory()?;
            Ok(disk)
        } else {
            let recovery = scan_fast(root)?;
            if let Some(fault) = recovery.fault {
                return Err(Error::Unavailable(format!("read-only recovery at segment {} offset {}: {}; preserve evidence and repair offline", fault.segment, fault.offset, fault.reason)));
            }
            let manifest = root.join("backup-manifest.json");
            if manifest.exists() {
                let expected: crate::maintenance::BackupManifest =
                    serde_json::from_reader(File::open(manifest)?)?;
                if expected.version != 1
                    || expected.journal_id != recovery.journal_id
                    || expected.upper_lsn > recovery.last_lsn
                {
                    return Err(invalid(
                        "backup manifest is ahead of or belongs to another journal",
                    ));
                }
            }
            let tail = recovery
                .segments
                .last()
                .ok_or_else(|| invalid("no segment"))?;
            let mut hash = Sha256::new();
            let mut reader = File::open(segment_path(root, tail.number))?;
            let mut buffer = [0; 64 * 1024];
            loop {
                let n = reader.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            let file = private_file(&segment_path(root, tail.number), false)?;
            let mut disk = Self {
                file,
                root: root.into(),
                id: recovery.journal_id,
                segment: tail.number,
                first_lsn: tail.first_lsn,
                last_lsn: tail.last_lsn,
                bytes: tail.valid_bytes,
                hash,
                data_syncs: 0,
                dir_syncs: 0,
                data_latency: SyncLatency::default(),
                directory_latency,
                written: 0,
                faults,
                _lock: lock,
            };
            // Re-establish durability even for complete lines whose old receipt was lost.
            disk.sync()?;
            disk.sync_directory()?;
            if tail.sealed {
                disk.next_segment()?;
            }
            Ok(disk)
        }
    }

    pub fn append(&mut self, line: &[u8], lsn: u64) -> Result<Location> {
        let location = Location {
            segment: self.segment,
            offset: self.bytes,
            bytes: line.len() as u64,
            lsn,
        };
        let mut rest = line;
        while !rest.is_empty() {
            if self
                .faults
                .fail_after_bytes
                .is_some_and(|n| self.written >= n)
            {
                return Err(std::io::Error::other("injected disk full/short write").into());
            }
            let left = self
                .faults
                .fail_after_bytes
                .map_or(u64::MAX, |n| n - self.written);
            let n = rest
                .len()
                .min(self.faults.short_write_bytes.unwrap_or(usize::MAX).max(1))
                .min(left as usize);
            let n = self.file.write(&rest[..n])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "journal write zero",
                )
                .into());
            }
            self.written += n as u64;
            rest = &rest[n..];
        }
        self.hash.update(line);
        self.bytes += line.len() as u64;
        self.last_lsn = lsn;
        Ok(location)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.data_syncs += 1;
        if self.faults.sync_delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.faults.sync_delay_ms));
        }
        if self.faults.fail_data_sync == Some(self.data_syncs) {
            return Err(std::io::Error::other("injected fsync failure").into());
        }
        let started = std::time::Instant::now();
        self.file.sync_all()?;
        self.data_latency.record(started.elapsed());
        Ok(())
    }
    fn sync_directory(&mut self) -> Result<()> {
        self.dir_syncs += 1;
        if self.faults.fail_dir_sync == Some(self.dir_syncs) {
            return Err(std::io::Error::other("injected directory fsync failure").into());
        }
        let started = std::time::Instant::now();
        sync_dir(&self.root.join("journal"))?;
        self.directory_latency.record(started.elapsed());
        Ok(())
    }
    fn structure(&mut self, kind: EventKind, payload: serde_json::Value) -> Result<()> {
        let tx = Transaction {
            v: 2,
            journal_id: self.id.clone(),
            lsn: self.last_lsn + 1,
            tx_id: uuid::Uuid::now_v7().to_string(),
            events: vec![Event::new(kind, payload)],
        };
        self.append(&codec::encode(&tx)?, tx.lsn)?;
        Ok(())
    }
    pub fn rotate(&mut self) -> Result<()> {
        self.structure(EventKind::SegmentSealed, json!({"segment": self.segment.to_string(), "first_lsn": self.first_lsn.to_string(),
            "last_lsn": self.last_lsn.to_string(), "digest": hex::encode(self.hash.clone().finalize())}))?;
        self.sync()?;
        self.next_segment()
    }
    fn next_segment(&mut self) -> Result<()> {
        let previous = hex::encode(self.hash.clone().finalize());
        self.segment += 1;
        self.first_lsn = self.last_lsn + 1;
        self.file = private_file(&segment_path(&self.root, self.segment), true)?;
        self.bytes = 0;
        self.hash = Sha256::new();
        self.structure(
            EventKind::SegmentStarted,
            json!({"segment": self.segment.to_string(), "previous_digest": previous}),
        )?;
        self.sync()?;
        self.sync_directory()
    }
}
