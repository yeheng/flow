//! Offline maintenance and derived caches. None of these functions executes business code.
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::codec::{self, EventKind};
use crate::storage::{
    private_dir, private_file, scan, scan_until, segment_path, sync_dir, Location, Recovery,
};
use crate::value::{read_value, ValueRef};
use crate::{invalid, Error, Result};

const CHECKPOINT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub version: u32,
    pub reducer_version: u32,
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
    pub boundary: Location,
    pub boundary_digest: String,
    pub inventory: Recovery,
    pub state: Value,
    pub state_digest: String,
}

#[derive(Serialize,Deserialize)]
struct CheckpointFile { data:Checkpoint, digest:String }

#[derive(Serialize)]
struct IndexEntry<'a> {
    location: &'a Location,
    event_index: usize,
    run_id: &'a Option<String>,
    dispatch_id: &'a Option<String>,
    output_id: Option<&'a str>,
    chunk_index: Option<&'a str>,
}

/// Atomic cache replacement includes syncing both the new file and its directory entry.
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

/// Stream location records to a disposable index. Payloads are never duplicated in this file.
pub fn rebuild_index(root: &Path, upper_lsn: u64) -> Result<Recovery> {
    let dir = root.join("indexes");
    private_dir(&dir)?;
    sync_dir(root)?;
    let temp = dir.join(format!(".{}.tmp", uuid::Uuid::now_v7()));
    let mut file = private_file(&temp, true)?;
    let recovery = scan_until(root, upper_lsn, |tx, location| {
        for (event_index, event) in tx.events.iter().enumerate() {
            let index = IndexEntry {
                location,
                event_index,
                run_id: &event.run_id,
                dispatch_id: &event.dispatch_id,
                output_id: event.payload.get("output_id").and_then(Value::as_str),
                chunk_index: event.payload.get("chunk_index").and_then(Value::as_str),
            };
            serde_json::to_writer(&mut file, &index)?;
            file.write_all(b"\n")?;
        }
        Ok(())
    })?;
    if let Some(fault) = &recovery.fault {
        return Err(invalid(fault.reason.clone()));
    }
    file.sync_all()?;
    fs::rename(&temp, dir.join("locations.jsonl"))?;
    sync_dir(&dir)?;
    Ok(recovery)
}

pub fn write_checkpoint(
    root: &Path,
    upper_lsn: u64,
    reducer_version: u32,
    state: Value,
) -> Result<()> {
    let mut boundary = None;
    let inventory = scan_until(root, upper_lsn, |_, location| {
        boundary = Some(location.clone());
        Ok(())
    })?;
    if let Some(fault) = &inventory.fault {
        return Err(invalid(fault.reason.clone()));
    }
    let boundary = boundary.ok_or_else(|| invalid("empty checkpoint"))?;
    let state_digest = codec::digest(&codec::bounded_json(&state, CHECKPOINT_BYTES)?);
    let boundary_digest = codec::digest(&read_location(root, &boundary)?);
    let checkpoint = Checkpoint {
        version: 1,
        reducer_version,
        journal_id: inventory.journal_id.clone(),
        lsn: inventory.last_lsn,
        inventory,
        boundary,
        boundary_digest,
        state,
        state_digest,
    };
    let dir = root.join("checkpoints");
    private_dir(&dir)?;
    sync_dir(root)?;
    let digest=codec::digest(&codec::bounded_json(&checkpoint,CHECKPOINT_BYTES)?);
    atomic_write(
        &dir.join("latest.json"),
        &codec::bounded_json(&CheckpointFile{data:checkpoint,digest}, CHECKPOINT_BYTES)?,
    )
}

pub fn read_location(root: &Path, location: &Location) -> Result<Vec<u8>> {
    if location.bytes > crate::MAX_LINE_BYTES as u64 {
        return Err(invalid("oversize index location"));
    }
    let mut file = File::open(segment_path(root, location.segment))?;
    file.seek(SeekFrom::Start(location.offset))?;
    let mut bytes = vec![0; location.bytes as usize];
    file.read_exact(&mut bytes)?;
    if codec::decode(&bytes)?.lsn != location.lsn {
        return Err(invalid("index boundary LSN mismatch"));
    }
    Ok(bytes)
}

/// A corrupt/incompatible cache is disposable. The caller falls back to full scan; no journal
/// repair is attempted to accommodate it. Fast validation trusts prior byte verification but
/// checks every known segment's existence, identity, length and the exact checkpoint boundary.
pub fn load_checkpoint(root: &Path, reducer_version: u32) -> Result<Option<Checkpoint>> {
    let path = root.join("checkpoints/latest.json");
    if !path.exists() {
        return Ok(None);
    }
    let attempt = || -> Result<Checkpoint> {
        if fs::metadata(&path)?.len() > CHECKPOINT_BYTES as u64 {
            return Err(invalid("oversize checkpoint"));
        }
        let file:CheckpointFile=serde_json::from_reader(File::open(path)?)?;
        if file.digest!=codec::digest(&codec::bounded_json(&file.data,CHECKPOINT_BYTES)?){return Err(invalid("checkpoint checksum mismatch"))}
        let cp=file.data;
        if cp.version != 1
            || cp.reducer_version != reducer_version
            || cp.lsn != cp.boundary.lsn
            || cp.journal_id != cp.inventory.journal_id
            || cp.lsn != cp.inventory.last_lsn
            || cp.state_digest != codec::digest(&codec::bounded_json(&cp.state, CHECKPOINT_BYTES)?)
            || cp.boundary_digest != codec::digest(&read_location(root, &cp.boundary)?)
        {
            return Err(invalid("checkpoint mismatch"));
        }
        for (i, segment) in cp.inventory.segments.iter().enumerate() {
            if segment.number != i as u64 + 1 {
                return Err(invalid("checkpoint segment sequence"));
            }
            let path = segment_path(root, segment.number);
            let len = fs::metadata(&path)?.len();
            if len < segment.valid_bytes || (segment.sealed && len != segment.valid_bytes) {
                return Err(invalid("checkpoint ahead of segment"));
            }
            let mut reader = BufReader::new(File::open(path)?);
            let tx = codec::decode(
                &codec::read_line(&mut reader)?.ok_or_else(|| invalid("missing start"))?,
            )?;
            if tx.journal_id != cp.journal_id
                || tx.lsn != segment.first_lsn
                || tx.events[0].kind != EventKind::SegmentStarted
            {
                return Err(invalid("checkpoint dataset mismatch"));
            }
        }
        if cp.inventory.segments.is_empty() || cp.inventory.fault.is_some() {
            return Err(invalid("invalid checkpoint inventory"));
        }
        let last=cp.inventory.segments.last().unwrap();
        if last.number!=cp.boundary.segment || last.last_lsn!=cp.lsn
            || cp.boundary.offset.checked_add(cp.boundary.bytes)!=Some(last.valid_bytes) {
            return Err(invalid("checkpoint inventory boundary mismatch"));
        }
        Ok(cp)
    };
    Ok(attempt().ok())
}

/// Full byte verification plus closure checks for every published value, without collecting
/// the entire history or objects in RAM. An unfinished object is retained evidence, not a value.
pub fn verify(root: &Path) -> Result<Recovery> {
    scan(root, |tx, _| {
        for event in &tx.events {
            if event.kind == EventKind::ValuePublished {
                let value: ValueRef = serde_json::from_value(event.payload.clone())?;
                read_value(root, tx.lsn, &value, true, &mut std::io::sink())?;
            }
        }
        Ok(())
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: u32,
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub upper_lsn: u64,
    pub repair: bool,
    pub source_fault: Option<crate::ScanFault>,
}

/// Copies only an explicit verified boundary. Online callers must provide their durable LSN;
/// offline callers hold an exclusive lock. Destination must not exist, including symlinks.
pub fn backup(root: &Path, destination: &Path, upper_lsn: u64) -> Result<Recovery> {
    let report = scan_until(root, upper_lsn, |_, _| Ok(()))?;
    if let Some(fault) = &report.fault {
        return Err(invalid(fault.reason.clone()));
    }
    copy_prefix(root, destination, &report, false)
}

/// No confirmation => no writes. The repair report explicitly preserves uncertainty about
/// missing acknowledged suffixes. Original files are never truncated or modified.
pub fn repair(root: &Path, destination: &Path, confirmed: bool) -> Result<Recovery> {
    let _lock = offline_lock(root)?;
    let report = scan(root, |_, _| Ok(()))?;
    if !confirmed {
        return Err(Error::Conflict(format!("repair confirmation required; candidate LSN {}, fault {:?}; suffix may contain acknowledged facts", report.last_lsn, report.fault)));
    }
    copy_prefix(root, destination, &report, true)
}

fn copy_prefix(
    root: &Path,
    destination: &Path,
    report: &Recovery,
    repair: bool,
) -> Result<Recovery> {
    if fs::symlink_metadata(destination).is_ok() {
        return Err(Error::Conflict(
            "backup/repair destination already exists".into(),
        ));
    }
    if report.last_lsn == 0 {
        return Err(invalid("no recoverable transaction"));
    }
    private_dir(destination)?;
    sync_dir(destination.parent().unwrap_or(Path::new(".")))?;
    private_dir(&destination.join("journal"))?;
    sync_dir(destination)?;
    for segment in &report.segments {
        if segment.valid_bytes == 0 {
            continue;
        }
        let mut source = File::open(segment_path(root, segment.number))?.take(segment.valid_bytes);
        let mut target = private_file(&segment_path(destination, segment.number), true)?;
        if std::io::copy(&mut source, &mut target)? != segment.valid_bytes {
            return Err(invalid("source shortened during backup"));
        }
        target.sync_all()?;
    }
    sync_dir(&destination.join("journal"))?;
    let copied = verify(destination)?;
    if copied.fault.is_some()
        || copied.last_lsn != report.last_lsn
        || copied.journal_id != report.journal_id
    {
        return Err(invalid("backup verification failed"));
    }
    let manifest = BackupManifest {
        version: 1,
        journal_id: report.journal_id.clone(),
        upper_lsn: report.last_lsn,
        repair,
        source_fault: report.fault.clone(),
    };
    atomic_write(
        &destination.join("backup-manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(copied)
}

pub fn offline_lock(root: &Path) -> Result<File> {
    let file = File::open(root)?;
    fs2::FileExt::try_lock_exclusive(&file)
        .map_err(|e| Error::Conflict(format!("offline tool requires stopped writer: {e}")))?;
    Ok(file)
}
