//! Disposable value addresses. Journal bytes and value digests remain authoritative.
//! Recovery scans once; live commits append addresses before publishing their durable cursor.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::codec::{EventKind, Transaction};
use crate::storage::{private_dir, scan_until, Location};
use crate::tail::TailReader;
use crate::{invalid, Result};

#[derive(Default)]
struct Addresses {
    journal_id: String,
    lsn: u64,
    tail: Option<TailReader>,
}
type Shared = Arc<Mutex<Addresses>>;
static ROOTS: OnceLock<Mutex<BTreeMap<PathBuf, Shared>>> = OnceLock::new();

fn shared(root: &Path) -> Result<Shared> {
    let key = root.canonicalize()?;
    let mut roots = ROOTS.get_or_init(Default::default).lock().unwrap();
    if !roots.contains_key(&key) && roots.len() >= 16 {
        if let Some(old) = roots
            .iter()
            .find(|(_, v)| Arc::strong_count(v) == 1)
            .map(|(k, _)| k.clone())
        {
            roots.remove(&old);
        }
    }
    Ok(roots.entry(key).or_default().clone())
}

fn directory(root: &Path) -> PathBuf {
    root.join("indexes").join("value-addresses")
}
fn path(root: &Path, id: &str) -> PathBuf {
    directory(root).join(format!("{}.jsonl", crate::codec::digest(id.as_bytes())))
}

fn append(root: &Path, tx: &Transaction, location: &Location) -> Result<()> {
    let ids: BTreeSet<_> = tx
        .events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ValueChunk | EventKind::ValuePublished))
        .filter_map(|e| e.payload["output_id"].as_str())
        .collect();
    for id in ids {
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path(root, id))?;
        serde_json::to_writer(&mut file, location)?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn rebuild_locked(root: &Path, upper: u64, state: &mut Addresses) -> Result<()> {
    *state = Addresses::default();
    let dir = directory(root);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    private_dir(&dir)?;
    let mut last = None;
    let recovery = scan_until(root, upper, |tx, location| {
        append(root, tx, location)?;
        last = Some(location.clone());
        Ok(())
    })?;
    if let Some(fault) = recovery.fault {
        return Err(invalid(fault.reason));
    }
    state.journal_id = recovery.journal_id;
    state.lsn = recovery.last_lsn;
    state.tail = last
        .map(|location| TailReader::after(root, &state.journal_id, &location))
        .transpose()?;
    Ok(())
}

/// Rebuild a disposable index after recovery or explicit offline maintenance.
pub fn rebuild(root: &Path, upper: u64) -> Result<()> {
    rebuild_locked(root, upper, &mut shared(root)?.lock().unwrap())
}

/// Called only for durable commits. Cache failure must never undo a journal commit.
pub(crate) fn committed(root: &Path, commits: &[crate::Commit]) {
    let Ok(shared) = shared(root) else { return };
    let mut state = shared.lock().unwrap();
    if state.journal_id.is_empty() {
        return;
    }
    let result: Result<()> = (|| {
        for commit in commits {
            if commit.transaction.lsn <= state.lsn {
                continue;
            }
            append(root, &commit.transaction, &commit.location)?;
            state.lsn = commit.transaction.lsn;
            state.tail = Some(TailReader::after(
                root,
                &state.journal_id,
                &commit.location,
            )?);
        }
        Ok(())
    })();
    if result.is_err() {
        *state = Addresses::default();
    }
}

/// Load only one object's locations. Offline readers incrementally index newly durable tails.
pub(crate) fn locations(root: &Path, upper: u64, journal_id: &str, id: &str) -> Result<Locations> {
    let shared = shared(root)?;
    let mut state = shared.lock().unwrap();
    if state.tail.is_none() {
        rebuild_locked(root, upper, &mut state)?;
    } else if state.lsn < upper {
        let result: Result<()> = (|| {
            while let Some((tx, location)) = state.tail.as_mut().unwrap().next(upper)? {
                append(root, &tx, &location)?;
                state.lsn = tx.lsn;
            }
            Ok(())
        })();
        if let Err(error) = result {
            *state = Addresses::default();
            return Err(error);
        }
    }
    if state.journal_id != journal_id {
        return Err(invalid("foreign value journal"));
    }
    let file = match File::open(path(root, id)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            rebuild_locked(root, upper, &mut state)?;
            File::open(path(root, id))?
        }
        Err(e) => return Err(e.into()),
    };
    Ok(Locations {
        reader: BufReader::new(file),
        upper,
        previous: 0,
        done: false,
    })
}

pub(crate) struct Locations {
    reader: BufReader<File>,
    upper: u64,
    previous: u64,
    done: bool,
}
impl Iterator for Locations {
    type Item = Result<Location>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result: Result<Option<Location>> = (|| {
            let mut line = Vec::new();
            let n = Read::by_ref(&mut self.reader)
                .take(1025)
                .read_until(b'\n', &mut line)?;
            if n == 0 {
                return Ok(None);
            }
            if n > 1024 || line.last() != Some(&b'\n') {
                return Err(invalid("invalid value address cache"));
            }
            let location: Location = serde_json::from_slice(&line)?;
            if location.lsn <= self.previous {
                return Err(invalid("unordered value addresses"));
            }
            self.previous = location.lsn;
            Ok((location.lsn <= self.upper).then_some(location))
        })();
        match result {
            Ok(Some(location)) => Some(Ok(location)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
