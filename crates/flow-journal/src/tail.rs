use crate::codec::{self, EventKind, Transaction};
use crate::storage::{segment_path, Location};
use crate::{invalid, Result};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Incremental reader of an externally supplied durable boundary. It never attempts to read
/// the writer's unsynced tail and keeps only a single bounded physical transaction in memory.
pub struct TailReader {
    root: PathBuf,
    journal_id: String,
    segment: u64,
    offset: u64,
    next_lsn: u64,
    reader: Option<BufReader<File>>,
}
impl TailReader {
    pub fn new(root: &Path, journal_id: &str) -> Self {
        Self {
            root: root.into(),
            journal_id: journal_id.into(),
            segment: 1,
            offset: 0,
            next_lsn: 1,
            reader: None,
        }
    }
    pub fn next(&mut self, durable_lsn: u64) -> Result<Option<(Transaction, Location)>> {
        if self.next_lsn > durable_lsn {
            return Ok(None);
        }
        if self.reader.is_none() {
            let mut file = File::open(segment_path(&self.root, self.segment))?;
            file.seek(SeekFrom::Start(self.offset))?;
            self.reader = Some(BufReader::new(file));
        }
        let line = codec::read_line(self.reader.as_mut().unwrap())?
            .ok_or_else(|| invalid("durable boundary beyond journal"))?;
        let tx = codec::decode(&line)?;
        if tx.journal_id != self.journal_id || tx.lsn != self.next_lsn {
            return Err(invalid("tail dataset/sequence mismatch"));
        }
        let location = Location {
            segment: self.segment,
            offset: self.offset,
            bytes: line.len() as u64,
            lsn: tx.lsn,
        };
        self.next_lsn += 1;
        self.offset += line.len() as u64;
        if tx.events[0].kind == EventKind::SegmentSealed {
            self.segment += 1;
            self.offset = 0;
            self.reader = None;
        }
        Ok(Some((tx, location)))
    }
}
