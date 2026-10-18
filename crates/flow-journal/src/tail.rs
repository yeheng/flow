use crate::storage::Location;
use crate::{invalid, Result, Transaction};
use std::path::{Path, PathBuf};

/// Incremental indexed fact reader. No WAL snapshot is retained between calls.
pub struct TailReader {
    root: PathBuf,
    journal_id: String,
    next_lsn: u64,
    run: Option<String>,
    conn: Option<rusqlite::Connection>,
}
impl TailReader {
    pub fn new(root: &Path, journal_id: &str) -> Self {
        Self {
            root: root.into(),
            journal_id: journal_id.into(),
            next_lsn: 1,
            run: None,
            conn: None,
        }
    }
    pub fn for_run(root: &Path, journal_id: &str, run: &str) -> Self {
        let mut reader = Self::new(root, journal_id);
        reader.run = Some(run.into());
        reader
    }
    pub fn next(&mut self, durable_lsn: u64) -> Result<Option<(Transaction, Location)>> {
        if self.next_lsn > durable_lsn {
            return Ok(None);
        }
        if self.conn.is_none() {
            let conn = crate::sqlite::read_connection(&self.root)?;
            if crate::sqlite::metadata(&conn)?.0 != self.journal_id {
                return Err(invalid("foreign dataset"));
            }
            self.conn = Some(conn);
        }
        if let Some(run) = &self.run {
            let next: Option<String> = self
                .conn
                .as_ref()
                .unwrap()
                .query_row(
                    "SELECT MIN(lsn) FROM events WHERE run_id=? AND lsn>=? AND lsn<=?",
                    rusqlite::params![
                        run,
                        format!("{:020}", self.next_lsn),
                        format!("{durable_lsn:020}")
                    ],
                    |r| r.get(0),
                )
                .map_err(crate::sqlite::db)?;
            let Some(next) = next else {
                self.next_lsn = durable_lsn.saturating_add(1);
                return Ok(None);
            };
            self.next_lsn = next
                .parse()
                .map_err(|_| invalid("invalid run fact sequence"))?;
        }
        let (tx, bytes) = crate::sqlite::transaction_with_size(
            self.conn.as_ref().unwrap(),
            &self.journal_id,
            self.next_lsn,
        )?;
        let location = Location { bytes, lsn: tx.lsn };
        self.next_lsn = self
            .next_lsn
            .checked_add(1)
            .ok_or_else(|| invalid("sequence overflow"))?;
        Ok(Some((tx, location)))
    }
}
