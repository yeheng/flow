use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::codec::{self, Event};
use crate::{invalid, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    Events,
    Audit,
    All,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub version: u32,
    pub journal_id: String,
    pub run_id: String,
    pub filter: Filter,
    #[serde(with = "crate::decimal")]
    pub upper_lsn: u64,
    #[serde(with = "crate::decimal")]
    pub next_lsn: u64,
    pub event_index: usize,
}
impl Cursor {
    pub fn first(journal_id: String, run_id: String, filter: Filter, upper_lsn: u64) -> Self {
        Self {
            version: 1,
            journal_id,
            run_id,
            filter,
            upper_lsn,
            next_lsn: 1,
            event_index: 0,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct PositionedEvent {
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
    pub event_index: usize,
    pub event: Event,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub events: Vec<PositionedEvent>,
    pub next_cursor: Option<Cursor>,
    pub snapshot_cursor: CommitCursor,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitCursor {
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
}

/// A fixed snapshot with bounded *scan work*, including empty filtered pages. Cursor data is
/// validated but is not authorization; the caller must authorize the run on every request.
pub fn page(
    root: &Path,
    journal_id: &str,
    run_id: &str,
    filter: Filter,
    mut cursor: Cursor,
    durable_lsn: u64,
    limit: usize,
) -> Result<Page> {
    if cursor.version != 1
        || cursor.journal_id != journal_id
        || cursor.run_id != run_id
        || cursor.filter != filter
        || cursor.upper_lsn > durable_lsn
        || cursor.next_lsn == 0
        || limit == 0
    {
        return Err(invalid("cursor binding/boundary mismatch"));
    }
    let snapshot_cursor = CommitCursor {
        journal_id: journal_id.into(),
        lsn: cursor.upper_lsn,
    };
    let mut events = Vec::new();
    let mut bytes = 2048;
    let conn = crate::sqlite::read_connection(root)?;
    if crate::sqlite::metadata(&conn)?.0 != journal_id {
        return Err(invalid("foreign dataset"));
    }
    // Seek directly to this run's next fact, rather than scanning other runs.
    let predicate = match filter {
        Filter::All => "",
        Filter::Events => " AND run_event=1",
        Filter::Audit => " AND audit_event=1",
    };
    let sql=format!("SELECT DISTINCT lsn FROM events WHERE run_id=? AND lsn>=? AND lsn<=?{predicate} ORDER BY lsn LIMIT 1024");
    let mut statement = conn.prepare(&sql).map_err(crate::sqlite::db)?;
    let lsns = statement
        .query_map(
            rusqlite::params![
                run_id,
                format!("{:020}", cursor.next_lsn),
                format!("{:020}", cursor.upper_lsn)
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(crate::sqlite::db)?;
    for lsn in lsns {
        let lsn: u64 = lsn
            .map_err(crate::sqlite::db)?
            .parse()
            .map_err(|_| invalid("invalid fact sequence"))?;
        if lsn != cursor.next_lsn {
            cursor.event_index = 0;
        }
        cursor.next_lsn = lsn;
        if cursor.next_lsn > cursor.upper_lsn {
            return Ok(Page {
                events,
                next_cursor: None,
                snapshot_cursor,
            });
        }
        let tx = crate::sqlite::transaction(&conn, journal_id, lsn)?;
        if tx.journal_id != journal_id
            || tx.lsn != cursor.next_lsn
            || cursor.event_index >= tx.events.len()
        {
            return Err(invalid("cursor position mismatch"));
        }
        for (index, event) in tx.events.iter().enumerate().skip(cursor.event_index) {
            let audit = event.is_audit();
            let include = event.run_id.as_deref() == Some(run_id)
                && match filter {
                    Filter::All => true,
                    Filter::Events => event.run_seq > 0,
                    Filter::Audit => audit,
                };
            if include {
                let positioned = PositionedEvent {
                    lsn: tx.lsn,
                    event_index: index,
                    event: event.clone(),
                };
                let n = codec::bounded_json(&positioned, crate::MAX_LINE_BYTES - 2048)?.len() + 1;
                if events.len() >= limit.min(256) || bytes + n > crate::MAX_LINE_BYTES {
                    cursor.event_index = index;
                    return Ok(Page {
                        events,
                        next_cursor: Some(cursor),
                        snapshot_cursor,
                    });
                }
                bytes += n;
                events.push(positioned);
            }
        }
        cursor.event_index = 0;
        cursor.next_lsn += 1;
    }
    // A bounded seek may end before the upper watermark. Check for another run fact.
    let more: bool = conn
        .query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM events WHERE run_id=? AND lsn>=? AND lsn<=?{predicate})"),
            rusqlite::params![
                run_id,
                format!("{:020}", cursor.next_lsn),
                format!("{:020}", cursor.upper_lsn)
            ],
            |r| r.get(0),
        )
        .map_err(crate::sqlite::db)?;
    let next_cursor = more.then_some(cursor);
    Ok(Page {
        events,
        next_cursor,
        snapshot_cursor,
    })
}
