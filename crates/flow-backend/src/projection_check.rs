//! Offline proof that the committed facts still reproduce the current rows.
//!
//! The journal keeps every accepted fact, but normal startup trusts `current_entities`.
//! This checker replays the facts through the same reducer used by the coordinator and
//! compares the resulting current rows, so a projection bug cannot hide behind a valid
//! checksum. It is an offline maintenance command: the service must be stopped.

use std::collections::BTreeMap;
use std::path::Path;

use flow_engine::journal_state::State;
use flow_journal::sqlite;
use flow_journal::EventKind;
use serde::Serialize;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

const MAX_REPORTED: usize = 64;

#[derive(Debug, Default, Serialize)]
pub struct ProjectionReport {
    pub journal_id: String,
    pub last_lsn: u64,
    /// Number of current rows the facts should produce.
    pub entities: usize,
    /// Mismatches found, capped at `MAX_REPORTED`.
    pub mismatches: Vec<ProjectionMismatch>,
    /// More mismatches exist than were reported.
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct ProjectionMismatch {
    pub kind: String,
    pub key: String,
    pub reason: String,
}

/// Replay all committed facts and compare them with the authoritative current rows.
pub async fn check(root: &Path) -> Result<ProjectionReport, Box<dyn std::error::Error>> {
    let reader = sqlite::read_connection(root)?;
    let (journal_id, upper) = sqlite::metadata(&reader)?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join(sqlite::DATABASE))
                .read_only(true),
        )
        .await?;

    let mut state = State {
        version: 1,
        journal_id: journal_id.clone(),
        compact_audits: true,
        ..Default::default()
    };
    // (last writer LSN, value). `None` means the row must be absent.
    let mut expected: BTreeMap<(String, String), (u64, Option<Value>)> = BTreeMap::new();
    for lsn in 1..=upper {
        let tx = sqlite::transaction(&reader, &journal_id, lsn)?;
        let rows = state.apply_with(&tx, |next| crate::journal_history::index_rows(next, &tx))?;
        for row in rows {
            expected.insert((row.kind, row.key), (lsn, row.value));
        }
        for event in &tx.events {
            // Published descriptors are written directly by the value store, not via
            // `EntityChange`. Reproduce the same row here.
            if event.kind == EventKind::ValuePublished {
                if let Some(id) = event.payload["output_id"].as_str() {
                    expected.insert(
                        ("published".into(), id.into()),
                        (
                            lsn,
                            Some(serde_json::json!({
                                "lsn": lsn.to_string(),
                                "value": event.payload,
                            })),
                        ),
                    );
                }
            }
        }
    }
    let mut report = ProjectionReport {
        journal_id,
        last_lsn: upper,
        entities: expected.len(),
        ..Default::default()
    };
    let mut push = |kind: &str, key: &str, reason: String| {
        if report.mismatches.len() < MAX_REPORTED {
            report.mismatches.push(ProjectionMismatch {
                kind: kind.into(),
                key: key.into(),
                reason,
            });
        } else {
            report.truncated = true;
        }
    };
    for ((kind, key), (lsn, value)) in &expected {
        let row: Option<(String, String, String)> = sqlx::query_as(
            "SELECT body,digest,last_lsn FROM current_entities WHERE kind=? AND key=?",
        )
        .bind(kind)
        .bind(key)
        .fetch_optional(&pool)
        .await?;
        match (value, row) {
            (None, None) => {}
            (None, Some(_)) => push(kind, key, "row should be absent".into()),
            (Some(_), None) => push(kind, key, "row missing".into()),
            (Some(expected), Some((body, digest, last_lsn))) => {
                if sqlite::entity_digest(kind, key, &body) != digest {
                    push(kind, key, "current row checksum mismatch".into());
                } else if serde_json::from_str::<Value>(&body)? != *expected {
                    push(
                        kind,
                        key,
                        "current row body differs from replayed facts".into(),
                    );
                } else if last_lsn != format!("{lsn:020}") {
                    push(
                        kind,
                        key,
                        format!("last_lsn {last_lsn} does not match replay LSN {lsn}"),
                    );
                }
            }
        }
    }
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT kind,key FROM current_entities")
        .fetch_all(&pool)
        .await?;
    for (kind, key) in rows {
        if !expected.contains_key(&(kind.clone(), key.clone())) {
            push(&kind, &key, "unexpected current row".into());
        }
    }
    pool.close().await;
    Ok(report)
}
