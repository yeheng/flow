//! Offline proof that the committed facts still reproduce the current rows.
//!
//! The journal keeps every accepted fact, but normal startup trusts `current_entities`.
//! This checker replays the facts through the same reducer used by the coordinator and
//! compares the resulting current rows, so a projection bug cannot hide behind a valid
//! checksum. It is an offline maintenance command: the service must be stopped.
//!
//! 本模块是离线校验：用协调器同一个 reducer 重放全部事实，再与当前行逐条比对。
//! 它守护的核心不变量是「当前行可由事实重放得到」——校验和合法并不代表
//! 业务状态正确，这正是投影 bug 最容易藏身的地方。
//! 需要它，是因为正常启动只加载当前行、不回放历史；一旦投影写错，
//! 没有任何在线机制会发现它。
//!
//! 本模块只报告、不修复：权威数据的修改必须走正常写路径，
//! 离线改库会破坏事实与状态的对应关系。

use std::collections::BTreeMap;
use std::path::Path;

use flow_engine::journal_state::State;
use flow_journal::sqlite;
use flow_journal::EventKind;
use serde::Serialize;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

/// 差异报告条数上限：差异可能极多，全部列出没人看得完。
const MAX_REPORTED: usize = 64;

/// 投影校验报告：重放结果与当前行的比对结论。
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

/// 一处投影差异：实体种类、键与原因。
#[derive(Debug, Serialize)]
pub struct ProjectionMismatch {
    pub kind: String,
    pub key: String,
    pub reason: String,
}

/// Replay all committed facts and compare them with the authoritative current rows.
///
/// # Errors
/// 数据库不可读、事实无法重放或当前行无法解析时返回错误。
pub async fn check(root: &Path) -> Result<ProjectionReport, Box<dyn std::error::Error>> {
    let reader = sqlite::read_connection(root)?;
    let (journal_id, upper) = sqlite::metadata(&reader)?;
    // 只读连接：校验过程绝不能改写权威数据。
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
        // 用协调器同一个 reducer 重放，并顺带算出该事务应当产生的当前行。
        let rows = state.apply_with(&tx, |next| crate::journal_history::index_rows(next, &tx))?;
        for row in rows {
            // 同一行可能被多个事务改写，最后一次即最终值。
            expected.insert((row.kind.as_str().to_owned(), row.key), (lsn, row.value));
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
        // 差异条数封顶：超出部分只置 truncated 标志，
        // 避免报告本身膨胀成不可读的大对象。
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
                // 依次核对：摘要、正文、写入时的 LSN。三者任一不符都说明
                // 当前行与事实脱节。
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
    // 反向检查：当前行里有、但重放结果没有的行同样是差异
    // （说明事实与当前行不是一一对应）。
    for (kind, key) in rows {
        if !expected.contains_key(&(kind.clone(), key.clone())) {
            push(&kind, &key, "unexpected current row".into());
        }
    }
    pool.close().await;
    Ok(report)
}
