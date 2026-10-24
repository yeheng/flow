//! 事实分页：按 run 与过滤条件读取固定快照内的一段事件。
//!
//! 本模块负责「读一页事实」：在给定的提交水位内，按 run、过滤条件与游标
//! 取出事件并返回可续读游标。
//! 核心不变量：一次分页返回的事件必须来自同一个固定快照（upper_lsn）；游标
//! 与 journal、run、过滤条件、快照绑定，不匹配即拒绝——跨 run 或跨快照拼
//! 接出的序列没有意义。
//! 需要它的原因：订阅与历史导出都要在有界工作量内取事实，既不能漏，也不能
//! 把一次请求变成全表扫描。
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::codec::{self, Event};
use crate::{invalid, Result};

/// 事实过滤条件。
///
/// `Events` 只看 run 内业务事实，`Audit` 只看审计事实，`All` 全看。过滤在
/// SQL 层完成，避免把无关事实读进内存再丢弃。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    Events,
    Audit,
    All,
}
/// 分页游标。
///
/// 游标绑定数据集、run、过滤条件与快照水位，并记录下一笔事务与事务内事件
/// 索引。它是续读位置而不是授权凭据：每次请求都必须重新授权这个 run。
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
    /// 构造一个从快照起点开始的游标。
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
/// 带位置信息的事实事件。
///
/// `lsn` 与 `event_index` 一起定位事实：单靠 LSN 无法定位事务内的第几条。
#[derive(Debug, Serialize, Deserialize)]
pub struct PositionedEvent {
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
    pub event_index: usize,
    pub event: Event,
}
/// 一页事实。
///
/// `next_cursor` 为 `None` 表示快照内已读完；`snapshot_cursor` 记录本页所属
/// 的快照水位，便于调用方在翻页过程中校验水位没有漂移。
#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub events: Vec<PositionedEvent>,
    pub next_cursor: Option<Cursor>,
    pub snapshot_cursor: CommitCursor,
}
/// 提交水位游标。
///
/// 只含数据集身份与水位，是「读到哪一版」的最小凭据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitCursor {
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
}

/// A fixed snapshot with bounded *scan work*, including empty filtered pages. Cursor data is
/// validated but is not authorization; the caller must authorize the run on every request.
///
/// 读取固定快照内的一页事实，扫描工作量有界（含「过滤后为空」的情形）。
/// 游标数据会被校验，但它不是授权：调用方必须在每次请求时重新授权这个 run，
/// 否则拿着旧游标就能读到别人 run 的事实。
pub fn page(
    root: &Path,
    journal_id: &str,
    run_id: &str,
    filter: Filter,
    mut cursor: Cursor,
    durable_lsn: u64,
    limit: usize,
) -> Result<Page> {
    // 游标必须与本次请求的绑定完全一致：不匹配说明游标被跨 run 或跨数据集
    // 复用，宁可拒绝也不能给出串了边的序列。
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
    // 预留 2 KiB 给每条事件的结构开销（位置字段与分隔符），避免精确计算。
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
        // 跳过的 LSN 之间可能有别的 run 的事实：游标必须回到事务开头，
        // 否则会从错误的事件索引继续。
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
                // 条数与字节双重上限：调用方给的条数上限不能突破单行上限。
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
        // 该事务内的事件已全部消费：游标推进到下一笔事务并从索引 0 开始。
        cursor.event_index = 0;
        cursor.next_lsn += 1;
    }
    // A bounded seek may end before the upper watermark. Check for another run fact.
    // 上面的 LSN 查询有 1024 条上限，可能提前结束；这里再确认快照内是否
    // 还有本 run 的事实，决定要不要给出续读游标。
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
