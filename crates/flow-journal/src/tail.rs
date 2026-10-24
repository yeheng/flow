//! 增量事实读取：按提交序号持续读出已提交事实。
//!
//! 本模块负责「持续消费事实」：维护一个只增的读位置，按需打开只读连接，
//! 逐笔返回已提交事务。
//! 核心不变量：读位置只前进不后退，返回的事务必须已提交且通过校验；两次
//! 调用之间不保留 WAL 快照，避免长期持有读事务挡住 checkpoint。
//! 需要它的原因：协调器与执行器需要低延迟地跟随权威写入，轮询式全量扫描
//! 既慢又浪费。
use crate::storage::Location;
use crate::{invalid, Result, Transaction};
use std::path::{Path, PathBuf};

/// Incremental indexed fact reader. No WAL snapshot is retained between calls.
///
/// 增量索引事实读取器：调用之间不保留 WAL 快照，连接可以长期复用而不挡住
/// 写者的 checkpoint。
pub struct TailReader {
    root: PathBuf,
    journal_id: String,
    next_lsn: u64,
    run: Option<String>,
    conn: Option<rusqlite::Connection>,
}
impl TailReader {
    /// 新建一个读取全部事实的 reader。
    pub fn new(root: &Path, journal_id: &str) -> Self {
        Self {
            root: root.into(),
            journal_id: journal_id.into(),
            next_lsn: 1,
            run: None,
            conn: None,
        }
    }
    /// 新建一个只跟随某个 run 的 reader。
    ///
    /// 按 run 定位可以直接命中 `events_run` 索引，跳过其他 run 的事实。
    pub fn for_run(root: &Path, journal_id: &str, run: &str) -> Self {
        let mut reader = Self::new(root, journal_id);
        reader.run = Some(run.into());
        reader
    }
    /// 取下一笔已提交事实。
    ///
    /// `durable_lsn` 由调用方给出（写者的已持久化水位）：超过水位的部分
    /// 还没有落盘，不能被当作事实返回。
    ///
    /// # Errors
    /// 数据集身份不符、事实序号非法或重建事务失败时返回错误。
    pub fn next(&mut self, durable_lsn: u64) -> Result<Option<(Transaction, Location)>> {
        // 还没有新提交：直接返回空，让调用方稍后再来。
        if self.next_lsn > durable_lsn {
            return Ok(None);
        }
        // 连接惰性打开：只有真的需要读时才占用一个只读连接。
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
                // 本轮水位内没有该 run 的事实：把位置推到水位末尾，等有新
                // 提交时再从头定位，避免重复扫描同一段区间。
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
        // 读位置只前进：事实账本是只追加的，回退会导致重复消费。
        self.next_lsn = self
            .next_lsn
            .checked_add(1)
            .ok_or_else(|| invalid("sequence overflow"))?;
        Ok(Some((tx, location)))
    }
}
