//! One SQLite authority: accepted facts and current entities share the same commit.
//!
//! 本模块是权威库的唯一落盘实现：事务行、事件行、当前行与值分块都写进同一
//! 个 SQLite 事务，读路径也只从这里重建事实。
//! 核心不变量：提交水位 `committed_lsn` 与实际最大 LSN 必须一致；任何读回
//! 的事实都要复核事件身份、尺寸与摘要，不一致即判损坏；当前行只能被覆盖
//! 更新，不能承载删除语义。
//! 需要它的原因：把事实与状态放进同一个事务是「提交成功即状态生效」的
//! 前提，也是崩溃恢复时唯一可信的依据。
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD, Engine};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::codec::{self, Event, EventKind, Transaction};
use crate::storage::{private_dir, sync_dir, FaultInjection, Location, SyncLatency};
use crate::value::{Chunk, ValueRef};
use crate::{invalid, Error, Result, MAX_VALUE_BYTES};

/// 权威库文件名。
///
/// 名字是运维契约：离线工具、备份与升级都按这个名字定位文件。
pub const DATABASE: &str = "flow.sqlite";
/// 存储格式版本。
///
/// 版本 4 与版本 3 表结构相同，差别在于「语义门」：旧程序读到版本 4 必须
/// 拒绝把它当 legacy DAG 解释，而不是按旧语义误读。
pub const FORMAT: i64 = 4;
/// 单条当前实体 body 的字节上限。
///
/// 当前行是投影而非历史档案：过大说明调用方塞了不该进投影的内容。
pub const MAX_ENTITY_BYTES: usize = 2 * 1024 * 1024;

/// Closed vocabulary at the authoritative write boundary. Stored SQL and JSON
/// names stay unchanged; unknown kinds fail deserialization instead of deleting
/// or creating a silently misspelled namespace.
///
/// 权威写边界的封闭词汇表：分类名直接进 `current_entities.kind` 并被分区
/// 索引依赖，因此名字一旦发布就不能改；未知分类必须报错，不能静默落到
/// 某个默认分区——那会让数据写进错误的索引分区且再也查不出来。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Run,
    RunNode,
    RunAttempt,
    Workflow,
    WorkflowVersion,
    Schedule,
    Webhook,
    Template,
    Dispatch,
    Child,
    Legacy,
    Command,
    Audit,
    Published,
    Engine,
    ScheduleFire,
    RunWorkflow,
}
impl EntityKind {
    /// 返回写入存储的分类名。
    ///
    /// 名字是对外稳定的存储契约：改名字等于换命名空间，历史行读不回来。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::RunNode => "run_node",
            Self::RunAttempt => "run_attempt",
            Self::Workflow => "workflow",
            Self::WorkflowVersion => "workflow_version",
            Self::Schedule => "schedule",
            Self::Webhook => "webhook",
            Self::Template => "template",
            Self::Dispatch => "dispatch",
            Self::Child => "child",
            Self::Legacy => "legacy",
            Self::Command => "command",
            Self::Audit => "audit",
            Self::Published => "published",
            Self::Engine => "engine",
            Self::ScheduleFire => "schedule_fire",
            Self::RunWorkflow => "run_workflow",
        }
    }
    /// 从存储里的名字反向解析分类。
    ///
    /// # Errors
    /// 名字不属于封闭词汇表时返回 [`Error::Invalid`]。
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "run" => Ok(Self::Run),
            "run_node" => Ok(Self::RunNode),
            "run_attempt" => Ok(Self::RunAttempt),
            "workflow" => Ok(Self::Workflow),
            "workflow_version" => Ok(Self::WorkflowVersion),
            "schedule" => Ok(Self::Schedule),
            "webhook" => Ok(Self::Webhook),
            "template" => Ok(Self::Template),
            "dispatch" => Ok(Self::Dispatch),
            "child" => Ok(Self::Child),
            "legacy" => Ok(Self::Legacy),
            "command" => Ok(Self::Command),
            "audit" => Ok(Self::Audit),
            "published" => Ok(Self::Published),
            "engine" => Ok(Self::Engine),
            "schedule_fire" => Ok(Self::ScheduleFire),
            "run_workflow" => Ok(Self::RunWorkflow),
            _ => Err(invalid(format!("unknown entity kind: {name}"))),
        }
    }
}

/// A current object, not a historical copy of every revision. None is a deletion.
///
/// 当前对象，不是每个修订版本的历史副本；`value` 为 `None` 表示删除该行。
/// 这里只保留「当前」，历史全在事实账本里——两份历史会各自漂移。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityChange {
    pub kind: EntityKind,
    pub key: String,
    pub value: Option<Value>,
}

/// 把 SQLite 错误翻译成本 crate 的错误分类。
///
/// 约束违例按 `Invalid` 处理（调用方输入问题，不冻结写者），其余按
/// `Unavailable` 处理（平台故障，冻结写者）。
pub(crate) fn db(error: rusqlite::Error) -> Error {
    // 约束违例（UNIQUE tx_id / CHECK event_count 等）意味着被拒的是这笔
    // 事务的内容，不是存储平台故障——按 Invalid 分类，不冻结唯一写者
    // （writer 的失败分类只放行 Invalid/Limit/Conflict）。误冻结会把一条
    // 坏输入升级成「权威不可用直到进程重启」。
    if matches!(
        error,
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ErrorCode::ConstraintViolation
    ) {
        return Error::Invalid(format!("SQLite constraint violation: {error}"));
    }
    Error::Unavailable(format!("SQLite: {error}"))
}

/// Bind checksums to identity as well as content, including arbitrary user keys.
///
/// 摘要覆盖 kind + key + body。只校验 body 会让「把 A 行的正文搬到 B 行」
/// 这类篡改通过校验；把身份一起纳入，才能发现内容被搬到了别的身份下。
pub fn entity_digest(kind: &str, key: &str, body: &str) -> String {
    let mut hash = Sha256::new();
    for bytes in [kind.as_bytes(), key.as_bytes(), body.as_bytes()] {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    hex::encode(hash.finalize())
}

/// 打开只读连接并校验存储格式版本。
///
/// 只读连接是读面（分页、tail、离线维护）唯一允许的访问方式：读路径绝不
/// 写入，也就不会与唯一写者争锁。
///
/// # Errors
/// 数据库是符号链接、格式版本不受支持或打开失败时返回错误。
pub fn read_connection(root: &Path) -> Result<Connection> {
    let path = root.join(DATABASE);
    // 拒绝符号链接：权威文件被换成链接等于把写权限交给任意目标。
    if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
        return Err(invalid("symlink database"));
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(db)?;
    conn.busy_timeout(Duration::from_secs(5)).map_err(db)?;
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(db)?;
    if version != FORMAT {
        return Err(invalid("unsupported SQLite storage format; use flow-journal-tool upgrade <source> <new-directory> for format 3"));
    }
    Ok(conn)
}

/// 读取数据集身份与已提交水位。
///
/// 水位是「已 COMMIT 的最高 LSN」，用十进制字符串存储以避免 64 位整数在
/// 不同语言客户端丢精度。
///
/// # Errors
/// 元数据缺失、格式非法或查询失败时返回错误。
pub fn metadata(conn: &Connection) -> Result<(String, u64)> {
    let (identity, lsn): (String, String) = conn
        .query_row(
            "SELECT journal_id,committed_lsn FROM store_meta WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(db)?;
    let lsn = lsn
        .parse()
        .map_err(|_| invalid("invalid SQLite commit sequence"))?;
    Ok((identity, lsn))
}

/// 读取一条当前实体并校验其摘要。
///
/// 摘要不符即报错：当前行是投影，投影被改坏时把内容返回给调用方，错误会
/// 顺着业务逻辑扩散出去。
pub(crate) fn entity(conn: &Connection, kind: &str, key: &str) -> Result<Option<Value>> {
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT body,digest FROM current_entities WHERE kind=? AND key=?",
            params![kind, key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(db)?;
    row.map(|(body, digest)| {
        if entity_digest(kind, key, &body) != digest {
            return Err(invalid("current entity checksum mismatch"));
        }
        Ok(serde_json::from_str(&body)?)
    })
    .transpose()
}

/// 计算用于列表排序的创建序键。
///
/// 只对真正有「创建时间」语义的分类计算，其余分类返回空串：缺失字段参与
/// 排序会产生看似随机、实则不可解释的顺序。
fn created_order(kind: &str, value: &Value) -> String {
    if !matches!(
        kind,
        "run" | "workflow" | "schedule" | "webhook" | "template"
    ) {
        return String::new();
    }
    value["created_at"]
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|ts| {
            // 统一到 UTC 与固定纳秒精度：时区或精度不一致会让字典序与真实
            // 时间序出现偏差，列表顺序就不再稳定。
            ts.with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        })
        .unwrap_or_default()
}

/// Decode the canonical key tuple once at write time into typed columns. Reads then
/// use plain indexed columns instead of `json_extract(key, ...)`.
///
/// 键是 JSON 元组，在写时解析一次拆成 parent/slot/extra 三列：读路径因此能
/// 直接走索引，而不是对 key 做 JSON 表达式（SQLite 无法为表达式建索引）。
fn key_parts(kind: &str, key: &str) -> (String, String, String) {
    let parts: Vec<String> = serde_json::from_str(key).unwrap_or_default();
    let get = |index: usize| parts.get(index).cloned().unwrap_or_default();
    match kind {
        "run_node" | "command" => (get(0), get(1), String::new()),
        "run_attempt" | "child" | "audit" => (get(0), get(1), get(2)),
        "workflow_version" => (get(0), get(1), String::new()),
        _ => (String::new(), String::new(), String::new()),
    }
}

/// 写一条当前实体（覆盖更新），或按请求删除它。
///
/// 回执类（command/audit）是不可变的：同键不同内容视为冲突而不是覆盖，
/// 否则命令幂等窗口会被悄悄改写，重放将得到不同结果。
pub(crate) fn apply_entity(conn: &Connection, lsn: u64, row: &EntityChange) -> Result<()> {
    if let Some(value) = &row.value {
        let body =
            String::from_utf8(codec::bounded_json(value, MAX_ENTITY_BYTES)?).expect("JSON UTF-8");
        // Receipts are immutable. A conflicting duplicate is an error, never an overwrite.
        if matches!(row.kind.as_str(), "command" | "audit") {
            if let Some(previous) = entity(conn, row.kind.as_str(), &row.key)? {
                if previous != *value {
                    return Err(Error::Conflict("conflicting permanent receipt".into()));
                }
                return Ok(());
            }
        }
        let (parent, slot, extra) = key_parts(row.kind.as_str(), &row.key);
        conn.execute("INSERT INTO current_entities(kind,key,parent,slot,extra,body,digest,created_order,last_lsn) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET parent=excluded.parent,slot=excluded.slot,extra=excluded.extra,body=excluded.body,digest=excluded.digest,created_order=excluded.created_order,last_lsn=excluded.last_lsn",
            params![row.kind.as_str(),row.key,parent,slot,extra,body,entity_digest(row.kind.as_str(),&row.key,&body),created_order(row.kind.as_str(),value),format!("{lsn:020}")]).map_err(db)?;
    } else {
        conn.execute(
            "DELETE FROM current_entities WHERE kind=? AND key=?",
            params![row.kind.as_str(), row.key],
        )
        .map_err(db)?;
    }
    Ok(())
}

/// Reconstruct a bounded accepted transaction; raw chunk bytes exist only in value_chunks.
///
/// 重建一个已接受事务的完整内容：逐条复核事件体、身份列与摘要，任何不一致
/// 都判为存储损坏，而不是返回半可信的事实。
///
/// # Errors
/// 事务不存在、身份列与事件体不符、尺寸或摘要不匹配时返回错误。
pub fn transaction(conn: &Connection, identity: &str, lsn: u64) -> Result<Transaction> {
    transaction_with_size(conn, identity, lsn).map(|(tx, _)| tx)
}

/// Same reconstruction, plus the recorded encoded size used by tail/subscription budgets.
///
/// 额外返回落盘时记录的编码字节数：tail 与订阅按字节预算限流，必须用落盘
/// 时的口径，而不是重新编码一遍（重新编码未必与落盘字节相同）。
pub fn transaction_with_size(
    conn: &Connection,
    identity: &str,
    lsn: u64,
) -> Result<(Transaction, u64)> {
    let (tx_id, count, digest, recorded_bytes): (String, i64, String, i64) = conn
        .query_row(
            "SELECT tx_id,event_count,digest,bytes FROM transactions WHERE lsn=?",
            [format!("{lsn:020}")],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(db)?;
    // rusqlite 0.38 dropped u64/usize FromSql/ToSql; SQLite integers are i64.
    let count: usize =
        usize::try_from(count).map_err(|_| invalid("invalid stored transaction metadata"))?;
    let recorded_bytes = u64::try_from(recorded_bytes)
        .map_err(|_| invalid("invalid stored transaction metadata"))?;
    let mut statement = conn
        .prepare("SELECT event_index,body,run_id,dispatch_id,run_event,audit_event FROM events WHERE lsn=? ORDER BY event_index")
        .map_err(db)?;
    let mut rows = statement.query([format!("{lsn:020}")]).map_err(db)?;
    let mut events = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.next().map_err(db)? {
        let index: i64 = row.get(0).map_err(db)?;
        let body: String = row.get(1).map_err(db)?;
        bytes = bytes.saturating_add(body.len());
        if index != events.len() as i64 || bytes > crate::MAX_LINE_BYTES {
            return Err(invalid("invalid stored transaction size/sequence"));
        }
        let mut event: Event = serde_json::from_str(&body)?;
        let run_id: Option<String> = row.get(2).map_err(db)?;
        let dispatch_id: Option<String> = row.get(3).map_err(db)?;
        let run_event: bool = row.get(4).map_err(db)?;
        let audit_event: bool = row.get(5).map_err(db)?;
        if run_id != event.run_id
            || dispatch_id != event.dispatch_id
            || run_event != (event.run_seq > 0)
            || audit_event != event.is_audit()
        {
            return Err(invalid("accepted event index identity mismatch"));
        }
        if event.kind == EventKind::ValueChunk {
            let (output, indexed_chunk, raw, hash): (String, i64, Vec<u8>, String) = conn
                .query_row(
                    "SELECT output_id,chunk_index,bytes,digest FROM value_chunks WHERE lsn=? AND event_index=?",
                    params![format!("{lsn:020}"), index],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .map_err(db)?;
            let indexed_chunk =
                u64::try_from(indexed_chunk).map_err(|_| invalid("invalid stored chunk index"))?;
            if Some(output.as_str()) != event.payload["output_id"].as_str()
                || Some(indexed_chunk)
                    != event.payload["chunk_index"]
                        .as_str()
                        .and_then(|s| s.parse::<u64>().ok())
                || raw.is_empty()
                || raw.len() > crate::CHUNK_BYTES
                || codec::digest(&raw) != hash
            {
                return Err(invalid("stored chunk checksum mismatch"));
            }
            let encoded = STANDARD.encode(raw);
            // The stored null (4 bytes) becomes a quoted base64 string (n+2 bytes).
            bytes = bytes.saturating_add(encoded.len().saturating_sub(2));
            if bytes > crate::MAX_LINE_BYTES {
                return Err(invalid("invalid stored transaction size"));
            }
            event.payload["data"] = Value::String(encoded);
        }
        events.push(event);
    }
    let tx = Transaction {
        v: 2,
        journal_id: identity.into(),
        lsn,
        tx_id,
        events,
    };
    let body = codec::bounded_json(&tx, crate::MAX_LINE_BYTES)?;
    if tx.events.len() != count
        || body.len() as u64 != recorded_bytes
        || codec::digest(&body) != digest
    {
        return Err(invalid("accepted transaction checksum/size mismatch"));
    }
    tx.validate()?;
    Ok((tx, recorded_bytes))
}

/// 权威磁盘句柄：唯一持有写连接的对象。
///
/// `last_lsn` 是已提交水位；`_lock` 是数据目录的排他文件锁，它的生命周期
/// 决定了「同一时刻只有一个写者」这条边界。
pub(crate) struct Disk {
    pub conn: Connection,
    pub id: String,
    pub last_lsn: u64,
    pub data_syncs: u64,
    pub data_latency: SyncLatency,
    pub directory_latency: SyncLatency,
    faults: FaultInjection,
    written: u64,
    before_batch: u64,
    _lock: File,
}

impl Disk {
    /// 打开（或新建）权威库，并接管数据目录的排他锁。
    ///
    /// 新建要求数据目录为空：旧格式不在此处导入，宁可拒绝启动，也不产生
    /// 「一半新一半旧」的库。
    ///
    /// # Errors
    /// 目录被锁、非空目录、符号链接或格式版本不受支持时返回错误。
    pub fn open(root: &Path, faults: FaultInjection) -> Result<Self> {
        private_dir(root)?;
        // 排他锁是单写者边界的物理保证：两个写者同时存在时，提交序号将
        // 无法自洽。
        let lock = File::open(root)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|e| Error::Conflict(format!("data directory is locked: {e}")))?;
        let path = root.join(DATABASE);
        let fresh = !path.exists();
        // 新库必须落在空目录：目录里有旧格式残留说明运维流程出错了。
        if fresh && std::fs::read_dir(root)?.next().is_some() {
            return Err(invalid(
                "new SQLite storage requires an empty data directory; old formats are not imported",
            ));
        }
        if std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(invalid("symlink database"));
        }
        if fresh {
            drop(crate::storage::private_file(&path, true)?);
        }
        let mut conn = Connection::open(&path).map_err(db)?;
        conn.busy_timeout(Duration::from_secs(5)).map_err(db)?;
        let mut initialize = fresh;
        if !fresh {
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .map_err(db)?;
            if version == 0 {
                // A crash before the schema transaction commits leaves an empty SQLite
                // file. It has no accepted facts; resume only this bootstrap state.
                let tables: i64 = conn
                    .query_row("SELECT COUNT(*) FROM sqlite_schema", [], |r| r.get(0))
                    .map_err(db)?;
                if tables != 0 {
                    return Err(invalid("unsupported SQLite storage format"));
                }
                initialize = true;
            } else if version != FORMAT {
                return Err(invalid("unsupported SQLite storage format; format 3 can be copied with flow-journal-tool upgrade <source> <new-directory>"));
            } else {
                metadata(&conn)?;
            }
        }
        // WAL + FULL 同步：COMMIT 返回前数据已落到盘上，崩溃后不会丢已确认
        // 的事务；fullfsync 让 fsync 真正落到设备而不是只到页缓存。
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA foreign_keys=ON; PRAGMA temp_store=FILE;").map_err(db)?;
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(db)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Unavailable(format!(
                "SQLite WAL required, got {journal_mode}"
            )));
        }
        if initialize {
            // 注入点：在建表事务提交前触发目录同步失败，用于演练恢复路径。
            if faults.fail_dir_sync == Some(1) {
                return Err(std::io::Error::other("injected directory sync failure").into());
            }
            let tx = conn.transaction().map_err(db)?;
            tx.execute_batch(include_str!("schema.sql")).map_err(db)?;
            tx.execute(
                "INSERT INTO store_meta(singleton,journal_id,committed_lsn) VALUES(1,?,'0')",
                [uuid::Uuid::now_v7().to_string()],
            )
            .map_err(db)?;
            tx.commit().map_err(db)?;
            sync_dir(root)?;
        }
        let (id, last_lsn) = metadata(&conn)?;
        // 水位必须与实际最大 LSN 一致：否则说明存在落在水位之外的事务行，
        // 读回路径会把它当成永远不可见的事实。
        let last: Option<String> = conn
            .query_row("SELECT MAX(lsn) FROM transactions", [], |r| r.get(0))
            .map_err(db)?;
        if last != ((last_lsn > 0).then(|| format!("{last_lsn:020}"))) {
            return Err(invalid("SQLite commit watermark mismatch"));
        }
        Ok(Self {
            conn,
            id,
            last_lsn,
            data_syncs: 0,
            data_latency: SyncLatency::default(),
            directory_latency: SyncLatency::default(),
            faults,
            written: 0,
            before_batch: last_lsn,
            _lock: lock,
        })
    }
    /// 开启一个覆盖整批事务的写事务。
    ///
    /// `BEGIN IMMEDIATE` 立即取写锁：延迟取锁会在提交点才发现冲突，整批
    /// 已经写好的事实只能回滚。
    pub fn begin(&mut self) -> Result<()> {
        self.before_batch = self.last_lsn;
        self.conn.execute_batch("BEGIN IMMEDIATE").map_err(db)
    }
    /// `bytes`/`digest` are the canonical encoded transaction computed once by the
    /// writer, never recomputed here. `changes` are the derived current rows.
    ///
    /// 摘要与字节数由写者算好后传入：在这里重算会得到「以本函数看到的
    /// LSN 为准」的另一份结果，与写者的记账对不上。
    pub fn append(
        &mut self,
        tx: &Transaction,
        changes: &[EntityChange],
        prepared: bool,
        bytes: u64,
        digest: &str,
    ) -> Result<Location> {
        // 领域模式：库一旦接收过带当前行投影的事务就不再接受纯事实事务，
        // 反之亦然——两种语义不能混在一个库里，否则读面无法判断哪套规则
        // 适用。
        let domain: bool = self
            .conn
            .query_row(
                "SELECT domain_mode FROM store_meta WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        if domain && !prepared {
            return Err(invalid("domain database requires a prepared transition"));
        }
        if prepared {
            if !domain && self.last_lsn > 0 {
                return Err(invalid("cannot mix fact-only and domain databases"));
            }
            self.conn
                .execute("UPDATE store_meta SET domain_mode=1 WHERE singleton=1", [])
                .map_err(db)?;
        }
        // 注入点：累计写入超过指定量后失败，用于演练「提交中途断电」。
        if self
            .faults
            .fail_after_bytes
            .is_some_and(|n| self.written.saturating_add(bytes) > n)
        {
            return Err(std::io::Error::other("injected SQLite write failure").into());
        }
        self.conn
            .execute(
                "INSERT INTO transactions VALUES(?,?,?,?,?)",
                params![
                    format!("{:020}", tx.lsn),
                    tx.tx_id,
                    tx.events.len() as i64,
                    digest,
                    bytes as i64
                ],
            )
            .map_err(db)?;
        // 事件逐条写入：事件索引是事务内定位事实的唯一依据，必须与读回
        // 路径的期望一致。
        for (index, event) in tx.events.iter().enumerate() {
            let mut stored = event.clone();
            let chunk = if event.kind == EventKind::ValueChunk {
                // 载荷形状错误是调用方输入问题（Invalid 不冻结），不是平台
                // 故障——Err(Json) 会被 writer 当存储故障冻结唯一写者。
                let chunk: Chunk = serde_json::from_value(event.payload.clone())
                    .map_err(|e| invalid(format!("malformed value chunk payload: {e}")))?;
                let raw = chunk.decode()?;
                stored.payload["data"] = Value::Null;
                Some((chunk, raw))
            } else {
                None
            };
            self.conn
                .execute(
                    "INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                    params![
                        format!("{:020}", tx.lsn),
                        index as i64,
                        event.run_id,
                        event.dispatch_id,
                        event.run_seq > 0,
                        event.is_audit(),
                        serde_json::to_string(&stored)?
                    ],
                )
                .map_err(db)?;
            if let Some((chunk, raw)) = chunk {
                self.conn
                    .execute(
                        "INSERT INTO value_chunks VALUES(?,?,?,?,?,?)",
                        params![
                            chunk.output_id,
                            chunk.chunk_index as i64,
                            format!("{:020}", tx.lsn),
                            index as i64,
                            raw,
                            codec::digest(&raw)
                        ],
                    )
                    .map_err(db)?;
            }
            // 值事件额外走一次闭环校验：分块连续性与发布摘要在字节仍在手上
            // 时确认一次，之后不再重复。
            if matches!(
                event.kind,
                EventKind::ValueChunk | EventKind::ValuePublished
            ) {
                self.apply_value(event, tx.lsn)?;
            }
        }
        // 当前行与事实在同一事务生效；提交水位最后更新，它是「这批事实已
        // 可见」的唯一权威标记。
        for change in changes {
            apply_entity(&self.conn, tx.lsn, change)?;
        }
        self.conn
            .execute(
                "UPDATE store_meta SET committed_lsn=? WHERE singleton=1",
                [tx.lsn.to_string()],
            )
            .map_err(db)?;
        self.written += bytes;
        self.last_lsn = tx.lsn;
        Ok(Location { bytes, lsn: tx.lsn })
    }
    /// Value validation happens once, here, while the bytes are in hand. The reducer
    /// only keeps verified publication descriptors; it never re-hashes a stream.
    ///
    /// 校验只发生一次：此刻原始字节还在手上，既能验证分块连续性与发布摘要，
    /// 又不必把整条流重新读一遍。
    fn apply_value(&self, event: &Event, lsn: u64) -> Result<()> {
        let id = event.payload["output_id"]
            .as_str()
            .ok_or_else(|| invalid("missing value identity"))?;
        let published: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM current_entities WHERE kind='published' AND key=?)",
                [id],
                |r| r.get(0),
            )
            .map_err(db)?;
        // 已发布的值不可再追加分块：发布摘要覆盖全部字节，事后追加会让
        // 摘要与内容不再对应。
        if published {
            return Err(invalid("value chunk/publication after publication"));
        }
        if event.kind == EventKind::ValueChunk {
            // The current row is already inserted, so its index must be the last one.
            let chunk: Chunk = serde_json::from_value(event.payload.clone())
                .map_err(|e| invalid(format!("malformed value chunk payload: {e}")))?;
            chunk.decode()?;
            let (count, total): (i64, i64) = self
                .conn
                .query_row(
                    "SELECT COUNT(*),COALESCE(SUM(length(bytes)),0) FROM value_chunks WHERE output_id=?",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(db)?;
            let (count, total) = (count as u64, total as u64);
            // 块索引必须恰好等于当前块数：重复或跳号都会让读回时的摘要
            // 不符。
            if chunk.chunk_index.checked_add(1) != Some(count) || total > MAX_VALUE_BYTES {
                return Err(invalid("value chunk gap/duplicate/limit"));
            }
            return Ok(());
        }
        let value: ValueRef = serde_json::from_value(event.payload.clone())
            .map_err(|e| invalid(format!("malformed value ref payload: {e}")))?;
        value.validate()?;
        if value.journal_id != self.id {
            return Err(invalid("published value belongs to another journal"));
        }
        let (count, total): (i64, i64) = self
            .conn
            .query_row(
                "SELECT COUNT(*),COALESCE(SUM(length(bytes)),0) FROM value_chunks WHERE output_id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(db)?;
        let (count, total) = (count as u64, total as u64);
        // 发布前块数与字节数都要对上，否则发布出去的是残缺值。
        if value.chunk_count != count || value.total_bytes != total {
            return Err(invalid("publication requires complete matching chunks"));
        }
        let mut hash = Sha256::new();
        let mut statement = self
            .conn
            .prepare(
                "SELECT chunk_index,bytes FROM value_chunks WHERE output_id=? ORDER BY chunk_index",
            )
            .map_err(db)?;
        let mut rows = statement.query([id]).map_err(db)?;
        // 按顺序重走一遍分块并重算摘要：发布描述符里的摘要必须与实际落盘
        // 字节一致，否则这个值无法被验证。
        let mut expected = 0i64;
        while let Some(row) = rows.next().map_err(db)? {
            let index: i64 = row.get(0).map_err(db)?;
            let bytes: Vec<u8> = row.get(1).map_err(db)?;
            if index != expected {
                return Err(invalid("non-contiguous stored value chunk"));
            }
            hash.update(&bytes);
            expected += 1;
        }
        if hex::encode(hash.finalize()) != value.digest {
            return Err(invalid("publication digest mismatch"));
        }
        apply_entity(
            &self.conn,
            lsn,
            &EntityChange {
                kind: crate::sqlite::EntityKind::Published,
                key: id.into(),
                value: Some(serde_json::json!({"lsn":lsn.to_string(),"value":event.payload})),
            },
        )?;
        Ok(())
    }
    /// 提交当前写事务。
    ///
    /// 这是整批事实唯一的持久化点：成功之后读者才能看到这批事实；失败则
    /// 调用方回滚整批。注入的同步延迟与失败用于演练崩溃窗口。
    pub fn sync(&mut self) -> Result<()> {
        self.data_syncs += 1;
        if let Some(notify) = self.faults.before_data_sync {
            notify(self.last_lsn);
        }
        if self.faults.sync_delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.faults.sync_delay_ms));
        }
        if self.faults.fail_data_sync == Some(self.data_syncs) {
            return Err(std::io::Error::other("injected SQLite commit failure").into());
        }
        let start = Instant::now();
        // COMMIT 的耗时即 fsync 成本，直接计入延迟直方图。
        self.conn.execute_batch("COMMIT").map_err(db)?;
        self.data_latency.record(start.elapsed());
        Ok(())
    }
    /// 回滚整批写入并把水位复位到批前值。
    ///
    /// 水位必须一起复位：否则回滚后水位会指向并不存在的事实，读面会认为
    /// 这些事实已经提交。
    pub fn rollback(&mut self) {
        let _ = self.conn.execute_batch("ROLLBACK");
        self.last_lsn = self.before_batch;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 钉住「磁盘写满」的失败语义：事实、当前行与提交水位必须一起回滚，
    /// 且重启后仍然是提交前的那一份权威，而不是半套数据。
    #[test]
    fn sqlite_full_rolls_back_facts_entities_and_commit_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut disk = Disk::open(dir.path(), FaultInjection::default()).unwrap();
        let tx = Transaction {
            v: 2,
            journal_id: disk.id.clone(),
            lsn: 1,
            tx_id: "confirmed".into(),
            events: vec![Event::new(EventKind::Command, serde_json::json!({}))],
        };
        disk.begin().unwrap();
        let body = codec::bounded_json(&tx, crate::MAX_LINE_BYTES).unwrap();
        disk.append(
            &tx,
            &[EntityChange {
                kind: crate::sqlite::EntityKind::Run,
                key: "r".into(),
                value: Some(serde_json::json!({"status":"before"})),
            }],
            true,
            body.len() as u64,
            &codec::digest(&body),
        )
        .unwrap();
        disk.sync().unwrap();
        let pages: i64 = disk
            .conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        disk.conn
            .pragma_update(None, "max_page_count", pages)
            .unwrap();
        let mut candidate = tx.clone();
        candidate.lsn = 2;
        candidate.tx_id = "too-large".into();
        candidate.events[0].payload = Value::String("x".repeat(512 * 1024));
        disk.begin().unwrap();
        let body = codec::bounded_json(&candidate, crate::MAX_LINE_BYTES).unwrap();
        let error = disk
            .append(
                &candidate,
                &[],
                true,
                body.len() as u64,
                &codec::digest(&body),
            )
            .unwrap_err();
        assert!(error.to_string().contains("full"), "{error}");
        disk.rollback();
        drop(disk);
        let disk = Disk::open(dir.path(), FaultInjection::default()).unwrap();
        assert_eq!(disk.last_lsn, 1);
        assert_eq!(
            entity(&disk.conn, "run", "r").unwrap().unwrap()["status"],
            "before"
        );
        let count: i64 = disk
            .conn
            .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
