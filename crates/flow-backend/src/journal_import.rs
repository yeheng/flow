//! Offline legacy preservation. Legacy executions are read-only baselines: missing actual
//! inputs are never fabricated and importing never dispatches an old external operation.
//!
//! 本模块把停写的 legacy（v1 SQLite）数据集保全成只读基线。
//! 它守护的核心不变量是「不伪造、不重放」：缺失的真实输入宁可标记为
//! 不可恢复也不编造，导入过程绝不派发旧的外部操作。
//! 需要它，是因为 legacy 数据是历史证据——一旦被改写或被重新执行，
//! 就再也无法证明当时究竟发生了什么。
use crate::journal::JournalBackend;
use flow_journal::{Event, EventKind, StoredValue};
use futures::TryStreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Column, Row, TypeInfo, ValueRef as _};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// 扫描 legacy 事件日志，产出「这份日志能证明什么」的报告。
///
/// 逐行统计：缺行/超长行、非法记录、序号断档、异主记录与终态。
/// 报告是只读事实的描述，不是修补动作——legacy 的原始状态无法重建。
fn log_report(path: &Path, run_id: &str, status: &Value) -> Result<Value> {
    use std::io::BufRead;
    let mut report = json!({"missing":!path.exists(),"partial_or_oversize":false,"invalid_records":0,"sequence_gaps":0,"foreign_run_records":0,"last_seq":0,"terminal_status":null,"metadata_conflict":false});
    if !path.exists() {
        return Ok(report);
    }
    let mut reader = std::io::BufReader::new(File::open(path)?);
    let mut last = 0u64;
    loop {
        let mut line = Vec::new();
        // 最多读 MAX_LINE_BYTES+1 字节：多一个字节就能区分「正好是上限」
        // 与「超长行」，超长行立即判为残缺并不再继续解析。
        let n = Read::by_ref(&mut reader)
            .take(flow_journal::MAX_LINE_BYTES as u64 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if n > flow_journal::MAX_LINE_BYTES || line.last() != Some(&b'\n') {
            report["partial_or_oversize"] = json!(true);
            break;
        }
        let Ok(event) = serde_json::from_slice::<Value>(&line) else {
            report["invalid_records"] = json!(report["invalid_records"].as_u64().unwrap_or(0) + 1);
            continue;
        };
        if event["run_id"] != run_id {
            report["foreign_run_records"] =
                json!(report["foreign_run_records"].as_u64().unwrap_or(0) + 1);
        }
        if let Some(seq) = event["seq"].as_u64() {
            if seq != last + 1 {
                report["sequence_gaps"] = json!(report["sequence_gaps"].as_u64().unwrap_or(0) + 1);
            }
            last = seq;
        } else {
            report["invalid_records"] = json!(report["invalid_records"].as_u64().unwrap_or(0) + 1);
        }
        match event["type"].as_str() {
            Some("run_completed") => report["terminal_status"] = json!("succeeded"),
            Some("run_failed") => report["terminal_status"] = json!("failed"),
            Some("run_cancelled") => report["terminal_status"] = json!("cancelled"),
            _ => {}
        }
    }
    report["last_seq"] = json!(last.to_string());
    report["metadata_conflict"] =
        json!(!report["terminal_status"].is_null() && report["terminal_status"] != *status);
    Ok(report)
}

/// 计算一个文件的 SHA-256 摘要（清单与复制校验共用）。
fn hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
/// 递归列出 legacy 快照的全部文件及其摘要。
///
/// 拒绝符号链接：链接可能指向快照之外，复制它会绕过证据保全的边界。
fn files(root: &Path, current: &Path, out: &mut BTreeMap<String, String>) -> Result<()> {
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err("legacy snapshot contains symlink".into());
        }
        if kind.is_dir() {
            files(root, &entry.path(), out)?;
        } else if kind.is_file() {
            let name = entry
                .path()
                .strip_prefix(root)?
                .to_str()
                .ok_or("non-UTF8 legacy path")?
                .to_owned();
            if name.ends_with("-shm") {
                // -shm 是 WAL 的共享内存索引，不是证据：它可重建，
                // 也不该被当成源数据保全。
                continue;
            }
            if out.len() >= 100_000 {
                // 清单有文件数上限：超大快照直接拒绝，而不是把内存吃光。
                return Err("legacy snapshot inventory exceeds 100000 files".into());
            }
            out.insert(name, hash(&entry.path())?);
        }
    }
    Ok(())
}
/// 按清单把源证据复制到目标目录（可重复执行、内容一致则跳过）。
///
/// 复制必须「发布式」：先写临时文件、校验摘要，再用硬链接原子发布。
/// 崩溃只会留下未发布的临时文件，不会出现半份证据冒充完整证据。
fn copy_evidence(source: &Path, target: &Path, inventory: &BTreeMap<String, String>) -> Result<()> {
    for (name, digest) in inventory {
        let destination = target.join(name);
        if destination.exists() {
            if hash(&destination)? != *digest {
                // 目标已存在但内容不同：说明目标不是这份快照的副本，
                // 继续写会破坏既有证据。
                return Err("existing legacy backup differs".into());
            }
            continue;
        }
        let parent = destination
            .parent()
            .ok_or("legacy copy path has no parent")?;
        std::fs::create_dir_all(parent)?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        // A crash leaves only an unpublished temporary file. Never expose partial evidence.
        let temporary = destination.with_file_name(format!(".flow-copy-{}", uuid::Uuid::now_v7()));
        let mut output = options.open(&temporary)?;
        let mut input = File::open(source.join(name))?;
        std::io::copy(&mut input, &mut output)?;
        output.sync_all()?;
        if hash(&temporary)? != *digest {
            return Err("source changed while backing up".into());
        }
        // hard_link publishes without replacing existing evidence, even on a race.
        std::fs::hard_link(&temporary, &destination)?;
        std::fs::remove_file(&temporary)?;
        let parent = destination
            .parent()
            .ok_or("legacy copy path has no parent")?;
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
/// 把一条 legacy 记录写成基线事实（幂等：内容不同则拒绝）。
///
/// 幂等键是 key 的摘要，因此中断后重跑会命中同一条命令；
/// 已存在且内容不同则报错——基线是不可改写的证据。
async fn baseline(b: &JournalBackend, key: &str, data: Value) -> Result<()> {
    let request = json!({"key":key,"data":data});
    if let Some(existing) = b.inspect(|state| state.legacy.get(key).cloned()).await {
        let stored: StoredValue = serde_json::from_value(existing["data"].clone())?;
        let original = flow_journal::value::materialize(
            b.journal.root(),
            b.journal.durable_lsn(),
            &stored,
            flow_journal::VALUE_BUDGET,
        )?;
        if original != data {
            return Err(format!("legacy baseline differs: {key}").into());
        }
        return Ok(());
    }
    let mut mapped = Vec::new();
    let mut values = Vec::new();
    if key.starts_with("workflows/") {
        mapped.push(Event::new(EventKind::WorkflowCreated,json!({"workflow_id":data["id"],"name":data["name"],"created_at":data["created_at"],"versions":{}})));
    } else if key.starts_with("workflow_versions/") {
        let definition: Value = serde_json::from_str(
            data["definition"]
                .as_str()
                .ok_or("legacy definition must be JSON text")?,
        )?;
        flow_engine::journal_state::validate_definition(&definition)?;
        let prepared = b
            .journal
            .prepare_json(definition, flow_journal::MAX_LINE_BYTES)
            .await?;
        let definition = prepared.stored().clone();
        values.push(prepared);
        mapped.push(Event::new(EventKind::WorkflowUpdated,json!({"workflow_id":data["workflow_id"],"version":{"version":data["version"],"definition":definition,"checksum":data["checksum"],"published":data["status"]=="published","created_at":data["created_at"]}})));
    } else if key.starts_with("schedules/") || key.starts_with("webhooks/") {
        let mut config = data.clone();
        // legacy 触发器一律导入为停用：legacy 的「已启用」不等于现在
        // 应该继续触发，续跑必须由人显式确认。
        config["enabled"] = json!(false);
        config["legacy_requires_activation"] = json!(true);
        if key.starts_with("schedules/") {
            config["input"] = match data["input"].as_str() {
                Some(raw) => serde_json::from_str(raw)?,
                None => Value::Null,
            };
        }
        mapped.push(Event::new(
            if key.starts_with("schedules/") {
                EventKind::ScheduleChanged
            } else {
                EventKind::WebhookChanged
            },
            config,
        ));
    }
    let prepared = b
        .journal
        .prepare_json(data, flow_journal::VALUE_BUDGET)
        .await?;
    let stored = prepared.stored().clone();
    values.push(prepared);
    b.command_selected(
        "legacy.import",
        Some(&flow_journal::codec::digest(key.as_bytes())),
        &request,
        None,
        values,
        |_| {
            Ok((
                {
                    mapped.push(Event::new(
                        EventKind::LegacyImport,
                        json!({"key":key,"data":stored,"integrity":"legacy_unproven"}),
                    ));
                    mapped
                },
                json!({"key":key}),
            ))
        },
    )
    .await?;
    Ok(())
}
/// 把一行 legacy SQLite 记录转成 JSON 值。
///
/// 只支持标量类型：legacy 表里没有 BLOB 语义，解析不了的列直接报错，
/// 而不是塞一个占位值冒充原始数据。
fn row_value(row: &sqlx::sqlite::SqliteRow) -> Result<Value> {
    let mut value = serde_json::Map::new();
    for column in row.columns() {
        let raw = row.try_get_raw(column.ordinal())?;
        let item = if raw.is_null() {
            Value::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" => json!(row.try_get::<i64, _>(column.ordinal())?),
                "REAL" => json!(row.try_get::<f64, _>(column.ordinal())?),
                "TEXT" => json!(row.try_get::<String, _>(column.ordinal())?),
                _ => return Err("unsupported legacy SQLite column type".into()),
            }
        };
        value.insert(column.name().into(), item);
    }
    Ok(Value::Object(value))
}

/// 导入一个停写的 legacy 数据集为新的只读基线数据集。
///
/// 流程：离线锁住源目录 → 盘点文件清单 → 拷贝证据副本 → 在副本上
/// checkpoint WAL 后只读读取 → 逐表写入基线事实 → 原样保留事件日志字节。
/// 源目录全程只读，任何写入都发生在副本或目标数据集里。
pub async fn import(source: &Path, db: &Path, destination: &Path) -> Result<Value> {
    let source = source.canonicalize()?;
    let db = db.canonicalize()?;
    let relative_db = db.strip_prefix(&source)?.to_path_buf();
    if destination.exists() && destination.canonicalize()?.starts_with(&source) {
        return Err("destination must be outside legacy source".into());
    }
    let parent = destination.parent().unwrap_or(Path::new("."));
    if parent.canonicalize()?.starts_with(&source) {
        return Err("destination must be outside legacy source".into());
    }
    let _lock = flow_journal::maintenance::offline_lock(&source)?;
    let mut inventory = BTreeMap::new();
    files(&source, &source, &mut inventory)?;
    // 源身份 = 清单指纹：中断后重跑时若源变了，指纹不同即被拒绝。
    let identity = flow_journal::codec::fingerprint(&json!(inventory), 16 * 1024 * 1024)?;
    let backend = JournalBackend::open(destination, Default::default()).await?;
    let result: Result<Value> = async {
    if backend.inspect(|state| state.legacy.is_empty() && (!state.workflows.is_empty() || !state.runs.is_empty() || !state.commands.is_empty())).await {
        // 目标数据集已有非 legacy 内容：拒绝在其上叠加导入，
        // 否则两套来源的事实会混在一个库里。
        return Err("legacy import requires a new dataset or the same unfinished import".into());
    }
    // Fixed request identity rejects a changed source during interrupted-import resume.
    baseline(&backend,"source",json!({"source_id":identity,"database":relative_db,"files":inventory})).await?;
    let backup=destination.join("legacy-source");
    copy_evidence(&source,&backup,&inventory)?;
    // 源库可能带着崩溃遗留的 -wal（清单允许没有 -shm）：只读打开无法跑
    // WAL 恢复会直接失败。先以可写打开**副本**（不动源）checkpoint 归并
    // WAL，再切换只读读取——源证据保持字节原样。
    let working = destination.join(format!(".legacy-working-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&working)?;
    let _working_cleanup = WorkingCopy(working.clone());
    copy_evidence(&backup, &working, &inventory)?;
    {
        let recover=sqlx::sqlite::SqlitePoolOptions::new().max_connections(1).connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(working.join(&relative_db))).await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)").execute(&recover).await?;
        recover.close().await;
    }
    let pool=sqlx::sqlite::SqlitePoolOptions::new().max_connections(1).connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(working.join(&relative_db)).read_only(true)).await?;
    let mut snapshot=pool.begin().await?;
    let mut counts=BTreeMap::new();
    for (table,query) in [("workflows","SELECT * FROM workflows ORDER BY id"),("workflow_versions","SELECT * FROM workflow_versions ORDER BY workflow_id, version"),("runs","SELECT * FROM runs ORDER BY id"),("schedules","SELECT * FROM schedules ORDER BY id"),("webhooks","SELECT * FROM webhooks ORDER BY token"),("schedule_fires","SELECT * FROM schedule_fires ORDER BY schedule_id, fire_at")] {
        let exists: i64=sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?").bind(table).fetch_one(&mut *snapshot).await?;
        if exists==0 {counts.insert(table,0u64);continue;}
        let mut rows=sqlx::query(query).fetch(&mut *snapshot);let mut count=0u64;
        while let Some(row)=rows.try_next().await? {
            let value=row_value(&row)?;
            let key=match table {"workflow_versions"=>format!("{}:{}",value["workflow_id"].as_str().ok_or("version workflow")?,value["version"]),"schedule_fires"=>format!("{}:{}",value["schedule_id"].as_str().ok_or("fire schedule")?,value["fire_at"].as_str().ok_or("fire time")?),"webhooks"=>value["token"].as_str().ok_or("webhook token")?.to_owned(),_=>value["id"].as_str().ok_or("legacy row id")?.to_owned()};
            baseline(&backend,&format!("{table}/{key}"),value.clone()).await?;
            if table=="runs" {
                // Preserve the original status and bytes separately from migration completeness.
                if Path::new(&key).components().count()!=1 || key==".." {return Err("unsafe legacy run identity".into());}
                let log=PathBuf::from("runs").join(&key).join("event.jsonl");
                let evidence=log_report(&backup.join(&log),&key,&value["status"])?;
                // 报告明确区分「原始状态」与「迁移完整性」：真实的节点输入
                // 已经不可恢复，只能标记为缺失并要求人工处理。
                let report=json!({"run_id":key,"metadata_status":value["status"],"actual_node_inputs":"missing_unrecoverable","audit_integrity":"legacy_unproven","log":evidence,"execution_mapping":"read_only_legacy","requires_manual_resolution":!matches!(value["status"].as_str(),Some("succeeded"|"failed"|"cancelled"))});
                baseline(&backend,&format!("reports/{key}"),report).await?;
            }
            count+=1;
        }
        counts.insert(table,count);
    }
    snapshot.commit().await?;pool.close().await;
    // Every original log byte, including corrupt/partial lines, becomes authority. Fragment
    // records use fixed ValueRefs and offsets, never a chunk-manifest tree.
    for (name,digest) in &inventory {
        if !name.ends_with("event.jsonl"){continue;}
        let path=backup.join(name);let mut file=tokio::fs::File::open(&path).await?;
        let length=std::fs::metadata(&path)?.len();let mut offset=0u64;
        use tokio::io::AsyncReadExt;
        while offset<length {
            let key=format!("raw/{name}/{offset}");let request_id=flow_journal::codec::digest(key.as_bytes());
            let n=(length-offset).min(128*1024*1024);
            if let Some(existing) = backend.inspect(|state| state.legacy.get(&key).cloned()).await {
                if existing["source_digest"] != *digest || existing["path"] != *name || existing["offset"].as_str().and_then(|s| s.parse::<u64>().ok()) != Some(offset) {
                    return Err("legacy raw fragment differs".into());
                }
use tokio::io::AsyncSeekExt;file.seek(std::io::SeekFrom::Start(offset+n)).await?;offset+=n;continue;}
            let mut fragment=(&mut file).take(n);
            let stored=StoredValue::Ref(backend.store_stream(&mut fragment,flow_journal::value::ValueCodec::Bytes).await?);
            backend.command("legacy.raw",Some(&request_id),&json!({"source":identity,"path":name,"offset":offset,"bytes":n}),|_|Ok((vec![Event::new(EventKind::LegacyImport,json!({"key":key,"data":stored,"path":name,"offset":offset.to_string(),"source_digest":digest,"integrity":"legacy_unproven"}))],json!({"key":key})))).await?;
            offset+=n;
        }
    }
    let report=json!({"source_id":identity,"counts":counts,"production_switched":false,"legacy_executions":"read_only","actual_node_inputs":"missing_unrecoverable"});
    baseline(&backend,"complete",report.clone()).await?;
    Ok(report)
    }.await;
    backend.close().await?;
    result
}

/// 临时工作副本的守卫：离开作用域即删除目录。
struct WorkingCopy(PathBuf);
impl Drop for WorkingCopy {
    /// 无论导入成功与否都清理副本目录，避免残留的证据副本被误当成源。
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
