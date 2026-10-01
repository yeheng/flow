//! Offline legacy preservation. Legacy executions are read-only baselines: missing actual
//! inputs are never fabricated and importing never dispatches an old external operation.
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
            report["invalid_records"] = json!(report["invalid_records"].as_u64().unwrap() + 1);
            continue;
        };
        if event["run_id"] != run_id {
            report["foreign_run_records"] =
                json!(report["foreign_run_records"].as_u64().unwrap() + 1);
        }
        if let Some(seq) = event["seq"].as_u64() {
            if seq != last + 1 {
                report["sequence_gaps"] = json!(report["sequence_gaps"].as_u64().unwrap() + 1);
            }
            last = seq;
        } else {
            report["invalid_records"] = json!(report["invalid_records"].as_u64().unwrap() + 1);
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
                continue;
            }
            if out.len() >= 100_000 {
                return Err("legacy snapshot inventory exceeds 100000 files".into());
            }
            out.insert(name, hash(&entry.path())?);
        }
    }
    Ok(())
}
fn copy_evidence(source: &Path, target: &Path, inventory: &BTreeMap<String, String>) -> Result<()> {
    for (name, digest) in inventory {
        let destination = target.join(name);
        if destination.exists() {
            if hash(&destination)? != *digest {
                return Err("existing legacy backup differs".into());
            }
            continue;
        }
        std::fs::create_dir_all(destination.parent().unwrap())?;
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
        File::open(destination.parent().unwrap())?.sync_all()?;
    }
    Ok(())
}
async fn baseline(b: &JournalBackend, key: &str, data: Value) -> Result<()> {
    let request = json!({"key":key,"data":data});
    let request_id = flow_journal::codec::digest(key.as_bytes());
    if b.command_status("legacy.import", &request_id)
        .await?
        .is_some()
    {
        b.command("legacy.import", Some(&request_id), &request, |_| {
            unreachable!("existing import decision")
        })
        .await?;
        return Ok(());
    }
    let mut mapped = Vec::new();
    if key.starts_with("workflows/") {
        mapped.push(Event::new(EventKind::WorkflowCreated,json!({"workflow_id":data["id"],"name":data["name"],"created_at":data["created_at"],"versions":{}})));
    } else if key.starts_with("workflow_versions/") {
        let definition: Value = serde_json::from_str(
            data["definition"]
                .as_str()
                .ok_or("legacy definition must be JSON text")?,
        )?;
        flow_engine::journal_state::validate_definition(&definition)?;
        let definition =
            flow_journal::value::store_json(&b.journal, definition, flow_journal::MAX_LINE_BYTES)
                .await?;
        mapped.push(Event::new(EventKind::WorkflowUpdated,json!({"workflow_id":data["workflow_id"],"version":{"version":data["version"],"definition":definition,"checksum":data["checksum"],"published":data["status"]=="published","created_at":data["created_at"]}})));
    } else if key.starts_with("schedules/") || key.starts_with("webhooks/") {
        let mut config = data.clone();
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
    let stored = flow_journal::value::store_json(&b.journal, data, 8 * 1024 * 1024).await?;
    b.command(
        "legacy.import",
        Some(&flow_journal::codec::digest(key.as_bytes())),
        &request,
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
    let identity = flow_journal::codec::fingerprint(&json!(inventory), 16 * 1024 * 1024)?;
    let backend = JournalBackend::open(destination, Default::default()).await?;
    let result: Result<Value> = async {
    let state=backend.state().await;
    if state.legacy.is_empty() && (!state.workflows.is_empty() || !state.runs.is_empty() || !state.commands.is_empty()) {return Err("legacy import requires a new dataset or the same unfinished import".into());}
    drop(state);
    // Fixed request identity rejects a changed source during interrupted-import resume.
    baseline(&backend,"source",json!({"source_id":identity,"database":relative_db,"files":inventory})).await?;
    let backup=destination.join("legacy-source");
    copy_evidence(&source,&backup,&inventory)?;
    let pool=sqlx::sqlite::SqlitePoolOptions::new().max_connections(1).connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(backup.join(&relative_db)).read_only(true)).await?;
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
            if backend.command_status("legacy.raw",&request_id).await?.is_some(){use tokio::io::AsyncSeekExt;file.seek(std::io::SeekFrom::Start(offset+n)).await?;offset+=n;continue;}
            let mut fragment=(&mut file).take(n);
            let stored=StoredValue::Ref(flow_journal::value::store_stream(&backend.journal,&mut fragment,flow_journal::value::ValueCodec::Bytes).await?);
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
