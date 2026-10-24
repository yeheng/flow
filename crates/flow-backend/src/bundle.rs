//! Versioned, read-only run evidence. Reading a bundle never starts an executor.
//!
//! 本模块实现证据包（bundle）：把一个 run 及其引用闭包导出成带清单的
//! 目录，并能用同一个 reducer 离线还原出只读状态。
//! 它守护的核心不变量是「证据只读且可验证」：正文按摘要命名、事件带
//! 原始位置、清单在全部证据写完并校验通过后才发布。
//! 需要它，是因为一次运行的证据必须能在原环境消失之后仍然可读、可验，
//! 且还原过程绝不能启动执行器或撤回任何外部效果。
use flow_engine::journal_state::{Operation, Prepared, Run, State, Wait};
use flow_journal::{codec, Event, EventKind, Transaction, ValueRef};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, Read, Write},
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// 事件正文预算：导出上限 64 MiB。
const FACT_BUDGET: usize = 64 * 1024 * 1024;
/// 引用正文总预算：导出上限 1 GiB。
const BODY_BUDGET: u64 = 1024 * 1024 * 1024;

/// 证据包清单：版本、run 闭包、事件摘要与正文引用。
///
/// `events_digest` 覆盖 events.jsonl 的全部字节；`values` 是正文引用的
/// 完整闭包——两者共同构成「包没被改过」的证明。
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub journal_id: String,
    pub root_run: String,
    pub upper_lsn: String,
    pub runs: BTreeSet<String>,
    pub events: usize,
    pub events_digest: String,
    pub values: BTreeMap<String, ValueRef>,
}

/// 创建一个仅所有者可访问的目录（unix 下 0700）。
fn private_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

/// 以「只新建、不覆盖」的方式创建文件（unix 下 0600）。
fn new_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// 正文在包内的路径：由摘要派生，不用原始 output_id 做路径名。
fn body_path(root: &Path, id: &str) -> PathBuf {
    root.join("values").join(codec::digest(id.as_bytes()))
}

/// 登记一个正文引用；同一 output_id 出现不同描述符即为冲突。
fn reference(reference: ValueRef, found: &mut BTreeMap<String, ValueRef>) -> Result<()> {
    reference.validate()?;
    if found
        .get(&reference.output_id)
        .is_some_and(|old| old != &reference)
    {
        return Err("conflicting bundle reference".into());
    }
    found.insert(reference.output_id.clone(), reference);
    Ok(())
}

/// 登记一个存储值里的正文引用（内联值没有引用）。
fn stored(value: &flow_journal::StoredValue, found: &mut BTreeMap<String, ValueRef>) -> Result<()> {
    if let flow_journal::StoredValue::Ref(r) = value {
        reference(r.clone(), found)?;
    }
    Ok(())
}

/// 登记一个外部操作结果的引用（含其内联的原始响应正文）。
fn outcome(
    value: &flow_journal::StoredValue,
    found: &mut BTreeMap<String, ValueRef>,
) -> Result<()> {
    stored(value, found)?;
    if let flow_journal::StoredValue::Inline(v) = value {
        if let Some(raw) = v.get("body_raw") {
            reference(serde_json::from_value(raw.clone())?, found)?;
        }
    }
    Ok(())
}

/// 登记一个准备阶段记录里的全部引用。
fn prepared(value: &Prepared, found: &mut BTreeMap<String, ValueRef>) -> Result<()> {
    stored(&value.input, found)?;
    stored(&value.params, found)?;
    for v in value.predecessors.values() {
        stored(v, found)?;
    }
    Ok(())
}
// Only typed engine fields carry references. An inline business object that looks
// like a descriptor is still user data, not a dependency of the evidence bundle.
fn references(event: &Event, found: &mut BTreeMap<String, ValueRef>) -> Result<()> {
    let p = &event.payload;
    match event.kind {
        EventKind::RunStarted => {
            let run: Run = serde_json::from_value(p.clone())?;
            stored(&run.input, found)?;
            stored(&run.definition, found)?;
            for v in run.output.iter().chain(run.signals.values()) {
                stored(v, found)?;
            }
            for n in run.nodes.values() {
                for v in n.output.iter() {
                    stored(v, found)?;
                }
                if let Some(v) = &n.prepared {
                    prepared(v, found)?;
                }
                if let Some(o) = &n.operation {
                    stored(&o.request, found)?;
                    if let Some(v) = &o.outcome {
                        outcome(v, found)?;
                    }
                }
                if let Some(wait) = &n.wait {
                    if let Some(v) = wait.output() {
                        stored(v, found)?;
                    }
                }
            }
            if let Some(e) = &run.execution {
                for d in e.definitions.values() {
                    stored(&d.definition, found)?;
                }
                if let Some(r) = &e.regression {
                    for f in r.fixtures.values() {
                        outcome(&f.outcome, found)?;
                    }
                    for v in r.signals.values() {
                        stored(v, found)?;
                    }
                }
            }
        }
        EventKind::InputPrepared => prepared(
            &serde_json::from_value::<Prepared>(p["prepared"].clone())?,
            found,
        )?,
        EventKind::OperationIntent | EventKind::OperationAuthorized => stored(
            &serde_json::from_value::<Operation>(p["operation"].clone())?.request,
            found,
        )?,
        EventKind::OperationOutcome => {
            outcome(&serde_json::from_value(p["outcome"].clone())?, found)?
        }
        EventKind::ValuePublished => reference(serde_json::from_value(p.clone())?, found)?,
        EventKind::SignalReceived => stored(&serde_json::from_value(p["payload"].clone())?, found)?,
        EventKind::NodeCompleted
        | EventKind::RunCompleted
        | EventKind::WaitResolved
        | EventKind::Adjudicated => stored(&serde_json::from_value(p["output"].clone())?, found)?,
        EventKind::WaitRegistered => {
            let wait: Wait = serde_json::from_value(p["wait"].clone())?;
            if let Some(v) = wait.output() {
                stored(v, found)?;
            }
        }
        EventKind::NodeFailed => {
            if let Some(input) = p.get("original_input") {
                stored(&serde_json::from_value(input.clone())?, found)?;
            }
            if let Some(pred) = p["predecessors"].as_object() {
                for v in pred.values() {
                    stored(&serde_json::from_value(v.clone())?, found)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Call with the data-directory offline lock held. A partial export has no manifest.
///
/// 导出一个 run 及其子 run 的引用闭包。
///
/// 顺序不可颠倒：先写正文与事件，再校验，最后才发布 manifest——
/// 没有 manifest 的目录只是未完成的导出，不会被误当成有效证据包。
pub fn export(root: &Path, run_id: &str, destination: &Path) -> Result<Manifest> {
    let conn = flow_journal::sqlite::read_connection(root)?;
    let (identity, upper) = flow_journal::sqlite::metadata(&conn)?;
    let mut runs = BTreeSet::new();
    let mut todo = vec![run_id.to_owned()];
    // 事实按 (LSN, 事件序号) 定位：还原时必须保留原始位置。
    let mut facts = BTreeMap::<(u64, usize), Event>::new();
    let mut bytes = 0;
    while let Some(run) = todo.pop() {
        if !runs.insert(run.clone()) {
            continue;
        }
        if runs.len() > 1000 {
            return Err("bundle exceeds 1000 runs".into());
        }
        let mut tail = flow_journal::tail::TailReader::for_run(root, &identity, &run);
        let mut found = false;
        while let Some((tx, _)) = tail.next(upper)? {
            for (index, event) in tx.events.into_iter().enumerate() {
                if event.kind == EventKind::RunStarted && event.payload["parent"]["run_id"] == run {
                    // 子 run 的创建事实在父 run 的事务里：顺着它收齐子闭包。
                    if let Some(child) = &event.run_id {
                        todo.push(child.clone());
                    }
                }
                if event.run_id.as_deref() == Some(&run) {
                    found |= event.kind == EventKind::RunStarted;
                    bytes += codec::bounded_json(&event, flow_journal::MAX_LINE_BYTES)?.len();
                    if bytes > FACT_BUDGET {
                        return Err("bundle fact budget exceeded (64 MiB)".into());
                    }
                    facts.insert((tx.lsn, index), event);
                }
            }
        }
        if !found {
            // 没有创建事实：这个 run 不属于本数据集，不能凭空造一条。
            return Err(format!("run {run} has no creation fact").into());
        }
    }
    let mut values = BTreeMap::new();
    for event in facts.values() {
        references(event, &mut values)?;
    }
    if values.len() > 10000
        || values
            .values()
            .try_fold(0u64, |n, v| n.checked_add(v.total_bytes))
            .is_none_or(|n| n > BODY_BUDGET)
    {
        return Err("bundle value budget exceeded".into());
    }
    private_dir(destination)?;
    private_dir(&destination.join("values"))?;
    for (id, reference) in &values {
        let mut file = new_file(&body_path(destination, id))?;
        flow_journal::value::read_value(root, upper, reference, true, &mut file)?;
        file.sync_all()?;
    }
    let mut output = new_file(&destination.join("events.jsonl"))?;
    let mut hash = Sha256::new();
    for ((lsn, event_index), event) in &facts {
        let positioned = flow_journal::page::PositionedEvent {
            lsn: *lsn,
            event_index: *event_index,
            event: event.clone(),
        };
        let mut line = codec::bounded_json(&positioned, flow_journal::MAX_LINE_BYTES)?;
        line.push(b'\n');
        output.write_all(&line)?;
        hash.update(line);
    }
    output.sync_all()?;
    let manifest = Manifest {
        version: 1,
        journal_id: identity,
        root_run: run_id.into(),
        upper_lsn: upper.to_string(),
        runs,
        events: facts.len(),
        events_digest: hex::encode(hash.finalize()),
        values,
    };
    // Publish completion only after all evidence has been written and checked.
    verify(destination, &manifest)?;
    let mut file = new_file(&destination.join("manifest.json"))?;
    serde_json::to_writer_pretty(&mut file, &manifest)?;
    file.sync_all()?;
    std::fs::File::open(destination)?.sync_all()?;
    Ok(manifest)
}

/// Verify the complete reference closure, then fold facts with the normal reducer.
/// Synthetic contiguous transaction numbers are local to this read-only view;
/// events.jsonl retains the original positions. No database is created or repaired.
///
/// 校验并还原一个证据包：核对摘要、顺序、边界与引用闭包，然后用同一个
/// reducer 折叠出只读状态。不会创建或修复任何数据库。
pub fn inspect(root: &Path) -> Result<(Manifest, State)> {
    let mut bytes = Vec::new();
    std::fs::File::open(root.join("manifest.json"))?
        .take((flow_journal::MAX_LINE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > flow_journal::MAX_LINE_BYTES {
        return Err("bundle manifest exceeds 1 MiB".into());
    }
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    let state = verify(root, &manifest)?;
    Ok((manifest, state))
}

/// 校验证据包的完整性并用 reducer 还原状态。
///
/// 校验项：版本与预算、每个正文的字节数与摘要、事件条数与整体摘要、
/// 事实顺序与边界、run 归属、引用闭包。
fn verify(root: &Path, manifest: &Manifest) -> Result<State> {
    if manifest.version != 1
        || manifest.events > 1_000_000
        || manifest.runs.len() > 1000
        || manifest.values.len() > 10000
    {
        return Err("unsupported bundle version or budget".into());
    }
    let mut total = 0u64;
    for (id, reference) in &manifest.values {
        reference.validate()?;
        if reference.journal_id != manifest.journal_id || id != &reference.output_id {
            return Err("bundle descriptor identity mismatch".into());
        }
        total = total
            .checked_add(reference.total_bytes)
            .ok_or("bundle size overflow")?;
        if total > BODY_BUDGET {
            return Err("bundle body budget exceeded".into());
        }
        // 多读一个字节：正好等于声明长度且摘要相同才算完整。
        let mut file = std::fs::File::open(body_path(root, id))?.take(reference.total_bytes + 1);
        let mut hash = Sha256::new();
        let count = std::io::copy(&mut file, &mut HashSink(&mut hash))?;
        if count != reference.total_bytes || hex::encode(hash.finalize()) != reference.digest {
            return Err(format!("bundle body checksum mismatch: {id}").into());
        }
    }
    let mut reader = std::io::BufReader::new(std::fs::File::open(root.join("events.jsonl"))?);
    let mut hash = Sha256::new();
    let mut facts = Vec::new();
    let mut total = 0;
    loop {
        let mut line = Vec::new();
        let n = reader
            .by_ref()
            .take((flow_journal::MAX_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        total += n;
        if total > FACT_BUDGET || n > flow_journal::MAX_LINE_BYTES || !line.ends_with(b"\n") {
            return Err("bundle line/fact budget exceeded".into());
        }
        hash.update(&line);
        facts.push(serde_json::from_slice::<flow_journal::page::PositionedEvent>(&line)?);
    }
    if facts.len() != manifest.events || hex::encode(hash.finalize()) != manifest.events_digest {
        return Err("bundle event checksum mismatch".into());
    }
    let mut found = BTreeMap::new();
    let mut previous = None;
    let mut publications = BTreeSet::new();
    for fact in &facts {
        let position = (fact.lsn, fact.event_index);
        if previous.is_some_and(|p| p >= position)
            || fact.lsn > manifest.upper_lsn.parse::<u64>()?
        {
            return Err("bundle fact order/boundary mismatch".into());
        }
        previous = Some(position);
        if !fact
            .event
            .run_id
            .as_ref()
            .is_some_and(|id| manifest.runs.contains(id))
        {
            return Err("foreign run in bundle".into());
        }
        references(&fact.event, &mut found)?;
        if fact.event.kind == EventKind::ValuePublished {
            publications.insert(
                fact.event.payload["output_id"]
                    .as_str()
                    .ok_or("publication identity missing")?
                    .to_owned(),
            );
        }
    }
    if found != manifest.values {
        return Err("bundle reference closure mismatch".into());
    }
    let mut state = State {
        version: 1,
        journal_id: manifest.journal_id.clone(),
        ..Default::default()
    };
    for reference in manifest
        .values
        .values()
        .filter(|r| !publications.contains(&r.output_id))
    {
        // 有发布事实的引用由 reducer 自己登记；其余的必须预置，
        // 否则 reducer 会认为这些正文不存在。
        state.values.remember(reference.clone())?;
    }
    let mut groups = BTreeMap::<u64, Vec<Event>>::new();
    for fact in facts {
        groups.entry(fact.lsn).or_default().push(fact.event);
    }
    for events in groups.into_values() {
        // 还原用本地连续事务号：原始 LSN 保留在 events.jsonl 里，
        // 但事务号必须连续，reducer 才认这批事实。
        state.apply(&Transaction {
            v: 2,
            journal_id: manifest.journal_id.clone(),
            lsn: state.applied_lsn + 1,
            tx_id: format!("bundle:{}", state.applied_lsn + 1),
            events,
        })?;
    }
    if !state.runs.contains_key(&manifest.root_run)
        || state.runs.keys().cloned().collect::<BTreeSet<_>>() != manifest.runs
    {
        // 还原出的 run 集合必须与清单完全一致：多一个说明包里有未申报的
        // 事实，少一个说明证据不完整。
        return Err("bundle run closure mismatch".into());
    }
    Ok(state)
}

/// 把写入的内容直接喂给摘要器，避免再拷贝一份。
struct HashSink<'a>(&'a mut Sha256);
impl Write for HashSink<'_> {
    /// 只更新摘要：写入内容不需要真正落盘。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    /// 无缓冲，无需刷新。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
