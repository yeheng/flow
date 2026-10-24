//! Current-state loading and entity changes for one atomic SQLite commit.
//!
//! 本模块负责两件事：把 SQLite 里的「当前行」还原成协调器的内存状态，
//! 以及把一次事务翻译成要写入 `current_entities` 的行变更。
//! 它守护的核心不变量是「当前行必须可由事实重放得到」——投影检查
//! （`projection_check`）正是用同一个 reducer 重放事实来验证这一点，
//! 因此这里的行计算与 reducer 必须共用同一份规则。
//! 需要它，是因为权威库既要保存全部事实，又要能被快速读取；把两者的
//! 翻译规则收在一处，才能保证「事实 → 状态」永远只有一种解释。
use crate::journal::{audit_rows, project_rows, JournalBackend, JournalError};
use flow_engine::journal_state::{Run, State};
use flow_journal::{EventKind, StoredValue, Transaction, ValueRef};
use flow_store::store::{EntityChange, Store};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
type Result<T> = std::result::Result<T, JournalError>;

/// 把存储层错误包装成统一的后端错误。
fn db(e: flow_store::store::Error) -> JournalError {
    JournalError::Storage(e.to_string())
}

/// Normal recovery loads current control metadata and active executions only.
/// Accepted history, completed executions and old attempts are never replayed here.
///
/// 启动恢复只装载「控制元数据 + 在飞执行」，不回放已接受的历史、
/// 已完成的执行与旧尝试——回放会让启动代价随历史长度线性增长，
/// 而当前行本来就是事实的物化结果。
pub(crate) async fn load(index: &Store, identity: &str, upper: u64) -> Result<State> {
    if upper > 0 && !index.domain_mode().await.map_err(db)? {
        // 有事实但库不是协调器模式：说明这是别的主体的只读库或事实库，
        // 拿它当权威写下去会污染别人。
        return Err(JournalError::Storage(
            "fact-only database is not a coordinator database".into(),
        ));
    }
    let mut state = State {
        version: 1,
        journal_id: identity.into(),
        applied_lsn: upper,
        compact_audits: true,
        ..Default::default()
    };
    for kind in [
        "workflow",
        "schedule",
        "webhook",
        "template",
        "legacy",
        "schedule_fire",
        "run_workflow",
    ] {
        // 分页遍历当前行：实体数量无上界，必须游标分页而不是一次全取。
        let mut after = String::new();
        loop {
            let keys = index
                .current_keys(kind, None, &after, upper, 256)
                .await
                .map_err(db)?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let value = index
                    .get_summary(kind, key)
                    .await
                    .map_err(db)?
                    .1
                    .ok_or_else(|| {
                        JournalError::Storage("missing current control entity".into())
                    })?;
                if matches!(kind, "schedule" | "webhook" | "template") && !value.is_object() {
                    // 控制实体的行必须是对象：后续所有 patch 都按字段合并，
                    // 行畸形会让合并把无关字段写进配置。
                    return Err(flow_journal::Error::Invalid(format!(
                        "malformed {kind} row: expected object"
                    ))
                    .into());
                }
                match kind {
                    "workflow" => {
                        // 只把「最新版本」与「已发布版本」两个热版本装进内存，
                        // 其余历史版本留在 SQLite 里按需冷读。
                        let hot_versions: BTreeSet<u64> = ["latest_version", "published_version"]
                            .into_iter()
                            .filter_map(|field| value[field].as_u64())
                            .filter(|version| *version > 0)
                            .collect();
                        let mut workflow: flow_engine::journal_state::Workflow =
                            serde_json::from_value(value)?;
                        for version in hot_versions {
                            let row = index
                                .current_at("workflow_version", &version_key(key, version)?, upper)
                                .await
                                .map_err(db)?
                                .ok_or_else(|| {
                                    JournalError::Storage("missing hot definition version".into())
                                })?;
                            workflow
                                .versions
                                .insert(version, serde_json::from_value(row)?);
                        }
                        state.workflows.insert(key.clone(), workflow);
                    }
                    "schedule" => {
                        state.schedules.insert(key.clone(), value);
                    }
                    "webhook" => {
                        state.webhooks.insert(key.clone(), value);
                    }
                    "template" => {
                        state.templates.insert(key.clone(), value);
                    }
                    "legacy" => {
                        state.legacy.insert(key.clone(), value);
                    }
                    "schedule_fire" => {
                        state
                            .schedule_fires
                            .insert(key.clone(), serde_json::from_value(value)?);
                    }
                    "run_workflow" => {
                        state.run_workflows.insert(key.clone());
                    }
                    _ => {
                        return Err(JournalError::Storage(format!(
                            "unsupported control entity kind: {kind}"
                        )))
                    }
                }
            }
            after = keys.last().unwrap().clone();
        }
    }
    if let Some(meta) = index.get("engine", "epoch").await.map_err(db)?.1 {
        state.master_epoch = meta["epoch"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| JournalError::Storage("invalid master epoch".into()))?;
    }
    // 在飞 run 分页装载；超过 R_MAX 说明库与当前约束不符，宁可拒绝启动
    // 也不要在超预算的状态上继续调度。
    let mut after = String::new();
    loop {
        let ids = index.active_run_ids(&after).await.map_err(db)?;
        if ids.is_empty() {
            break;
        }
        for id in &ids {
            if state.runs.len() >= crate::journal::R_MAX {
                return Err(JournalError::Storage("active run capacity exceeded".into()));
            }
            if let Some(run) = run_at(index, id, upper, false).await? {
                state.runs.insert(id.clone(), run);
            }
        }
        after = ids.last().unwrap().clone();
    }
    Ok(state)
}

/// 读取某个 run 在 `upper` 时刻的当前行。
///
/// 读出的 LSN 必须等于请求的 `upper`：跨过决策边界读到的行可能属于
/// 另一个提交，拼起来的快照不是任何一个真实状态。
pub(crate) async fn run_at(index: &Store, id: &str, upper: u64, full: bool) -> Result<Option<Run>> {
    let (lsn, value) = if full {
        index.get("run", id).await.map_err(db)?
    } else {
        index.get_execution(id).await.map_err(db)?
    };
    if lsn != upper {
        return Err(JournalError::Storage(
            "current run read crossed decision boundary".into(),
        ));
    }
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(JournalError::from)
}

/// Load cold current objects before preview, while the coordinator owns the decision lock.
///
/// 提交前把「这次事务会碰到、但内存里还没有」的对象读进来：
/// 已发布定义、被引用的 run、历史 attempt 账本与正文描述符。
/// 必须提前装载，因为 reducer 是纯函数——它只能基于内存状态判断迁移
/// 是否合法，缺对象时只能拒绝，而不是替调用方去查库。
pub(crate) async fn prepare(backend: &JournalBackend, tx: &Transaction) -> Result<()> {
    let index = &backend.store;
    let upper = backend.inspect(|s| s.applied_lsn).await;
    for event in &tx.events {
        if event.kind == EventKind::WorkflowPublished {
            // 发布事件要更新「已发布版本」指针，必须先把该版本实体读进来。
            if let (Some(id), Some(version)) = (
                event.payload["workflow_id"].as_str(),
                event.payload["version"]
                    .as_str()
                    .and_then(|v| v.parse().ok())
                    .or_else(|| event.payload["version"].as_u64()),
            ) {
                load_version(backend, id, version).await?;
            }
        }
    }
    let ids: BTreeSet<_> = tx
        .events
        .iter()
        .filter_map(|e| e.run_id.as_deref())
        .collect();
    for id in ids {
        if backend.inspect(|s| !s.runs.contains_key(id)).await {
            if let Some(run) = run_at(index, id, upper, false).await? {
                backend
                    .update_state(|s| {
                        s.runs.insert(id.into(), run);
                    })
                    .await;
            }
        }
    }
    let mut descriptors = BTreeMap::<String, Option<ValueRef>>::new();
    /// 递归收集事件载荷里出现的正文引用。
    ///
    /// 用户 JSON 里可能长得像描述符的对象也走这条路，因此收集阶段
    /// 不做拒绝——真正的引用一致性由 reducer 判定（见下面 remember）。
    fn refs(value: &Value, found: &mut BTreeMap<String, Option<ValueRef>>) {
        if value.get("journal_id").is_some()
            && value.get("output_id").is_some()
            && value.get("digest").is_some()
        {
            if let Ok(reference) = serde_json::from_value::<ValueRef>(value.clone()) {
                found.insert(reference.output_id.clone(), Some(reference));
                return;
            }
        }
        if value.get("type").and_then(Value::as_str) == Some("ref") {
            if let Ok(StoredValue::Ref(r)) = serde_json::from_value(value.clone()) {
                found.insert(r.output_id.clone(), Some(r));
                return;
            }
        }
        match value {
            Value::Object(m) => {
                for v in m.values() {
                    refs(v, found);
                }
            }
            Value::Array(a) => {
                for v in a {
                    refs(v, found);
                }
            }
            _ => {}
        }
    }
    let fresh: BTreeSet<_> = tx
        .events
        .iter()
        .filter(|e| e.kind == EventKind::DispatchStarted)
        .filter_map(|e| e.dispatch_id.as_deref())
        .collect();
    for event in &tx.events {
        if let (Some(run), Some(node), Some(dispatch)) = (
            event.run_id.as_deref(),
            event
                .node_id
                .as_deref()
                .or_else(|| event.payload["node_id"].as_str()),
            event.dispatch_id.as_deref(),
        ) {
            // 旧 attempt 账本可能已被裁剪出内存；不补回来就无法判定
            // 「这个 dispatch 是不是已经有结果」。
            let missing = backend
                .inspect(|state| {
                    state
                        .runs
                        .get(run)
                        .and_then(|r| r.nodes.get(node))
                        .is_some_and(|n| {
                            !fresh.contains(dispatch) && !n.attempts.contains_key(dispatch)
                        })
                })
                .await;
            if missing {
                let key = serde_json::to_string(&(run, node, dispatch))?;
                if let Some(value) = index
                    .current_at("run_attempt", &key, upper)
                    .await
                    .map_err(db)?
                {
                    let attempt = serde_json::from_value(value)?;
                    backend
                        .update_state(|state| {
                            if let Some(node) =
                                state.runs.get_mut(run).and_then(|r| r.nodes.get_mut(node))
                            {
                                node.attempts.insert(dispatch.into(), attempt);
                            }
                        })
                        .await;
                }
            }
        }
        refs(&event.payload, &mut descriptors);
    }
    for (id, expected) in descriptors {
        if let Some(expected) = &expected {
            if backend
                .inspect(|s| s.values.check(&StoredValue::Ref(expected.clone())).is_ok())
                .await
            {
                // 内存里已经认识这个引用，无需回库。
                continue;
            }
        }
        if let Some(record) = index.get("published", &id).await.map_err(db)?.1 {
            let lsn = record["lsn"]
                .as_str()
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| JournalError::Storage("invalid publication prefix".into()))?;
            if lsn <= upper {
                let descriptor: ValueRef = serde_json::from_value(record["value"].clone())?;
                // A mismatched reference remains a reducer error; user JSON can also contain
                // objects that happen to look like descriptors, so do not reject it here.
                if expected.as_ref().is_none_or(|r| r == &descriptor) {
                    backend
                        .update_state(|s| s.values.remember(descriptor))
                        .await?;
                }
            }
        }
    }
    Ok(())
}

/// 计算一次事务要写入 `current_entities` 的行变更。
///
/// 行的种类与内容必须与 reducer 看到的事实严格对应：投影检查会用同一
/// 份逻辑重放事实并比对当前行，两边不一致就说明投影有 bug。
pub(crate) fn index_rows(
    state: &State,
    tx: &Transaction,
) -> flow_journal::Result<Vec<EntityChange>> {
    let mut rows = audit_rows(tx)?;
    rows.extend(project_rows(state, tx)?);
    for event in &tx.events {
        if event.kind == EventKind::Command {
            // 永久命令回执：写进 current_entities 后即可在重启后直接查回，
            // 不必回放全部历史事实。
            let mut record: flow_engine::journal_state::CommandRecord =
                serde_json::from_value(event.payload.clone())?;
            record.lsn = tx.lsn;
            if let Some(id) = &record.request_id {
                rows.push(EntityChange {
                    kind: flow_journal::sqlite::EntityKind::Command,
                    key: flow_engine::journal_state::command_key(&record.scope, id),
                    value: Some(serde_json::to_value(record)?),
                });
            }
        }
        if event.kind == EventKind::MasterEpochStarted {
            rows.push(EntityChange {
                kind: flow_journal::sqlite::EntityKind::Engine,
                key: "epoch".into(),
                value: Some(json!({"epoch":state.master_epoch.to_string()})),
            });
        }
        if event.kind == EventKind::ScheduleChanged {
            if let Some(id) = event.payload["id"].as_str() {
                rows.push(EntityChange {
                    kind: flow_journal::sqlite::EntityKind::ScheduleFire,
                    key: id.into(),
                    value: state
                        .schedule_fires
                        .get(id)
                        .map(serde_json::to_value)
                        .transpose()?,
                });
            }
        }
        if event.kind == EventKind::Command {
            // 定时触发产生的 run.start 命令会同时推进「该时隙已烧掉」的
            // 记录，防止同一时隙被重复触发。
            if let Some(key) = event.payload["scope"]
                .as_str()
                .and_then(|s| s.strip_prefix("run.start:schedule:"))
            {
                if let Some(fire) = state.schedule_fires.get(key) {
                    rows.push(EntityChange {
                        kind: flow_journal::sqlite::EntityKind::ScheduleFire,
                        key: key.into(),
                        value: Some(serde_json::to_value(fire)?),
                    });
                }
            }
        }
        if event.kind == EventKind::RunStarted {
            if let Some(workflow) = event.payload["workflow_id"].as_str() {
                rows.push(EntityChange {
                    kind: flow_journal::sqlite::EntityKind::RunWorkflow,
                    key: workflow.into(),
                    value: Some(json!({"workflow_id":workflow})),
                });
            }
            if let (Some(parent), Some(node), Some(child)) = (
                event.payload["parent"]["run_id"].as_str(),
                event.payload["parent"]["node_id"].as_str(),
                event.run_id.as_deref(),
            ) {
                // 父子关系写成行，父 run 的时间线才能在重启后仍还原出子 run。
                rows.push(EntityChange {
                    kind: flow_journal::sqlite::EntityKind::Child,
                    key: serde_json::to_string(&(parent, node, child))?,
                    value: Some(json!({"lsn":format!("{:020}",tx.lsn)})),
                });
            }
        }
        if event.kind == EventKind::DispatchStarted {
            // dispatch 归属索引：迟到结果靠它找回所属 run/node/attempt。
            if let (Some(dispatch), Some(run), Some(node)) =
                (&event.dispatch_id, &event.run_id, &event.node_id)
            {
                rows.push(EntityChange {
                    kind: flow_journal::sqlite::EntityKind::Dispatch,
                    key: dispatch.clone(),
                    value: Some(
                        json!({"run_id":run,"node_id":node,"attempt":event.payload["attempt"]}),
                    ),
                });
            }
        }
    }
    Ok(rows)
}

/// 裁剪协调器的热状态：只保留仍在用的东西。
///
/// 裁剪只影响内存占用，不影响正确性——被裁掉的东西都能从 SQLite 的
/// 当前行按需读回，所以这里可以放心地按「最后两个版本 + 在飞 run」收口。
pub(crate) fn trim(state: &mut State) {
    for workflow in state.workflows.values_mut() {
        let latest = workflow.versions.last_key_value().map(|(v, _)| *v);
        let published = workflow
            .versions
            .iter()
            .rev()
            .find(|(_, v)| v.published)
            .map(|(v, _)| *v);
        // 只保留最新版本与已发布版本：前者是编辑目标，后者是执行依赖。
        workflow
            .versions
            .retain(|version, _| Some(*version) == latest || Some(*version) == published);
    }
    // 终态 run 不再需要调度，直接丢弃出内存。
    state.runs.retain(|_, run| !run.terminal());
    for run in state.runs.values_mut() {
        for node in run.nodes.values_mut() {
            // 只保留当前 attempt 的账本；旧 attempt 是历史，不参与判定。
            node.attempts.retain(|id, _| *id == node.dispatch_id);
        }
    }
    state.values.trim_cache();
}

/// Definition history stays in SQLite. A cold version is loaded only while the
/// coordinator owns the decision lock, and trimmed after that decision commits.
///
/// 版本索引键：工作流 ID + 补零到 20 位的版本号——补零让字典序等于数值序，
/// 索引的顺序分页才有确定意义。
pub(crate) fn version_key(workflow: &str, version: u64) -> Result<String> {
    Ok(serde_json::to_string(&(
        workflow,
        format!("{version:020}"),
    ))?)
}

/// 把一个冷版本读进内存（若尚未装载）。
pub(crate) async fn load_version(
    backend: &JournalBackend,
    workflow: &str,
    version: u64,
) -> Result<()> {
    let needed = backend
        .inspect(|state| {
            state
                .workflows
                .get(workflow)
                .is_some_and(|w| !w.versions.contains_key(&version))
        })
        .await;
    if !needed {
        return Ok(());
    }
    let upper = backend.inspect(|s| s.applied_lsn).await;
    if let Some(row) = backend
        .store
        .current_at("workflow_version", &version_key(workflow, version)?, upper)
        .await
        .map_err(db)?
    {
        let version_value = serde_json::from_value(row)?;
        backend
            .update_state(|state| {
                let workflow = state.workflows.get_mut(workflow).ok_or_else(|| {
                    JournalError::Storage("workflow missing while loading version".into())
                })?;
                workflow.versions.insert(version, version_value);
                Ok::<_, JournalError>(())
            })
            .await?;
    }
    Ok(())
}

/// 命令提交前把命令涉及的冷对象装进内存。
///
/// 命令的裁决闭包可能引用尚未装载的定义版本（workflow.update、webhook
/// patch 等），不预装就会在 reducer 里看到「版本不存在」而拒绝合法迁移。
pub(crate) async fn prepare_command(backend: &JournalBackend, request: &Value) -> Result<()> {
    if let (Some(workflow), Some(version)) =
        (request["workflow_id"].as_str(), request["version"].as_u64())
    {
        load_version(backend, workflow, version).await?;
    }
    if let Some(patch) = request.get("patch") {
        // patch 语义是「合并」：未给出的字段沿用旧值，因此解析版本与
        // 工作流时要回落到旧配置里已固定的值。
        let previous = backend
            .inspect(|state| {
                request["key"]
                    .as_str()
                    .and_then(|key| state.webhooks.get(key))
                    .cloned()
            })
            .await;
        let workflow = patch["workflow_id"]
            .as_str()
            .or_else(|| previous.as_ref().and_then(|v| v["workflow_id"].as_str()));
        let version = patch["invoke_version"]
            .as_u64()
            .or_else(|| previous.as_ref().and_then(|v| v["invoke_version"].as_u64()));
        if let (Some(workflow), Some(version)) = (workflow, version) {
            let workflow = workflow.to_owned();
            load_version(backend, &workflow, version).await?;
        }
    }
    Ok(())
}
