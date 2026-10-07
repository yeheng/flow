//! V2 coordinator. Persistent waits occupy no active execution task.
//! IPC mode (phase 2) replaces the in-process node executor with a managed
//! subprocess pool; journal semantics are byte-identical between modes.
use crate::execution::remote::{RemoteDispatcher, RemoteOptions};
use crate::execution::{ExecutionMode, IpcDispatcher};
use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::{id, node_event, now};
use crate::journal_execution::Attempt;
use flow_engine::journal_state::{Parent, Run, Wait};
use flow_engine::{Definition, NodeLogger, NodeType};
use flow_journal::{Event, EventKind, StoredValue};
use futures::FutureExt;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

/// 节点执行端口：一期进程内 / 二期本地 IPC / 三期远程 agent。
#[derive(Clone)]
pub(crate) enum ExecutionPort {
    InProcess,
    #[cfg(test)]
    PanicOnScript,
    Ipc(Arc<IpcDispatcher>),
    Remote(Arc<RemoteDispatcher>),
}

type Result<T> = std::result::Result<T, JournalError>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

impl JournalBackend {
    pub async fn start_execution(self: &Arc<Self>) -> Result<()> {
        self.start_execution_with_port(16, ExecutionPort::InProcess)
            .await
    }
    pub async fn start_execution_with_limit(self: &Arc<Self>, limit: usize) -> Result<()> {
        if !(1..=16).contains(&limit) {
            return Err(invalid("A_max must be within 1..=16").into());
        }
        self.start_execution_with_port(limit, ExecutionPort::InProcess)
            .await
    }
    /// 二期 IPC 模式：先持久提交新 master_epoch，再创建进程池与派发器
    /// （取得独占写权后持久提交新任期，再派发任务；二期 §3.2）。
    pub async fn start_execution_ipc(self: &Arc<Self>, mode: ExecutionMode) -> Result<()> {
        let crate::execution::ExecutionMode::Ipc(options) = mode else {
            return self.start_execution().await;
        };
        // I09：启用 IPC 时二进制缺失直接失败，不静默回退进程内执行。
        if !options.executor.program.is_file() {
            return Err(invalid(format!(
                "executor binary missing: {}",
                options.executor.program.display()
            ))
            .into());
        }
        let epoch = self.bump_master_epoch().await?;
        let pool = crate::execution::ExecutorPool::new(options, self.journal.id().into(), epoch);
        let dispatcher = Arc::new(IpcDispatcher::new(pool));
        self.start_execution_with_port(16, ExecutionPort::Ipc(dispatcher))
            .await
    }
    /// 持久提交新任期并返回新 epoch。
    pub(crate) async fn bump_master_epoch(self: &Arc<Self>) -> Result<u64> {
        self.internal(|state| {
            let epoch = state.master_epoch + 1;
            Ok((
                vec![Event::new(
                    EventKind::MasterEpochStarted,
                    json!({"epoch": epoch.to_string()}),
                )],
                epoch,
            ))
        })
        .await
    }
    /// 三期远程模式：epoch + TLS 监听 + 远程派发器（R1）。
    pub async fn start_execution_remote(self: &Arc<Self>, options: RemoteOptions) -> Result<()> {
        let epoch = self.bump_master_epoch().await?;
        let manager = crate::execution::remote::AgentManager::new(
            self.clone(),
            self.journal.id().into(),
            epoch,
        );
        manager.listen_tls(options.clone()).await?;
        let dispatcher = Arc::new(RemoteDispatcher::new(manager, options));
        self.start_execution_with_port(16, ExecutionPort::Remote(dispatcher))
            .await
    }

    /// 三期远程（测试/in-process 入口）：用既有 manager 启动远程端口。
    pub async fn start_execution_remote_manager(
        self: &Arc<Self>,
        manager: Arc<crate::execution::remote::AgentManager>,
        options: RemoteOptions,
    ) -> Result<()> {
        let dispatcher = Arc::new(RemoteDispatcher::new(manager, options));
        self.start_execution_with_port(16, ExecutionPort::Remote(dispatcher))
            .await
    }

    async fn start_execution_with_port(
        self: &Arc<Self>,
        limit: usize,
        port: ExecutionPort,
    ) -> Result<()> {
        let mut driver = self.execution.lock().await;
        if driver.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return Ok(());
        }
        let observation = self.observations.clone();
        let weak = Arc::downgrade(self);
        let cancel = self.cancellation();
        let notify = self.execution_notification();
        notify.notify_one();
        *driver = Some(tokio::spawn(async move {
            loop {
                let restarted = AssertUnwindSafe(async {
            let mut tasks: tokio::task::JoinSet<((String, String), Result<()>)> =
                tokio::task::JoinSet::new();
            let mut active = HashSet::new();
            let mut wakes: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
            loop {
                let delay = wakes
                    .first_key_value()
                    .map_or(Duration::from_secs(60), |(at, _)| {
                        Duration::from_millis(
                            at.saturating_sub(chrono::Utc::now().timestamp_millis())
                                .max(0) as u64,
                        )
                    });
                let mut finished = None;
                tokio::select! {
                    _=cancel.cancelled()=>{tasks.abort_all();while tasks.join_next().await.is_some(){}return},
                    completed=tasks.join_next(),if !tasks.is_empty()=>{
                        match completed {
                            Some(Ok((key,result)))=>{active.remove(&key);finished=Some(key.0);if let Err(error)=result {tracing::error!(%error,"v2 node task stopped; committed evidence retained");}},
                            Some(Err(error))=>{tracing::error!(%error,"v2 task exited unexpectedly; rebuilding coordinator");tasks.abort_all();while tasks.join_next().await.is_some(){}return},
                            None=>{}
                        }
                    },
                    _=notify.notified()=>{},
                    _=tokio::time::sleep(delay)=>{}
                }
                let Some(backend) = weak.upgrade() else {
                    return;
                };
                if backend.journal.stats().failure.is_some() {
                    continue;
                }
                if let Some(run) = finished {
                    backend.defer_run(run).await;
                }
                let now = chrono::Utc::now().timestamp_millis();
                while wakes.first_key_value().is_some_and(|(at, _)| *at <= now) {
                    let (_, runs) = wakes.pop_first().unwrap();
                    for run in runs {
                        backend.defer_run(run).await;
                    }
                }
                let mut runs = backend.take_dirty_runs().await.into_iter();
                while let Some(run_id) = runs.next() {
                    if tasks.len() >= limit {
                        backend
                            .defer_runs(std::iter::once(run_id).chain(runs).collect())
                            .await;
                        break;
                    }
                    let result:Result<()>=AssertUnwindSafe(async {
                        let run=backend.inspect(|s|s.runs.get(&run_id).cloned()).await.ok_or_else(||invalid("run missing"))?;
                        if run.terminal(){return Ok(())}
                        // A cancelled parent durably drives bounded one-child cancellation,
                        // so restart resumes propagation without a giant transaction.
                        if let Some(parent)=&run.parent {
                            let cancelled=backend.inspect(|s|s.runs.get(&parent.run_id).is_some_and(|r|r.status=="cancelled")).await;
                            if cancelled {backend.run_cancel(&run_id,Some("parent-cancel")).await?;return Ok(())}
                        }
                        let definition=load_definition(&backend,&run).await?;
                        for node in &definition.nodes {
                            if active.contains(&(run_id.clone(),node.id.clone())){continue}
                            let snapshot=backend.inspect(|s|s.runs.get(&run_id).cloned()).await.ok_or_else(||invalid("run missing"))?;
                            if snapshot.terminal(){break}
                            if let Some(record)=snapshot.nodes.get(&node.id) {
                                if record.status=="waiting" {
                                    let wait=record.wait.as_ref().ok_or_else(||invalid("waiting node missing wait"))?;
                                    if wait.kind=="delay" && wait.wake_at.is_some_and(|at|at<=chrono::Utc::now().timestamp_millis()) {
                                        backend.internal(|s|{let r=&s.runs[&run_id];Ok((vec![node_event(r,&node.id,EventKind::WaitResolved,json!({"output":wait.output.clone().unwrap_or(StoredValue::Inline(Value::Null))}),true)],()))}).await?;
                                    }
                                    if matches!(wait.kind.as_str(),"delay"|"retry") {if let Some(at)=wait.wake_at {if at>chrono::Utc::now().timestamp_millis(){wakes.entry(at).or_default().insert(run_id.clone());}}}
                                    if wait.kind != "retry" || wait.wake_at.is_none_or(|at|at>chrono::Utc::now().timestamp_millis()) {continue;}
                                }
                                if record.status=="succeeded"||record.status=="skipped"{continue}
                                // v1 语义（DESIGN §6.6，engine_recovery 钉死）：
                                // 节点失败不立即终结 run——独立分支继续跑完，失败
                                // 分支的下游被跳过（reason=upstream_failed），全部
                                // 节点终态后由收尾判定写 RunFailed。
                                if record.status=="failed" {continue}
                                if record.operation.as_ref().is_some_and(|op|op.outcome.is_none()) {
                                    let attempt=Attempt{backend:backend.clone(),run_id:run_id.clone(),node_id:node.id.clone(),dispatch_id:record.dispatch_id.clone()};
                                    attempt.finish(EventKind::WaitRegistered,json!({"wait":Wait{kind:"uncertain".into(),wake_at:None,child_run_id:None,output:None},"integrity":"unknown"})).await?;continue;
                                }
                            }
                            let incoming=definition.incoming(&node.id);
                            let mut ready=true;let mut skip=false;let mut predecessors=BTreeMap::new();
                            let mut skip_reason="";
                            for edge in incoming {
                                match snapshot.nodes.get(&edge.from) {
                                    Some(n) if n.status=="skipped"=>{skip=true;ready=false;if skip_reason.is_empty(){skip_reason="upstream_skipped"}}
                                    Some(n) if n.status=="failed"=>{skip=true;ready=false;if skip_reason.is_empty(){skip_reason="upstream_failed"}}
                                    Some(n) if n.status=="succeeded"=> {
                                        let accepted=edge.port.as_deref().is_none_or(|port|n.branch==Some(port=="true"));
                                        if !accepted{skip=true;ready=false;if skip_reason.is_empty(){skip_reason="branch_not_taken"}}else if let Some(output)=&n.output{predecessors.insert(edge.from.clone(),output.clone());}
                                    }
                                    _=>ready=false,
                                }
                            }
                            if skip {backend.internal(|s|Ok((vec![node_event(&s.runs[&run_id],&node.id,EventKind::NodeSkipped,json!({"reason":skip_reason}),true)],()))).await?;continue}
                            if !ready{continue}
                            if tasks.len()>=limit{backend.defer_run(run_id.clone()).await;continue}
                            let attempt=Attempt::begin(backend.clone(),run_id.clone(),node.id.clone()).await?;
                            let key=(run_id.clone(),node.id.clone());active.insert(key.clone());
                            backend.execution_peak.fetch_max(tasks.len()+1,std::sync::atomic::Ordering::Relaxed);
                            let node=node.clone();let observation=observation.clone();let port=port.clone();
                            tasks.spawn(async move {
                                let result=AssertUnwindSafe(async { match &port{
                                    #[cfg(test)]
                                    ExecutionPort::PanicOnScript if node.kind() == Some(NodeType::Script) => panic!("injected execution panic"),
                                    #[cfg(test)]
                                    ExecutionPort::PanicOnScript => execute(&attempt, &node, predecessors.clone(), NodeLogger::disabled()).await,
                                    ExecutionPort::InProcess=>{
                                        let logger=observation.map(|store|NodeLogger::observation(store.logger(attempt.run_id.clone(),attempt.dispatch_id.clone()),attempt.node_id.clone(),1)).unwrap_or_else(NodeLogger::disabled);
                                        execute(&attempt,&node,predecessors.clone(),logger).await
                                    }
                                    ExecutionPort::Ipc(dispatcher)=>{
                                        dispatcher.execute(&attempt,&node,predecessors.clone(),observation).await
                                    }
                                    ExecutionPort::Remote(dispatcher)=>{
                                        dispatcher.execute(&attempt,&node,predecessors.clone(),observation).await
                                    }
                                }}).catch_unwind().await.unwrap_or_else(|_| Err(invalid("node execution panicked; committed operation evidence retained").into()));
                                if let Err(error)=&result {
                                    let snapshot=attempt.snapshot().await;
                                    if let Ok(snapshot)=snapshot {
                                        let Some(n)=snapshot.nodes.get(&attempt.node_id) else {return (key,Err(invalid("dispatch node missing").into()))};
                                        let uncertain=n.operation.as_ref().is_some_and(|o|o.outcome.is_none());
                                        let mut payload=if uncertain {json!({"wait":Wait{kind:"uncertain".into(),wake_at:None,child_run_id:None,output:None},"integrity":"unknown"})}
                                            else {json!({"error":error.to_string(),"preparation_failed":n.prepared.is_none(),"original_input":snapshot.input,"predecessors":predecessors,"integrity":"incomplete"})};
                                        let policy=node.retry();
                                        if !uncertain && n.operation.is_none() && n.prepared.is_some()
                                            && matches!(node.kind(),Some(NodeType::Script|NodeType::Condition))
                                            && n.attempt<policy.max_attempts.min(100) {
                                            let delay=policy.backoff_ms.min(i64::MAX as u64) as i64;
                                            payload["retry_wake_at"]=json!(chrono::Utc::now().timestamp_millis().saturating_add(delay));
                                        }
                                        let _=attempt.finish(if uncertain{EventKind::WaitRegistered}else{EventKind::NodeFailed},payload).await;
                                    }
                                }
                                (key,result)
                            });
                        }
                        let snapshot=backend.inspect(|s|s.runs.get(&run_id).cloned()).await.ok_or_else(||invalid("run missing"))?;
                        if !snapshot.terminal() && definition.nodes.iter().all(|n|snapshot.nodes.get(&n.id).is_some_and(|n|matches!(n.status.as_str(),"succeeded"|"skipped"|"failed"))) {
                            // 失败收口（v1 §6.6）：任一节点终态 failed → RunFailed，
                            // 错误取定义序第一个 failed 节点；否则全部 end 计入
                            // 输出（无输出/skipped 的 end 记 null，不再丢弃——
                            // 多 end 时形状恒为 map，与 v1 singular_or_map 一致）。
                            let failed=definition.nodes.iter().find(|n|snapshot.nodes.get(&n.id).is_some_and(|r|r.status=="failed"));
                            if let Some(failed_node)=failed {
                                let error=backend.inspect(|s|s.runs[&run_id].nodes[&failed_node.id].error.clone()).await;
                                finish_run(&backend,&run_id,None,error).await?;
                            } else {
                                let ends=definition.nodes.iter().filter(|n|n.kind()==Some(NodeType::End)).map(|n|(n.id.clone(),snapshot.nodes.get(&n.id).and_then(|r|r.output.clone()).unwrap_or(StoredValue::Inline(Value::Null)))).collect();
                                let output=join_values(&backend,ends).await?;finish_run(&backend,&run_id,Some(output),None).await?;
                            }
                        }
                        Ok(())
                    }).catch_unwind().await.unwrap_or_else(|_| Err(invalid("coordinator panicked while advancing run").into()));
                    if let Err(error) = result {
                        tracing::error!(%run_id,%error,"v2 coordinator will retry from committed evidence");
                        // Retry reads/coordination with a delay; new events may also wake the run.
                        wakes.entry(chrono::Utc::now().timestamp_millis().saturating_add(1000))
                            .or_default().insert(run_id);
                    }
                }
            }
            }).catch_unwind().await;
                if cancel.is_cancelled() {
                    return;
                }
                let Some(backend) = weak.upgrade() else {
                    return;
                };
                tracing::error!(
                    panicked = restarted.is_err(),
                    "restarting v2 coordinator from committed state"
                );
                let pending = backend
                    .inspect(|state| {
                        state
                            .runs
                            .values()
                            .filter(|run| !run.terminal())
                            .map(|run| run.run_id.clone())
                            .collect()
                    })
                    .await;
                backend.defer_runs(pending).await;
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                notify.notify_one();
            }
        }));
        Ok(())
    }
}

async fn load_definition(backend: &JournalBackend, run: &Run) -> Result<Definition> {
    let root = backend.journal.root().to_path_buf();
    let value = run.definition.clone();
    let upper = backend.journal.durable_lsn();
    let value = tokio::task::spawn_blocking(move || {
        flow_journal::value::materialize(&root, upper, &value, flow_journal::MAX_LINE_BYTES)
    })
    .await
    .map_err(|e| invalid(e.to_string()))??;
    Ok(flow_engine::journal_state::validate_definition(&value)?)
}
async fn join_values(
    backend: &JournalBackend,
    mut values: BTreeMap<String, StoredValue>,
) -> Result<StoredValue> {
    if values.len() == 1 {
        return Ok(values.pop_first().unwrap().1);
    }
    if values.is_empty() {
        return Ok(StoredValue::Inline(Value::Null));
    }
    use flow_journal::value::JsonPart;
    let mut parts = vec![JsonPart::Literal(b"{".to_vec())];
    for (i, (key, value)) in values.into_iter().enumerate() {
        let mut prefix = if i > 0 { b",".to_vec() } else { vec![] };
        prefix.extend(serde_json::to_vec(&key)?);
        prefix.push(b':');
        parts.push(JsonPart::Literal(prefix));
        parts.push(JsonPart::Value(value));
    }
    parts.push(JsonPart::Literal(b"}".to_vec()));
    let (mut reader, producer) = flow_journal::value::compose(
        backend.journal.root().into(),
        backend.journal.durable_lsn(),
        parts,
    );
    let result = flow_journal::value::store_stream(
        &backend.journal,
        &mut reader,
        flow_journal::value::ValueCodec::Json,
    )
    .await;
    drop(reader);
    let production = producer.await.map_err(|e| invalid(e.to_string()))?;
    let value = result?;
    production?;
    Ok(StoredValue::Ref(value))
}

async fn execute(
    attempt: &Attempt,
    node: &flow_engine::Node,
    predecessors: BTreeMap<String, StoredValue>,
    logger: NodeLogger,
) -> Result<()> {
    let prepared = attempt.prepare(node, predecessors).await?;
    let output = match node.kind().ok_or_else(|| invalid("unknown node type"))? {
        NodeType::Start => (prepared.input.clone(), None),
        NodeType::End => (
            join_values(&attempt.backend, prepared.predecessors.clone()).await?,
            None,
        ),
        NodeType::Script | NodeType::Condition => attempt.compute(node, &prepared, logger).await?,
        kind @ (NodeType::HttpCall | NodeType::Llm | NodeType::Email) => {
            let run = attempt.snapshot().await?;
            let previous = run.nodes[&attempt.node_id]
                .operation
                .as_ref()
                .and_then(|o| o.outcome.clone());
            (
                if let Some(previous) = previous {
                    attempt.http_output(&previous, kind).await?
                } else {
                    attempt.http(&prepared, kind).await?
                },
                None,
            )
        }
        NodeType::Delay => {
            let params = flow_journal::value::materialize(
                attempt.backend.journal.root(),
                attempt.backend.journal.durable_lsn(),
                &prepared.params,
                8 * 1024 * 1024,
            )?;
            let ms = params["ms"]
                .as_u64()
                .or_else(|| params["ms"].as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| invalid("delay ms required"))?;
            let wake = chrono::Utc::now()
                .timestamp_millis()
                .checked_add(i64::try_from(ms).map_err(|_| invalid("delay overflow"))?)
                .ok_or_else(|| invalid("delay overflow"))?;
            return attempt
                .wait(Wait {
                    kind: "delay".into(),
                    wake_at: Some(wake),
                    child_run_id: None,
                    output: Some(StoredValue::Inline(json!({"slept_ms":ms}))),
                })
                .await;
        }
        NodeType::HumanTask => {
            return attempt
                .wait(Wait {
                    kind: "signal".into(),
                    wake_at: None,
                    child_run_id: None,
                    output: None,
                })
                .await
        }
        NodeType::SubWorkflow => return start_child(attempt, &prepared).await,
    };
    attempt
        .finish(
            EventKind::NodeCompleted,
            json!({"output":output.0,"branch":output.1}),
        )
        .await
}

async fn start_child(
    attempt: &Attempt,
    prepared: &flow_engine::journal_state::Prepared,
) -> Result<()> {
    let params = flow_journal::value::materialize(
        attempt.backend.journal.root(),
        attempt.backend.journal.durable_lsn(),
        &prepared.params,
        8 * 1024 * 1024,
    )?;
    let workflow_id = params["workflow_id"]
        .as_str()
        .ok_or_else(|| invalid("child workflow required"))?;
    let input = if let Some(mapping) = params.get("input_mapping") {
        flow_journal::value::store_json(&attempt.backend.journal, mapping.clone(), 8 * 1024 * 1024)
            .await?
    } else {
        prepared.input.clone()
    };
    attempt.backend.internal(|s|{
        let parent=&s.runs[&attempt.run_id];if parent.terminal(){return Err(invalid("parent terminal"))}
        if parent.depth>=flow_engine::MAX_SUB_WORKFLOW_DEPTH{return Err(invalid("child depth exceeded"))}
        if s.runs.values().filter(|r|!r.terminal()).count()>=1000{return Err(flow_journal::Error::Limit("R_max=1000".into()))}
        let definition=s.workflows.get(workflow_id).and_then(|w|w.versions.values().rev().find(|v|v.published)).ok_or_else(||invalid("child published definition missing"))?;
        let child_id=id();
        let child=Run{run_id:child_id.clone(),workflow_id:workflow_id.into(),workflow_version:definition.version,definition:definition.definition.clone(),input,
            source:"sub_workflow".into(),source_detail:None,parent:Some(Parent{run_id:attempt.run_id.clone(),node_id:attempt.node_id.clone()}),depth:parent.depth+1,created_at:now(),status:"running".into(),output:None,error:None,last_run_seq:0,nodes:BTreeMap::new()};
        let mut created=Event::new(EventKind::RunStarted,serde_json::to_value(child)?);created.run_id=Some(child_id.clone());created.run_seq=1;
        let n=&parent.nodes[&attempt.node_id];
        let wait=node_event(parent,&attempt.node_id,EventKind::WaitRegistered,json!({"wait":Wait{kind:"child".into(),wake_at:None,child_run_id:Some(child_id),output:None},"sealed_through":n.attempts[&n.dispatch_id].audit_seq.to_string()}),true);
        Ok((vec![created,wait],()))
    }).await
}

async fn finish_run(
    backend: &JournalBackend,
    run_id: &str,
    output: Option<StoredValue>,
    error: Option<String>,
) -> Result<()> {
    backend.internal(|s|{
        let run=&s.runs[run_id];if run.terminal(){return Ok((vec![],()))}
        let mut event=Event::new(if error.is_some(){EventKind::RunFailed}else{EventKind::RunCompleted},json!({"output":output,"error":error}));event.run_id=Some(run_id.into());event.run_seq=run.last_run_seq+1;
        let mut events=vec![event];
        if let Some(parent)=&run.parent {
            let parent_run=&s.runs[&parent.run_id];
            if !parent_run.terminal(){
                let node=&parent_run.nodes[&parent.node_id];
                let payload=if error.is_some(){json!({"error":error,"sealed_through":node.attempts[&node.dispatch_id].audit_seq.to_string()})}else{json!({"output":output})};
                events.push(node_event(parent_run,&parent.node_id,if error.is_some(){EventKind::NodeFailed}else{EventKind::WaitResolved},payload,true));
            }
        }
        Ok((events,()))
    }).await
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    async fn install(backend: &JournalBackend, code: String) -> String {
        let created = backend
            .workflow_create("driver-regression", None)
            .await
            .unwrap();
        let id = created.result["workflow_id"].as_str().unwrap();
        backend.workflow_update(id, json!({"nodes":[{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":code}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}), None).await.unwrap();
        backend.workflow_publish(id, 1, None).await.unwrap();
        let started = backend
            .run_start(id, None, json!(null), "manual", None, None)
            .await
            .unwrap();
        started.result["run_id"].as_str().unwrap().into()
    }
    async fn terminal(backend: &JournalBackend, id: &str) -> Run {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let run = backend.state().await.runs[id].clone();
                if run.terminal() {
                    return run;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn transient_materialize_error_retries_without_another_command() {
        let root = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(root.path(), Default::default())
            .await
            .unwrap();
        let id = install(&backend, format!("return 1; //{}", "x".repeat(128 * 1024))).await;
        let path = root.path().join("journal");
        let hidden = root.path().join("temporarily-unavailable");
        std::fs::rename(&path, &hidden).unwrap();
        backend.start_execution().await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(backend.state().await.runs[&id].nodes.is_empty());
        std::fs::rename(&hidden, &path).unwrap();
        assert_eq!(terminal(&backend, &id).await.status, "succeeded");
        backend.close().await.unwrap();
    }
    #[tokio::test]
    async fn node_panic_seals_failure_and_driver_keeps_serving() {
        let root = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(root.path(), Default::default())
            .await
            .unwrap();
        backend
            .start_execution_with_port(2, ExecutionPort::PanicOnScript)
            .await
            .unwrap();
        for _ in 0..2 {
            let id = install(&backend, "return 1;".into()).await;
            let run = terminal(&backend, &id).await;
            assert_eq!(run.status, "failed");
            assert!(run.nodes["n"].error.as_ref().unwrap().contains("panicked"));
            assert!(run.nodes["n"]
                .attempts
                .values()
                .all(|attempt| attempt.sealed));
        }
        backend.close().await.unwrap();
    }
}
