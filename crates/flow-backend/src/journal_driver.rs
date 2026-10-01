//! V2 coordinator. Persistent waits occupy no active execution task.
//! IPC mode (phase 2) replaces the in-process node executor with a managed
//! subprocess pool; journal semantics are byte-identical between modes.
use crate::execution::{ExecutionMode, IpcDispatcher};
use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::{id, node_event, now};
use crate::journal_execution::Attempt;
use flow_engine::journal_state::{Parent, Run, Wait};
use flow_engine::{Definition, NodeLogger, NodeType};
use flow_journal::{Event, EventKind, StoredValue};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// 节点执行端口：一期进程内 / 二期 IPC 子进程。
#[derive(Clone)]
pub(crate) enum ExecutionPort {
    InProcess,
    Ipc(Arc<IpcDispatcher>),
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
        if !options.executor_bin.is_file() {
            return Err(invalid(format!(
                "executor binary missing: {}",
                options.executor_bin.display()
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
    async fn start_execution_with_port(
        self: &Arc<Self>,
        limit: usize,
        port: ExecutionPort,
    ) -> Result<()> {
        let mut driver = self.execution.lock().await;
        if driver.is_some() {
            return Ok(());
        }
        let observation = self.observations.clone();
        let weak = Arc::downgrade(self);
        let cancel = self.cancellation();
        let notify = self.execution_notification();
        let port = port;
        notify.notify_one();
        *driver = Some(tokio::spawn(async move {
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
                            Some(Err(error))=>{tracing::error!(%error,"v2 task panic: stopping dispatch, preserving unresolved facts");tasks.abort_all();return},
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
                    let result:Result<()>=async {
                        let run=backend.inspect(|s|s.runs[&run_id].clone()).await;
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
                            let snapshot=backend.inspect(|s|s.runs[&run_id].clone()).await;
                            if snapshot.terminal(){break}
                            if let Some(record)=snapshot.nodes.get(&node.id) {
                                if record.status=="waiting" {
                                    let wait=record.wait.as_ref().unwrap();
                                    if wait.kind=="delay" && wait.wake_at.is_some_and(|at|at<=chrono::Utc::now().timestamp_millis()) {
                                        backend.internal(|s|{let r=&s.runs[&run_id];Ok((vec![node_event(r,&node.id,EventKind::WaitResolved,json!({"output":wait.output.clone().unwrap_or(StoredValue::Inline(Value::Null))}),true)],()))}).await?;
                                    }
                                    if matches!(wait.kind.as_str(),"delay"|"retry") {if let Some(at)=wait.wake_at {if at>chrono::Utc::now().timestamp_millis(){wakes.entry(at).or_default().insert(run_id.clone());}}}
                                    if wait.kind != "retry" || wait.wake_at.is_none_or(|at|at>chrono::Utc::now().timestamp_millis()) {continue;}
                                }
                                if record.status=="succeeded"||record.status=="skipped"{continue}
                                if record.status=="failed" {finish_run(&backend,&run_id,None,record.error.clone()).await?;break}
                                if record.operation.as_ref().is_some_and(|op|op.outcome.is_none()) {
                                    let attempt=Attempt{backend:backend.clone(),run_id:run_id.clone(),node_id:node.id.clone(),dispatch_id:record.dispatch_id.clone()};
                                    attempt.finish(EventKind::WaitRegistered,json!({"wait":Wait{kind:"uncertain".into(),wake_at:None,child_run_id:None,output:None},"integrity":"unknown"})).await?;continue;
                                }
                            }
                            let incoming=definition.incoming(&node.id);
                            let mut ready=true;let mut skip=false;let mut predecessors=BTreeMap::new();
                            for edge in incoming {
                                match snapshot.nodes.get(&edge.from) {
                                    Some(n) if n.status=="skipped"=>{skip=true;ready=false;}
                                    Some(n) if n.status=="succeeded"=>{
                                        let accepted=edge.port.as_deref().is_none_or(|port|n.branch==Some(port=="true"));
                                        if !accepted{skip=true;ready=false;}else if let Some(output)=&n.output{predecessors.insert(edge.from.clone(),output.clone());}
                                    }
                                    _=>ready=false,
                                }
                            }
                            if skip {backend.internal(|s|Ok((vec![node_event(&s.runs[&run_id],&node.id,EventKind::NodeSkipped,json!({}),true)],()))).await?;continue}
                            if !ready{continue}
                            if tasks.len()>=limit{backend.defer_run(run_id.clone()).await;continue}
                            let attempt=Attempt::begin(backend.clone(),run_id.clone(),node.id.clone()).await?;
                            let key=(run_id.clone(),node.id.clone());active.insert(key.clone());
                            backend.execution_peak.fetch_max(tasks.len()+1,std::sync::atomic::Ordering::Relaxed);
                            let node=node.clone();let observation=observation.clone();let port=port.clone();
                            tasks.spawn(async move {
                                let result=match &port{
                                    ExecutionPort::InProcess=>{
                                        let logger=observation.map(|store|NodeLogger::observation(store.logger(attempt.run_id.clone(),attempt.dispatch_id.clone()),attempt.node_id.clone(),1)).unwrap_or_else(NodeLogger::disabled);
                                        execute(&attempt,&node,predecessors.clone(),logger).await
                                    }
                                    ExecutionPort::Ipc(dispatcher)=>{
                                        dispatcher.execute(&attempt,&node,predecessors.clone(),observation).await
                                    }
                                };
                                if let Err(error)=&result {
                                    let snapshot=attempt.snapshot().await;
                                    if let Ok(snapshot)=snapshot {
                                        let n=&snapshot.nodes[&attempt.node_id];
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
                        let snapshot=backend.inspect(|s|s.runs[&run_id].clone()).await;
                        if !snapshot.terminal() && definition.nodes.iter().all(|n|snapshot.nodes.get(&n.id).is_some_and(|n|matches!(n.status.as_str(),"succeeded"|"skipped"))) {
                            let ends=definition.nodes.iter().filter(|n|n.kind()==Some(NodeType::End)).filter_map(|n|snapshot.nodes.get(&n.id).and_then(|r|r.output.clone()).map(|v|(n.id.clone(),v))).collect();
                            let output=join_values(&backend,ends).await?;finish_run(&backend,&run_id,Some(output),None).await?;
                        }
                        Ok(())
                    }.await;
                    if let Err(error) = result {
                        tracing::error!(%run_id,%error,"v2 coordinator stopped this run without replaying external work");
                    }
                }
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
