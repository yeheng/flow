//! Attempt 簿记：派发开始/快照/结果提交的单行事务原语。
//!
//! 节点执行语义只存在于 flow-executor（进程内路径已全平台删除）；主进程
//! 只经这里落 journal 事实——执行器侧的等价语义经 dispatch 的审计/结果
//! 消息驱动同一组事务。
use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::{id, node_event};
use flow_engine::journal_state::Run;
use flow_journal::EventKind;
use serde_json::{json, Value};
use std::sync::Arc;

type Result<T> = std::result::Result<T, JournalError>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

#[derive(Clone)]
pub(crate) struct Attempt {
    pub backend: Arc<JournalBackend>,
    pub run_id: String,
    pub node_id: String,
    pub dispatch_id: String,
}

impl Attempt {
    pub async fn begin(
        backend: Arc<JournalBackend>,
        run_id: String,
        node_id: String,
    ) -> Result<Self> {
        let dispatch_id = id();
        backend
            .internal_for_run(&run_id, |state| {
                let run = state
                    .runs
                    .get(&run_id)
                    .ok_or_else(|| invalid("run not found"))?;
                if run.terminal() {
                    return Err(invalid("run terminal"));
                }
                let old = run.nodes.get(&node_id);
                let execution = old.map(|n| n.node_execution_id.clone()).unwrap_or_else(id);
                let attempt = old.map_or(1, |n| n.attempt + 1);
                let mut event = node_event(
                    run,
                    &node_id,
                    EventKind::DispatchStarted,
                    json!({"node_execution_id":execution,"attempt":attempt.to_string()}),
                    true,
                );
                event.dispatch_id = Some(dispatch_id.clone());
                Ok((vec![event], ()))
            })
            .await?;
        Ok(Self {
            backend,
            run_id,
            node_id,
            dispatch_id,
        })
    }
    pub async fn snapshot(&self) -> Result<Run> {
        self.backend
            .run_snapshot(&self.run_id)
            .await?
            .ok_or_else(|| invalid("run missing").into())
    }
    pub async fn finish(&self, kind: EventKind, mut payload: Value) -> Result<()> {
        self.backend
            .internal_for_run(&self.run_id, |state| {
                let run = state
                    .runs
                    .get(&self.run_id)
                    .ok_or_else(|| invalid("run missing"))?;
                let node = run
                    .nodes
                    .get(&self.node_id)
                    .ok_or_else(|| invalid("node missing"))?;
                if node.dispatch_id != self.dispatch_id {
                    return Err(invalid("stale result"));
                }
                if let Some(original) = &node.attempts[&self.dispatch_id].result {
                    let mut previous = original.payload.clone();
                    previous.as_object_mut().unwrap().remove("sealed_through");
                    if original.kind == kind && previous == payload {
                        return Ok((vec![], ()));
                    }
                    return Err(flow_journal::Error::Conflict(
                        "different result for sealed dispatch".into(),
                    ));
                }
                let seq = node.attempts[&self.dispatch_id].audit_seq
                    + u64::from(kind == EventKind::NodeCompleted);
                payload["sealed_through"] = json!(seq.to_string());
                let mut event = node_event(run, &self.node_id, kind, payload, true);
                if kind == EventKind::NodeCompleted {
                    event.audit_seq = seq;
                }
                Ok((vec![event], ()))
            })
            .await
    }
}
