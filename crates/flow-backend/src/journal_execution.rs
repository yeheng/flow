//! Attempt 簿记：派发开始/快照/结果提交的单行事务原语。
//!
//! 节点执行语义只存在于 flow-executor（进程内路径已全平台删除）；主进程
//! 只经这里落 journal 事实——执行器侧的等价语义经 dispatch 的审计/结果
//! 消息驱动同一组事务。
use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::{id, node_event};
use flow_engine::journal_state::Run;
use flow_journal::{EventKind, StoredValue};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

type Result<T> = std::result::Result<T, JournalError>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

/// NodeFailed 证据载荷的预算收口（铁律 1：失败证据必须能落到 journal
/// 事件上）。载荷原样内嵌 original_input 与全部前驱输出——宽扇入下两者
/// 相加可超 1 MiB 事务行上限，超限提交被 bounded_json 拒绝、错误又被
/// `let _ =` 吞掉后节点保持 Running，下一轮 advance_run 重派发，形成
/// 确定性活锁（attempt 无限增长、每轮烧一个执行器进程）。这里逐级降级：
/// 超限时先把前驱输出替换为尺寸描述符，仍超限再把 original_input 降级，
/// 保证任何失败都能写成一条事务。
pub(crate) fn failure_evidence(
    error: &str,
    preparation_failed: bool,
    original_input: &StoredValue,
    predecessors: &BTreeMap<String, StoredValue>,
) -> Value {
    /// 事务行上限减封套余量（事件头/run_seq/attempt 账本等）。
    const EVIDENCE_BUDGET: usize = flow_journal::MAX_LINE_BYTES - 4096;
    fn fits(payload: &Value) -> bool {
        flow_journal::codec::bounded_json(payload, EVIDENCE_BUDGET).is_ok()
    }
    /// 内嵌值的降级形态：保留「这里曾有证据」的事实与量级，不保留字节。
    fn omitted(value: &StoredValue) -> Value {
        let bytes = serde_json::to_value(value)
            .ok()
            .and_then(|v| serde_json::to_vec(&v).ok())
            .map_or(0, |b| b.len());
        json!({"evidence_omitted": true, "bytes": bytes})
    }
    let mut payload = json!({
        "error": error,
        "preparation_failed": preparation_failed,
        "original_input": serde_json::to_value(original_input).unwrap_or(Value::Null),
        "predecessors": serde_json::to_value(predecessors).unwrap_or(json!({})),
        "integrity": "incomplete",
    });
    if fits(&payload) {
        return payload;
    }
    payload["predecessors"] = json!({
        "evidence_omitted": true,
        "entries": predecessors
            .iter()
            .map(|(id, value)| (id.clone(), omitted(value)))
            .collect::<serde_json::Map<String, Value>>(),
    });
    if fits(&payload) {
        return payload;
    }
    payload["original_input"] = omitted(original_input);
    payload
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
                    .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
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
                    // I20 幂等比较：剥掉随提交填充的封口字段（sealed_through）
                    // 与随重传重算的 wall-clock 字段（retry_wake_at）——同一
                    // dispatch 的可重试 NodeFailed 重提时后者必然不等，严格整
                    // 载荷比较会把幂等重传误判成 Conflict。载荷非对象（历史
                    // 数据畸形）不参与幂等回退，直接走冲突路径。
                    let mut previous = original.payload.clone();
                    let mut current = payload;
                    for value in [&mut previous, &mut current] {
                        if let Some(object) = value.as_object_mut() {
                            object.remove("sealed_through");
                            object.remove("retry_wake_at");
                        }
                    }
                    if original.kind == kind && previous == current {
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
