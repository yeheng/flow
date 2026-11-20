//! Maps committed workflow state to the existing v8 worker task payload.

use std::collections::BTreeMap;

use flow_engine::execution_protocol::{ExecuteTask, TaskValue};
use flow_engine::journal_state::{Node as NodeRecord, Run};
use flow_engine::Node;
use flow_journal::StoredValue;
use serde_json::json;

use crate::journal::JournalError;
use crate::journal_execution::Attempt;

pub(super) fn prepare(
    attempt: &Attempt,
    run: &Run,
    node: &Node,
    record: &NodeRecord,
    predecessors: &BTreeMap<String, StoredValue>,
    observability: bool,
) -> Result<ExecuteTask, JournalError> {
    // 恢复路径携带上一份结果：重挂后的执行器据此识别「这份结果已经
    // 产出过」，从而走幂等提交而不是重新执行。
    let mut previous_outcome = record
        .operation
        .as_ref()
        .and_then(|o| o.outcome.clone())
        .map(|v| serde_json::to_value(&v))
        .transpose()?;
    let mut node_value = serde_json::to_value(node)?;
    if let Some(execution) = &run.execution {
        if execution.runtime != flow_engine::RUNTIME_ID {
            return Err(flow_journal::Error::Invalid(
                "run requires a different executor runtime".into(),
            )
            .into());
        }
        let fixture = execution
            .regression
            .as_ref()
            .and_then(|r| r.fixtures.get(&execution.node_key(&node.id)));
        if let Some(fixture) = fixture {
            previous_outcome = Some(serde_json::to_value(&fixture.outcome)?);
        }
        let mut execution_value = json!({
            "runtime": execution.runtime, "clock_ms":execution.clock_ms,
            "seed": format!("{}:{}", execution.seed, execution.node_key(&node.id)),
            "regression": execution.regression.is_some(),
            "fixture_fingerprint": fixture.map(|f| &f.fingerprint),
            "environment": execution.environment,
        });
        // 冻结的 agent 配置（与 operation 请求里的 agent 是同一份快照的
        // 两次序列化；测试断言两者相等）。旧 journal / 未配置时无此键。
        if let Some(agent) = &execution.agent {
            execution_value["agent"] = serde_json::to_value(agent)?;
        }
        // 09 §8：受管回合权限（TurnPermission）随执行上下文下发——执行器
        // 据此装配 permission/request 反向通道；legacy run 无这些键 =
        // 未受管回合。
        if let Some(policy) = &run.policy {
            execution_value["permissions_managed"] = json!(!policy.legacy);
            execution_value["permissions_mode"] = json!(policy.mode.as_str());
            execution_value["policy_digest"] = json!(policy.digest);
        }
        node_value["_flow_execution"] = execution_value;
    }
    if let Some(binding) = node
        .plugin_type()
        .and_then(|kind| run.plugin_bindings.as_ref()?.get(kind))
    {
        node_value["_flow_plugin"] = json!({
            "id": binding.plugin_id,
            "content_hash": binding.content_hash,
        });
    }
    Ok(ExecuteTask {
        run_id: attempt.run_id.clone(),
        node_id: attempt.node_id.clone(),
        node_execution_id: record.node_execution_id.clone(),
        attempt: record.attempt,
        node: node_value,
        input: task_value(&run.input),
        predecessors: predecessors
            .iter()
            .map(|(id, value)| (id.clone(), task_value(value)))
            .collect(),
        prepared: record.prepared.as_ref().map(|committed| {
            flow_engine::execution_protocol::PreparedMeta {
                engine_build: committed.engine_build.clone(),
                node_semantics: committed.node_semantics,
                params: task_value(&committed.params),
            }
        }),
        previous_outcome,
        observability,
        // v8：存在待审批/待沿用操作身份时随任务下发——执行器重发
        // RequestOperation 时沿用原 operation_id（§6.1），不新建实例。
        pending_operation: record
            .approval
            .as_ref()
            .map(|approval| approval.request.operation_id.clone()),
    })
}

/// StoredValue → 控制帧声明：Ref 保留 journal 身份（输出回引），
/// Inline 只声明存在（字节走 data 通道）。
fn task_value(stored: &StoredValue) -> TaskValue {
    match stored {
        StoredValue::Ref(reference) => TaskValue::Ref(reference.clone()),
        StoredValue::Inline(_) => TaskValue::Local,
    }
}
