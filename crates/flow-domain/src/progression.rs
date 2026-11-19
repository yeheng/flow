//! Deterministic workflow decisions over committed node state.

use std::collections::BTreeMap;

use flow_facts::StoredValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Definition, Node, NodeType};

/// 节点执行状态（`Node.status`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Skipped,
    Failed,
}

impl NodeStatus {
    /// 节点状态的稳定字符串形式（与 `serde` 的 snake_case 重命名一致）。
    ///
    /// 这是对外暴露的稳定字面量，会落进快照、HTTP 响应与事件载荷；
    /// 改字面量等于改协议，必须同步改 `serde` 属性。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

/// A node conclusion shared by the in-memory FSM and the journal reducer.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum NodeOutcome {
    Succeeded {
        output: StoredValue,
        branch: Option<bool>,
    },
    Failed {
        error: String,
    },
    Skipped {
        skip_reason: Option<String>,
    },
}

/// The graph rules borrow only the node fields needed for planning.
pub struct NodeView<'a> {
    pub status: NodeStatus,
    pub output: Option<&'a StoredValue>,
    pub branch: Option<bool>,
    pub error: Option<&'a str>,
    pub skip_reason: Option<&'a str>,
}

pub trait WorkflowNodeState {
    fn view(&self) -> NodeView<'_>;
}

impl WorkflowNodeState for NodeOutcome {
    fn view(&self) -> NodeView<'_> {
        let mut view = NodeView {
            status: NodeStatus::Succeeded,
            output: None,
            branch: None,
            error: None,
            skip_reason: None,
        };
        match self {
            Self::Succeeded { output, branch } => {
                view.output = Some(output);
                view.branch = *branch;
            }
            Self::Failed { error } => {
                view.status = NodeStatus::Failed;
                view.error = Some(error);
            }
            Self::Skipped { skip_reason } => {
                view.status = NodeStatus::Skipped;
                view.skip_reason = skip_reason.as_deref();
            }
        }
        view
    }
}

/// 纯状态推断（reducer）：一个节点在当前快照的入边结论。只读 committed
/// facts，决策与副作用彻底分离——副作用全部落在 journal 事实里。
#[derive(Debug, Clone)]
pub enum NodeGate {
    /// 前驱未全部终态，本轮不动。
    Blocked,
    /// 分支不可达或上游失败/跳过：写 NodeSkipped（携带原因）。
    Skip(&'static str),
    /// 可派发，携带已接受的前驱输出。
    Ready(BTreeMap<String, StoredValue>),
}

/// A settled node has no further work. Otherwise evaluate its incoming edges.
pub fn node_gate(
    definition: &Definition,
    nodes: &BTreeMap<String, impl WorkflowNodeState>,
    node: &Node,
) -> NodeGate {
    if nodes
        .get(&node.id)
        .is_some_and(|record| is_settled(record.view().status))
    {
        return NodeGate::Blocked;
    }
    incoming_gate(definition, nodes, node)
}

pub fn is_settled(status: NodeStatus) -> bool {
    matches!(
        status,
        NodeStatus::Succeeded | NodeStatus::Skipped | NodeStatus::Failed
    )
}

/// Decide whether a node can advance from committed predecessor states.
///
/// v2 defaults to selected joins: wait for every incoming branch to settle,
/// then use only active paths. v1 defaults to requiring every predecessor.
pub fn incoming_gate(
    definition: &Definition,
    nodes: &BTreeMap<String, impl WorkflowNodeState>,
    node: &Node,
) -> NodeGate {
    let selected = node.params["join"]
        .as_str()
        .map_or(definition.semantics >= 2, |j| j == "selected");
    if selected {
        // selected 汇合：等全部入边确定，然后只走被选中的成功路径；
        // 未选分支记 inactive，全部入边 inactive 时传播 inactive。
        let mut pending = false;
        let mut failed = false;
        let mut active = 0;
        let mut predecessors = BTreeMap::new();
        for edge in definition.incoming(&node.id) {
            match nodes.get(&edge.from).map(WorkflowNodeState::view) {
                Some(n)
                    if n.status == NodeStatus::Skipped
                        && matches!(n.skip_reason, Some("branch_not_taken" | "inactive")) => {}
                Some(n) if matches!(n.status, NodeStatus::Failed | NodeStatus::Skipped) => {
                    failed = true
                }
                Some(n) if n.status == NodeStatus::Succeeded => {
                    if edge
                        .port
                        .as_deref()
                        .is_none_or(|p| n.branch == Some(p == "true"))
                    {
                        active += 1;
                        if let Some(output) = n.output {
                            predecessors.insert(edge.from.clone(), output.clone());
                        }
                    }
                }
                _ => pending = true,
            }
        }
        return if pending {
            NodeGate::Blocked
        } else if failed {
            NodeGate::Skip("upstream_failed")
        } else if active == 0 && definition.incoming_count(&node.id) > 0 {
            NodeGate::Skip("inactive")
        } else {
            NodeGate::Ready(predecessors)
        };
    }
    let mut ready = true;
    let mut skip = false;
    let mut predecessors = BTreeMap::new();
    let mut skip_reason = "";
    for edge in definition.incoming(&node.id) {
        match nodes.get(&edge.from).map(WorkflowNodeState::view) {
            Some(n) if n.status == NodeStatus::Skipped => {
                // v1 语义：前驱被跳过则下游也跳过（跳转会向下游传播）。
                skip = true;
                ready = false;
                if skip_reason.is_empty() {
                    skip_reason = "upstream_skipped";
                }
            }
            Some(n) if n.status == NodeStatus::Failed => {
                skip = true;
                ready = false;
                if skip_reason.is_empty() {
                    skip_reason = "upstream_failed";
                }
            }
            Some(n) if n.status == NodeStatus::Succeeded => {
                let accepted = edge
                    .port
                    .as_deref()
                    .is_none_or(|port| n.branch == Some(port == "true"));
                if !accepted {
                    skip = true;
                    ready = false;
                    if skip_reason.is_empty() {
                        skip_reason = "branch_not_taken";
                    }
                } else if let Some(output) = n.output {
                    predecessors.insert(edge.from.clone(), output.clone());
                }
            }
            _ => ready = false,
        }
    }
    if skip {
        NodeGate::Skip(skip_reason)
    } else if !ready {
        NodeGate::Blocked
    } else {
        NodeGate::Ready(predecessors)
    }
}

/// How a settled graph completes before output materialization or commit.
#[derive(Debug, PartialEq)]
pub enum RunSettlement {
    Failed(Option<String>),
    Completed(BTreeMap<String, StoredValue>),
}

/// Determine the outcome of an active run once all its nodes are terminal.
pub fn run_settlement(
    definition: &Definition,
    nodes: &BTreeMap<String, impl WorkflowNodeState>,
) -> Option<RunSettlement> {
    if !definition.nodes.iter().all(|node| {
        nodes
            .get(&node.id)
            .is_some_and(|record| is_settled(record.view().status))
    }) {
        return None;
    }

    if let Some(failed) = definition
        .nodes
        .iter()
        .find(|node| nodes[&node.id].view().status == NodeStatus::Failed)
    {
        return Some(RunSettlement::Failed(
            nodes[&failed.id].view().error.map(str::to_owned),
        ));
    }

    let mut ends = BTreeMap::new();
    for node in &definition.nodes {
        if node.kind() != Some(NodeType::End) {
            continue;
        }
        let record = nodes[&node.id].view();
        // v1 includes skipped ends; v2 includes only ends reached by active paths.
        if definition.semantics == 1 || record.status == NodeStatus::Succeeded {
            ends.insert(
                node.id.clone(),
                record
                    .output
                    .cloned()
                    .unwrap_or(StoredValue::Inline(Value::Null)),
            );
        }
    }
    if definition.semantics >= 2 && ends.is_empty() {
        return Some(RunSettlement::Failed(Some(
            "no active end node completed".into(),
        )));
    }
    Some(RunSettlement::Completed(ends))
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use crate::journal_state::Node as NodeState;
    use serde_json::json;

    /// 构造一个带给定边的定义（闸门单测用）。
    fn definition(semantics: u32, edges: Value) -> Definition {
        serde_json::from_value(json!({
            "semantics": semantics,
            "nodes": [
                {"id":"s","type":"start"},
                {"id":"a","type":"script"},
                {"id":"b","type":"script"},
                {"id":"x","type":"script"},
                {"id":"y","type":"script"},
                {"id":"e","type":"end"}
            ],
            "edges": edges
        }))
        .unwrap()
    }

    /// 按 ID 取定义中的节点。
    fn node<'a>(id: &str, def: &'a Definition) -> &'a Node {
        def.nodes.iter().find(|n| n.id == id).unwrap()
    }

    /// (id, status, branch, output)——output 为业务值，此处包装成 inline StoredValue。
    fn view(nodes: &[(&str, &str, Option<bool>, Option<Value>)]) -> BTreeMap<String, NodeState> {
        nodes
            .iter()
            .map(|(id, status, branch, output)| {
                let output = output
                    .as_ref()
                    .map(|value| StoredValue::inline(value.clone()).unwrap());
                (
                    (*id).to_string(),
                    NodeState {
                        node_execution_id: String::new(),
                        attempt: 1,
                        dispatch_id: String::new(),
                        status: serde_json::from_value::<NodeStatus>(json!(status)).unwrap(),
                        branch: *branch,
                        output,
                        prepared: None,
                        wait: None,
                        operation: None,
                        attempts: BTreeMap::new(),
                        error: None,
                        skip_reason: (*status == "skipped").then(|| "inactive".into()),
                        approval: None,
                        permit: None,
                    },
                )
            })
            .collect()
    }

    /// 闸门：前驱未定则节点保持阻塞。
    #[test]
    fn gate_blocks_until_all_predecessors_settle() {
        let def = definition(
            1,
            json!([
                {"from":"s","to":"a"},{"from":"s","to":"b"},
                {"from":"a","to":"e"},{"from":"b","to":"e"}
            ]),
        );
        let settled = view(&[
            ("s", "succeeded", None, None),
            ("a", "succeeded", None, Some(json!(1))),
        ]);
        assert!(matches!(
            incoming_gate(&def, &settled, node("e", &def)),
            NodeGate::Blocked
        ));
        // 未注册的入边节点（缺前驱记录）同样阻塞。
        let empty = view(&[]);
        assert!(matches!(
            incoming_gate(&def, &empty, node("a", &def)),
            NodeGate::Blocked
        ));
    }

    /// 闸门：只收集被接受的前驱输出。
    #[test]
    fn gate_collects_accepted_predecessor_outputs() {
        let def = definition(
            1,
            json!([
                {"from":"s","to":"a"},{"from":"s","to":"b"},
                {"from":"a","to":"e"},{"from":"b","to":"e"}
            ]),
        );
        let view = view(&[
            ("s", "succeeded", None, None),
            ("a", "succeeded", None, Some(json!({"a":1}))),
            ("b", "succeeded", None, Some(json!(2))),
        ]);
        let NodeGate::Ready(predecessors) = incoming_gate(&def, &view, node("e", &def)) else {
            panic!("expected ready");
        };
        assert_eq!(
            predecessors,
            BTreeMap::from([
                ("a".to_string(), StoredValue::Inline(json!({"a":1}))),
                ("b".to_string(), StoredValue::Inline(json!(2))),
            ])
        );
    }

    /// 闸门：失败或跳过的上游会让下游被跳过。
    #[test]
    fn gate_skips_downstream_of_failed_or_skipped() {
        let def = definition(
            1,
            json!([
                {"from":"s","to":"a"},{"from":"s","to":"b"},
                {"from":"a","to":"e"},{"from":"b","to":"e"}
            ]),
        );
        let failed = view(&[
            ("s", "succeeded", None, None),
            ("a", "failed", None, None),
            ("b", "skipped", None, None),
        ]);
        assert!(matches!(
            incoming_gate(&def, &failed, node("e", &def)),
            NodeGate::Skip("upstream_failed")
        ));
        let view = view(&[
            ("s", "succeeded", None, None),
            ("a", "skipped", None, None),
            ("b", "skipped", None, None),
        ]);
        assert!(matches!(
            incoming_gate(&def, &view, node("e", &def)),
            NodeGate::Skip("upstream_skipped")
        ));
    }

    /// 闸门：条件节点的分支端口决定哪条边被接受。
    #[test]
    fn gate_honors_condition_branch_ports() {
        let def = definition(
            1,
            json!([
                {"from":"a","to":"x","port":"true"},
                {"from":"a","to":"y","port":"false"}
            ]),
        );
        let view = view(&[("a", "succeeded", Some(false), Some(json!(1)))]);
        assert!(matches!(
            incoming_gate(&def, &view, node("x", &def)),
            NodeGate::Skip("branch_not_taken")
        ));
        let NodeGate::Ready(predecessors) = incoming_gate(&def, &view, node("y", &def)) else {
            panic!("expected ready");
        };
        assert_eq!(predecessors["a"], StoredValue::Inline(json!(1)));
        // 无端口的边不受分支影响。
        let plain = definition(1, json!([{"from":"a","to":"x"}]));
        let NodeGate::Ready(_) = incoming_gate(&plain, &view, node("x", &plain)) else {
            panic!("expected ready");
        };
    }

    /// 闸门：没有入边的节点（如 start）直接就绪。
    #[test]
    fn node_without_incoming_edges_is_ready() {
        let def = definition(1, json!([{"from":"s","to":"a"}]));
        let view = view(&[]);
        assert!(matches!(
            incoming_gate(&def, &view, node("s", &def)),
            NodeGate::Ready(_)
        ));
    }

    #[test]
    fn selected_join_and_settlement_use_only_active_paths() {
        let edges = json!([
            {"from":"a","to":"x","port":"true"},
            {"from":"a","to":"y","port":"false"},
            {"from":"x","to":"e"},{"from":"y","to":"e"}
        ]);
        let legacy = definition(1, edges.clone());
        let def = definition(2, edges);
        let mut nodes = view(&[
            ("s", "succeeded", None, None),
            ("a", "succeeded", Some(true), None),
            ("b", "succeeded", None, None),
            ("x", "succeeded", None, Some(json!(42))),
        ]);
        assert!(matches!(
            incoming_gate(&def, &nodes, node("e", &def)),
            NodeGate::Blocked
        ));
        assert!(matches!(
            incoming_gate(&def, &nodes, node("y", &def)),
            NodeGate::Skip("inactive")
        ));
        nodes.extend(view(&[("y", "skipped", None, None)]));
        let NodeGate::Ready(inputs) = incoming_gate(&def, &nodes, node("e", &def)) else {
            panic!("selected join must accept the active branch");
        };
        assert_eq!(
            inputs,
            BTreeMap::from([("x".into(), StoredValue::Inline(json!(42)))])
        );
        assert_eq!(run_settlement(&def, &nodes), None);
        nodes.extend(view(&[("e", "succeeded", None, Some(json!(42)))]));
        assert_eq!(
            run_settlement(&def, &nodes),
            Some(RunSettlement::Completed(BTreeMap::from([(
                "e".into(),
                StoredValue::Inline(json!(42)),
            )])))
        );
        nodes.extend(view(&[("e", "skipped", None, None)]));
        assert_eq!(
            run_settlement(&def, &nodes),
            Some(RunSettlement::Failed(Some(
                "no active end node completed".into()
            )))
        );
        assert_eq!(
            run_settlement(&legacy, &nodes),
            Some(RunSettlement::Completed(BTreeMap::from([(
                "e".into(),
                StoredValue::Inline(Value::Null),
            )])))
        );
    }
}
