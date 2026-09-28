use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 节点类型。前端拖拽面板与引擎共用这一份定义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Start,
    End,
    Script,
    Condition,
    Delay,
    HttpCall,
    HumanTask,
    /// 调用另一个已发布工作流作为子 run，等待其终态并透传输出
    SubWorkflow,
}

impl NodeType {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeType::Start => "start",
            NodeType::End => "end",
            NodeType::Script => "script",
            NodeType::Condition => "condition",
            NodeType::Delay => "delay",
            NodeType::HttpCall => "http_call",
            NodeType::HumanTask => "human_task",
            NodeType::SubWorkflow => "sub_workflow",
        }
    }

    pub fn parse(s: &str) -> Option<NodeType> {
        Some(match s {
            "start" => NodeType::Start,
            "end" => NodeType::End,
            "script" => NodeType::Script,
            "condition" => NodeType::Condition,
            "delay" => NodeType::Delay,
            "http_call" => NodeType::HttpCall,
            "human_task" => NodeType::HumanTask,
            "sub_workflow" => NodeType::SubWorkflow,
            _ => return None,
        })
    }

    /// 崩溃后是否不可安全重放：有外部副作用的节点必须人工裁决。
    pub fn has_side_effect(self) -> bool {
        matches!(self, NodeType::HttpCall)
    }
}

/// http_call 允许的方法。validate 与前端能力清单（nodetypes.list）共用这一份，
/// 校验按大小写不敏感处理（与执行层 to_uppercase 后解析一致）。
pub const HTTP_METHODS: [&str; 5] = ["GET", "POST", "PUT", "PATCH", "DELETE"];

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Position {
    #[serde(default)]
    pub x: f64,
    #[serde(default)]
    pub y: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<Position>,
    #[serde(default)]
    pub params: Value,
}

impl Node {
    pub fn kind(&self) -> Option<NodeType> {
        NodeType::parse(&self.node_type)
    }

    pub fn param_str(&self, key: &str) -> Option<&str> {
        self.params.get(key).and_then(Value::as_str)
    }

    pub fn param_u64(&self, key: &str) -> Option<u64> {
        self.params.get(key).and_then(Value::as_u64)
    }

    pub fn retry(&self) -> RetryPolicy {
        RetryPolicy::from_params(&self.params)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 1,
            backoff_ms: 0,
        }
    }
}

impl RetryPolicy {
    pub fn from_params(params: &Value) -> RetryPolicy {
        let retry = params.get("retry");
        RetryPolicy {
            max_attempts: retry
                .and_then(|r| r.get("max_attempts"))
                .and_then(Value::as_u64)
                .unwrap_or(1)
                .max(1) as u32,
            backoff_ms: retry
                .and_then(|r| r.get("backoff_ms"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// condition 节点的出口端口："true" / "false"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
}

/// 工作流图。前端拖拽的产物，整体作为一个不可变版本存入数据库。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Definition {
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub edges: Vec<Edge>,
}

impl Definition {
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub fn node_type(&self, id: &str) -> Option<NodeType> {
        self.node(id).and_then(Node::kind)
    }

    pub fn incoming(&self, id: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == id).collect()
    }

    pub fn outgoing(&self, id: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == id).collect()
    }

    pub fn start_node(&self) -> Option<&Node> {
        self.nodes
            .iter()
            .find(|n| n.kind() == Some(NodeType::Start))
    }

    /// 建图即校验：拖拽生成的图在发布前必须过这一关。
    pub fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            return Err("工作流没有任何节点".into());
        }

        let mut seen = HashSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty() {
                return Err("存在空的节点 id".into());
            }
            if !seen.insert(node.id.as_str()) {
                return Err(format!("节点 id 重复：{}", node.id));
            }
            let kind = node
                .kind()
                .ok_or_else(|| format!("节点 {} 的类型未知：{}", node.id, node.node_type))?;
            validate_params(node, kind)?;
        }

        let starts = self
            .nodes
            .iter()
            .filter(|n| n.kind() == Some(NodeType::Start))
            .count();
        if starts != 1 {
            return Err(format!("必须且只能有一个 start 节点，当前有 {starts} 个"));
        }
        if !self.nodes.iter().any(|n| n.kind() == Some(NodeType::End)) {
            return Err("至少需要一个 end 节点".into());
        }

        let mut edge_keys = HashSet::new();
        for edge in &self.edges {
            if self.node(&edge.from).is_none() {
                return Err(format!("边的起点不存在：{}", edge.from));
            }
            if self.node(&edge.to).is_none() {
                return Err(format!("边的终点不存在：{}", edge.to));
            }
            if edge.from == edge.to {
                return Err(format!("节点 {} 存在自环", edge.from));
            }
            if !edge_keys.insert((edge.from.as_str(), edge.to.as_str(), edge.port.as_deref())) {
                return Err(format!("重复的边：{} -> {}", edge.from, edge.to));
            }

            let from_type = self.node_type(&edge.from).unwrap();
            match from_type {
                NodeType::Condition => match edge.port.as_deref() {
                    Some("true") | Some("false") => {}
                    other => {
                        return Err(format!(
                            "condition 节点 {} 的出边端口必须是 true/false，当前为 {:?}",
                            edge.from, other
                        ))
                    }
                },
                _ => {
                    if edge.port.is_some() {
                        return Err(format!("非 condition 节点 {} 的出边不能带端口", edge.from));
                    }
                }
            }
        }

        for node in &self.nodes {
            let kind = node.kind().unwrap();
            match kind {
                NodeType::Start => {
                    if !self.incoming(&node.id).is_empty() {
                        return Err(format!("start 节点 {} 不能有入边", node.id));
                    }
                }
                NodeType::End => {
                    if !self.outgoing(&node.id).is_empty() {
                        return Err(format!("end 节点 {} 不能有出边", node.id));
                    }
                }
                _ => {
                    if self.incoming(&node.id).is_empty() {
                        return Err(format!("节点 {} 没有入边，永远无法触发", node.id));
                    }
                }
            }
        }

        self.check_acyclic_and_reachable()
    }

    fn check_acyclic_and_reachable(&self) -> Result<(), String> {
        let mut indegree: HashMap<&str, usize> = self
            .nodes
            .iter()
            .map(|n| (n.id.as_str(), self.incoming(&n.id).len()))
            .collect();

        let mut queue: VecDeque<&str> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(id, _)| *id)
            .collect();
        let mut sorted = 0usize;
        while let Some(id) = queue.pop_front() {
            sorted += 1;
            for edge in self.outgoing(id) {
                if let Some(d) = indegree.get_mut(edge.to.as_str()) {
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(edge.to.as_str());
                    }
                }
            }
        }
        if sorted != self.nodes.len() {
            return Err("工作流存在环，必须是 DAG".into());
        }

        let start = self.start_node().unwrap();
        let mut visited: HashSet<&str> = HashSet::new();
        let mut stack = vec![start.id.as_str()];
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            for edge in self.outgoing(id) {
                stack.push(edge.to.as_str());
            }
        }
        let unreachable: Vec<&str> = self
            .nodes
            .iter()
            .map(|n| n.id.as_str())
            .filter(|id| !visited.contains(id))
            .collect();
        if !unreachable.is_empty() {
            return Err(format!(
                "存在从 start 不可达的节点：{}",
                unreachable.join(", ")
            ));
        }

        Ok(())
    }
}

fn validate_params(node: &Node, kind: NodeType) -> Result<(), String> {
    let need_str = |key: &str| match node.param_str(key) {
        Some(v) if !v.trim().is_empty() => Ok(()),
        _ => Err(format!(
            "节点 {}（{}）缺少参数 {}",
            node.id,
            kind.as_str(),
            key
        )),
    };
    match kind {
        NodeType::Script => need_str("code"),
        NodeType::Condition => need_str("expr"),
        NodeType::HttpCall => {
            need_str("url")?;
            match node.param_str("method") {
                Some(method) if method.trim().is_empty() => {
                    return Err(format!("节点 {} 的 method 不能为空", node.id));
                }
                Some(method) if !HTTP_METHODS.contains(&method.trim().to_uppercase().as_str()) => {
                    return Err(format!(
                        "节点 {} 的 method 非法：{method:?}（允许 {}）",
                        node.id,
                        HTTP_METHODS.join("/")
                    ));
                }
                _ => {}
            }
            Ok(())
        }
        NodeType::Delay => match node.param_u64("ms") {
            Some(_) => Ok(()),
            None => Err(format!("节点 {}（delay）缺少参数 ms", node.id)),
        },
        NodeType::SubWorkflow => need_str("workflow_id"),
        NodeType::Start | NodeType::End | NodeType::HumanTask => Ok(()),
    }
}

/// 图校验 / 重试策略 / 词汇表的表驱动单测。
///
/// `validate` 是「建图即校验」的判官（DESIGN.md §4），过去只有 `ws_rpc.rs` /
/// `backend-e2e` 的黑盒覆盖——要起进程才知道一句「工作流存在环」。规则本身是
/// 纯函数，在这按表驱动逐条钉住。
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 一个带合法默认参数的节点。用它的前提是想验证**图结构**规则，
    /// 不想被「缺参数」提前挡下——所以按类型补上必填参数。
    fn node(id: &str, kind: &str) -> Value {
        let params = match kind {
            "script" => json!({"code": "return 1;"}),
            "condition" => json!({"expr": "true"}),
            "delay" => json!({"ms": 5}),
            "sub_workflow" => json!({"workflow_id": "child"}),
            "http_call" => json!({"url": "http://127.0.0.1:1"}),
            _ => json!({}),
        };
        json!({"id": id, "type": kind, "params": params})
    }

    fn def(nodes: Vec<Value>, edges: Vec<Value>) -> Definition {
        serde_json::from_value(json!({"nodes": nodes, "edges": edges}))
            .expect("测试定义应可反序列化")
    }

    /// start → n → end 的一条直线。
    fn linear() -> Definition {
        def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![json!({"from": "s", "to": "n"}), json!({"from": "n", "to": "e"})],
        )
    }

    #[test]
    fn linear_graph_passes() {
        assert!(linear().validate().is_ok());
    }

    #[test]
    fn empty_graph_and_start_end_shape() {
        assert_eq!(def(vec![], vec![]).validate().unwrap_err(), "工作流没有任何节点");
        // 没有 start
        let no_start = def(vec![node("n", "script"), node("e", "end")], vec![json!({"from":"n","to":"e"})]);
        assert!(no_start.validate().unwrap_err().contains("start 节点"));
        // 两个 start
        let two_starts = def(
            vec![node("s1", "start"), node("s2", "start"), node("e", "end")],
            vec![json!({"from":"s1","to":"e"}), json!({"from":"s2","to":"e"})],
        );
        assert!(two_starts.validate().unwrap_err().contains("start 节点"));
        // 没有 end
        let no_end = def(vec![node("s", "start"), node("n", "script")], vec![json!({"from":"s","to":"n"})]);
        assert!(no_end.validate().unwrap_err().contains("end 节点"));
    }

    #[test]
    fn duplicate_ids_blank_ids_and_unknown_types_are_rejected() {
        let dup = def(
            vec![node("s", "start"), node("n", "script"), node("n", "delay")],
            vec![],
        );
        assert!(dup.validate().unwrap_err().contains("id 重复"), "{:?}", dup.validate());
        let blank = def(vec![node(" ", "start"), node("e", "end")], vec![json!({"from":" ","to":"e"})]);
        assert!(blank.validate().unwrap_err().contains("空的节点 id"));
        let unknown = def(vec![node("s", "start"), node("x", "wat"), node("e", "end")], vec![]);
        assert!(unknown.validate().unwrap_err().contains("类型未知"));
    }

    #[test]
    fn edges_must_reference_existing_nodes_and_be_unique() {
        let dangling = def(
            vec![node("s", "start"), node("e", "end")],
            vec![json!({"from": "s", "to": "nope"})],
        );
        assert!(dangling.validate().unwrap_err().contains("终点不存在"));

        let self_loop = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"n"}), json!({"from":"n","to":"e"})],
        );
        assert!(self_loop.validate().unwrap_err().contains("自环"));

        let dupe = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"}), json!({"from":"n","to":"e"})],
        );
        assert!(dupe.validate().unwrap_err().contains("重复的边"));
    }

    #[test]
    fn condition_ports_only_on_condition_nodes() {
        let ok = def(
            vec![node("s", "start"), node("c", "condition")],
            vec![json!({"from":"c","to":"s","port":"true"})],
        );
        assert!(ok.validate().is_err(), "没有 end 会先失败；换 end 再看端口规则");

        let with_ends = def(
            vec![node("s", "start"), node("c", "condition"), node("t", "script"), node("e", "end")],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"t","port":"true"}),
                json!({"from":"t","to":"e"}),
            ],
        );
        assert!(with_ends.validate().is_ok(), "{:?}", with_ends.validate());

        let bad_port = def(
            vec![node("s", "start"), node("c", "condition"), node("t", "script"), node("e", "end")],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"t","port":"maybe"}),
                json!({"from":"t","to":"e"}),
            ],
        );
        assert!(bad_port.validate().unwrap_err().contains("端口必须是 true/false"));

        let stray_port = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![json!({"from":"s","to":"n","port":"true"}), json!({"from":"n","to":"e"})],
        );
        assert!(stray_port.validate().unwrap_err().contains("不能带端口"));
    }

    #[test]
    fn start_has_no_incoming_and_end_has_no_outgoing() {
        let bad_start = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![json!({"from":"n","to":"s"}), json!({"from":"s","to":"e"})],
        );
        assert!(bad_start.validate().unwrap_err().contains("start 节点"));

        // end 出边也违规
        let bad_end = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"}), json!({"from":"e","to":"n"})],
        );
        assert!(bad_end.validate().unwrap_err().contains("end 节点"));
    }

    #[test]
    fn cycles_and_unreachable_nodes_are_rejected() {
        let cyclic = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![
                json!({"from":"s","to":"n"}),
                json!({"from":"n","to":"n"}),   // 自环：先被自环规则拦下
                json!({"from":"n","to":"e"}),
            ],
        );
        assert!(cyclic.validate().unwrap_err().contains("自环"));

        // 两节点互成环（nodes_scope 那一类用例的形状）
        let two_loop = def(
            vec![
                node("s", "start"), node("n", "script"), node("m", "script"), node("e", "end"),
            ],
            vec![
                json!({"from":"s","to":"n"}),
                json!({"from":"n","to":"m"}),
                json!({"from":"m","to":"n"}),
                json!({"from":"m","to":"e"}),
            ],
        );
        assert!(two_loop.validate().unwrap_err().contains("环"));

        // 中间节点没有入边：永远触发不了
        let no_incoming = def(
            vec![node("s", "start"), node("dead", "script"), node("e", "end")],
            vec![json!({"from":"s","to":"e"})],
        );
        assert!(no_incoming.validate().unwrap_err().contains("没有入边"));

        // 「不可达」分支其实是防御性的：非 start 节点都被要求有一条入边，所以
        // 任何不可达节点沿入边往上走必然撞到环（跑不出有限图）或被「没有入边」
        // 先拦下。两种形状各验一条，钉住这个结论——日后若有人加出第三条漏网的
        // 形状，这里会响。
        let acyclic_island = def(
            vec![
                node("s", "start"), node("e", "end"),
                node("a", "script"), node("b", "script"),
            ],
            vec![json!({"from":"s","to":"e"}), json!({"from":"a","to":"b"})],
        );
        assert!(
            acyclic_island.validate().unwrap_err().contains("没有入边"),
            "无环孤岛必被「没有入边」拦下"
        );
        let cyclic_island = def(
            vec![
                node("s", "start"), node("e", "end"),
                node("a", "script"), node("b", "script"),
            ],
            vec![
                json!({"from":"s","to":"e"}),
                json!({"from":"a","to":"b"}),
                json!({"from":"b","to":"a"}),
            ],
        );
        assert!(
            cyclic_island.validate().unwrap_err().contains("环"),
            "成环孤岛必被「环」拦下"
        );
    }

    #[test]
    fn node_params_are_validated_per_kind() {
        for (kind, params, needle) in [
            ("script", json!({}), "code"),
            ("condition", json!({}), "expr"),
            ("delay", json!({}), "ms"),
            ("sub_workflow", json!({}), "workflow_id"),
        ] {
            let d = def(
                vec![
                    node("s", "start"),
                    json!({"id": "n", "type": kind, "params": params}),
                    node("e", "end"),
                ],
                vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
            );
            assert!(d.validate().unwrap_err().contains(needle), "{kind} 应缺 {needle}");
        }

        // http_call：url 必填，method 大小写不敏感且受白名单约束
        for method in ["GET", "get", "Put", "delete"] {
            let d = def(
                vec![
                    node("s", "start"),
                    json!({"id":"n","type":"http_call",
                           "params":{"url":"http://x","method":method}}),
                    node("e", "end"),
                ],
                vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
            );
            assert!(d.validate().is_ok(), "{method} 应被接受");
        }
        for method in ["HEAD", "FETCH", ""] {
            let d = def(
                vec![
                    node("s", "start"),
                    json!({"id":"n","type":"http_call",
                           "params":{"url":"http://x","method":method}}),
                    node("e", "end"),
                ],
                vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
            );
            assert!(d.validate().is_err(), "{method:?} 应被拒绝");
        }
    }

    #[test]
    fn retry_policy_defaults_and_clamps() {
        // 无 retry：1 次不重试、0 退避
        assert_eq!(
            RetryPolicy::from_params(&json!({})),
            RetryPolicy { max_attempts: 1, backoff_ms: 0 }
        );
        assert_eq!(RetryPolicy::default(), RetryPolicy { max_attempts: 1, backoff_ms: 0 });
        // 0 被抬到 1：max_attempts 永远至少一次
        assert_eq!(
            RetryPolicy::from_params(&json!({"retry": {"max_attempts": 0, "backoff_ms": 9}})),
            RetryPolicy { max_attempts: 1, backoff_ms: 9 }
        );
        // 缺 backoff_ms 用 0
        assert_eq!(
            RetryPolicy::from_params(&json!({"retry": {"max_attempts": 5}})),
            RetryPolicy { max_attempts: 5, backoff_ms: 0 }
        );
    }

    #[test]
    fn node_type_vocabulary_round_trips_and_flags_side_effects() {
        for kind in [
            "start", "end", "script", "condition", "delay", "http_call", "human_task",
            "sub_workflow",
        ] {
            let parsed = NodeType::parse(kind).unwrap_or_else(|| panic!("{kind} 应可解析"));
            assert_eq!(parsed.as_str(), kind, "{kind} 应原样往返");
        }
        assert!(NodeType::parse("nope").is_none());
        assert!(NodeType::parse("").is_none());

        // 只有 http_call 有外部副作用（崩溃后不可安全重放）
        assert!(NodeType::HttpCall.has_side_effect());
        for kind in [NodeType::Start, NodeType::End, NodeType::Script, NodeType::Delay] {
            assert!(!kind.has_side_effect(), "{kind:?} 不该有副作用");
        }
    }

    #[test]
    fn graph_accessors_and_lookup() {
        let d = linear();
        assert_eq!(d.node("n").unwrap().kind(), Some(NodeType::Script));
        assert!(d.node("nope").is_none());
        assert_eq!(d.node_type("n"), Some(NodeType::Script));
        assert_eq!(d.start_node().unwrap().id, "s");
        assert_eq!(d.incoming("n").len(), 1);
        assert_eq!(d.outgoing("s").len(), 1);
        assert_eq!(d.incoming("s").len(), 0, "start 没有入边");
        assert_eq!(d.outgoing("e").len(), 0, "end 没有出边");
    }
}
