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

    pub fn type_map(&self) -> HashMap<String, NodeType> {
        self.nodes
            .iter()
            .filter_map(|n| n.kind().map(|k| (n.id.clone(), k)))
            .collect()
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
