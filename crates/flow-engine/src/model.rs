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
    /// 调 OpenAI 兼容的 chat/completions 接口，输出 content/model/usage
    Llm,
    /// 经 HTTP API（Resend 兼容格式）发邮件
    Email,
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
            NodeType::Llm => "llm",
            NodeType::Email => "email",
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
            "llm" => NodeType::Llm,
            "email" => NodeType::Email,
            _ => return None,
        })
    }

    /// 崩溃后是否不可安全重放：有外部副作用的节点必须人工裁决。
    pub fn has_side_effect(self) -> bool {
        matches!(self, NodeType::HttpCall | NodeType::Llm | NodeType::Email)
    }

    /// 全部节点类型。数组顺序 = nodetypes.list 响应顺序 = 前端面板顺序。
    pub const ALL: [NodeType; 10] = [
        NodeType::Start,
        NodeType::End,
        NodeType::Script,
        NodeType::Condition,
        NodeType::Delay,
        NodeType::HttpCall,
        NodeType::HumanTask,
        NodeType::SubWorkflow,
        NodeType::Llm,
        NodeType::Email,
    ];

    /// 前端拖拽面板 + 参数表单所需的能力描述（nodetypes.list 的单条）。
    ///
    /// `params_schema` 是 JSON Schema draft-07 子集（type/required/properties/enum/default），
    /// 另带 `x-widget`（code/json/workflow-picker）、`x-label`、`x-help` 扩展，
    /// 前端据此递归渲染参数表单，后端 validate 仍以本文件的 validate_params 为准。
    pub fn descriptor(self) -> Value {
        match self {
            NodeType::Start => serde_json::json!({
                "type": "start",
                "label": "开始",
                "category": "control",
                "max_instances": 1,
                "ports": [{"id": "out", "label": "出"}],
                "params_schema": {"type": "object", "properties": {}}
            }),
            NodeType::End => serde_json::json!({
                "type": "end",
                "label": "结束",
                "category": "control",
                "ports": [{"id": "in", "label": "入"}],
                "params_schema": {"type": "object", "properties": {}}
            }),
            NodeType::Script => serde_json::json!({
                "type": "script",
                "label": "脚本",
                "category": "compute",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["code"],
                    "properties": {
                        "code": {"type": "string", "x-widget": "code", "x-label": "JS 函数体",
                                 "x-help": "可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果"},
                        "timeout_ms": {"type": "integer", "default": 2000, "x-label": "脚本超时（毫秒）"}
                    }
                },
                "supports_retry": true
            }),
            NodeType::Condition => serde_json::json!({
                "type": "condition",
                "label": "条件分支",
                "category": "control",
                "ports": [{"id": "in", "label": "入"}, {"id": "true", "label": "真"}, {"id": "false", "label": "假"}],
                "params_schema": {
                    "type": "object",
                    "required": ["expr"],
                    "properties": {
                        "expr": {"type": "string", "x-widget": "code", "x-label": "条件表达式",
                                 "x-help": "表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes"},
                        "timeout_ms": {"type": "integer", "default": 2000, "x-label": "求值超时（毫秒）"}
                    }
                }
            }),
            NodeType::Delay => serde_json::json!({
                "type": "delay",
                "label": "等待",
                "category": "control",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["ms"],
                    "properties": {
                        "ms": {"type": "integer", "x-label": "时长（毫秒）",
                                "x-help": "数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）"}
                    }
                }
            }),
            NodeType::HttpCall => serde_json::json!({
                "type": "http_call",
                "label": "HTTP 请求",
                "category": "integration",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["url"],
                    "properties": {
                        "method": {"type": "string", "enum": HTTP_METHODS, "default": "GET", "x-label": "方法"},
                        "url": {"type": "string", "x-label": "URL",
                                "x-help": "支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）"},
                        "headers": {"x-widget": "json", "default": {}, "x-label": "请求头"},
                        "body": {"x-widget": "json", "x-label": "请求体"},
                        "timeout_ms": {"type": "integer", "default": 30000, "x-label": "HTTP 超时（毫秒）"}
                    }
                },
                "supports_retry": true,
                "side_effect": true
            }),
            NodeType::HumanTask => serde_json::json!({
                "type": "human_task",
                "label": "人工节点",
                "category": "human",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "properties": {
                        "prompt": {"type": "string", "x-label": "提示"}
                    }
                }
            }),
            NodeType::SubWorkflow => serde_json::json!({
                "type": "sub_workflow",
                "label": "子工作流",
                "category": "control",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["workflow_id"],
                    "properties": {
                        "workflow_id": {"type": "string", "x-widget": "workflow-picker", "x-label": "目标工作流",
                                        "x-help": "调用其最新已发布版本作为子 run；子 run 输出透传为本节点输出；子 run 失败传导为本节点 fatal（DESIGN §6.8），重试策略只覆盖启动/等待类错误"},
                        "input_mapping": {"x-widget": "json", "x-label": "子 run 输入映射",
                                        "x-help": "JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入"}
                    }
                },
                "supports_retry": true
            }),
            NodeType::Llm => serde_json::json!({
                "type": "llm",
                "label": "LLM 调用",
                "category": "ai",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["api_key", "model", "prompt"],
                    "properties": {
                        "base_url": {"type": "string", "default": "https://api.openai.com/v1", "x-label": "API 地址",
                                     "x-help": "OpenAI 兼容端点，请求发往 {base_url}/chat/completions"},
                        "api_key": {"type": "string", "x-secret": true, "x-label": "API 密钥名称",
                                    "x-help": "只存密钥名称（如 OPENAI_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list"},
                        "model": {"type": "string", "x-label": "模型"},
                        "system": {"type": "string", "x-label": "系统提示"},
                        "prompt": {"type": "string", "x-widget": "code", "x-label": "提示词",
                                   "x-help": "支持 ${input.x} / ${nodes.n.y} 模板"},
                        "temperature": {"type": "number", "x-label": "温度"},
                        "max_tokens": {"type": "integer", "x-label": "最大 token 数"},
                        "json_mode": {"type": "boolean", "default": false, "x-label": "JSON 模式",
                                      "x-help": "开启后请求带 response_format: {\"type\":\"json_object\"}"}
                    }
                },
                "supports_retry": true,
                "side_effect": true
            }),
            NodeType::Email => serde_json::json!({
                "type": "email",
                "label": "邮件",
                "category": "notify",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["api_key", "from", "to", "subject", "body"],
                    "properties": {
                        "endpoint": {"type": "string", "default": "https://api.resend.com/emails", "x-label": "API 端点",
                                     "x-help": "Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证"},
                        "api_key": {"type": "string", "x-secret": true, "x-label": "API 密钥名称",
                                    "x-help": "只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list"},
                        "from": {"type": "string", "x-label": "发件人"},
                        "to": {"type": "string", "x-label": "收件人"},
                        "subject": {"type": "string", "x-label": "主题",
                                    "x-help": "支持 ${input.x} / ${nodes.n.y} 模板"},
                        "body": {"type": "string", "x-widget": "code", "x-label": "正文",
                                 "x-help": "纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板"}
                    }
                },
                "supports_retry": true,
                "side_effect": true
            }),
        }
    }

    /// params_schema 中标记 `x-secret` 的参数名。definition 里这些参数只存
    /// 密钥**名称**，真值在执行前按名称从 `FLOW_SECRET_<名称>` 环境变量注入
    /// （见 secrets.rs）。列表从 descriptor 派生，schema 是唯一事实源。
    pub fn secret_params(self) -> Vec<String> {
        let descriptor = self.descriptor();
        let Some(properties) = descriptor
            .pointer("/params_schema/properties")
            .and_then(Value::as_object)
        else {
            return Vec::new();
        };
        properties
            .iter()
            .filter(|(_, schema)| schema.get("x-secret") == Some(&Value::Bool(true)))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// 该类型 params 中**不可**被 `${}` 模板展开的「代码承载字段」。
    ///
    /// `script.code` / `condition.expr` 是用户 JS：其中的 `${}` 是 JS 模板
    /// 字面量，不是 flow 模板，展开会破坏用户代码。其余参数一律在执行前
    /// 统一展开（与 http_call 同一份 `expr::expand_templates`，DESIGN §10）。
    /// 词汇表只有这一处：新增类型时编译器不逼你，但执行层的默认行为
    /// （全展开）对纯参数类型就是正确的。
    pub fn opaque_params(self) -> &'static [&'static str] {
        match self {
            NodeType::Script => &["code"],
            NodeType::Condition => &["expr"],
            _ => &[],
        }
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
                // ${} 模板 method 执行期展开后才能判定，放行；执行层按同一份
                // HTTP_METHODS 再验一次（exec::run_http），两层共用一个词汇表
                Some(method) if method.contains("${") => {}
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
            // ${} 模板要在执行期展开后才可判定：放行，运行期 parse
            // （裸数字字符串仍拒绝——一种参数一种形态，不留灰色地带）
            None if node.param_str("ms").is_some_and(|s| s.contains("${")) => Ok(()),
            None => Err(format!("节点 {}（delay）缺少参数 ms", node.id)),
        },
        NodeType::SubWorkflow => {
            need_str("workflow_id")?;
            // input_mapping 省略 = 沿用旧语义（父 run 输入快照，DESIGN §6.8）
            match node.params.get("input_mapping") {
                None | Some(serde_json::Value::Null) => Ok(()),
                Some(serde_json::Value::Object(_)) => Ok(()),
                Some(serde_json::Value::String(s)) if !s.trim().is_empty() => Ok(()),
                Some(_) => Err(format!(
                    "节点 {}（sub_workflow）的 input_mapping 必须是对象或 ${{}} 模板字符串",
                    node.id
                )),
            }
        }
        NodeType::Llm => {
            need_str("api_key")?;
            need_str("model")?;
            need_str("prompt")
        }
        NodeType::Email => {
            need_str("api_key")?;
            need_str("from")?;
            need_str("to")?;
            need_str("subject")?;
            need_str("body")
        }
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
            vec![
                json!({"from": "s", "to": "n"}),
                json!({"from": "n", "to": "e"}),
            ],
        )
    }

    #[test]
    fn linear_graph_passes() {
        assert!(linear().validate().is_ok());
    }

    #[test]
    fn empty_graph_and_start_end_shape() {
        assert_eq!(
            def(vec![], vec![]).validate().unwrap_err(),
            "工作流没有任何节点"
        );
        // 没有 start
        let no_start = def(
            vec![node("n", "script"), node("e", "end")],
            vec![json!({"from":"n","to":"e"})],
        );
        assert!(no_start.validate().unwrap_err().contains("start 节点"));
        // 两个 start
        let two_starts = def(
            vec![node("s1", "start"), node("s2", "start"), node("e", "end")],
            vec![json!({"from":"s1","to":"e"}), json!({"from":"s2","to":"e"})],
        );
        assert!(two_starts.validate().unwrap_err().contains("start 节点"));
        // 没有 end
        let no_end = def(
            vec![node("s", "start"), node("n", "script")],
            vec![json!({"from":"s","to":"n"})],
        );
        assert!(no_end.validate().unwrap_err().contains("end 节点"));
    }

    #[test]
    fn duplicate_ids_blank_ids_and_unknown_types_are_rejected() {
        let dup = def(
            vec![node("s", "start"), node("n", "script"), node("n", "delay")],
            vec![],
        );
        assert!(
            dup.validate().unwrap_err().contains("id 重复"),
            "{:?}",
            dup.validate()
        );
        let blank = def(
            vec![node(" ", "start"), node("e", "end")],
            vec![json!({"from":" ","to":"e"})],
        );
        assert!(blank.validate().unwrap_err().contains("空的节点 id"));
        let unknown = def(
            vec![node("s", "start"), node("x", "wat"), node("e", "end")],
            vec![],
        );
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
            vec![
                json!({"from":"s","to":"n"}),
                json!({"from":"n","to":"n"}),
                json!({"from":"n","to":"e"}),
            ],
        );
        assert!(self_loop.validate().unwrap_err().contains("自环"));

        let dupe = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![
                json!({"from":"s","to":"n"}),
                json!({"from":"n","to":"e"}),
                json!({"from":"n","to":"e"}),
            ],
        );
        assert!(dupe.validate().unwrap_err().contains("重复的边"));
    }

    #[test]
    fn condition_ports_only_on_condition_nodes() {
        let ok = def(
            vec![node("s", "start"), node("c", "condition")],
            vec![json!({"from":"c","to":"s","port":"true"})],
        );
        assert!(
            ok.validate().is_err(),
            "没有 end 会先失败；换 end 再看端口规则"
        );

        let with_ends = def(
            vec![
                node("s", "start"),
                node("c", "condition"),
                node("t", "script"),
                node("e", "end"),
            ],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"t","port":"true"}),
                json!({"from":"t","to":"e"}),
            ],
        );
        assert!(with_ends.validate().is_ok(), "{:?}", with_ends.validate());

        let bad_port = def(
            vec![
                node("s", "start"),
                node("c", "condition"),
                node("t", "script"),
                node("e", "end"),
            ],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"t","port":"maybe"}),
                json!({"from":"t","to":"e"}),
            ],
        );
        assert!(bad_port
            .validate()
            .unwrap_err()
            .contains("端口必须是 true/false"));

        let stray_port = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![
                json!({"from":"s","to":"n","port":"true"}),
                json!({"from":"n","to":"e"}),
            ],
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
            vec![
                json!({"from":"s","to":"n"}),
                json!({"from":"n","to":"e"}),
                json!({"from":"e","to":"n"}),
            ],
        );
        assert!(bad_end.validate().unwrap_err().contains("end 节点"));
    }

    #[test]
    fn cycles_and_unreachable_nodes_are_rejected() {
        let cyclic = def(
            vec![node("s", "start"), node("n", "script"), node("e", "end")],
            vec![
                json!({"from":"s","to":"n"}),
                json!({"from":"n","to":"n"}), // 自环：先被自环规则拦下
                json!({"from":"n","to":"e"}),
            ],
        );
        assert!(cyclic.validate().unwrap_err().contains("自环"));

        // 两节点互成环（nodes_scope 那一类用例的形状）
        let two_loop = def(
            vec![
                node("s", "start"),
                node("n", "script"),
                node("m", "script"),
                node("e", "end"),
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
                node("s", "start"),
                node("e", "end"),
                node("a", "script"),
                node("b", "script"),
            ],
            vec![json!({"from":"s","to":"e"}), json!({"from":"a","to":"b"})],
        );
        assert!(
            acyclic_island.validate().unwrap_err().contains("没有入边"),
            "无环孤岛必被「没有入边」拦下"
        );
        let cyclic_island = def(
            vec![
                node("s", "start"),
                node("e", "end"),
                node("a", "script"),
                node("b", "script"),
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
            assert!(
                d.validate().unwrap_err().contains(needle),
                "{kind} 应缺 {needle}"
            );
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

    /// `${}` 模板参数在建图期放行、执行期判定（DESIGN §5 规则 1、§10）：
    /// 模板 ms / 模板 method 通过；裸数字串、标量 mapping 仍被拒绝。
    #[test]
    fn template_params_pass_validation_while_ambiguous_forms_are_rejected() {
        fn check(kind: &str, params: Value) -> Result<(), String> {
            let d = def(
                vec![
                    node("s", "start"),
                    json!({"id": "n", "type": kind, "params": params}),
                    node("e", "end"),
                ],
                vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
            );
            d.validate()
        }

        assert!(check("delay", json!({"ms": "${input.tick}"})).is_ok());
        assert!(
            check("delay", json!({"ms": "5000"})).is_err(),
            "裸数字串不留灰色地带"
        );
        assert!(check("delay", json!({"ms": "soon"})).is_err());
        assert!(check(
            "http_call",
            json!({"url": "http://x", "method": "${input.m}"})
        )
        .is_ok());

        // input_mapping：对象 / 非空模板串放行，其余拒绝
        assert!(check(
            "sub_workflow",
            json!({"workflow_id": "c", "input_mapping": {"a": "${input.x}"}})
        )
        .is_ok());
        assert!(check(
            "sub_workflow",
            json!({"workflow_id": "c", "input_mapping": "${nodes.n1}"})
        )
        .is_ok());
        assert!(check(
            "sub_workflow",
            json!({"workflow_id": "c", "input_mapping": 7})
        )
        .is_err());
        assert!(check(
            "sub_workflow",
            json!({"workflow_id": "c", "input_mapping": ""})
        )
        .is_err());
    }

    #[test]
    fn retry_policy_defaults_and_clamps() {
        // 无 retry：1 次不重试、0 退避
        assert_eq!(
            RetryPolicy::from_params(&json!({})),
            RetryPolicy {
                max_attempts: 1,
                backoff_ms: 0
            }
        );
        assert_eq!(
            RetryPolicy::default(),
            RetryPolicy {
                max_attempts: 1,
                backoff_ms: 0
            }
        );
        // 0 被抬到 1：max_attempts 永远至少一次
        assert_eq!(
            RetryPolicy::from_params(&json!({"retry": {"max_attempts": 0, "backoff_ms": 9}})),
            RetryPolicy {
                max_attempts: 1,
                backoff_ms: 9
            }
        );
        // 缺 backoff_ms 用 0
        assert_eq!(
            RetryPolicy::from_params(&json!({"retry": {"max_attempts": 5}})),
            RetryPolicy {
                max_attempts: 5,
                backoff_ms: 0
            }
        );
    }

    #[test]
    fn node_type_vocabulary_round_trips_and_flags_side_effects() {
        for kind in [
            "start",
            "end",
            "script",
            "condition",
            "delay",
            "http_call",
            "human_task",
            "sub_workflow",
            "llm",
            "email",
        ] {
            let parsed = NodeType::parse(kind).unwrap_or_else(|| panic!("{kind} 应可解析"));
            assert_eq!(parsed.as_str(), kind, "{kind} 应原样往返");
        }
        assert_eq!(NodeType::ALL.len(), 10, "新类型必须登记进 ALL");
        for kind in NodeType::ALL {
            assert_eq!(
                kind.descriptor()["type"],
                json!(kind.as_str()),
                "{kind:?} 的 descriptor.type 必须与词汇表一致"
            );
        }
        assert!(NodeType::parse("nope").is_none());
        assert!(NodeType::parse("").is_none());

        // http_call / llm / email 有外部副作用（崩溃后不可安全重放）
        for kind in [NodeType::HttpCall, NodeType::Llm, NodeType::Email] {
            assert!(kind.has_side_effect(), "{kind:?} 应有副作用");
        }
        for kind in [
            NodeType::Start,
            NodeType::End,
            NodeType::Script,
            NodeType::Delay,
        ] {
            assert!(!kind.has_side_effect(), "{kind:?} 不该有副作用");
        }
    }

    /// x-secret 参数清单从 descriptor 派生：schema 是唯一事实源。
    #[test]
    fn secret_params_come_from_descriptor_schema() {
        assert_eq!(NodeType::Llm.secret_params(), vec!["api_key".to_string()]);
        assert_eq!(NodeType::Email.secret_params(), vec!["api_key".to_string()]);
        assert!(NodeType::HttpCall.secret_params().is_empty());
        assert!(NodeType::Script.secret_params().is_empty());
    }

    /// llm / email 的必填参数校验。
    #[test]
    fn llm_and_email_require_their_params() {
        let llm = def(
            vec![
                node("s", "start"),
                json!({"id": "n", "type": "llm", "params": {"model": "m"}}),
                node("e", "end"),
            ],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
        );
        let err = llm.validate().unwrap_err();
        assert!(err.contains("api_key"), "{err}");

        let ok = def(
            vec![
                node("s", "start"),
                json!({"id": "n", "type": "llm", "params": {"api_key": "K", "model": "m", "prompt": "p"}}),
                node("e", "end"),
            ],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
        );
        assert!(ok.validate().is_ok());

        let email = def(
            vec![
                node("s", "start"),
                json!({"id": "n", "type": "email", "params": {"api_key": "K", "from": "a@b.c"}}),
                node("e", "end"),
            ],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
        );
        let err = email.validate().unwrap_err();
        assert!(err.contains("to"), "{err}");
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
