use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{LazyLock, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 节点类型。前端拖拽面板与引擎共用这一份定义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    /// 外部 agent CLI（kimi / claude / codex 等）：prompt 经 stdin 传入，stdout 作为输出
    Harness,
    /// 经 HTTP API（Resend 兼容格式）发邮件
    Email,
}

/// 节点类型与其字符串名的对应表：**变体清单的唯一来源**。
///
/// `as_str` / `parse` / `ALL` / `descriptor` 的下标 / `has_side_effect` 全部
/// 由这一张表派生。**新增节点类型只改这张表与 enum**：这些面各自手写时，
/// 漏改的那几处不会编译报错，只在运行期表现为「类型不认识」或前端面板少一项。
///
/// 表用 `(变体, 字符串)` 而不是靠 `as_str` 反推：字符串是 wire 格式与落库内容，
/// 让它显式出现在表里，改名时编译器与快照测试一起响。
const NODE_TYPE_TABLE: [(NodeType, &str); 10] = [
    (NodeType::Start, "start"),
    (NodeType::End, "end"),
    (NodeType::Script, "script"),
    (NodeType::Condition, "condition"),
    (NodeType::Delay, "delay"),
    (NodeType::HttpCall, "http_call"),
    (NodeType::HumanTask, "human_task"),
    (NodeType::SubWorkflow, "sub_workflow"),
    (NodeType::Harness, "harness"),
    (NodeType::Email, "email"),
];

impl NodeType {
    /// 全部节点类型，顺序 = 表的顺序 = `nodetypes.list` 响应顺序 = 前端面板顺序。
    pub const ALL: [NodeType; 10] = [
        NodeType::Start,
        NodeType::End,
        NodeType::Script,
        NodeType::Condition,
        NodeType::Delay,
        NodeType::HttpCall,
        NodeType::HumanTask,
        NodeType::SubWorkflow,
        NodeType::Harness,
        NodeType::Email,
    ];

    pub fn as_str(self) -> &'static str {
        NODE_TYPE_TABLE
            .iter()
            .find(|(kind, _)| *kind == self)
            .map(|(_, name)| *name)
            .expect("NodeType 的每个变体都在 NODE_TYPE_TABLE 里（node_type_table_is_exhaustive）")
    }

    pub fn parse(s: &str) -> Option<NodeType> {
        NODE_TYPE_TABLE
            .iter()
            .find(|(_, name)| *name == s)
            .map(|(kind, _)| *kind)
    }

    /// 崩溃后是否不可安全重放：有外部副作用的节点必须人工裁决。
    pub fn has_side_effect(self) -> bool {
        matches!(
            self,
            NodeType::HttpCall | NodeType::Harness | NodeType::Email
        )
    }

    /// 能力描述的**主体**（`nodetypes.list` 单条去掉 `"type"` 字段）。
    ///
    /// `"type"` 由 [`Self::descriptor`] 从 [`NODE_TYPE_TABLE`] 注入，不在这里
    /// 重写一遍：同一个类型名字符串出现在表、`as_str`、`parse`、这里共四处时，
    /// 改名必漏其中一处。
    ///
    /// `params_schema` 是 JSON Schema draft-07 子集
    /// （type/required/properties/enum/default），另带 `x-widget`
    /// （code/json/workflow-picker）、`x-label`、`x-help`、`x-secret` 扩展，
    /// 前端据此递归渲染参数表单；后端校验以 [`validate_params`] 为准，
    /// 它读的必填清单也来自本函数（见 [`Self::required_params`]）。
    fn descriptor_body(self) -> Value {
        match self {
            NodeType::Start => serde_json::json!({
                "label": "开始",
                "category": "control",
                "max_instances": 1,
                "ports": [{"id": "out", "label": "出"}],
                "params_schema": {"type": "object", "properties": {}}
            }),
            NodeType::End => serde_json::json!({
                "label": "结束",
                "category": "control",
                "ports": [{"id": "in", "label": "入"}],
                "params_schema": {"type": "object", "properties": {}}
            }),
            NodeType::Script => serde_json::json!({
                "label": "脚本",
                "category": "compute",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["code"],
                    "properties": {
                        "code": {"type": "string", "x-widget": "code", "x-opaque": true,
                                 "x-label": "JS 函数体",
                                 "x-help": "可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果"},
                        "timeout_ms": {"type": "integer", "default": 2000, "x-label": "脚本超时（毫秒）"}
                    }
                },
                "supports_retry": true
            }),
            NodeType::Condition => serde_json::json!({
                "label": "条件分支",
                "category": "control",
                "ports": [{"id": "in", "label": "入"}, {"id": "true", "label": "真"}, {"id": "false", "label": "假"}],
                "params_schema": {
                    "type": "object",
                    "required": ["expr"],
                    "properties": {
                        "expr": {"type": "string", "x-widget": "code", "x-opaque": true,
                                 "x-label": "条件表达式",
                                 "x-help": "表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes"},
                        "timeout_ms": {"type": "integer", "default": 2000, "x-label": "求值超时（毫秒）"}
                    }
                }
            }),
            NodeType::Delay => serde_json::json!({
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
                        "headers": {"x-widget": "key-value", "default": {}, "x-label": "请求头"},
                        "body": {"x-widget": "json", "x-label": "请求体"},
                        "proxy": {"type": "string", "x-label": "代理",
                                  "x-help": "http(s)://[user:pass@]host:port，留空直连"},
                        "timeout_ms": {"type": "integer", "default": 30000, "x-label": "HTTP 超时（毫秒）"}
                    }
                },
                "supports_retry": true,
                "side_effect": true
            }),
            NodeType::HumanTask => serde_json::json!({
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
            NodeType::Harness => serde_json::json!({
                "label": "Harness 代理",
                "category": "ai",
                "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
                "params_schema": {
                    "type": "object",
                    "required": ["command", "prompt"],
                    "properties": {
                        "command": {"type": "string", "x-label": "命令",
                                    "x-help": "要执行的 harness CLI（如 kimi / claude / codex）；prompt 经 stdin 传入，stdout 作为输出"},
                        "prompt": {"type": "string", "x-widget": "code", "x-label": "提示词"},
                        "args": {"type": "array", "items": {"type": "string"}, "x-widget": "json", "x-label": "额外参数"},
                        "workdir": {"type": "string", "x-label": "工作目录"},
                        "timeout_ms": {"type": "number", "default": 300000, "x-label": "超时 (ms)"}
                    }
                },
                "supports_retry": true,
                "side_effect": true
            }),
            NodeType::Email => serde_json::json!({
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

    /// 能力描述（`nodetypes.list` 的单条）。内容由 [`Self::descriptor_body`]
    /// 决定，`"type"` 从 [`NODE_TYPE_TABLE`] 注入。
    ///
    /// 一次性构建 + 缓存：每次调用重跑一遍 `json!` 纯属浪费——构造出来的
    /// 那棵 20 行 schema 树没人改动。返回 `&'static Value` 而非 clone，
    /// 调用方（RPC 的 `node_types`）本来就只要拼进响应里。
    ///
    /// 下标用 `self as usize` 依赖「enum 声明序 == `ALL` 序」；两者都由
    /// [`NODE_TYPE_TABLE`] 的顺序与 enum 声明共同决定，
    /// `node_type_table_is_exhaustive` 钉住这条。
    pub fn descriptor(self) -> &'static Value {
        static DESCRIPTORS: LazyLock<[Value; 10]> = LazyLock::new(|| {
            std::array::from_fn(|i| {
                let kind = NodeType::ALL[i];
                let mut body = kind.descriptor_body();
                body.as_object_mut()
                    .expect("descriptor_body 顶层必为对象")
                    .insert("type".into(), Value::String(kind.as_str().into()));
                body
            })
        });
        &DESCRIPTORS[self as usize]
    }

    /// `params_schema.required` 里的参数名（建图期必填校验的依据）。
    ///
    /// **从 descriptor 派生**，不另写一份清单：`required` 是前端渲染表单与
    /// 后端校验共用的同一份事实，两处手写时改一处漏另一处的症状是
    /// 「前端把可选参数渲染成必填框」或反之。
    ///
    /// 返回 `Vec<&str>` 而非 `&'static [...]`：descriptor 是 `LazyLock` 里的
    /// `Value`，借出的引用活不到 `'static`。调用点是建图校验与 `workflow.update`
    /// （每次保存一次），分配成本无关紧要。
    pub fn required_params(self) -> Vec<&'static str> {
        self.descriptor()
            .pointer("/params_schema/required")
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }

    /// params_schema 中标记 `x-secret` 的参数名。definition 里这些参数只存
    /// 密钥**名称**，真值在执行前按名称从 `FLOW_SECRET_<名称>` 环境变量注入
    /// （见 secrets.rs）。
    ///
    /// **从 descriptor 派生**（扫 `properties` 找 `x-secret: true`）：x-secret
    /// 标记是给前端的渲染指令，派生后「标记了但执行期不注入」这种半配置状态
    /// 不可能存在——分开维护时一个测试代替不了一处真相。
    ///
    /// 代价（可接受）：调用点 `resolve_node_secrets` 在每个节点执行上、
    /// `missing_secrets` 在每次 `workflow.update` 上，各扫一遍 20 行的
    /// `properties`。descriptor 已缓存为 `&'static`，扫的是内存里的树，
    /// 不涉及序列化。
    pub fn secret_params(self) -> Vec<&'static str> {
        self.descriptor()
            .pointer("/params_schema/properties")
            .and_then(Value::as_object)
            .map(|props| {
                props
                    .iter()
                    .filter(|(_, schema)| schema.get("x-secret") == Some(&Value::Bool(true)))
                    .map(|(key, _)| key.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 该类型 params 中**不可**被 `${}` 模板展开的「代码承载字段」。
    ///
    /// `script.code` / `condition.expr` 是用户 JS：其中的 `${}` 是 JS 模板
    /// 字面量，不是 flow 模板，展开会破坏用户代码。其余参数一律在执行前
    /// 统一展开（与 http_call 同一份 `expr::expand_templates`，DESIGN §10）。
    ///
    /// **从 descriptor 的 `x-opaque: true` 派生**，不另写一张表。不能用
    /// `x-widget: "code"` 代替——那只是「前端用代码编辑器渲染」的 UI 提示，
    /// `harness.prompt` 与 `email.body` 同样是 `x-widget: code`，但它们**要**被展开
    /// （用户在提示词里写 `${input.x}`）。`x-opaque` 是「这段内容是 flow 自己
    /// 的语法，不参与展开」的显式声明。
    ///
    /// 词汇表的默认行为（全展开）对纯参数类型正确，所以新增类型通常无需声明。
    pub fn opaque_params(self) -> Vec<&'static str> {
        self.descriptor()
            .pointer("/params_schema/properties")
            .and_then(Value::as_object)
            .map(|props| {
                props
                    .iter()
                    .filter(|(_, schema)| schema.get("x-opaque") == Some(&Value::Bool(true)))
                    .map(|(key, _)| key.as_str())
                    .collect()
            })
            .unwrap_or_default()
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
                .clamp(1, u32::MAX as u64) as u32,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Definition {
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub edges: Vec<Edge>,
    /// 邻接索引，一次构建。**必须 skip**：Definition 是落库的版本快照，
    /// `flow_store` 还按它的序列化字节算 checksum 决定是否复用版本号
    /// （store/src/lib.rs）。让索引进入序列化就等于给每个 definition 的
    /// checksum 掺入缓存内容，版本复用契约当场失效。`skip` 保持字节不变。
    ///
    /// 私有字段顺带强制了「只能经 `serde_json::from_value` 构造」——全仓库
    /// 确实没有任何调用方用结构体字面量构造 `Definition`。
    #[serde(skip)]
    adj: OnceLock<DefinitionIndex>,
}

/// 邻接索引：按 id 预分组入边/出边 + 节点直查。
///
/// 存在理由：`incoming()` / `outgoing()` 若每次全扫 `edges` 并**分配一个
/// Vec**，则 `check_acyclic_and_reachable`（为算入度给每个节点分配 Vec 只为取
/// `.len()`）、`Driver::plan` 与 `prepare_inputs`（一个 run 的生命周期里对每个
/// 节点各调一次）都是 O(N·E) + 每调用一次分配。
///
/// **收益从约 200 节点起才为正**（release 实测 `validate` 一次，索引 vs 全扫
/// incoming 的比值）：10 节点 0.1x（索引更慢，要先建表）、50 节点 0.4x、
/// 200 节点 0.8x、1000 节点 4.9x、2000 节点 6.7x。小图上索引是净开销——
/// 但小图本身就只花几十微秒，付得起。`examples/` 里最大的定义是 5 节点。
#[derive(Debug, Clone, Default)]
struct DefinitionIndex {
    by_id: HashMap<String, usize>,
    incoming: HashMap<String, Vec<usize>>,
    outgoing: HashMap<String, Vec<usize>>,
}

/// 相等性只看图本身：邻接索引是纯缓存（且被 `serde(skip)` 排除），
/// 「两个定义相等」= 它们的序列化字节相等——这正是版本复用按 checksum 判定的
/// 那条契约。
impl PartialEq for Definition {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes && self.edges == other.edges
    }
}

impl Definition {
    /// 一次性构建的邻接索引。
    ///
    /// **契约：`nodes` / `edges` 在构造后不可变。** 全仓库唯一的构造入口是
    /// `serde_json::from_value`（`nodes`/`edges` 是 `pub`，但没有任何调用方写过
    /// 它们——全仓 `\.nodes\s*=` / `\.edges\s*=` 零命中），所以这里不做失效判断：
    /// 那是为不存在的场景写的机制。要改定义，走新的反序列化，拿到的是全新
    /// `Definition`。若日后有人开始写这两个字段，`OnceLock` 会静默返回**过期**
    /// 的索引——那时必须换成每次重建或改字段可见性。
    fn index(&self) -> &DefinitionIndex {
        self.adj.get_or_init(|| {
            let mut by_id = HashMap::with_capacity(self.nodes.len());
            for (i, node) in self.nodes.iter().enumerate() {
                by_id.entry(node.id.clone()).or_insert(i);
            }
            let mut incoming: HashMap<String, Vec<usize>> = HashMap::new();
            let mut outgoing: HashMap<String, Vec<usize>> = HashMap::new();
            for (i, edge) in self.edges.iter().enumerate() {
                incoming.entry(edge.to.clone()).or_default().push(i);
                outgoing.entry(edge.from.clone()).or_default().push(i);
            }
            DefinitionIndex {
                by_id,
                incoming,
                outgoing,
            }
        })
    }

    pub fn node(&self, id: &str) -> Option<&Node> {
        self.index().by_id.get(id).map(|i| &self.nodes[*i])
    }

    pub fn node_type(&self, id: &str) -> Option<NodeType> {
        self.node(id).and_then(Node::kind)
    }

    pub fn incoming(&self, id: &str) -> Vec<&Edge> {
        match self.index().incoming.get(id) {
            Some(edges) => edges.iter().map(|i| &self.edges[*i]).collect(),
            None => Vec::new(),
        }
    }

    pub fn outgoing(&self, id: &str) -> Vec<&Edge> {
        match self.index().outgoing.get(id) {
            Some(edges) => edges.iter().map(|i| &self.edges[*i]).collect(),
            None => Vec::new(),
        }
    }

    /// 入边条数：不必物化 `Vec<&Edge>`（校验入度、判定 start/end 走这条）。
    pub fn incoming_count(&self, id: &str) -> usize {
        self.index().incoming.get(id).map_or(0, Vec::len)
    }

    pub fn outgoing_count(&self, id: &str) -> usize {
        self.index().outgoing.get(id).map_or(0, Vec::len)
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
        // condition 出边的目标集合：同一目标不得被两个端口同时指向（见下方规则）
        let mut condition_targets: HashMap<&str, HashSet<&str>> = HashMap::new();
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
                NodeType::Condition => {
                    match edge.port.as_deref() {
                        Some("true") | Some("false") => {}
                        other => {
                            return Err(format!(
                                "condition 节点 {} 的出边端口必须是 true/false，当前为 {:?}",
                                edge.from, other
                            ))
                        }
                    }
                    // 同一 condition 的多个端口不得指向同一节点：AND-join 语义
                    // （§6.2）下，未被选中的那条出边判为 Unsatisfied，会把**整个**
                    // 目标节点跳过——于是「走 true 分支」反而把 true 分支的下游
                    // 跳掉，run 仍记成功。这是错误建模，不是分支合流：真要合流
                    // 应指向不同节点，或引入显式 join 策略。建图期拒掉，别让运行期
                    // 静默跳过。
                    if !condition_targets
                        .entry(edge.from.as_str())
                        .or_default()
                        .insert(edge.to.as_str())
                    {
                        return Err(format!(
                            "condition 节点 {} 的多个端口不能指向同一节点 {}\
                             （未被选中的端口会让该节点被跳过）",
                            edge.from, edge.to
                        ));
                    }
                }
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
                    if self.incoming_count(&node.id) > 0 {
                        return Err(format!("start 节点 {} 不能有入边", node.id));
                    }
                }
                NodeType::End => {
                    if self.outgoing_count(&node.id) > 0 {
                        return Err(format!("end 节点 {} 不能有出边", node.id));
                    }
                }
                _ => {
                    if self.incoming_count(&node.id) == 0 {
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
            .map(|n| (n.id.as_str(), self.incoming_count(&n.id)))
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

/// 按类型校验节点参数。
///
/// **必填清单从 [`NodeType::required_params`] 读**（即 descriptor 的
/// `params_schema.required`），不在这里重写一份：前端表单与后端校验共用同一份
/// 「哪些参数必填」，两处手写时改一处漏另一处的症状是「前端把可选参数渲染成
/// 必填框」或「后端拒掉前端允许留空的参数」。
///
/// 下面 match 只放**无法表达在 `required` 里的规则**：枚举白名单、类型约束、
/// 「`${}` 模板放行到执行期判定」。纯参数类型（start/end/human_task）没有
/// 额外规则，`required` 为空即通过——新增这类类型不用碰这个函数。
///
/// `pub`：整图校验（`validate`）与节点模板的**逐节点**校验共用这一份——
/// 模板片段不是完整 Definition，不能跑整图规则，但参数规则必须同源。
pub fn validate_params(node: &Node, kind: NodeType) -> Result<(), String> {
    for key in kind.required_params() {
        // `${}` 模板放行到执行期判定（DESIGN §5 规则 1）：展开后才知道是不是
        // 整数/非空串，建图期一律认它「填了」。先判模板再判类型，顺序反了会把
        // `ms: "${input.tick}"` 这类合法定义误拒。
        if node.param_str(key).is_some_and(|v| v.contains("${")) {
            continue;
        }
        // 按 schema 声明的类型判「必填」，不能一律当字符串查：`delay.ms` 声明为
        // integer，值是数字，`param_str` 恒为 None——用字符串判会让所有 delay
        // 节点在建图期被误拒。
        let schema_type = kind
            .descriptor()
            .pointer(&format!("/params_schema/properties/{key}/type"))
            .and_then(Value::as_str)
            .unwrap_or("string");
        let missing = match schema_type {
            "integer" | "number" => node.params.get(key).and_then(Value::as_u64).is_none(),
            "boolean" => node.params.get(key).and_then(Value::as_bool).is_none(),
            // 字符串必填：空串与纯空白都算「没填」
            _ => node.param_str(key).is_none_or(|v| v.trim().is_empty()),
        };
        if missing {
            return Err(format!(
                "节点 {}（{}）缺少参数 {}",
                node.id,
                kind.as_str(),
                key
            ));
        }
    }
    // `required` 只表达「必须有非空字符串」；下面按类型补其余约束。
    match kind {
        NodeType::HttpCall => match node.param_str("method") {
            Some(method) if method.trim().is_empty() => {
                Err(format!("节点 {} 的 method 不能为空", node.id))
            }
            // ${} 模板 method 执行期展开后才能判定，放行；执行层按同一份
            // HTTP_METHODS 再验一次（exec::run_http），两层共用一个词汇表
            Some(method) if method.contains("${") => Ok(()),
            Some(method) if !HTTP_METHODS.contains(&method.trim().to_uppercase().as_str()) => {
                Err(format!(
                    "节点 {} 的 method 非法：{method:?}（允许 {}）",
                    node.id,
                    HTTP_METHODS.join("/")
                ))
            }
            _ => Ok(()),
        },
        NodeType::Delay => match node.param_u64("ms") {
            Some(_) => Ok(()),
            // ${} 模板要在执行期展开后才可判定：放行，运行期 parse
            // （裸数字字符串仍拒绝——一种参数一种形态，不留灰色地带）
            None if node.param_str("ms").is_some_and(|s| s.contains("${")) => Ok(()),
            None => Err(format!(
                "节点 {}（delay）的 ms 必须是整数或 ${{}} 模板",
                node.id
            )),
        },
        NodeType::SubWorkflow => {
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
        _ => Ok(()),
    }
}

/// 图校验 / 重试策略 / 词汇表的表驱动单测。
///
/// `validate` 是「建图即校验」的判官（DESIGN.md §4）。黑盒覆盖要起进程才知道
/// 一句「工作流存在环」，而规则本身是纯函数——在这按表驱动逐条钉住。
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

    /// condition 的多个端口不得指向同一节点。
    ///
    /// 这个形状曾被 validate 放行，运行期后果是**静默数据丢失**：condition 求值
    /// 为真、走了 true 分支，但目标节点因 false 那条出边判为 Unsatisfied 而被
    /// 整个跳过（§6.2 的 AND-join 规则），run 仍记 `succeeded`、输出为 null。
    /// 建图期拒掉。互斥分支指向**不同**节点后合流是另一回事，见
    /// `and_join_skips_when_one_branch_skipped`。
    #[test]
    fn condition_ports_must_target_distinct_nodes() {
        let dup = def(
            vec![node("s", "start"), node("c", "condition"), node("e", "end")],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"e","port":"true"}),
                json!({"from":"c","to":"e","port":"false"}),
            ],
        );
        let err = dup.validate().unwrap_err();
        assert!(
            err.contains("多个端口不能指向同一节点"),
            "condition 两端口指同一节点应在建图期被拒：{err}"
        );

        // 端口指向不同节点是合法形状（普通 if/else）
        let distinct = def(
            vec![
                node("s", "start"),
                node("c", "condition"),
                node("t", "script"),
                node("f", "script"),
                node("e", "end"),
            ],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"t","port":"true"}),
                json!({"from":"c","to":"f","port":"false"}),
                json!({"from":"t","to":"e"}),
            ],
        );
        assert!(distinct.validate().is_ok(), "{:?}", distinct.validate());

        // 同一对 (from,to) 重复边仍由「重复的边」规则先拦下（port 不同故不算重复）
        let same_target_same_port = def(
            vec![node("s", "start"), node("c", "condition"), node("e", "end")],
            vec![
                json!({"from":"s","to":"c"}),
                json!({"from":"c","to":"e","port":"true"}),
                json!({"from":"c","to":"e","port":"true"}),
            ],
        );
        assert!(
            same_target_same_port
                .validate()
                .unwrap_err()
                .contains("重复的边"),
            "同端口重复边仍归「重复的边」规则"
        );
    }

    #[test]
    fn retry_policy_defaults_and_clamps() {
        assert_eq!(
            RetryPolicy::from_params(&json!({"retry":{"max_attempts":1u64 << 40}})).max_attempts,
            u32::MAX
        );

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
            "harness",
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

        // http_call / harness / email 有外部副作用（崩溃后不可安全重放）
        for kind in [NodeType::HttpCall, NodeType::Harness, NodeType::Email] {
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

    /// x-secret 参数就是 email 的 `api_key`，且**只在**这个类型上。
    ///
    /// 清单已从 descriptor 派生（`x-secret: true` 扫描），所以这里断言的是
    /// 「哪些类型该有密钥」这个业务事实，而不是「派生是否一致」——后者已由
    /// 派生本身保证，再写一遍只是同义反复。
    #[test]
    fn secret_params_are_the_expected_keys() {
        assert_eq!(NodeType::Email.secret_params(), vec!["api_key"]);
        for kind in NodeType::ALL {
            let expected = matches!(kind, NodeType::Email);
            assert_eq!(
                !kind.secret_params().is_empty(),
                expected,
                "{} 的密钥参数清单与预期不符",
                kind.as_str()
            );
        }
    }

    /// `required` 与 `validate_params` 共用一份：建图期拒掉的参数，必须正好是
    /// descriptor 声明为 required 且节点没填的那些。
    ///
    /// 必填清单已从 descriptor 派生，所以「一致」不需要测；这里测的是**派生之后
    /// 校验规则仍成立**：逐个把 required 参数挖空，都必须被拒。
    ///
    /// 校验在第一个缺失处就返回（不逐条报全），所以断言的是「被拒 + 报错点名了
    /// 某个 required 参数」，而不是恰好是当前挖空的那个。
    #[test]
    fn required_params_are_actually_enforced_by_validate() {
        for kind in NodeType::ALL {
            if matches!(kind, NodeType::Start | NodeType::End) {
                continue;
            }
            for key in kind.required_params() {
                // 只带 start / end 与本节点（params 为空）：所有 required 都缺
                let bare = def(
                    vec![
                        node("s", "start"),
                        json!({"id":"n","type":kind.as_str(),"params":{}}),
                        node("e", "end"),
                    ],
                    vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
                );
                let err = bare
                    .validate()
                    .expect_err(&format!("{}({key}) 缺失时必须被拒", kind.as_str()));
                assert!(
                    kind.required_params().iter().any(|k| err.contains(k)),
                    "{}({key}) 缺失时的报错应点名某个 required 参数，实际：{err}",
                    kind.as_str()
                );
            }
        }
    }

    /// `opaque_params`（不参与 `${}` 展开的代码字段）只有 script.code 与
    /// condition.expr 两个——harness.prompt / email.body 虽是 `x-widget: code`，
    /// 但**要**被展开，所以不能带 `x-opaque`。
    #[test]
    fn opaque_params_are_exactly_the_js_bearing_fields() {
        assert_eq!(NodeType::Script.opaque_params(), vec!["code"]);
        assert_eq!(NodeType::Condition.opaque_params(), vec!["expr"]);
        for kind in NodeType::ALL {
            let is_js_host = matches!(kind, NodeType::Script | NodeType::Condition);
            assert_eq!(
                !kind.opaque_params().is_empty(),
                is_js_host,
                "{} 的 opaque 字段清单与预期不符：{:?}",
                kind.as_str(),
                kind.opaque_params()
            );
        }
        // 带 x-widget: code 但要展开的两个字段，不得被误标为 opaque
        for (kind, key) in [(NodeType::Harness, "prompt"), (NodeType::Email, "body")] {
            assert!(
                !kind.opaque_params().contains(&key),
                "{kind:?}.{key} 是用户数据（要展开），不能标 x-opaque"
            );
        }
    }

    /// `NODE_TYPE_TABLE` 是变体清单的唯一来源：与 enum 双向覆盖，且字符串唯一。
    ///
    /// `ALL` 的长度与 enum 变体数都由类型标注编译期保证；这条守的是**内容**——
    /// 表里漏一个变体会让 `as_str` 对它 panic，重复会让 `parse` 少认一个类型。
    #[test]
    fn node_type_table_is_exhaustive() {
        // 表 → ALL：每项都在，且顺序一致（descriptor 用下标取值，顺序错了会串）
        let from_table: Vec<NodeType> = NODE_TYPE_TABLE.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            from_table,
            NodeType::ALL.to_vec(),
            "表与 ALL 的内容或顺序不一致"
        );
        // 字符串唯一：重复会让 parse 静默丢掉后面的类型
        let mut names: Vec<&str> = NODE_TYPE_TABLE.iter().map(|(_, n)| *n).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), NODE_TYPE_TABLE.len(), "表里有重复的类型名");
        // ALL → 表：每个变体都能查到自己的名字（as_str 的 expect 不该触发）
        for kind in NodeType::ALL {
            let name = kind.as_str();
            assert_eq!(NodeType::parse(name), Some(kind), "{kind:?} 的往返断了");
            // descriptor 的 type 字段由表注入，必须等于 as_str
            assert_eq!(
                kind.descriptor()["type"].as_str(),
                Some(name),
                "{} 的 descriptor.type 与 as_str 不一致",
                kind.as_str()
            );
        }
        assert_eq!(NodeType::parse("nope"), None);
    }

    /// harness / email 的必填参数校验。
    #[test]
    fn harness_and_email_require_their_params() {
        let harness = def(
            vec![
                node("s", "start"),
                json!({"id": "n", "type": "harness", "params": {"command": "kimi"}}),
                node("e", "end"),
            ],
            vec![json!({"from":"s","to":"n"}), json!({"from":"n","to":"e"})],
        );
        let err = harness.validate().unwrap_err();
        assert!(err.contains("prompt"), "{err}");

        let ok = def(
            vec![
                node("s", "start"),
                json!({"id": "n", "type": "harness", "params": {"command": "kimi", "prompt": "p"}}),
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

    /// 邻接索引是纯缓存：**绝不进入序列化**。
    ///
    /// `flow_store::definition_checksum` 按 `serde_json::to_vec(definition)`
    /// 算 checksum 决定「相同定义复用版本号」。索引一旦进了序列化，每个
    /// definition 的 checksum 都会掺入缓存内容，版本复用契约当场失效。
    #[test]
    fn adjacency_index_never_enters_serialization() {
        let d = linear();
        // 先访问一次把索引建出来（有缓存的实例 vs 干净实例）
        assert_eq!(d.incoming_count("n"), 1);
        assert_eq!(d.outgoing_count("s"), 1);
        let after_cache = serde_json::to_string(&d).unwrap();

        let fresh = linear(); // 全新反序列化，索引未建
        let before_cache = serde_json::to_string(&fresh).unwrap();
        assert_eq!(
            before_cache, after_cache,
            "建索引前后序列化字节必须完全一致"
        );
        // 反序列化回来仍然相等（PartialEq 手动实现，不含缓存）
        assert_eq!(fresh, d);
    }

    /// 索引访问器与全扫描实现必须给出同一答案（邻接分组最容易写错的形状：
    /// 入/出边搞反、重复边、指向不存在节点的边）。
    #[test]
    fn indexed_accessors_match_linear_scan() {
        let d = def(
            vec![
                node("s", "start"),
                node("a", "script"),
                node("b", "script"),
                node("c", "condition"),
                node("e", "end"),
            ],
            vec![
                json!({"from": "s", "to": "a"}),
                json!({"from": "a", "to": "b"}),
                json!({"from": "b", "to": "c"}),
                json!({"from": "c", "to": "e", "port": "true"}),
                json!({"from": "c", "to": "a", "port": "false"}),
            ],
        );
        for id in ["s", "a", "b", "c", "e", "nope"] {
            let linear_in: Vec<_> = d.edges.iter().filter(|e| e.to == id).collect();
            let linear_out: Vec<_> = d.edges.iter().filter(|e| e.from == id).collect();
            assert_eq!(d.incoming(id), linear_in, "incoming({id}) 不一致");
            assert_eq!(d.outgoing(id), linear_out, "outgoing({id}) 不一致");
            assert_eq!(d.incoming_count(id), linear_in.len());
            assert_eq!(d.outgoing_count(id), linear_out.len());
            assert_eq!(d.node(id).is_some(), d.nodes.iter().any(|n| n.id == id));
        }
        // 重复边也要两条都在（去重是 validate 的职责，不是索引的）
        let dup = def(
            vec![node("s", "start"), node("a", "script"), node("e", "end")],
            vec![
                json!({"from": "s", "to": "a"}),
                json!({"from": "s", "to": "a"}),
                json!({"from": "a", "to": "e"}),
            ],
        );
        assert_eq!(dup.incoming_count("a"), 2, "重复边不应被索引悄悄去重");
    }

    /// 防回退护栏：`validate` 不得退回 O(N·E) 全扫描。1000 节点 / 2000 边
    /// 的宽图，全扫描版本要跑百万次边比较；带索引的版本是线性的。
    #[test]
    fn validate_scales_linearly_on_a_wide_graph() {
        const N: usize = 2000; // script 节点数，另有 start 与 end
        let mut nodes: Vec<Value> = vec![json!({"id": "s", "type": "start"})];
        for i in 0..N {
            nodes.push(json!({"id": format!("n{i}"), "type": "script",
                             "params": {"code": "return 1;"}}));
        }
        nodes.push(json!({"id": "e", "type": "end"}));

        let mut edges: Vec<Value> = vec![json!({"from": "s", "to": "n0"})];
        for i in 0..N {
            if i + 1 < N {
                // 链式（保证可达）+ 短程扇出（保证入度/出度都非平凡），
                // 边一律从小 id 指向大 id → 无环
                edges.push(json!({"from": format!("n{i}"), "to": format!("n{}", i + 1)}));
                for j in 2..=3 {
                    if i + j < N {
                        edges.push(json!({"from": format!("n{i}"), "to": format!("n{}", i + j)}));
                    }
                }
            } else {
                edges.push(json!({"from": format!("n{i}"), "to": "e"}));
            }
        }

        let d = def(nodes, edges);
        assert!(d.validate().is_ok(), "宽图应当是合法 DAG");
        let started = std::time::Instant::now();
        assert!(d.validate().is_ok());
        let elapsed = started.elapsed();
        // release 实测：N=2000 时带索引约 2.3ms、全扫 incoming 约 15.3ms；
        // debug 下两者都慢一个量级。阈值取 250ms：既能真的红（去掉索引后
        // debug 宽图远超它），又不随机器波动误报。守的是「索引没被绕过」，
        // 不是绝对性能。
        assert!(
            elapsed.as_millis() < 250,
            "validate 在 {N} 节点宽图上耗时 {elapsed:?}——邻接索引被绕过了？"
        );
    }
}
