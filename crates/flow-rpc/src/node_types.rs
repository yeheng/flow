use flow_domain::NodeType;
use serde_json::{json, Value};
use std::sync::LazyLock;

/// Add product labels and form hints to the domain-owned node schema.
pub(crate) fn descriptor(kind: NodeType) -> &'static Value {
    static DESCRIPTORS: LazyLock<[Value; NodeType::ALL.len()]> =
        LazyLock::new(|| NodeType::ALL.map(decorate));
    &DESCRIPTORS[kind as usize]
}

fn decorate(kind: NodeType) -> Value {
    let (label, category, params) = presentation(kind);
    let mut descriptor = kind.descriptor().clone();
    descriptor["label"] = json!(label);
    descriptor["category"] = json!(category);
    for port in descriptor["ports"].as_array_mut().unwrap() {
        let id = port["id"].as_str().unwrap();
        port["label"] = json!(match id {
            "in" => "入",
            "out" => "出",
            "true" => "真",
            "false" => "假",
            _ => id,
        });
    }
    let properties = descriptor["params_schema"]["properties"]
        .as_object_mut()
        .unwrap();
    for (key, hints) in params.as_object().unwrap() {
        properties
            .get_mut(key)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .extend(hints.as_object().unwrap().clone());
    }
    if let Some(join) = properties.get_mut("join") {
        join["x-label"] = json!("合流规则");
        join["x-help"] = json!("selected 等全部入边确定后执行已选路径；all 要求每个前驱成功。省略时沿用流程版本的默认规则");
    }
    if let Some(effect) = properties.get_mut("effect_policy") {
        effect["x-label"] = json!("未知结果恢复");
        effect["x-help"] = json!("manual 等人工核对；repeatable 仅用于重复调用仍安全且无时效限制的接口，并受重试次数限制");
    }
    descriptor
}

fn presentation(kind: NodeType) -> (&'static str, &'static str, Value) {
    match kind {
        NodeType::Start => ("开始", "control", json!({})),
        NodeType::End => ("结束", "control", json!({})),
        NodeType::Script => (
            "脚本",
            "compute",
            json!({
                "code": {"x-help": "可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果", "x-label": "JS 函数体", "x-widget": "code"},
                "timeout_ms": {"x-label": "脚本超时（毫秒）"},
            }),
        ),
        NodeType::Condition => (
            "条件分支",
            "control",
            json!({
                "expr": {"x-help": "表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes", "x-label": "条件表达式", "x-widget": "code"},
                "timeout_ms": {"x-label": "求值超时（毫秒）"},
            }),
        ),
        NodeType::Delay => (
            "等待",
            "control",
            json!({
                "ms": {"x-help": "数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）", "x-label": "时长（毫秒）"},
            }),
        ),
        NodeType::HttpCall => (
            "HTTP 请求",
            "integration",
            json!({
                "body": {"x-label": "请求体", "x-widget": "json"},
                "headers": {"x-label": "请求头", "x-widget": "key-value"},
                "method": {"x-label": "方法"},
                "proxy": {"x-help": "http(s)://[user:pass@]host:port，留空直连", "x-label": "代理"},
                "timeout_ms": {"x-label": "HTTP 超时（毫秒）"},
                "url": {"x-help": "支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）", "x-label": "URL"},
            }),
        ),
        NodeType::HumanTask => (
            "人工节点",
            "human",
            json!({
                "prompt": {"x-label": "提示"},
            }),
        ),
        NodeType::Input => (
            "输入",
            "human",
            json!({
                "fields": {"x-help": "JSON 数组，每项 {\"key\":\"token\",\"label\":\"显示名\",\"default\":…}（label/default 可省）。run 输入已覆盖全部字段（默认值可兜底）时直接放行，输出这些字段的值；否则挂起等用户填写（run.signal 交付的对象成为本节点输出）", "x-label": "输入字段", "x-widget": "json"},
            }),
        ),
        NodeType::SubWorkflow => (
            "子工作流",
            "control",
            json!({
                "input_mapping": {"x-help": "JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入", "x-label": "子 run 输入映射", "x-widget": "json"},
                "version": {"x-help": "省略时在运行启动时固定最新已发布版本", "x-label": "子流程版本"},
                "workflow_id": {"x-help": "v2 在运行启动时固定已发布子流程；输出透传，失败向上传导", "x-label": "目标工作流", "x-widget": "workflow-picker"},
            }),
        ),
        NodeType::Harness => (
            "Harness 代理",
            "ai",
            json!({
                "api_key": {"x-help": "仅 mode=agent：只存密钥名称，真值来自密钥管理（secrets.list 提供候选）", "x-label": "API 密钥名称"},
                "args": {"x-help": "仅 mode=command 可用", "x-label": "额外参数", "x-widget": "json"},
                "command": {"x-help": "mode=command 时必填；进程级记录：prompt 经 stdin，最终 stdout 作为输出；逐模型/工具记录使用模型调用和 HTTP 节点", "x-label": "命令"},
                "mode": {"x-help": "command 运行外部 CLI（需 command 参数）；agent 使用内置 flow-harness-agent（需 api_key，workdir 可留空，模型配置见设置页 [harness.agent]）", "x-label": "模式"},
                "prompt": {"x-label": "提示词", "x-widget": "code"},
                "timeout_ms": {"x-label": "超时 (ms)"},
                "workdir": {"x-help": "仅 mode=agent；可选。缺省 = 每次 run 的临时沙箱（结束后删除）；配置后为 agent 工作根（绝对路径且须存在，agent 可读写其中文件）", "x-label": "工作目录"},
            }),
        ),
        NodeType::Email => (
            "邮件",
            "notify",
            json!({
                "api_key": {"x-help": "只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list", "x-label": "API 密钥名称"},
                "body": {"x-help": "纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板", "x-label": "正文", "x-widget": "code"},
                "endpoint": {"x-help": "Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证", "x-label": "API 端点"},
                "from": {"x-label": "发件人"},
                "subject": {"x-help": "支持 ${input.x} / ${nodes.n.y} 模板", "x-label": "主题"},
                "to": {"x-label": "收件人"},
            }),
        ),
        NodeType::Llm => (
            "模型调用",
            "ai",
            json!({
                "body": {"x-label": "模型请求", "x-widget": "json"},
                "credential": {"x-help": "{scheme: bearer, secret_ref: 密钥名称}", "x-label": "凭据引用", "x-widget": "json"},
                "headers": {"x-label": "请求头", "x-widget": "key-value"},
                "response_schema": {"x-label": "响应约束", "x-widget": "json"},
                "timeout_ms": {"x-label": "超时（毫秒）"},
                "url": {"x-label": "模型 API 地址"},
            }),
        ),
    }
}
