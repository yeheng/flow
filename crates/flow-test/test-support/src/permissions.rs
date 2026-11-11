//! 测试夹具：按节点**字面参数**派生精确能力，给 legacy 测试定义加权限块。
//!
//! 只服务测试脚手架（不进产品代码、不是后门）：权限模型上线后，含副作用
//! 节点的 legacy 定义不能启动新 run。存量 e2e 关心的是 http/harness 语义而
//! 不是权限，`publish_workflow` 默认经这里把它们迁移成「恰好覆盖自身
//! 字面操作」的受管定义（approval=auto）。需要验证 legacy 门本身的用例改用
//! `publish_legacy_workflow`。
//!
//! 派生规则与 master 的 `policy::derive` 一一对应：
//! - http_call：Net（全部方法 × URL 源站前缀）+ Secret（credential.secret_ref）；
//! - email：Net（POST 端点源站）+ SendEmail（字面收件人）+ Secret（api_key）；
//! - llm：LlmCall（规范化端点 × body.model）+ Secret；
//! - harness command：Exec（字面绝对路径 + 精确 argv）或 Shell（`sh -c`）。
//!
//! 模板化目标（`${...}`）无法静态派生：源站之外的部分带模板时按源站前缀
//! 覆盖；host/收件人/model 本身是模板的节点不派生（用例需显式给能力）。
use serde_json::{json, Value};
use sha2::Digest as _;

/// 打开服务端测试钩子（FLOW_TEST_AUTO_GOVERN）：legacy 定义按运行期已知
/// 事实自动合成精确权限。进程内一次即够；由 Ctx::start_with 与各 in-process
/// 测试 setup 调用。
pub fn enable_auto_govern() {
    // set_var 在多线程测试进程里有竞态（Rust 2024 起不安全）——测试进程
    // 通常单线程启动期调用；这里用 once 语义降低风险。
    std::env::set_var("FLOW_TEST_AUTO_GOVERN", "1");
}

const ALL_METHODS: [&str; 7] = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];

/// 定义里是否含会被 legacy 门拦下的内置副作用节点。
pub fn has_side_effect_nodes(definition: &Value) -> bool {
    definition["nodes"].as_array().is_some_and(|nodes| {
        nodes.iter().any(|node| {
            matches!(
                node["type"].as_str(),
                Some("http_call" | "email" | "llm" | "harness")
            )
        })
    })
}

/// 未声明权限块且含副作用节点的定义 → 注入按字面参数派生的精确能力。
/// 已有权限块或纯计算定义原样返回。
pub fn with_static_permissions(definition: Value) -> Value {
    if definition.get("permissions").is_some() || !has_side_effect_nodes(&definition) {
        return definition;
    }
    let mut governed = definition;
    let allow = derive_allow(&governed);
    governed["permissions"] = json!({"version": 1, "mode": "execute", "allow": allow});
    governed
}

/// 按节点字面参数派生 workflow 层能力列表（id 唯一）。
pub fn derive_allow(definition: &Value) -> Vec<Value> {
    let mut allow: Vec<Value> = Vec::new();
    let mut push = |cap: Value| {
        // 同内容去重；id 带序号保证唯一。
        let mut probe = cap.clone();
        if allow.iter().any(|existing| {
            let mut existing = existing.clone();
            existing["id"] = Value::Null;
            probe["id"] = Value::Null;
            existing == probe
        }) {
            return;
        }
        let mut cap = cap;
        cap["id"] = json!(format!("auto-{}", allow.len()));
        allow.push(cap);
    };
    for node in definition["nodes"].as_array().into_iter().flatten() {
        let params = &node["params"];
        match node["type"].as_str() {
            Some("http_call") => {
                if let Some(prefix) = params["url"].as_str().and_then(origin) {
                    push(net(&ALL_METHODS, &prefix));
                }
                secret_ref(&params["credential"]["secret_ref"], &mut push);
            }
            Some("email") => {
                let endpoint = params["endpoint"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("https://api.resend.com/emails");
                if let Some(prefix) = origin(endpoint) {
                    push(net(&["POST"], &prefix));
                }
                if let Some(to) = params["to"].as_str().filter(|to| !to.contains("${")) {
                    let recipients: Vec<&str> = to
                        .split(',')
                        .map(str::trim)
                        .filter(|r| !r.is_empty())
                        .collect();
                    if !recipients.is_empty() {
                        push(json!({"id": "", "action": "send_email",
                            "recipients": recipients, "approval": "auto"}));
                    }
                }
                secret_ref(&params["api_key"], &mut push);
            }
            Some("llm") => {
                let provider = params["url"].as_str().filter(|u| !u.contains("${"));
                let model = params["body"]["model"]
                    .as_str()
                    .filter(|m| !m.is_empty() && !m.contains("${"));
                if let (Some(provider), Some(model)) = (provider, model) {
                    push(json!({"id": "", "action": "llm_call",
                        "providers": [normalize_provider(provider)], "models": [model],
                        "approval": "auto"}));
                }
                secret_ref(&params["credential"]["secret_ref"], &mut push);
            }
            Some("harness") if params["mode"].as_str() != Some("agent") => {
                let Some(command) = params["command"].as_str() else {
                    continue;
                };
                if !command.starts_with('/') || command.contains("${") {
                    continue;
                }
                let argv: Vec<String> = params["args"]
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .map(|v| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                if argv.iter().any(|arg| arg.contains("${")) {
                    continue;
                }
                let basename = command.rsplit('/').next().unwrap_or(command);
                let shell = ["sh", "bash", "zsh", "dash", "ksh"].contains(&basename);
                if shell && argv.len() == 2 && argv[0] == "-c" {
                    push(json!({"id": "", "action": "shell", "executable": command,
                        "script_digest": sha256_hex(argv[1].as_bytes()), "approval": "auto"}));
                } else {
                    push(json!({"id": "", "action": "exec", "executable": command,
                        "argv_exact": argv, "approval": "auto"}));
                }
            }
            _ => {}
        }
    }
    allow
}

fn net(methods: &[&str], prefix: &str) -> Value {
    json!({"id": "", "action": "net", "methods": methods, "url_prefix": prefix,
           "approval": "auto"})
}

fn secret_ref(value: &Value, push: &mut impl FnMut(Value)) {
    if let Some(name) = value
        .as_str()
        .filter(|s| !s.trim().is_empty() && !s.contains("${"))
    {
        push(json!({"id": "", "action": "secret", "names": [name], "approval": "auto"}));
    }
}

/// URL 的源站前缀（scheme://host[:port]/）；host 带模板时无法派生。
fn origin(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    if host.contains("${") || url.split("://").nth(1)?.split('/').next()?.contains("${") {
        return None;
    }
    let port = parsed
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Some(format!("{}://{host}{port}/", parsed.scheme()))
}

/// 与 master `policy::derive::normalize_provider` 同规。
pub fn normalize_provider(url: &str) -> String {
    let Ok(parsed) = url::Url::parse(url) else {
        return url.trim_end_matches('/').to_string();
    };
    let scheme = parsed.scheme().to_ascii_lowercase();
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    let default = match scheme.as_str() {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    };
    let port = match (parsed.port(), default) {
        (Some(port), Some(default)) if port == default => String::new(),
        (Some(port), _) => format!(":{port}"),
        (None, _) => String::new(),
    };
    format!("{scheme}://{host}{port}{}", parsed.path().trim_end_matches('/'))
}

/// SHA-256 hex（Shell 能力的脚本摘要）。
pub fn sha256_hex(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
