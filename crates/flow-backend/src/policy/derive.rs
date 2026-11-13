//! 需求集合派生：从冻结的节点定义 + 操作请求 JSON 推导 [`OperationSpec`]。
//!
//! master 从**认证连接**推导请求，不信任插件自报的身份、hash 或风险级别
//! （§3.3）。派生输入只有两样：节点类型（来自冻结定义）与请求字节（已过
//! 指纹校验）。模板已在此前物化——动作中的动态目标在运行时求值后重新
//! 匹配（§3.5）。
//!
//! 比设计更严的两条（见 plans/2026-11-09-permission-model-impl.md）：
//! - email 额外要求 endpoint 的 Net(POST) 能力（防模板化端点送出密钥）；
//! - harness command 必须是规范化绝对路径，不做 PATH 解析（TOCTOU）。

use flow_dto::permission::{canonical_digest, sha256_hex, OperationSpec};
use flow_engine::{Node, NodeType};
use serde_json::Value;

/// 空环境摘要：harness 子进程继承执行器环境，不受请求控制，用固定值
/// 占位（所有请求一致，不参与区分）。
fn env_digest() -> String {
    sha256_hex(b"{}")
}

/// 把端点 URL 规范化为 provider 身份：小写 scheme/host、去默认端口、保留
/// 路径（LlmCall.provider 用）。
pub fn normalize_provider(url: &str) -> String {
    let Ok(parsed) = url::Url::parse(url) else {
        return url.trim_end_matches('/').to_string();
    };
    let scheme = parsed.scheme().to_ascii_lowercase();
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    let explicit_port = parsed.port();
    let default_port = match scheme.as_str() {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    };
    let port = match (explicit_port, default_port) {
        (Some(port), Some(default)) if port == default => String::new(),
        (Some(port), _) => format!(":{port}"),
        (None, _) => String::new(),
    };
    let path = parsed.path().trim_end_matches('/');
    format!("{scheme}://{host}{port}{path}")
}

/// 按节点类型派生一次外部操作的需求集合。
///
/// # Errors
/// 载荷形状非法（可执行文件非绝对路径、动态插件节点等）返回错误文本，
/// G3 按 Deny 处理——不能执行的载荷不该进入审批。
pub fn derive_specs(node: &Node, request: &Value) -> Result<Vec<OperationSpec>, String> {
    let kind = node
        .kind()
        .ok_or_else(|| "动态插件节点尚未接入权限模型（P1 之前执行被拒绝）".to_string())?;
    match kind {
        NodeType::HttpCall => http_specs(request),
        NodeType::Email => {
            let mut specs = http_specs(request)?;
            specs.push(email_spec(request)?);
            Ok(specs)
        }
        NodeType::Llm => {
            let mut specs = http_specs(request)?;
            specs.push(llm_spec(request)?);
            Ok(specs)
        }
        NodeType::Harness => {
            if node.is_harness_agent() {
                let mut specs = vec![agent_llm_spec(request)?];
                if let Some(secret) = secret_spec(request) {
                    specs.push(secret);
                }
                Ok(specs)
            } else {
                harness_command_specs(request)
            }
        }
        // 纯计算与控制流节点没有外部操作：能走到这里的只有上述副作用类型
        // （has_side_effect），其余类型出现在此属于平台 bug，宁可拒绝。
        _ => Err(format!("节点类型 {kind:?} 不产生外部操作，不应进入授权")),
    }
}

/// Http/Email/Llm 共用：请求本身是统一 HTTP 形态 + 可选凭据引用。
fn http_specs(request: &Value) -> Result<Vec<OperationSpec>, String> {
    let method = request["method"]
        .as_str()
        .ok_or_else(|| "请求缺少 method".to_string())?
        .to_uppercase();
    let url = request["url"]
        .as_str()
        .ok_or_else(|| "请求缺少 url".to_string())?
        .to_string();
    let headers_digest = canonical_digest(&request["headers"].clamp_object());
    let body_digest = canonical_digest(&request["body"].clamp_object());
    let mut specs = vec![OperationSpec::Http {
        method: method.clone(),
        url,
        headers_digest: headers_digest.clone(),
        body_digest: body_digest.clone(),
    }];
    if let Some(proxy) = proxy_spec(request, &method, &headers_digest, &body_digest)? {
        specs.push(proxy);
    }
    if let Some(secret) = secret_spec(request) {
        specs.push(secret);
    }
    Ok(specs)
}

/// proxy 参数 → 代理出站点的第二条 Net 需求。
///
/// 设了代理的请求有两条出站路径：目标 URL 之外，字节的实际去向是代理
/// 主机（http 目标可见全部载荷；https 目标也拿到 CONNECT 的目标主机）。
/// 只按目标 URL 授权等于允许「经 proxy 参数向任意主机外发字节」，因此
/// 代理 origin 派生为额外一条 Http 需求——需求集合内 AND、层内 OR：
/// 目标与代理各自需要 Net 能力覆盖，缺一即 Deny（09 §13.2）。
///
/// spec URL 归一化为 `scheme://host[:port]/`：剥 userinfo（代理凭据不进
/// 需求与审计；完整请求字节仍由指纹绑定）、剥 path/query（授权只需
/// 覆盖 origin）。method/摘要与目标 spec 相同——http 代理可见完整载荷，
/// https 隧道下这是更严的绑定而非更宽。scheme 限 http/https，与
/// [`validate_url_prefix`](flow_dto::permission) 的授权面一致。
fn proxy_spec(
    request: &Value,
    method: &str,
    headers_digest: &str,
    body_digest: &str,
) -> Result<Option<OperationSpec>, String> {
    let Some(proxy) = request["proxy"].as_str().filter(|s| !s.trim().is_empty()) else {
        return Ok(None);
    };
    let parsed = url::Url::parse(proxy).map_err(|e| format!("proxy URL 非法：{e}"))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(format!(
            "proxy 只支持 http/https（与 Net 授权的 url_prefix 一致）：{proxy}"
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("proxy URL 缺少主机：{proxy}"))?;
    let origin = match parsed.port() {
        Some(port) => format!("{}://{host}:{port}/", parsed.scheme()),
        None => format!("{}://{host}/", parsed.scheme()),
    };
    Ok(Some(OperationSpec::Http {
        method: method.to_string(),
        url: origin,
        headers_digest: headers_digest.to_string(),
        body_digest: body_digest.to_string(),
    }))
}

/// credential.secret_ref → Secret 需求（只以引用参与请求，真值走凭据通道）。
fn secret_spec(request: &Value) -> Option<OperationSpec> {
    request["credential"]["secret_ref"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(|name| OperationSpec::Secret {
            secret_name: name.to_string(),
        })
}

/// email：全部收件人 + 载荷摘要（To/Cc/Bcc 全部校验；当前 body 形态只有 to）。
fn email_spec(request: &Value) -> Result<OperationSpec, String> {
    let to = request["body"]["to"].as_str().unwrap_or_default();
    let recipients: Vec<String> = to
        .split(',')
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .map(str::to_string)
        .collect();
    if recipients.is_empty() {
        return Err("email 请求缺少收件人".into());
    }
    Ok(OperationSpec::SendEmail {
        recipients,
        payload_digest: canonical_digest(&request["body"].clamp_object()),
    })
}

/// llm 节点：provider = 规范化端点，model = body.model（缺失按空串，匹配
/// 要求显式列出，fail-closed）。
fn llm_spec(request: &Value) -> Result<OperationSpec, String> {
    let url = request["url"]
        .as_str()
        .ok_or_else(|| "请求缺少 url".to_string())?;
    Ok(OperationSpec::LlmCall {
        provider: normalize_provider(url),
        model: request["body"]["model"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        payload_digest: canonical_digest(&request["body"].clamp_object()),
    })
}

/// harness agent：初始 LlmCall 来自冻结的 agent 配置（base_url/model），
/// 载荷摘要绑定 prompt 全文。
fn agent_llm_spec(request: &Value) -> Result<OperationSpec, String> {
    let agent = &request["agent"];
    let base_url = agent["base_url"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "agent 请求缺少 base_url".to_string())?;
    let prompt = request["prompt"].as_str().unwrap_or_default();
    Ok(OperationSpec::LlmCall {
        provider: normalize_provider(base_url),
        model: agent["model"].as_str().unwrap_or_default().to_string(),
        payload_digest: sha256_hex(prompt.as_bytes()),
    })
}

/// harness command：规范化绝对路径 + 精确 argv（不做 PATH 解析）；
/// `<shell> -c <script>` 归为 Shell（摘要绑定脚本全文）。
fn harness_command_specs(request: &Value) -> Result<Vec<OperationSpec>, String> {
    let executable = request["command"]
        .as_str()
        .ok_or_else(|| "harness 请求缺少 command".to_string())?;
    if !executable.starts_with('/') {
        return Err(format!(
            "harness command 必须是绝对路径（当前 {executable:?}）；相对命令不受权限模型支持"
        ));
    }
    if executable
        .split('/')
        .skip(1)
        .any(|component| matches!(component, "" | "." | ".."))
    {
        return Err(format!(
            "harness command 必须规范化（无 ./..// 组件）：{executable}"
        ));
    }
    let argv: Vec<String> = request["args"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    let cwd = request["workdir"].as_str().unwrap_or_default().to_string();
    let prompt = request["prompt"].as_str().unwrap_or_default();
    let stdin_digest = (!prompt.is_empty()).then(|| sha256_hex(prompt.as_bytes()));
    let shells = ["sh", "bash", "zsh", "dash", "ksh"];
    let basename = executable.rsplit('/').next().unwrap_or(executable);
    let spec = if shells.contains(&basename) && argv.len() == 2 && argv[0] == "-c" {
        OperationSpec::Shell {
            executable: executable.to_string(),
            script_digest: sha256_hex(argv[1].as_bytes()),
            cwd,
            env_digest: env_digest(),
            stdin_digest,
        }
    } else {
        OperationSpec::Exec {
            executable: executable.to_string(),
            argv,
            cwd,
            env_digest: env_digest(),
            stdin_digest,
        }
    };
    Ok(vec![spec])
}

/// Value 的小帮手：null 视为空对象（canonical 编码需要确定的输入）。
trait ClampObject {
    fn clamp_object(&self) -> Value;
}

impl ClampObject for Value {
    fn clamp_object(&self) -> Value {
        match self {
            Value::Null => Value::Object(serde_json::Map::new()),
            other => other.clone(),
        }
    }
}

/// harness 内置工具 → 需求集合（G5 闭集映射，§8）。
///
/// 未知工具 / evolve / 后台 bash → Err（Deny：不能默认映射为已允许的能力）。
/// todo 写状态文件（FileWrite todos.json）；spawn_subagents 的子代理模型
/// 调用随子代理自身逐次裁决（父调用本身映射为只读工作区检查）。
///
/// # Errors
/// 未知工具或无法派生（命令后台化等）返回错误文本。
pub fn tool_specs(
    tool: &str,
    params: &Value,
    _cwd: Option<&str>,
) -> Result<Vec<OperationSpec>, String> {
    let path = |value: &Value| -> String {
        // "."（工作区根）→ ""（只匹配 subtree 能力；精确 path 不放行）。
        let path = value.as_str().unwrap_or(".").trim().trim_end_matches('/');
        if path == "." || path.is_empty() {
            String::new()
        } else {
            path.trim_start_matches("./").to_string()
        }
    };
    match tool {
        "read" => Ok(vec![OperationSpec::FileRead {
            path: path(&params["path"]),
        }]),
        "list" => Ok(vec![OperationSpec::FileRead {
            path: path(&params["path"]),
        }]),
        "grep" => Ok(vec![OperationSpec::FileRead {
            path: params["path"].as_str().unwrap_or(".").trim().to_string(),
        }]),
        "write" => {
            let content = params["content"].as_str().unwrap_or_default();
            Ok(vec![OperationSpec::FileWrite {
                path: path(&params["path"]),
                content_digest: flow_dto::permission::sha256_hex(content.as_bytes()),
            }])
        }
        "edit" => {
            let script = serde_json::to_string(&params["edits"]).unwrap_or_default();
            Ok(vec![OperationSpec::FileWrite {
                path: path(&params["path"]),
                content_digest: flow_dto::permission::sha256_hex(script.as_bytes()),
            }])
        }
        "bash" => {
            if params["run_in_background"].as_bool() == Some(true) {
                return Err("后台 bash 不受权限模型支持（结果不可收口）".into());
            }
            let command = params["command"]
                .as_str()
                .ok_or_else(|| "bash 缺少 command".to_string())?;
            Ok(vec![OperationSpec::Shell {
                executable: "/bin/sh".into(),
                script_digest: flow_dto::permission::sha256_hex(command.as_bytes()),
                cwd: String::new(),
                env_digest: flow_dto::permission::sha256_hex(b"{}"),
                stdin_digest: None,
            }])
        }
        "fetch" => {
            let url = params["url"]
                .as_str()
                .ok_or_else(|| "fetch 缺少 url".to_string())?;
            Ok(vec![OperationSpec::Http {
                method: "GET".into(),
                url: url.to_string(),
                headers_digest: flow_dto::permission::sha256_hex(b"{}"),
                body_digest: flow_dto::permission::sha256_hex(b"{}"),
            }])
        }
        "todo" => Ok(vec![OperationSpec::FileWrite {
            path: "todos.json".into(),
            content_digest: flow_dto::permission::sha256_hex(
                serde_json::to_string(&params)
                    .unwrap_or_default()
                    .as_bytes(),
            ),
        }]),
        "spawn_subagents" => Ok(vec![OperationSpec::FileRead {
            path: String::new(),
        }]),
        "evolve" => Err("evolve 写宿主配置目录，首版不授权".into()),
        other => Err(format!("未知工具 {other}：不默认映射为已允许的能力")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_dto::permission::Action;
    use serde_json::json;

    fn node(kind: &str, params: Value) -> Node {
        serde_json::from_value(json!({"id":"n","type":kind,"params":params})).unwrap()
    }

    #[test]
    fn http_request_derives_net_and_secret() {
        let node = node("http_call", json!({}));
        let specs = derive_specs(
            &node,
            &json!({
                "method": "post", "url": "https://Api.Example.com/v1/x",
                "headers": {"a": 1}, "body": {"q": 2},
                "credential": {"secret_ref": "KEY"}
            }),
        )
        .unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].action(), Action::Net);
        assert!(
            matches!(&specs[0], OperationSpec::Http { method, url, .. } if method=="POST" && url=="https://Api.Example.com/v1/x")
        );
        assert_eq!(specs[1].action(), Action::Secret);
        // 无凭据引用 → 无 Secret 需求。
        let specs = derive_specs(
            &node,
            &json!({"method":"GET","url":"https://a.b/","headers":{},"body":null}),
        )
        .unwrap();
        assert_eq!(specs.len(), 1);
    }

    #[test]
    fn proxy_derives_second_net_need_normalized_to_origin() {
        let node = node("http_call", json!({}));
        // 带 userinfo/path/query 的代理：spec 只取 origin，凭据与路径不进需求。
        let specs = derive_specs(
            &node,
            &json!({
                "method": "GET", "url": "https://api.example.com/v1",
                "headers": {}, "body": null,
                "proxy": "http://user:pass@proxy.local:8080/ignored?x=1"
            }),
        )
        .unwrap();
        assert_eq!(specs.len(), 2, "目标 + 代理两条 Net 需求");
        match &specs[1] {
            OperationSpec::Http { method, url, .. } => {
                assert_eq!(method, "GET");
                assert_eq!(url, "http://proxy.local:8080/");
            }
            other => panic!("proxy spec 形状错误：{other:?}"),
        }
        // 代理 origin 必须能被同前缀的能力覆盖（端到端匹配面自洽）。
        let grant = flow_dto::permission::parse_capability(&json!({
            "id": "p", "action": "net", "methods": ["GET"],
            "url_prefix": "http://proxy.local:8080/", "approval": "auto"
        }))
        .unwrap();
        assert!(grant.pattern.matches(&specs[1]));

        // 默认端口归一：无端口的 https 代理。
        let specs = derive_specs(
            &node,
            &json!({
                "method": "GET", "url": "https://a.b/", "headers": {}, "body": null,
                "proxy": "https://proxy.internal"
            }),
        )
        .unwrap();
        assert!(matches!(&specs[1], OperationSpec::Http { url, .. } if url == "https://proxy.internal/"));

        // 空串/缺失/null（llm 节点的默认形态）→ 无代理需求。
        for proxy in [json!(""), json!(null)] {
            let specs = derive_specs(
                &node,
                &json!({"method":"GET","url":"https://a.b/","headers":{},"body":null,"proxy":proxy}),
            )
            .unwrap();
            assert_eq!(specs.len(), 1, "proxy={proxy} 不产生需求");
        }

        // socks5 代理与非法 URL：fail-closed，派生失败即 Deny。
        for proxy in ["socks5://proxy.local:1080", "not a url"] {
            assert!(
                derive_specs(
                    &node,
                    &json!({"method":"GET","url":"https://a.b/","headers":{},"body":null,"proxy":proxy}),
                )
                .is_err(),
                "proxy={proxy} 应拒绝派生"
            );
        }
    }

    #[test]
    fn email_derives_send_email_plus_net_plus_secret() {
        let node = node("email", json!({}));
        let specs = derive_specs(
            &node,
            &json!({
                "method":"POST","url":"https://api.resend.com/emails",
                "headers":{},"body":{"from":"a@x.y","to":"a@x.y, b@x.y","subject":"s","text":"t"},
                "credential":{"scheme":"bearer","secret_ref":"RESEND_KEY"}
            }),
        )
        .unwrap();
        let actions: Vec<Action> = specs.iter().map(|s| s.action()).collect();
        assert!(actions.contains(&Action::SendEmail));
        assert!(actions.contains(&Action::Net));
        assert!(actions.contains(&Action::Secret));
        match specs.iter().find(|s| s.action() == Action::SendEmail) {
            Some(OperationSpec::SendEmail { recipients, .. }) => {
                assert_eq!(recipients, &vec!["a@x.y".to_string(), "b@x.y".to_string()]);
            }
            other => panic!("send_email spec missing: {other:?}"),
        }
        // 缺收件人 → 错误（Deny 路径）。
        assert!(derive_specs(
            &node,
            &json!({"method":"POST","url":"https://a.b/","headers":{},"body":{"to":""}})
        )
        .is_err());
    }

    #[test]
    fn llm_derives_provider_model_from_endpoint_and_body() {
        let node = node("llm", json!({}));
        let specs = derive_specs(
            &node,
            &json!({
                "method":"POST","url":"https://Api.openai.com:443/v1/chat/completions/",
                "headers":{},"body":{"model":"gpt-4o","messages":[]},
                "credential":{"secret_ref":"OPENAI"}
            }),
        )
        .unwrap();
        match specs.iter().find(|s| s.action() == Action::LlmCall) {
            Some(OperationSpec::LlmCall {
                provider, model, ..
            }) => {
                assert_eq!(provider, "https://api.openai.com/v1/chat/completions");
                assert_eq!(model, "gpt-4o");
            }
            other => panic!("llm spec missing: {other:?}"),
        }
        // 模型缺失 → 空串（fail-closed：不匹配任何显式 models 列表）。
        let specs = derive_specs(
            &node,
            &json!({"method":"POST","url":"https://a.b/v1","headers":{},"body":{}}),
        )
        .unwrap();
        match specs.iter().find(|s| s.action() == Action::LlmCall) {
            Some(OperationSpec::LlmCall { model, .. }) => assert_eq!(model, ""),
            other => panic!("llm spec missing: {other:?}"),
        }
    }

    #[test]
    fn harness_command_requires_absolute_normalized_path() {
        let node = node("harness", json!({"mode": "command"}));
        let ok = json!({"command":"/usr/bin/git","args":["status"],"workdir":"/tmp","prompt":"p"});
        let specs = derive_specs(&node, &ok).unwrap();
        match &specs[0] {
            OperationSpec::Exec {
                executable,
                argv,
                stdin_digest,
                ..
            } => {
                assert_eq!(executable, "/usr/bin/git");
                assert_eq!(argv, &vec!["status".to_string()]);
                assert_eq!(stdin_digest.as_deref(), Some(sha256_hex(b"p").as_str()));
            }
            other => panic!("exec spec expected: {other:?}"),
        }
        // 相对命令与未规范化路径拒绝。
        assert!(derive_specs(&node, &json!({"command":"git","args":[]})).is_err());
        assert!(derive_specs(&node, &json!({"command":"/usr/bin/./git","args":[]})).is_err());
        assert!(derive_specs(&node, &json!({"command":"/usr/bin/../bin/git","args":[]})).is_err());
    }

    #[test]
    fn harness_shell_minus_c_is_shell_spec() {
        let node = node("harness", json!({"mode": "command"}));
        let script = "git status && git push origin main";
        let specs = derive_specs(
            &node,
            &json!({"command":"/bin/sh","args":["-c",script],"workdir":"/tmp","prompt":""}),
        )
        .unwrap();
        match &specs[0] {
            OperationSpec::Shell {
                executable,
                script_digest,
                ..
            } => {
                assert_eq!(executable, "/bin/sh");
                assert_eq!(script_digest, &sha256_hex(script.as_bytes()));
            }
            other => panic!("shell spec expected: {other:?}"),
        }
        // shell 但不是 -c 两参形态 → 仍按 Exec。
        let specs = derive_specs(
            &node,
            &json!({"command":"/bin/bash","args":["-c","x","extra"],"prompt":""}),
        )
        .unwrap();
        assert!(matches!(specs[0], OperationSpec::Exec { .. }));
    }

    #[test]
    fn harness_agent_derives_llm_and_secret() {
        let node = node("harness", json!({"mode": "agent"}));
        let specs = derive_specs(
            &node,
            &json!({
                "mode":"agent","prompt":"p","workdir":"/w","timeout_ms":1000,
                "agent":{"base_url":"https://api.deepseek.com/v1","model":"deepseek-chat"},
                "credential":{"scheme":"bearer","secret_ref":"DEEPSEEK_KEY"}
            }),
        )
        .unwrap();
        // agent 请求没有 url/body：llm_spec 以 agent 配置为准。
        match specs.iter().find(|s| s.action() == Action::LlmCall) {
            Some(OperationSpec::LlmCall {
                provider, model, ..
            }) => {
                assert_eq!(provider, "https://api.deepseek.com/v1");
                assert_eq!(model, "deepseek-chat");
            }
            other => panic!("llm spec missing: {other:?}"),
        }
        assert_eq!(specs[1].action(), Action::Secret);
    }

    #[test]
    fn dynamic_plugin_node_is_rejected_until_p1() {
        let node = node("echo", json!({}));
        assert!(derive_specs(&node, &json!({})).is_err());
    }

    #[test]
    fn normalize_provider_strips_default_port_and_trailing_slash() {
        assert_eq!(
            normalize_provider("HTTPS://Api.Example.com:443/v1/"),
            "https://api.example.com/v1"
        );
        assert_eq!(
            normalize_provider("http://api.example.com:8080"),
            "http://api.example.com:8080"
        );
    }
}
