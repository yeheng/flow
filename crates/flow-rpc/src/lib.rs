//! flow-rpc：jsonrpsee WebSocket 服务（bin: flow-journal-server）。
//!
//! v1 RPC 面（`flow-server` 的无认证契约，35 个方法直连 `AnyBackend`）已随
//! flow-server 退役。本 crate 现在只有一个产品面：
//! - [`journal_v2`]：journal v2 协议（token 认证、写命令幂等键、回执语义）
//!   + v1 物化数据形状（`flow_backend::journal_arm` 折影）——浏览器前端、
//!   桌面内嵌服务、flow-cli 全部走这一份方法集；
//! - [`journal_triggers`]：cron 扫描与 `POST /hooks/<key>` HTTP 入口；
//! - [`journal_download`]：有界值下载 HTTP。
//!
//! 本文件的职责只剩产品装配与共享助手：`AppState` / `ConfigState`
//! （统一配置面 + 持久化密钥，挂在 v2 模块的 config.*/secrets.* 上）、
//! timeline 投影（`timeline_value`）、展示脱敏（`redact_display_envelope`）、
//! 节点类型目录与产品面校验（cron / 模板片段 / 密钥名）。
//!
//! 错误码词汇（v2 单一来源）：`-32001` unauthorized、`-32011` not found、
//! `-32012` conflict、`-32020` COMMITTED_NOT_VISIBLE、`-32021` limit/超限、
//! `-32602` invalid params、`-32603` internal。

use std::sync::Arc;

use flow_backend::{AnyBackend, Definition, Envelope, Event, NodeState, NodeType, RunState};
use jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned};
use serde_json::{json, Value};

pub mod journal_download;
pub mod journal_triggers;
pub mod journal_v2;

/// RPC 层状态：v2 产品装配的工作副本（config.get/update、secrets.set/delete）。
pub struct AppState {
    pub backend: AnyBackend,
    /// 统一配置面（config.get / config.update 的工作副本）。
    /// `Some` 时 update 可写回文件；`None`（测试简装）只回默认值。
    /// 这里的 config 是**文件级**视图（默认值 + 文件，不含 env 覆盖）——
    /// 文件才是用户可编辑的真相，env 覆盖名单单列展示。
    pub config: Option<ConfigState>,
    /// 持久化密钥存储（secrets.set/delete 落这里）；None 时这两个方法报
    /// 「未启用」。引擎侧取值经 secrets::get_secret（stored 优先，env 兜底）。
    pub secrets: Option<Arc<flow_engine::secrets_store::SecretFileStore>>,
}

/// 进程启动时装配的配置工作副本。
pub struct ConfigState {
    pub config: std::sync::RwLock<flow_config::Config>,
    /// 配置文件路径；None 时 update 会落到 `./flow.toml`。
    pub path: Option<std::path::PathBuf>,
    /// 启动时生效的 env 覆盖名单（仅名称，值可能含凭据不回传）。
    pub env_overrides: Vec<String>,
}

impl AppState {
    /// 产品入口：配置 + 密钥存储全量装配。密钥文件在 data_dir 下
    /// （secret.key + secrets.json）；安装进程级 SecretSource 失败（重复）
    /// 只告警——先装先得，第二个入口不该发生。
    pub async fn for_production(
        backend: AnyBackend,
        config_state: ConfigState,
        data_dir: &std::path::Path,
    ) -> (
        Arc<AppState>,
        Option<Arc<flow_engine::secrets_store::SecretFileStore>>,
    ) {
        let store = match flow_engine::secrets_store::SecretFileStore::open(data_dir) {
            Ok(store) => {
                let store = Arc::new(store);
                if let Err(err) = flow_engine::secrets::install_secret_source(store.clone()) {
                    tracing::warn!(%err, "持久化密钥来源安装失败，仅环境变量密钥生效");
                    None
                } else {
                    Some(store)
                }
            }
            Err(err) => {
                tracing::warn!(%err, "密钥存储初始化失败，secrets.set/delete 不可用");
                None
            }
        };
        (
            Arc::new(AppState {
                backend,
                config: Some(config_state),
                secrets: store.clone(),
            }),
            store,
        )
    }
}

/// config.get / config.update 的公共响应体。
/// `config.storage.database_url` 含凭据：一律脱敏为 `"<set>"` / null；
/// update 侧收到 `"<set>"` 表示保持原值不变。
pub(crate) fn config_view(state: &AppState) -> Value {
    let (config, path, env_overrides) = match &state.config {
        Some(cs) => (
            Some(cs.config.read().unwrap().clone()),
            cs.path.clone(),
            cs.env_overrides.clone(),
        ),
        None => (None, None, Vec::new()),
    };
    let mut config = config.unwrap_or_default();
    if config
        .storage
        .database_url
        .as_deref()
        .is_some_and(|v| !v.is_empty())
    {
        config.storage.database_url = Some("<set>".into());
    }
    json!({
        "config": config,
        "config_path": path,
        "env_overrides": env_overrides,
    })
}

pub fn toml_config_from_str(text: &str) -> Result<flow_config::Config, Box<dyn std::error::Error>> {
    Ok(flow_config::Config::from_toml_str(text)?)
}

pub fn timeline_value(
    run_id: &str,
    run_status: &str,
    workflow_id: &str,
    workflow_version: i64,
    definition: &Definition,
    snapshot: &RunState,
) -> Value {
    let nodes: Vec<Value> = definition
        .nodes
        .iter()
        .map(|node| {
            let record = snapshot.record(&node.id);
            // output 展示出口脱敏（固定敏感键）：事件日志里的原始 output 是
            // 下游节点的数据面，不能动；这里只脱时间线的展示值。
            // input 在 node_started 写入时已脱敏，直接透传。
            // 节点输出的唯一所有者是 NodeRecord.output（见 fold.rs）
            //
            // 同一规则在订阅通知上还有一份（`redact_display_envelope`）——
            // 两个展示面必须同规则，否则前端会用一个覆盖另一个。
            let output = record
                .output()
                .cloned()
                .map(|v| flow_backend::redact_value(&v));
            let mut entry = json!({
                "id": node.id,
                "name": node.name,
                "type": node.node_type,
                "state": record.state.label(),
                // attempts / error 从 state 派生（NodeRecord 不存副本，见
                // NodeState::attempt / ::error）——wire 形状逐字不变。
                "attempts": record.state.attempt(),
                "started_at": record.started_at,
                "ended_at": record.ended_at,
                "duration_ms": record.duration_ms,
                "input": record.input,
                "output": output,
                "error": record.state.error(),
                "child_run_id": record.child_run_id,
            });
            if let NodeState::Skipped { reason } = &record.state {
                entry["reason"] = json!(reason);
            }
            entry
        })
        .collect();

    json!({
        "run_id": run_id,
        "status": run_status,
        "phase": snapshot.phase,
        "workflow_id": workflow_id,
        "workflow_version": workflow_version,
        "started_at": snapshot.started_at,
        "ended_at": snapshot.ended_at,
        // run 级 output 是**数据面**：与 run.get / run_completed 事件逐字一致。
        // 脱敏只发生在节点级展示值（上面 nodes[].output）——节点的输出可能是
        // 上游 HTTP 响应（会回显凭据），而 run 级 output 就是 run.get 那个值。
        // 同名字段必须同值：否则客户端从 timeline 重建输出会拿到污染数据。
        "output": snapshot.output,
        "fatal_error": snapshot.fatal_error,
        "last_seq": snapshot.last_seq,
        "nodes": nodes,
    })
}

/// 订阅通知的展示脱敏：`node_completed.output` 按与 [`timeline_value`] 同一
/// 规则脱敏后下发。
///
/// **为什么订阅也要脱敏**：事件日志里存的是原始 output（数据面，下游节点要
/// 消费，不能动），而展示面有两处——timeline 投影与订阅通知。只脱一处时，
/// 前端 `applyEvent(node_completed)` 会用事件里的原始值**覆盖** timeline 的
/// 脱敏值（monitor-logic.ts），实时观看的 run 于是把敏感值原样显示出来，
/// 而同一个 run 事后查看反而是脱敏的——同一个字段两种命运。
///
/// 与 timeline 的差异只有一处：run 级 output（`run_completed`）两面都不脱敏
/// （它就是 run.get 那个数据面值，同名字段必须同值），见 timeline_value 注释。
pub(crate) fn redact_display_envelope(mut envelope: Envelope) -> Envelope {
    if let Event::NodeCompleted { output, .. } = &mut envelope.event {
        *output = flow_backend::redact_value(output);
    }
    envelope
}

/// 前端拖拽面板 + 参数表单所需的能力清单。
///
/// 单条描述的唯一来源是 `flow_engine::NodeType::descriptor`（与引擎共用一份
/// 定义），这里只负责按序组装；schema 约定见 descriptor 的文档注释。
pub(crate) fn node_types() -> Value {
    json!(NodeType::ALL
        .iter()
        .map(|kind| kind.descriptor().clone())
        .collect::<Vec<Value>>())
}

/// cron 合法性校验（标准 5 字段，分 时 日 月 周，本地时间）。非法表达式
/// -32602（与 v2 参数错误同一层）。
pub(crate) fn validate_cron(expr: &str) -> Result<(), ErrorObjectOwned> {
    expr.parse::<cron_parser::Schedule>()
        .map(|_| ())
        .map_err(|e| invalid(format!("非法的 cron 表达式：{e}")))
}

/// schedule 响应：实体字段 + 服务端算好的 next_fire_at（RFC3339，本地时区偏移）。
/// 存量数据 cron 损坏时 next_fire_at 为 null，不让 list 整个失败。
pub(crate) fn schedule_value(schedule: &flow_backend::Schedule) -> Value {
    let next_fire_at = schedule
        .cron_expr
        .parse::<cron_parser::Schedule>()
        .ok()
        .and_then(|cron| cron.next_after(&chrono::Local::now()))
        .map(|t| t.to_rfc3339());
    let mut value = json!(schedule);
    value["next_fire_at"] = json!(next_fire_at);
    value
}

pub(crate) fn invalid(message: impl ToString) -> ErrorObjectOwned {
    ErrorObject::owned(-32602, message.to_string(), None::<()>)
}

/// 持久化密钥名称约束：环境变量后缀的合法子集（字母数字下划线），
/// 1..=64 字符。名称会拼进 FLOW_SECRET_<名称> 展示，不能引入歧义字符。
pub(crate) fn validate_secret_name(name: &str) -> Result<(), ErrorObjectOwned> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if valid {
        Ok(())
    } else {
        Err(invalid(
            "密钥名称须为 1..=64 个字母/数字/下划线（如 OPENAI_KEY）",
        ))
    }
}

/// 模板名约束：非空、去首尾空白后 1..=100 字符。
pub(crate) fn validate_template_name(name: &str) -> Result<(), ErrorObjectOwned> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.len() > 100 {
        return Err(invalid("模板名须为 1..=100 个字符"));
    }
    Ok(())
}

/// 画布片段校验（template.create / template.update 共用）：
/// - nodes 非空数组；每项 `{id, type, name, position?, params}`——类型已知，
///   params 过 [`flow_engine::model::validate_params`]（与整图同源的参数规则）；
/// - edges 数组（可为空）；端点必须在片段内；带 sourceHandle 的边只允许
///   condition 节点且端口为 "true"/"false"（与整图规则一致）。
///
/// 返回**归一化**后的片段：只保留白名单字段，杜绝面板存进任意 JSON。
pub(crate) fn validate_fragment(nodes: &Value, edges: &Value) -> Result<(Value, Value), ErrorObjectOwned> {
    let nodes_arr = nodes
        .as_array()
        .ok_or_else(|| invalid("nodes 必须是数组"))?;
    if nodes_arr.is_empty() {
        return Err(invalid("片段至少要有一个节点"));
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut normalized_nodes = Vec::with_capacity(nodes_arr.len());
    for raw in nodes_arr {
        let id = raw["id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("片段节点缺少非空 id"))?;
        if !ids.insert(id.to_string()) {
            return Err(invalid(format!("片段节点 id 重复：{id}")));
        }
        let type_str = raw["type"]
            .as_str()
            .ok_or_else(|| invalid(format!("片段节点 {id} 缺少 type")))?;
        let kind = NodeType::parse(type_str)
            .ok_or_else(|| invalid(format!("片段节点 {id} 的类型未知：{type_str}")))?;
        let name = raw["name"].as_str().unwrap_or(id);
        let params = raw.get("params").cloned().unwrap_or(json!({}));
        if !params.is_object() {
            return Err(invalid(format!("片段节点 {id} 的 params 必须是对象")));
        }
        let node = flow_engine::Node {
            id: id.to_string(),
            node_type: type_str.to_string(),
            name: name.to_string(),
            position: None,
            params,
        };
        flow_engine::model::validate_params(&node, kind)
            .map_err(|e| invalid(format!("片段节点 {id} 参数非法：{e}")))?;
        let position = raw.get("position").filter(|p| p.is_object()).cloned();
        let mut normalized = json!({
            "id": node.id,
            "type": type_str,
            "name": node.name,
            "params": node.params,
        });
        if let Some(position) = position {
            normalized["position"] = position;
        }
        normalized_nodes.push(normalized);
    }

    let edges_arr = edges
        .as_array()
        .ok_or_else(|| invalid("edges 必须是数组"))?;
    let mut normalized_edges = Vec::with_capacity(edges_arr.len());
    for raw in edges_arr {
        let source = raw["source"].as_str().unwrap_or_default();
        let target = raw["target"].as_str().unwrap_or_default();
        if !ids.contains(source) || !ids.contains(target) {
            return Err(invalid(format!(
                "片段边的端点不在片段内：{source} → {target}"
            )));
        }
        if source == target {
            return Err(invalid(format!("片段不允许自环：{source}")));
        }
        let mut normalized = json!({ "source": source, "target": target });
        if let Some(handle) = raw["sourceHandle"].as_str() {
            if handle != "out" {
                let source_kind = NodeType::parse(
                    normalized_nodes
                        .iter()
                        .find(|n| n["id"] == json!(source))
                        .and_then(|n| n["type"].as_str())
                        .unwrap_or_default(),
                );
                if source_kind != Some(NodeType::Condition) || !matches!(handle, "true" | "false") {
                    return Err(invalid(format!(
                        "片段边 {source}→{target} 的 sourceHandle 非法：{handle}（仅 condition 节点可用 true/false）"
                    )));
                }
            }
            normalized["sourceHandle"] = json!(handle);
        }
        if let Some(handle) = raw["targetHandle"].as_str() {
            normalized["targetHandle"] = json!(handle);
        }
        normalized_edges.push(normalized);
    }
    Ok((
        Value::Array(normalized_nodes),
        Value::Array(normalized_edges),
    ))
}

/// `nodetypes.list` 响应快照：descriptor 注册表收敛（model.rs `NodeType::descriptor`）
/// 是纯重构，响应必须逐字节不变。新增/修改节点类型时同步更新本快照。
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// 订阅通知的展示脱敏：node_completed 的 output 脱敏，其余事件与字段原样。
    /// 回归的是「只脱 timeline 不脱订阅 → 前端用事件原始值覆盖脱敏值」。
    #[test]
    fn subscription_notice_redacts_node_output_like_timeline() {
        let envelope = Envelope {
            seq: 7,
            ts: Utc::now(),
            run_id: "r".into(),
            event: Event::NodeCompleted {
                node_id: "n".into(),
                attempt: 1,
                output: serde_json::json!({
                    "status": 200,
                    "headers": {"set-cookie": "sid=secret-value", "content-type": "application/json"},
                    "body": {"ok": true}
                }),
                duration_ms: 12,
            },
        };
        let redacted = redact_display_envelope(envelope);
        let Event::NodeCompleted { output, .. } = &redacted.event else {
            panic!("事件类型不该被改写");
        };
        assert_eq!(output["headers"]["content-type"], json!("application/json"));
        assert_eq!(output["headers"]["set-cookie"], json!("***"));
        assert_eq!(output["body"]["ok"], json!(true));
    }

    /// run 级 output 是数据面（= run.get 的值），两个展示面都不动它。
    #[test]
    fn subscription_notice_keeps_run_level_output_verbatim() {
        let envelope = Envelope {
            seq: 9,
            ts: Utc::now(),
            run_id: "r".into(),
            event: Event::RunCompleted {
                output: serde_json::json!({"api_key": "sk-live"}),
            },
        };
        let redacted = redact_display_envelope(envelope);
        let Event::RunCompleted { output } = &redacted.event else {
            panic!("事件类型不该被改写");
        };
        assert_eq!(output["api_key"], json!("sk-live"));
    }

    #[test]
    fn node_types_snapshot_is_stable() {
        const SNAPSHOT: &str = r#"[{"category":"control","label":"开始","max_instances":1,"params_schema":{"properties":{},"type":"object"},"ports":[{"id":"out","label":"出"}],"type":"start"},{"category":"control","label":"结束","params_schema":{"properties":{},"type":"object"},"ports":[{"id":"in","label":"入"}],"type":"end"},{"category":"compute","label":"脚本","params_schema":{"properties":{"code":{"type":"string","x-help":"可用 input（run 输入）与 nodes（上游节点输出），用 return 返回结果","x-label":"JS 函数体","x-opaque":true,"x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"脚本超时（毫秒）"}},"required":["code"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"script"},{"category":"control","label":"条件分支","params_schema":{"properties":{"expr":{"type":"string","x-help":"表达式结果按真值判定（非空字符串、非 0 数为真），可用 input 与 nodes","x-label":"条件表达式","x-opaque":true,"x-widget":"code"},"timeout_ms":{"default":2000,"type":"integer","x-label":"求值超时（毫秒）"}},"required":["expr"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"true","label":"真"},{"id":"false","label":"假"}],"type":"condition"},{"category":"control","label":"等待","params_schema":{"properties":{"ms":{"type":"integer","x-help":"数字，或 ${input.x} / ${nodes.n.y} 模板（展开结果须为整数）","x-label":"时长（毫秒）"}},"required":["ms"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"delay"},{"category":"integration","label":"HTTP 请求","params_schema":{"properties":{"body":{"x-label":"请求体","x-widget":"json"},"headers":{"default":{},"x-label":"请求头","x-widget":"key-value"},"method":{"default":"GET","enum":["GET","POST","PUT","PATCH","DELETE"],"type":"string","x-label":"方法"},"proxy":{"type":"string","x-help":"http(s)://[user:pass@]host:port，留空直连","x-label":"代理"},"timeout_ms":{"default":30000,"type":"integer","x-label":"HTTP 超时（毫秒）"},"url":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n2.y} 模板（${} 内不能含 }）","x-label":"URL"}},"required":["url"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"http_call"},{"category":"human","label":"人工节点","params_schema":{"properties":{"prompt":{"type":"string","x-label":"提示"}},"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"type":"human_task"},{"category":"control","label":"子工作流","params_schema":{"properties":{"input_mapping":{"x-help":"JSON 对象，值支持 ${input.x} / ${nodes.n.y} 模板；展开结果整体作为子 run 输入。省略 = 沿用父 run 输入","x-label":"子 run 输入映射","x-widget":"json"},"workflow_id":{"type":"string","x-help":"调用其最新已发布版本作为子 run；子 run 输出透传为本节点输出；子 run 失败传导为本节点 fatal（DESIGN §6.8），重试策略只覆盖启动/等待类错误","x-label":"目标工作流","x-widget":"workflow-picker"}},"required":["workflow_id"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"supports_retry":true,"type":"sub_workflow"},{"category":"ai","label":"Harness 代理","params_schema":{"properties":{"args":{"items":{"type":"string"},"type":"array","x-label":"额外参数","x-widget":"json"},"command":{"type":"string","x-help":"要执行的 harness CLI（如 kimi / claude / codex）；prompt 经 stdin 传入，stdout 作为输出","x-label":"命令"},"prompt":{"type":"string","x-label":"提示词","x-widget":"code"},"timeout_ms":{"default":300000,"type":"number","x-label":"超时 (ms)"},"workdir":{"type":"string","x-label":"工作目录"}},"required":["command","prompt"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"harness"},{"category":"notify","label":"邮件","params_schema":{"properties":{"api_key":{"type":"string","x-help":"只存密钥名称（如 RESEND_KEY），真值来自 FLOW_SECRET_<名称> 环境变量；已配置的名称见 secrets.list","x-label":"API 密钥名称","x-secret":true},"body":{"type":"string","x-help":"纯文本（作为 text 字段发送），支持 ${input.x} / ${nodes.n.y} 模板","x-label":"正文","x-widget":"code"},"endpoint":{"default":"https://api.resend.com/emails","type":"string","x-help":"Resend 兼容接口：POST {from, to, subject, text}，Bearer 认证","x-label":"API 端点"},"from":{"type":"string","x-label":"发件人"},"subject":{"type":"string","x-help":"支持 ${input.x} / ${nodes.n.y} 模板","x-label":"主题"},"to":{"type":"string","x-label":"收件人"}},"required":["api_key","from","to","subject","body"],"type":"object"},"ports":[{"id":"in","label":"入"},{"id":"out","label":"出"}],"side_effect":true,"supports_retry":true,"type":"email"}]"#;
        assert_eq!(
            serde_json::to_string(&crate::node_types()).unwrap(),
            SNAPSHOT,
            "nodetypes.list 响应变了：若是有意变更，更新本快照"
        );
    }
}
