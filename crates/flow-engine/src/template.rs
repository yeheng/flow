//! Shared rules for reusable canvas fragments at every command entry point.
//!
//! 本模块负责「画布片段」（工作流模板）的校验与归一化，是 template.create /
//! template.update / workflow.import 等入口共用的唯一规则出处。
//!
//! 它守护的核心不变量是**片段与整图同源**：片段的节点参数走整图同一份
//! `validate_params`，边的端口规则与整图一致；片段落库前被归一化成
//! 白名单字段，杜绝面板把任意 JSON 塞进库里。
//!
//! 不这样做的代价：片段里的定义能在编辑器里画出来、存得进去，却在运行期
//! 因「类型不认识」「参数非法」而失败——错误被推迟到执行时，且现场与
//! 画布对不上，排查成本极高。
use crate::NodeType;
use serde_json::{json, Value};

/// 构造一条「输入非法」错误。
///
/// 片段类错误统一走 `flow_journal::Error::Invalid`：它属于**请求形状不对**
/// （4xx 类），而不是执行失败，两者在 RPC 层的回执语义不同。
fn invalid(message: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(message.into())
}

/// Validate a merged template document and normalize its fragment for persistence.
pub fn normalize(value: &mut Value) -> flow_journal::Result<()> {
    validate_name(
        value["name"]
            .as_str()
            .ok_or_else(|| invalid("template name required"))?,
    )?;
    // category 允许缺省（null），但必须是字符串或 null：其它形态说明
    // 客户端送来了非预期结构，宁可当场拒掉也不猜。
    if !value["category"].is_null() && !value["category"].is_string() {
        return Err(invalid("template category must be a string or null"));
    }
    let (nodes, edges) = normalize_fragment(&value["nodes"], &value["edges"])?;
    // 归一化结果写回原文档：调用方拿到的就是可落库的形态，不需要
    // 自己在「校验通过」与「存的是另一份」之间做对应。
    value["nodes"] = nodes;
    value["edges"] = edges;
    Ok(())
}

/// 模板名约束：非空、去首尾空白后 1..=100 字符。
///
/// 上限 100 是展示位约束（模板名进列表标题）；原始长度另设 1024 的
/// 上限挡「填充 100 个空格 + 内容」这类绕过——只看去空白后的长度
/// 会让实际存储的字节数无界。
pub fn validate_name(name: &str) -> Result<(), flow_journal::Error> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.len() > 100 || name.len() > 1024 {
        return Err(invalid("模板名须为 1..=100 个字符"));
    }
    Ok(())
}

/// 画布片段校验（template.create / template.update 共用）：
/// - nodes 非空数组；每项 `{id, type, name, position?, params}`——类型已知，
///   params 过 [`crate::model::validate_params`]（与整图同源的参数规则）；
/// - edges 数组（可为空）；端点必须在片段内；带 sourceHandle 的边只允许
///   condition 节点且端口为 "true"/"false"（与整图规则一致）。
///
/// 返回**归一化**后的片段：只保留白名单字段，杜绝面板存进任意 JSON。
pub fn normalize_fragment(
    nodes: &Value,
    edges: &Value,
) -> Result<(Value, Value), flow_journal::Error> {
    let nodes_arr = nodes
        .as_array()
        .ok_or_else(|| invalid("nodes 必须是数组"))?;
    if nodes_arr.is_empty() {
        return Err(invalid("片段至少要有一个节点"));
    }
    // 节点 id 去重集合同时充当「边端点必须在片段内」的查表依据：
    // 用 BTreeSet 是为了让报错顺序稳定（可复现的诊断信息）。
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
        // 类型必须能被词汇表认识：片段里的未知类型在运行期必然失败，
        // 与其在执行时炸，不如在建片段时就拒掉。
        let kind = NodeType::parse(type_str)
            .ok_or_else(|| invalid(format!("片段节点 {id} 的类型未知：{type_str}")))?;
        let name = raw["name"].as_str().unwrap_or(id);
        let params = raw.get("params").cloned().unwrap_or(json!({}));
        if !params.is_object() {
            return Err(invalid(format!("片段节点 {id} 的 params 必须是对象")));
        }
        let node = crate::Node {
            id: id.to_string(),
            node_type: type_str.to_string(),
            name: name.to_string(),
            // position 留空：片段不是画布坐标的权威来源，落库时
            // 位置由插入画布的一方决定，不从片段继承。
            position: None,
            params,
        };
        // 参数规则与整图同源（同一份 validate_params），只是不做
        // 整图级的 DAG 校验——片段本身不是完整 Definition。
        crate::model::validate_params(&node, kind)
            .map_err(|e| invalid(format!("片段节点 {id} 参数非法：{e}")))?;
        let position = raw.get("position").filter(|p| p.is_object()).cloned();
        // 白名单字段：只保留 id/type/name/params(/position)，其余
        // 一律丢弃，杜绝面板存进任意 JSON。
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
        // 端点必须在片段内：片段是自包含的，引用外部节点会让插入
        // 画布时得到一条断边。
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
                // 非 "out" 的端口只允许 condition 节点的 true/false：
                // 与整图规则一致，否则片段插进画布后会带出非法端口。
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
