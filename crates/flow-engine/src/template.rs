//! Shared rules for reusable canvas fragments at every command entry point.
use crate::NodeType;
use serde_json::{json, Value};

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
    if !value["category"].is_null() && !value["category"].is_string() {
        return Err(invalid("template category must be a string or null"));
    }
    let (nodes, edges) = normalize_fragment(&value["nodes"], &value["edges"])?;
    value["nodes"] = nodes;
    value["edges"] = edges;
    Ok(())
}

/// 模板名约束：非空、去首尾空白后 1..=100 字符。
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
        let node = crate::Node {
            id: id.to_string(),
            node_type: type_str.to_string(),
            name: name.to_string(),
            position: None,
            params,
        };
        crate::model::validate_params(&node, kind)
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
