//! 密钥机制：definition 的 params 只存密钥**名称**，真值来自环境变量
//! `FLOW_SECRET_<名称>`（如 `FLOW_SECRET_OPENAI_KEY` 对应名称 `OPENAI_KEY`）。
//!
//! 三条边界：
//! - 名称列表对前端公开（`secrets.list`），真值永远不出进程内存；
//! - 执行注入发生在 dispatch 之前（模板展开已由 driver 的 start_node 完成）——
//!   真值不参与 `${}` 展开，也不会随 node_started 落盘（事件在注入前已写）；
//! - `workflow.update` 提前校验名称存在，把配置错误挡在发布前。

use std::sync::{Arc, OnceLock};

use serde_json::Value;

use crate::model::{Definition, Node};

/// 密钥环境变量前缀。名称 `OPENAI_KEY` ↔ 环境变量 `FLOW_SECRET_OPENAI_KEY`。
pub const SECRET_ENV_PREFIX: &str = "FLOW_SECRET_";

/// 追加的密钥来源（web 界面管理的**持久化密钥**，AES-GCM 落盘，见
/// [`crate::secrets_store::SecretFileStore`]）。进程入口装一次；装上后
/// `get_secret` 先查它，环境变量兜底——环境变量机制原样保留。
pub trait SecretSource: Send + Sync {
    fn get(&self, name: &str) -> Option<String>;
    fn names(&self) -> Vec<String>;
}

static STORED: OnceLock<Arc<dyn SecretSource>> = OnceLock::new();

/// 安装持久化密钥来源。重复安装报错（进程入口只调一次；测试各自直连 store）。
pub fn install_secret_source(source: Arc<dyn SecretSource>) -> Result<(), String> {
    STORED
        .set(source)
        .map_err(|_| "secret source already installed".to_string())
}

fn stored() -> Option<&'static Arc<dyn SecretSource>> {
    STORED.get()
}

/// 密钥来源归因：stored（界面管理）优先于 env。
pub fn source_of(name: &str) -> &'static str {
    match stored() {
        Some(source) if source.get(name).is_some() => "stored",
        _ => "env",
    }
}

/// 密钥清单（排序，去重，永远不含值），附来源归因。
pub fn list_secrets() -> Vec<(String, &'static str)> {
    let mut out: Vec<(String, &'static str)> = Vec::new();
    if let Some(source) = stored() {
        for name in source.names() {
            if !name.is_empty() {
                out.push((name, "stored"));
            }
        }
    }
    for key in std::env::vars() {
        if let Some(name) = key.0.strip_prefix(SECRET_ENV_PREFIX) {
            if !name.is_empty() && !out.iter().any(|(n, _)| n == name) {
                out.push((name.to_string(), "env"));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup_by(|a, b| a.0 == b.0);
    out
}

/// 按名称取密钥真值：先查持久化来源（若有），回落环境变量。
/// 不缓存（进程内可随时改，测试友好）。
pub fn get_secret(name: &str) -> Option<String> {
    if let Some(value) = stored().and_then(|s| s.get(name)) {
        return Some(value);
    }
    std::env::var(format!("{SECRET_ENV_PREFIX}{name}")).ok()
}

/// 已配置的密钥名称列表（排序，永远不含值）。
pub fn list_secret_names() -> Vec<String> {
    let mut names: Vec<String> = list_secrets().into_iter().map(|(name, _)| name).collect();
    names.sort();
    names.dedup();
    names
}

/// 把节点 params 里 x-secret 参数的名称解析为真值。返回 `None` 表示该节点
/// 没有需要解析的参数（零克隆快速路径）；名称对应的环境变量不存在是参数
/// 错误，由调用方包装成 fatal。
pub fn resolve_node_secrets(node: &Node) -> Result<Option<Node>, String> {
    let Some(kind) = node.kind() else {
        return Ok(None);
    };
    let keys = kind.secret_params();
    if keys.is_empty() {
        return Ok(None);
    }
    let Some(map) = node.params.as_object() else {
        return Ok(None);
    };
    if !keys.iter().any(|key| map.contains_key(*key)) {
        return Ok(None);
    }
    let mut params = node.params.clone();
    for key in keys {
        let Some(name) = params.get(key).and_then(Value::as_str) else {
            continue;
        };
        let value = get_secret(name).ok_or_else(|| {
            format!(
                "节点 {} 的密钥 {name} 未配置：请设置环境变量 {SECRET_ENV_PREFIX}{name}",
                node.id
            )
        })?;
        params[key] = Value::String(value);
    }
    Ok(Some(Node {
        params,
        ..node.clone()
    }))
}

/// `workflow.update` 的提前校验：列出所有引用了不存在密钥的（节点 id, 名称）。
/// `${}` 模板名称要到执行期展开后才能确定，这里放行（执行期缺失仍会 fatal）。
pub fn missing_secrets(definition: &Definition) -> Vec<(String, String)> {
    let mut missing = Vec::new();
    for node in &definition.nodes {
        let Some(kind) = node.kind() else {
            continue;
        };
        for key in kind.secret_params() {
            let Some(name) = node.params.get(key).and_then(Value::as_str) else {
                continue;
            };
            if name.contains("${") {
                continue;
            }
            if get_secret(name).is_none() {
                missing.push((node.id.clone(), name.to_string()));
            }
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    // 环境变量是进程全局状态：每个测试用独占名称，互不干扰。
    #[test]
    fn get_secret_reads_env_by_name() {
        std::env::set_var("FLOW_SECRET_TEST_GET", "s3cret");
        assert_eq!(get_secret("TEST_GET").as_deref(), Some("s3cret"));
        std::env::remove_var("FLOW_SECRET_TEST_GET");
        assert_eq!(get_secret("TEST_GET"), None);
        assert_eq!(get_secret("TEST_DEFINITELY_MISSING"), None);
    }

    #[test]
    fn list_secret_names_sorted_without_values() {
        std::env::set_var("FLOW_SECRET_TEST_LIST_B", "v1");
        std::env::set_var("FLOW_SECRET_TEST_LIST_A", "v2");
        let names = list_secret_names();
        let pos_a = names.iter().position(|n| n == "TEST_LIST_A").unwrap();
        let pos_b = names.iter().position(|n| n == "TEST_LIST_B").unwrap();
        assert!(pos_a < pos_b, "必须按名称排序：{names:?}");
        std::env::remove_var("FLOW_SECRET_TEST_LIST_A");
        std::env::remove_var("FLOW_SECRET_TEST_LIST_B");
        assert!(!list_secret_names().iter().any(|n| n == "TEST_LIST_A"));
    }
}
