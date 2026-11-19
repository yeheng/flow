//! Prepared input for the script worker, shared by local and managed execution.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::NodeType;

pub const SCRIPT_V1_HANDLER: &str = "script.v1";

/// Frozen clock and seed for reproducible script evaluation.
#[derive(Clone, Copy, Deserialize, Serialize)]
pub struct JsDeterminism {
    pub clock_ms: i64,
    pub seed: u32,
}

#[derive(Deserialize, Serialize)]
pub struct ScriptTask {
    pub code: String,
    pub input: Value,
    pub nodes: Value,
    pub timeout_ms: Option<u64>,
    pub determinism: Option<JsDeterminism>,
}

impl ScriptTask {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(2000).min(30_000))
    }

    pub fn from_prepared(
        kind: NodeType,
        params: &Value,
        input: Value,
        nodes: Value,
        determinism: Option<JsDeterminism>,
    ) -> Option<Self> {
        let code = match kind {
            NodeType::Script => params["code"].as_str().unwrap_or("").to_owned(),
            NodeType::Condition => {
                format!("return ({});", params["expr"].as_str().unwrap_or("false"))
            }
            _ => return None,
        };
        Some(Self {
            code,
            input,
            nodes,
            timeout_ms: params["timeout_ms"].as_u64(),
            determinism,
        })
    }
}

/// JavaScript truthiness for JSON values, including empty arrays and objects.
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}
