//! Workflow definitions and validation, independent of storage and execution facilities.
pub mod model;
pub mod schema;
pub use model::{Definition, Edge, Node, NodeType, Position, RetryPolicy, HTTP_METHODS};

pub mod journal_state;
pub mod template;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EnvironmentBinding {
    pub id: String,
    pub revision: u64,
    pub config: serde_json::Value,
}

/// 子工作流嵌套的硬上限。子流程调用是「run 套 run」的递归结构，
/// 没有上限时一个自引用的定义能把执行器拖进无界递归（栈溢出或
/// 无限派发），且崩溃点在最深处的子 run，事后极难定位。
/// 取 16 是「远深于任何合理业务嵌套」与「仍在可诊断范围内」的折中。
pub const MAX_SUB_WORKFLOW_DEPTH: u32 = 16;
