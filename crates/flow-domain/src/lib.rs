//! Workflow definitions and validation, independent of storage and execution facilities.
pub mod model;
pub mod schema;
pub use model::{Definition, Edge, Node, NodeType, Position, RetryPolicy, HTTP_METHODS};

pub mod journal_state;
pub mod progression;
pub mod script;
pub mod template;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EnvironmentBinding {
    pub id: String,
    pub revision: u64,
    pub config: serde_json::Value,
}

/// 输入准备事实的冻结内容：节点将要消费的输入、前驱输出与参数。
///
/// `node_semantics` 记录执行语义版本，用于判断旧准备事实能否被当前引擎
/// 解释；`engine_build` 记录写入该准备事实的引擎构建号。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Prepared {
    pub input: flow_facts::StoredValue,
    pub predecessors: std::collections::BTreeMap<String, flow_facts::StoredValue>,
    pub params: flow_facts::StoredValue,
    pub engine_build: String,
    pub node_semantics: u32,
}

/// 子工作流嵌套的硬上限。子流程调用是「run 套 run」的递归结构，
/// 没有上限时一个自引用的定义能把执行器拖进无界递归（栈溢出或
/// 无限派发），且崩溃点在最深处的子 run，事后极难定位。
/// 取 16 是「远深于任何合理业务嵌套」与「仍在可诊断范围内」的折中。
pub const MAX_SUB_WORKFLOW_DEPTH: u32 = 16;
