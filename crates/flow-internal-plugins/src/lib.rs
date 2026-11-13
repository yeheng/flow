//! 内置节点实现库：静态编译进执行器的节点语义（T4 迁入）。
//!
//! 当前承载（T4 两批迁移后）：
//! - `script`：Script/Condition 的 JS 求值器（原 flow-engine::expr）；
//! - `command`：harness command/agent 模式的请求装配与输出解析；
//! - `email`：email 节点请求构造。
//!
//! HTTP 出站的执行编排仍在运行时（授权与不确定分类与审计交织）。
//!
//! 边界：纯实现，无执行器协议状态；授权/审计/恢复编排不在本包。

pub mod command;
pub mod email;
pub mod script;

pub use command::{
    agent_output, agent_params, agent_request, frozen_agent_config, harness_command,
    harness_output, harness_request, singular_or_map, stderr_tail, AgentParams, HarnessRequest,
    NodeFailure,
};
pub use email::email_request;
pub use script::{
    eval_body, eval_body_recorded, eval_expr, expand_templates, expand_templates_recorded,
    expand_templates_with_config, truthy, JsDeterminism, JsLimits,
};
