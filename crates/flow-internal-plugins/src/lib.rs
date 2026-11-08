//! 内置节点实现库：静态编译进执行器的节点语义（T4 迁入）。
//!
//! 当前承载：Script/Condition 的 JS 求值器（原 flow-engine::expr，含
//! 确定性上下文、预算、记录日志与超时）。后续分批迁入：HttpCall/Email
//! 请求构造与命令模式处理。
//!
//! 边界：纯实现，无执行器协议状态；授权/审计/恢复编排不在本包。

pub mod script;

pub use script::{
    eval_body, eval_body_recorded, eval_expr, expand_templates, expand_templates_recorded, truthy,
    JsDeterminism, JsLimits,
};
