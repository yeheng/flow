//! flow-agent：受信任的远程执行中继（三期 §1.1/§1.2）。
//!
//! agent 自己不推进 DAG、不写权威运行状态、不产生权威 ACK/Permit/
//! ResultCommitted（端到端确认只能来自主进程）；管理本机受控执行器、
//! 报告容量、按已确认路由有界公平转发。

pub mod pool;
pub mod relay;
pub mod runtime;
pub mod tls;

pub const AGENT_BUILD: &str = env!("CARGO_PKG_VERSION");

/// agent 上联能力（与执行器能力分别协商）。
pub const AGENT_CAPABILITIES: &[&str] = &["relay", "resource_report", "reconnect"];
