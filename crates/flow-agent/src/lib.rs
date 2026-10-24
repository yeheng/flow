//! flow-agent：受信任的远程执行中继（三期 §1.1/§1.2）。
//!
//! agent 自己不推进 DAG、不写权威运行状态、不产生权威 ACK/Permit/
//! ResultCommitted（端到端确认只能来自主进程）；管理本机受控执行器、
//! 报告容量、按已确认路由有界公平转发。
//!
//! **它是什么、不是什么**：
//!
//! - 是：执行算力的提供者 + 主进程与远端执行器之间的中继；
//! - 不是：权威的副本。agent 崩溃只会丢掉它托管的那部分算力，
//!   master 的 SQLite 权威不受影响，恢复后重新纳管即可。
//!
//! 这个边界是刻意的：一旦 agent 也持有权威状态，就会出现两份权威，
//! 二者冲突时无法判定谁对。宁可让 agent 变成「可随时重建的无状态中继」。
//!
//! 模块划分：
//!
//! - [`runtime`]：上联会话与执行器生命周期；
//! - [`pool`]：本机执行器池与容量报告；
//! - [`relay`]：按已确认路由转发帧；
//! - [`tls`]：mTLS 客户端配置。

pub mod pool;
pub mod relay;
pub mod runtime;
pub mod tls;

/// agent 的构建版本（用于日志与握手时的版本比对）。
pub const AGENT_BUILD: &str = env!("CARGO_PKG_VERSION");

/// agent 上联能力（与执行器能力分别协商）。
///
/// 单独列出能力而不是靠版本号隐含：master 需要据此决定能不能让这个
/// agent 承担某种流量（例如没有 `reconnect` 的 agent 不能参与断线重连
/// 场景），而版本号无法表达这种细粒度差异。
pub const AGENT_CAPABILITIES: &[&str] = &["relay", "resource_report", "reconnect"];
