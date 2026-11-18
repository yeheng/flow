//! flow 引擎 crate 的公共门面：模块编排、跨层常量与对外重导出。
//!
//! 本文件只做三件事——声明子模块、重导出稳定入口、定义全局不变量常量；
//! 任何执行逻辑都不放在这里。节点语义在 `model`、表达式求值在 `expr`、
//! 传输协议在 `execution_protocol`：同一份事实散落在多处时，修复一处
//! 必然漏掉另一处，且漏改处不会编译报错。
//!
//! 它守护的核心契约是**执行兼容性标识**（`RUNTIME_ID`）与**子流程深度
//! 上限**（`MAX_SUB_WORKFLOW_DEPTH`）：前者决定 master 与 executor 能否
//! 互认同一套节点语义，后者是防止子流程无限递归的硬闸门。
//!
//! 不这样做的代价：执行器与 master 版本漂移时，在飞 run 会被按新语义
//! 执行——结果是「悄悄跑错」而不是「响亮拒绝」，事后无法从证据倒推。
//!
//! `runtime` feature 门控的是**需要执行期依赖**的模块（SQLite 存储、
//! HTTP 客户端、QuickJS 沙箱、密钥加密）。关闭它可得到一个只含协议与
//! 数据模型的精简构建，用于不需要执行侧的纯消费方。

#[cfg(feature = "runtime")]
pub mod error;
#[cfg(feature = "runtime")]
pub mod exec;
pub use flow_dispatcher::execution_protocol;
pub use flow_domain::journal_state;
pub use flow_dto::AgentRunConfig;
#[cfg(feature = "runtime")]
pub mod log_types;
pub use flow_domain::model;
#[cfg(feature = "runtime")]
pub mod nodelog;
#[cfg(feature = "runtime")]
pub mod observation;
pub use flow_dispatcher::process;
pub use flow_domain::schema;
pub mod environments;
pub mod secrets;
#[cfg(feature = "runtime")]
pub mod secrets_store;
pub use flow_domain::template;
/// Versioned handler selected by the workflow adapter for prepared script computation.
pub const SCRIPT_V1_HANDLER: &str = "script.v1";
pub use flow_domain::MAX_SUB_WORKFLOW_DEPTH;
/// Execution compatibility, frozen into runs. Bump when node semantics, snapshot
/// interpretation or executor contracts become incompatible. Never accept an old
/// source hash as this compatibility version without an explicit migration.
pub const RUNTIME_ID: &str = env!("FLOW_COMPATIBILITY_ID");
/// 显式的执行兼容版本号（如 `flow-execution-v1`）。与 [`RUNTIME_ID`] 成对使用：
/// `RUNTIME_ID` 是它的稳定摘要，`RUNTIME_VERSION` 是人能读的版本标签。
/// 升级语义而无法保证兼容时，两者都要提升，并让 master 与 executor 锁步升级。
pub const RUNTIME_VERSION: &str = env!("FLOW_RUNTIME_VERSION");
/// Exact source/dependency provenance; diagnostic only, never a resume gate.
pub const BUILD_ID: &str = env!("FLOW_BUILD_ID");
#[cfg(feature = "runtime")]
pub use error::EngineError;
#[cfg(feature = "runtime")]
pub use log_types::{LogLevel, LogStream};
pub use model::{Definition, Edge, Node, NodeType, Position, RetryPolicy, HTTP_METHODS};
#[cfg(feature = "runtime")]
pub use nodelog::{budget_from_env, redact_url, redact_value, LogBudget, LogLine, NodeLogger};
