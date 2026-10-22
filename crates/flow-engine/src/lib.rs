#[cfg(feature = "runtime")]
pub mod error;
#[cfg(feature = "runtime")]
pub mod exec;
pub mod execution_protocol;
#[cfg(feature = "runtime")]
pub mod expr;
#[cfg(feature = "runtime")]
pub mod journal_state;
pub use flow_dto::AgentRunConfig;
#[cfg(feature = "runtime")]
pub mod log_types;
#[cfg(feature = "runtime")]
pub mod model;
#[cfg(feature = "runtime")]
pub mod nodelog;
#[cfg(feature = "runtime")]
pub mod observation;
pub mod process;
#[cfg(feature = "runtime")]
pub mod schema;
#[cfg(feature = "runtime")]
pub mod secrets;
#[cfg(feature = "runtime")]
pub mod secrets_store;
pub const MAX_SUB_WORKFLOW_DEPTH: u32 = 16;
/// Execution compatibility, frozen into runs. Bump when node semantics, snapshot
/// interpretation or executor contracts become incompatible. Never accept an old
/// source hash as this compatibility version without an explicit migration.
pub const RUNTIME_ID: &str = env!("FLOW_COMPATIBILITY_ID");
pub const RUNTIME_VERSION: &str = env!("FLOW_RUNTIME_VERSION");
/// Exact source/dependency provenance; diagnostic only, never a resume gate.
pub const BUILD_ID: &str = env!("FLOW_BUILD_ID");
#[cfg(feature = "runtime")]
pub use error::EngineError;
#[cfg(feature = "runtime")]
pub use log_types::{LogLevel, LogStream};
#[cfg(feature = "runtime")]
pub use model::{Definition, Edge, Node, NodeType, Position, RetryPolicy, HTTP_METHODS};
#[cfg(feature = "runtime")]
pub use nodelog::{budget_from_env, redact_url, redact_value, LogBudget, LogLine, NodeLogger};
