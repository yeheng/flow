//! conga — a pi-style pluggable agent core.
//!
//! A single `agent_loop` function plus an `ExtensionApi` trait. Extra tools and
//! hooks come from in-process Rust extension crates that call `register` at
//! host startup (optionally behind Cargo features). No dynamic library loading.

pub mod agent_loop;
pub mod cancel;
pub mod config_file;
pub mod error;
pub mod extension;
pub mod guard;
pub mod providers;
pub mod steer;
pub mod storage;
pub mod types;

#[cfg(test)]
pub(crate) mod test_util;
pub use agent_loop::{agent_loop, run_agent_loop};
pub use cancel::CancelSignal;
pub use error::{AgentError, ToolError};
pub use extension::{ExtensionApi, ExtensionApiImpl};
pub use providers::{AnthropicProvider, ConfigError, OpenAiCompat, ProviderConfig};
pub use steer::SteerQueue;

pub use storage::{is_valid_session_id, EventStorage, JsonlStorage, SessionMeta};

pub use types::context::{
    AgentContext, AgentLoopConfig, AgentTunables, ModelSpec, ProviderApi, RetryPolicy, StreamChunk,
    StreamFn,
};
pub use types::event::{AgentEvent, ContentDelta};
pub use types::message::{
    AgentMessage, AssistantMessage, ContentBlock, StopReason, ToolResultMessage, Usage, UserMessage,
};
pub use types::session_event::{
    cache_stats, derive_messages, live_range_start, repair_unanswered_tool_calls, CacheStats,
    CancelCause, SessionEvent, TurnEndReason,
};
pub use types::tool::{
    HookChain, RiskLevel, ToolCallCtx, ToolCallVerdict, ToolContext, ToolDefinition, ToolFn,
    ToolResult,
};

/// Current monotonically-increasing time in milliseconds since UNIX epoch.
///
/// Used for message timestamps.
pub fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
