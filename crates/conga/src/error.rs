//! Error types for the agent core.
//!
//! Populated in stage 3b.

use thiserror::Error;

/// Top-level error returned by `run_agent_loop`.
#[derive(Debug, Error)]
pub enum AgentError {
    #[error("provider error: {0}")]
    Provider(String),
    #[error("tool not found: {0}")]
    ToolNotFound(String),
    #[error("tool error: {0}")]
    Tool(String),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("corrupt transcript: {0}")]
    Transcript(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid session id: {0}")]
    InvalidSessionId(String),
    #[error("a turn is already running on this host")]
    TurnInProgress,
    #[error("context transform failed: {0}")]
    ContextTransform(String),
}

/// Error returned by a tool's `execute` closure.
#[derive(Debug, Error)]
pub enum ToolError {
    #[error("{0}")]
    Message(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl From<ToolError> for AgentError {
    fn from(e: ToolError) -> Self {
        AgentError::Tool(e.to_string())
    }
}
