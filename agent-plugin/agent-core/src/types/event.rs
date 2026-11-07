//! `AgentEvent` — single-direction state-change signals.
//!
//! **Every variant is a pure notification.** None of them ask the agent to do
//! anything — interceptable behavior (block / modify tool calls) lives in the
//! separate `register_before_tool_call` / `register_after_tool_call` hook API,
//! not here.

use std::path::PathBuf;

use crate::types::message::{AssistantMessage, ToolResultMessage};

/// All events emitted during an agent loop run.
#[derive(Debug, Clone)]
pub enum AgentEvent {
    // ── lifecycle ──
    AgentStart,
    AgentEnd,
    TurnStart,
    TurnEnd {
        message: AssistantMessage,
        tool_results: Vec<ToolResultMessage>,
    },

    // ── messages ──
    MessageStart,
    MessageUpdate {
        delta: ContentDelta,
    },
    MessageEnd {
        message: AssistantMessage,
    },

    // ── tool execution (notification only — interception is via hooks) ──
    ToolExecutionStart {
        tool_call_id: String,
        tool_name: String,
        args: serde_json::Value,
    },
    ToolExecutionEnd {
        tool_call_id: String,
        result: ToolResultMessage,
        is_error: bool,
    },

    // ── LLM calls ──
    BeforeProviderRequest {
        model: String,
    },
    AfterProviderResponse {
        model: String,
        response: AssistantMessage,
    },

    // ── session ──
    SessionStart {
        session_id: String,
        cwd: PathBuf,
    },
    SessionEnd {
        session_id: String,
    },

    // ── errors ──
    Error {
        message: String,
    },
}

/// A streamed delta. Emitted alongside `MessageUpdate`.
#[derive(Debug, Clone)]
pub enum ContentDelta {
    TextDelta(String),
    ThinkingDelta(String),
    ToolCallDelta {
        id: String,
        name: Option<String>,
        args_delta: String,
    },
}
