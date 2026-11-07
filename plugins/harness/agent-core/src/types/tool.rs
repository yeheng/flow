//! `ToolDefinition` / `ToolFn` / `ToolContext` / `ToolResult` + hook verdicts.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::error::ToolError;
use crate::types::message::{ContentBlock, ToolResultMessage};

/// Risk level of a tool call - determines whether the host auto-approves,
/// prompts, or blocks based on the active permission mode. Lives on
/// [`ToolDefinition`] so the agent loop can forward it to hooks without a
/// hardcoded name table.
///
/// Grading rule: a tool that can execute arbitrary code or commands is
/// **always `High`** — no exceptions, no "but it's sandboxed". `bash` and
/// the PTY `terminal` tool must never diverge: a lower grade on either is
/// a privilege-escalation hole (the model will learn to prefer the tool
/// that skips the approver).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RiskLevel {
    Low,
    Medium,
    #[default]
    High,
}

/// A tool registered with the agent. `parameters` is a JSON Schema; the host
/// validates args before calling `execute`.
#[derive(Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub label: String,
    pub description: String,
    pub parameters: serde_json::Value,
    pub risk: RiskLevel,
    pub execute: ToolFn,
}

impl std::fmt::Debug for ToolDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolDefinition")
            .field("name", &self.name)
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

/// The signature every tool's `execute` closure must match.
///
/// No `on_update` callback (V0.1 omits streaming tool progress — no consumer
/// exists among the 5 built-in tools).
pub type ToolFn = Arc<
    dyn Fn(ToolCallCtx) -> Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send>>
        + Send
        + Sync,
>;

/// Arguments handed to a tool invocation.
#[derive(Debug, Clone)]
pub struct ToolCallCtx {
    pub tool_call_id: String,
    pub args: serde_json::Value,
    pub signal: Arc<AtomicBool>,
    pub ctx: ToolContext,
}

impl ToolCallCtx {
    /// True if the caller has requested this invocation be cancelled. Tools
    /// should check this at entry and inside long loops, returning promptly
    /// (e.g. an "aborted" error) when set.
    pub fn aborted(&self) -> bool {
        self.signal.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Context passed into a tool. `state_dir` is this plugin's **private** state
/// directory (`~/.conga/sessions/{session_id}/tool_state/{tool_name}/`); the
/// tool reads/writes its own files there.
#[derive(Clone)]
pub struct ToolContext {
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
    pub session_id: String,
    pub state_dir: PathBuf,
}

impl std::fmt::Debug for ToolContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolContext")
            .field("cwd", &self.cwd)
            .field("env", &self.env)
            .field("session_id", &self.session_id)
            .field("state_dir", &self.state_dir)
            .finish()
    }
}

/// A tool's result. `details` is plugin-private (the agent never reads it).
#[derive(Debug, Clone)]
pub struct ToolResult {
    pub content: Vec<ContentBlock>,
    pub details: serde_json::Value,
    pub is_error: bool,
}

impl ToolResult {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            content: vec![ContentBlock::text(s)],
            details: serde_json::Value::Null,
            is_error: false,
        }
    }

    pub fn error(s: impl Into<String>) -> Self {
        Self {
            content: vec![ContentBlock::text(s)],
            details: serde_json::Value::Null,
            is_error: true,
        }
    }
}

/// Verdict returned by a `before_tool_call` hook — controls whether/how a tool
/// call proceeds.
#[derive(Debug, Clone)]
pub enum ToolCallVerdict {
    /// Let the call through unchanged.
    Allow,
    /// Refuse the call; `reason` becomes the ToolResult sent back to the LLM.
    Block(String),
    /// Replace the args, then execute.
    Modify(serde_json::Value),
}

/// Object-safe hook chain the agent loop consults around each tool call.
///
/// `before_tool_call` is async because hosts may need to ask a human for
/// approval (CLI: stdin; gateway: WebSocket round-trip). `after_tool_call`
/// stays sync — it is a pure transformation (redact etc.).
///
/// Cancellation contract: while the agent loop is suspended in
/// `before_tool_call().await`, an abort signal does NOT automatically cancel
/// the future. An implementor that may block on a human must check the abort
/// signal itself (or accept a cancel channel) and return promptly when set.
///
/// Defined in `types` (not `extension`) so `AgentLoopConfig` can hold an
/// `Option<Arc<dyn HookChain>>` without a circular dependency. The concrete
/// implementation is `ExtensionApiImpl`; `None` means "no hooks installed"
/// (the default — used by tests and the bare `agent_loop` helper).
pub trait HookChain: Send + Sync {
    /// Consult all `before_tool_call` handlers. First `Block` wins; otherwise
    /// the last `Modify` wins; default `Allow`.
    fn before_tool_call<'a>(
        &'a self,
        tool_call_id: &'a str,
        tool_name: &'a str,
        args: &'a serde_json::Value,
        risk: RiskLevel,
    ) -> Pin<Box<dyn Future<Output = ToolCallVerdict> + Send + 'a>>;

    /// Consult all `after_tool_call` handlers, each may replace the result.
    fn after_tool_call(&self, tool_call_id: &str, result: &ToolResultMessage) -> ToolResultMessage;
}
