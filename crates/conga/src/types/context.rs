//! `AgentContext` / `AgentLoopConfig` — what the agent sees and how it runs.

use std::collections::HashMap;
use std::time::Duration;

use std::path::PathBuf;
use std::sync::Arc;

use crate::cancel::CancelSignal;
use crate::error::AgentError;
use crate::types::message::{AgentMessage, ModelId, StopReason};
use crate::types::session_event::SessionEvent;
use crate::types::tool::ToolDefinition;

/// Everything the agent sees for one run. Deliberately has **no plugin-shared
/// state field** — plugin-private state lives in files under `ToolContext.state_dir`.
#[derive(Clone)]
pub struct AgentContext {
    pub system_prompt: String,
    pub messages: Vec<crate::types::message::AgentMessage>,
    pub tools: Vec<ToolDefinition>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
    pub session_id: String,
}

impl std::fmt::Debug for AgentContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentContext")
            .field("system_prompt", &self.system_prompt)
            .field("messages_len", &self.messages.len())
            .field("tools_len", &self.tools.len())
            .field("cwd", &self.cwd)
            .field("session_id", &self.session_id)
            .finish()
    }
}

/// How a single agent loop invocation is configured.
#[derive(Clone)]
pub struct AgentLoopConfig {
    pub model: ModelSpec,
    /// Hard ceiling on outer-loop turns. Default 50.
    pub max_turns: usize,
    /// Hard ceiling on tool calls executed within a single turn. Default 20.
    pub max_tool_calls_per_turn: usize,
    /// Engine-level safety net for one tool call: a tool whose Future never
    /// resolves (wedged MCP server, deadlocked plugin) is cut off and
    /// reported as an error tool_result instead of hanging the whole loop.
    /// `None` (the default) imposes no engine limit — tools with their own
    /// timeouts (bash, fetch, MCP, external) keep governing themselves, and
    /// a tool may legitimately run longer than any fixed default. Hosts that
    /// want the net set it via `CONGA_TOOL_TIMEOUT_S`.
    pub tool_timeout: Option<Duration>,
    /// Cooperative abort: cancels the loop at the next safe point. Async
    /// waiters (SSE download, approval waits) are woken the instant
    /// [`CancelSignal::cancel`](crate::CancelSignal::cancel) fires - no polling.
    pub signal: Option<CancelSignal>,
    /// The LLM call entry point. Injected by the host so the loop is
    /// provider-agnostic and testable with a mock.
    pub stream_fn: Arc<dyn StreamFn>,
    /// Optional hook chain consulted around each tool call (block / modify /
    /// redact). `None` = no hooks (default — used by tests and `agent_loop`).
    pub hooks: Option<Arc<dyn crate::types::tool::HookChain>>,
    /// Retry policy for LLM calls that fail before any content is streamed
    /// (connection errors, non-2xx). Mid-stream failures are surfaced, not
    /// retried, to avoid duplicating partial output already emitted.
    pub retry: RetryPolicy,
    /// Optional persistence callback: called with each `SessionEvent` as it
    /// is produced, in crash-safe order (Assistant BEFORE any tool in it
    /// executes; each ToolResult after any hook rewriting). `None` = no
    /// persistence (bare `agent_loop` and tests). A persist `Err` aborts the
    /// run (fail loud - storage failures are never silently swallowed).
    #[allow(clippy::type_complexity)]
    pub persist: Option<Arc<dyn Fn(&SessionEvent) -> Result<(), AgentError> + Send + Sync>>,
    /// Mid-turn user input: transports push text onto this queue while the
    /// loop runs; the loop drains it at the top of each turn iteration and
    /// appends each item as a real `User` message (persisted like any
    /// other). `None` = no steering (bare `agent_loop` and tests).
    pub steer: Option<crate::steer::SteerQueue>,
    /// Optional transform applied to the message list before EVERY LLM
    /// call — the seam host compaction (or redaction, auditing) hooks
    /// into. Pure wire view: the loop's accumulator, returned messages,
    /// and persisted events always keep the full uncompacted history.
    /// `Err` fails the run loud (never silently swallowed).
    #[allow(clippy::type_complexity)]
    pub transform_context:
        Option<Arc<dyn Fn(&[AgentMessage]) -> Result<Vec<AgentMessage>, AgentError> + Send + Sync>>,
}

impl std::fmt::Debug for AgentLoopConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentLoopConfig")
            .field("model", &self.model.id)
            .field("max_turns", &self.max_turns)
            .field("max_tool_calls_per_turn", &self.max_tool_calls_per_turn)
            .finish_non_exhaustive()
    }
}

/// Retry policy for a single LLM call. Backoff is exponential:
/// `initial_delay_ms * 2^(attempt-1)`, capped at `max_delay_ms`. When
/// `jitter` is on, a bounded pseudo-random offset (± delay/4) is applied so
/// concurrent workers retried by the same clock don't thunder into the
/// provider in lockstep.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Max retries after the initial attempt (0 = no retry).
    pub max_retries: usize,
    /// Delay before the first retry, in milliseconds.
    pub initial_delay_ms: u64,
    /// Upper bound on backoff delay, in milliseconds.
    pub max_delay_ms: u64,
    /// Apply bounded jitter to every backoff (default true).
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            initial_delay_ms: 500,
            max_delay_ms: 8_000,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// No retries - for tests that want deterministic, fast failure.
    pub fn off() -> Self {
        Self {
            max_retries: 0,
            initial_delay_ms: 0,
            max_delay_ms: 0,
            jitter: false,
        }
    }
}

/// Loop tunables loadable from the environment (all optional, with defaults).
///
/// Recognized env vars:
/// - `CONGA_MAX_TURNS`        - outer-loop turn ceiling (default 50)
/// - `CONGA_MAX_TOOL_CALLS`   - per-turn tool-call ceiling (default 20)
/// - `CONGA_MAX_TOKENS`       - model output token cap (default 4096)
/// - `CONGA_RETRY_MAX`        - max LLM-call retries (default 2)
/// - `CONGA_RETRY_INITIAL_MS` - first retry backoff ms (default 500)
/// - `CONGA_TOOL_TIMEOUT_S`   - engine-level per-call tool timeout in
///   seconds (default: none)
///
/// Malformed values fall back to the default silently.
#[derive(Debug, Clone)]
pub struct AgentTunables {
    pub max_turns: usize,
    pub max_tool_calls_per_turn: usize,
    pub max_tokens: usize,
    pub retry: RetryPolicy,
    /// Engine-level tool timeout (see `AgentLoopConfig::tool_timeout`);
    pub tool_timeout_secs: Option<u64>,
}

impl AgentTunables {
    /// Read tunables from the process environment.
    pub fn from_env() -> Self {
        Self::from_env_with(&|k: &str| std::env::var(k))
    }

    /// Same as [`from_env`](Self::from_env) but with an injectable lookup -
    /// used by tests to avoid mutating process env.
    pub fn from_env_with(lookup: &dyn Fn(&str) -> Result<String, std::env::VarError>) -> Self {
        let default_retry = RetryPolicy::default();
        Self {
            max_turns: env_parse(lookup, "CONGA_MAX_TURNS", 50),
            max_tool_calls_per_turn: env_parse(lookup, "CONGA_MAX_TOOL_CALLS", 20),
            max_tokens: env_parse(lookup, "CONGA_MAX_TOKENS", 4096),
            tool_timeout_secs: lookup("CONGA_TOOL_TIMEOUT_S")
                .ok()
                .and_then(|s| s.parse().ok()),
            retry: RetryPolicy {
                max_retries: env_parse(lookup, "CONGA_RETRY_MAX", default_retry.max_retries),
                initial_delay_ms: env_parse(
                    lookup,
                    "CONGA_RETRY_INITIAL_MS",
                    default_retry.initial_delay_ms,
                ),
                max_delay_ms: env_parse(lookup, "CONGA_RETRY_MAX_MS", default_retry.max_delay_ms),
                jitter: default_retry.jitter,
            },
        }
    }
}

/// Parse `key` from the lookup as `T`, falling back to `default` on miss or
/// parse failure.
fn env_parse<T: std::str::FromStr>(
    lookup: &dyn Fn(&str) -> Result<String, std::env::VarError>,
    key: &str,
    default: T,
) -> T {
    lookup(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Specification of a model the host wants to talk to.
#[derive(Debug, Clone)]
pub struct ModelSpec {
    pub id: ModelId,
    pub api: ProviderApi,
    pub max_tokens: usize,
}

/// Which wire protocol the provider speaks. Determines `convert_to_llm` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderApi {
    /// OpenAI / DeepSeek / 智谱 / xAI / Groq / Ollama / vLLM / etc.
    OpenAiCompat,
    /// Anthropic native messages API.
    Anthropic,
}

/// A provider-agnostic stream chunk produced by `StreamFn`.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamChunk {
    TextDelta(String),
    ToolCallDelta {
        /// OpenAI-compat stream key for parallel tool calls
        /// (`tool_calls[].index`): first appearance opens a call,
        /// continuations repeat the index without id/name. `None` when the
        /// provider keys by id (Anthropic) or omits the field.
        index: Option<u32>,
        id: String,
        name: Option<String>,
        args_delta: String,
    },
    ThinkingDelta(String),
    Usage {
        input: u64,
        output: u64,
        /// Prompt tokens served from the provider cache; 0 = not reported.
        cache_read: u64,
        /// Prompt tokens written into the provider cache; 0 = not reported.
        cache_write: u64,
    },
    /// Provider-reported stop signal (OpenAI `finish_reason`, Anthropic
    /// `message_delta.stop_reason`), already mapped to the internal
    /// [`StopReason`]. Emitted at most once per stream, before `Done`.
    /// When present it overrides the loop's content-based stop guess, so a
    /// length-truncated response classifies as `MaxTokens` instead of a
    /// bogus tool call with malformed arguments.
    Stop(StopReason),
    Done,
    Error(String),
}

/// The LLM call entry point, injected into `AgentLoopConfig`.
///
/// Returns a boxed async iterator of [`StreamChunk`]. Providers implement this;
/// tests inject a mock that yields a canned chunk sequence.
pub trait StreamFn: Send + Sync {
    fn stream(
        &self,
        model: &ModelSpec,
        messages: &[crate::types::message::AgentMessage],
        system_prompt: &str,
        tools: &[ToolDefinition],
        signal: Option<CancelSignal>,
    ) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = StreamChunk> + Send>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::fake_env;

    #[test]
    fn tunables_default_when_unset() {
        let t = AgentTunables::from_env_with(&fake_env(&[]));
        assert_eq!(t.max_turns, 50);
        assert_eq!(t.max_tool_calls_per_turn, 20);
        assert_eq!(t.max_tokens, 4096);
        assert_eq!(t.retry.max_retries, 2);
    }

    #[test]
    fn tunables_parse_values() {
        let t = AgentTunables::from_env_with(&fake_env(&[
            ("CONGA_MAX_TURNS", "5"),
            ("CONGA_MAX_TOOL_CALLS", "8"),
            ("CONGA_MAX_TOKENS", "123"),
            ("CONGA_RETRY_MAX", "3"),
            ("CONGA_RETRY_INITIAL_MS", "100"),
            ("CONGA_RETRY_MAX_MS", "2000"),
        ]));
        assert_eq!(t.max_turns, 5);
        assert_eq!(t.max_tool_calls_per_turn, 8);
        assert_eq!(t.max_tokens, 123);
        assert_eq!(t.retry.max_retries, 3);
        assert_eq!(t.retry.initial_delay_ms, 100);
        assert_eq!(t.retry.max_delay_ms, 2000);
    }

    #[test]
    fn tunables_malformed_falls_back() {
        let t = AgentTunables::from_env_with(&fake_env(&[("CONGA_MAX_TURNS", "not-a-number")]));
        assert_eq!(t.max_turns, 50);
    }
}
