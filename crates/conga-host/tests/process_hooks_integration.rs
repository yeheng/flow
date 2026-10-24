//! End-to-end: a blocking project process hook must stop a tool call in
//! the REAL agent loop, composed in a HookStack exactly as assemble_host
//! composes it (extra gates first, permission policy last).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use conga::{
    AgentLoopConfig, AgentMessage, CancelSignal, ContentBlock, ModelSpec, ProviderApi, RetryPolicy,
    RiskLevel, StreamChunk, StreamFn, ToolDefinition, UserMessage,
};
use conga_host::hooks::HookStack;
use conga_host::permission::{Approver, Mode, PermissionPolicy};
use conga_host::ProcessHookChain;

/// Minimal stateful stream mock: the first call emits one assistant bash
/// tool call then Done; every later call emits plain text then Done, so
/// the loop stops cleanly after the blocked result instead of replaying
/// the call (which would loop to max_turns).
struct OneToolCallThenText {
    emitted_call: AtomicBool,
}

impl StreamFn for OneToolCallThenText {
    fn stream(
        &self,
        _model: &ModelSpec,
        _messages: &[AgentMessage],
        _system: &str,
        _tools: &[ToolDefinition],
        _signal: Option<CancelSignal>,
    ) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = StreamChunk> + Send>> {
        if self.emitted_call.swap(true, Ordering::SeqCst) {
            Box::pin(futures_util::stream::iter(vec![
                StreamChunk::TextDelta("stopped after the block".into()),
                StreamChunk::Done,
            ]))
        } else {
            Box::pin(futures_util::stream::iter(vec![
                StreamChunk::ToolCallDelta {
                    index: None,
                    id: "tc-1".into(),
                    name: Some("bash".into()),
                    args_delta: r#"{"command":"echo hi"}"#.into(),
                },
                StreamChunk::Done,
            ]))
        }
    }
}

#[tokio::test]
async fn blocking_process_hook_stops_tool_call_in_agent_loop() {
    // Hermetic HOME: `discover()` also loads a developer-global
    // `~/.conga/hooks.json` (via dirs::home_dir → $HOME); a global hook
    // matching `bash` would change the verdict under test. Pin $HOME to an
    // empty tempdir for this test's duration and restore it on every path.
    // (This binary holds exactly one test, so a parallel test racing on the
    // same env var is not a concern; the comment stays true while it does.)
    let _home_guard = tempdir_env_home(); // Drop restores $HOME (panic-safe)

    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join(".conga")).unwrap();
    std::fs::write(
        tmp.path().join(".conga/hooks.json"),
        r#"{"PreToolUse": [{"matcher": "bash", "hooks": [
            {"type": "command", "command": "echo 'blocked: test policy' >&2; exit 2"}
        ]}]}"#,
    )
    .unwrap();

    // Exactly the assemble_host composition: process chain first, policy last.
    let process =
        ProcessHookChain::discover(tmp.path()).expect("project hooks.json must be discovered");
    let approver: Approver = Arc::new(|_name: &str, _args: &serde_json::Value| {
        Box::pin(async { true }) // auto-approve: the policy is not under test
    });
    let policy = Arc::new(PermissionPolicy::new(Mode::AutoEdit, approver));
    let stack = HookStack::new(vec![process, policy]);

    // The tool records if its body ever runs; if it somehow does, it says so.
    let ran = Arc::new(AtomicBool::new(false));
    let ran_probe = Arc::clone(&ran);
    let bash = ToolDefinition {
        name: "bash".into(),
        label: "Bash".into(),
        description: "test".into(),
        parameters: serde_json::json!({"type": "object"}),
        risk: RiskLevel::Low,
        execute: Arc::new(move |_ctx: conga::ToolCallCtx| {
            let ran = Arc::clone(&ran_probe);
            Box::pin(async move {
                ran.store(true, Ordering::SeqCst);
                Ok(conga::ToolResult::error("MUST NOT RUN".to_string()))
            })
        }),
    };

    let config = AgentLoopConfig {
        model: ModelSpec {
            id: "mock".into(),
            api: ProviderApi::OpenAiCompat,
            max_tokens: 128,
        },
        max_turns: 5,
        max_tool_calls_per_turn: 5,
        tool_timeout: None,
        signal: None,
        stream_fn: Arc::new(OneToolCallThenText {
            emitted_call: AtomicBool::new(false),
        }),
        hooks: Some(Arc::new(stack)),
        retry: RetryPolicy::off(),
        persist: None,
        steer: None,
        transform_context: None,
    };
    let context = conga::AgentContext {
        system_prompt: "sys".into(),
        messages: vec![],
        tools: vec![bash],
        cwd: tmp.path().to_path_buf(),
        env: Default::default(),
        session_id: "process-hooks-it".into(),
    };
    let messages = conga::agent_loop(
        vec![AgentMessage::User(UserMessage {
            content: vec![ContentBlock::text("run it")],
            timestamp: conga::now(),
        })],
        context,
        config,
    )
    .await
    .unwrap();

    // The loop recorded exactly one tool result — the hook's Block reason,
    // not the tool's own output — and the tool body never ran.
    let tool_results: Vec<&conga::ToolResultMessage> = messages
        .iter()
        .filter_map(|m| match m {
            AgentMessage::ToolResult(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(tool_results.len(), 1, "messages: {messages:?}");
    assert!(
        tool_results[0].is_error,
        "blocked result is an error result"
    );
    let text = match &tool_results[0].content[0] {
        ContentBlock::Text { text } => text.clone(),
        other => panic!("expected text content, got {other:?}"),
    };
    assert!(
        text.contains("blocked: test policy"),
        "block reason must land as the persisted tool result, got: {text}"
    );
    assert!(
        !ran.load(Ordering::SeqCst),
        "tool body must never run when the hook blocks"
    );
}

/// Point `$HOME` at an empty tempdir until dropped; `Drop` restores the
/// original value (or removes it if unset), so panics unwind with the env
/// intact. Safe here because this test binary runs exactly one test — no
/// sibling race on the same var. (Edition 2021: `set_var` is safe.)
struct HomeGuard {
    _dir: tempfile::TempDir, // keeps the scratch dir alive; removed on drop
    prior: Option<std::ffi::OsString>,
}

impl Drop for HomeGuard {
    fn drop(&mut self) {
        match &self.prior {
            Some(h) => std::env::set_var("HOME", h),
            None => std::env::remove_var("HOME"),
        }
    }
}

fn tempdir_env_home() -> HomeGuard {
    // dirs::home_dir() reads $HOME on unix; an empty dir means no global
    // hooks.json, so discover() sees only the project file. Capture the
    // prior value BEFORE overwriting it.
    let prior = std::env::var_os("HOME");
    let dir = tempfile::tempdir().unwrap();
    std::env::set_var("HOME", dir.path());
    HomeGuard { _dir: dir, prior }
}
