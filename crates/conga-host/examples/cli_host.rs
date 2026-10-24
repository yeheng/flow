//! Minimal host: read user input, run the agent loop, print streamed tokens.
//!
//! Demonstrates "use conga to do something" - the full extent of wiring a
//! host needs.
//!
//! Config via env (load a `.env` with `dotenvy`, or export these):
//!   CONGA_LLM_BASE_URL - provider base URL (e.g. https://api.deepseek.com/v1)
//!   CONGA_LLM_KEY      - API key
//!   CONGA_LLM_MODEL    - model id
//!   CONGA_LLM_API      - "openai" (default) or "anthropic"
//!   CONGA_MAX_TURNS / CONGA_MAX_TOOL_CALLS / CONGA_MAX_TOKENS - loop knobs
//!   CONGA_RETRY_*      - retry policy (max / initial_ms / max_ms)
//!
//! Without CONGA_LLM_KEY this prints a canned reply (smoke test of plumbing).

use std::sync::Arc;

use conga::{
    agent_loop, AgentContext, AgentMessage, AgentTunables, AnthropicProvider, ContentBlock,
    ModelSpec, OpenAiCompat, ProviderApi, StreamChunk, StreamFn, UserMessage,
};
use futures_util::Stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env if present (host responsibility; real env vars take precedence).
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let prompt = std::env::args().nth(1).unwrap_or_else(|| "Hello!".into());

    let cwd = std::env::current_dir()?;
    let context = AgentContext {
        system_prompt: "You are a helpful, concise assistant.".into(),
        messages: vec![],
        tools: conga_host::built_in_tools(),
        cwd,
        env: std::env::vars().collect(),
        session_id: uuid::Uuid::new_v4().to_string(),
    };

    let user_msg = AgentMessage::User(UserMessage {
        content: vec![ContentBlock::text(prompt)],
        timestamp: conga::now(),
    });

    // Loop tunables from env (max_turns / max_tokens / thinking / retry ...).
    let tunables = AgentTunables::from_env();

    // Provider connection from env: CONGA_LLM_BASE_URL / KEY / MODEL / API / *_PROXY.
    // cfg.api picks the provider impl. Falls back to a mock if not configured.
    let (model, stream_fn): (ModelSpec, Arc<dyn StreamFn>) = match conga::ProviderConfig::from_env()
    {
        Ok(cfg) => {
            let stream: Arc<dyn StreamFn> = match cfg.api {
                ProviderApi::OpenAiCompat => Arc::new(OpenAiCompat::from_config(&cfg)),
                ProviderApi::Anthropic => Arc::new(AnthropicProvider::from_config(&cfg)),
            };
            let model = ModelSpec {
                id: cfg.model.clone(),
                api: cfg.api,
                max_tokens: tunables.max_tokens,
            };
            (model, stream)
        }
        Err(e) => {
            println!("(no LLM config: {e}; using mock reply)\n");
            (
                ModelSpec {
                    id: "mock".into(),
                    api: ProviderApi::OpenAiCompat,
                    max_tokens: tunables.max_tokens,
                },
                Arc::new(MockStream),
            )
        }
    };

    // Persist every Assistant/ToolResult as it happens (assistant hits the
    // log before any tool in it executes), then frame the turn.
    let session_id = context.session_id.clone();
    let store = conga::EventStorage::new(conga::JsonlStorage::default_root().base_dir_clone());
    let persist_store = store.clone();
    let persist_sid = session_id.clone();
    let config = conga::AgentLoopConfig {
        model,
        max_turns: tunables.max_turns,
        max_tool_calls_per_turn: tunables.max_tool_calls_per_turn,
        tool_timeout: tunables
            .tool_timeout_secs
            .map(std::time::Duration::from_secs),
        signal: Some(conga::CancelSignal::new()),
        stream_fn,
        hooks: None,
        retry: tunables.retry,
        persist: Some(Arc::new(move |ev: &conga::SessionEvent| {
            let store = persist_store.clone();
            let sid = persist_sid.clone();
            let handle = tokio::runtime::Handle::current();
            std::thread::scope(|s| {
                s.spawn(move || handle.block_on(store.append_event(&sid, ev)))
                    .join()
                    .expect("persist bridge thread panicked")
            })
        })),
        steer: None,
        transform_context: None,
    };
    store
        .append_event(&session_id, &conga::SessionEvent::TurnStart)
        .await?;
    store
        .append_event(&session_id, &conga::SessionEvent::User(user_msg.clone()))
        .await?;

    let msgs = agent_loop(vec![user_msg], context, config).await?;

    // Print the final assistant text (streaming already printed deltas in a
    // real host; here we just surface the assembled message).
    for m in &msgs {
        if let AgentMessage::Assistant(a) = m {
            for b in &a.content {
                if let ContentBlock::Text { text } = b {
                    print!("{text}");
                }
            }
        }
    }
    println!();
    store
        .append_event(
            &session_id,
            &conga::SessionEvent::TurnEnd {
                reason: conga::TurnEndReason::Completed,
            },
        )
        .await?;
    eprintln!(
        "(session {} saved: {})",
        session_id,
        store.events_path(&session_id).display()
    );
    Ok(())
}

/// A no-network mock that emits a single canned line - proves the loop runs.
struct MockStream;
impl StreamFn for MockStream {
    fn stream(
        &self,
        _model: &conga::ModelSpec,
        _messages: &[AgentMessage],
        _system: &str,
        _tools: &[conga::ToolDefinition],
        _signal: Option<conga::CancelSignal>,
    ) -> std::pin::Pin<Box<dyn Stream<Item = StreamChunk> + Send>> {
        Box::pin(futures_util::stream::iter(vec![
            StreamChunk::TextDelta("(mock) conga loop ran successfully.".into()),
            StreamChunk::Done,
        ]))
    }
}
