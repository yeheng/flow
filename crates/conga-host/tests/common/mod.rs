//! Deterministic fake `StreamFn` for offline integration tests (dev-only).

#![cfg(test)]

use std::collections::VecDeque;
use std::pin::Pin;

use conga::{AgentMessage, ModelSpec, StreamChunk, StreamFn, ToolDefinition};
use parking_lot::Mutex;

/// Each `stream()` call pops one script and yields its chunks in order.
/// The Nth call returns the Nth script, so a tool call is script 1
/// (`ToolCallDelta → Done`, triggers execution) and script 2 (`TextDelta →
/// Done`) closes the turn. Fully deterministic.
///
/// Script underflow panics rather than silently yielding `Done`: a silent
/// fallback would turn a mis-written test (or a retry the test didn't budget
/// for) into a false positive pass.
///
/// Every call also records the message list it was handed, so tests can
/// assert on the exact provider request the host assembled.
pub struct FakeStream {
    scripts: Mutex<VecDeque<Vec<StreamChunk>>>,
    seen: Mutex<Vec<Vec<AgentMessage>>>,
}

impl FakeStream {
    pub fn new(scripts: Vec<Vec<StreamChunk>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            seen: Mutex::new(Vec::new()),
        }
    }

    /// The message lists handed to each `stream()` call, in call order.
    pub fn seen(&self) -> Vec<Vec<AgentMessage>> {
        self.seen.lock().clone()
    }
}

impl StreamFn for FakeStream {
    fn stream(
        &self,
        _model: &ModelSpec,
        messages: &[AgentMessage],
        _system: &str,
        _tools: &[ToolDefinition],
        _signal: Option<conga::CancelSignal>,
    ) -> Pin<Box<dyn futures_util::Stream<Item = StreamChunk> + Send>> {
        self.seen.lock().push(messages.to_vec());
        let chunks = self.scripts.lock().pop_front().expect(
            "FakeStream: script underflow — test supplied fewer scripts than stream() calls",
        );
        Box::pin(futures_util::stream::iter(chunks))
    }
}
