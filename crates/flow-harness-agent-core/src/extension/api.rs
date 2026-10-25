//! `ExtensionApi` trait + `ExtensionApiImpl` - how extension crates register.
//!
//! Host/cli composition root calls `ext::register(&mut api)` for each linked
//! extension crate. See `docs/plugin-tutorial.md`.

use std::future::Future;
use std::pin::Pin;

use crate::types::message::ToolResultMessage;
use crate::types::tool::{RiskLevel, ToolCallVerdict, ToolDefinition};

/// A `before_tool_call` hook handler. Returns a verdict controlling flow.
pub trait BeforeToolCallHandler: Send + Sync {
    fn call(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        args: &serde_json::Value,
        risk: RiskLevel,
    ) -> ToolCallVerdict;
}

/// An `after_tool_call` hook handler. Returns a replacement result, or None to
/// leave it unchanged.
pub trait AfterToolCallHandler: Send + Sync {
    fn call(&self, tool_call_id: &str, result: &ToolResultMessage) -> Option<ToolResultMessage>;
}

/// The surface an extension crate uses to register capabilities with the agent.
///
/// **Events vs hooks are type-separated**: the agent loop emits events through
/// the `emit` closure passed to `run_agent_loop` (pure observation); hooks
/// registered here return a verdict / replacement that controls agent flow.
/// The two cannot be confused at the type level.
pub trait ExtensionApi: Send + Sync {
    /// Register a tool the LLM may call.
    fn register_tool(&mut self, tool: ToolDefinition);

    /// Register a `before_tool_call` hook (block / modify args).
    fn register_before_tool_call(&mut self, handler: Box<dyn BeforeToolCallHandler>);

    /// Register an `after_tool_call` hook (replace result).
    fn register_after_tool_call(&mut self, handler: Box<dyn AfterToolCallHandler>);
}

/// Concrete registry holding everything extension crates have registered.
#[derive(Default)]
pub struct ExtensionApiImpl {
    pub tools: Vec<ToolDefinition>,
    pub before_hooks: Vec<Box<dyn BeforeToolCallHandler>>,
    pub after_hooks: Vec<Box<dyn AfterToolCallHandler>>,
}

impl ExtensionApiImpl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run all `before_tool_call` hooks in registration order, combining their
    /// verdicts: the first `Block` wins; otherwise the last `Modify` wins;
    /// `Allow` is the default.
    pub fn before_tool_call(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        args: &serde_json::Value,
        risk: RiskLevel,
    ) -> ToolCallVerdict {
        let mut verdict = ToolCallVerdict::Allow;
        for h in &self.before_hooks {
            match h.call(tool_call_id, tool_name, args, risk) {
                v @ ToolCallVerdict::Block(_) => return v,
                v @ ToolCallVerdict::Modify(_) => verdict = v,
                ToolCallVerdict::Allow => {}
            }
        }
        verdict
    }

    /// Run all `after_tool_call` hooks in order, each may replace the result.
    pub fn after_tool_call(
        &self,
        tool_call_id: &str,
        result: &ToolResultMessage,
    ) -> ToolResultMessage {
        let mut current = result.clone();
        for h in &self.after_hooks {
            if let Some(replaced) = h.call(tool_call_id, &current) {
                current = replaced;
            }
        }
        current
    }
}

impl crate::types::tool::HookChain for ExtensionApiImpl {
    fn before_tool_call<'a>(
        &'a self,
        tool_call_id: &'a str,
        tool_name: &'a str,
        args: &'a serde_json::Value,
        risk: RiskLevel,
    ) -> Pin<Box<dyn Future<Output = ToolCallVerdict> + Send + 'a>> {
        Box::pin(async move {
            ExtensionApiImpl::before_tool_call(self, tool_call_id, tool_name, args, risk)
        })
    }

    fn after_tool_call(&self, tool_call_id: &str, result: &ToolResultMessage) -> ToolResultMessage {
        ExtensionApiImpl::after_tool_call(self, tool_call_id, result)
    }
}

impl ExtensionApi for ExtensionApiImpl {
    fn register_tool(&mut self, tool: ToolDefinition) {
        self.tools.push(tool);
    }

    fn register_before_tool_call(&mut self, handler: Box<dyn BeforeToolCallHandler>) {
        self.before_hooks.push(handler);
    }

    fn register_after_tool_call(&mut self, handler: Box<dyn AfterToolCallHandler>) {
        self.after_hooks.push(handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::message::{ContentBlock, ToolResultMessage};

    struct Blocker;
    impl BeforeToolCallHandler for Blocker {
        fn call(&self, _: &str, _: &str, _: &serde_json::Value, _: RiskLevel) -> ToolCallVerdict {
            ToolCallVerdict::Block("no".into())
        }
    }

    #[test]
    fn before_hook_block_wins() {
        let mut api = ExtensionApiImpl::new();
        api.register_before_tool_call(Box::new(Blocker));
        let v = api.before_tool_call("id", "bash", &serde_json::json!({}), RiskLevel::High);
        assert!(matches!(v, ToolCallVerdict::Block(_)));
    }

    #[test]
    fn after_hook_replaces_result() {
        struct Redactor;
        impl AfterToolCallHandler for Redactor {
            fn call(&self, _: &str, _: &ToolResultMessage) -> Option<ToolResultMessage> {
                Some(ToolResultMessage {
                    tool_call_id: "id".into(),
                    tool_name: "x".into(),
                    content: vec![ContentBlock::text("[REDACTED]")],
                    is_error: false,
                    timestamp: 0,
                })
            }
        }
        let mut api = ExtensionApiImpl::new();
        api.register_after_tool_call(Box::new(Redactor));
        let orig = ToolResultMessage {
            tool_call_id: "id".into(),
            tool_name: "x".into(),
            content: vec![ContentBlock::text("secret")],
            is_error: false,
            timestamp: 0,
        };
        let out = api.after_tool_call("id", &orig);
        match &out.content[0] {
            ContentBlock::Text { text } => assert_eq!(text, "[REDACTED]"),
            _ => panic!(),
        }
    }
}
