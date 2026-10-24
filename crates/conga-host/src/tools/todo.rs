//! `todo` tool — the agent's multi-step working memory.
//!
//! Private state under `ToolContext.state_dir` (one JSON file per session);
//! moved into the built-in set because a harness agent needs task tracking
//! in every transport, not behind a CLI feature flag.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use conga::{ContentBlock, RiskLevel, ToolCallCtx, ToolDefinition, ToolError, ToolResult};

#[derive(Default, Serialize, Deserialize, Clone)]
struct Todo {
    id: u64,
    text: String,
    done: bool,
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct State {
    todos: Vec<Todo>,
    next_id: u64,
}

fn load(ctx: &conga::ToolContext) -> State {
    let path = ctx.state_dir.join("todos.json");
    std::fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn save(ctx: &conga::ToolContext, state: &State) {
    let _ = std::fs::create_dir_all(&ctx.state_dir);
    if let Ok(bytes) = serde_json::to_vec(state) {
        let _ = std::fs::write(ctx.state_dir.join("todos.json"), bytes);
    }
}

pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: "todo".into(),
        label: "Todo".into(),
        description: "Track multi-step work: add items before starting, toggle them done as you finish, list to review progress. State persists per session.".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": ["add", "list", "toggle", "clear"], "description": "add: create a todo; list: show all; toggle: flip done by id; clear: remove all" },
                "text": { "type": "string", "description": "todo text (action=add)" },
                "id": { "type": "integer", "description": "todo id (action=toggle)" }
            },
            "required": ["action"]
        }),
        risk: RiskLevel::Low,
        execute: Arc::new(|ctx| Box::pin(execute(ctx))),
    }
}

async fn execute(ctx: ToolCallCtx) -> Result<ToolResult, ToolError> {
    let action = ctx.args["action"].as_str().unwrap_or("list");
    let mut state = load(&ctx.ctx);

    let (text, is_error) = match action {
        "add" => match ctx.args["text"].as_str() {
            Some(t) if !t.trim().is_empty() => {
                state.next_id += 1;
                let id = state.next_id;
                state.todos.push(Todo {
                    id,
                    text: t.to_string(),
                    done: false,
                });
                save(&ctx.ctx, &state);
                (format!("added #{id}: {t}"), false)
            }
            _ => ("text is required for action=add".to_string(), true),
        },
        "list" => {
            if state.todos.is_empty() {
                ("(no todos)".to_string(), false)
            } else {
                let lines: Vec<String> = state
                    .todos
                    .iter()
                    .map(|t| {
                        format!(
                            "#{} [{}] {}",
                            t.id,
                            if t.done { "done" } else { "    " },
                            t.text
                        )
                    })
                    .collect();
                (lines.join("\n"), false)
            }
        }
        "toggle" => match ctx.args["id"].as_u64() {
            Some(id) => match state.todos.iter_mut().find(|t| t.id == id) {
                Some(t) => {
                    t.done = !t.done;
                    let desc = format!(
                        "#{} {} -> {}",
                        t.id,
                        t.text,
                        if t.done { "done" } else { "open" }
                    );
                    save(&ctx.ctx, &state);
                    (desc, false)
                }
                None => (format!("no todo #{id}"), true),
            },
            None => ("id is required for action=toggle".to_string(), true),
        },
        "clear" => {
            state.todos.clear();
            save(&ctx.ctx, &state);
            ("cleared".to_string(), false)
        }
        _ => (format!("unknown action: {action}"), true),
    };

    Ok(ToolResult {
        content: vec![ContentBlock::text(text)],
        details: serde_json::json!({"action": action}),
        is_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::types::tool::ToolContext;
    use std::sync::atomic::AtomicBool;

    async fn run(args: serde_json::Value, state_dir: &std::path::Path) -> ToolResult {
        let t = tool();
        (t.execute)(ToolCallCtx {
            tool_call_id: "x".into(),
            args,
            signal: Arc::new(AtomicBool::new(false)),
            ctx: ToolContext {
                cwd: state_dir.to_path_buf(),
                env: Default::default(),
                session_id: "s".into(),
                state_dir: state_dir.to_path_buf(),
            },
        })
        .await
        .unwrap()
    }

    fn text_of(r: &ToolResult) -> String {
        match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => panic!("expected text"),
        }
    }

    #[tokio::test]
    async fn add_list_toggle_clear_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let r = run(
            serde_json::json!({"action": "add", "text": "first task"}),
            tmp.path(),
        )
        .await;
        assert!(!r.is_error);
        assert!(text_of(&r).contains("#1"));

        let r = run(
            serde_json::json!({"action": "add", "text": "second task"}),
            tmp.path(),
        )
        .await;
        assert!(text_of(&r).contains("#2"));

        let r = run(serde_json::json!({"action": "list"}), tmp.path()).await;
        let text = text_of(&r);
        assert!(text.contains("first task") && text.contains("second task"));

        let r = run(serde_json::json!({"action": "toggle", "id": 1}), tmp.path()).await;
        assert!(!r.is_error);
        assert!(text_of(&r).contains("done"));

        // State survives across calls (files under state_dir).
        let r = run(serde_json::json!({"action": "list"}), tmp.path()).await;
        assert!(text_of(&r).contains("[done]"));

        let r = run(serde_json::json!({"action": "clear"}), tmp.path()).await;
        assert!(!r.is_error);
        let r = run(serde_json::json!({"action": "list"}), tmp.path()).await;
        assert_eq!(text_of(&r), "(no todos)");
    }

    #[tokio::test]
    async fn add_requires_text() {
        let tmp = tempfile::tempdir().unwrap();
        let r = run(serde_json::json!({"action": "add"}), tmp.path()).await;
        assert!(r.is_error);
        assert!(text_of(&r).contains("text is required"));
    }

    #[tokio::test]
    async fn toggle_requires_known_id() {
        let tmp = tempfile::tempdir().unwrap();
        let r = run(
            serde_json::json!({"action": "toggle", "id": 99}),
            tmp.path(),
        )
        .await;
        assert!(r.is_error);
    }
}
