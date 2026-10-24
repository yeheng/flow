//! `read` tool — read a file with optional offset/limit pagination.

use std::sync::Arc;

use conga::types::tool::{RiskLevel, ToolCallCtx, ToolDefinition, ToolResult};
use conga::ContentBlock;

/// Maximum bytes returned in one read, to keep tool results bounded.
const MAX_BYTES: usize = 2_000_000;

pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: "read".into(),
        label: "Read".into(),
        description: "Read a UTF-8 text file. Supports offset/limit by line.".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "File path relative to cwd (absolute allowed under ~/.conga)" },
                "offset": { "type": "integer", "description": "1-based start line (default 1)" },
                "limit": { "type": "integer", "description": "Max lines to return (default all)" }
            },
            "required": ["path"]
        }),
        risk: RiskLevel::Low,
        execute: Arc::new(|ctx| Box::pin(execute(ctx))),
    }
}

async fn execute(ctx: ToolCallCtx) -> Result<ToolResult, conga::error::ToolError> {
    if ctx.aborted() {
        return Ok(ToolResult::error("aborted".to_string()));
    }
    let path = ctx.args["path"]
        .as_str()
        .ok_or_else(|| conga::error::ToolError::Message("path is required".into()))?;
    let offset = ctx.args["offset"].as_u64().unwrap_or(1) as usize;
    let limit = ctx.args["limit"].as_u64().map(|l| l as usize);

    let full = match super::resolve_read_path(&ctx.ctx.cwd, path) {
        Ok(p) => p,
        Err(msg) => return Ok(ToolResult::error(msg)),
    };
    let bytes = tokio::fs::read(&full).await?;
    if bytes.len() > MAX_BYTES {
        return Ok(ToolResult::error(format!(
            "file too large ({} bytes > {} limit)",
            bytes.len(),
            MAX_BYTES
        )));
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let lines: Vec<&str> = text.lines().collect();
    // `offset` is 1-based (default 1 = first line); convert to a 0-based index.
    let start = offset.saturating_sub(1).min(lines.len());
    let end = match limit {
        Some(l) => (start + l).min(lines.len()),
        None => lines.len(),
    };
    let out: String = lines[start..end]
        .iter()
        .enumerate()
        .map(|(i, l)| format!("{:>6}\t{}", start + i + 1, l))
        .collect::<Vec<_>>()
        .join("\n");
    // Freeze oversize output at birth: spill to disk + head preview, so
    // the persisted event stores a bounded, immutable text — a read of a
    // near-2MB file must not become a mutable context blob that later
    // compaction rewrites (breaking the cache prefix every turn).
    let out = super::spill_or_truncate(&ctx, &out);

    Ok(ToolResult {
        content: vec![ContentBlock::text(if out.is_empty() {
            "(empty)".to_string()
        } else {
            out
        })],
        details: serde_json::json!({"path": path, "lines": end - start}),
        is_error: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::types::tool::ToolContext;
    use std::sync::atomic::AtomicBool;

    async fn run(args: serde_json::Value, cwd: &std::path::Path) -> ToolResult {
        let t = tool();
        (t.execute)(ToolCallCtx {
            tool_call_id: "x".into(),
            args,
            signal: Arc::new(AtomicBool::new(false)),
            ctx: ToolContext {
                cwd: cwd.to_path_buf(),
                env: Default::default(),
                session_id: "s".into(),
                state_dir: cwd.to_path_buf(),
            },
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn oversize_read_spills_to_disk() {
        // Freeze-at-birth: a read whose rendered output exceeds the
        // in-context cap is spilled to `<state_dir>/spill/` and replaced
        // by a head preview + path — the persisted tool result is final
        // and bounded, never a blob that later compaction rewrites.
        let tmp = tempfile::tempdir().unwrap();
        let big = "x".repeat(250_000);
        tokio::fs::write(tmp.path().join("big.txt"), &big)
            .await
            .unwrap();
        let r = run(serde_json::json!({"path": "big.txt"}), tmp.path()).await;
        let text = match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => panic!(),
        };
        assert!(
            text.contains("full output saved to"),
            "expected spill notice, got {} bytes: {}…",
            text.len(),
            &text[..80.min(text.len())]
        );
        assert!(tmp.path().join("spill").is_dir(), "spill dir must exist");
    }

    #[tokio::test]
    async fn reads_file_with_line_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("f.txt"), "a\nb\nc")
            .await
            .unwrap();
        let r = run(serde_json::json!({"path": "f.txt"}), tmp.path()).await;
        let text = match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => panic!(),
        };
        assert!(text.contains("1\ta"));
        assert!(text.contains("3\tc"));
    }

    #[tokio::test]
    async fn respects_offset_and_limit() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("f.txt"), "1\n2\n3\n4\n5")
            .await
            .unwrap();
        // offset is 1-based: offset=2 starts at line 2. limit=2 -> lines 2,3.
        let r = run(
            serde_json::json!({"path": "f.txt", "offset": 2, "limit": 2}),
            tmp.path(),
        )
        .await;
        let text = match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => panic!(),
        };
        assert!(text.contains("2"));
        assert!(text.contains("3"));
        assert!(!text.contains("1\t"));
        assert!(!text.contains("4"));
    }

    #[tokio::test]
    async fn aborts_on_signal() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("f.txt"), "secret")
            .await
            .unwrap();
        let t = tool();
        let r = (t.execute)(ToolCallCtx {
            tool_call_id: "x".into(),
            args: serde_json::json!({"path": "f.txt"}),
            signal: Arc::new(AtomicBool::new(true)),
            ctx: ToolContext {
                cwd: tmp.path().to_path_buf(),
                env: Default::default(),
                session_id: "s".into(),
                state_dir: tmp.path().to_path_buf(),
            },
        })
        .await
        .unwrap();
        assert!(r.is_error);
    }

    #[tokio::test]
    async fn rejects_path_escape() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("f.txt"), "x")
            .await
            .unwrap();
        // `..` that would go above cwd -> rejected.
        let r = run(serde_json::json!({"path": "../../etc/passwd"}), tmp.path()).await;
        assert!(r.is_error, "path escape must be rejected");
        // absolute path -> rejected.
        let r = run(serde_json::json!({"path": "/etc/passwd"}), tmp.path()).await;
        assert!(r.is_error, "absolute path must be rejected");
        // legitimate path still works.
        let r = run(serde_json::json!({"path": "f.txt"}), tmp.path()).await;
        assert!(!r.is_error, "normal path must still work");
    }
}
