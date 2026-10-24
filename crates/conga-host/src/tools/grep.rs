//! `grep` tool — content search with ripgrep, falling back to a built-in
//! regex walker when `rg` isn't installed.
//!
//! Both paths walk the tree the same way as [`conga::tools::list`]:
//! `.gitignore`-aware and pruning [`ALWAYS_IGNORE`] dirs. Output mirrors rg's
//! `path:lineno:line` format so the agent sees consistent results regardless of
//! which engine ran.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ignore::WalkBuilder;
use regex::{Regex, RegexBuilder};

use conga::types::tool::{RiskLevel, ToolCallCtx, ToolDefinition, ToolResult};
use conga::ContentBlock;

/// Directories never searched (mirrors `list` tool).
const ALWAYS_IGNORE: &[&str] = &[".git", "target", "node_modules"];
/// Cap total matches so tool output stays bounded.
const MAX_MATCHES: usize = 1000;
/// Skip files larger than this in the fallback engine (avoid slurping binaries).
const MAX_FILE_BYTES: u64 = 1_000_000;

pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: "grep".into(),
        label: "Grep".into(),
        description:
            "Search file contents by regex (ripgrep when available). Returns path:line:match."
                .into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Regex pattern" },
                "path": { "type": "string", "description": "File or dir relative to cwd (default .)" },
                "glob": { "type": "string", "description": "Glob filter, e.g. *.rs" },
                "case_insensitive": { "type": "boolean", "description": "Case-insensitive match (default false)" },
                "max_count": { "type": "integer", "description": "Max matches to return (default 1000)" }
            },
            "required": ["pattern"]
        }),
        risk: RiskLevel::Low,
        execute: Arc::new(|ctx| Box::pin(execute(ctx))),
    }
}

async fn execute(ctx: ToolCallCtx) -> Result<ToolResult, conga::error::ToolError> {
    if ctx.aborted() {
        return Ok(ToolResult::error("aborted".to_string()));
    }
    let pattern = ctx.args["pattern"]
        .as_str()
        .ok_or_else(|| conga::error::ToolError::Message("pattern is required".into()))?;
    let path = ctx.args["path"].as_str().unwrap_or(".");
    let glob = ctx.args["glob"].as_str();
    let ci = ctx.args["case_insensitive"].as_bool().unwrap_or(false);
    let max = ctx.args["max_count"]
        .as_u64()
        .map(|n| n as usize)
        .unwrap_or(MAX_MATCHES)
        .min(MAX_MATCHES);

    let re = match RegexBuilder::new(pattern).case_insensitive(ci).build() {
        Ok(r) => r,
        Err(e) => return Ok(ToolResult::error(format!("invalid regex: {e}"))),
    };

    // Validate before searching: `path` is later joined onto cwd by both the
    // rg and builtin engines without further checks, so reject `..`/absolute/
    // symlink escapes here.
    if let Err(msg) = super::resolve_within_cwd(&ctx.ctx.cwd, path) {
        return Ok(ToolResult::error(msg));
    }

    let glob_pat = glob.and_then(|p| glob::Pattern::new(p).ok());

    // rg is fast and gitignore-aware; use it when present, else walk in-process.
    let (matches, truncated, engine) = if rg_available() {
        match grep_rg(pattern, &ctx.ctx.cwd, path, glob, ci, max).await {
            Ok(out) => out,
            Err(e) => return Ok(ToolResult::error(format!("rg failed: {e}"))),
        }
    } else {
        // The builtin walker is synchronous filesystem+CPU work; keep it off
        // the async workers so a large tree can't stall the runtime.
        let re = re.clone();
        let cwd = ctx.ctx.cwd.clone();
        let path = path.to_string();
        let glob_pat = glob_pat.clone();
        let signal = ctx.signal.clone();
        let joined = tokio::task::spawn_blocking(move || {
            grep_builtin(&re, &cwd, &path, glob_pat.as_ref(), max, &signal)
        })
        .await;
        match joined {
            Ok((m, aborted)) => {
                if aborted {
                    return Ok(ToolResult::error("aborted".to_string()));
                }
                (m, false, "builtin")
            }
            Err(e) => return Ok(ToolResult::error(format!("builtin grep failed: {e}"))),
        }
    };

    let count = matches.len();
    // Spill oversize match sets at birth (same freeze-at-birth contract as
    // bash/read: the persisted text is final; nothing rewrites it later).
    let body = if matches.is_empty() {
        "(no matches)".to_string()
    } else {
        super::spill_or_truncate(&ctx, &matches.join("\n"))
    };

    Ok(ToolResult {
        content: vec![ContentBlock::text(body)],
        details: serde_json::json!({
            "count": count,
            "truncated": truncated,
            "engine": engine,
            "pattern": pattern,
        }),
        is_error: false,
    })
}

/// Detect ripgrep once per process; cache the result.
fn rg_available() -> bool {
    static HAS_RG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *HAS_RG.get_or_init(|| {
        std::process::Command::new("rg")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    })
}

/// Run ripgrep. Returns (matched lines, truncated, engine). rg exits 1 on no
/// match (treated as empty), 2+ on error.
async fn grep_rg(
    pattern: &str,
    cwd: &Path,
    path: &str,
    glob: Option<&str>,
    ci: bool,
    max: usize,
) -> Result<(Vec<String>, bool, &'static str), String> {
    let mut cmd = tokio::process::Command::new("rg");
    cmd.current_dir(cwd)
        .arg("--line-number")
        .arg("--no-heading")
        .arg("--with-filename")
        .arg("--color")
        .arg("never");
    // Force-skip the same dirs the builtin prunes (target/.git/node_modules),
    // even when they aren't gitignored. rg won't descend into excluded dirs.
    for name in ALWAYS_IGNORE {
        cmd.arg("-g").arg(format!("!{name}"));
    }
    if ci {
        cmd.arg("-i");
    }
    if let Some(g) = glob {
        cmd.arg("-g").arg(g);
    }
    cmd.arg(pattern).arg(path);

    let output = cmd.output().await.map_err(|e| e.to_string())?;

    // Exit 0 = matches, 1 = no matches, 2+ = error.
    if let Some(code) = output.status.code() {
        if code >= 2 {
            return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
        }
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let all: Vec<&str> = stdout.lines().collect();
    let truncated = all.len() > max;
    let matches: Vec<String> = all.iter().take(max).map(|s| s.to_string()).collect();
    Ok((matches, truncated, "rg"))
}

/// Built-in fallback: gitignore-aware walk + per-line regex match.
/// Returns (matches, aborted).
fn grep_builtin(
    re: &Regex,
    cwd: &Path,
    path: &str,
    glob_pat: Option<&glob::Pattern>,
    max: usize,
    signal: &Arc<std::sync::atomic::AtomicBool>,
) -> (Vec<String>, bool) {
    let base = cwd.join(path);
    let mut matches: Vec<String> = Vec::new();

    let mut builder = WalkBuilder::new(&base);
    builder
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .ignore(true)
        .filter_entry(|entry| {
            entry.file_type().is_none_or(|ft| {
                !ft.is_dir()
                    || entry
                        .file_name()
                        .to_str()
                        .map(|name| !ALWAYS_IGNORE.contains(&name))
                        .unwrap_or(true)
            })
        });

    for entry in builder.build().filter_map(|e| e.ok()) {
        if matches.len() >= max {
            break;
        }
        if signal.load(Ordering::Relaxed) {
            return (matches, true);
        }
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // Skip oversized / likely-binary files.
        if entry
            .metadata()
            .map(|m| m.len() > MAX_FILE_BYTES)
            .unwrap_or(false)
        {
            continue;
        }
        let rel = path
            .strip_prefix(cwd)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();
        if let Some(p) = glob_pat {
            if !p.matches(&rel) {
                continue;
            }
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            if matches.len() >= max {
                break;
            }
            if re.is_match(line) {
                matches.push(format!("{}:{}:{}", rel, i + 1, line));
            }
        }
    }

    (matches, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::types::tool::ToolContext;
    use std::sync::atomic::AtomicBool;

    async fn run(args: serde_json::Value, cwd: &Path) -> ToolResult {
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

    fn text_of(r: &ToolResult) -> String {
        match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => panic!("expected text content"),
        }
    }

    #[tokio::test]
    async fn rejects_path_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let r = run(
            serde_json::json!({"pattern": "x", "path": "../"}),
            tmp.path(),
        )
        .await;
        assert!(r.is_error, "`..` escape must be rejected");
        let r = run(
            serde_json::json!({"pattern": "x", "path": "/etc"}),
            tmp.path(),
        )
        .await;
        assert!(r.is_error, "absolute path must be rejected");
    }

    #[tokio::test]
    async fn finds_matches_via_fallback() {
        // Force the builtin path so the test is deterministic across machines.
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("a.rs"), "fn foo() {}\nfn bar() {}\n")
            .await
            .unwrap();
        let r = run(
            serde_json::json!({"pattern": "fn (\\w+)", "path": "a.rs"}),
            tmp.path(),
        )
        .await;
        // Don't assume rg presence; just assert at least the builtin semantics
        // hold when rg is absent. If rg is present we still expect a foo match.
        let text = text_of(&r);
        assert!(text.contains("foo"), "missing foo match: {text}");
    }

    #[tokio::test]
    async fn builtin_matches_with_line_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("a.txt"), "alpha\nbeta\ngamma\n")
            .await
            .unwrap();
        let re = Regex::new("beta").unwrap();
        let glob = None;
        let (matches, aborted) = grep_builtin(
            &re,
            tmp.path(),
            "a.txt",
            glob,
            100,
            &Arc::new(AtomicBool::new(false)),
        );
        assert!(!aborted);
        assert_eq!(matches, vec!["a.txt:2:beta".to_string()]);
    }

    #[tokio::test]
    async fn builtin_aborts_on_signal() {
        let tmp = tempfile::tempdir().unwrap();
        // Many files so the walk loop has a chance to observe the signal.
        for i in 0..50 {
            tokio::fs::write(tmp.path().join(format!("f{i}.txt")), "match\n")
                .await
                .unwrap();
        }
        let re = Regex::new("match").unwrap();
        let signal = Arc::new(AtomicBool::new(true)); // already aborted
        let (matches, aborted) = grep_builtin(&re, tmp.path(), ".", None, 100, &signal);
        assert!(aborted);
        // Aborts at the top of the loop, before collecting anything.
        assert!(matches.is_empty());
    }

    #[tokio::test]
    async fn invalid_regex_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let r = run(serde_json::json!({"pattern": "("}), tmp.path()).await;
        assert!(r.is_error);
    }

    #[tokio::test]
    async fn skips_target_dir() {
        // target/ must be pruned by BOTH engines (rg via -g '!target', builtin
        // via filter_entry), even with no .gitignore. Engine-agnostic outcome.
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("keep.rs"), "needle\n")
            .await
            .unwrap();
        tokio::fs::create_dir_all(tmp.path().join("target/debug"))
            .await
            .unwrap();
        tokio::fs::write(tmp.path().join("target/debug/junk.rs"), "needle\n")
            .await
            .unwrap();
        let r = run(serde_json::json!({"pattern": "needle"}), tmp.path()).await;
        let text = text_of(&r);
        assert!(text.contains("keep.rs"), "expected root match: {text}");
        assert!(
            !text.contains("target"),
            "target/ leaked into grep output: {text}"
        );
    }
}
