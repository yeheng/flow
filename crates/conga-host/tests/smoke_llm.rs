//! 端到端冒烟测试：需要真实 LLM 配置（CONGA_LLM_*），默认 #[ignore]。
//!
//! 运行方式（需 .env 或环境变量已配置）：
//! ```sh
//! set -a && source .env && set +a
//! cargo test -p conga-host --test smoke_llm -- --ignored --nocapture
//! ```
//!
//! 走与 CLI 完全相同的 `Host::run_turn` 路径（真实 provider stream_fn），
//! 只是不经过 reedline/TTY。会话写入 tempdir，不污染 `~/.conga/sessions`。

#![cfg(test)]

use std::sync::Arc;

use conga::AgentMessage;
use conga_host::{ConfigLoader, EventPrinter, Host, Mode, PermissionPolicy, SessionManager};

/// 构造一次完整的 `Host::run_turn`，验证 ConfigLoader + Host + EventPrinter
/// 端到端能跑通（真实 LLM）。
#[tokio::test]
#[ignore]
async fn end_to_end_basic_chat() {
    let cfg = ConfigLoader::load().expect("CONGA_LLM_* must be set");
    let tmp = tempfile::tempdir().unwrap();

    // FullAuto 模式，避免 approver 阻塞（无 stdin）。
    let host = Host::new(
        cfg,
        SessionManager::with_root(tmp.path().to_path_buf()),
        Arc::new(PermissionPolicy::new(
            Mode::FullAuto,
            Arc::new(|_, _| Box::pin(async { false })),
        )),
        "You are a test assistant. Follow instructions exactly.".into(),
        conga_host::built_in_tools(),
    )
    .with_max_turns(3);

    let mut buf: Vec<u8> = Vec::new();
    let summary = host
        .run_turn("Reply with exactly: pong", |ev| {
            EventPrinter::new(&mut buf).on_event(&ev);
        })
        .await
        .expect("agent loop should complete");
    let new_msgs = &summary.new_messages;

    // 至少有一条 assistant 消息。
    assert!(
        new_msgs
            .iter()
            .any(|m| matches!(m, AgentMessage::Assistant(_))),
        "expected at least one assistant message, got: {new_msgs:?}"
    );

    // EventPrinter 应该输出了点什么（至少 usage 或文本）。
    let out = String::from_utf8_lossy(&buf);
    assert!(!out.is_empty(), "printer produced no output");
    eprintln!("--- printer output ---\n{out}");
}

/// 冒烟测试 3：工具调用。FullAuto 模式下让 LLM 调用 `list` 工具。
#[tokio::test]
#[ignore]
async fn end_to_end_tool_call() {
    let cfg = ConfigLoader::load().expect("CONGA_LLM_* must be set");
    let tmp = tempfile::tempdir().unwrap();

    let host = Host::new(
        cfg,
        SessionManager::with_root(tmp.path().to_path_buf()),
        Arc::new(PermissionPolicy::new(
            Mode::FullAuto,
            Arc::new(|_, _| Box::pin(async { false })),
        )),
        "You are a test assistant. You MUST use tools when asked to.".into(),
        conga_host::built_in_tools(),
    )
    .with_max_turns(3);

    let summary = host
        .run_turn(
            "Use the `list` tool to list the current directory, then reply with the count of entries in one sentence.",
            |_| {},
        )
        .await
        .expect("agent loop should complete");
    let new_msgs = &summary.new_messages;

    assert!(
        new_msgs
            .iter()
            .any(|m| matches!(m, AgentMessage::ToolResult { .. })),
        "expected a tool result after the list call, got: {:?}",
        new_msgs
    );
    assert!(
        new_msgs
            .iter()
            .any(|m| matches!(m, AgentMessage::Assistant(_))),
        "expected a closing assistant message"
    );
}
