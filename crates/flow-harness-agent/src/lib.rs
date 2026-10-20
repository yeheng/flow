//! flow-harness-agent：harness 节点 agent 模式的 runner。
//!
//! 对 conga-host headless 装配入口（`conga_host::headless`）的直接调用：
//! settings.json 永不读、API key 只经 stdin 进结构体（任何环境变量都不
//! 携带）、workdir 的 `.env`/`.conga/config.toml` 不被装配（headless 不调
//! dotenvy/config 发现）。本 crate 不依赖任何 flow crate——与 executor 的
//! 契约只有三样：`CONGA_*` 非敏感 env、stdin 的单行 JSON、stdout 的单行
//! JSON + 退出码（0 完成 / 1 回合失败 / 130 中止 / 2 装配或输入错误）。

use conga::{AgentMessage, ContentBlock, StopReason, TurnEndReason};
use conga_host::TurnSummary;

/// stdin 载荷：密钥真值、任务文本与 agent 工作根（canonical 后的绝对路径）。
/// 不落盘、不进 journal；`workdir` 经 `headless_prepare` 成为 `CONGA_PROJECT_DIR`。
#[derive(Debug, serde::Deserialize)]
pub struct RunnerInput {
    pub api_key: String,
    pub prompt: String,
    pub workdir: std::path::PathBuf,
}

/// stdout 单行结果。成功/失败都尽力输出，executor 按行解析。
#[derive(Debug, serde::Serialize)]
pub struct RunnerOutcome {
    pub text: String,
    pub stop_reason: String,
    pub error: Option<String>,
    #[serde(skip)]
    pub exit_code: i32,
}

/// 最终文本上限。flow 派生期按 `body_raw > 8 MiB` 拒绝（flow-executor
/// task.rs），8 MiB 文本 + 截断标记会恰好越过那条线；留出标记与余量。
pub const TEXT_LIMIT_BYTES: usize = 7 * 1024 * 1024 + 512 * 1024;
const TRUNCATED_MARKER: &str = "\n[truncated]";

/// 把一次 `run_turn` 的结果投影成单行输出与退出码。
///
/// 成功判定（R5）：`Completed` 且末条 assistant 的 stop_reason 为
/// `EndTurn`；末条 `ToolUse` = max_turns 耗尽（失败，末轮工具副作用已
/// 发生）；中间 `MaxTokens` 不单独判死——只看**末条** assistant。
pub fn summarize(summary: &TurnSummary) -> RunnerOutcome {
    let last_assistant = summary.new_messages.iter().rev().find_map(|m| match m {
        AgentMessage::Assistant(a) => Some(a),
        _ => None,
    });
    let (text, stop_reason) = match last_assistant {
        Some(message) => {
            let joined = message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            let reason = match &message.stop_reason {
                StopReason::EndTurn => "end_turn",
                StopReason::ToolUse => "tool_use",
                StopReason::MaxTokens => "max_tokens",
                StopReason::Error(_) => "error",
                StopReason::Aborted => "aborted",
            };
            (joined, reason.to_string())
        }
        None => (String::new(), "no_assistant_message".to_string()),
    };
    let text = cap_text(text);
    match &summary.reason {
        TurnEndReason::Completed => {
            let ok = matches!(
                last_assistant.map(|m| &m.stop_reason),
                Some(StopReason::EndTurn)
            );
            if ok {
                RunnerOutcome {
                    text,
                    stop_reason: stop_reason.clone(),
                    error: None,
                    exit_code: 0,
                }
            } else if stop_reason == "tool_use" {
                RunnerOutcome {
                    text,
                    stop_reason: stop_reason.clone(),
                    error: Some(
                        "max turns exceeded (last assistant stopped on tool_use); \
                                side effects of the final turn's tools have already happened"
                            .into(),
                    ),
                    exit_code: 1,
                }
            } else {
                let detail =
                    format!("turn completed without end_turn (last stop_reason: {stop_reason})");
                RunnerOutcome {
                    text,
                    stop_reason,
                    error: Some(detail),
                    exit_code: 1,
                }
            }
        }
        TurnEndReason::Error { message } => RunnerOutcome {
            text,
            stop_reason: stop_reason.clone(),
            error: Some(message.clone()),
            exit_code: 1,
        },
        // 防御性：flow 的取消/超时走进程组 SIGKILL（executor 侧 Uncertain），
        // 不依赖这条路径。
        TurnEndReason::Aborted { .. } => RunnerOutcome {
            text,
            stop_reason: stop_reason.clone(),
            error: Some("turn aborted".into()),
            exit_code: 130,
        },
    }
}

/// 派生期 8 MiB 检查之前的自我截断：在字符边界截断并写标记，把「输出
/// 过大」变成带标记的可见结果，而不是 journal 侧的 Business。
fn cap_text(text: String) -> String {
    if text.len() <= TEXT_LIMIT_BYTES {
        return text;
    }
    let mut cut = TEXT_LIMIT_BYTES;
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    let mut capped = text.into_bytes();
    capped.truncate(cut);
    let mut capped = String::from_utf8_lossy(&capped).into_owned();
    capped.push_str(TRUNCATED_MARKER);
    capped
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::types::message::{AssistantMessage, FunctionCall};

    fn assistant(stop: StopReason, text: &str) -> AgentMessage {
        AgentMessage::Assistant(AssistantMessage {
            content: vec![ContentBlock::text(text)],
            model: "test-model".into(),
            stop_reason: stop,
            usage: None,
            timestamp: 0,
            stream_indices: Vec::new(),
        })
    }

    fn summary(reason: TurnEndReason, messages: Vec<AgentMessage>) -> TurnSummary {
        TurnSummary {
            reason,
            new_messages: messages,
        }
    }

    #[test]
    fn end_turn_is_success_with_text() {
        let s = summary(
            TurnEndReason::Completed,
            vec![
                assistant(StopReason::ToolUse, "I'll check."),
                assistant(StopReason::EndTurn, "final answer"),
            ],
        );
        let out = summarize(&s);
        assert_eq!(out.exit_code, 0);
        assert_eq!(out.text, "final answer");
        assert_eq!(out.stop_reason, "end_turn");
        assert!(out.error.is_none());
    }

    #[test]
    fn last_tool_use_means_max_turns_exhausted() {
        let s = summary(
            TurnEndReason::Completed,
            vec![assistant(StopReason::ToolUse, "running more tools")],
        );
        let out = summarize(&s);
        assert_eq!(out.exit_code, 1);
        assert_eq!(out.stop_reason, "tool_use");
        assert!(out.error.as_deref().unwrap().contains("max turns"));
    }

    #[test]
    fn mid_turn_max_tokens_then_end_turn_still_succeeds() {
        let s = summary(
            TurnEndReason::Completed,
            vec![
                assistant(StopReason::MaxTokens, "partial..."),
                assistant(StopReason::ToolUse, "next"),
                assistant(StopReason::EndTurn, "done"),
            ],
        );
        assert_eq!(summarize(&s).exit_code, 0);
    }

    #[test]
    fn trailing_max_tokens_is_not_silently_successful() {
        let s = summary(
            TurnEndReason::Completed,
            vec![assistant(StopReason::MaxTokens, "cut off")],
        );
        let out = summarize(&s);
        assert_eq!(out.exit_code, 1);
        assert_eq!(out.stop_reason, "max_tokens");
    }

    #[test]
    fn error_and_aborted_map_to_their_exit_codes() {
        let s = summary(
            TurnEndReason::Error {
                message: "provider 500".into(),
            },
            vec![],
        );
        let out = summarize(&s);
        assert_eq!(out.exit_code, 1);
        assert_eq!(out.error.as_deref(), Some("provider 500"));

        let s = summary(TurnEndReason::Aborted { cause: None }, vec![]);
        assert_eq!(summarize(&s).exit_code, 130);
    }

    #[test]
    fn text_blocks_are_joined_and_tool_blocks_ignored() {
        let message = AssistantMessage {
            content: vec![
                ContentBlock::text("part one "),
                ContentBlock::ToolCall {
                    tool_call: conga::types::message::ToolCall {
                        id: "t1".into(),
                        function: FunctionCall {
                            name: "bash".into(),
                            arguments: "{}".into(),
                        },
                    },
                },
                ContentBlock::text("part two"),
            ],
            model: "m".into(),
            stop_reason: StopReason::EndTurn,
            usage: None,
            timestamp: 0,
            stream_indices: Vec::new(),
        };
        let s = summary(
            TurnEndReason::Completed,
            vec![AgentMessage::Assistant(message)],
        );
        let out = summarize(&s);
        assert_eq!(out.text, "part one part two");
        assert_eq!(out.exit_code, 0);
    }

    #[test]
    fn oversized_text_is_truncated_with_marker_under_the_derive_limit() {
        let big = "汉".repeat(TEXT_LIMIT_BYTES); // 每字符 3 字节
        let s = summary(
            TurnEndReason::Completed,
            vec![assistant(StopReason::EndTurn, &big)],
        );
        let out = summarize(&s);
        assert!(out.text.ends_with(TRUNCATED_MARKER), "{}", out.text.len());
        assert!(out.text.len() <= TEXT_LIMIT_BYTES + TRUNCATED_MARKER.len());
        // 截断只砍字节不换性质：仍是成功结果
        assert_eq!(out.exit_code, 0);
    }
}
