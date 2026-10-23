//! harness agent 模式（mode=agent）的结果投影：把 conga `run_turn` 的
//! `TurnSummary` 投影成 outcome 字段（exit_code/text/stderr 元数据语义与
//! 原 runner 二进制的退出码契约一致：0 完成 / 1 回合失败或 max_turns
//! 耗尽 / 130 中止）。

use conga::{AgentMessage, ContentBlock, StopReason, TurnEndReason};
use conga_host::TurnSummary;

/// stdout/body_raw 的最终文本上限。journal 派生按 `body_raw > 8 MiB` 拒绝，
/// 8 MiB 文本 + 截断标记会恰好越过那条线；留出标记与余量。
pub const TEXT_LIMIT_BYTES: usize = 7 * 1024 * 1024 + 512 * 1024;
const TRUNCATED_MARKER: &str = "\n[truncated]";

/// 一次 `run_turn` 的投影结果（outcome 组装用）。
pub struct AgentTurnResult {
    pub text: String,
    pub stop_reason: String,
    pub error: Option<String>,
    pub exit_code: i32,
}

/// 成功判定（与 conga 语义对齐）：`Completed` 且末条 assistant 的
/// stop_reason 为 `EndTurn`；末条 `ToolUse` = max_turns 耗尽（失败，末轮
/// 工具副作用已发生）；中间 `MaxTokens` 不单独判死——只看**末条**。
pub fn summarize(summary: &TurnSummary) -> AgentTurnResult {
    let last_assistant = summary.new_messages.iter().rev().find_map(|m| match m {
        AgentMessage::Assistant(a) => Some(a),
        _ => None,
    });
    let (text, stop_reason) = match last_assistant {
        Some(message) => {
            let joined = join_text(message.content.iter().filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            }));
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
    match &summary.reason {
        TurnEndReason::Completed => {
            let ok = matches!(
                last_assistant.map(|m| &m.stop_reason),
                Some(StopReason::EndTurn)
            );
            if ok {
                AgentTurnResult {
                    text,
                    stop_reason,
                    error: None,
                    exit_code: 0,
                }
            } else if stop_reason == "tool_use" {
                AgentTurnResult {
                    text,
                    stop_reason,
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
                AgentTurnResult {
                    text,
                    stop_reason,
                    error: Some(detail),
                    exit_code: 1,
                }
            }
        }
        TurnEndReason::Error { message } => AgentTurnResult {
            text,
            stop_reason,
            error: Some(message.clone()),
            exit_code: 1,
        },
        // 防御性：flow 的取消/超时在 agent_run 的 select 分支直接判
        // Uncertain，不经过这里。
        TurnEndReason::Aborted { .. } => AgentTurnResult {
            text,
            stop_reason,
            error: Some("turn aborted".into()),
            exit_code: 130,
        },
    }
}

/// 派生期 8 MiB 检查之前的自我截断：在字符边界截断并写标记，把「输出
/// 过大」变成带标记的可见结果，而不是 journal 侧的 Business。
fn join_text<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    let mut result = String::new();
    for part in parts {
        let remaining = TEXT_LIMIT_BYTES - result.len();
        if part.len() <= remaining {
            result.push_str(part);
            continue;
        }
        let mut cut = remaining;
        while !part.is_char_boundary(cut) {
            cut -= 1;
        }
        result.push_str(&part[..cut]);
        result.push_str(TRUNCATED_MARKER);
        break;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::types::message::{AssistantMessage, FunctionCall, ToolCall};
    use conga::{AgentMessage, ContentBlock, StopReason};

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
                    tool_call: ToolCall {
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
        assert_eq!(out.exit_code, 0);
    }
}
