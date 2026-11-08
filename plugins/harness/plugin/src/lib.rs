//! agent-plugin：harness agent 的插件服务进程逻辑。
//!
//! 职责一句话：**把一次 agent 回合变成一条 JSON-RPC 请求的生命周期**——
//! `agent/runTurn` 请求进来，`agent/event` 通知流出去，摘要响应收口。
//!
//! 三层结构（与 docs/protocol/wire.md 对齐）：
//! 1. 通道：proc-ipc（fd=3 或回连+令牌），本 crate 不关心怎么拿到的；
//! 2. 协议：plugin-proto 的帧 + JSON-RPC 2.0 子集 + initialize 握手；
//! 3. 方法层（本 crate）：`agent/runTurn` / `agent/event` / `$/cancelRequest`。
//!
//! 隐私边界（与原 executor 内联实现完全一致，见 agent_progress 的设计
//! 合同）：`agent/event` 只携带元信息——轮次、模型名、停止原因、token
//! 用量、工具名与调用 ID、耗时、成败。模型正文、thinking、工具参数与
//! 结果正文、prompt 一律不过线。
//!
//! 错误码契约（宿主据此区分确定性失败与不确定失败）：
//! - `-32010`：装配失败（配置非法/代理不可用）→ 宿主按确定性失败收口；
//! - `-32011`：回合失败（provider 错误、取消、超时类）→ 宿主按不确定
//!   失败收口，随后的恢复语义由宿主决定。

use agent_core::AgentEvent;
pub use flow_harness_protocol::{
    RunTurnParams, TurnOutcome, TurnSpec, WireEvent, ASSEMBLY_ERROR, TURN_ERROR,
};

use agent_host::TurnSummary;
use flow_plugin_sdk::protocol::jsonrpc::{Error, ErrorCode, Id, Message};
use flow_plugin_sdk::protocol::PROTOCOL_VERSION;

/// 把一条 `AgentEvent` 投影成 wire 事件；不适合过线的事件返回 `None`。
///
/// 计时在这里做（插件侧有精确的请求/工具起点），宿主只负责展示。
pub fn project_event(event: &AgentEvent, clocks: &EventClocks) -> Option<WireEvent> {
    match event {
        AgentEvent::TurnStart => Some(WireEvent::TurnStart),
        AgentEvent::BeforeProviderRequest { model } => {
            clocks.provider_request_started();
            Some(WireEvent::ProviderRequest {
                model: model.clone(),
            })
        }
        AgentEvent::AfterProviderResponse { response, .. } => Some(WireEvent::ProviderResponse {
            stop_reason: stop_reason_literal(response.stop_reason.clone()).to_string(),
            tools: response
                .content
                .iter()
                .filter(|block| matches!(block, agent_core::ContentBlock::ToolCall { .. }))
                .count(),
            input_tokens: response.usage.as_ref().map(|usage| usage.input_tokens),
            output_tokens: response.usage.as_ref().map(|usage| usage.output_tokens),
            ms: clocks.provider_request_elapsed_ms(),
        }),
        AgentEvent::ToolExecutionStart {
            tool_call_id,
            tool_name,
            ..
        } => {
            clocks.tool_started(tool_call_id);
            Some(WireEvent::ToolStart {
                id: tool_call_id.clone(),
                name: tool_name.clone(),
            })
        }
        AgentEvent::ToolExecutionEnd {
            tool_call_id,
            result,
            is_error,
        } => Some(WireEvent::ToolEnd {
            id: tool_call_id.clone(),
            name: result.tool_name.clone(),
            is_error: *is_error || result.is_error,
            ms: clocks.tool_elapsed_ms(tool_call_id),
        }),
        AgentEvent::TurnEnd { tool_results, .. } => Some(WireEvent::TurnEnd {
            tool_results: tool_results.len(),
            errors: tool_results.iter().filter(|result| result.is_error).count(),
            ms: clocks.turn_elapsed_ms(),
        }),
        AgentEvent::AgentEnd => Some(WireEvent::AgentEnd),
        AgentEvent::Error { .. } => Some(WireEvent::Error),
        // 流式正文 / thinking / 工具参数：不过线（隐私边界）。
        _ => None,
    }
}

/// 事件计时器：跟踪请求/工具/轮次起点。
///
/// 单线程使用（事件回调内），内部可变。
#[derive(Default)]
pub struct EventClocks {
    provider_request_started: std::cell::Cell<Option<std::time::Instant>>,
    turn_started: std::cell::Cell<Option<std::time::Instant>>,
    tools: std::cell::RefCell<std::collections::HashMap<String, std::time::Instant>>,
}

impl EventClocks {
    fn provider_request_started(&self) {
        self.provider_request_started
            .set(Some(std::time::Instant::now()));
    }

    fn provider_request_elapsed_ms(&self) -> u64 {
        self.provider_request_started
            .get()
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }

    fn tool_started(&self, id: &str) {
        if self.turn_started.get().is_none() {
            self.turn_started.set(Some(std::time::Instant::now()));
        }
        self.tools
            .borrow_mut()
            .insert(id.to_string(), std::time::Instant::now());
    }

    fn tool_elapsed_ms(&self, id: &str) -> Option<u64> {
        self.tools
            .borrow_mut()
            .remove(id)
            .map(|started| started.elapsed().as_millis() as u64)
    }

    fn turn_elapsed_ms(&self) -> u64 {
        self.turn_started
            .get()
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }
}

fn stop_reason_literal(reason: agent_core::StopReason) -> &'static str {
    match reason {
        agent_core::StopReason::EndTurn => "end_turn",
        agent_core::StopReason::ToolUse => "tool_use",
        agent_core::StopReason::MaxTokens => "max_tokens",
        agent_core::StopReason::Error(_) => "error",
        agent_core::StopReason::Aborted => "aborted",
    }
}

// ---------------------------------------------------------------------------
// 结果投影（自 flow-executor/agent_summary.rs 迁入，语义与测试原样）
// ---------------------------------------------------------------------------

/// stdout/body_raw 的最终文本上限。journal 派生按 `body_raw > 8 MiB` 拒绝，
/// 8 MiB 文本 + 截断标记会恰好越过那条线；留出标记与余量。
pub const TEXT_LIMIT_BYTES: usize = 7 * 1024 * 1024 + 512 * 1024;
/// 截断标记：让读者能区分「本来就短」与「被截断了」。
const TRUNCATED_MARKER: &str = "\n[truncated]";

/// 一次 `run_turn` 的投影（原 `summarize`，输出直接作为响应体）。
///
/// 核心判定是「只看末条 assistant」：中间的 MaxTokens / 工具调用都不是
/// 终态，只有最后一条消息的停止原因决定这次 agent 到底算不算完成。
/// 误判方向被刻意选成保守——把已完成判成失败可以重试，把失败判成
/// 成功会掩盖工具副作用。
pub fn summarize(summary: &TurnSummary) -> TurnOutcome {
    use agent_core::{AgentMessage, ContentBlock, StopReason, TurnEndReason};

    let last_assistant = summary.new_messages.iter().rev().find_map(|m| match m {
        AgentMessage::Assistant(a) => Some(a),
        _ => None,
    });
    let (text, stop_reason) = match last_assistant {
        Some(message) => {
            // 只拼接文本块：工具调用块的内容不是「输出」。
            let joined = join_text(message.content.iter().filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            }));
            (
                joined,
                stop_reason_literal(message.stop_reason.clone()).to_string(),
            )
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
                TurnOutcome {
                    text,
                    stop_reason,
                    error: None,
                    exit_code: 0,
                }
            } else if stop_reason == "tool_use" {
                // 末条停在 tool_use = max_turns 耗尽：工具副作用已经发生，
                // 不能当成「再跑一次就好」，明确判失败并说明原因。
                TurnOutcome {
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
                TurnOutcome {
                    text,
                    stop_reason,
                    error: Some(detail),
                    exit_code: 1,
                }
            }
        }
        TurnEndReason::Error { message } => TurnOutcome {
            text,
            stop_reason,
            error: Some(message.clone()),
            exit_code: 1,
        },
        // 防御性：宿主的取消/超时在适配层直接判 Uncertain，不经过这里。
        TurnEndReason::Aborted { .. } => TurnOutcome {
            text,
            stop_reason,
            error: Some("turn aborted".into()),
            exit_code: 130,
        },
    }
}

/// 派生期 8 MiB 检查之前的自我截断：在字符边界截断并写标记，把「输出
/// 过大」变成带标记的可见结果，而不是 journal 侧的 Business。
fn join_text<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    let mut result = String::new();
    for part in parts {
        let remaining = TEXT_LIMIT_BYTES.saturating_sub(result.len());
        if part.len() <= remaining {
            result.push_str(part);
            continue;
        }
        let mut cut = remaining;
        while cut > 0 && !part.is_char_boundary(cut) {
            cut -= 1;
        }
        result.push_str(&part[..cut]);
        result.push_str(TRUNCATED_MARKER);
        break;
    }
    result
}

// ---------------------------------------------------------------------------
// 服务循环
// ---------------------------------------------------------------------------

/// 构造 initialize 成功响应（含回连令牌回验，wire.md §3.5）。
pub fn initialize_result(
    token: Option<String>,
) -> flow_plugin_sdk::protocol::handshake::InitializeResult {
    flow_plugin_sdk::protocol::handshake::InitializeResult {
        protocol_version: PROTOCOL_VERSION,
        capabilities: vec![],
        server_info: serde_json::json!({"name": "flow-agent-plugin", "version": "0.1.0"}),
        session_token: token,
    }
}

/// 应用错误（装配/回合）的 JSON-RPC error。
pub fn app_error(code: i64, message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Application(code), message)
}

/// 服务循环的读侧消息分类（供 serve 与测试复用）。
pub enum Incoming {
    Request(flow_plugin_sdk::protocol::jsonrpc::Request),
    Notification(flow_plugin_sdk::protocol::jsonrpc::Notification),
    Response(flow_plugin_sdk::protocol::jsonrpc::Response),
}

/// 把一条入站消息分类；不合法结构在这里就拒绝。
pub fn classify(message: Message) -> Result<Incoming, Error> {
    match message {
        Message::Request(request) => Ok(Incoming::Request(request)),
        Message::Notification(notification) => Ok(Incoming::Notification(notification)),
        Message::Response(response) => Ok(Incoming::Response(response)),
    }
}

/// 回合句柄：serve 循环持有，取消时打信号。
pub struct TurnHandle {
    /// 取消信号：`$/cancelRequest` 命中在飞回合时触发。
    pub cancel: agent_core::CancelSignal,
    /// 在飞回合的请求 id（用于取消配对）。
    pub request_id: Id,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::types::message::{AssistantMessage, ToolResultMessage, Usage};
    use agent_core::{AgentMessage, ContentBlock, StopReason, TurnEndReason};
    use agent_host::TurnSummary;
    use serde_json::json;

    fn assistant(text: &str, stop: StopReason) -> AgentMessage {
        let mut message = AssistantMessage::new(&"m".into());
        message.content.push(ContentBlock::text(text));
        message.stop_reason = stop;
        AgentMessage::Assistant(message)
    }

    /// 末条 assistant 的停止原因决定成败；中间消息不是终态。
    #[test]
    fn summarize_uses_last_assistant_only() {
        let summary = TurnSummary {
            reason: TurnEndReason::Completed,
            new_messages: vec![
                assistant("中间截断不算终态", StopReason::MaxTokens),
                assistant("final", StopReason::EndTurn),
            ],
        };
        let outcome = summarize(&summary);
        assert_eq!(outcome.exit_code, 0);
        assert_eq!(outcome.text, "final");
        assert!(outcome.error.is_none());
    }

    #[test]
    fn summarize_maps_failure_shapes() {
        // 末条 tool_use + Completed = max_turns 耗尽（副作用已发生）。
        let summary = TurnSummary {
            reason: TurnEndReason::Completed,
            new_messages: vec![assistant("t", StopReason::ToolUse)],
        };
        let outcome = summarize(&summary);
        assert_eq!(outcome.exit_code, 1);
        assert!(outcome.error.unwrap().contains("max turns"));

        // 回合错误 → 1。
        let summary = TurnSummary {
            reason: TurnEndReason::Error {
                message: "provider boom".into(),
            },
            new_messages: vec![],
        };
        let outcome = summarize(&summary);
        assert_eq!(outcome.exit_code, 1);
        assert!(outcome.error.unwrap().contains("boom"));

        // 中止 → 130（runner 中止契约）。
        let summary = TurnSummary {
            reason: TurnEndReason::Aborted { cause: None },
            new_messages: vec![],
        };
        assert_eq!(summarize(&summary).exit_code, 130);
    }

    /// 文本上限：截断后的长度必须仍落在 journal 派生的 8 MiB 线内。
    #[test]
    fn text_limit_truncates_below_journal_ceiling() {
        let big = "x".repeat(TEXT_LIMIT_BYTES + 4096);
        let summary = TurnSummary {
            reason: TurnEndReason::Completed,
            new_messages: vec![assistant(&big, StopReason::EndTurn)],
        };
        let outcome = summarize(&summary);
        assert!(outcome.text.len() <= TEXT_LIMIT_BYTES + TRUNCATED_MARKER.len());
        assert!(outcome.text.ends_with(TRUNCATED_MARKER));
    }

    /// 隐私边界：投影后的 wire 事件不得包含正文/参数/结果体。
    #[test]
    fn wire_events_carry_metadata_only() {
        let clocks = EventClocks::default();
        let mut response = AssistantMessage::new(&"m".into());
        response
            .content
            .push(ContentBlock::text("private-response"));
        response.stop_reason = StopReason::Error("private-provider-error".into());
        response.usage = Some(Usage {
            input_tokens: 12,
            output_tokens: 3,
            ..Usage::default()
        });
        let events = vec![
            AgentEvent::TurnStart,
            AgentEvent::BeforeProviderRequest { model: "m".into() },
            AgentEvent::AfterProviderResponse {
                model: "m".into(),
                response,
            },
            AgentEvent::ToolExecutionStart {
                tool_call_id: "call".into(),
                tool_name: "bash".into(),
                args: json!({"command": "private-command"}),
            },
            AgentEvent::ToolExecutionEnd {
                tool_call_id: "call".into(),
                result: ToolResultMessage {
                    tool_call_id: "call".into(),
                    tool_name: "bash".into(),
                    content: vec![ContentBlock::text("private-tool-result")],
                    is_error: true,
                    timestamp: 0,
                },
                is_error: false,
            },
            AgentEvent::TurnEnd {
                message: AssistantMessage::new(&"m".into()),
                tool_results: vec![],
            },
            AgentEvent::AgentEnd,
        ];
        let mut wire = Vec::new();
        for event in &events {
            if let Some(projected) = project_event(event, &clocks) {
                wire.push(serde_json::to_string(&projected).unwrap());
            }
        }
        let joined = wire.join("\n");
        assert!(!joined.contains("private-"));
        // 流式正文事件被整体丢弃，而不是带出去。
        assert!(project_event(
            &AgentEvent::MessageUpdate {
                delta: agent_core::ContentDelta::TextDelta("private-delta".into())
            },
            &clocks
        )
        .is_none());
        // 取消原因里的文本也不过线（Error 事件只投「发生了错误」）。
        // Error 事件只投「发生了错误」，message 正文不过线。
        let projected = project_event(
            &AgentEvent::Error {
                message: "private-error-detail".into(),
            },
            &clocks,
        )
        .unwrap();
        assert!(matches!(projected, WireEvent::Error));
        assert!(!serde_json::to_string(&projected)
            .unwrap()
            .contains("private-error-detail"));
    }
}
