//! Agent 的实时进度投影，复用有界节点日志通道，不记录 prompt、正文或工具参数。
//!
//! 进度日志是**可丢弃观测**，不是审计：它走与 nodelog 相同的有界队列与
//! 丢弃计数，不占 audit_seq、不占持久窗口。因此它可以为了可读性做聚合
//! （按轮/按请求计数），但绝不能承担「证明发生了什么」的职责。
//!
//! 隐私边界（设计合同）：只投影元信息——轮次、模型名、请求次数、token
//! 用量、工具名与调用 ID、耗时、成败。模型正文、thinking、工具参数与
//! 结果正文一律不进日志；prompt 更是从不进入。

use std::collections::BTreeMap;
use std::time::Instant;

use conga::{AgentEvent, ContentBlock, StopReason};
use flow_engine::NodeLogger;

/// Agent 事件的实时进度投影器。
///
/// 生命周期与一次 `run_turn` 相同：它只持有计数与计时，不持有会话或
/// 工具对象，因此不会妨碍宿主清理。
pub(crate) struct AgentProgress<'a> {
    /// 观测出口（有界、非阻塞）。
    logger: &'a NodeLogger,
    /// 已开始的轮数。
    rounds: usize,
    /// 当前轮内的模型请求次数。
    requests: usize,
    /// 当前轮起点（用于轮耗时）。
    round_started: Instant,
    /// 当前请求起点（用于请求耗时）。
    request_started: Instant,
    /// 进行中的工具调用及其起点（按调用 ID 索引，结束即移除）。
    tools: BTreeMap<String, Instant>,
}

impl<'a> AgentProgress<'a> {
    /// 新建投影器（计数从零开始）。
    pub(crate) fn new(logger: &'a NodeLogger) -> Self {
        Self {
            logger,
            rounds: 0,
            requests: 0,
            round_started: Instant::now(),
            request_started: Instant::now(),
            tools: BTreeMap::new(),
        }
    }

    /// 已开始的轮数（用于节点结果摘要）。
    pub(crate) fn rounds(&self) -> usize {
        self.rounds
    }

    /// 把一个 agent 事件投影成一条（或多条）观测日志。
    ///
    /// 只输出元信息；未知事件一律忽略（`MessageUpdate` 等流式事件携带
    /// 正文，绝不能外泄）。
    pub(crate) fn on_event(&mut self, event: AgentEvent) {
        let logger = self.logger;
        match event {
            AgentEvent::TurnStart => {
                self.rounds += 1;
                self.requests = 0;
                self.round_started = Instant::now();
                logger.info(format!("agent 第 {} 轮开始", self.rounds));
            }
            AgentEvent::BeforeProviderRequest { model } => {
                self.requests += 1;
                self.request_started = Instant::now();
                let line = format!(
                    "agent 第 {} 轮 → 模型请求（{model}，第 {} 次）",
                    self.rounds, self.requests
                );
                if self.requests > 1 {
                    // 同一轮内的第 2 次及以后请求 = 重试：升级为 warn，
                    // 让运行页一眼看到 provider 在重试。
                    logger.warn(line);
                } else {
                    logger.info(line);
                }
            }
            AgentEvent::AfterProviderResponse { response, .. } => {
                // 停止原因映射成人话：终态语义（完成/工具/截断/失败/中止）
                // 直接决定用户看到的结果，必须显式呈现。
                let status = match &response.stop_reason {
                    StopReason::EndTurn => "完成",
                    StopReason::ToolUse => "调用工具",
                    StopReason::MaxTokens => "达到输出上限",
                    StopReason::Error(_) => "失败",
                    StopReason::Aborted => "中止",
                };
                // 只数工具调用块，不读参数内容。
                let tools = response
                    .content
                    .iter()
                    .filter(|block| matches!(block, ContentBlock::ToolCall { .. }))
                    .count();
                let usage = response
                    .usage
                    .map(|usage| {
                        format!(
                            "，输入 {} / 输出 {} tokens",
                            usage.input_tokens, usage.output_tokens
                        )
                    })
                    .unwrap_or_default();
                let line = format!(
                    "agent 第 {} 轮 ← 模型响应：{status}（{}ms，{tools} 个工具调用{usage}）",
                    self.rounds,
                    self.request_started.elapsed().as_millis()
                );
                if matches!(
                    response.stop_reason,
                    StopReason::Error(_) | StopReason::MaxTokens | StopReason::Aborted
                ) {
                    // 失败/截断/中止升级为 warn：这些终态在节点结果里
                    // 会体现为失败，观测侧应先一步示警。
                    logger.warn(line);
                } else {
                    logger.info(line);
                }
            }
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                ..
            } => {
                // 按调用 ID 记起点：结束事件可能乱序到达，只能靠 ID 配对。
                self.tools.insert(tool_call_id.clone(), Instant::now());
                logger.info(format!(
                    "agent 第 {} 轮 → 工具 {tool_name} 开始（{tool_call_id}）",
                    self.rounds
                ));
            }
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                result,
                is_error,
            } => {
                // 找不到起点时不报错：结束先于开始到达是可能的（重连/
                // 乱序），耗时退化为不显示即可。
                let elapsed = self
                    .tools
                    .remove(&tool_call_id)
                    .map(|started| format!("，{}ms", started.elapsed().as_millis()))
                    .unwrap_or_default();
                // 两个错误标记都算失败：事件级 is_error 与结果体里的
                // is_error 可能只有一个置位。
                let failed = is_error || result.is_error;
                let status = if failed { "失败" } else { "完成" };
                let line = format!(
                    "agent 第 {} 轮 ← 工具 {} {status}（{tool_call_id}{elapsed}）",
                    self.rounds, result.tool_name
                );
                if failed {
                    logger.warn(line);
                } else {
                    logger.info(line);
                }
            }
            AgentEvent::TurnEnd { tool_results, .. } => {
                let errors = tool_results.iter().filter(|result| result.is_error).count();
                logger.info(format!(
                    "agent 第 {} 轮结束（{}ms，{} 个工具结果，{errors} 个失败）",
                    self.rounds,
                    self.round_started.elapsed().as_millis(),
                    tool_results.len()
                ));
            }
            AgentEvent::AgentEnd => {
                logger.info(format!("agent 多轮循环结束（共 {} 轮）", self.rounds))
            }
            AgentEvent::Error { .. } => logger.error(format!(
                "agent 第 {} 轮执行错误（详情见节点结果）",
                self.rounds
            )),
            // 流式文本、thinking、工具参数与结果正文不进入进度日志。
            //
            // 未知事件同样忽略：这是「宁可少记也不外泄」的默认策略。
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::types::message::{AssistantMessage, ToolResultMessage, Usage};
    use conga::ContentDelta;
    use flow_engine::log_types::LogLevel;
    use flow_engine::LogBudget;
    use serde_json::json;

    /// 重试与失败必须可见，但事件载荷（正文/参数）绝不能进日志。
    #[test]
    fn retries_and_failures_are_visible_without_logging_event_payloads() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let logger = NodeLogger::new(tx, LogBudget::new(100), "agent", 1);
        let mut progress = AgentProgress::new(&logger);
        progress.on_event(AgentEvent::TurnStart);
        for _ in 0..2 {
            progress.on_event(AgentEvent::BeforeProviderRequest { model: "m".into() });
        }
        for delta in [
            ContentDelta::TextDelta("private-text".into()),
            ContentDelta::ThinkingDelta("private-thinking".into()),
            ContentDelta::ToolCallDelta {
                id: "call".into(),
                name: Some("bash".into()),
                args_delta: "private-args".into(),
            },
        ] {
            progress.on_event(AgentEvent::MessageUpdate { delta });
        }
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
        progress.on_event(AgentEvent::AfterProviderResponse {
            model: "m".into(),
            response,
        });
        progress.on_event(AgentEvent::ToolExecutionStart {
            tool_call_id: "call".into(),
            tool_name: "bash".into(),
            args: json!({"command":"private-command"}),
        });
        progress.on_event(AgentEvent::ToolExecutionEnd {
            tool_call_id: "call".into(),
            result: ToolResultMessage {
                tool_call_id: "call".into(),
                tool_name: "bash".into(),
                content: vec![ContentBlock::text("private-tool-result")],
                is_error: true,
                timestamp: 0,
            },
            is_error: true,
        });
        progress.on_event(AgentEvent::TurnStart);
        progress.on_event(AgentEvent::BeforeProviderRequest { model: "m".into() });
        assert_eq!(progress.rounds(), 2);

        let mut logs = Vec::new();
        while let Ok(line) = rx.try_recv() {
            logs.push(line);
        }
        assert!(logs.iter().any(|line| line.level == LogLevel::Warn
            && line.message.contains("第 1 轮 → 模型请求（m，第 2 次）")));
        assert!(logs.iter().any(|line| line.level == LogLevel::Info
            && line.message.contains("第 2 轮 → 模型请求（m，第 1 次）")));
        assert!(logs.iter().any(|line| line.level == LogLevel::Warn
            && line.message.contains("模型响应：失败")
            && line.message.contains("输入 12 / 输出 3 tokens")));
        assert!(logs
            .iter()
            .any(|line| line.level == LogLevel::Warn && line.message.contains("工具 bash 失败")));
        assert!(!serde_json::to_string(&logs).unwrap().contains("private-"));
    }
}
