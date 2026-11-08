//! Agent 的实时进度投影（wire 事件版），复用有界节点日志通道。
//!
//! 进度日志是**可丢弃观测**，不是审计：它走与 nodelog 相同的有界队列与
//! 丢弃计数，不占 audit_seq、不占持久窗口。因此它可以为了可读性做聚合
//! （按轮/按请求计数），但绝不能承担「证明发生了什么」的职责。
//!
//! 隐私边界（设计合同）：本层消费的是插件侧投影后的
//! [`WireEvent`](flow_harness_plugin::WireEvent)——**wire 上本来就不存在**模型
//! 正文、thinking、工具参数与结果正文、prompt（投影规则见 agent-plugin
//! crate）。本层只做展示聚合：轮次、请求次数、停止原因、token 用量、
//! 工具名与调用 ID、耗时、成败。

use std::time::Instant;

use flow_engine::NodeLogger;
use flow_harness_protocol::WireEvent;

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
    /// 当前轮起点（用于轮耗时的兜底显示）。
    round_started: Instant,
}

impl<'a> AgentProgress<'a> {
    /// 新建投影器（计数从零开始）。
    pub(crate) fn new(logger: &'a NodeLogger) -> Self {
        Self {
            logger,
            rounds: 0,
            requests: 0,
            round_started: Instant::now(),
        }
    }

    /// 已开始的轮数（用于节点结果摘要）。
    pub(crate) fn rounds(&self) -> usize {
        self.rounds
    }

    /// 把一个 wire 事件投影成一条观测日志。
    pub(crate) fn on_wire_event(&mut self, event: WireEvent) {
        let logger = self.logger;
        match event {
            WireEvent::TurnStart => {
                self.rounds += 1;
                self.requests = 0;
                self.round_started = Instant::now();
                logger.info(format!("agent 第 {} 轮开始", self.rounds));
            }
            WireEvent::ProviderRequest { model } => {
                self.requests += 1;
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
            WireEvent::ProviderResponse {
                stop_reason,
                tools,
                input_tokens,
                output_tokens,
                ms,
            } => {
                // 停止原因映射成人话：终态语义（完成/工具/截断/失败/中止）
                // 直接决定用户看到的结果，必须显式呈现。
                let (status, severe) = match stop_reason.as_str() {
                    "end_turn" => ("完成", false),
                    "tool_use" => ("调用工具", false),
                    "max_tokens" => ("达到输出上限", true),
                    "error" => ("失败", true),
                    "aborted" => ("中止", true),
                    _ => ("未知终态", true),
                };
                let usage = match (input_tokens, output_tokens) {
                    (Some(input), Some(output)) => {
                        format!("，输入 {input} / 输出 {output} tokens")
                    }
                    _ => String::new(),
                };
                let line = format!(
                    "agent 第 {} 轮 ← 模型响应：{status}（{ms}ms，{tools} 个工具调用{usage}）",
                    self.rounds
                );
                if severe {
                    // 失败/截断/中止升级为 warn：这些终态在节点结果里
                    // 会体现为失败，观测侧应先一步示警。
                    logger.warn(line);
                } else {
                    logger.info(line);
                }
            }
            WireEvent::ToolStart { id, name } => {
                logger.info(format!(
                    "agent 第 {} 轮 → 工具 {name} 开始（{id}）",
                    self.rounds
                ));
            }
            WireEvent::ToolEnd {
                id,
                name,
                is_error,
                ms,
            } => {
                let elapsed = ms.map(|ms| format!("，{ms}ms")).unwrap_or_default();
                let status = if is_error { "失败" } else { "完成" };
                let line = format!(
                    "agent 第 {} 轮 ← 工具 {name} {status}（{id}{elapsed}）",
                    self.rounds
                );
                if is_error {
                    logger.warn(line);
                } else {
                    logger.info(line);
                }
            }
            WireEvent::TurnEnd {
                tool_results,
                errors,
                ms,
            } => {
                logger.info(format!(
                    "agent 第 {} 轮结束（{ms}ms，{tool_results} 个工具结果，{errors} 个失败）",
                    self.rounds
                ));
            }
            WireEvent::AgentEnd => {
                logger.info(format!("agent 多轮循环结束（共 {} 轮）", self.rounds))
            }
            WireEvent::Error => logger.error(format!(
                "agent 第 {} 轮执行错误（详情见节点结果）",
                self.rounds
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_engine::log_types::LogLevel;
    use flow_engine::LogBudget;

    /// 重试与失败必须可见；wire 事件本身只带元信息，日志层不得拼出任何
    /// 载荷（这里同时回归格式串，防止观测文案漂移）。
    #[test]
    fn retries_and_failures_are_visible_with_stable_format() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let logger = NodeLogger::new(tx, LogBudget::new(100), "agent", 1);
        let mut progress = AgentProgress::new(&logger);
        progress.on_wire_event(WireEvent::TurnStart);
        progress.on_wire_event(WireEvent::ProviderRequest { model: "m".into() });
        progress.on_wire_event(WireEvent::ProviderRequest { model: "m".into() });
        progress.on_wire_event(WireEvent::ProviderResponse {
            stop_reason: "error".into(),
            tools: 0,
            input_tokens: Some(12),
            output_tokens: Some(3),
            ms: 45,
        });
        progress.on_wire_event(WireEvent::ToolStart {
            id: "call".into(),
            name: "bash".into(),
        });
        progress.on_wire_event(WireEvent::ToolEnd {
            id: "call".into(),
            name: "bash".into(),
            is_error: true,
            ms: Some(7),
        });
        progress.on_wire_event(WireEvent::TurnEnd {
            tool_results: 1,
            errors: 1,
            ms: 120,
        });
        progress.on_wire_event(WireEvent::TurnStart);
        progress.on_wire_event(WireEvent::ProviderRequest { model: "m".into() });
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
        assert!(logs.iter().any(|line| line
            .message
            .contains("第 1 轮结束（120ms，1 个工具结果，1 个失败）")));
        // 兜底：round_started 之外没有其他 Instant 断言，纯格式回归。
        let _ = progress.round_started;
    }
}
