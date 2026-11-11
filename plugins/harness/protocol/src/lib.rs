//! harness agent 方法层协议（wire 契约）。
//!
//! 只含跨进程 DTO、方法名与应用错误码（`agent/runTurn` / `agent/event`
//! / `$/cancelRequest` 的载荷形状）。两侧消费者：
//!
//! - 宿主 `flow-executor`：构造 [`RunTurnParams`]、消费 [`WireEvent`]
//!   与 [`TurnOutcome`]；
//! - 实现 `flow-harness-plugin`：解析请求、投影事件、回摘要。
//!
//! 隐私边界是协议的一部分：[`WireEvent`] 的字段集就是「允许过线的
//! 元信息全集」——模型正文、thinking、工具参数与结果体、prompt 在
//! wire 上不存在。改字段集 = 改隐私边界 = 改协议。

use serde::{Deserialize, Serialize};

/// 装配失败的应用错误码（确定性失败：宿主按 Business 收口）。
pub const ASSEMBLY_ERROR: i64 = -32010;
/// 回合失败的应用错误码（不确定失败：宿主按 Uncertain 收口）。
pub const TURN_ERROR: i64 = -32011;

/// 节点执行入口方法名。
pub const RUN_TURN_METHOD: &str = "agent/runTurn";
/// 进度事件通知方法名。
pub const EVENT_METHOD: &str = "agent/event";

/// 回合权限（09 §8 TurnPermission）：缺省 = 未受管（legacy 路径，仅限
/// 测试钩子自动治理之前的兼容；生产 master 一律下发受管回合）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnPermission {
    /// `execute` | `plan`（plan 是不可提升的只读上限，插件侧提前拒绝
    /// 变更性工具，其余仍逐条问宿主）。
    pub execution_mode: String,
    /// master 冻结的 workflow/node 权限身份（诊断与审计关联）。
    pub policy_digest: String,
    /// 所有工具调用必须经 `permission/request` 反向请求裁决；未协商
    /// master_permit_v1 能力或宿主不可达时 fail closed。
    pub managed: bool,
}

/// `agent/runTurn` 的请求参数。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTurnParams {
    /// 模型与 provider 规格。
    pub spec: TurnSpec,
    /// API key：经通道传给被 spawn 的插件进程（同信任域，env 干净）。
    pub api_key: String,
    /// 本回合的用户消息（prompt）。
    pub prompt: String,
    /// 工作目录（项目目录）。
    pub workdir: String,
    /// 状态根目录（harness 沙箱根）。
    pub roots: String,
    /// 回合权限（缺省 None = 未受管 legacy 回合）。
    #[serde(default)]
    pub permission: Option<TurnPermission>,
}

/// 模型与 provider 规格。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnSpec {
    pub base_url: String,
    pub model: String,
    /// `openai`（OpenAI-compat，缺省）或 `anthropic`。
    #[serde(default)]
    pub api: String,
    pub proxy: Option<String>,
    pub max_turns: Option<usize>,
    pub max_tool_calls_per_turn: Option<usize>,
    pub max_tokens: Option<usize>,
    pub tool_timeout_secs: Option<u64>,
}

/// `agent/runTurn` 的成功响应：摘要（含文本截断契约）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnOutcome {
    /// assistant 的文本输出（已按上限截断）。
    pub text: String,
    /// 末条 assistant 的停止原因（wire 字面量）。
    pub stop_reason: String,
    /// 失败原因（成功时为 None）。
    pub error: Option<String>,
    /// 映射到 runner 退出码契约的退出码（0/1/130）。
    pub exit_code: i32,
}

/// 进度事件（`agent/event` 通知的参数）：只携带元信息。
///
/// 字段集 = 允许过线的元信息全集（隐私边界见模块文档）；计时在插件侧
/// 完成后随事件过线，宿主只负责展示。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireEvent {
    TurnStart,
    ProviderRequest {
        model: String,
    },
    ProviderResponse {
        /// 停止原因字面量（end_turn/tool_use/max_tokens/error/aborted）。
        stop_reason: String,
        /// 本响应中的工具调用块数量（只计数，不带内容）。
        tools: usize,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        /// 请求耗时（毫秒）。
        ms: u64,
    },
    ToolStart {
        id: String,
        name: String,
    },
    ToolEnd {
        id: String,
        name: String,
        is_error: bool,
        ms: Option<u64>,
    },
    TurnEnd {
        tool_results: usize,
        errors: usize,
        ms: u64,
    },
    AgentEnd,
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WireEvent 的 serde 标签形态是 wire 契约（kind 字段 + snake_case）：
    /// 这里钉住，防止 derive 属性被误动。
    #[test]
    fn wire_event_tag_format_is_pinned() {
        let event = WireEvent::ProviderResponse {
            stop_reason: "end_turn".into(),
            tools: 1,
            input_tokens: Some(12),
            output_tokens: Some(3),
            ms: 45,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "provider_response");
        let back: WireEvent = serde_json::from_value(json).unwrap();
        assert!(matches!(back, WireEvent::ProviderResponse { ms: 45, .. }));

        let json = serde_json::to_value(WireEvent::TurnStart).unwrap();
        assert_eq!(json, serde_json::json!({"kind": "turn_start"}));
    }

    #[test]
    fn turn_dto_roundtrip() {
        let params = RunTurnParams {
            permission: None,
            spec: TurnSpec {
                base_url: "http://x".into(),
                model: "m".into(),
                ..Default::default()
            },
            api_key: "k".into(),
            prompt: "p".into(),
            workdir: "/w".into(),
            roots: "/r".into(),
        };
        let json = serde_json::to_value(&params).unwrap();
        let back: RunTurnParams = serde_json::from_value(json).unwrap();
        assert_eq!(back.spec.base_url, "http://x");
    }
}
