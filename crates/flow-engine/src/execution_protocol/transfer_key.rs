//! 传输键（transfer id）：data 通道分块传输的值标识。
//!
//! wire 字符串格式已冻结（历史 journal 里的 op-body 约定、主进程与执行器
//! 双方按同一字符串对账），构造与解析集中在这里——散落各文件的魔术前缀
//! 拼接一旦 typo 只会静默丢块（执行器死等 InputReady 直到 deadline），
//! 收敛到单一类型后拼写错误在构造处即不可表达。

use std::fmt;

/// 一条 data 通道传输值的标识。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransferKey {
    /// `in:run-input` — run 输入。
    RunInput,
    /// `in:<node_id>` — 前驱节点输出。
    NodeInput(String),
    /// `in:prepared-params` — 已提交的准备参数。
    PreparedParams,
    /// `op-body:<output_id>` — 外部操作 Outcome 的原始字节（恢复路径由
    /// 主进程随 previous_outcome 重传；执行器捕获后本地 insert_ready）。
    OperationBody(String),
    /// `op:<operation_id>` — 执行器→主进程的操作请求字节；transfer_id 与
    /// RequestOperation.command_id 同串。
    OperationRequest(String),
}

impl TransferKey {
    /// wire 字符串（格式冻结，禁止变更）。
    pub fn to_wire(&self) -> String {
        match self {
            Self::RunInput => "in:run-input".into(),
            Self::NodeInput(node_id) => format!("in:{node_id}"),
            Self::PreparedParams => "in:prepared-params".into(),
            Self::OperationBody(output_id) => format!("op-body:{output_id}"),
            Self::OperationRequest(operation_id) => format!("op:{operation_id}"),
        }
    }

    /// 从 wire 字符串解析；非本协议格式返回 None。
    pub fn parse(wire: &str) -> Option<Self> {
        if let Some(node_id) = wire.strip_prefix("in:") {
            return match node_id {
                "run-input" => Some(Self::RunInput),
                "prepared-params" => Some(Self::PreparedParams),
                _ => Some(Self::NodeInput(node_id.into())),
            };
        }
        if let Some(output_id) = wire.strip_prefix("op-body:") {
            return Some(Self::OperationBody(output_id.into()));
        }
        wire.strip_prefix("op:")
            .map(|operation_id| Self::OperationRequest(operation_id.into()))
    }
}

impl fmt::Display for TransferKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_wire())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format_is_frozen() {
        assert_eq!(TransferKey::RunInput.to_wire(), "in:run-input");
        assert_eq!(TransferKey::NodeInput("n1".into()).to_wire(), "in:n1");
        assert_eq!(TransferKey::PreparedParams.to_wire(), "in:prepared-params");
        assert_eq!(
            TransferKey::OperationBody("out-9".into()).to_wire(),
            "op-body:out-9"
        );
        assert_eq!(
            TransferKey::OperationRequest("op-1".into()).to_wire(),
            "op:op-1"
        );
    }

    #[test]
    fn parse_roundtrips_and_rejects_foreign_keys() {
        for key in [
            TransferKey::RunInput,
            TransferKey::NodeInput("n1".into()),
            TransferKey::PreparedParams,
            TransferKey::OperationBody("out-9".into()),
            TransferKey::OperationRequest("op-1".into()),
        ] {
            assert_eq!(TransferKey::parse(&key.to_wire()), Some(key));
        }
        assert_eq!(TransferKey::parse("out:run-input"), None);
        assert_eq!(TransferKey::parse("op"), None);
        // 前缀即节点名的边界：节点恰好叫 "run-input" 时与保留键同串——
        // wire 上无法区分，构造侧用保留键承担该歧义（定义校验另有节点名
        // 约束）。
        assert_eq!(
            TransferKey::parse("in:run-input"),
            Some(TransferKey::RunInput)
        );
    }
}
