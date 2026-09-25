use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("io 错误：{0}")]
    Io(#[from] std::io::Error),
    #[error("json 错误：{0}")]
    Json(#[from] serde_json::Error),
    #[error("工作流定义非法：{0}")]
    InvalidDefinition(String),
    #[error("run 不存在：{0}")]
    RunNotFound(String),
    #[error("run 已存在：{0}")]
    RunExists(String),
    #[error("run 当前不接受输入：{0}")]
    NotLive(String),
    #[error("非法信号：{0}")]
    InvalidSignal(String),
    #[error("表达式求值失败：{0}")]
    Expr(String),
    #[error("节点错误：{0}")]
    Node(String),
    #[error("事件日志损坏：{0}")]
    LogCorrupted(String),
    #[error("run 所有权已丢失（租约被其他实例接管）")]
    LeaseLost,
    #[error("后端错误：{0}")]
    Backend(String),
}

impl EngineError {
    /// 所有权丢失：本实例必须停止派发、丢弃未提交结果并静默退出，
    /// 不允许再写任何事件或状态投影（DISTRIBUTED.md §5.2）。
    pub fn is_lease_lost(&self) -> bool {
        matches!(self, EngineError::LeaseLost)
    }

    /// 基础设施故障（磁盘/数据库 IO、序列化、日志损坏）：不是工作流本身的失败，
    /// 不允许写成 run_failed 终态——run 挂起 awaiting_resume 等待恢复或人工介入
    ///（DESIGN.md §7）。
    pub fn is_platform_fault(&self) -> bool {
        matches!(
            self,
            EngineError::Io(_)
                | EngineError::Json(_)
                | EngineError::Backend(_)
                | EngineError::LogCorrupted(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_fault() -> std::io::Error {
        std::io::Error::other("disk full")
    }

    #[test]
    fn platform_fault_is_not_a_workflow_failure() {
        // 平台故障（IO/序列化/后端/日志损坏）→ awaiting_resume 挂起，
        // 不允许被 run() 写成 run_failed 终态
        assert!(EngineError::Io(io_fault()).is_platform_fault());
        assert!(EngineError::Backend("db down".into()).is_platform_fault());
        assert!(EngineError::LogCorrupted("seq gap".into()).is_platform_fault());
        assert!(EngineError::Json(serde_json::Error::io(io_fault())).is_platform_fault());
        // 工作流语义失败保持现有 run_failed 语义
        assert!(!EngineError::Node("调度停滞".into()).is_platform_fault());
        assert!(!EngineError::Expr("boom".into()).is_platform_fault());
        // 所有权丢失走静默退出，不进故障分类
        assert!(!EngineError::LeaseLost.is_platform_fault());
    }

    #[test]
    fn lease_lost_detection_is_exact() {
        assert!(EngineError::LeaseLost.is_lease_lost());
        assert!(!EngineError::Backend("lease".into()).is_lease_lost());
    }
}
