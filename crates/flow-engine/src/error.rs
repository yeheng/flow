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
}
