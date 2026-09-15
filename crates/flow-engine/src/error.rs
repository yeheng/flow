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
}
