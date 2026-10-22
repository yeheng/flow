use thiserror::Error;

/// Expression evaluation errors. Scheduling and storage failures belong to their
/// owning crates; they are not classified by the expression evaluator.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("表达式求值失败：{0}")]
    Expr(String),
}
