//! Postgres 后端错误。

use flow_engine::EngineError;

#[derive(Debug, thiserror::Error)]
pub enum PgError {
    #[error("数据库错误：{0}")]
    Sql(#[from] sqlx::Error),
    #[error("json 错误：{0}")]
    Json(#[from] serde_json::Error),
    #[error("引擎错误：{0}")]
    Engine(#[from] EngineError),
    #[error("run 不存在：{0}")]
    RunNotFound(String),
    #[error("冲突：{0}")]
    Conflict(String),
    #[error("非法参数：{0}")]
    Invalid(String),
    #[error("集群模式不符：{0}")]
    ModeMismatch(String),
    #[error("元数据与执行日志不一致：{0}")]
    IdentityMismatch(String),
}

impl PgError {
    pub fn is_lease_lost(&self) -> bool {
        matches!(self, PgError::Engine(e) if e.is_lease_lost())
    }
}
