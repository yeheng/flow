//! flow-backend：后端适配层。
//!
//! **架构决策（焊死）**：单机 `SQLite + data_dir/runs/<id>/event.jsonl` 是本系统的
//! 权威与默认后端（`sqlite.rs`，DESIGN.md）；Postgres 共享日志后端
//! （DISTRIBUTED.md）是**可替代**的等价实现（`pg.rs`），用于多节点执行。
//!
//! ## 接口哲学（闭集，不搞开放扩展）
//!
//! 后端集合是闭的：sqlite / postgres。上层拿到的是 [`AnyBackend`] 枚举，
//! **不是 `dyn Trait`**——每加一个方法，编译器逼你把两个臂都写完；
//! 没有"带默认实现的 trait 方法被某个后端悄悄继承"这种坑。
//! 公共面只包含**两个后端都诚实实现**的方法；pg 独有的能力
//! （如持久 inbox 查询 `signal_status`）是 `PgBackend` 的固有方法，
//! 由 RPC 边缘单点 match 暴露，不进公共面。
//!
//! ## 语义差异由各实现吸收（对上层不可见）
//!
//! - run 创建：SQLite 走 initializing → run_started 两段协议；
//!   Postgres 走单事务原子创建（DISTRIBUTED.md §3）；
//! - 信号：SQLite 进程内同步交付（响应只回显客户端提供的 signal_id，
//!   **从不伪造**）；Postgres 走持久 inbox，signal_id 必填且稳定复用，
//!   可能返回 pending；
//! - 订阅：SQLite 是进程内 broadcast（零成本流，不 spawn）；
//!   Postgres 按 run_id 维护游标轮询共享日志。
//!
//! 「只有 published 版本可执行 + 创建前校验定义」这条规则只有一份实现：
//! [`resolve_runnable_definition`]，两个后端的 create_run 都走它。
//!
//! 依赖方向（不可反转）：
//!
//! ```text
//! flow-rpc ──> flow-backend ──> flow-engine ──> flow-dto
//!                         ├──> flow-store ──> flow-dto
//!                         └──> flow-pg ────> flow-engine, flow-dto
//! flow-store ✗ flow-pg    （两个后端互相独立，互不感知）
//! flow-engine ✗ flow-*    （引擎不依赖存储；状态出口走 RunEventSink）
//! ```

use std::sync::Arc;

use futures::stream::BoxStream;
use serde_json::Value;
use thiserror::Error;

pub use flow_dto::{RunRecord, WorkflowSummary, WorkflowVersion};
pub use flow_engine::{Definition, Envelope, RunState};

mod child;
mod pg;
mod sqlite;

pub use child::LocalChildLauncher;
pub use pg::PgBackend;
pub use sqlite::{recover_unfinished, SqliteBackend, StoreObserver};

/// 适配层错误：两个后端原生错误的公共超集。
/// 上层（RPC）据此映射 JSON-RPC 错误码，不再分叉处理具体后端的错误类型。
///
/// 注意：没有 `Unsupported` 变体。公共面上的方法两个后端都必须诚实实现；
/// 后端专属能力在 RPC 边缘 match 枚举时处理，不存在"声明了但不会"。
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("工作流不存在：{0}")]
    WorkflowNotFound(String),
    #[error("版本不存在：{0} v{1}")]
    VersionNotFound(String, i64),
    #[error("版本尚未发布：{0} v{1}")]
    VersionNotPublished(String, i64),
    #[error("run 不存在：{0}")]
    RunNotFound(String),
    #[error("参数非法：{0}")]
    Invalid(String),
    #[error("冲突：{0}")]
    Conflict(String),
    #[error("内部错误：{0}")]
    Internal(String),
}

impl BackendError {
    /// 便捷构造，避免调用侧到处 `to_string()`。
    pub fn internal(err: impl std::fmt::Display) -> BackendError {
        BackendError::Internal(err.to_string())
    }
}

/// run.start 的输入（AnyBackend::create_run）。
pub struct CreateRun {
    pub workflow_id: String,
    pub version: Option<i64>,
    pub input: Value,
}

#[derive(Debug)]
pub struct CreatedRun {
    pub run_id: String,
    pub workflow_version: i64,
}

/// 信号/取消请求。Postgres 后端要求 signal_id 稳定复用（幂等）；
/// SQLite 后端进程内同步交付，signal_id 仅原样回显、缺省时不伪造。
#[derive(Debug)]
pub struct SignalRequest {
    pub run_id: String,
    pub signal_id: Option<String>,
    pub node_id: String,
    pub payload: Value,
}

/// 信号落账结果（DISTRIBUTED.md §6.1）：
/// - `delivered=true`：已写入事件并生效；
/// - `status=pending`：已入队尚未处理（仅 Postgres），客户端用
///   `PgBackend::signal_status` 查询；
/// - `status=rejected`：非法请求被拒，error 携带原因。
///
/// `signal_id` 只在真有一个可查询的 id 时出现（Postgres inbox）。
/// SQLite 同步交付没有账可查，回显客户端提供的 id 或不带。
#[derive(Debug, Clone)]
pub struct SignalAck {
    pub signal_id: Option<String>,
    pub status: String,
    pub delivered: bool,
    pub event_seq: Option<u64>,
    pub error: Option<Value>,
}

/// 后端选择：闭集枚举，不是 trait 对象。
/// 两个臂都是 `Arc`，克隆廉价；上层（flow-rpc）按需 match，
/// 其余方法静态派发（每方法两个臂，编译器保证谁也不许漏写）。
#[derive(Clone)]
pub enum AnyBackend {
    /// canonical：SQLite + event.jsonl（DESIGN.md 全部语义）。
    Sqlite(Arc<SqliteBackend>),
    /// 可替代：Postgres 共享日志 + epoch 租约 + 持久 inbox（DISTRIBUTED.md）。
    Postgres(Arc<PgBackend>),
}

impl AnyBackend {
    pub fn name(&self) -> &'static str {
        match self {
            AnyBackend::Sqlite(_) => "sqlite",
            AnyBackend::Postgres(_) => "postgres",
        }
    }

    /// 启动日志用的一段描述（db 路径 / 实例与角色，不含敏感信息）。
    pub fn describe(&self) -> String {
        match self {
            AnyBackend::Sqlite(b) => b.describe(),
            AnyBackend::Postgres(b) => b.describe(),
        }
    }

    /// 进入可服务状态（幂等；调用一次，在 RPC 起服务之前）：
    /// SQLite = 崩溃恢复（recover_unfinished）；
    /// Postgres = executor 扫描循环（gateway 角色为空操作）。
    pub async fn start(&self) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.start().await,
            AnyBackend::Postgres(b) => b.start().await,
        }
    }

    /// 优雅停机：停止派发、abort inflight、按后端语义释放资源。
    pub async fn shutdown(&self) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.shutdown().await,
            AnyBackend::Postgres(b) => b.shutdown().await,
        }
    }

    pub async fn create_workflow(&self, name: &str) -> Result<String, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.create_workflow(name).await,
            AnyBackend::Postgres(b) => b.create_workflow(name).await,
        }
    }

    pub async fn update_workflow(
        &self,
        workflow_id: &str,
        definition: &Value,
    ) -> Result<i64, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.update_workflow(workflow_id, definition).await,
            AnyBackend::Postgres(b) => b.update_workflow(workflow_id, definition).await,
        }
    }

    pub async fn publish(&self, workflow_id: &str, version: i64) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.publish(workflow_id, version).await,
            AnyBackend::Postgres(b) => b.publish(workflow_id, version).await,
        }
    }

    pub async fn get_version(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.get_version(workflow_id, version).await,
            AnyBackend::Postgres(b) => b.get_version(workflow_id, version).await,
        }
    }

    pub async fn latest_published(&self, workflow_id: &str) -> Result<Option<i64>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.latest_published(workflow_id).await,
            AnyBackend::Postgres(b) => b.latest_published(workflow_id).await,
        }
    }

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.list_workflows().await,
            AnyBackend::Postgres(b) => b.list_workflows().await,
        }
    }

    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.delete_workflow(workflow_id).await,
            AnyBackend::Postgres(b) => b.delete_workflow(workflow_id).await,
        }
    }

    /// 创建并启动 run（run.start）。初始化协议由实现吸收：
    /// SQLite 两段式（initializing → run_started），Postgres 单事务原子创建。
    /// published 解析与定义校验单点在 [`resolve_runnable_definition`]。
    pub async fn create_run(&self, spec: CreateRun) -> Result<CreatedRun, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.create_run(spec).await,
            AnyBackend::Postgres(b) => b.create_run(spec).await,
        }
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.get_run(run_id).await,
            AnyBackend::Postgres(b) => b.get_run(run_id).await,
        }
    }

    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.list_runs(workflow_id, limit).await,
            AnyBackend::Postgres(b) => b.list_runs(workflow_id, limit).await,
        }
    }

    pub async fn read_events(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
    ) -> Result<Vec<Envelope>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.read_events(run_id, from_seq).await,
            AnyBackend::Postgres(b) => b.read_events(run_id, from_seq).await,
        }
    }

    pub async fn snapshot(&self, run_id: &str) -> Result<RunState, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.snapshot(run_id).await,
            AnyBackend::Postgres(b) => b.snapshot(run_id).await,
        }
    }

    /// 本进程正在驱动的 run（live 标记）。
    pub fn is_live(&self, run_id: &str) -> bool {
        match self {
            AnyBackend::Sqlite(b) => b.is_live(run_id),
            AnyBackend::Postgres(b) => b.is_live(run_id),
        }
    }

    /// 交付信号（human_task / 副作用节点裁决）。
    pub async fn signal(&self, req: SignalRequest) -> Result<SignalAck, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.signal(req).await,
            AnyBackend::Postgres(b) => b.signal(req).await,
        }
    }

    /// 取消 run。SQLite 仅对活着的 run 生效；Postgres 经持久 inbox 消费。
    pub async fn cancel(
        &self,
        run_id: &str,
        signal_id: Option<String>,
    ) -> Result<SignalAck, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.cancel(run_id, signal_id).await,
            AnyBackend::Postgres(b) => b.cancel(run_id, signal_id).await,
        }
    }

    /// 订阅事件流。SQLite：进程内 broadcast（Lagged 丢事件，用 run.events 补齐）；
    /// Postgres：按 run_id 维护 last_seq 轮询共享日志（DISTRIBUTED.md §8）。
    /// 指定 run_id 且该 run 已终结追平后，流自然结束。
    pub fn subscribe(&self, run_id: Option<String>) -> BoxStream<'static, Envelope> {
        match self {
            AnyBackend::Sqlite(b) => b.subscribe(run_id),
            AnyBackend::Postgres(b) => b.subscribe(run_id),
        }
    }
}

// ---- 「可执行版本」规则：单一实现，两个后端共用 ----

/// 元数据读取的最小面，仅供 [`resolve_runnable_definition`] 复用规则用，
/// 不是公共后端契约。
#[async_trait::async_trait]
pub(crate) trait VersionSource: Send + Sync {
    async fn get_version_by_ref(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, BackendError>;
    async fn latest_published_version(
        &self,
        workflow_id: &str,
    ) -> Result<Option<i64>, BackendError>;
}

/// 解析可执行版本并校验定义——run.start 的完整前置规则，只有这一份实现：
/// 1. 显式 version 校验存在且 published；省略取 latest published；
/// 2. definition 解析 + `Definition::validate`（建图即校验，不等到运行）。
///
/// 两个后端的 `create_run` 都从这里拿 (version, definition)，
/// 不允许各写一份「只有 published 可执行」。
pub(crate) async fn resolve_runnable_definition(
    source: &dyn VersionSource,
    workflow_id: &str,
    version: Option<i64>,
) -> Result<(i64, Definition), BackendError> {
    let version = match version {
        Some(version) => version,
        None => source
            .latest_published_version(workflow_id)
            .await?
            .ok_or_else(|| {
                BackendError::Invalid(format!(
                    "工作流 {workflow_id} 没有已发布版本，先 publish 再执行"
                ))
            })?,
    };
    let stored = source
        .get_version_by_ref(workflow_id, Some(version))
        .await?;
    if !stored.is_published() {
        return Err(BackendError::VersionNotPublished(
            workflow_id.to_string(),
            version,
        ));
    }
    let definition: Definition = serde_json::from_value(stored.definition)
        .map_err(|e| BackendError::Invalid(format!("定义结构非法：{e}")))?;
    definition
        .validate()
        .map_err(|e| BackendError::Invalid(e.to_string()))?;
    Ok((version, definition))
}

/// 工厂：按 FLOW_BACKEND 构造后端。
/// `sqlite`（缺省，canonical）| `postgres`（可替代，多节点执行）。
/// 这是整个进程唯一读取 FLOW_BACKEND 的地方。
pub async fn open_from_env() -> Result<AnyBackend, BackendError> {
    let kind = std::env::var("FLOW_BACKEND").unwrap_or_else(|_| "sqlite".into());
    match kind.as_str() {
        "sqlite" => Ok(AnyBackend::Sqlite(Arc::new(
            SqliteBackend::from_env().await?,
        ))),
        "postgres" | "postgresql" => {
            Ok(AnyBackend::Postgres(Arc::new(PgBackend::from_env().await?)))
        }
        other => Err(BackendError::Invalid(format!(
            "未知 FLOW_BACKEND：{other}（支持 sqlite | postgres）"
        ))),
    }
}
