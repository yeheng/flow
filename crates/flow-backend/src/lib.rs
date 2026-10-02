//! flow-backend：后端适配层。
//!
//! **架构决策（焊死）**：单机 `SQLite + data_dir/runs/<id>/event.jsonl` 是本系统的
//! 权威与默认后端（`sqlite.rs`，DESIGN.md）；Postgres 共享日志后端（`pg.rs`）是
//! **可替代**的等价实现，用于多节点执行——其设计（租约 / 持久 inbox / 接管）记录在
//! `flow-pg` 各模块的头注释里。
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
//!   Postgres 走单事务原子创建（`flow-pg/src/lease.rs::create_run`）；
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
use futures::StreamExt;
use serde_json::Value;
use thiserror::Error;

pub use flow_dto::{
    DbRunSource, DbRunStatus, RunRecord, RunStats, Schedule, Webhook, WorkflowRunStats,
    WorkflowSummary, WorkflowVersion,
};
pub use flow_engine::{
    redact_value, secrets, Definition, Envelope, Event, LogLevel, LogStream, NodeState, NodeType,
    RunState, HTTP_METHODS,
};

mod child;
pub mod execution;
pub mod journal;
mod journal_commands;
mod journal_driver;
mod journal_execution;
pub mod journal_import;
mod pg;
mod run_tail;
mod sqlite;

pub use child::LocalChildLauncher;
pub use pg::PgBackend;
// PgBackend::connect 的公共签名暴露了 PgConfig，这里重导出让调用方能命名该类型
pub use flow_pg::PgConfig;
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
    #[error("signal 不存在：{0}")]
    SignalNotFound(String),
    #[error("schedule 不存在：{0}")]
    ScheduleNotFound(String),
    #[error("webhook 不存在：{0}")]
    WebhookNotFound(String),
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
/// source/source_detail 是触发来源归因：RPC 入口填 manual，调度器/webhook
/// 填各自的来源与 id/token，子 run 在 launcher 内部标注（不走本结构）。
pub struct CreateRun {
    pub workflow_id: String,
    pub version: Option<i64>,
    pub input: Value,
    pub source: String,
    pub source_detail: Option<String>,
}

pub use flow_dto::{CreatedRun, SignalAck};

/// 信号/取消请求。Postgres 后端要求 signal_id 稳定复用（幂等）；
/// SQLite 后端进程内同步交付，signal_id 仅原样回显、缺省时不伪造。
#[derive(Debug)]
pub struct SignalRequest {
    pub run_id: String,
    pub signal_id: Option<String>,
    pub node_id: String,
    pub payload: Value,
}

/// 后端选择：闭集枚举，不是 trait 对象。
/// 两个臂都是 `Arc`，克隆廉价；上层（flow-rpc）按需 match，
/// 其余方法静态派发（每方法两个臂，编译器保证谁也不许漏写）。
#[derive(Clone)]
pub enum AnyBackend {
    /// canonical：SQLite + event.jsonl（DESIGN.md 全部语义）。
    Sqlite(Arc<SqliteBackend>),
    /// 可替代：Postgres 共享日志 + epoch 租约 + 持久 inbox（设计见 `flow-pg` 各模块头注释）。
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

    pub async fn list_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowVersion>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.list_versions(workflow_id).await,
            AnyBackend::Postgres(b) => b.list_versions(workflow_id).await,
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

    // ---- schedules / webhooks（触发器） ----

    /// 创建 cron 调度。cron 合法性校验在 RPC 边缘（-32010），存储层只持久化。
    pub async fn create_schedule(
        &self,
        workflow_id: &str,
        cron_expr: &str,
        input: Option<&Value>,
        enabled: bool,
    ) -> Result<Schedule, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => {
                b.create_schedule(workflow_id, cron_expr, input, enabled)
                    .await
            }
            AnyBackend::Postgres(b) => {
                b.create_schedule(workflow_id, cron_expr, input, enabled)
                    .await
            }
        }
    }

    pub async fn list_schedules(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<Schedule>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.list_schedules(workflow_id).await,
            AnyBackend::Postgres(b) => b.list_schedules(workflow_id).await,
        }
    }

    /// 部分更新：None 字段不动；input 用 Option<Option<Value>> 区分「不改」与「清空」。
    pub async fn update_schedule(
        &self,
        id: &str,
        cron_expr: Option<&str>,
        input: Option<Option<Value>>,
        enabled: Option<bool>,
    ) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.update_schedule(id, cron_expr, input, enabled).await,
            AnyBackend::Postgres(b) => b.update_schedule(id, cron_expr, input, enabled).await,
        }
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.delete_schedule(id).await,
            AnyBackend::Postgres(b) => b.delete_schedule(id).await,
        }
    }

    /// 触发去重：同一 (schedule_id, fire_at) 只插入成功一次（多节点天然分布式锁）。
    pub async fn try_insert_fire(
        &self,
        schedule_id: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.try_insert_fire(schedule_id, fire_at).await,
            AnyBackend::Postgres(b) => b.try_insert_fire(schedule_id, fire_at).await,
        }
    }

    /// 撤销一次触发去重：调度器已拿到触发权但 `create_run` 遇**瞬时**故障时调用，
    /// 让下一个 tick 重试同一触发点。不撤销则该分钟的火永久丢失。
    pub async fn delete_fire(
        &self,
        schedule_id: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.delete_fire(schedule_id, fire_at).await,
            AnyBackend::Postgres(b) => b.delete_fire(schedule_id, fire_at).await,
        }
    }

    pub async fn create_webhook(&self, workflow_id: &str) -> Result<Webhook, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.create_webhook(workflow_id).await,
            AnyBackend::Postgres(b) => b.create_webhook(workflow_id).await,
        }
    }

    pub async fn list_webhooks(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<Webhook>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.list_webhooks(workflow_id).await,
            AnyBackend::Postgres(b) => b.list_webhooks(workflow_id).await,
        }
    }

    pub async fn get_webhook(&self, token: &str) -> Result<Option<Webhook>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.get_webhook(token).await,
            AnyBackend::Postgres(b) => b.get_webhook(token).await,
        }
    }

    pub async fn set_webhook_enabled(
        &self,
        token: &str,
        enabled: bool,
    ) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.set_webhook_enabled(token, enabled).await,
            AnyBackend::Postgres(b) => b.set_webhook_enabled(token, enabled).await,
        }
    }

    pub async fn delete_webhook(&self, token: &str) -> Result<(), BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.delete_webhook(token).await,
            AnyBackend::Postgres(b) => b.delete_webhook(token).await,
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
        status: Option<&str>,
        source: Option<&str>,
        before_run_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => {
                b.list_runs(workflow_id, status, source, before_run_id, limit)
                    .await
            }
            AnyBackend::Postgres(b) => {
                b.list_runs(workflow_id, status, source, before_run_id, limit)
                    .await
            }
        }
    }

    /// run.stats：GROUP BY 精确计数；workflow_id 为 None 时附带按工作流分组。
    pub async fn run_stats(&self, workflow_id: Option<&str>) -> Result<RunStats, BackendError> {
        match self {
            AnyBackend::Sqlite(b) => b.run_stats(workflow_id).await,
            AnyBackend::Postgres(b) => b.run_stats(workflow_id).await,
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

    /// 订阅事件流。SQLite：进程内 broadcast；Postgres：共享轮询器扇出共享日志增量
    ///（`flow-pg/src/subscribe.rs`，扫描次数与订阅者数无关）。不指定 run_id 时是纯实时增量
    ///（Lagged 丢事件，用 run.events 按 from_seq 补齐）；指定 run_id 先回放完整日志
    /// 再接实时增量，该 run 已终结追平后流自然结束。两臂共用 run_tail 状态机，
    /// 语义一致（缺口补齐、失败重试、按 seq 去重），契约不分叉。
    pub fn subscribe(&self, run_id: Option<String>) -> BoxStream<'static, Envelope> {
        match self {
            AnyBackend::Sqlite(b) => b.subscribe(run_id),
            AnyBackend::Postgres(b) => b.subscribe(run_id),
        }
    }
}

/// broadcast 接收端包装成流：向前转发，Lagged 丢事件不致命
///（上层用 run.events 按 from_seq 补齐），通道关闭即流结束。
/// SQLite 的实时推送与 Postgres 的全局订阅共用这一段。
pub(crate) fn broadcast_tail(
    events: tokio::sync::broadcast::Receiver<Envelope>,
    filter: Option<String>,
) -> BoxStream<'static, Envelope> {
    futures::stream::unfold(events, move |mut events| {
        // unfold 的闭包是 FnMut：filter 每次克隆进 async 块
        let filter = filter.clone();
        async move {
            loop {
                match events.recv().await {
                    Ok(envelope) => {
                        if let Some(filter) = &filter {
                            if envelope.run_id != *filter {
                                continue;
                            }
                        }
                        return Some((envelope, events));
                    }
                    // Lagged：丢事件不致命，客户端按 from_seq 补齐
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        }
    })
    .boxed()
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

/// 进程入口的 runtime 形态提示：SQLite 后端整体跑在单线程 runtime
/// （current_thread，单 OS 线程；异步任务仍并发，JS 求值/文件 IO 经
/// spawn_blocking 走独立阻塞线程），Postgres 对等模式保持多线程。
/// 与排他 flock、单连接池一起构成「单进程·单线程·单写者」三件套。
/// 只应在 main() 构造 tokio runtime 时调用一次——这是部署形态选择，
/// 不属于 RPC 请求处理路径对后端的感知（那条禁令针对 lib 内逻辑）。
pub fn prefer_current_thread_runtime() -> bool {
    match std::env::var("FLOW_BACKEND") {
        Ok(v) => !v.eq_ignore_ascii_case("postgres") && !v.eq_ignore_ascii_case("postgresql"),
        Err(_) => true, // 缺省 sqlite
    }
}

/// 工厂：按 FLOW_BACKEND 构造后端。
/// `sqlite`（缺省，canonical）| `postgres`（可替代，多节点执行）。
/// 这是进程入口选择后端的唯一地方（`prefer_current_thread_runtime` 也读同一个
/// 变量决定 runtime 形态，它在构造 runtime 之前跑，改这里要同步改那边）。
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

/// 二期 I09：按环境变量（FLOW_EXECUTION_MODE / FLOW_EXECUTOR_BIN）选择
/// 执行模式并启动调度。IPC 模式二进制缺失/版本不兼容直接失败，不静默
/// 落回进程内执行。
pub async fn start_execution_from_env(
    backend: &std::sync::Arc<crate::journal::JournalBackend>,
) -> Result<(), crate::journal::JournalError> {
    match execution::mode_from_env() {
        Ok(execution::ExecutionMode::InProcess) => backend.start_execution().await,
        Ok(mode @ execution::ExecutionMode::Ipc(_)) => backend.start_execution_ipc(mode).await,
        Err(error) if error.starts_with("invalid FLOW_EXECUTION_MODE") => Err(
            crate::journal::JournalError::Journal(flow_journal::Error::Invalid(error)),
        ),
        Err(_) => match execution::remote_options_from_env() {
            // mode_from_env 找不到执行器二进制时回落检查 remote 模式。
            Ok(options) if std::env::var("FLOW_EXECUTION_MODE").as_deref() == Ok("remote") => {
                backend.start_execution_remote(options).await
            }
            _ => backend.start_execution().await,
        },
    }
}
