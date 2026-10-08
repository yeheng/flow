//! SQLite 后端（**canonical，焊死的默认架构**）：DESIGN.md 的单机实现。
//!
//! 组合三件事并吸收到 [`AnyBackend`](crate::AnyBackend) 边界之后：
//! - `flow_store::Store`：workflow / versions / runs 元数据（SQLite WAL）；
//! - `flow_engine::Engine`：每 run 一个 `event.jsonl` 的单写者 Driver；
//! - `recover_unfinished`：启动时从完整磁盘日志恢复未结束的 run。
//!
//! 与 Postgres 后端的语义差异（对上层不可见）：
//! - run 创建：insert initializing → 持久化 run_started → 启动 Driver（两段协议）；
//! - 信号：进程内同步交付。响应只回显客户端提供的 signal_id，**从不伪造**
//!   （没有持久 inbox，伪造一个查不到的 id 是欺骗客户端）；
//! - 订阅：进程内 broadcast 直接包装成流，零 spawn、零 channel。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;

use flow_engine::{
    DbRunStatus, Engine, EngineError, Envelope, ResumeOutcome, RunObserver, RunState, Signal,
    StartRun, StatusUpdate,
};
use flow_store::{Store, StoreError};

use crate::child::LocalChildLauncher;
use crate::{
    resolve_runnable_definition, BackendError, CreateRun, CreatedRun, NodeTemplate,
    NodeTemplateSummary, RunRecord, RunStats, Schedule, SignalAck, SignalRequest, Webhook,
    WorkflowSummary, WorkflowVersion,
};

/// 把引擎的 run 状态变化落到 runs 表。引擎本身不依赖存储实现，适配在 SQLite
/// 后端内部完成。
pub struct StoreObserver {
    store: Arc<Store>,
}

impl StoreObserver {
    pub fn new(store: Arc<Store>) -> StoreObserver {
        StoreObserver { store }
    }
}

impl RunObserver for StoreObserver {
    fn on_status<'a>(&'a self, update: StatusUpdate<'a>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Err(err) = self
                .store
                .set_run_status(
                    update.run_id,
                    update.status.as_str(),
                    update.output,
                    update.error,
                    None,
                )
                .await
            {
                tracing::error!(run_id = %update.run_id, error = %err, "回写 run 状态失败");
            }
        })
    }
}

/// SQLite + event.jsonl 后端。
pub struct SqliteBackend {
    store: Arc<Store>,
    engine: Arc<Engine>,
    data_dir: PathBuf,
    db_path: PathBuf,
    /// data_dir 的排他 flock，随后端存续持有：SQLite 后端是单进程设计，
    /// 第二个实例（哪怕同机）共享 data_dir 会被启动恢复劫持成双写者
    /// （重复 seq + 重复副作用，DESIGN §12.14）。锁目录本身的 fd——
    /// 无锁文件残留，进程死亡（含 SIGKILL）内核自动释放。Drop 即解锁。
    _dir_lock: std::fs::File,
}

impl SqliteBackend {
    pub async fn open(
        data_dir: impl Into<PathBuf>,
        db_path: impl AsRef<Path>,
    ) -> Result<SqliteBackend, BackendError> {
        let data_dir = data_dir.into();
        let db_path = db_path.as_ref().to_path_buf();
        let io_err =
            |err: std::io::Error| BackendError::internal(format!("data_dir 访问失败：{err}"));
        std::fs::create_dir_all(&data_dir).map_err(io_err)?;
        // 排他 flock：把「单进程假设」从文档焊成代码强制（DESIGN §12.14）。
        // LOCK_NB 立即失败——排队等待只会掩盖部署错误（正确的多节点形态是
        // postgres 后端，不是多个 sqlite 实例共享磁盘）。同进程二次 open
        // （独立 fd）同样被拒：flock 按 open file description 判定。
        let dir_lock = std::fs::File::open(&data_dir).map_err(io_err)?;
        fs2::FileExt::try_lock_exclusive(&dir_lock).map_err(|err| {
            BackendError::Conflict(format!(
                "data_dir {} 已被另一个 flow 实例持有（{}）；\
                 SQLite 后端是单进程设计，多节点部署请用 postgres 后端",
                data_dir.display(),
                err
            ))
        })?;
        let store = Arc::new(Store::open(&db_path).await.map_err(sqlite_err)?);
        let engine = Arc::new(Engine::new(
            &data_dir,
            Arc::new(StoreObserver::new(store.clone())),
        ));
        let backend = SqliteBackend {
            store,
            engine,
            data_dir,
            db_path,
            _dir_lock: dir_lock,
        };
        // 两阶段注入：launcher 依赖 Engine，Engine 的 Driver 需要 launcher
        backend
            .engine
            .set_child_launcher(Arc::new(LocalChildLauncher::new(
                backend.store.clone(),
                backend.engine.clone(),
            )));
        Ok(backend)
    }

    /// FLOW_DATA_DIR（默认 data）/ FLOW_DB（默认 <data_dir>/flow.db）。
    pub async fn from_env() -> Result<SqliteBackend, BackendError> {
        let data_dir =
            PathBuf::from(std::env::var("FLOW_DATA_DIR").unwrap_or_else(|_| "data".into()));
        let db_path = std::env::var("FLOW_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("flow.db"));
        Self::open(data_dir, db_path).await
    }

    /// 测试与诊断入口：底层 Store。
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// 测试与诊断入口：底层 Engine。
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn name(&self) -> &'static str {
        "sqlite"
    }

    // ---- 可复用节点模板（store 直通） ----

    pub async fn template_create(
        &self,
        name: &str,
        category: Option<&str>,
        nodes: &Value,
        edges: &Value,
    ) -> Result<NodeTemplate, BackendError> {
        self.store
            .create_template(name, category, nodes, edges)
            .await
            .map_err(sqlite_err)
    }

    pub async fn template_list(&self) -> Result<Vec<NodeTemplateSummary>, BackendError> {
        self.store
            .list_template_summaries()
            .await
            .map_err(sqlite_err)
    }

    pub async fn template_get(&self, id: &str) -> Result<NodeTemplate, BackendError> {
        self.store.get_template(id).await.map_err(sqlite_err)
    }

    pub async fn template_update(
        &self,
        id: &str,
        name: Option<&str>,
        category: Option<Option<&str>>,
        nodes: Option<&Value>,
        edges: Option<&Value>,
    ) -> Result<NodeTemplate, BackendError> {
        self.store
            .update_template(id, name, category, nodes, edges)
            .await
            .map_err(sqlite_err)
    }

    pub async fn template_delete(&self, id: &str) -> Result<bool, BackendError> {
        self.store.delete_template(id).await.map_err(sqlite_err)
    }

    pub fn describe(&self) -> String {
        format!("db = {}", self.db_path.display())
    }

    /// 启动即崩溃恢复：把 runs 表里未结束的 run 从 event.jsonl 折叠回来继续跑。
    pub async fn start(&self) -> Result<(), BackendError> {
        for (run_id, error) in recover_unfinished(self).await? {
            tracing::error!(run_id = %run_id, error = %error, "恢复 run 失败");
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), BackendError> {
        // 单机后端没有后台扫描循环；Driver 生命周期由 Engine 注册表管理
        Ok(())
    }

    pub async fn create_workflow(&self, name: &str) -> Result<String, BackendError> {
        self.store.create_workflow(name).await.map_err(sqlite_err)
    }

    pub async fn update_workflow(
        &self,
        workflow_id: &str,
        definition: &Value,
    ) -> Result<i64, BackendError> {
        self.store
            .update_workflow(workflow_id, definition)
            .await
            .map_err(sqlite_err)
    }

    pub async fn publish(&self, workflow_id: &str, version: i64) -> Result<(), BackendError> {
        self.store
            .publish(workflow_id, version)
            .await
            .map_err(sqlite_err)
    }

    pub async fn get_version(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, BackendError> {
        self.store
            .get_version(workflow_id, version)
            .await
            .map_err(sqlite_err)
    }

    pub async fn latest_published(&self, workflow_id: &str) -> Result<Option<i64>, BackendError> {
        self.store
            .latest_published(workflow_id)
            .await
            .map_err(sqlite_err)
    }

    pub async fn list_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowVersion>, BackendError> {
        self.store
            .list_versions(workflow_id)
            .await
            .map_err(sqlite_err)
    }

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, BackendError> {
        self.store.list_workflows().await.map_err(sqlite_err)
    }

    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), BackendError> {
        self.store
            .delete_workflow(workflow_id)
            .await
            .map_err(sqlite_err)
    }

    // ---- schedules / webhooks ----

    pub async fn create_schedule(
        &self,
        workflow_id: &str,
        cron_expr: &str,
        input: Option<&Value>,
        enabled: bool,
    ) -> Result<Schedule, BackendError> {
        self.store
            .create_schedule(workflow_id, cron_expr, input, enabled)
            .await
            .map_err(sqlite_err)
    }

    pub async fn list_schedules(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<Schedule>, BackendError> {
        self.store
            .list_schedules(workflow_id)
            .await
            .map_err(sqlite_err)
    }

    pub async fn update_schedule(
        &self,
        id: &str,
        cron_expr: Option<&str>,
        input: Option<Option<Value>>,
        enabled: Option<bool>,
    ) -> Result<(), BackendError> {
        self.store
            .update_schedule(id, cron_expr, input, enabled)
            .await
            .map_err(sqlite_err)
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<(), BackendError> {
        self.store.delete_schedule(id).await.map_err(sqlite_err)
    }

    /// sqlite 臂的 fire_at 列是 TEXT：统一 RFC3339 秒精度，保证去重键稳定。
    pub async fn try_insert_fire(
        &self,
        schedule_id: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, BackendError> {
        let key = fire_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.store
            .try_insert_fire(schedule_id, &key)
            .await
            .map_err(sqlite_err)
    }

    /// 撤销一次触发去重（sqlite 臂的 fire_at 列是 TEXT，同一 RFC3339 秒精度键）。
    pub async fn delete_fire(
        &self,
        schedule_id: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), BackendError> {
        let key = fire_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.store
            .delete_fire(schedule_id, &key)
            .await
            .map_err(sqlite_err)
    }

    pub async fn create_webhook(&self, workflow_id: &str) -> Result<Webhook, BackendError> {
        self.store
            .create_webhook(workflow_id)
            .await
            .map_err(sqlite_err)
    }

    pub async fn list_webhooks(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<Webhook>, BackendError> {
        self.store
            .list_webhooks(workflow_id)
            .await
            .map_err(sqlite_err)
    }

    pub async fn get_webhook(&self, token: &str) -> Result<Option<Webhook>, BackendError> {
        self.store.get_webhook(token).await.map_err(sqlite_err)
    }

    pub async fn set_webhook_enabled(
        &self,
        token: &str,
        enabled: bool,
    ) -> Result<(), BackendError> {
        self.store
            .set_webhook_enabled(token, enabled)
            .await
            .map_err(sqlite_err)
    }

    pub async fn delete_webhook(&self, token: &str) -> Result<(), BackendError> {
        self.store.delete_webhook(token).await.map_err(sqlite_err)
    }

    /// run.start 的单机协议：校验 published → insert initializing →
    /// 持久化 run_started → 启动 Driver；初始化错误回写 failed（DESIGN.md §9）。
    pub async fn create_run(&self, spec: CreateRun) -> Result<CreatedRun, BackendError> {
        // 规则单点：published 解析 + 定义校验只有 resolve_runnable_definition 一份
        let (version, definition) =
            resolve_runnable_definition(self, &spec.workflow_id, spec.version).await?;

        let run_id = uuid::Uuid::now_v7().to_string();
        self.store
            .insert_run(
                &run_id,
                &spec.workflow_id,
                version,
                &spec.input,
                DbRunStatus::Initializing.as_str(),
                &spec.source,
                spec.source_detail.as_deref(),
            )
            .await
            .map_err(sqlite_err)?;

        let spec = StartRun {
            run_id,
            workflow_id: spec.workflow_id,
            workflow_version: version,
            definition,
            input: spec.input,
            depth: 0,
        };
        let run_id = spec.run_id.clone();
        if let Err(err) = self.engine.start_run(spec).await {
            let message = err.to_string();
            if let Err(write_err) = self
                .store
                .set_run_status(
                    &run_id,
                    DbRunStatus::Failed.as_str(),
                    None,
                    Some(&message),
                    None,
                )
                .await
            {
                tracing::error!(run_id = %run_id, error = %write_err, "回写 run failed 状态失败");
            }
            return Err(engine_err(err));
        }
        Ok(CreatedRun {
            run_id,
            workflow_version: version,
        })
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, BackendError> {
        self.store.get_run(run_id).await.map_err(sqlite_err)
    }

    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        status: Option<&str>,
        source: Option<&str>,
        before_run_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, BackendError> {
        self.store
            .list_runs(workflow_id, status, source, before_run_id, limit)
            .await
            .map_err(sqlite_err)
    }

    pub async fn run_stats(&self, workflow_id: Option<&str>) -> Result<RunStats, BackendError> {
        self.store.run_stats(workflow_id).await.map_err(sqlite_err)
    }

    pub async fn read_events(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
    ) -> Result<Vec<Envelope>, BackendError> {
        self.engine
            .read_events(run_id, from_seq)
            .await
            .map_err(engine_err)
    }

    pub async fn snapshot(&self, run_id: &str) -> Result<RunState, BackendError> {
        self.engine.snapshot(run_id).await.map_err(engine_err)
    }

    pub fn is_live(&self, run_id: &str) -> bool {
        self.engine.is_live(run_id)
    }

    /// 进程内同步交付。signal_id 只回显客户端提供的值，缺省时响应不带——
    /// 没有持久 inbox，伪造一个查不到的 id 是欺骗客户端。
    pub async fn signal(&self, req: SignalRequest) -> Result<SignalAck, BackendError> {
        let outcome = self
            .engine
            .signal(
                &req.run_id,
                Signal {
                    node_id: req.node_id,
                    payload: req.payload,
                },
            )
            .await;
        if let Err(EngineError::NotLive(_)) = &outcome {
            // registry 无此 run：区分「不存在」与「存在但不在跑」，
            // 与 Postgres 落账语义对齐（RunNotFound / Conflict）
            let run = self.store.get_run(&req.run_id).await.map_err(sqlite_err)?;
            return Err(BackendError::Conflict(format!(
                "run {} 当前不在运行中（状态 {}）",
                run.id, run.status
            )));
        }
        outcome.map_err(engine_err)?;
        Ok(SignalAck {
            signal_id: req.signal_id,
            status: "applied".into(),
            delivered: true,
            event_seq: None,
            error: None,
        })
    }

    pub async fn cancel(
        &self,
        run_id: &str,
        signal_id: Option<String>,
    ) -> Result<SignalAck, BackendError> {
        if self.engine.cancel(run_id).await {
            // 回显客户端提供的 signal_id（与 signal()/PG 臂一致）；没有持久
            // inbox，缺省时不伪造
            return Ok(SignalAck {
                signal_id,
                status: "applied".into(),
                delivered: true,
                event_seq: None,
                error: None,
            });
        }
        let run = self.store.get_run(run_id).await.map_err(sqlite_err)?;
        Err(BackendError::Conflict(format!(
            "run {} 当前不在运行中（状态 {}）",
            run.id, run.status
        )))
    }

    /// 不指定 run_id：纯实时增量（Lagged 丢事件时上层用 run.events 补齐）；
    /// 指定 run_id：回放 + 追流、缺口补齐、终态自然结束（与 PG 臂共用 run_tail）。
    pub fn subscribe(
        &self,
        run_id: Option<String>,
    ) -> futures::stream::BoxStream<'static, Envelope> {
        match run_id {
            None => crate::broadcast_tail(self.engine.subscribe(), None),
            Some(run_id) => crate::run_tail::run_tail(
                Arc::new(EngineReader(self.engine.clone())),
                self.engine.subscribe(),
                run_id,
            ),
        }
    }
}

/// 引擎文件日志的读取适配（run_tail 的 EventReader 实现）。
struct EngineReader(Arc<Engine>);

impl crate::run_tail::EventReader for EngineReader {
    fn read_events<'a>(
        &'a self,
        run_id: &'a str,
        from_seq: Option<u64>,
    ) -> BoxFuture<'a, Result<Vec<Envelope>, BackendError>> {
        Box::pin(async move {
            self.0
                .read_events(run_id, from_seq)
                .await
                .map_err(engine_err)
        })
    }
}

#[async_trait::async_trait]
impl crate::VersionSource for SqliteBackend {
    async fn get_version_by_ref(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, BackendError> {
        self.get_version(workflow_id, version).await
    }

    async fn latest_published_version(
        &self,
        workflow_id: &str,
    ) -> Result<Option<i64>, BackendError> {
        self.latest_published(workflow_id).await
    }
}

fn sqlite_err(err: StoreError) -> BackendError {
    match err {
        StoreError::WorkflowNotFound(id) => BackendError::WorkflowNotFound(id),
        StoreError::VersionNotFound(id, v) => BackendError::VersionNotFound(id, v),
        StoreError::VersionNotPublished(id, v) => BackendError::VersionNotPublished(id, v),
        StoreError::RunNotFound(id) => BackendError::RunNotFound(id),
        StoreError::ScheduleNotFound(id) => BackendError::ScheduleNotFound(id),
        StoreError::WebhookNotFound(token) => BackendError::WebhookNotFound(token),
        StoreError::TemplateNotFound(id) => BackendError::TemplateNotFound(id),
        StoreError::TemplateNameTaken(name) => BackendError::TemplateNameTaken(name),
        StoreError::Conflict(msg) => BackendError::Conflict(msg),
        StoreError::InvalidStatus(s) => BackendError::Internal(format!("非法的 run 状态：{s}")),
        StoreError::InvalidSource(s) => BackendError::Internal(format!("非法的 run 来源：{s}")),
        other => BackendError::internal(other),
    }
}

fn engine_err(err: EngineError) -> BackendError {
    match err {
        EngineError::RunNotFound(id) => BackendError::RunNotFound(id),
        EngineError::RunExists(id) => BackendError::Conflict(format!("run {id} 已存在")),
        EngineError::NotLive(msg) => BackendError::Conflict(msg),
        EngineError::InvalidSignal(msg) => BackendError::Invalid(msg),
        EngineError::InvalidDefinition(msg) => BackendError::Invalid(msg),
        other => BackendError::internal(other),
    }
}

/// 启动恢复：把 runs 表里未结束的 run 从 event.jsonl 折叠回来继续跑。
///
/// 事件日志是权威：日志已终结但 DB 未回填的情况在这里补齐。
/// 日志缺失或损坏时隔离并报告错误，不猜测副作用（DESIGN.md §7）。
pub async fn recover_unfinished(
    backend: &SqliteBackend,
) -> Result<Vec<(String, String)>, BackendError> {
    let store = &backend.store;
    let engine = &backend.engine;
    let mut failures = Vec::new();
    for run in store.unfinished_runs().await.map_err(sqlite_err)? {
        let version = match store
            .get_version(&run.workflow_id, Some(run.workflow_version))
            .await
        {
            Ok(version) => version,
            Err(err) => {
                failures.push((run.id.clone(), err.to_string()));
                continue;
            }
        };
        let definition: flow_engine::Definition = match serde_json::from_value(version.definition) {
            Ok(definition) => definition,
            Err(err) => {
                failures.push((run.id.clone(), format!("定义无法解析：{err}")));
                continue;
            }
        };

        let spec = StartRun {
            run_id: run.id.clone(),
            workflow_id: run.workflow_id.clone(),
            workflow_version: run.workflow_version,
            definition,
            input: run.input.clone(),
            // 恢复路径：深度以日志中的 run_started 为准，这里只是占位
            depth: 0,
        };

        match engine.resume_run(spec).await {
            Ok(ResumeOutcome::Resumed) => {
                tracing::info!(run_id = %run.id, "恢复未完成的 run");
            }
            Ok(ResumeOutcome::AlreadyTerminal(boxed)) => {
                let terminal = *boxed;
                // 崩溃发生在「事件已落盘、DB 未回填」之间：以事件为准修正 DB。
                // status 映射唯一来源是 RunPhase::as_db_status（PG 臂
                // reconcile_terminal 共用同一份，不各写一遍 match）。
                let status = terminal.phase.as_db_status();
                match store
                    .set_run_status(
                        &run.id,
                        status.as_str(),
                        terminal.output.as_ref(),
                        terminal.fatal_error.as_deref(),
                        // 结束时刻以事件日志为准，不用重启时刻顶替
                        terminal.ended_at.as_ref(),
                    )
                    .await
                {
                    Ok(()) => {
                        tracing::info!(run_id = %run.id, "事件日志已终结，回填 DB 状态")
                    }
                    // 单个 run 的回填失败不中断整个恢复循环：其余 run 照常恢复
                    Err(err) => failures.push((run.id.clone(), format!("回填终态失败：{err}"))),
                }
            }
            Err(err) => {
                let message = err.to_string();
                if matches!(
                    err,
                    EngineError::RunNotFound(_) | EngineError::LogCorrupted(_)
                ) {
                    // 日志缺失不是「重放副作用安全」的证据（DESIGN.md §7.2）
                    let status = if run.status == DbRunStatus::Initializing.as_str() {
                        DbRunStatus::Failed
                    } else {
                        DbRunStatus::AwaitingResume
                    };
                    if let Err(err) = store
                        .set_run_status(&run.id, status.as_str(), None, Some(&message), None)
                        .await
                    {
                        failures.push((run.id.clone(), format!("隔离投影失败：{err}")));
                    }
                }
                failures.push((run.id.clone(), message));
            }
        }
    }
    Ok(failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 状态词汇表契约：DbRunStatus 的每个取值都必须被 store 接受；
    /// 词汇表外的字符串必须在写入时被拒绝——否则 run 会从崩溃恢复扫描里静默消失。
    /// （词汇表本身现在单一来源在 flow-dto，此测试钉住 store 写入口真的在校验。）
    #[tokio::test]
    async fn engine_run_statuses_are_the_only_statuses_store_accepts() {
        let root = flow_test_support::io::TempDir::new("flow-backend-status-contract");
        let store = Store::open(root.join("flow.db")).await.unwrap();
        let wf = store.create_workflow("wf").await.unwrap();
        store
            .update_workflow(&wf, &json!({"nodes": []}))
            .await
            .unwrap();

        let statuses = [
            DbRunStatus::Initializing,
            DbRunStatus::Running,
            DbRunStatus::AwaitingResume,
            DbRunStatus::Succeeded,
            DbRunStatus::Failed,
            DbRunStatus::Cancelled,
        ];
        for (i, status) in statuses.iter().enumerate() {
            let run_id = format!("r-{i}");
            store
                .insert_run(
                    &run_id,
                    &wf,
                    1,
                    &Value::Null,
                    status.as_str(),
                    "manual",
                    None,
                )
                .await
                .unwrap();
            store
                .set_run_status(&run_id, status.as_str(), None, None, None)
                .await
                .unwrap();
        }

        // 非终态必须进入未完成扫描（这是恢复的输入）
        assert_eq!(store.unfinished_runs().await.unwrap().len(), 3);

        // 词汇表外的状态在写入时当场报错
        let err = store
            .insert_run("r-unknown", &wf, 1, &Value::Null, "paused", "manual", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("paused"), "{err}");

        drop(store);
        // TempDir 的 Drop 负责删目录——断言失败也不会漏垃圾
    }

    /// 「只有 published 可执行 + 创建前校验」规则的唯一真相源测试：
    /// resolve_runnable_definition 是两个后端 create_run 共用的前置，
    /// 规则断言钉在这里一份，不再分散在各后端。
    #[tokio::test]
    async fn resolve_runnable_definition_enforces_published_and_validates() {
        let root = flow_test_support::io::TempDir::new("flow-backend-resolve");
        let backend = SqliteBackend::open(root.path(), root.join("flow.db"))
            .await
            .unwrap();
        let wf = backend.create_workflow("t").await.unwrap();

        // 从未发布过：明确报错，不猜测
        let err = resolve_runnable_definition(&backend, &wf, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("没有已发布版本"), "{err}");

        // draft 版本显式指定：拒绝
        let v1 = backend
            .update_workflow(&wf, &def_line("return 1;"))
            .await
            .unwrap();
        let err = resolve_runnable_definition(&backend, &wf, Some(v1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::BackendError::VersionNotPublished(..)),
            "{err}"
        );

        // 发布后：省略版本取 latest published，定义解析校验通过
        backend.publish(&wf, v1).await.unwrap();
        let (version, definition) = resolve_runnable_definition(&backend, &wf, None)
            .await
            .unwrap();
        assert_eq!(version, v1);
        assert_eq!(definition.nodes.len(), 3);

        // 定义非法（start 缺失）：创建前拒绝，不等到运行
        let v2 = backend
            .update_workflow(&wf, &json!({"nodes": [], "edges": []}))
            .await
            .unwrap();
        backend.publish(&wf, v2).await.unwrap();
        let err = resolve_runnable_definition(&backend, &wf, None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::BackendError::Invalid(_)), "{err}");

        drop(backend);
        // TempDir 的 Drop 负责删目录——断言失败也不会漏垃圾
    }

    fn def_line(code: &str) -> Value {
        json!({
            "nodes": [
                {"id": "s", "type": "start"},
                {"id": "n", "type": "script", "params": {"code": code}},
                {"id": "e", "type": "end"}
            ],
            "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
        })
    }
}
