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
    DbRunStatus, Engine, EngineError, Envelope, ResumeOutcome, RunObserver, RunPhase, RunState,
    Signal, StartRun, StatusUpdate,
};
use flow_store::{Store, StoreError};

use crate::child::LocalChildLauncher;
use crate::{
    resolve_runnable_definition, BackendError, CreateRun, CreatedRun, RunRecord, SignalAck,
    SignalRequest, WorkflowSummary, WorkflowVersion,
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
}

impl SqliteBackend {
    pub async fn open(
        data_dir: impl Into<PathBuf>,
        db_path: impl AsRef<Path>,
    ) -> Result<SqliteBackend, BackendError> {
        let data_dir = data_dir.into();
        let db_path = db_path.as_ref().to_path_buf();
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

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, BackendError> {
        self.store.list_workflows().await.map_err(sqlite_err)
    }

    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), BackendError> {
        self.store
            .delete_workflow(workflow_id)
            .await
            .map_err(sqlite_err)
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
                .set_run_status(&run_id, DbRunStatus::Failed.as_str(), None, Some(&message))
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
        limit: i64,
    ) -> Result<Vec<RunRecord>, BackendError> {
        self.store
            .list_runs(workflow_id, limit)
            .await
            .map_err(sqlite_err)
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
        _signal_id: Option<String>,
    ) -> Result<SignalAck, BackendError> {
        if self.engine.cancel(run_id).await {
            return Ok(SignalAck {
                signal_id: None,
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
        StoreError::Conflict(msg) => BackendError::Conflict(msg),
        StoreError::InvalidStatus(s) => BackendError::Internal(format!("非法的 run 状态：{s}")),
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
                let status = match terminal.phase {
                    RunPhase::Succeeded => DbRunStatus::Succeeded,
                    RunPhase::Failed => DbRunStatus::Failed,
                    RunPhase::Cancelled => DbRunStatus::Cancelled,
                    RunPhase::Running => DbRunStatus::Running,
                };
                store
                    .set_run_status(
                        &run.id,
                        status.as_str(),
                        terminal.output.as_ref(),
                        terminal.fatal_error.as_deref(),
                    )
                    .await
                    .map_err(sqlite_err)?;
                tracing::info!(run_id = %run.id, "事件日志已终结，回填 DB 状态");
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
                    store
                        .set_run_status(&run.id, status.as_str(), None, Some(&message))
                        .await
                        .map_err(sqlite_err)?;
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
        let root = std::env::temp_dir().join(format!(
            "flow-backend-status-contract-{}",
            uuid::Uuid::now_v7()
        ));
        let store = Store::open(root.join("flow.db")).await.unwrap();

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
                .insert_run(&run_id, "wf", 1, &Value::Null, status.as_str())
                .await
                .unwrap();
            store
                .set_run_status(&run_id, status.as_str(), None, None)
                .await
                .unwrap();
        }

        // 非终态必须进入未完成扫描（这是恢复的输入）
        assert_eq!(store.unfinished_runs().await.unwrap().len(), 3);

        // 词汇表外的状态在写入时当场报错
        let err = store
            .insert_run("r-unknown", "wf", 1, &Value::Null, "paused")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("paused"), "{err}");

        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// 「只有 published 可执行 + 创建前校验」规则的唯一真相源测试：
    /// resolve_runnable_definition 是两个后端 create_run 共用的前置，
    /// 规则断言钉在这里一份，不再分散在各后端。
    #[tokio::test]
    async fn resolve_runnable_definition_enforces_published_and_validates() {
        let root =
            std::env::temp_dir().join(format!("flow-backend-resolve-{}", uuid::Uuid::now_v7()));
        let backend = SqliteBackend::open(&root, root.join("flow.db"))
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
        std::fs::remove_dir_all(root).unwrap();
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
