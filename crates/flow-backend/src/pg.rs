//! Postgres 后端适配器（**可替代架构**）：DISTRIBUTED.md 的共享日志 + epoch 租约 +
//! 持久 inbox，封装到 [`AnyBackend`](crate::AnyBackend) 边界之后。
//!
//! 吸收的语义差异（对上层不可见）：
//! - run 创建：单事务原子创建（insert run + seq=1 RunStarted），无 initializing 段；
//!   published 解析与定义校验单点在 `resolve_runnable_definition`；
//! - 信号/取消：持久 inbox + 等待落账，可能返回 pending；signal_id 必填且稳定复用；
//! - 订阅：进程内共享轮询器扇出共享日志增量（扫描次数与订阅者数无关），
//!   LISTEN/NOTIFY 作低延迟唤醒（DISTRIBUTED.md §8）；
//! - 生命周期：start 起 executor 扫描循环（gateway 角色跳过），shutdown 优雅停机。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;
use tokio::task::JoinHandle;

use flow_engine::{Envelope, RunState};
use flow_pg::{CreateRun as PgCreateRun, PgConfig, PgEngine, PgError};

use crate::{
    resolve_runnable_definition, BackendError, CreateRun, CreatedRun, RunRecord, SignalAck,
    SignalRequest, WorkflowSummary, WorkflowVersion,
};

/// Postgres 后端：gateway 入口 + executor 生命周期 + 只读查询。
pub struct PgBackend {
    engine: Arc<PgEngine>,
    executor_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl PgBackend {
    pub async fn connect(database_url: &str, cfg: PgConfig) -> Result<PgBackend, BackendError> {
        let engine = Arc::new(PgEngine::connect(database_url, cfg).await.map_err(pg_err)?);
        Ok(PgBackend {
            engine,
            executor_task: tokio::sync::Mutex::new(None),
        })
    }

    /// FLOW_DATABASE_URL 必填；其余见 DISTRIBUTED.md §10。
    pub async fn from_env() -> Result<PgBackend, BackendError> {
        let url = std::env::var("FLOW_DATABASE_URL")
            .map_err(|_| BackendError::Invalid("Postgres 模式必须设置 FLOW_DATABASE_URL".into()))?;
        Self::connect(&url, PgConfig::from_env()).await
    }

    /// 测试与诊断入口：底层 PgEngine。
    pub fn engine(&self) -> &Arc<PgEngine> {
        &self.engine
    }

    pub fn name(&self) -> &'static str {
        "postgres"
    }

    pub fn describe(&self) -> String {
        format!(
            "instance = {}, role = {:?}, max_runs = {}, ttl_ms = {}",
            self.engine.instance_id().unwrap_or("gateway"),
            self.engine.config().role,
            self.engine.config().max_runs,
            self.engine.config().lease_ttl.as_millis() as u64,
        )
    }

    /// 起 executor 扫描循环（peer 模式唯一的租约获取入口）；gateway 角色跳过。
    pub async fn start(&self) -> Result<(), BackendError> {
        let mut slot = self.executor_task.lock().await;
        if slot.is_some() {
            return Ok(());
        }
        if self.engine.config().role == flow_pg::Role::Gateway {
            return Ok(());
        }
        let engine = self.engine.clone();
        *slot = Some(tokio::spawn(async move {
            if let Err(err) = engine.run_executor().await {
                tracing::error!(error = %err, "executor 扫描循环退出");
            }
        }));
        Ok(())
    }

    /// 停机：停止扫描并等待本地 Driver 退出（abort inflight + 安全时释放租约）。
    pub async fn shutdown(&self) -> Result<(), BackendError> {
        self.engine.shutdown().await;
        let task = self.executor_task.lock().await.take();
        if let Some(task) = task {
            let _ = task.await;
        }
        Ok(())
    }

    pub async fn create_workflow(&self, name: &str) -> Result<String, BackendError> {
        self.engine
            .store()
            .create_workflow(name)
            .await
            .map_err(pg_err)
    }

    pub async fn update_workflow(
        &self,
        workflow_id: &str,
        definition: &Value,
    ) -> Result<i64, BackendError> {
        self.engine
            .store()
            .update_workflow(workflow_id, definition)
            .await
            .map_err(pg_err)
    }

    pub async fn publish(&self, workflow_id: &str, version: i64) -> Result<(), BackendError> {
        self.engine
            .store()
            .publish(workflow_id, version)
            .await
            .map_err(pg_err)
    }

    pub async fn get_version(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion, BackendError> {
        self.engine
            .store()
            .get_version(workflow_id, version)
            .await
            .map_err(pg_err)
    }

    pub async fn latest_published(&self, workflow_id: &str) -> Result<Option<i64>, BackendError> {
        self.engine
            .store()
            .latest_published(workflow_id)
            .await
            .map_err(pg_err)
    }

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, BackendError> {
        self.engine.store().list_workflows().await.map_err(pg_err)
    }

    pub async fn delete_workflow(&self, workflow_id: &str) -> Result<(), BackendError> {
        self.engine
            .store()
            .delete_workflow(workflow_id)
            .await
            .map_err(pg_err)
    }

    /// 单事务原子创建：insert run + seq=1 RunStarted（DISTRIBUTED.md §3）。
    /// published 解析与定义校验单点在 `resolve_runnable_definition`，
    /// 这里拿到的是已解析的版本——底层不再重复实现这条规则。
    pub async fn create_run(&self, spec: CreateRun) -> Result<CreatedRun, BackendError> {
        let (version, _definition) =
            resolve_runnable_definition(self, &spec.workflow_id, spec.version).await?;
        self.engine
            .create_run(PgCreateRun {
                workflow_id: spec.workflow_id,
                version,
                input: spec.input,
            })
            .await
            .map_err(pg_err)
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, BackendError> {
        self.engine.store().get_run(run_id).await.map_err(pg_err)
    }

    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<RunRecord>, BackendError> {
        self.engine
            .store()
            .list_runs(workflow_id, limit)
            .await
            .map_err(pg_err)
    }

    pub async fn read_events(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
    ) -> Result<Vec<Envelope>, BackendError> {
        self.engine
            .read_events(run_id, from_seq)
            .await
            .map_err(pg_err)
    }

    pub async fn snapshot(&self, run_id: &str) -> Result<RunState, BackendError> {
        self.engine.snapshot(run_id).await.map_err(pg_err)
    }

    pub fn is_live(&self, run_id: &str) -> bool {
        self.engine.is_live(run_id)
    }

    /// 持久 inbox + 等待落账（DISTRIBUTED.md §6.1）。signal_id 必填且稳定复用。
    pub async fn signal(&self, req: SignalRequest) -> Result<SignalAck, BackendError> {
        let Some(signal_id) = req.signal_id else {
            return Err(BackendError::Invalid(
                "Postgres 后端的 run.signal 必须带 signal_id（1-128 字符），重试时复用同一个 id"
                    .into(),
            ));
        };
        self.engine
            .signal(&req.run_id, &signal_id, &req.node_id, &req.payload)
            .await
            .map_err(pg_err)
    }

    /// 取消经持久 inbox：消费事务写 RunCancelled + 终态投影 + 释放租约（§6.2）。
    pub async fn cancel(
        &self,
        run_id: &str,
        signal_id: Option<String>,
    ) -> Result<SignalAck, BackendError> {
        self.engine.cancel(run_id, signal_id).await.map_err(pg_err)
    }

    /// pg 专属能力（不在 AnyBackend 公共面上）：查询持久 inbox 的落账状态。
    /// RPC 层在此方法的注册点单点 match 暴露。
    pub async fn signal_status(
        &self,
        run_id: &str,
        signal_id: &str,
    ) -> Result<SignalAck, BackendError> {
        self.engine
            .signal_status(run_id, signal_id)
            .await
            .map_err(pg_err)
    }

    /// 订阅：进程内共享轮询器把共享日志的增量扇出给所有订阅者（DISTRIBUTED.md §8），
    /// 查询次数与订阅者数无关；LISTEN/NOTIFY 是低延迟唤醒提示，正确性不依赖通知，
    /// 通知丢失由 subscribe_poll 兜底轮询兜住。
    /// 不指定 run_id：纯实时增量（共享轮询器扇出，扫描次数与订阅者数无关），
    /// 历史事件用 run.events 补齐；指定 run_id：回放 + 追流、缺口补齐、
    /// 终态自然结束（与 SQLite 臂共用 run_tail，语义一致）。
    pub fn subscribe(
        &self,
        run_id: Option<String>,
    ) -> futures::stream::BoxStream<'static, Envelope> {
        match run_id {
            None => crate::broadcast_tail(self.engine.subscribe_events(), None),
            Some(run_id) => crate::run_tail::run_tail(
                Arc::new(PgReader(self.engine.clone())),
                self.engine.subscribe_events(),
                run_id,
            ),
        }
    }
}

/// 共享日志的读取适配（run_tail 的 EventReader 实现）。
struct PgReader(Arc<PgEngine>);

impl crate::run_tail::EventReader for PgReader {
    fn read_events<'a>(
        &'a self,
        run_id: &'a str,
        from_seq: Option<u64>,
    ) -> BoxFuture<'a, Result<Vec<Envelope>, BackendError>> {
        Box::pin(async move { self.0.read_events(run_id, from_seq).await.map_err(pg_err) })
    }
}

#[async_trait::async_trait]
impl crate::VersionSource for PgBackend {
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

fn pg_err(err: PgError) -> BackendError {
    match err {
        PgError::RunNotFound(id) => BackendError::RunNotFound(id),
        PgError::WorkflowNotFound(id) => BackendError::WorkflowNotFound(id),
        PgError::VersionNotFound(id, v) => BackendError::VersionNotFound(id, v),
        PgError::VersionNotPublished(id, v) => BackendError::VersionNotPublished(id, v),
        PgError::SignalNotFound(id) => BackendError::SignalNotFound(id),
        PgError::Conflict(msg) => BackendError::Conflict(msg),
        PgError::Invalid(msg) => BackendError::Invalid(msg),
        other => BackendError::internal(other),
    }
}
