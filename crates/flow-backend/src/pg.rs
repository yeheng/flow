//! Postgres 后端适配器（**可替代架构**）：DISTRIBUTED.md 的共享日志 + epoch 租约 +
//! 持久 inbox，封装到 [`AnyBackend`](crate::AnyBackend) 边界之后。
//!
//! 吸收的语义差异（对上层不可见）：
//! - run 创建：单事务原子创建（insert run + seq=1 RunStarted），无 initializing 段；
//!   published 解析与定义校验单点在 `resolve_runnable_definition`；
//! - 信号/取消：持久 inbox + 等待落账，可能返回 pending；signal_id 必填且稳定复用；
//! - 订阅：按 run_id 维护 last_seq 轮询共享日志（DISTRIBUTED.md §8）；
//! - 生命周期：start 起 executor 扫描循环（gateway 角色跳过），shutdown 优雅停机。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

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
            .map(|created| CreatedRun {
                run_id: created.run_id,
                workflow_version: created.workflow_version,
            })
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
            .map(Into::into)
            .map_err(pg_err)
    }

    /// 取消经持久 inbox：消费事务写 RunCancelled + 终态投影 + 释放租约（§6.2）。
    pub async fn cancel(
        &self,
        run_id: &str,
        signal_id: Option<String>,
    ) -> Result<SignalAck, BackendError> {
        self.engine
            .cancel(run_id, signal_id)
            .await
            .map(Into::into)
            .map_err(pg_err)
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
            .map(Into::into)
            .map_err(pg_err)
    }

    /// 订阅：按 run_id 维护 last_seq 轮询增量（DISTRIBUTED.md §8）。游标只在确认
    /// 转发后推进，重复消息按 seq 去重（游标本体），缺口由 run.events 补齐。
    /// 指定 run_id 且该 run 已终结追平后，流自然结束。
    pub fn subscribe(
        &self,
        run_id: Option<String>,
    ) -> futures::stream::BoxStream<'static, Envelope> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(256);
        let engine = self.engine.clone();
        tokio::spawn(async move {
            let filter = run_id;
            let poll = engine.config().subscribe_poll;
            // 时钟偏差留 5s 余量：捕获订阅开始前后的新 run
            let sub_start = chrono::Utc::now() - chrono::Duration::seconds(5);
            // run_id -> 已确认转发的 last_seq
            let mut cursors: HashMap<String, u64> = HashMap::new();
            // 已终结且追平、不必再查的 run
            let mut drained: HashSet<String> = HashSet::new();
            loop {
                if tx.is_closed() {
                    tracing::debug!("pg subscribe: sink closed, exit");
                    break;
                }
                // 候选：过滤指定的 run，或（活跃 ∪ 订阅后创建）∪ 尚在游标中的 run
                // 短 run 可能在两次轮询之间走完一生：按 started_at 捕获订阅开始后的新 run。
                let mut candidates: Vec<(String, bool)> = Vec::new();
                if let Some(run_id) = &filter {
                    let terminal = match engine.store().get_run(run_id).await {
                        Ok(run) => !matches!(run.status.as_str(), "running" | "awaiting_resume"),
                        Err(_) => true,
                    };
                    candidates.push((run_id.clone(), terminal));
                } else {
                    let watched = engine.watch_runs(sub_start).await.unwrap_or_default();
                    for (run_id, terminal) in watched {
                        candidates.push((run_id, terminal));
                    }
                    for run_id in cursors.keys() {
                        if !candidates.iter().any(|(id, _)| id == run_id) {
                            candidates.push((run_id.clone(), true));
                        }
                    }
                }
                for (run_id, terminal) in candidates {
                    if drained.contains(&run_id) {
                        continue;
                    }
                    let head = cursors.entry(run_id.clone()).or_insert(0);
                    let events = match engine.read_events(&run_id, Some(*head + 1)).await {
                        Ok(events) => events,
                        Err(_) => continue,
                    };
                    for envelope in &events {
                        // 只推进已确认转发的游标；转发失败即订阅者断开
                        if tx.send(envelope.clone()).await.is_err() {
                            return;
                        }
                        *head = envelope.seq;
                    }
                    if terminal && events.is_empty() {
                        // 终结且已追平：丢弃游标
                        cursors.remove(&run_id);
                        drained.insert(run_id.clone());
                        // 已终结 run 无限累积会撑爆订阅生命周期内的内存；
                        // 超阈值整体清空只损失一点轮询冗余（终态 run 重查一次即空）
                        if drained.len() > 4096 {
                            drained.clear();
                            tracing::warn!("订阅 drained 集超过 4096，整体清空");
                        }
                        if filter.is_some() {
                            return; // 指定 run 已终结：订阅自然结束
                        }
                    }
                }
                tokio::time::sleep(poll).await;
            }
        });
        ReceiverStream::new(rx).boxed()
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
        PgError::Conflict(msg) => BackendError::Conflict(msg),
        PgError::Invalid(msg) => BackendError::Invalid(msg),
        other => BackendError::internal(other),
    }
}

impl From<flow_pg::gateway::SignalAck> for SignalAck {
    fn from(ack: flow_pg::gateway::SignalAck) -> Self {
        // Postgres 的 signal_id 是真实落账的 inbox 主键，可查询，始终携带
        SignalAck {
            signal_id: Some(ack.signal_id),
            status: ack.status,
            delivered: ack.delivered,
            event_seq: ack.event_seq,
            error: ack.error,
        }
    }
}
