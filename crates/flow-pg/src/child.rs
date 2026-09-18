//! Postgres 后端的 sub_workflow 启动器（DISTRIBUTED.md §3）：
//! 父子 run 可能在不同实例上执行，不能依赖内存通道——
//! start 走 gateway 的单事务创建，await 轮询共享 runs 投影，cancel 经持久 inbox。

use std::time::Duration;

use flow_engine::{ChildRunLauncher, ChildRunOutcome, EngineError};
use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::config::PgConfig;
use crate::error::PgError;
use crate::gateway::{self, InboxKind};
use crate::lease;
use crate::metadata::PgStore;

pub struct PgChildLauncher {
    pool: PgPool,
    cfg: PgConfig,
}

impl PgChildLauncher {
    pub fn new(pool: PgPool, cfg: PgConfig) -> PgChildLauncher {
        PgChildLauncher { pool, cfg }
    }
}

impl ChildRunLauncher for PgChildLauncher {
    fn start<'a>(
        &'a self,
        child_run_id: &'a str,
        workflow_id: &'a str,
        input: Value,
        depth: u32,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let store = PgStore::new(self.pool.clone());
            let version = store
                .latest_published(workflow_id)
                .await
                .map_err(|e| EngineError::Backend(e.to_string()))?
                .ok_or_else(|| {
                    EngineError::Node(format!("工作流 {workflow_id} 没有已发布版本"))
                })?;
            let stored = store
                .get_version(workflow_id, Some(version))
                .await
                .map_err(|e| EngineError::Backend(e.to_string()))?;
            let definition: flow_engine::Definition =
                serde_json::from_value(stored.definition.clone())
                    .map_err(|e| EngineError::InvalidDefinition(format!("定义无法解析：{e}")))?;
            definition.validate().map_err(EngineError::InvalidDefinition)?;

            match lease::create_run(&self.pool, child_run_id, workflow_id, version, &input, depth)
                .await
            {
                Ok(()) => Ok(()),
                // 崩溃重放/接管后的重复 start：子 run 已入队，调用方附着等待
                Err(PgError::Sql(sqlx::Error::Database(e))) if e.is_unique_violation() => {
                    Err(EngineError::RunExists(child_run_id.to_string()))
                }
                Err(err) => Err(EngineError::Backend(err.to_string())),
            }
        })
    }

    fn await_terminal<'a>(
        &'a self,
        child_run_id: &'a str,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<ChildRunOutcome, EngineError>> {
        Box::pin(async move {
            let store = PgStore::new(self.pool.clone());
            let poll = self.cfg.subscribe_poll.max(Duration::from_millis(50));
            loop {
                let run = store
                    .get_run(child_run_id)
                    .await
                    .map_err(|e| EngineError::Backend(e.to_string()))?;
                match run.status.as_str() {
                    "succeeded" => {
                        return Ok(ChildRunOutcome::Succeeded(
                            run.output.unwrap_or(Value::Null),
                        ))
                    }
                    "failed" => {
                        return Ok(ChildRunOutcome::Failed(
                            run.error.unwrap_or_else(|| "未知错误".into()),
                        ))
                    }
                    "cancelled" => return Ok(ChildRunOutcome::Cancelled),
                    _ => {}
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(ChildRunOutcome::Cancelled),
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        })
    }

    fn cancel<'a>(&'a self, child_run_id: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let signal_id = format!("child-cancel-{}", uuid::Uuid::now_v7());
            let result = gateway::enqueue(
                &self.pool,
                child_run_id,
                &signal_id,
                InboxKind::Cancel,
                None,
                &Value::Object(serde_json::Map::new()),
            )
            .await;
            match result {
                Ok(_) => {
                    let _ = gateway::wait_applied(
                        &self.pool,
                        child_run_id,
                        &signal_id,
                        self.cfg.signal_wait,
                        self.cfg.signal_poll,
                    )
                    .await;
                }
                // 子 run 已终结或不属于本集群：best-effort，不升级
                Err(err) => {
                    tracing::debug!(run_id = %child_run_id, error = %err, "子 run 取消入队失败（best-effort）")
                }
            }
        })
    }
}
