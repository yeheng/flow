//! 单机后端的 sub_workflow 启动器：子 run 与父 run 同进程、同引擎。
//!
//! - start：解析 latest published → insert_run(initializing) → engine.start_run；
//!   `RunExists`（崩溃重放的重复 start）跳过启动、直接附着等待；
//! - await_terminal：订阅引擎事件等终态，事件延迟时用轮询兜底（Lagged 丢事件不致命）；
//! - cancel：best-effort 调用 engine.cancel，子 run 不在本进程运行时不升级。

use std::sync::Arc;
use std::time::Duration;

use flow_engine::{
    ChildRunLauncher, ChildRunOutcome, DbRunStatus, Definition, Engine, EngineError, RunPhase,
    StartRun,
};
use flow_store::Store;
use futures::future::BoxFuture;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

const AWAIT_POLL: Duration = Duration::from_millis(200);

pub struct LocalChildLauncher {
    store: Arc<Store>,
    engine: Arc<Engine>,
}

impl LocalChildLauncher {
    pub fn new(store: Arc<Store>, engine: Arc<Engine>) -> LocalChildLauncher {
        LocalChildLauncher { store, engine }
    }
}

impl ChildRunLauncher for LocalChildLauncher {
    fn start<'a>(
        &'a self,
        child_run_id: &'a str,
        workflow_id: &'a str,
        input: Value,
        depth: u32,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let version = self
                .store
                .latest_published(workflow_id)
                .await
                .map_err(|e| EngineError::Backend(e.to_string()))?
                .ok_or_else(|| {
                    EngineError::Node(format!("工作流 {workflow_id} 没有已发布版本"))
                })?;
            let stored = self
                .store
                .get_version(workflow_id, Some(version))
                .await
                .map_err(|e| EngineError::Backend(e.to_string()))?;
            let definition: Definition = serde_json::from_value(stored.definition)
                .map_err(|e| EngineError::InvalidDefinition(format!("定义无法解析：{e}")))?;

            // 崩溃重放时 runs 行已存在：跳过插入，start_run 撞 RunExists 后附着等待
            let exists = self.store.get_run(child_run_id).await.is_ok();
            if !exists {
                self.store
                    .insert_run(
                        child_run_id,
                        workflow_id,
                        version,
                        &input,
                        DbRunStatus::Initializing.as_str(),
                    )
                    .await
                    .map_err(|e| EngineError::Backend(e.to_string()))?;
            }
            match self
                .engine
                .start_run(StartRun {
                    run_id: child_run_id.to_string(),
                    workflow_id: workflow_id.to_string(),
                    workflow_version: version,
                    definition,
                    input,
                    depth,
                })
                .await
            {
                Ok(()) => Ok(()),
                Err(EngineError::RunExists(_)) => Ok(()),
                Err(err) => Err(err),
            }
        })
    }

    fn await_terminal<'a>(
        &'a self,
        child_run_id: &'a str,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<ChildRunOutcome, EngineError>> {
        Box::pin(async move {
            let mut events = self.engine.subscribe();
            loop {
                let state = self.engine.snapshot(child_run_id).await?;
                match state.phase {
                    RunPhase::Succeeded => {
                        return Ok(ChildRunOutcome::Succeeded(
                            state.output.unwrap_or(Value::Null),
                        ))
                    }
                    RunPhase::Failed => {
                        return Ok(ChildRunOutcome::Failed(
                            state.fatal_error.unwrap_or_else(|| "未知错误".into()),
                        ))
                    }
                    RunPhase::Cancelled => return Ok(ChildRunOutcome::Cancelled),
                    RunPhase::Running => {}
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(ChildRunOutcome::Cancelled),
                    // 兜底轮询：订阅 Lagged 或事件恰好落在订阅建立之前时不死等
                    _ = tokio::time::sleep(AWAIT_POLL) => {}
                    received = events.recv() => {
                        match received {
                            Ok(env) if env.run_id != child_run_id => continue,
                            Ok(_) => {}
                            // Lagged：丢事件没关系，下一轮 snapshot 是权威
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                tokio::time::sleep(AWAIT_POLL).await
                            }
                        }
                    }
                }
            }
        })
    }

    fn cancel<'a>(&'a self, child_run_id: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if !self.engine.cancel(child_run_id).await {
                tracing::debug!(run_id = %child_run_id, "子 run 未在本进程运行（可能已终结），跳过取消");
            }
        })
    }
}
