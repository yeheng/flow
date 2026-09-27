//! SQLite 后端的 sub_workflow 启动器：子 run 与父 run 同进程、同引擎。
//! （原在 flow-rpc，适配层重构后归入 flow-backend 的 sqlite 实现。）
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
use flow_store::{Store, StoreError};
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
            // 崩溃重放时 runs 行已存在：沿用行内已钉的版本与输入（定义是不可变
            // 快照，run 钉死某一版）。绝不在重放路径重新解析 latest published——
            // 重启前发布的新版会让事件日志与元数据分叉，之后 resume_run 的身份
            // 校验判 LogCorrupted，run 永久不可恢复。
            // 只有「行不存在」才走 fresh start；瞬时读错误必须原样暴露。
            // 否则会误入 latest_published 分支——版本钉死不变量全靠 runs
            // 主键兜底，错误也被吞成不透明的 Backend 失败
            let existing = match self.store.get_run(child_run_id).await {
                Ok(run) => Some(run),
                Err(StoreError::RunNotFound(_)) => None,
                Err(err) => return Err(EngineError::Backend(err.to_string())),
            };
            let version = match &existing {
                Some(run) => run.workflow_version,
                None => self
                    .store
                    .latest_published(workflow_id)
                    .await
                    .map_err(|e| EngineError::Backend(e.to_string()))?
                    .ok_or_else(|| {
                        EngineError::Node(format!("工作流 {workflow_id} 没有已发布版本"))
                    })?,
            };
            let input = match &existing {
                Some(run) => run.input.clone(),
                None => input,
            };
            let stored = self
                .store
                .get_version(workflow_id, Some(version))
                .await
                .map_err(|e| EngineError::Backend(e.to_string()))?;
            let definition: Definition = serde_json::from_value(stored.definition)
                .map_err(|e| EngineError::InvalidDefinition(format!("定义无法解析：{e}")))?;

            if existing.is_none() {
                self.store
                    .insert_run(
                        child_run_id,
                        workflow_id,
                        version,
                        &input,
                        DbRunStatus::Initializing.as_str(),
                        flow_dto::DbRunSource::SubWorkflow.as_str(),
                        None,
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
            let mut events = Some(self.engine.subscribe());
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
                    RunPhase::Running => {
                        // 空日志 + DB 终态：子 run 的初始化被中断（崩溃发生在
                        // run_started 落盘之前，恢复流程已按 DESIGN §7.2 标 failed）。
                        // 事件日志永远不会出现终态事件，只等日志会挂死，
                        // 以 DB 投影为权威结束等待。正常启动窗口内 DB 仍是
                        // initializing/running，不受影响。
                        if state.last_seq == 0 {
                            if let Ok(run) = self.store.get_run(child_run_id).await {
                                match run.status.as_str() {
                                    s if s == DbRunStatus::Failed.as_str() => {
                                        return Ok(ChildRunOutcome::Failed(
                                            run.error.unwrap_or_else(|| "子 run 初始化中断".into()),
                                        ));
                                    }
                                    s if s == DbRunStatus::Cancelled.as_str() => {
                                        return Ok(ChildRunOutcome::Cancelled)
                                    }
                                    // 空日志不可能对应 RunCompleted 事件：投影矛盾
                                    // 按失败处理，不把拿不到的输出编造成成功
                                    s if s == DbRunStatus::Succeeded.as_str() => {
                                        return Ok(ChildRunOutcome::Failed(format!(
                                            "子 run {child_run_id} 投影为 succeeded 但事件日志为空"
                                        )));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                // 等待窗口内只被「本 run 的事件」或兜底超时唤醒：无关 run 的每条
                // 事件都触发一次全量 snapshot（整份 event.jsonl 读 + fold），
                // 等待成本会随进程事件流量无界放大。结构与 PG 臂一致
                //（flow-pg/src/child.rs）
                let wait = tokio::time::sleep(AWAIT_POLL);
                tokio::pin!(wait);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(ChildRunOutcome::Cancelled),
                        _ = &mut wait => break,
                        received = async {
                            match events.as_mut() {
                                Some(rx) => rx.recv().await,
                                None => std::future::pending().await,
                            }
                        }, if events.is_some() => {
                            match received {
                                // 无关 run 的事件：继续等，不重查、不重置兜底计时
                                Ok(env) if env.run_id != child_run_id => {}
                                // 本 run 的事件：立刻重查状态
                                Ok(_) => break,
                                // Lagged：丢的是唤醒不是数据，下一轮 snapshot 是权威
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                                // 引擎通道关闭：退化为纯兜底轮询
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    events = None
                                }
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
