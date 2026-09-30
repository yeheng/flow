//! cron 定时调度循环（schedules 表 → run.start）。
//!
//! 模型：
//! - 每 [`TICK_INTERVAL`] 扫一次全部 schedule，启用中的算「最近一次 <= now 的
//!   整分触发点」（cron 触发点分钟对齐）；
//! - `try_insert_fire` 是唯一的发射闸：同一 (schedule_id, fire_at) 只插得进一次，
//!   重复 tick 与多节点（pg）竞争都靠它去重，插不进就不是你的火；
//! - 每个 tick 每个 schedule 最多补一次火，不追补停机期间错过的全部触发点；
//! - 触发即 `create_run`（等价 run.start：跑该 workflow 当前 published 版本，
//!   输入取 schedule.input）；没有 published 版本时本次跳过，不重试。

use std::time::Duration;

use chrono::{DateTime, Local, Utc};
use cron_parser::Schedule as CronSchedule;
use serde_json::Value;

use flow_backend::{AnyBackend, BackendError, CreateRun, Schedule};

pub const TICK_INTERVAL: Duration = Duration::from_secs(20);

/// main 启动的后台任务（FLOW_SCHEDULER=off 时不调用）。interval 首次立即触发：
/// 开机先补一轮到期的火。
pub async fn run(backend: AnyBackend) -> ! {
    let mut tick = tokio::time::interval(TICK_INTERVAL);
    loop {
        tick.tick().await;
        fire_due(&backend, Local::now()).await;
    }
}

/// 单轮扫描：注入 now 以便单测。单个 schedule 失败不影响其余。
/// pub：归因契约测试（contracts.rs）直接驱动一轮，不等 20s tick。
pub async fn fire_due(backend: &AnyBackend, now: DateTime<Local>) {
    let schedules = match backend.list_schedules(None).await {
        Ok(schedules) => schedules,
        Err(err) => {
            tracing::error!(error = %err, "调度器拉取 schedule 列表失败");
            return;
        }
    };
    for schedule in schedules.iter().filter(|s| s.enabled) {
        if let Err(err) = fire_one(backend, schedule, now).await {
            tracing::error!(schedule_id = %schedule.id, error = %err, "调度器触发失败");
        }
    }
}

/// 到期判定 + 去重 + 触发。返回是否真的点了火。
async fn fire_one(
    backend: &AnyBackend,
    schedule: &Schedule,
    now: DateTime<Local>,
) -> Result<bool, BackendError> {
    // cron 在 RPC 创建边缘已校验；存量数据解析失败只告警跳过，不炸掉整轮
    let cron: CronSchedule = match schedule.cron_expr.parse() {
        Ok(cron) => cron,
        Err(err) => {
            tracing::warn!(schedule_id = %schedule.id, cron = %schedule.cron_expr, error = %err, "schedule 的 cron 无法解析，跳过");
            return Ok(false);
        }
    };
    // 最近一次 <= now 的触发点。cron 触发点分钟对齐（秒=0），
    // 所以「<= now」等价于「< now+1s」（prev_before 是严格小于）。
    let Some(fire_at) = cron.previous_before(&(now + chrono::Duration::seconds(1))) else {
        return Ok(false); // 该表达式在当前时间之前没有任何触发点
    };
    let fire_at = fire_at.with_timezone(&Utc);
    if !backend.try_insert_fire(&schedule.id, fire_at).await? {
        return Ok(false); // 本次触发权已被拿走（重复 tick 或其他节点）
    }
    let created = backend
        .create_run(CreateRun {
            workflow_id: schedule.workflow_id.clone(),
            version: None, // 当前 published 版本
            input: schedule.input.clone().unwrap_or(Value::Null),
            source: flow_backend::DbRunSource::Schedule.as_str().to_string(),
            source_detail: Some(schedule.id.clone()),
        })
        .await;
    match created {
        Ok(created) => {
            tracing::info!(
                schedule_id = %schedule.id,
                workflow_id = %schedule.workflow_id,
                run_id = %created.run_id,
                fire_at = %fire_at,
                "cron 触发 run"
            );
            Ok(true)
        }
        // 配置类失败：本次跳过（火已入账，不重试不追补）——重试也不会变好，
        // 下一分钟的触发点是新行，与本行无关。
        Err(err @ (BackendError::Invalid(_) | BackendError::WorkflowNotFound(_))) => {
            tracing::warn!(schedule_id = %schedule.id, error = %err, "schedule 指向的 workflow 不可执行，本次跳过");
            Ok(false)
        }
        // 瞬时故障（磁盘满 / DB 不可达 / 锁超时）：**撤销去重行**，让下一个
        // tick 重试同一个触发点。否则这一分钟的火永久丢失——去重键含 fire_at，
        // 下一轮算的是新一分钟的键，与本行无关，注释里「不追补」的设计决策
        // 本意是「不追补停机期间错过的点」，不是「静默吞掉一次磁盘错误」。
        Err(err) => {
            if let Err(revoke_err) = backend.delete_fire(&schedule.id, fire_at).await {
                // 撤销也失败：只能等运维介入，记 error 而不是 warn
                tracing::error!(
                    schedule_id = %schedule.id,
                    error = %revoke_err,
                    "触发失败且撤销去重行失败，本次触发点将丢失"
                );
            } else {
                tracing::warn!(
                    schedule_id = %schedule.id,
                    error = %err,
                    "触发 run 遇瞬时故障，已撤销去重，下个 tick 重试"
                );
            }
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests;
