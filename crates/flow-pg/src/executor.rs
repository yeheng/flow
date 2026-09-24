//! 对等模式的 executor（DISTRIBUTED.md §5.4 / §7）：
//! 扫描无租约或租约过期的 running/awaiting_resume run 作为候选，
//! 逐个执行获取事务；提交后读事件、折叠、按 §7 分类恢复并驱动。
//!
//! 容量许可覆盖「获取中 + 正在恢复 + 已驱动」的 run，
//! 释放许可跟随 Driver 退出，防止多个扫描循环重复消费空闲额度。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use flow_engine::{
    spawn_driver, Definition, DriverSpec, EngineError, RecoveryPlan, RunEventSink, RunState,
};

use crate::error::PgError;
use crate::lease::{self, AcquireOutcome};
use crate::sink::PgRunSink;

struct LocalRun {
    /// 所有权丢失/停机 → Driver 静默退出。
    lost: CancellationToken,
    /// Driver 完全退出（inflight 已 abort）后触发。
    done: Arc<tokio::sync::Notify>,
    // 容量许可：registry 条目移除（即 Driver 退出）时释放
    _permit: tokio::sync::OwnedSemaphorePermit,
}

pub enum TakeOver {
    Driving,
    AlreadyTerminal,
    NotEligible(String),
}

/// executor 状态：本地 registry + 容量 + 停机信号。
pub struct ExecutorState {
    pub instance_id: String,
    local: Mutex<HashMap<String, LocalRun>>,
    permits: Arc<Semaphore>,
    pub shutdown: CancellationToken,
}

impl ExecutorState {
    pub fn new(instance_id: String, max_runs: usize) -> ExecutorState {
        ExecutorState {
            instance_id,
            local: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_runs)),
            shutdown: CancellationToken::new(),
        }
    }

    pub fn is_live(&self, run_id: &str) -> bool {
        self.local.lock().contains_key(run_id)
    }

    pub fn driving_count(&self) -> usize {
        self.local.lock().len()
    }
}

/// 扫描循环：直到 shutdown 被触发。返回前等待所有本地 Driver 完全退出。
pub async fn run_scan_loop(
    state: &Arc<ExecutorState>,
    store: &crate::metadata::PgStore,
    cfg: &crate::config::PgConfig,
) -> Result<(), PgError> {
    tracing::info!(
        instance = %state.instance_id,
        max_runs = cfg.max_runs,
        "executor 扫描循环启动"
    );
    loop {
        if state.shutdown.is_cancelled() {
            break;
        }
        let available = state.permits.available_permits();
        if available > 0 {
            let candidates = store
                .takeover_candidates((available as i64 * 2).max(2))
                .await?;
            for run_id in candidates {
                if state.shutdown.is_cancelled() {
                    break;
                }
                if state.is_live(&run_id) {
                    continue;
                }
                // 容量许可先于获取事务；接管失败立即归还
                let Ok(permit) = state.permits.clone().try_acquire_owned() else {
                    break;
                };
                match take_over(state, store, cfg, &run_id, permit).await {
                    Ok(TakeOver::Driving) => {
                        tracing::info!(run_id = %run_id, "已接管 run");
                    }
                    Ok(TakeOver::AlreadyTerminal) => {
                        tracing::info!(run_id = %run_id, "接管时发现日志已终结，已回填投影");
                    }
                    Ok(TakeOver::NotEligible(reason)) => {
                        tracing::debug!(run_id = %run_id, reason = %reason, "候选不可接管");
                    }
                    Err(err) => {
                        tracing::error!(run_id = %run_id, error = %err, "接管 run 失败");
                    }
                }
            }
        }
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(cfg.scan_interval) => {}
        }
    }
    shutdown_local(state).await;
    Ok(())
}

/// 停机：触发所有本地 Driver 的 lost 分支（abort inflight + 安全时释放租约），
/// 等待它们完全退出。
pub async fn shutdown_local(state: &Arc<ExecutorState>) {
    let entries: Vec<(String, CancellationToken, Arc<tokio::sync::Notify>)> = state
        .local
        .lock()
        .iter()
        .map(|(id, r)| (id.clone(), r.lost.clone(), r.done.clone()))
        .collect();
    for (_, lost, _) in &entries {
        lost.cancel();
    }
    let started = std::time::Instant::now();
    let deadline = Duration::from_secs(15);
    for (run_id, _, done) in &entries {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            tracing::warn!(run_id = %run_id, "停机等待超时，放弃等待 Driver 退出");
            break;
        }
        let _ = tokio::time::timeout(remaining, done.notified()).await;
    }
}

/// 接管单个 run：获取租约 → 读事件 → 身份校验 → 恢复分类 → 驱动。
async fn take_over(
    state: &Arc<ExecutorState>,
    store: &crate::metadata::PgStore,
    cfg: &crate::config::PgConfig,
    run_id: &str,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<TakeOver, PgError> {
    let instance = state.instance_id.clone();
    let AcquireOutcome::Acquired { epoch } =
        lease::acquire(store.pool(), run_id, &instance, cfg.lease_ttl).await?
    else {
        return Ok(TakeOver::NotEligible("无法获取租约".into()));
    };

    // 提交后读取事件并折叠。恢复过程中再失去租约，则下一次受保护操作失败，
    // 不得继续派发（§5.2 步骤 4）。
    let events = PgRunSink::read_events(store.pool(), run_id, None).await?;
    let folded = RunState::from_events(&events).map_err(PgError::Engine)?;

    let run = store.get_run(run_id).await?;
    let identity_ok = folded.workflow_id.as_deref() == Some(&run.workflow_id)
        && folded.workflow_version == Some(run.workflow_version)
        && folded.input == run.input;

    if folded.phase.is_terminal() {
        // 事件已终结（投影缺失的防御路径）：以事件为准修正 DB（§7.2）
        let mut sink = PgRunSink::new(
            store.pool().clone(),
            run_id.to_string(),
            instance.clone(),
            epoch,
            folded.last_seq,
        );
        sink.reconcile_terminal(&folded).await?;
        return Ok(TakeOver::AlreadyTerminal);
    }

    if !identity_ok {
        return isolate_corrupted(store, cfg, &instance, epoch, run_id, &run, &folded).await;
    }

    let version = store
        .get_version(&run.workflow_id, Some(run.workflow_version))
        .await?;
    let definition: Definition = serde_json::from_value(version.definition)
        .map_err(|e| PgError::Engine(EngineError::LogCorrupted(format!("定义无法解析：{e}"))))?;

    let mut folded = folded;
    folded.ensure_nodes(&definition);
    let plan = RecoveryPlan::classify(&definition, &folded);
    for node_id in &plan.adjudicate {
        // 副作用准入检查（§7）：接管后绝不自动重放有外部副作用的节点，
        // 一律进入人工裁决。
        tracing::warn!(
            run_id = %run_id,
            node_id = %node_id,
            "接管后副作用节点状态不明，等待人工裁决"
        );
    }

    let sink = PgRunSink::new(
        store.pool().clone(),
        run_id.to_string(),
        instance.clone(),
        epoch,
        folded.last_seq,
    );
    // PG 模式：用户取消与信号都经持久 inbox 交付，本地通道不参与；
    // 事件订阅走共享日志轮询（DISTRIBUTED.md §8），无本地广播。
    let cancel = CancellationToken::new();
    let lost = CancellationToken::new();

    let spec = DriverSpec {
        run_id: run_id.to_string(),
        definition: Arc::new(definition),
        input: folded.input.clone(),
        // 深度以共享日志中的 run_started 为准（接管恢复时 spec 无权威来源）
        depth: folded.depth,
        child_launcher: Some(Arc::new(crate::child::PgChildLauncher::new(
            store.pool().clone(),
            cfg.clone(),
        ))),
        sink: Box::new(sink),
        events_tx: None,
        cancel,
        ownership_lost: lost.clone(),
        signal_rx: None,
        inbox_poll: cfg.inbox_poll,
    };
    let handle = spawn_driver(spec, folded, plan);

    // 续期监督：TTL/3 周期续期；LeaseLost 时触发静默退出。
    // 暂时性错误（网络抖动）不立即放弃，下个周期重试。
    let supervisor_pool = store.pool().clone();
    let supervisor_run = run_id.to_string();
    let supervisor_instance = instance.clone();
    let supervisor_lost = lost.clone();
    let ttl = cfg.lease_ttl;
    let supervisor = tokio::spawn(async move {
        let mut tick = tokio::time::interval((ttl / 3).max(Duration::from_millis(50)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // 第一次立即触发，跳过
        loop {
            tokio::select! {
                _ = supervisor_lost.cancelled() => break,
                _ = tick.tick() => {
                    match lease::renew(&supervisor_pool, &supervisor_run, &supervisor_instance, epoch, ttl).await {
                        Ok(()) => {}
                        Err(err) if err.is_lease_lost() => {
                            tracing::warn!(run_id = %supervisor_run, "续期失败：租约已被接管，停止本地派发");
                            supervisor_lost.cancel();
                            break;
                        }
                        Err(err) => {
                            tracing::error!(run_id = %supervisor_run, error = %err, "续期暂时失败，下个周期重试");
                        }
                    }
                }
            }
        }
    });

    let done = Arc::new(tokio::sync::Notify::new());
    // Driver 完全退出后清理本地 registry（容量许可随之释放）
    let local_state = Arc::clone(state);
    let cleanup_run = run_id.to_string();
    let done_clone = done.clone();
    let supervisor_abort = supervisor.abort_handle();
    tokio::spawn(async move {
        let _ = handle.await;
        supervisor_abort.abort();
        local_state.local.lock().remove(&cleanup_run);
        done_clone.notify_waiters();
    });
    state.local.lock().insert(
        run_id.to_string(),
        LocalRun {
            lost,
            done,
            _permit: permit,
        },
    );
    Ok(TakeOver::Driving)
}

/// 身份不符：保留 awaiting_resume、报告恢复错误并释放租约（DESIGN.md §7.1）。
async fn isolate_corrupted(
    store: &crate::metadata::PgStore,
    _cfg: &crate::config::PgConfig,
    instance: &str,
    epoch: i64,
    run_id: &str,
    run: &crate::metadata::RunRecord,
    folded: &RunState,
) -> Result<TakeOver, PgError> {
    let message = format!(
        "事件日志与 run 元数据不一致（日志为 {:?}/v{:?}，元数据为 {}/v{}；input 匹配失败则同样隔离）",
        folded.workflow_id, folded.workflow_version, run.workflow_id, run.workflow_version
    );
    tracing::error!(run_id = %run_id, error = %message, "接管时发现身份不符，隔离 run");
    let mut sink = PgRunSink::new(
        store.pool().clone(),
        run_id.to_string(),
        instance.to_string(),
        epoch,
        folded.last_seq,
    );
    if let Err(err) = sink
        .project_status(flow_engine::DbRunStatus::AwaitingResume, Some(&message))
        .await
    {
        tracing::error!(run_id = %run_id, error = %err, "隔离投影失败");
    }
    let _ = lease::release(store.pool(), run_id, instance, epoch).await;
    Ok(TakeOver::NotEligible(message))
}
