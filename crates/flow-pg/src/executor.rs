//! 对等模式的 executor（`flow-pg/src/executor.rs` + `driver::RecoveryPlan::classify`）：
//! 扫描无租约或租约过期的 running/awaiting_resume run 作为候选，
//! 逐个执行获取事务；提交后读事件、折叠、按 §7 分类恢复并驱动。
//!
//! 容量许可覆盖「获取中 + 正在恢复 + 已驱动」的 run，
//! 释放许可跟随 Driver 退出，防止多个扫描循环重复消费空闲额度。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use flow_engine::{
    spawn_driver, DbRunStatus, Definition, DriverSpec, EngineError, RecoveryPlan, RunEventSink,
    RunState,
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

/// executor 状态：本地 registry + 容量 + 停机信号 + 共享事件 hub。
pub struct ExecutorState {
    pub instance_id: String,
    local: Mutex<HashMap<String, LocalRun>>,
    permits: Arc<Semaphore>,
    pub shutdown: CancellationToken,
    /// 进程内唯一的订阅轮询器；子 run 等待复用它的扇出，不再每等待一条 LISTEN 连接。
    hub: Arc<crate::subscribe::EventHub>,
    /// 已判定不可恢复（身份不符/日志损坏/版本缺失）的 run：避免每个扫描周期
    /// 重放同一段死路（抢租约→读全量日志→fold→同一结论）。进程内记忆，
    /// 重启后复查一次再拉黑——事件日志不可变，结论不会漂移。
    quarantined: Mutex<HashSet<String>>,
}

impl ExecutorState {
    pub fn new(
        instance_id: String,
        max_runs: usize,
        hub: Arc<crate::subscribe::EventHub>,
    ) -> ExecutorState {
        ExecutorState {
            instance_id,
            local: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_runs)),
            shutdown: CancellationToken::new(),
            hub,
            quarantined: Mutex::new(HashSet::new()),
        }
    }

    pub fn quarantine(&self, run_id: &str) {
        self.quarantined.lock().insert(run_id.to_string());
    }

    pub fn is_quarantined(&self, run_id: &str) -> bool {
        self.quarantined.lock().contains(run_id)
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
            let candidates = match store
                .takeover_candidates((available as i64 * 2).max(2))
                .await
            {
                Ok(candidates) => candidates,
                // 候选查询失败（failover/语句超时/池抖动）只跳过本轮。
                // 扫描循环是 executor 的心脏，不能被一次瞬时错误打死；
                // 循环底部的 sleep 决定重试节奏
                Err(err) => {
                    tracing::warn!(error = %err, "候选查询失败，本轮跳过");
                    Vec::new()
                }
            };
            for run_id in candidates {
                if state.shutdown.is_cancelled() {
                    break;
                }
                if state.is_live(&run_id) {
                    continue;
                }
                if state.is_quarantined(&run_id) {
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
    // 先注册全部 waiter 再触发取消：notify_waiters 只唤醒「已注册」的等待者，
    // 逐个顺序 await 会丢掉「等前一个 run 时后面的 run 已退出」的唤醒，
    // 白白烧掉整个停机预算
    let waits: Vec<_> = entries.iter().map(|(_, _, done)| done.notified()).collect();
    for (_, lost, _) in &entries {
        lost.cancel();
    }
    if tokio::time::timeout(Duration::from_secs(15), futures::future::join_all(waits))
        .await
        .is_err()
    {
        let pending: Vec<&str> = entries.iter().map(|(id, _, _)| id.as_str()).collect();
        tracing::warn!(
            runs = pending.join(","),
            "停机等待超时，放弃等待 Driver 退出"
        );
    }
}

/// 接管单个 run：获取租约 → 读事件 → 身份校验 → 恢复分类 → 驱动。
///
/// 拿到租约后的一切失败都必须在返回前释放租约（否则要熬满 TTL 才能被重新
/// 接管）；永久性失败（日志损坏/版本缺失）另加隔离投影并拉黑本进程，
/// 避免每个扫描周期重放同一段死路。
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

    match drive_after_acquire(state, store, cfg, run_id, &instance, epoch, permit).await {
        Ok(takeover) => Ok(takeover),
        Err(err) => {
            let message = err.to_string();
            if is_unrecoverable(&err) {
                tracing::error!(run_id = %run_id, error = %message, "run 不可恢复，隔离并拉黑");
                let mut sink = PgRunSink::new(
                    store.pool().clone(),
                    run_id.to_string(),
                    instance.clone(),
                    epoch,
                    0,
                );
                if let Err(project_err) = sink
                    .project_status(DbRunStatus::AwaitingResume, Some(&message))
                    .await
                {
                    tracing::error!(run_id = %run_id, error = %project_err, "隔离投影失败");
                }
                let _ = lease::release(store.pool(), run_id, &instance, epoch).await;
                state.quarantine(run_id);
                Ok(TakeOver::NotEligible(message))
            } else {
                // 瞬时错误：立即释放租约，让下一轮（或别的实例）尽快重试
                tracing::debug!(run_id = %run_id, error = %message, "接管失败，已释放租约");
                let _ = lease::release(store.pool(), run_id, &instance, epoch).await;
                Err(err)
            }
        }
    }
}

/// 永久不可恢复：重试一万次也是同一结论（身份不符在内部单独处理，
/// 因为诊断信息需要原始行）。
fn is_unrecoverable(err: &PgError) -> bool {
    matches!(
        err,
        PgError::Engine(EngineError::LogCorrupted(_))
            | PgError::Engine(EngineError::InvalidDefinition(_))
            | PgError::WorkflowNotFound(_)
            | PgError::VersionNotFound(..)
    )
}

/// 租约已持有后的接管主体：读事件 → 身份校验 → 恢复分类 → 驱动。
async fn drive_after_acquire(
    state: &Arc<ExecutorState>,
    store: &crate::metadata::PgStore,
    cfg: &crate::config::PgConfig,
    run_id: &str,
    instance: &str,
    epoch: i64,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<TakeOver, PgError> {
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
            instance.to_string(),
            epoch,
            folded.last_seq,
        );
        sink.reconcile_terminal(&folded).await?;
        return Ok(TakeOver::AlreadyTerminal);
    }

    if !identity_ok {
        return isolate_corrupted(state, store, instance, epoch, run_id, &run, &folded).await;
    }

    let version = store
        .get_version(&run.workflow_id, Some(run.workflow_version))
        .await?;
    let definition: Definition = serde_json::from_value(version.definition)
        .map_err(|e| PgError::Engine(EngineError::LogCorrupted(format!("定义无法解析：{e}"))))?;

    // 定义合法性：与单机臂 `Engine::resume_run` 同一处校验（发布期已校验过一次，
    // 这里是第二道）。**不能不校验**：`RecoveryPlan::classify` 对
    // `definition.node_type()` 返回 None 的节点是静默 `continue`——不登记任何槽位，
    // 于是该节点既不被派发也不被跳过，`plan()` 看不见它（只遍历 definition.nodes）、
    // `all_terminal()` 看得见它（遍历 records），slots 空而非终态 ⇒ `EngineError::Bug`
    // ⇒ 每次接管都把 run 挂进 awaiting_resume，永远推不动。
    // InvalidDefinition 归不可恢复（与 LogCorrupted 同一条隔离出口）。
    definition
        .validate()
        .map_err(|e| PgError::Engine(EngineError::InvalidDefinition(e)))?;

    // 定义外节点 = 日志损坏：在补 Pending 与分类之前就拒（与单机臂
    // `Engine::resume_run` 同一处校验，两臂同契约）。幽灵记录停在非终态时
    // 终止判定永远不成立，run 会永久卡在 awaiting_resume（fold.rs
    // `validate_nodes_in_definition` 的完整论证）。
    folded.validate_nodes_in_definition(&definition)?;

    let mut folded = folded;
    folded.ensure_nodes(&definition);
    let plan = RecoveryPlan::classify(&definition, &folded);
    for node_id in plan.adjudicating_nodes() {
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
        instance.to_string(),
        epoch,
        folded.last_seq,
    );
    // PG 模式：用户取消与信号都经持久 inbox 交付，本地通道不参与；
    // 事件订阅走共享日志轮询（`flow-pg/src/subscribe.rs`），无本地广播。
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
            state.hub.clone(),
        ))),
        sink: Box::new(sink),
        events_tx: None,
        cancel,
        ownership_lost: lost.clone(),
        signal_rx: None,
        inbox_poll: cfg.inbox_poll,
    };
    // 注册必须先于 spawn：驱动可能瞬间跑完（如恢复计划直接 finalize），
    // 清理任务的 remove 打在空表上会让条目永远没人清——许可泄漏、
    // is_live 永真、done 永不触发（停机白等满预算）
    let done = Arc::new(tokio::sync::Notify::new());
    state.local.lock().insert(
        run_id.to_string(),
        LocalRun {
            lost: lost.clone(),
            done: done.clone(),
            _permit: permit,
        },
    );
    let handle = spawn_driver(spec, folded, plan);

    // 续期监督：TTL/3 周期续期；LeaseLost 时触发静默退出。
    // 暂时性错误（网络抖动）不立即放弃，下个周期重试。
    let supervisor_pool = store.pool().clone();
    let supervisor_run = run_id.to_string();
    let supervisor_instance = instance.to_string();
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

    // Driver 完全退出后清理本地 registry（容量许可随之释放）
    let local_state = Arc::clone(state);
    let cleanup_run = run_id.to_string();
    let done_clone = done;
    let supervisor_abort = supervisor.abort_handle();
    tokio::spawn(async move {
        let _ = handle.await;
        supervisor_abort.abort();
        local_state.local.lock().remove(&cleanup_run);
        done_clone.notify_waiters();
    });
    Ok(TakeOver::Driving)
}

/// 身份不符：保留 awaiting_resume、报告恢复错误、释放租约并拉黑本进程
/// （DESIGN.md §7.1）。事件日志与元数据都不会自动愈合，重新扫描只会
/// 无限重放同一段死路。
async fn isolate_corrupted(
    state: &Arc<ExecutorState>,
    store: &crate::metadata::PgStore,
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
    // 状态投影与租约释放**同一事务**：分两次提交会留下一个可观测的中间态
    // ——status 已是 awaiting_resume 而 lease_owner 还在。观察者（run.timeline
    // 的 live 标记、运维巡检、以及 recovery 回归里「隔离必须释放租约」这条断言）
    // 在这个窗口里读到的就是「已隔离但仍被租约持有」的矛盾状态。
    let mut tx = store.pool().begin().await?;
    // 锁行并校验 owner/epoch：与 project_status 走同一套准入检查
    // （lease.rs 的 check_writable 语义），不依赖「刚 acquire 过所以一定还对」。
    let row: Option<(String, i64)> =
        sqlx::query_as("SELECT lease_owner, lease_epoch FROM runs WHERE id = $1 FOR UPDATE")
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PgError::from)?;
    match row {
        Some((owner, row_epoch)) if owner == instance && row_epoch == epoch => {}
        Some((owner, row_epoch)) => {
            // 租约已易主：不得改他的行，交给新持有者处理
            tracing::warn!(
                run_id = %run_id,
                current_owner = %owner,
                current_epoch = row_epoch,
                expected_epoch = epoch,
                "隔离时租约已易主，放弃投影"
            );
            return Ok(TakeOver::NotEligible(message));
        }
        None => {
            tracing::warn!(run_id = %run_id, "隔离时 run 行已消失，放弃投影");
            return Ok(TakeOver::NotEligible(message));
        }
    }
    sqlx::query(
        "UPDATE runs SET status = $1, output = NULL, error = $2,
                lease_owner = NULL, lease_expires_at = NULL
         WHERE id = $3",
    )
    .bind(flow_engine::DbRunStatus::AwaitingResume.as_str())
    .bind(&message)
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .map_err(PgError::from)?;
    tx.commit().await.map_err(PgError::from)?;
    state.quarantine(run_id);
    Ok(TakeOver::NotEligible(message))
}
