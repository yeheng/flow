//! flow-pg：Postgres 后端——共享日志、epoch 租约、持久 inbox 的实现。
//!
//! **本 crate 的模块头注释即是分布式设计的权威记录。** 曾经有一份
//! `docs/DISTRIBUTED.md` 专门写这套设计，现已删除：那份文档与代码各改各的、
//! 互相引用逐渐失真（删除时已有 40+ 处引用指向不存在的章节）。设计逐条落进
//! 下面这些模块的头注释，改代码时顺带就在改设计：
//! - `lease.rs` 所有权协议：行锁、准入检查、租约获取/续期/释放、fencing；
//! - `gateway.rs` 持久 inbox：入队、确认、查询；
//! - `executor.rs` 对等模式扫描与接管；
//! - `subscribe.rs` 共享日志轮询与 LISTEN/NOTIFY 唤醒；
//! - `config.rs` 部署配置与环境变量。
//!
//! 依赖方向：flow-pg → flow-engine（事件模型、fold、Driver），
//! 引擎不依赖本 crate（Phase 0 后端边界：`RunEventSink` trait 在 flow-engine）。

pub mod child;
pub mod config;
pub mod error;
pub mod executor;
pub mod gateway;
pub mod lease;
pub mod metadata;
pub mod schema;
pub mod sink;
pub mod subscribe;

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sqlx::postgres::PgConnectOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub use child::PgChildLauncher;
pub use config::{PgConfig, Role};
pub use error::PgError;
use executor::ExecutorState;
use flow_engine::{Envelope, RunState};
pub use metadata::{RunRecord, WorkflowSummary, WorkflowVersion};
pub use sink::PgRunSink;

/// 事件通知频道（§8）：run_events 每次提交后 NOTIFY 一次，载荷为 run_id。
pub const EVENTS_CHANNEL: &str = "flow_events";

/// 事件通知流（§8 的低延迟提示）：任意 run 提交事件后产出其 run_id。
/// 返回前已完成 LISTEN，调用方随后产生的事件必然可达；
/// 连接中断自动退避重连，断开期间的通知由消费方的兜底轮询兜住。
///
/// **注意（sqlx 0.9 实测）**：`PgListener` 终身占用池里 acquired 的一个连接，
/// drop 返回的 Receiver **不会**归还它——后台任务只在收到下一条通知时退出。
/// 每个订阅一条监听会耗尽连接池。进程内应复用唯一的共享轮询器
///（[`subscribe::EventHub`]/`subscribe_events`），不要按订阅开监听。
pub async fn event_notifications(
    pool: &PgPool,
) -> Result<tokio::sync::mpsc::Receiver<String>, PgError> {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    // 契约：返回前必须完成首次 LISTEN，调用方随后产生的事件必然可达。
    listener.listen(EVENTS_CHANNEL).await?;
    let pool = pool.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(256);
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            // 通知投递直到连接出错或**接收端被丢弃**。
            // 后者不能靠 `tx.send()` 的 is_err 兜住：连接健康时任务阻塞在
            // `recv()` 上，一条通知都不来就永远等不到 send——而 `PgListener`
            // 终身持有池里一个连接，于是每次调用本函数漏一个 LISTEN 后端。
            // `tx.closed()` 在接收端 drop 时立即就绪，是唯一可靠的唤醒源。
            loop {
                tokio::select! {
                    biased;
                    _ = tx.closed() => return,
                    received = listener.recv() => {
                        match received {
                            Ok(note) => {
                                if tx.send(note.payload().to_string()).await.is_err() {
                                    return;
                                }
                            }
                            Err(_) => break, // 连接出错，去重连
                        }
                    }
                }
            }
            // 接收端已走人（EventHub::stop → poll_loop 返回 → rx drop）。
            // 下面这条检查必须在**重连循环里**：连接断着的时候 `tx.send()` 根本
            // 不会被调用，只靠它退出的话，本循环会每 ≤5s 重连一次、每次
            // connect_with 再占住一个池连接，直到进程结束。
            if !should_keep_reconnecting(&tx) {
                return;
            }
            tracing::warn!("pg 事件通知连接中断，{backoff:?} 后重连");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
            // 重连成功必须重新 LISTEN，否则静默丢通知
            loop {
                if !should_keep_reconnecting(&tx) {
                    return;
                }
                match sqlx::postgres::PgListener::connect_with(&pool).await {
                    Ok(mut new) => match new.listen(EVENTS_CHANNEL).await {
                        Ok(()) => {
                            listener = new;
                            backoff = Duration::from_millis(200);
                            break;
                        }
                        Err(err) => tracing::warn!("pg LISTEN {EVENTS_CHANNEL} 失败：{err}"),
                    },
                    Err(err) => tracing::warn!("pg 事件通知重连失败：{err}"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    });
    Ok(rx)
}

/// Postgres 后端引擎门面：gateway 入口 + executor + 只读查询。
pub struct PgEngine {
    pool: PgPool,
    store: metadata::PgStore,
    cfg: PgConfig,
    executor: Option<Arc<ExecutorState>>,
    hub: Arc<subscribe::EventHub>,
}

/// run.start 的输入。`version` 必须是已解析的 published 版本——
/// 「只有 published 可执行 + 创建前校验定义」这条规则单点在 flow-backend 的
/// `resolve_runnable_definition`，本方法不重复实现（`flow-pg/src/lease.rs::create_run`）。
pub struct CreateRun {
    pub workflow_id: String,
    pub version: i64,
    pub input: Value,
    /// 触发来源归因（flow-dto DbRunSource 词汇表）
    pub source: String,
    pub source_detail: Option<String>,
}

/// run 创建结果。类型单一来源在 flow-dto。
pub use flow_dto::CreatedRun;

impl PgEngine {
    /// 连接并初始化 schema。会话级超时按 §5.1 配置。
    pub async fn connect(database_url: &str, cfg: PgConfig) -> Result<PgEngine, PgError> {
        let opts: PgConnectOptions = database_url
            .parse::<PgConnectOptions>()
            .map_err(|e| PgError::Invalid(format!("FLOW_DATABASE_URL 无法解析：{e}")))?
            .application_name("flow")
            .options([
                (
                    "statement_timeout",
                    cfg.statement_timeout.as_millis().to_string(),
                ),
                ("lock_timeout", cfg.lock_timeout.as_millis().to_string()),
                (
                    "idle_in_transaction_session_timeout",
                    cfg.idle_tx_timeout.as_millis().to_string(),
                ),
            ]);
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await?;
        schema::init(&pool).await?;
        let store = metadata::PgStore::new(pool.clone());
        let hub = subscribe::EventHub::start(pool.clone(), cfg.clone());
        let executor = match cfg.role {
            Role::Gateway => None,
            Role::All | Role::Executor => Some(Arc::new(ExecutorState::new(
                instance_uuid(),
                cfg.max_runs,
                hub.clone(),
            ))),
        };
        Ok(PgEngine {
            pool,
            store,
            cfg,
            executor,
            hub,
        })
    }

    pub fn store(&self) -> &metadata::PgStore {
        &self.store
    }

    pub fn config(&self) -> &PgConfig {
        &self.cfg
    }

    /// 事件通知流（§8 低延迟提示），见 [`event_notifications`]。
    pub async fn event_notifications(
        &self,
    ) -> Result<tokio::sync::mpsc::Receiver<String>, PgError> {
        event_notifications(&self.pool).await
    }

    /// 共享事件订阅流（§8）：进程内单份轮询的增量扇出，见 [`subscribe::EventHub`]。
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Envelope> {
        self.hub.subscribe()
    }

    pub fn instance_id(&self) -> Option<&str> {
        self.executor.as_ref().map(|e| e.instance_id.as_str())
    }

    /// 本进程正在驱动的 run（live 标记）。
    pub fn is_live(&self, run_id: &str) -> bool {
        self.executor
            .as_ref()
            .map(|e| e.is_live(run_id))
            .unwrap_or(false)
    }

    // ---- gateway：run.start ----

    /// 单事务创建 run：校验后的版本号 → insert run + seq=1 RunStarted（§3）。
    /// 提交后 run 已入队（running、lease 为空），由 executor 扫描获得执行容量。
    /// 版本解析与定义校验在适配层（flow-backend）单点完成。
    pub async fn create_run(&self, spec: CreateRun) -> Result<CreatedRun, PgError> {
        let run_id = uuid::Uuid::now_v7().to_string();
        lease::create_run(
            &self.pool,
            &run_id,
            &spec.workflow_id,
            spec.version,
            &spec.input,
            0,
            &spec.source,
            spec.source_detail.as_deref(),
        )
        .await?;
        Ok(CreatedRun {
            run_id,
            workflow_version: spec.version,
        })
    }

    // ---- gateway：信号与取消（§6）----

    /// 提交信号到持久 inbox 并等待落账。signal_id 必须由客户端提供且稳定复用。
    /// 返回 pending 时客户端用 signal_status 查询。
    pub async fn signal(
        &self,
        run_id: &str,
        signal_id: &str,
        node_id: &str,
        payload: &Value,
    ) -> Result<gateway::SignalAck, PgError> {
        validate_signal_id(signal_id)?;
        match gateway::enqueue(
            &self.pool,
            run_id,
            signal_id,
            gateway::InboxKind::Signal,
            Some(node_id),
            payload,
        )
        .await?
        {
            gateway::EnqueueOutcome::Existing(ack) => Ok(ack),
            gateway::EnqueueOutcome::Accepted => {
                gateway::wait_applied(
                    &self.pool,
                    run_id,
                    signal_id,
                    self.cfg.signal_wait,
                    self.cfg.signal_poll,
                )
                .await
            }
        }
    }

    /// 提交取消命令。signal_id 可省略（服务端生成）；省略时重试非幂等。
    pub async fn cancel(
        &self,
        run_id: &str,
        signal_id: Option<String>,
    ) -> Result<gateway::SignalAck, PgError> {
        let signal_id = match signal_id {
            Some(id) => {
                validate_signal_id(&id)?;
                id
            }
            None => uuid::Uuid::now_v7().to_string(),
        };
        match gateway::enqueue(
            &self.pool,
            run_id,
            &signal_id,
            gateway::InboxKind::Cancel,
            None,
            &Value::Object(serde_json::Map::new()),
        )
        .await?
        {
            gateway::EnqueueOutcome::Existing(ack) => Ok(ack),
            gateway::EnqueueOutcome::Accepted => {
                gateway::wait_applied(
                    &self.pool,
                    run_id,
                    &signal_id,
                    self.cfg.signal_wait,
                    self.cfg.signal_poll,
                )
                .await
            }
        }
    }

    /// 查询信号落账状态（run.signal_status）。
    pub async fn signal_status(
        &self,
        run_id: &str,
        signal_id: &str,
    ) -> Result<gateway::SignalAck, PgError> {
        gateway::status_of(&self.pool, run_id, signal_id).await
    }

    // ---- 只读查询 ----

    pub async fn read_events(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
    ) -> Result<Vec<Envelope>, PgError> {
        PgRunSink::read_events(&self.pool, run_id, from_seq).await
    }

    pub async fn snapshot(&self, run_id: &str) -> Result<RunState, PgError> {
        let events = self.read_events(run_id, None).await?;
        RunState::from_events(&events).map_err(PgError::Engine)
    }

    // ---- executor ----

    /// 启动扫描循环（阻塞直到 shutdown）。
    pub async fn run_executor(&self) -> Result<(), PgError> {
        let Some(executor) = self.executor.as_ref() else {
            return Err(PgError::Invalid("本进程角色不含 executor".into()));
        };
        executor::run_scan_loop(executor, &self.store, &self.cfg).await
    }

    /// 停机：停止扫描与共享订阅轮询，等待本地 Driver 退出。
    pub async fn shutdown(&self) {
        if let Some(executor) = self.executor.as_ref() {
            executor.shutdown.cancel();
            executor::shutdown_local(executor).await;
        }
        // Driver 退出后再停 hub：停机期间写入的终态事件仍会被扇出给订阅者
        self.hub.stop().await;
    }
}

/// 通知任务在「连接断了」之后、重连之前要不要继续：接收端已被丢弃就退出。
///
/// 这条检查必须存在，且必须在**重连循环内部**：接收端 `rx` 随 `EventHub::stop`
/// → `poll_loop` 返回而 drop，但连接断着的时候 `tx.send()` 根本不会被调用，
/// 只靠 send 的 `is_err()` 退出的话，重连循环会每 ≤5s 跑一次、每次
/// `connect_with` 再占住一个池连接，直到进程结束。PG 不可达期间每次重连都会
/// 失败并重试——那正是连接池被慢慢吃光的场景。
///
/// 抽成函数是为了能单测：这个条件只有「接收端已关闭」一种为假，而它在真实
/// 进程里难以构造（要求 drop 的瞬间 PG 恰好不可达）。
fn should_keep_reconnecting(tx: &tokio::sync::mpsc::Sender<String>) -> bool {
    !tx.is_closed()
}

fn validate_signal_id(signal_id: &str) -> Result<(), PgError> {
    if signal_id.trim().is_empty() || signal_id.len() > 128 {
        return Err(PgError::Invalid(
            "signal_id 必须为 1-128 字符的非空字符串，且重试时复用同一个 id".into(),
        ));
    }
    Ok(())
}

/// 每次进程启动新生成的 instance UUID；运维节点名可以重复，实例标识不可复用。
fn instance_uuid() -> String {
    format!("inst-{}", uuid::Uuid::now_v7())
}

#[cfg(test)]
mod notification_reconnect_tests {
    use super::should_keep_reconnecting;

    /// 接收端还在：重连循环必须继续（否则通知会静默停摆，
    /// 兜底轮询成了唯一路径）。
    #[tokio::test]
    async fn keeps_reconnecting_while_receiver_alive() {
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(4);
        assert!(should_keep_reconnecting(&tx));
        drop(rx);
    }

    /// 接收端已丢弃：重连循环必须退出。
    ///
    /// 这是连接池被吃光的根因：PG 不可达时每次重连都失败并重试，若不检查
    /// `is_closed()`，这个循环会跑到进程结束，每次尝试都占一个池连接。
    #[tokio::test]
    async fn stops_reconnecting_once_receiver_dropped() {
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(4);
        drop(rx);
        assert!(
            !should_keep_reconnecting(&tx),
            "接收端已 drop 时必须停止重连，否则泄漏池连接"
        );
    }
}
