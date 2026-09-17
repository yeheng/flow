//! flow-pg：Postgres 后端（DISTRIBUTED.md 的共享日志、epoch 租约、持久 inbox）。
//!
//! 依赖方向：flow-pg → flow-engine（事件模型、fold、Driver），
//! 引擎不依赖本 crate（Phase 0 后端边界：`RunEventSink` trait 在 flow-engine）。

pub mod config;
pub mod error;
pub mod executor;
pub mod gateway;
pub mod lease;
pub mod metadata;
pub mod schema;
pub mod sink;

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sqlx::postgres::PgConnectOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

pub use config::{PgConfig, Role};
pub use error::PgError;
pub use metadata::{RunRecord, WorkflowSummary, WorkflowVersion};
pub use sink::PgRunSink;

use executor::ExecutorState;
use flow_engine::{Envelope, RunState};

/// Postgres 后端引擎门面：gateway 入口 + executor + 只读查询。
pub struct PgEngine {
    pool: PgPool,
    store: metadata::PgStore,
    cfg: PgConfig,
    executor: Option<Arc<ExecutorState>>,
}

/// run.start 的输入。
pub struct CreateRun {
    pub workflow_id: String,
    pub version: Option<i64>,
    pub input: Value,
}

#[derive(Debug)]
pub struct CreatedRun {
    pub run_id: String,
    pub workflow_version: i64,
}

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
        let executor = match cfg.role {
            Role::Gateway => None,
            Role::All | Role::Executor => {
                Some(Arc::new(ExecutorState::new(instance_uuid(), cfg.max_runs)))
            }
        };
        Ok(PgEngine {
            pool,
            store,
            cfg,
            executor,
        })
    }

    pub fn store(&self) -> &metadata::PgStore {
        &self.store
    }

    pub fn config(&self) -> &PgConfig {
        &self.cfg
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

    /// 校验 published 版本 → 单事务插入 run + seq=1 RunStarted（§3）。
    /// 提交后 run 已入队（running、lease 为空），由 executor 扫描获得执行容量。
    pub async fn create_run(&self, spec: CreateRun) -> Result<CreatedRun, PgError> {
        let version = match spec.version {
            Some(v) => v,
            None => self
                .store
                .latest_published(&spec.workflow_id)
                .await?
                .ok_or_else(|| {
                    PgError::Invalid(format!(
                        "工作流 {} 没有已发布版本，先 publish 再执行",
                        spec.workflow_id
                    ))
                })?,
        };
        let stored = self
            .store
            .get_version(&spec.workflow_id, Some(version))
            .await?;
        if !stored.is_published() {
            return Err(PgError::Conflict(format!(
                "workflow {} v{version} 尚未发布",
                spec.workflow_id
            )));
        }
        // 定义结构在创建前校验一次（执行侧 executor 也会再校验）
        let definition: flow_engine::Definition = serde_json::from_value(stored.definition.clone())
            .map_err(|e| PgError::Invalid(format!("定义结构非法：{e}")))?;
        definition
            .validate()
            .map_err(|e| PgError::Invalid(format!("工作流定义非法：{e}")))?;

        let run_id = uuid::Uuid::now_v7().to_string();
        lease::create_run(&self.pool, &run_id, &spec.workflow_id, version, &spec.input).await?;
        Ok(CreatedRun {
            run_id,
            workflow_version: version,
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

    /// 停机：停止扫描并等待本地 Driver 退出。
    pub async fn shutdown(&self) {
        if let Some(executor) = self.executor.as_ref() {
            executor.shutdown.cancel();
            executor::shutdown_local(executor).await;
        }
    }

    pub fn shutdown_token(&self) -> Option<CancellationToken> {
        self.executor.as_ref().map(|e| e.shutdown.clone())
    }

    /// 订阅轮询的候选 run（§8）：活跃 run + 订阅开始后创建的 run。
    /// 短 run 可能在两次轮询之间走完一生，只有按 started_at 捕获才不漏。
    /// 返回 (run_id, 是否已终结)。
    pub async fn watch_runs(
        &self,
        started_after: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<(String, bool)>, PgError> {
        let rows = sqlx::query(
            "SELECT id, status FROM runs
             WHERE status IN ('running', 'awaiting_resume')
                OR started_at >= $1
             ORDER BY started_at ASC
             LIMIT 256",
        )
        .bind(started_after)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let status: String = row.try_get("status")?;
            let terminal = !matches!(status.as_str(), "running" | "awaiting_resume");
            out.push((id, terminal));
        }
        Ok(out)
    }
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
