//! gateway 入口（DISTRIBUTED.md §6）：持久 inbox 的入队、确认与查询。
//!
//! - 入队只代表 accepted，不能返回 delivered=true；
//! - 相同 signal_id + 相同内容返回原结果（幂等），不同内容返回 conflict；
//! - 若原请求已处理，即使 run 已终结也可查询原结果；
//! - gateway 不得通过直接清租约或只改 status 实现取消。

use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use crate::error::PgError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxKind {
    Signal,
    Cancel,
}

impl InboxKind {}

/// 入队后的观察结果（轮询所得）。
#[derive(Debug, Clone)]
pub struct SignalAck {
    pub signal_id: String,
    /// applied：已写入事件并生效；rejected：非法请求被拒；pending：尚未处理。
    pub status: String,
    pub delivered: bool,
    pub event_seq: Option<u64>,
    pub error: Option<Value>,
}

/// 入队结果。
#[derive(Debug)]
pub enum EnqueueOutcome {
    /// 新插入，等待消费。
    Accepted,
    /// 相同 signal_id + 相同内容：返回原结果（可能已处理）。
    Existing(SignalAck),
}

/// 锁 run 行的事务中检查 run 未终结，插入 inbox（§6.1）。
pub async fn enqueue(
    pool: &PgPool,
    run_id: &str,
    signal_id: &str,
    kind: InboxKind,
    node_id: Option<&str>,
    payload: &Value,
) -> Result<EnqueueOutcome, PgError> {
    let mut tx = pool.begin().await?;
    let run_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM runs WHERE id = $1 FOR UPDATE")
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(run_status) = run_status else {
        return Err(PgError::RunNotFound(run_id.to_string()));
    };

    // 幂等：相同 signal_id 已存在
    let existing = sqlx::query(
        "SELECT kind, node_id, payload, status, event_seq, error
         FROM run_signals WHERE run_id = $1 AND signal_id = $2 FOR UPDATE",
    )
    .bind(run_id)
    .bind(signal_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(row) = existing {
        let existing_kind: String = row.try_get("kind")?;
        let existing_node: Option<String> = row.try_get("node_id")?;
        let existing_payload: Value = row.try_get("payload")?;
        if existing_kind != kind_str(kind)
            || existing_node.as_deref() != node_id
            || existing_payload != *payload
        {
            return Err(PgError::Conflict(format!(
                "signal_id {signal_id} 已用不同内容提交"
            )));
        }
        let status: String = row.try_get("status")?;
        let event_seq: Option<i64> = row.try_get("event_seq")?;
        let error: Option<Value> = row.try_get("error")?;
        tx.commit().await?;
        return Ok(EnqueueOutcome::Existing(SignalAck {
            signal_id: signal_id.to_string(),
            delivered: status == "applied",
            status,
            event_seq: event_seq.map(|s| s as u64),
            error,
        }));
    }

    let is_terminal = !matches!(run_status.as_str(), "running" | "awaiting_resume");
    if is_terminal {
        return Err(PgError::Conflict(format!(
            "run {run_id} 已终结（{run_status}），拒绝新的输入"
        )));
    }
    sqlx::query(
        "INSERT INTO run_signals (run_id, signal_id, kind, node_id, payload, created_at)
         VALUES ($1, $2, $3, $4, $5, clock_timestamp())",
    )
    .bind(run_id)
    .bind(signal_id)
    .bind(kind_str(kind))
    .bind(node_id)
    .bind(payload)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(EnqueueOutcome::Accepted)
}

fn kind_str(kind: InboxKind) -> &'static str {
    match kind {
        InboxKind::Signal => "signal",
        InboxKind::Cancel => "cancel",
    }
}

/// 查询 inbox 行当前状态。
pub async fn status_of(pool: &PgPool, run_id: &str, signal_id: &str) -> Result<SignalAck, PgError> {
    let row = sqlx::query(
        "SELECT status, event_seq, error FROM run_signals WHERE run_id = $1 AND signal_id = $2",
    )
    .bind(run_id)
    .bind(signal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| PgError::RunNotFound(format!("signal {signal_id} 不存在")))?;
    let status: String = row.try_get("status")?;
    let event_seq: Option<i64> = row.try_get("event_seq")?;
    let error: Option<Value> = row.try_get("error")?;
    Ok(SignalAck {
        signal_id: signal_id.to_string(),
        delivered: status == "applied",
        status,
        event_seq: event_seq.map(|s| s as u64),
        error,
    })
}

/// gateway 等待该行变为 applied/rejected（§6.1）：applied 才返回 delivered，
/// rejected 返回错误；超时返回明确的 pending 结果和 signal_id。
pub async fn wait_applied(
    pool: &PgPool,
    run_id: &str,
    signal_id: &str,
    wait: std::time::Duration,
    poll: std::time::Duration,
) -> Result<SignalAck, PgError> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let ack = status_of(pool, run_id, signal_id).await?;
        if ack.status != "pending" {
            return Ok(ack);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(ack); // pending
        }
        tokio::time::sleep(poll).await;
    }
}

/// rejected 落账的错误载荷（校验失败 / conflict）。
pub fn rejected_error(code: &str, message: &str) -> Value {
    json!({ "code": code, "message": message })
}
