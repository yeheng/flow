//! gateway 入口（DISTRIBUTED.md §6）：持久 inbox 的入队、确认与查询。
//!
//! - 入队只代表 accepted，不能返回 delivered=true；
//! - 相同 signal_id + 相同内容返回原结果（幂等），不同内容返回 conflict；
//! - 若原请求已处理，即使 run 已终结也可查询原结果；
//! - gateway 不得通过直接清租约或只改 status 实现取消。

use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::error::PgError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxKind {
    Signal,
    Cancel,
}

impl InboxKind {
    pub fn as_str(self) -> &'static str {
        kind_str(self)
    }
}

/// 入队后的观察结果（轮询所得）。类型单一来源在 flow-dto；
/// Postgres 的 signal_id 是真实落账的 inbox 主键，构造时始终 Some。
pub use flow_dto::SignalAck;

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
            || !jsonb_equivalent(&existing_payload, payload)
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
            signal_id: Some(signal_id.to_string()),
            delivered: status == "applied",
            status,
            event_seq: event_seq.map(|s| s as u64),
            error,
        }));
    }

    let is_terminal = !flow_dto::DbRunStatus::is_active_str(&run_status);
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
    .ok_or_else(|| PgError::SignalNotFound(signal_id.to_string()))?;
    let status: String = row.try_get("status")?;
    let event_seq: Option<i64> = row.try_get("event_seq")?;
    let error: Option<Value> = row.try_get("error")?;
    Ok(SignalAck {
        signal_id: Some(signal_id.to_string()),
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

/// jsonb 语义等价比较：PG 把 jsonb 数值按 numeric 语义归一（1 与 1.0 相等），
/// 而 serde_json::Number 的相等按表示（PosInt(1) != Float(1.0)）。幂等校验若按
/// Rust 表示比较，语义相同的重试（如 1 与 1.0）会被误判成「signal_id 已用
/// 不同内容提交」，客户端永远无法用同一个 id 重投。结构递归比较；数值仅在
/// 表示不同**且双方都能无损折算 f64**时才折算——超出 2^53 的整数折算会
/// 坍缩（u64::MAX 与 u64::MAX-1 同值），宁可判不等让幂等重试报 conflict，
/// 也不能把不同的整数当成同一个。
fn jsonb_equivalent(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_equivalent(x, y),
        (Value::Object(m), Value::Object(n)) => {
            m.len() == n.len()
                && m.iter()
                    .all(|(k, v)| n.get(k).is_some_and(|w| jsonb_equivalent(v, w)))
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(u, v)| jsonb_equivalent(u, v))
        }
        _ => a == b,
    }
}

fn number_equivalent(x: &serde_json::Number, y: &serde_json::Number) -> bool {
    if x == y {
        return true;
    }
    // 双方都能无损表示成 f64 才比（浮点数本身无损；整数需 ≤ 2^53）
    let as_f64_lossless = |n: &serde_json::Number| match (n.as_u64(), n.as_i64()) {
        (Some(u), _) if u > (1u64 << 53) => None,
        (_, Some(i)) if i.unsigned_abs() > (1u64 << 53) => None,
        _ => n.as_f64(),
    };
    match (as_f64_lossless(x), as_f64_lossless(y)) {
        (Some(f), Some(g)) => f == g,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn payload_equality_follows_jsonb_semantics() {
        // 同一数值的不同表示：PG 视为相等，幂等重试必须放行
        assert!(jsonb_equivalent(
            &json!({"ratio": 1.0}),
            &json!({"ratio": 1})
        ));
        assert!(jsonb_equivalent(&json!(2), &json!(2.0)));
        // 值不同就是不同
        assert!(!jsonb_equivalent(&json!(1), &json!(2)));
        // 类型不同就是不同（字符串 "1" 不是数字 1）
        assert!(!jsonb_equivalent(&json!("1"), &json!(1)));
        // 大整数精确比较：表示相同时不走 f64 折算
        let big = u64::MAX;
        assert!(jsonb_equivalent(&json!(big), &json!(big)));
        assert!(!jsonb_equivalent(&json!(big), &json!(big - 1)));
        // 结构递归
        assert!(jsonb_equivalent(
            &json!({"a": [1, {"b": 2.0}]}),
            &json!({"a": [1.0, {"b": 2}]})
        ));
        assert!(!jsonb_equivalent(
            &json!({"a": 1}),
            &json!({"a": 1, "b": 2})
        ));
    }
}
