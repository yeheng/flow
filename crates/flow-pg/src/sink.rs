//! PgRunSink：单个 run 的受保护事件出口（`RunEventSink` 的 Postgres 实现）。
//!
//! 每个受保护操作都在同一连接/事务内：锁 runs 行 → 准入检查（owner/epoch/
//! 有效期/last_seq）→ 分配 seq 并插入事件 → 提交。不返回行即 LeaseLost，
//! 调用方（Driver）必须停止派发并静默退出（§5.3）。

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use flow_engine::{
    CommitOutcome, DbRunStatus, EngineError, Envelope, Event, PendingInput, PendingInputKind,
    RunEventSink, RunState,
};

use crate::error::PgError;
use crate::lease;

pub struct PgRunSink {
    pool: PgPool,
    run_id: String,
    instance_id: String,
    epoch: i64,
    /// 本 sink 已确认提交的 last_seq（与内存 fold 一致）。
    last_seq: u64,
}

impl PgRunSink {
    /// 测试与调度器模式也需要直接构造 sink。
    pub fn new(
        pool: PgPool,
        run_id: String,
        instance_id: String,
        epoch: i64,
        last_seq: u64,
    ) -> PgRunSink {
        PgRunSink {
            pool,
            run_id,
            instance_id,
            epoch,
            last_seq,
        }
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    fn sql_err(err: sqlx::Error) -> EngineError {
        EngineError::Backend(err.to_string())
    }

    async fn begin(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, EngineError> {
        self.pool.begin().await.map_err(Self::sql_err)
    }

    /// 锁行 + 准入检查。返回行快照；失败时事务随后由调用方 drop 回滚。
    async fn lock_and_check(
        &self,
        tx: &mut sqlx::postgres::PgConnection,
        expected_last_seq: Option<u64>,
    ) -> Result<lease::RunRow, EngineError> {
        let row = lease::lock_run_row(tx, &self.run_id)
            .await
            .map_err(Self::sql_err)?
            .ok_or_else(|| EngineError::RunNotFound(self.run_id.clone()))?;
        lease::check_writable(&row, &self.instance_id, self.epoch, expected_last_seq)?;
        Ok(row)
    }

    /// 分配 seq 并插入事件（在已持锁的事务内）。返回 (seq, ts)。
    async fn allocate_and_insert(
        &mut self,
        tx: &mut sqlx::postgres::PgConnection,
        event: &Event,
    ) -> Result<(u64, DateTime<Utc>), EngineError> {
        let payload = lease::payload_of(event)?;
        let rec = sqlx::query(
            "WITH owned AS (
                 UPDATE runs SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq
             )
             INSERT INTO run_events (run_id, seq, ts, payload)
             SELECT $1, owned.last_seq, clock_timestamp(), $2 FROM owned
             RETURNING seq, ts",
        )
        .bind(&self.run_id)
        .bind(payload)
        .fetch_one(&mut *tx)
        .await
        .map_err(Self::sql_err)?;
        let seq: i64 = rec.try_get("seq").map_err(Self::sql_err)?;
        let ts: DateTime<Utc> = rec.try_get("ts").map_err(Self::sql_err)?;
        // 与插入同事务 NOTIFY（§8 低延迟提示），提交时才投递
        lease::queue_event_notify(tx, &self.run_id)
            .await
            .map_err(Self::sql_err)?;
        Ok((seq as u64, ts))
    }

    fn envelope(&self, seq: u64, ts: DateTime<Utc>, event: Event) -> Envelope {
        Envelope {
            seq,
            ts,
            run_id: self.run_id.clone(),
            event,
        }
    }

    /// 拒绝剩余 pending 输入，避免终态 run 留下永不处理的输入（§5.3）。
    async fn reject_pending_inputs(
        tx: &mut sqlx::postgres::PgConnection,
        run_id: &str,
    ) -> Result<(), EngineError> {
        sqlx::query(
            "UPDATE run_signals SET status = 'rejected',
                    error = jsonb_build_object('code', 'conflict', 'message', 'run 已终结')
             WHERE run_id = $1 AND status = 'pending'",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(Self::sql_err)?;
        Ok(())
    }

    /// 终态投影的内容：status / output / error。
    fn terminal_projection(
        event: &Event,
    ) -> Result<(DbRunStatus, Option<Value>, Option<String>), EngineError> {
        match event {
            Event::RunCompleted { output } => {
                Ok((DbRunStatus::Succeeded, Some(output.clone()), None))
            }
            Event::RunFailed { error } => Ok((DbRunStatus::Failed, None, Some(error.clone()))),
            Event::RunCancelled {} => Ok((DbRunStatus::Cancelled, None, None)),
            other => Err(EngineError::Node(format!(
                "append_terminal 只接受终态事件，收到 {}",
                other.kind()
            ))),
        }
    }

    /// 接管后发现日志已终结：以事件为准修正 DB（投影 + 释放租约），不插入事件。
    pub async fn reconcile_terminal(&mut self, state: &RunState) -> Result<(), EngineError> {
        let status = state.phase.as_db_status();
        if !status.is_terminal() {
            return Ok(());
        }
        let mut tx = self.begin().await?;
        self.lock_and_check(&mut tx, None).await?;
        sqlx::query(
            "UPDATE runs SET status = $1, output = $2, error = $3, ended_at = clock_timestamp(),
                    lease_owner = NULL, lease_expires_at = NULL
             WHERE id = $4",
        )
        .bind(status.as_str())
        .bind(state.output.clone())
        .bind(state.fatal_error.clone())
        .bind(&self.run_id)
        .execute(&mut *tx)
        .await
        .map_err(Self::sql_err)?;
        Self::reject_pending_inputs(&mut tx, &self.run_id).await?;
        tx.commit().await.map_err(Self::sql_err)?;
        Ok(())
    }

    /// 事件读取（只读，不需要租约）。
    /// from_seq 为 None 时全量读取并校验「首条=1 + 相邻连续」；
    /// Some(n) 时 SQL 下推过滤（只取 seq >= n）并校验相邻连续——
    /// 订阅轮询的增量读取不必（也无法）要求首条为 1。
    pub async fn read_events(
        pool: &PgPool,
        run_id: &str,
        from_seq: Option<u64>,
    ) -> Result<Vec<Envelope>, PgError> {
        type Validate = fn(&[Envelope]) -> Result<(), flow_engine::EngineError>;
        let (from, validate): (i64, Validate) = match from_seq {
            Some(from) => (from as i64, flow_engine::validate_sequence_contiguous),
            None => (1, flow_engine::validate_sequence),
        };
        let rows = sqlx::query(
            "SELECT seq, ts, payload FROM run_events
             WHERE run_id = $1 AND seq >= $2 ORDER BY seq ASC",
        )
        .bind(run_id)
        .bind(from)
        .fetch_all(pool)
        .await?;
        // 回放起点的空结果必须区分「run 不存在」与「已追平」：订阅状态机
        // （run_tail）依赖 RunNotFound 立即结束流。增量读取（from > 1）为空
        // 是正常的追平信号，不做存在性检查。
        if rows.is_empty() && from <= 1 {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id = $1)")
                    .bind(run_id)
                    .fetch_one(pool)
                    .await?;
            if !exists {
                return Err(PgError::RunNotFound(run_id.to_string()));
            }
        }
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i64 = row.try_get("seq")?;
            let ts: DateTime<Utc> = row.try_get("ts")?;
            let payload: Value = row.try_get("payload")?;
            let event: Event = serde_json::from_value(payload).map_err(|e| {
                PgError::Engine(EngineError::LogCorrupted(format!(
                    "run {run_id} 事件 {} 无法解析：{e}",
                    seq
                )))
            })?;
            events.push(Envelope {
                seq: seq as u64,
                ts,
                run_id: run_id.to_string(),
                event,
            });
        }
        validate(&events)?;
        Ok(events)
    }
}

impl RunEventSink for PgRunSink {
    /// 受保护追加（§5.3）。没有返回行（租约失效/状态不对/last_seq 漂移）时
    /// 回滚并返回 LeaseLost。
    fn append<'a>(&'a mut self, event: Event) -> BoxFuture<'a, Result<Envelope, EngineError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, Some(self.last_seq)).await?;
            let (seq, ts) = self.allocate_and_insert(&mut tx, &event).await?;
            tx.commit().await.map_err(Self::sql_err)?;
            self.last_seq = seq;
            Ok(self.envelope(seq, ts, event))
        })
    }

    /// 批量节点日志：**两条语句 + 一个事务**（锁行/租约校验一次、seq 一次
    /// 分配、整批一次插入、一次 NOTIFY、一次提交）。
    ///
    /// 逐条 append_log 在 PG 上 = 每行一个受保护事务 + 3 个往返 + WAL 刷盘，
    /// 日志的"廉价层"语义会被抹平，刷屏 run 的 driver 循环会成串往返。批内
    /// 用 `jsonb_array_elements ... WITH ORDINALITY` 一次插完：runs 行在事务里
    /// 只被 UPDATE 一次（N 次的话是 N 个必然废弃的 MVCC 行版本），`runs` 行锁
    /// 的持有时间也从 O(N) 往返降到 O(1)——`run.cancel` 不会被日志刷屏堵住。
    fn append_log_batch<'a>(
        &'a mut self,
        events: Vec<Event>,
    ) -> BoxFuture<'a, Result<Vec<Envelope>, EngineError>> {
        Box::pin(async move {
            if events.is_empty() {
                return Ok(Vec::new());
            }
            let payloads = events
                .iter()
                .map(lease::payload_of)
                .collect::<Result<Vec<_>, _>>()?;
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, Some(self.last_seq)).await?;
            // 一次加 N。base = 本批**第一条**的 seq（= 旧 last_seq + 1），
            // 批内 seq = base + (ord - 1)，末条正好是新的 last_seq。
            let base: i64 = sqlx::query_scalar(
                "UPDATE runs SET last_seq = last_seq + $2 WHERE id = $1
                 RETURNING last_seq - $2 + 1",
            )
            .bind(&self.run_id)
            .bind(events.len() as i64)
            .fetch_one(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            let rows = sqlx::query(
                "INSERT INTO run_events (run_id, seq, ts, payload)
                 SELECT $1, $2 + g.ord - 1, clock_timestamp(), g.payload
                 FROM jsonb_array_elements($3::jsonb) WITH ORDINALITY AS g(payload, ord)
                 RETURNING seq, ts",
            )
            .bind(&self.run_id)
            .bind(base)
            .bind(Value::Array(payloads))
            .fetch_all(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            // 与插入同事务 NOTIFY（§8 低延迟提示），提交时才投递
            lease::queue_event_notify(&mut tx, &self.run_id)
                .await
                .map_err(Self::sql_err)?;
            tx.commit().await.map_err(Self::sql_err)?;
            // RETURNING 不保证行序：按 seq 定位回原事件，包络严格升序。
            // last_seq 只有一个来源——DB 算出来的批次末值，不本地重算。
            let mut slots: Vec<Option<(u64, DateTime<Utc>)>> = vec![None; events.len()];
            for row in rows {
                let seq: i64 = row.try_get("seq").map_err(Self::sql_err)?;
                let ts: DateTime<Utc> = row.try_get("ts").map_err(Self::sql_err)?;
                let offset = (seq - base) as usize;
                let slot = slots.get_mut(offset).ok_or_else(|| {
                    EngineError::Backend(format!("run {} 批日志 seq 越界：{seq}", self.run_id))
                })?;
                *slot = Some((seq as u64, ts));
            }
            let mut envelopes = Vec::with_capacity(events.len());
            for (event, slot) in events.into_iter().zip(slots) {
                let (seq, ts) = slot.ok_or_else(|| {
                    EngineError::Backend(format!("run {} 批日志缺行", self.run_id))
                })?;
                envelopes.push(self.envelope(seq, ts, event));
            }
            self.last_seq = envelopes.last().map_or(self.last_seq, |e| e.seq);
            Ok(envelopes)
        })
    }

    /// 终态追加：事件 + 元数据投影 + 清空租约 + 拒绝剩余 pending 输入，同一事务。
    fn append_terminal<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, EngineError>> {
        Box::pin(async move {
            let (status, output, error) = Self::terminal_projection(&event)?;
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, Some(self.last_seq)).await?;
            let (seq, ts) = self.allocate_and_insert(&mut tx, &event).await?;
            sqlx::query(
                "UPDATE runs SET status = $1, output = $2, error = $3,
                        ended_at = clock_timestamp(),
                        lease_owner = NULL, lease_expires_at = NULL
                 WHERE id = $4",
            )
            .bind(status.as_str())
            .bind(output)
            .bind(error)
            .bind(&self.run_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            Self::reject_pending_inputs(&mut tx, &self.run_id).await?;
            tx.commit().await.map_err(Self::sql_err)?;
            self.last_seq = seq;
            Ok(self.envelope(seq, ts, event))
        })
    }

    /// 非终态投影更新，同样匹配 owner/epoch（§5.3）。
    fn project_status<'a>(
        &'a mut self,
        status: DbRunStatus,
        error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, None).await?;
            sqlx::query("UPDATE runs SET status = $1, output = NULL, error = $2 WHERE id = $3")
                .bind(status.as_str())
                .bind(error)
                .bind(&self.run_id)
                .execute(&mut *tx)
                .await
                .map_err(Self::sql_err)?;
            tx.commit().await.map_err(Self::sql_err)?;
            Ok(())
        })
    }

    /// 未消费的外部输入（§6.2 步骤 2 的可见性读取；真正的锁定在 commit/reject）。
    fn poll_inputs<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<PendingInput>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT signal_id, kind, node_id, payload FROM run_signals
                 WHERE run_id = $1 AND status = 'pending'
                 ORDER BY created_at, signal_id
                 LIMIT 16",
            )
            .bind(&self.run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(Self::sql_err)?;
            Ok(rows
                .into_iter()
                .filter_map(|row| {
                    let signal_id: String = row.try_get("signal_id").ok()?;
                    let kind: String = row.try_get("kind").ok()?;
                    let node_id: Option<String> = row.try_get("node_id").ok()?;
                    let payload: Value = row.try_get("payload").ok()?;
                    let kind = match kind.as_str() {
                        "signal" => PendingInputKind::Signal,
                        "cancel" => PendingInputKind::Cancel,
                        _ => return None,
                    };
                    Some(PendingInput {
                        signal_id,
                        kind,
                        node_id,
                        payload,
                    })
                })
                .collect())
        })
    }

    /// 原子消费有效信号（§6.2 步骤 4）：锁 runs 行 → 锁信号行 → 分配 seq 插入
    /// SignalReceived → 标记 applied。信号行已非 pending 时返回 Duplicate。
    fn commit_signal<'a>(
        &'a mut self,
        input: &'a PendingInput,
        event: Event,
    ) -> BoxFuture<'a, Result<CommitOutcome, EngineError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, Some(self.last_seq)).await?;
            let status: Option<String> = sqlx::query_scalar(
                "SELECT status FROM run_signals WHERE run_id = $1 AND signal_id = $2 FOR UPDATE",
            )
            .bind(&self.run_id)
            .bind(&input.signal_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            match status.as_deref() {
                None => {
                    return Err(EngineError::Node(format!(
                        "signal {} 不存在",
                        input.signal_id
                    )))
                }
                Some(s) if s != "pending" => {
                    tx.commit().await.map_err(Self::sql_err)?;
                    return Ok(CommitOutcome::Duplicate);
                }
                _ => {}
            }
            let (seq, ts) = self.allocate_and_insert(&mut tx, &event).await?;
            sqlx::query(
                "UPDATE run_signals SET status = 'applied', event_seq = $1
                 WHERE run_id = $2 AND signal_id = $3",
            )
            .bind(seq as i64)
            .bind(&self.run_id)
            .bind(&input.signal_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            tx.commit().await.map_err(Self::sql_err)?;
            self.last_seq = seq;
            Ok(CommitOutcome::Applied(self.envelope(seq, ts, event)))
        })
    }

    /// 非法请求只标 rejected，不写事件、不写 RunFailed（§6.2 步骤 3）。
    fn reject_signal<'a>(
        &'a mut self,
        input: &'a PendingInput,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, None).await?;
            let affected = sqlx::query(
                "UPDATE run_signals SET status = 'rejected', error = $1
                 WHERE run_id = $2 AND signal_id = $3 AND status = 'pending'",
            )
            .bind(json!({ "code": "invalid", "message": reason }))
            .bind(&self.run_id)
            .bind(&input.signal_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::sql_err)?
            .rows_affected();
            tx.commit().await.map_err(Self::sql_err)?;
            if affected == 0 {
                tracing::debug!(run_id = %self.run_id, signal_id = %input.signal_id, "信号已被处理，拒绝跳过");
            }
            Ok(())
        })
    }

    /// 消费取消命令：RunCancelled + 终态投影 + 租约释放 + 拒绝其余 pending 输入，
    /// 同一事务（§6.2）；提交后 Driver abort 本地任务。
    fn consume_cancel<'a>(
        &'a mut self,
        input: &'a PendingInput,
    ) -> BoxFuture<'a, Result<CommitOutcome, EngineError>> {
        Box::pin(async move {
            let event = Event::RunCancelled {};
            let mut tx = self.begin().await?;
            self.lock_and_check(&mut tx, Some(self.last_seq)).await?;
            let status: Option<String> = sqlx::query_scalar(
                "SELECT status FROM run_signals WHERE run_id = $1 AND signal_id = $2 FOR UPDATE",
            )
            .bind(&self.run_id)
            .bind(&input.signal_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            match status.as_deref() {
                None => {
                    return Err(EngineError::Node(format!(
                        "cancel {} 不存在",
                        input.signal_id
                    )))
                }
                Some(s) if s != "pending" => {
                    tx.commit().await.map_err(Self::sql_err)?;
                    return Ok(CommitOutcome::Duplicate);
                }
                _ => {}
            }
            let (seq, ts) = self.allocate_and_insert(&mut tx, &event).await?;
            // 先把本信号行落 applied：同事务稍后的 reject_pending_inputs 只动
            // status='pending' 的行，不先标记的话这条取消会被自己误拒
            // （gateway 的 wait_applied 看到 rejected → run.cancel 返回 -32012，
            // 而取消其实已生效——契约要求 delivered 语义）
            sqlx::query(
                "UPDATE run_signals SET status = 'applied', event_seq = $1
                 WHERE run_id = $2 AND signal_id = $3",
            )
            .bind(seq as i64)
            .bind(&self.run_id)
            .bind(&input.signal_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            sqlx::query(
                "UPDATE runs SET status = $1, output = NULL, error = NULL,
                        ended_at = clock_timestamp(),
                        lease_owner = NULL, lease_expires_at = NULL
                 WHERE id = $2",
            )
            .bind(DbRunStatus::Cancelled.as_str())
            .bind(&self.run_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::sql_err)?;
            Self::reject_pending_inputs(&mut tx, &self.run_id).await?;
            tx.commit().await.map_err(Self::sql_err)?;
            self.last_seq = seq;
            Ok(CommitOutcome::Applied(self.envelope(seq, ts, event)))
        })
    }

    /// 优雅释放租约（§5.2）。失败不重试，等 TTL 到期由接管流程处理。
    fn release<'a>(&'a mut self) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            lease::release(&self.pool, &self.run_id, &self.instance_id, self.epoch).await
        })
    }
}
