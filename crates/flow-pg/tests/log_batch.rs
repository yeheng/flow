//! 批量节点日志在 Postgres 上的成本契约（DESIGN §3.2 / 可观察性设计 §3.3）：
//! 整批 = **一次 runs 行更新 + 一个事务**（锁行 + 租约校验 + seq 分配 +
//! 整批插入 + NOTIFY + 提交）。
//!
//! 这个断言是 `append_log_batch` PG 覆写存在的**唯一**理由：逐条 append 是
//! N 个受保护事务 + 3N 往返 + N 次刷盘，日志的"廉价层"语义会被完全抹平。
//! 没有它，"PG 覆写为单事务"只是一句注释——engine 侧的契约测试只验证
//! driver 调了批接口，trait 默认实现（逐条循环）也满足那个断言。
//!
//! 度量方式：**runs 表上的 UPDATE 触发器 + 墙钟**。
//! 为什么不数 `pg_stat_database.xact_commit`：统计来自收集器，会话读到的
//! 快照既滞后又在实测里严重漏计（50 个真实显式事务只读到 +2），拿它当
//! 判据等于没有判据。触发器不依赖任何统计口径：批实现只 UPDATE 一次
//! runs 行（last_seq += N），逐条实现 UPDATE N 次，差异被 pg_sleep 放大到
//! 调度噪声淹不掉。
//!
//! 用例不启动 PgEngine：网关的扫描循环 / inbox 轮询本身就在写库，会污染
//! 任何墙钟度量。

mod common;

use std::time::{Duration, Instant};

use common::*;
use flow_engine::{Event, LogLevel, LogStream, RunEventSink};
use flow_pg::lease::{self, AcquireOutcome};
use flow_pg::PgRunSink;

/// 触发器里每次 runs UPDATE 的固定延迟。
const SLOW_UPDATE: Duration = Duration::from_millis(30);

/// 造一个「已有 run_started(seq=1)、租约在手」的 sink。
/// 绕开 `create_run` 是为了不起 PgEngine（见文件头注释）。
async fn sink_with_live_run(pool: &sqlx::PgPool, run_id: &str) -> PgRunSink {
    sqlx::query("INSERT INTO workflows (id, name) VALUES ('wf-logbatch', '日志批')")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO workflow_versions (workflow_id, version, definition, checksum, status)
         VALUES ('wf-logbatch', 1, '{}'::jsonb, 'ck', 'published')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runs (id, workflow_id, workflow_version, status, input, last_seq)
         VALUES ($1, 'wf-logbatch', 1, 'running', '{}'::jsonb, 1)",
    )
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO run_events (run_id, seq, ts, payload)
         VALUES ($1, 1, clock_timestamp(), '{\"type\":\"run_started\"}'::jsonb)",
    )
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();

    let AcquireOutcome::Acquired { epoch } =
        lease::acquire(pool, run_id, "inst-A", Duration::from_secs(30))
            .await
            .unwrap()
    else {
        panic!("应能获取租约")
    };
    PgRunSink::new(pool.clone(), run_id.to_string(), "inst-A".into(), epoch, 1)
}

/// runs 表 UPDATE 触发器：每次更新睡 SLOW_UPDATE，把「更新了几次」变成墙钟。
/// `CREATE FUNCTION` 的体里没有绑定参数可用（函数体不是查询模板），
/// 所以延迟只能内联——值来自本文件的 const，不是外部输入。
async fn install_slow_runs_update(pool: &sqlx::PgPool) {
    let body = format!(
        "CREATE OR REPLACE FUNCTION flow_test_slow_runs_update() RETURNS trigger AS $$
         BEGIN PERFORM pg_sleep({}); RETURN NEW; END; $$ LANGUAGE plpgsql",
        SLOW_UPDATE.as_secs_f64()
    );
    sqlx::query(sqlx::AssertSqlSafe(body))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER flow_test_slow_runs_update AFTER UPDATE ON runs
         FOR EACH ROW EXECUTE FUNCTION flow_test_slow_runs_update()",
    )
    .execute(pool)
    .await
    .unwrap();
}

fn log_lines(n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| Event::NodeLog {
            node_id: "n1".into(),
            attempt: 1,
            level: LogLevel::Info,
            stream: LogStream::Stdout,
            message: format!("m{i}"),
        })
        .collect()
}

/// 50 条日志一次写完 = 一次 runs 更新；逐条 = 50 次。
///
/// 双向对照：同一个 sink 上先批后逐条，两个耗时都必须量到，两个比值必须
/// 拉开。这样测试不会退化成"永远通过的空判据"——如果触发器没装上或
/// 两条路径都变快，比值会立刻塌下来。
#[tokio::test]
async fn log_batch_updates_runs_once_not_once_per_line() {
    let Some(db) = test_db().await else { return };
    let pool = db.pool().clone();
    let run_id = "run-logbatch-1";
    let mut sink = sink_with_live_run(&pool, run_id).await;
    install_slow_runs_update(&pool).await;

    const N: usize = 50;
    let batch_start = Instant::now();
    let envelopes = sink.append_log_batch(log_lines(N)).await.unwrap();
    let batch_elapsed = batch_start.elapsed();

    // seq 严格连续升序，首条接在 run_started 之后
    assert_eq!(envelopes.len(), N);
    for (i, e) in envelopes.iter().enumerate() {
        assert_eq!(e.seq, 2 + i as u64, "批内 seq 必须连续：{e:?}");
        assert!(matches!(e.event, Event::NodeLog { .. }));
    }

    // 对照组：同样的 50 行逐条写，逐条 = 每行一次 runs 更新
    let single_start = Instant::now();
    for event in log_lines(N) {
        sink.append(event).await.unwrap();
    }
    let single_elapsed = single_start.elapsed();

    // 两边都必须真的踩到触发器（否则比值没有意义）
    assert!(
        batch_elapsed >= SLOW_UPDATE,
        "批路径只用了 {batch_elapsed:?}，触发器没生效，对照无效"
    );
    assert!(
        single_elapsed >= SLOW_UPDATE * (N as u32 / 2),
        "逐条路径只用了 {single_elapsed:?}，触发器没生效，对照无效"
    );
    // 批 = 1 次更新（~30ms），逐条 = 50 次（~1.5s）。阈值按「更新次数」写，
    // 余量给往返与调度；逐条实现会超出十倍以上，翻不过来。
    assert!(
        batch_elapsed < SLOW_UPDATE * 10,
        "批 {batch_elapsed:?} 超过 10 次更新的量：批事务退化成逐条了"
    );
    assert!(
        batch_elapsed * 5 < single_elapsed,
        "批 {batch_elapsed:?} vs 逐条 {single_elapsed:?}：批事务没拉开差距"
    );

    // last_seq 取自 DB：批后与逐条后的 seq 全部连续，无跳号无重号
    let next = sink
        .append(Event::NodeStarted {
            node_id: "n1".into(),
            attempt: 2,
            child_run_id: None,
            input: None,
        })
        .await
        .unwrap();
    assert_eq!(next.seq, 2 + 2 * N as u64, "批后 seq 不得跳号或重号");

    let evs = events(&pool, run_id).await;
    assert_eq!(evs.len(), 2 * N + 2);
    for (i, row) in evs.iter().enumerate() {
        assert_eq!(row.0, i as i64 + 1, "事件日志 seq 连续：{row:?}");
    }
}

/// 空批短路：一次 runs 更新都不该有。
#[tokio::test]
async fn empty_log_batch_touches_runs_zero_times() {
    let Some(db) = test_db().await else { return };
    let pool = db.pool().clone();
    let run_id = "run-logbatch-empty";
    let mut sink = sink_with_live_run(&pool, run_id).await;
    install_slow_runs_update(&pool).await;

    let start = Instant::now();
    let envelopes = sink.append_log_batch(Vec::new()).await.unwrap();
    let elapsed = start.elapsed();
    assert!(envelopes.is_empty());
    assert!(
        elapsed < SLOW_UPDATE,
        "空批用了 {elapsed:?}，不该有任何语句"
    );
}

/// 批次之间 seq 同样连续（多批驱动）：每批一次 runs 更新，跨批不跳号。
#[tokio::test]
async fn consecutive_batches_keep_sequence_contiguous() {
    let Some(db) = test_db().await else { return };
    let pool = db.pool().clone();
    let run_id = "run-logbatch-multi";
    let mut sink = sink_with_live_run(&pool, run_id).await;
    install_slow_runs_update(&pool).await;

    const BATCHES: usize = 4;
    const PER_BATCH: usize = 25;
    let start = Instant::now();
    for _ in 0..BATCHES {
        sink.append_log_batch(log_lines(PER_BATCH)).await.unwrap();
    }
    let elapsed = start.elapsed();
    // 4 批 = 4 次更新（~120ms）；逐条会是 100 次（~3s）
    assert!(
        elapsed < SLOW_UPDATE * (BATCHES as u32 + 6),
        "{} 批用了 {elapsed:?}：批事务退化成逐条了",
        BATCHES
    );

    sink.append(Event::NodeStarted {
        node_id: "n1".into(),
        attempt: 1,
        child_run_id: None,
        input: None,
    })
    .await
    .unwrap();
    let evs = events(&pool, run_id).await;
    assert_eq!(evs.len(), BATCHES * PER_BATCH + 2);
    for (i, row) in evs.iter().enumerate() {
        assert_eq!(row.0, i as i64 + 1, "跨批 seq 必须连续：{row:?}");
    }
}
