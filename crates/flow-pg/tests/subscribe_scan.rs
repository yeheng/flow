//! 订阅扫描候选的配额契约（§8）：活跃 run 与「30s 窗口内终结」的 run 各有
//! 自己的 LIMIT 256 配额，互不挤占。
//!
//! 回归背景（backend-perf 的 subscribe_latency 场景量出来的坑）：两集合共用
//! 一个 `ORDER BY started_at ASC LIMIT 256` 时，刚批量终结的老 run 会把名额
//! 占满，**新活跃 run 一个都进不了扫描**——它的事件不被任何游标追平，订阅端
//! 要等老 run 退出 ended_at 回看窗口（最多 30s）才恢复推送，p95 从亚秒劣化到
//! ~25s。这里钉两条：活跃 run 永不被终态补漏挤出、ended 配额有界且优先最近
//! 终结的（最可能漏终态的）。

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::*;

/// 往 runs 表直插一行（本测试只关心扫描候选，不需要真执行）。
async fn insert_run(
    pool: &sqlx::PgPool,
    id: &str,
    wf: &str,
    v: i64,
    status: &str,
    started: DateTime<Utc>,
    ended: Option<DateTime<Utc>>,
) {
    sqlx::query(
        "INSERT INTO runs (id, workflow_id, workflow_version, status, input, last_seq, started_at, ended_at)
         VALUES ($1, $2, $3, $4, 'null', 1, $5, $6)",
    )
    .bind(id)
    .bind(wf)
    .bind(v)
    .bind(status)
    .bind(started)
    .bind(ended)
    .execute(pool)
    .await
    .expect("插入 run 行失败");
}

/// 300 个刚终结的老 run 挤满回看窗口时，唯一活跃的**新** run 必须仍进候选。
#[tokio::test]
async fn active_run_is_never_crowded_out_by_recently_ended() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "scan-quota", def_line("return 1;")).await;
    let pool = db.pool().clone();

    let base = Utc::now();
    for i in 0..300 {
        // started_at 严格早于活跃 run（旧代码按 started_at ASC 取名额时它们排前面）；
        // ended_at 必须落在 30s 回看窗口内，否则根本不进终态补漏集合
        let started = base - Duration::from_secs(3600) + Duration::from_millis(i);
        let ended = base - Duration::from_secs(20) + Duration::from_millis(i);
        insert_run(
            &pool,
            &format!("old-{i:03}"),
            &wf,
            v,
            "succeeded",
            started,
            Some(ended),
        )
        .await;
    }
    // 最新才创建的活跃 run：旧代码按 started_at ASC 取 256 个时它排在最后
    insert_run(
        &pool,
        "active-new",
        &wf,
        v,
        "running",
        base + Duration::from_secs(60),
        None,
    )
    .await;

    let candidates = engine
        .store()
        .watch_candidates(Duration::from_secs(30))
        .await
        .unwrap();

    assert!(
        candidates
            .iter()
            .any(|(id, terminal)| id == "active-new" && !terminal),
        "活跃 run 必须进订阅扫描候选（被终态补漏挤出 = 事件推送饿死）：{candidates:?}"
    );
    let ended = candidates.iter().filter(|(_, terminal)| *terminal).count();
    assert!(ended <= 256, "ended 配额必须有界，实际 {ended}");

    db.cleanup().await;
}

/// ended 配额优先最近终结的 run（短 run 最可能漏终态），且总量有界。
#[tokio::test]
async fn ended_quota_is_bounded_and_prefers_recent() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "scan-quota-ended", def_line("return 1;")).await;
    let pool = db.pool().clone();

    let base = Utc::now();
    for i in 0..300 {
        // 终结时间严格递增且都在回看窗口内：old-000 最老、old-299 最新
        let started = base - Duration::from_secs(3600) + Duration::from_millis(i);
        let ended = base - Duration::from_secs(20) + Duration::from_millis(i);
        insert_run(
            &pool,
            &format!("old-{i:03}"),
            &wf,
            v,
            "succeeded",
            started,
            Some(ended),
        )
        .await;
    }

    let candidates = engine
        .store()
        .watch_candidates(Duration::from_secs(30))
        .await
        .unwrap();
    let ids: Vec<&str> = candidates.iter().map(|(id, _)| id.as_str()).collect();

    assert_eq!(ids.len(), 256, "ended 配额应恰好取满 256：{ids:?}");
    assert!(ids.contains(&"old-299"), "最近终结的必须优先：{ids:?}");
    assert!(
        !ids.contains(&"old-000"),
        "配额已满时最老的应被挤出（下一轮/窗口滑动后再补）：{ids:?}"
    );

    db.cleanup().await;
}
