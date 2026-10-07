//! §8：事件提交通知（LISTEN/NOTIFY）。通知只是低延迟唤醒提示，
//! 本测试钉住它的覆盖面和延迟：创建（seq=1）与执行追加都必须触发
//! run_id 通知，且延迟远低于兜底轮询间隔。

mod common;

use std::time::Duration;

use common::*;
use flow_test_support::pg::TestDb;

#[tokio::test]
async fn event_notifications_cover_create_and_append() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::All).await;
    // 返回前已完成 LISTEN：之后提交的事件通知必然可达
    let mut notify = engine
        .event_notifications()
        .await
        .expect("创建事件监听失败");

    let (wf, v) = publish_definition(&engine, "notify", def_line("return 1;")).await;
    let t0 = tokio::time::Instant::now();
    let run_id = start_run(&engine, &wf, v, serde_json::json!({})).await;

    let exec = engine.clone();
    tokio::spawn(async move { exec.run_executor().await });

    // 创建（seq=1 RunStarted）与 executor 追加（node_started…run_completed）
    // 两类写路径都必须触发通知；载荷只能是 run_id 本身
    let mut seen = 0;
    while seen < 2 {
        let id = tokio::time::timeout(Duration::from_secs(10), notify.recv())
            .await
            .unwrap_or_else(|_| panic!("10s 内未收齐 run {run_id} 的 2 条事件通知"))
            .expect("通知流提前结束");
        assert_eq!(id.len(), run_id.len(), "通知载荷只应携带 run_id：{id}");
        if id == run_id {
            seen += 1;
        }
    }
    let latency = t0.elapsed();
    assert!(
        latency < Duration::from_secs(2),
        "NOTIFY 唤醒延迟应远低于兜底轮询：{latency:?}"
    );

    wait_until("run 完成", Duration::from_secs(15), || async {
        run_status(db.pool(), &run_id).await == "succeeded"
    })
    .await;

    db.cleanup().await;
}

/// §8 资源边界：`EventHub::stop()` 之后订阅流必须结束。
///
/// 若 `EventHub` 直接持有 `broadcast::Sender`，`stop()` 只取消轮询任务时
/// 最后一个 sender 始终存活 → 订阅者永远收不到 `RecvError::Closed`，
/// 挂在 `run_tail` / `broadcast_tail` 的 select 里不退出，停机时每次订阅
/// 留一个永不退出的 task。
#[tokio::test]
async fn hub_stop_ends_existing_subscriptions() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;

    // stop 之前建立的接收端
    let rx = engine.subscribe_events();
    let rx2 = engine.subscribe_events();

    engine.shutdown().await;

    // 两个既有接收端都必须在有限时间内收到 Closed，而不是永远挂住
    for (label, mut r) in [("rx1", rx), ("rx2", rx2)] {
        loop {
            match tokio::time::timeout(Duration::from_secs(10), r.recv()).await {
                Ok(Ok(_)) => continue, // 停机前的残留事件，继续收
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                Ok(Err(other)) => panic!("{label} 期望 Closed，实际 {other:?}"),
                Err(_) => panic!("{label} 在 stop 之后仍未收到 Closed（订阅流挂死）"),
            }
        }
    }

    db.cleanup().await;
}

/// §8 资源边界：反复建/丢通知流不得在 PG 里累积 LISTEN 后端连接。
///
/// 每次 `event_notifications()` 都占住池里一个连接（`PgListener` 终身持有，
/// 本文件开头的注释有说明）。反复建立并丢弃接收端后，PG 侧的 LISTEN 后端数
/// 必须回到基线——否则每建一个通知流就漏一个连接。
#[tokio::test]
async fn repeated_notification_streams_do_not_leak_pg_backends() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;

    let count_listeners = |db: &TestDb| {
        let pool = db.pool().clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity WHERE query ILIKE '%LISTEN%'",
            )
            .fetch_one(&pool)
            .await
            .expect("query listener count")
        }
    };

    // 基线：把前面测试可能留下的后端等它自然消退
    tokio::time::sleep(Duration::from_secs(2)).await;
    let baseline = count_listeners(&db).await;

    for _ in 0..4 {
        drop(
            engine
                .event_notifications()
                .await
                .expect("create notification stream"),
        );
    }
    // 给连接回收留时间（后端退出不是瞬时的）
    tokio::time::sleep(Duration::from_secs(5)).await;

    let after = count_listeners(&db).await;
    assert!(
        after <= baseline + 1,
        "反复建立通知流后 LISTEN 后端数从 {baseline} 涨到 {after}（连接未回收）"
    );

    engine.shutdown().await;
    db.cleanup().await;
}
