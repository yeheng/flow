//! §8：事件提交通知（LISTEN/NOTIFY）。通知只是低延迟唤醒提示，
//! 本测试钉住它的覆盖面和延迟：创建（seq=1）与执行追加都必须触发
//! run_id 通知，且延迟远低于兜底轮询间隔。

mod common;

use std::time::Duration;

use common::*;

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
        run_status(&db.pool, &run_id).await == "succeeded"
    })
    .await;

    db.close().await;
}
