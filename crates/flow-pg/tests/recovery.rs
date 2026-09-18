//! 接管恢复分类测试（DISTRIBUTED.md §11 Phase 1 验收 3/5 + §7）：
//! 纯节点重放、待重试重建退避、human_task 继续等待、副作用节点人工裁决
//! （副作用准入检查：接管后不自动重放）、已记录裁决消费、fatal 保留。

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use flow_engine::RunEventSink;
use flow_pg::lease::{self, AcquireOutcome};
use flow_pg::PgRunSink;

use serde_json::json;

/// §11.3：Running 纯节点 → 接管后重放（attempt+1）。
#[tokio::test]
async fn takeover_replays_pure_node_with_new_attempt() {
    let Some(db) = test_db().await else { return };
    let gw = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&gw, "r1", def_line("return input.x + 1;")).await;
    let run_id = start_run(&gw, &wf, v, json!({"x": 41})).await;
    let pool = db.pool.clone();

    // 模拟崩溃窗口：NodeStarted 已提交，节点执行结果未知，执行进程消失
    let AcquireOutcome::Acquired { epoch } =
        lease::acquire(&pool, &run_id, "inst-A", Duration::from_millis(300))
            .await
            .unwrap()
    else {
        panic!("应能获取租约")
    };
    let mut sink = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), epoch, 1);
    sink.append(flow_engine::Event::NodeStarted {
        node_id: "n1".into(),
        attempt: 1,
        child_run_id: None,
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    // 新实例接管并驱动
    let b = engine(&db, flow_pg::Role::All).await;
    let runner = tokio::spawn({
        let b = b.clone();
        async move { b.run_executor().await }
    });
    wait_until("run 完成", Duration::from_secs(10), || async {
        run_status(&pool, &run_id).await == "succeeded"
    })
    .await;
    assert_eq!(run_output(&pool, &run_id).await, Some(json!(42)));

    let evs = events(&pool, &run_id).await;
    // n1 重放：node_started attempt 1 和 2，completed attempt 2
    let started = evs
        .iter()
        .filter(|(_, k, n, _, _)| k == "node_started" && n.as_deref() == Some("n1"))
        .count();
    let completed = evs
        .iter()
        .filter(|(_, k, n, _, p)| {
            k == "node_completed"
                && n.as_deref() == Some("n1")
                && p.get("attempt") == Some(&json!(2))
        })
        .count();
    assert_eq!(started, 2, "纯节点必须以 attempt+1 重放：{evs:?}");
    assert_eq!(completed, 1);
    assert_eq!(attempts_of(&evs, "end", "node_completed").len(), 1);

    runner.abort();
    b.shutdown().await;
    db.close().await;
}

/// §11.3：Failed{retryable:true} → 接管后重建退避计时器，继续重试直到成功。
#[tokio::test]
async fn takeover_resumes_retry_backoff() {
    let Some(db) = test_db().await else { return };
    // 前两次 500（一次在接管前，一次在接管后），第三次成功
    let resp = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let resp2 = resp.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let resp2 = resp2.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let n = resp2.fetch_add(1, Ordering::SeqCst);
                let (status, body) = if n < 2 {
                    ("500 Internal Server Error", "nope")
                } else {
                    ("200 OK", "ok")
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let gw = engine(&db, flow_pg::Role::Gateway).await;
    let mut def = def_http(None);
    def["nodes"][1]["params"]["url"] = json!(format!("http://{addr}/flaky"));
    def["nodes"][1]["params"]["retry"] = json!({"max_attempts": 3, "backoff_ms": 300});
    let (wf, v) = publish_definition(&gw, "r2", def).await;
    let run_id = start_run(&gw, &wf, v, json!(null)).await;
    let pool = db.pool.clone();

    let a = engine(&db, flow_pg::Role::All).await;
    let runner_a = tokio::spawn({
        let a = a.clone();
        async move { a.run_executor().await }
    });

    // 等 attempt 1 失败并进入退避
    wait_until("attempt 1 可重试失败", Duration::from_secs(10), || async {
        events(&pool, &run_id).await.iter().any(|(_, k, n, _, p)| {
            k == "node_failed"
                && n.as_deref() == Some("h")
                && p.get("retryable") == Some(&json!(true))
        })
    })
    .await;

    // 模拟执行进程消失：退避计时器被丢弃，租约不再续期
    a.shutdown().await;
    let _ = runner_a.await.unwrap();

    // 新实例接管：从 NodeFailed 时间戳重建退避（已过期 → 立即重试）
    let b = engine(&db, flow_pg::Role::All).await;
    let runner_b = tokio::spawn({
        let b = b.clone();
        async move { b.run_executor().await }
    });
    wait_until("run 完成", Duration::from_secs(15), || async {
        matches!(
            run_status(&pool, &run_id).await.as_str(),
            "succeeded" | "failed"
        )
    })
    .await;
    assert_eq!(
        run_status(&pool, &run_id).await,
        "succeeded",
        "第三次应 200"
    );

    let evs = events(&pool, &run_id).await;
    let started = evs
        .iter()
        .filter(|(_, k, n, _, _)| k == "node_started" && n.as_deref() == Some("h"))
        .count();
    assert_eq!(started, 3, "三次尝试：{evs:?}");
    assert_eq!(resp.load(Ordering::SeqCst), 3, "恰好三次真实请求");
    // 下游 end 只执行一次
    assert_eq!(attempts_of(&evs, "end", "node_completed").len(), 1);

    runner_b.abort();
    b.shutdown().await;
    db.close().await;
}

/// §11.5：NodeStarted 后、发 HTTP 前执行进程消失 → 接管者必须等人工裁决
/// （副作用准入检查：不自动重放），裁决 retry 后恰好发出一次请求。
#[tokio::test]
async fn takeover_http_side_effect_requires_adjudication_then_retries_once() {
    let Some(db) = test_db().await else { return };
    let (addr, count) = http_counter("HTTP/1.1 200 OK").await;
    let gw = engine(&db, flow_pg::Role::Gateway).await;
    let mut def = def_http(None);
    def["nodes"][1]["params"]["url"] = json!(format!("http://{addr}/pay"));
    let (wf, v) = publish_definition(&gw, "r3", def).await;
    let run_id = start_run(&gw, &wf, v, json!(null)).await;
    let pool = db.pool.clone();

    // NodeStarted 已落盘，HTTP 未发出（SIGSTOP/SIGKILL 窗口）
    let AcquireOutcome::Acquired { epoch } =
        lease::acquire(&pool, &run_id, "inst-A", Duration::from_millis(300))
            .await
            .unwrap()
    else {
        panic!("应能获取租约")
    };
    let mut sink = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), epoch, 1);
    sink.append(flow_engine::Event::NodeStarted {
        node_id: "h".into(),
        attempt: 1,
        child_run_id: None,
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let b = engine(&db, flow_pg::Role::All).await;
    let runner = tokio::spawn({
        let b = b.clone();
        async move { b.run_executor().await }
    });

    // 接管后必须进入人工裁决，不发请求
    wait_until("进入 awaiting_resume", Duration::from_secs(10), || async {
        run_status(&pool, &run_id).await == "awaiting_resume"
    })
    .await;
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "接管后不得自动重放副作用节点"
    );

    // 无效裁决（非法 action）→ rejected，节点继续等待
    let ack = b
        .signal(&run_id, "bad-1", "h", &json!({"action": "nonsense"}))
        .await
        .unwrap();
    assert_eq!(ack.status, "rejected");
    assert_eq!(
        run_status(&pool, &run_id).await,
        "awaiting_resume",
        "非法裁决不得改变 run"
    );

    // 合法 retry 裁决 → 重新执行，恰好一次请求
    let ack = b
        .signal(&run_id, "adj-1", "h", &json!({"action": "retry"}))
        .await
        .unwrap();
    assert!(ack.delivered, "有效裁决必须 delivered：{ack:?}");
    wait_until("run 完成", Duration::from_secs(10), || async {
        run_status(&pool, &run_id).await == "succeeded"
    })
    .await;
    assert_eq!(count.load(Ordering::SeqCst), 1, "裁决后恰好发出一次请求");

    // 重复裁决（同 id）幂等返回 delivered
    let ack = b
        .signal(&run_id, "adj-1", "h", &json!({"action": "retry"}))
        .await
        .unwrap();
    assert!(ack.delivered, "重复查询原结果：{ack:?}");

    runner.abort();
    b.shutdown().await;
    db.close().await;
}

/// §7：已记录的裁决在接管时被消费——succeeded 不再发请求。
#[tokio::test]
async fn takeover_consumes_recorded_adjudication() {
    let Some(db) = test_db().await else { return };
    let (addr, count) = http_counter("HTTP/1.1 200 OK").await;
    let gw = engine(&db, flow_pg::Role::Gateway).await;
    let mut def = def_http(None);
    def["nodes"][1]["params"]["url"] = json!(format!("http://{addr}/pay"));
    let (wf, v) = publish_definition(&gw, "r4", def).await;
    let run_id = start_run(&gw, &wf, v, json!(null)).await;
    let pool = db.pool.clone();

    let AcquireOutcome::Acquired { epoch } =
        lease::acquire(&pool, &run_id, "inst-A", Duration::from_millis(300))
            .await
            .unwrap()
    else {
        panic!("应能获取租约")
    };
    let mut sink = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), epoch, 1);
    sink.append(flow_engine::Event::NodeStarted {
        node_id: "h".into(),
        attempt: 1,
        child_run_id: None,
    })
    .await
    .unwrap();
    sink.append(flow_engine::Event::SignalReceived {
        node_id: "h".into(),
        payload: json!({"action": "succeeded", "output": {"manually": "verified"}}),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let b = engine(&db, flow_pg::Role::All).await;
    let runner = tokio::spawn({
        let b = b.clone();
        async move { b.run_executor().await }
    });

    wait_until("run 完成", Duration::from_secs(10), || async {
        run_status(&pool, &run_id).await == "succeeded"
    })
    .await;
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "已裁决 succeeded 不得再发请求"
    );
    assert_eq!(
        run_output(&pool, &run_id).await,
        Some(json!({"manually": "verified"}))
    );

    runner.abort();
    b.shutdown().await;
    db.close().await;
}

/// §11.3：human_task 等待中接管 → 继续等待，不重复写 node_started；信号照常交付。
#[tokio::test]
async fn takeover_continues_human_wait_without_restarting() {
    let Some(db) = test_db().await else { return };
    let gw = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&gw, "r5", def_human()).await;
    let run_id = start_run(&gw, &wf, v, json!(null)).await;
    let pool = db.pool.clone();

    let a = engine(&db, flow_pg::Role::All).await;
    let runner_a = tokio::spawn({
        let a = a.clone();
        async move { a.run_executor().await }
    });
    wait_until("human 节点已开始", Duration::from_secs(10), || async {
        events(&pool, &run_id)
            .await
            .iter()
            .any(|(_, k, n, _, _)| k == "node_started" && n.as_deref() == Some("h"))
    })
    .await;

    // A 下线（等待无副作用 → 释放租约）
    a.shutdown().await;
    let _ = runner_a.await.unwrap();

    let b = engine(&db, flow_pg::Role::All).await;
    let runner_b = tokio::spawn({
        let b = b.clone();
        async move { b.run_executor().await }
    });
    // 给 B 一点时间接管（也不许重复写 started）
    tokio::time::sleep(Duration::from_millis(600)).await;

    let ack = b
        .signal(&run_id, "h-sig-1", "h", &json!({"approved": true}))
        .await
        .unwrap();
    assert!(ack.delivered, "接管后信号必须照常交付：{ack:?}");
    wait_until("run 完成", Duration::from_secs(10), || async {
        run_status(&pool, &run_id).await == "succeeded"
    })
    .await;

    let evs = events(&pool, &run_id).await;
    let started = evs
        .iter()
        .filter(|(_, k, n, _, _)| k == "node_started" && n.as_deref() == Some("h"))
        .count();
    assert_eq!(started, 1, "接管不得重复写 node_started：{evs:?}");
    assert_eq!(
        run_output(&pool, &run_id).await,
        Some(json!({"approved": true}))
    );

    runner_b.abort();
    b.shutdown().await;
    db.close().await;
}

/// §11.3：fatal 失败在接管后保留结论，独立分支跑到自然终态后 run 才判失败。
#[tokio::test]
async fn takeover_preserves_fatal_and_independent_branch_finishes() {
    let Some(db) = test_db().await else { return };
    let gw = engine(&db, flow_pg::Role::Gateway).await;
    let def = json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "n_fail", "type": "script", "name": "必败", "params": {"code": "throw new Error('boom');"}},
            {"id": "n_ok", "type": "delay", "name": "慢分支", "params": {"ms": 1200}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "n_fail"},
            {"from": "start", "to": "n_ok"},
            {"from": "n_fail", "to": "end"},
            {"from": "n_ok", "to": "end"}
        ]
    });
    let (wf, v) = publish_definition(&gw, "r6", def).await;
    let run_id = start_run(&gw, &wf, v, json!(null)).await;
    let pool = db.pool.clone();

    let a = engine(&db, flow_pg::Role::All).await;
    let runner_a = tokio::spawn({
        let a = a.clone();
        async move { a.run_executor().await }
    });

    // n_fail 致命失败已持久化，独立分支还在跑
    wait_until("n_fail 致命失败", Duration::from_secs(10), || async {
        events(&pool, &run_id).await.iter().any(|(_, k, n, _, p)| {
            k == "node_failed"
                && n.as_deref() == Some("n_fail")
                && p.get("retryable") == Some(&json!(false))
        })
    })
    .await;
    assert_eq!(
        run_status(&pool, &run_id).await,
        "running",
        "独立分支未完不许判失败"
    );

    // A 下线：RunFailed 尚未写（n_ok 在途）——这正是「节点失败后、run 终态前崩溃」窗口
    a.shutdown().await;
    let _ = runner_a.await.unwrap();
    assert_eq!(run_status(&pool, &run_id).await, "running");
    let evs_before = events(&pool, &run_id).await;
    assert!(
        !evs_before.iter().any(|(_, k, _, _, _)| k == "run_failed"),
        "下线时不得已有 run_failed"
    );

    // B 接管：n_ok 重放，end 因 upstream_failed 跳过，run 以 fatal 收尾
    let b = engine(&db, flow_pg::Role::All).await;
    let runner_b = tokio::spawn({
        let b = b.clone();
        async move { b.run_executor().await }
    });
    wait_until("run 失败收尾", Duration::from_secs(15), || async {
        run_status(&pool, &run_id).await == "failed"
    })
    .await;

    let err = run_error(&pool, &run_id).await.unwrap();
    assert!(err.contains("n_fail"), "fatal 结论必须保留：{err}");
    let evs = events(&pool, &run_id).await;
    let ok_started = evs
        .iter()
        .filter(|(_, k, n, _, _)| k == "node_started" && n.as_deref() == Some("n_ok"))
        .count();
    assert_eq!(ok_started, 2, "独立分支（纯节点）重放：{evs:?}");
    assert!(evs.iter().any(|(_, k, n, _, p)| k == "node_skipped"
        && n.as_deref() == Some("end")
        && p.get("reason") == Some(&json!("upstream_failed"))));
    assert!(evs.iter().any(|(_, k, _, _, _)| k == "run_failed"));

    runner_b.abort();
    b.shutdown().await;
    db.close().await;
}
