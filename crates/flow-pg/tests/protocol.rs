//! 租约协议测试（DISTRIBUTED.md §11 Phase 1 验收 1/2/6）：
//! - 接管必须等待在途提交，提交后必须读到该事件；
//! - 接管完成后旧持有者的一切操作都失败，同实例名也不能复用旧权利；
//! - run 创建事务原子性、状态词汇表、终态事务的 pending 输入拒绝。

mod common;

use std::time::Duration;

use common::*;
use flow_engine::RunEventSink;
use flow_pg::lease::{self, AcquireOutcome};
use flow_pg::PgRunSink;
use sqlx::Row;

/// §11.1：A 持锁未提交时（租约已过期），B 接管必须等待；A 提交后 B 必须读到该事件。
#[tokio::test]
async fn takeover_waits_for_inflight_commit_then_reads_it() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "t1", def_line("return input.x + 1;")).await;
    let run_id = start_run(&engine, &wf, v, serde_json::json!({"x": 1})).await;
    let pool = db.pool.clone();

    // A 获取租约并追加一个 NodeStarted（短 TTL，随后让它过期）
    let AcquireOutcome::Acquired { epoch: a_epoch } =
        lease::acquire(&pool, &run_id, "inst-A", Duration::from_millis(300))
            .await
            .unwrap()
    else {
        panic!("A 应能获取租约");
    };
    let mut sink_a = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), a_epoch, 1);
    let env = sink_a
        .append(flow_engine::Event::NodeStarted {
            node_id: "n1".into(),
            attempt: 2,
            child_run_id: None,
        })
        .await
        .unwrap();
    assert_eq!(env.seq, 2);

    // 模拟 A 在「租约校验后、事件提交前」暂停：另开事务锁行 + 插入事件但不提交，
    // 此时 A 的租约按墙钟已过期，但事务持有的行锁仍在。
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM runs WHERE id = $1 FOR UPDATE")
        .bind(&run_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO run_events (run_id, seq, ts, payload)
         VALUES ($1, 3, clock_timestamp(), $2)",
    )
    .bind(&run_id)
    .bind(serde_json::json!({"type": "node_completed", "node_id": "n1", "attempt": 2, "output": 2, "duration_ms": 1}))
    .execute(&mut *tx)
    .await
    .unwrap();
    // 等租约过期（300ms TTL）
    tokio::time::sleep(Duration::from_millis(500)).await;

    // B 接管：必须被 A 的行锁挡住
    let pool_b = pool.clone();
    let run_b = run_id.clone();
    let b_task = tokio::spawn(async move {
        lease::acquire(&pool_b, &run_b, "inst-B", Duration::from_secs(30)).await
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!b_task.is_finished(), "B 的接管必须等待 A 的在途事务");

    // A 的事务提交 → B 获准，且必须读到 A 刚提交的事件
    tx.commit().await.unwrap();
    let acquired = tokio::time::timeout(Duration::from_secs(5), b_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let AcquireOutcome::Acquired { epoch: b_epoch } = acquired else {
        panic!("B 在 A 提交后应能接管");
    };
    assert!(b_epoch > a_epoch, "接管必须递增 epoch");

    let events = PgRunSink::read_events(&pool, &run_id, None).await.unwrap();
    assert_eq!(events.len(), 3, "B 必须看到 A 提交的 seq=3 事件");
    assert_eq!(events[2].seq, 3);

    db.close().await;
}

/// §11.2：接管完成后，旧持有者的追加/续期/投影/释放全部 LeaseLost；
/// 同实例名重新获取也必须被拒（租约被 B 有效持有），不能复用旧权利。
#[tokio::test]
async fn after_takeover_old_owner_operations_fail() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "t2", def_line("return 1;")).await;
    let run_id = start_run(&engine, &wf, v, serde_json::json!(null)).await;
    let pool = db.pool.clone();

    let AcquireOutcome::Acquired { epoch: a_epoch } =
        lease::acquire(&pool, &run_id, "inst-A", Duration::from_millis(300))
            .await
            .unwrap()
    else {
        panic!("A 应能获取租约");
    };
    let mut sink_a = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), a_epoch, 1);

    // 租约过期后 B 接管
    tokio::time::sleep(Duration::from_millis(500)).await;
    let AcquireOutcome::Acquired { epoch: b_epoch } =
        lease::acquire(&pool, &run_id, "inst-B", Duration::from_secs(30))
            .await
            .unwrap()
    else {
        panic!("B 应能接管过期租约");
    };

    // A 的所有受保护操作都必须失败（LeaseLost）
    let err = sink_a
        .append(flow_engine::Event::NodeStarted {
            node_id: "n1".into(),
            attempt: 1,
            child_run_id: None,
        })
        .await
        .unwrap_err();
    assert!(err.is_lease_lost(), "旧持有者追加必须 LeaseLost：{err}");

    let err = lease::renew(&pool, &run_id, "inst-A", a_epoch, Duration::from_secs(30))
        .await
        .unwrap_err();
    assert!(err.is_lease_lost(), "旧持有者续期必须 LeaseLost：{err}");

    let err = lease::release(&pool, &run_id, "inst-A", a_epoch)
        .await
        .unwrap_err();
    assert!(err.is_lease_lost(), "旧持有者释放必须 LeaseLost：{err}");

    let mut sink_stale = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), a_epoch, 1);
    let err = sink_stale
        .project_status(flow_engine::DbRunStatus::Running, None)
        .await
        .unwrap_err();
    assert!(err.is_lease_lost(), "旧持有者投影必须 LeaseLost：{err}");

    // 同实例名（重启后 id 相同的误配置）也不能复用：租约被 B 持有 → 不可获取
    let again = lease::acquire(&pool, &run_id, "inst-A", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(
        matches!(again, AcquireOutcome::NotEligible(_)),
        "B 持有期间同实例名不得获取：{again:?}"
    );

    // B 的一切操作正常
    let mut sink_b = PgRunSink::new(pool.clone(), run_id.clone(), "inst-B".into(), b_epoch, 1);
    let env = sink_b
        .append(flow_engine::Event::NodeStarted {
            node_id: "n1".into(),
            attempt: 1,
            child_run_id: None,
        })
        .await
        .unwrap();
    assert_eq!(env.seq, 2);
    lease::renew(&pool, &run_id, "inst-B", b_epoch, Duration::from_secs(30))
        .await
        .unwrap();

    // A 的 last_seq 视角漂移（内存 fold 落后）也必须被拒——防重复事件
    let mut sink_drift = PgRunSink::new(pool.clone(), run_id.clone(), "inst-B".into(), b_epoch, 1);
    let err = sink_drift
        .append(flow_engine::Event::NodeStarted {
            node_id: "n1".into(),
            attempt: 2,
            child_run_id: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, flow_engine::EngineError::LogCorrupted(_)),
        "last_seq 漂移必须报一致性错误：{err}"
    );

    db.close().await;
}

/// §11.6：run 创建事务原子性 + Postgres 状态词汇表 + 孤立状态拒绝。
#[tokio::test]
async fn run_start_is_atomic_and_status_vocabulary_is_enforced() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "t3", def_line("return 1;")).await;

    let created = engine
        .create_run(flow_pg::CreateRun {
            workflow_id: wf.clone(),
            version: Some(v),
            input: serde_json::json!({"k": 1}),
        })
        .await
        .unwrap();
    let pool = db.pool.clone();
    let rec = sqlx::query(
        "SELECT status, last_seq, lease_owner, lease_expires_at FROM runs WHERE id = $1",
    )
    .bind(&created.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rec.get::<String, _>("status"), "running");
    assert_eq!(rec.get::<i64, _>("last_seq"), 1);
    assert!(rec.get::<Option<String>, _>("lease_owner").is_none());
    assert!(rec
        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("lease_expires_at")
        .is_none());
    let events = PgRunSink::read_events(&pool, &created.run_id, None)
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "创建事务必须同时插入 seq=1 的 RunStarted");

    // draft 版本不可执行
    let version = engine
        .store()
        .update_workflow(&wf, &def_line("return 2;"))
        .await
        .unwrap();
    let err = engine
        .create_run(flow_pg::CreateRun {
            workflow_id: wf.clone(),
            version: Some(version),
            input: serde_json::json!(null),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("尚未发布"), "{err}");

    // 不存在的 workflow
    let err = engine
        .create_run(flow_pg::CreateRun {
            workflow_id: "no-such-wf".into(),
            version: None,
            input: serde_json::json!(null),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, flow_pg::PgError::Invalid(_)), "{err:?}");

    // 状态词汇表：Postgres 后端不存在 initializing，CHECK 约束当场拒绝
    let err = sqlx::query(
        "INSERT INTO runs (id, workflow_id, workflow_version, status, input, last_seq)
         VALUES ('bad-status', $1, $2, 'initializing', 'null', 1)",
    )
    .bind(&wf)
    .bind(v)
    .execute(&pool)
    .await;
    assert!(err.is_err(), "initializing 必须被 CHECK 约束拒绝");

    // 已有 run 的 workflow 拒删
    let err = engine.store().delete_workflow(&wf).await.unwrap_err();
    assert!(err.to_string().contains("拒绝删除"), "{err}");

    db.close().await;
}

/// §5.3：终态追加在同一事务里写事件、投影、清租约，并把剩余 pending 输入标 rejected。
#[tokio::test]
async fn terminal_append_rejects_pending_inputs_and_releases_lease() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "t4", def_line("return 1;")).await;
    let run_id = start_run(&engine, &wf, v, serde_json::json!(null)).await;
    let pool = db.pool.clone();

    let AcquireOutcome::Acquired { epoch } =
        lease::acquire(&pool, &run_id, "inst-A", Duration::from_secs(30))
            .await
            .unwrap()
    else {
        panic!("应能获取租约");
    };
    let mut sink = PgRunSink::new(pool.clone(), run_id.clone(), "inst-A".into(), epoch, 1);
    sink.append(flow_engine::Event::NodeStarted {
        node_id: "n1".into(),
        attempt: 1,
        child_run_id: None,
    })
    .await
    .unwrap();

    // 构造一条 pending 输入（绕过 gateway 的终态检查，模拟终态前一刻到达的信号）
    {
        let mut tx = pool.begin().await.unwrap();
        insert_pending_signal_raw(
            &mut tx,
            &run_id,
            "sig-late",
            "n1",
            serde_json::json!({"late": true}),
        )
        .await;
        tx.commit().await.unwrap();
    }

    sink.append_terminal(flow_engine::Event::RunCompleted {
        output: serde_json::json!({"done": true}),
    })
    .await
    .unwrap();

    let rec = sqlx::query(
        "SELECT status, output, ended_at, lease_owner, lease_expires_at, last_seq FROM runs WHERE id = $1",
    )
    .bind(&run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rec.get::<String, _>("status"), "succeeded");
    assert_eq!(
        rec.get::<Option<serde_json::Value>, _>("output").unwrap(),
        serde_json::json!({"done": true})
    );
    assert!(rec
        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("ended_at")
        .is_some());
    assert!(rec.get::<Option<String>, _>("lease_owner").is_none());
    assert_eq!(rec.get::<i64, _>("last_seq"), 3);

    let sig = sqlx::query(
        "SELECT status, error FROM run_signals WHERE run_id = $1 AND signal_id = 'sig-late'",
    )
    .bind(&run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        sig.get::<String, _>("status"),
        "rejected",
        "终态 run 不得留下永不处理的输入"
    );
    assert_eq!(
        sig.get::<serde_json::Value, _>("error")
            .get("code")
            .and_then(|c| c.as_str()),
        Some("conflict")
    );

    // 终态后追加也必须 LeaseLost（status 不再是 running/awaiting_resume）
    let err = sink
        .append(flow_engine::Event::NodeStarted {
            node_id: "n1".into(),
            attempt: 2,
            child_run_id: None,
        })
        .await
        .unwrap_err();
    assert!(err.is_lease_lost(), "终态后追加必须 LeaseLost：{err}");

    db.close().await;
}

/// §6.1：信号入队的幂等与冲突语义。
#[tokio::test]
async fn signal_enqueue_is_idempotent_and_rejects_mismatch() {
    let Some(db) = test_db().await else { return };
    let engine = engine(&db, flow_pg::Role::Gateway).await;
    let (wf, v) = publish_definition(&engine, "t5", def_human()).await;
    let run_id = start_run(&engine, &wf, v, serde_json::json!(null)).await;
    let pool = db.pool.clone();

    let payload = serde_json::json!({"answer": 42});
    let ack = flow_pg::gateway::enqueue(
        &pool,
        &run_id,
        "s1",
        flow_pg::gateway::InboxKind::Signal,
        Some("h"),
        &payload,
    )
    .await
    .unwrap();
    assert!(matches!(ack, flow_pg::gateway::EnqueueOutcome::Accepted));

    // 相同 id + 相同内容 → 返回原结果
    let ack = flow_pg::gateway::enqueue(
        &pool,
        &run_id,
        "s1",
        flow_pg::gateway::InboxKind::Signal,
        Some("h"),
        &payload,
    )
    .await
    .unwrap();
    match ack {
        flow_pg::gateway::EnqueueOutcome::Existing(ack) => assert_eq!(ack.status, "pending"),
        other => panic!("相同 id+内容必须幂等：{other:?}"),
    }

    // 相同 id + 不同 payload → conflict
    let err = flow_pg::gateway::enqueue(
        &pool,
        &run_id,
        "s1",
        flow_pg::gateway::InboxKind::Signal,
        Some("h"),
        &serde_json::json!({"answer": 43}),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, flow_pg::PgError::Conflict(_)), "{err:?}");

    // 相同 id + 不同 node → conflict
    let err = flow_pg::gateway::enqueue(
        &pool,
        &run_id,
        "s1",
        flow_pg::gateway::InboxKind::Signal,
        Some("start"),
        &payload,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, flow_pg::PgError::Conflict(_)), "{err:?}");

    // 不存在的 run
    let err = flow_pg::gateway::enqueue(
        &pool,
        "no-run",
        "s9",
        flow_pg::gateway::InboxKind::Signal,
        Some("h"),
        &payload,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, flow_pg::PgError::RunNotFound(_)));

    db.close().await;
}
