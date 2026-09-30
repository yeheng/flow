//! Postgres 模式的 RPC 集成测试：真起 flow-server 进程（gateway/executor 角色
//! 拆分、SIGKILL 崩溃恢复、订阅轮询）。
//!
//! 需要 FLOW_TEST_DATABASE_URL（默认 127.0.0.1:54329 的本地测试库）；
//! 不可达时跳过。进程脚手架与测试库生命周期都在 `common`（ws_rpc 同指一份，
//! 两套 test_db 各自漂移过一次）。
//!
//! 测试库的 docker 容器由 flow-test-support::pg 管（backend-e2e 也用同一份）：
//! 数据目录 tmpfs、删除带 -v、启动回收孤儿 volume。这里默认连的是一个已经
//! 起着的外部 PG（开发用），所以只做「连库 + 建独立测试库」。

mod common;

use std::time::Duration;

use futures::StreamExt;
use jsonrpsee::core::client::SubscriptionClientT;
use serde_json::{json, Value};

use common::{call, call_err, line_def, named, publish, test_db, wait_status, ServerProc};

/// 集群冒烟：定义生命周期 → run.start 原子创建 → executor 驱动到终态 →
/// timeline / events / signal_status 可读。
#[tokio::test]
async fn postgres_cluster_smoke() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn_pg(&db.url, "all");
    let client = server.client().await;

    // draft 拒绝执行
    let created: Value = call(&client, "workflow.create", json!({"name": "冒烟"})).await;
    let wf = created["workflow_id"].as_str().unwrap().to_string();
    let updated: Value = call(
        &client,
        "workflow.update",
        json!({"workflow_id": wf, "definition": line_def("return 1 + 1;")}),
    )
    .await;
    let draft_v = updated["version"].as_i64().unwrap();
    let err = call_err(
        &client,
        "run.start",
        json!({"workflow_id": wf, "version": draft_v}),
    )
    .await;
    assert!(err.contains("尚未发布"), "{err}");
    call::<Value>(
        &client,
        "workflow.publish",
        json!({"workflow_id": wf, "version": draft_v}),
    )
    .await;

    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": wf, "input": {"x": 5}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let run = wait_status(&client, &run_id, "succeeded", Duration::from_secs(15)).await;
    assert_eq!(run["run"]["output"], json!(2));

    // 时间线
    let timeline: Value = call(&client, "run.timeline", json!({"run_id": run_id})).await;
    assert_eq!(timeline["phase"], json!("succeeded"));
    assert_eq!(timeline["nodes"].as_array().unwrap().len(), 3);

    // 事件增量拉取（from_seq 闭区间语义）
    let evs: Value = call(
        &client,
        "run.events",
        json!({"run_id": run_id, "from_seq": 3}),
    )
    .await;
    let list = evs["events"].as_array().unwrap();
    assert!(list.len() >= 2);
    assert_eq!(list[0]["seq"], json!(3));

    // nodetypes 能力清单
    let nt: Value = call(&client, "nodetypes.list", json!({})).await;
    assert!(nt["node_types"].as_array().unwrap().len() >= 6);

    server.kill();
    db.cleanup().await;
}

/// §6.1：human 信号经持久 inbox 交付；幂等与冲突语义在 RPC 层可见。
#[tokio::test]
async fn postgres_human_signal_delivery() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn_pg(&db.url, "all");
    let client = server.client().await;

    let (wf, _) = publish(
        &client,
        "人工",
        json!({
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "approve", "type": "human_task", "name": "审批"},
                {"id": "end", "type": "end"}
            ],
            "edges": [
                {"from": "start", "to": "approve"},
                {"from": "approve", "to": "end"}
            ]
        }),
    )
    .await;
    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // 等人土节点进入等待（信号先校验再持久化，节点未开始时会被拒）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let evs: Value = call(&client, "run.events", json!({"run_id": run_id})).await;
        let hit = evs["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == json!("node_started") && e["node_id"] == json!("approve"));
        if hit {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "human 节点未开始");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Postgres 模式必须带 signal_id
    let err = call_err(
        &client,
        "run.signal",
        json!({"run_id": run_id, "node_id": "approve", "payload": {}}),
    )
    .await;
    assert!(err.contains("signal_id"), "{err}");

    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "sig-1", "node_id": "approve", "payload": {"ok": true}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(true), "{ack}");
    assert!(ack["event_seq"].as_u64().unwrap() > 1);

    // 重复同 id 同内容：幂等返回 delivered
    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "sig-1", "node_id": "approve", "payload": {"ok": true}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(true));

    // 同 id 不同内容：conflict
    let err = call_err(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "sig-1", "node_id": "approve", "payload": {"ok": false}}),
    )
    .await;
    assert!(
        err.contains("不同内容") || err.contains("conflict") || err.contains("-32012"),
        "{err}"
    );

    wait_status(&client, &run_id, "succeeded", Duration::from_secs(10)).await;

    // signal_status 可查已应用结果（run 终结后仍可查询原结果）
    let st: Value = call(
        &client,
        "run.signal_status",
        json!({"run_id": run_id, "signal_id": "sig-1"}),
    )
    .await;
    assert_eq!(st["status"], json!("applied"));
    assert_eq!(st["delivered"], json!(true));

    server.kill();
    db.cleanup().await;
}

/// gateway/executor 角色拆分：无 executor 时信号 pending（不是 delivered），
/// executor 加入后落账，run.signal_status 可查。
#[tokio::test]
async fn signal_pending_without_executor_then_delivered() {
    let Some(db) = test_db().await else { return };
    let mut gateway = ServerProc::spawn_pg(&db.url, "gateway");
    let client = gateway.client().await;

    let (wf, _) = publish(&client, "拆分", line_def("return input.v;")).await;
    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": wf, "input": {"v": 1}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // human 信号进入 pending（还没有 executor 驱动该 run）
    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "signal_id": "s-1", "node_id": "end", "payload": {"x": 1}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(false), "{ack}");
    assert_eq!(ack["pending"], json!(true));
    assert_eq!(ack["signal_id"], json!("s-1"));

    let st: Value = call(
        &client,
        "run.signal_status",
        json!({"run_id": run_id, "signal_id": "s-1"}),
    )
    .await;
    assert_eq!(st["status"], json!("pending"));

    // executor 加入后接管并消费（该信号非法——end 不等待——最终 rejected）
    let mut executor = ServerProc::spawn_pg(&db.url, "executor");
    let _ = &executor;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let st: Value = call(
            &client,
            "run.signal_status",
            json!({"run_id": run_id, "signal_id": "s-1"}),
        )
        .await;
        if st["status"] != json!("pending") {
            assert_eq!(st["status"], json!("rejected"), "{st}");
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "信号一直 pending");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    gateway.kill();
    executor.kill();
    db.cleanup().await;
}

/// §11.3：SIGKILL 后同库重启，未完成 run 从共享日志恢复（纯节点重放）。
#[tokio::test]
async fn sigkill_recovery_from_shared_log() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn_pg(&db.url, "all");
    let client = server.client().await;

    let def = json!({
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "wait", "type": "delay", "name": "等待", "params": {"ms": 900}},
            {"id": "n1", "type": "script", "name": "脚本", "params": {"code": "return 'done';"}},
            {"id": "end", "type": "end"}
        ],
        "edges": [
            {"from": "start", "to": "wait"},
            {"from": "wait", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    });
    let (wf, _) = publish(&client, "恢复", def).await;
    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    // 等 delay 节点开始后 SIGKILL
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let evs: Value = call(&client, "run.events", json!({"run_id": run_id})).await;
        let hit = evs["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == json!("node_started") && e["node_id"] == json!("wait"));
        if hit {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "delay 未开始");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.kill();

    // 同库重启：run 必须被新进程接管并完成
    let mut server2 = ServerProc::spawn_pg(&db.url, "all");
    let client2 = server2.client().await;
    let run = wait_status(&client2, &run_id, "succeeded", Duration::from_secs(20)).await;
    assert_eq!(run["run"]["output"], json!("done"));

    let evs: Value = call(&client2, "run.events", json!({"run_id": run_id})).await;
    let events = evs["events"].as_array().unwrap();
    let wait_starts = events
        .iter()
        .filter(|e| e["type"] == json!("node_started") && e["node_id"] == json!("wait"))
        .count();
    assert_eq!(
        wait_starts, 2,
        "delay 节点应重放一次（attempt 1/2）：{events:?}"
    );

    server2.kill();
    db.cleanup().await;
}

/// §8：订阅按 run_id 维护游标轮询增量；seq 严格递增，终态事件可达。
#[tokio::test]
async fn subscription_streams_events_in_order() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn_pg(&db.url, "all");
    let client = server.client().await;

    let (wf, _) = publish(&client, "订阅", line_def("return 'hi';")).await;

    let mut sub = client
        .subscribe::<Value, _>("run.subscribe", named(json!({})), "run.unsubscribe")
        .await
        .expect("订阅失败");

    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let mut last_seq = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "订阅未收到 run_completed"
        );
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("订阅超时")
            .expect("订阅流结束");
        let envelope = match msg {
            Ok(v) => v,
            Err(err) => panic!("订阅错误：{err}"),
        };
        assert_eq!(envelope["run_id"], json!(run_id));
        let seq = envelope["seq"].as_u64().unwrap();
        assert!(seq > last_seq, "seq 必须递增：{seq} after {last_seq}");
        last_seq = seq;
        if envelope["type"] == json!("run_completed") {
            break;
        }
    }

    server.kill();
    db.cleanup().await;
}

/// §8：LISTEN/NOTIFY 是订阅的低延迟唤醒路径。服务端兜底轮询拉长到 30s：
/// 纯轮询下订阅开始后新 run 的事件必然等 30s，10s 内收到 run_completed
/// 只能来自 NOTIFY 唤醒。
#[tokio::test]
async fn subscription_woken_by_notify_not_poll() {
    let Some(db) = test_db().await else { return };
    let mut server =
        ServerProc::spawn_pg_with(&db.url, "all", 5000, &[("FLOW_SUBSCRIBE_POLL_MS", "30000")]);
    let client = server.client().await;

    let (wf, _) = publish(&client, "通知唤醒", line_def("return 'hi';")).await;

    let mut sub = client
        .subscribe::<Value, _>("run.subscribe", named(json!({})), "run.unsubscribe")
        .await
        .expect("订阅失败");
    // 等服务端订阅任务完成 LISTEN（subscribe 流建立是异步的）
    tokio::time::sleep(Duration::from_millis(500)).await;

    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, sub.next())
            .await
            .expect("10s 内未收到 run_completed：NOTIFY 唤醒失效，只剩 30s 兜底轮询")
            .expect("订阅流结束");
        let envelope = match msg {
            Ok(v) => v,
            Err(err) => panic!("订阅错误：{err}"),
        };
        if envelope["run_id"] == json!(run_id) && envelope["type"] == json!("run_completed") {
            break;
        }
    }

    server.kill();
    db.cleanup().await;
}

/// 指定 run_id 的订阅（流级契约）：先回放完整历史（seq 从 1 严格递增），
/// 终态追平后流自然结束。断言打在 `PgBackend::subscribe` 上：wire 层的
/// JSON-RPC 订阅只承载事件本体，jsonrpsee 的服务端关闭不会通知客户端
/// （客户端流只在连接断开时结束），「流结束」只在服务端生效。
#[tokio::test]
async fn subscribe_specific_run_replays_and_ends() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn_pg(&db.url, "all");
    let client = server.client().await;

    let (wf, _) = publish(&client, "回放订阅", line_def("return 'hi';")).await;
    let started: Value = call(&client, "run.start", json!({"workflow_id": wf})).await;
    let run_id = started["run_id"].as_str().unwrap().to_string();
    wait_status(&client, &run_id, "succeeded", Duration::from_secs(20)).await;

    // run 已终结才订阅：仍回放完整日志并以「流结束」收尾
    let backend = flow_backend::PgBackend::connect(&db.url, flow_backend::PgConfig::default())
        .await
        .expect("连接后端失败");
    let mut stream = backend.subscribe(Some(run_id.clone()));
    let mut kinds = Vec::new();
    let mut last_seq = 0u64;
    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("订阅流未在终态追平后自然结束")
    {
        assert_eq!(envelope.run_id, run_id);
        assert!(
            envelope.seq > last_seq,
            "回放必须从头且 seq 严格递增：{} after {last_seq}",
            envelope.seq
        );
        last_seq = envelope.seq;
        kinds.push(envelope.event.kind().to_string());
    }
    assert_eq!(kinds.first().unwrap(), "run_started", "{kinds:?}");
    assert_eq!(kinds.last().unwrap(), "run_completed", "{kinds:?}");

    server.kill();
    db.cleanup().await;
}

/// Postgres 模式的 sub_workflow：子 run 经 gateway 单事务创建（确定性 id 幂等），
/// 父 run 靠 LISTEN/NOTIFY 唤醒 + 兜底轮询等待终态（父子可能落在不同 executor）。
#[tokio::test]
async fn postgres_sub_workflow_end_to_end() {
    let Some(db) = test_db().await else { return };
    let mut server = ServerProc::spawn_pg(&db.url, "all");
    let client = server.client().await;

    let (child_wf, _) = publish(&client, "子流程", line_def("return { got: input };")).await;
    let (parent_wf, _) = publish(
        &client,
        "父流程",
        json!({
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "sub", "type": "sub_workflow", "params": {"workflow_id": child_wf}},
                {"id": "end", "type": "end"}
            ],
            "edges": [{"from": "start", "to": "sub"}, {"from": "sub", "to": "end"}]
        }),
    )
    .await;

    let started: Value = call(
        &client,
        "run.start",
        json!({"workflow_id": parent_wf, "input": {"amount": 7}}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_string();

    let run = wait_status(&client, &run_id, "succeeded", Duration::from_secs(20)).await;
    assert_eq!(run["run"]["output"], json!({"got": {"amount": 7}}));

    // 父日志的时间线带确定性 child_run_id；子 run 的 run_started 记录深度 1
    let timeline: Value = call(&client, "run.timeline", json!({"run_id": run_id})).await;
    let sub = timeline["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == "sub")
        .unwrap();
    let child_run_id = sub["child_run_id"].as_str().unwrap().to_string();
    assert_eq!(child_run_id, format!("{run_id}:sub:1"));

    let child = wait_status(&client, &child_run_id, "succeeded", Duration::from_secs(10)).await;
    assert_eq!(child["run"]["output"], json!({"got": {"amount": 7}}));
    let events: Value = call(&client, "run.events", json!({"run_id": child_run_id})).await;
    assert_eq!(events["events"][0]["depth"], json!(1));

    server.kill();
    db.cleanup().await;
}
