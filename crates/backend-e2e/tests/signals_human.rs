//! 外部信号：human_task 交付、取消、裁决前置校验、pg 持久 inbox 落账语义。
//!
//! 契约来源：DESIGN.md §6.7（外部信号）、§9（signal_id 两后端差异）、
//! `flow-pg/src/gateway.rs`（持久 inbox）。

use backend_e2e::common::fixtures::{delay_def, human_def, linear_def, timeline_node};
use backend_e2e::common::{
    call, call_err, call_json, connect, publish_workflow, shared, spawn_pg_server, start_run,
    wait_run_status, wait_run_terminal, Ctx, TestDb, E2E_DB_PREFIX, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

e2e_test!(
    human_task_waits_then_signal_completes_run,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "审批", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({ "amount": 99 })).await;

        // 等待中：run 非终态、节点 running、活着的
        let timeline = wait_node_running(&client, &run_id, "h").await;
        assert_eq!(timeline_node(&timeline, "h")["state"], json!("running"));
        let live = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        assert_eq!(live["live"], json!(true));

        // 交付信号：payload 即节点输出（§6.7）
        let payload = json!({ "approved_by": "alice", "note": "ok" });
        let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "node_id": "h", "signal_id": "sig-approve", "payload": payload}),
    )
    .await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");
        if ctx.is_pg() {
            // Postgres：signal_id 必填且落账可查，响应带 event_seq
            assert!(ack["signal_id"].is_string(), "{ack}");
            assert!(ack["event_seq"].is_i64(), "{ack}");
        } else {
            // SQLite：同步交付，回显客户端提供的 id、没有持久账可查（无 event_seq）
            assert_eq!(ack["signal_id"], json!("sig-approve"), "{ack}");
            assert!(ack.get("event_seq").is_none(), "{ack}");
        }

        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(
            run["run"]["output"], payload,
            "human_task 输出 = 信号 payload"
        );

        // signal_received 事件已落盘
        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let kinds: Vec<&str> = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["type"].as_str().unwrap())
            .collect();
        assert!(kinds.contains(&"signal_received"), "{kinds:?}");
        let signal_event = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == json!("signal_received"))
            .unwrap();
        assert_eq!(signal_event["node_id"], json!("h"));
        assert_eq!(signal_event["payload"], payload);
    })
);

e2e_test!(
    sqlite_signal_response_echoes_provided_signal_id,
    |ctx: &mut Ctx| Box::pin(async move {
        if ctx.is_pg() {
            return; // Postgres 分支由 pg 专属用例覆盖
        }
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "回显", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_running(&client, &run_id, "h").await;

        let ack: Value = call(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "h", "signal_id": "client-id-1", "payload": {}}),
        )
        .await;
        assert_eq!(ack["delivered"], json!(true));
        assert_eq!(
            ack["signal_id"],
            json!("client-id-1"),
            "只回显客户端提供的 id"
        );
        wait_run_terminal(&client, &run_id, SHORT).await;
    })
);

e2e_test!(signal_to_non_waiting_node_is_rejected, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "死活", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_running(&client, &run_id, "h").await;

        // 节点不存在 / 节点不在等信号：-32010（Driver 先校验再落账，§6.7）
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "ghost", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32010, "{err}");

        // start 节点已经 completed，不在等待
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "start", "payload": {}}),
        )
        .await;
        assert!(err.code() == -32010 || err.code() == -32012, "{err}");

        // run 尚未被信号改变：仍在等 h
        let run = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        assert_eq!(run["run"]["status"], json!("running"));
    })
});

e2e_test!(
    signal_error_codes_for_unknown_and_finished_runs,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // run 不存在：-32011（pg 上 signal_id 必填先于存在性校验，两臂都带上）
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": "nope", "node_id": "h", "signal_id": "sig-x", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        // run 已终结：-32012（两臂同一组码）
        let (workflow_id, _) = publish_workflow(&client, "终态", linear_def("return 1;")).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_run_terminal(&client, &run_id, SHORT).await;
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "n1", "signal_id": "sig-y", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");
    })
);

e2e_test!(
    cancel_live_run_is_delivered_and_terminal,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "取消", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_running(&client, &run_id, "h").await;

        let ack: Value = call_json(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");
        if ctx.is_pg() {
            // Postgres 取消经持久 inbox：signal_id 落账可查
            assert!(ack["signal_id"].is_string(), "{ack}");
        } else {
            assert!(ack.get("signal_id").is_none(), "{ack}");
        }

        let run = wait_run_status(&client, &run_id, "cancelled", TIMEOUT).await;
        assert_eq!(run["live"], json!(false));
        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        assert_eq!(events.last().unwrap()["type"], json!("run_cancelled"));

        // 终态后 cancel：conflict
        let err = call_err(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(err.code(), -32012, "{err}");

        // cancel 未知 run：-32011
        let err = call_err(&client, "run.cancel", json!({"run_id": "nope"})).await;
        assert_eq!(err.code(), -32011, "{err}");
    })
);

e2e_test!(cancel_aborts_inflight_delay, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "长等待", delay_def(30_000)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let timeline = wait_node_running(&client, &run_id, "d").await;
        assert_eq!(timeline_node(&timeline, "d")["state"], json!("running"));

        let started = std::time::Instant::now();
        call_json(&client, "run.cancel", json!({"run_id": run_id})).await;
        let run = wait_run_status(&client, &run_id, "cancelled", TIMEOUT).await;
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "cancel 必须立刻中断在飞的 delay，而不是等 30s：{:?}",
            started.elapsed()
        );
        assert_eq!(run["run"]["status"], json!("cancelled"));

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline["phase"], json!("cancelled"));
    }
));

// ---- Postgres 专属：持久 inbox 落账 ----

e2e_test!(pg_signal_id_required, |ctx: &mut Ctx| Box::pin(
    async move {
        if !ctx.is_pg() {
            return;
        }
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "必填", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_running(&client, &run_id, "h").await;

        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "h", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32010, "Postgres 必须带 signal_id：{err}");

        // 空字符串 / 超长同样拒绝（1-128 字符）
        for bad in ["", &"x".repeat(129)] {
            let err = call_err(
                &client,
                "run.signal",
                json!({"run_id": run_id, "node_id": "h", "signal_id": bad, "payload": {}}),
            )
            .await;
            assert_eq!(err.code(), -32010, "signal_id {bad:?}：{err}");
        }
    }
));

e2e_test!(
    pg_signal_status_reports_delivered,
    |ctx: &mut Ctx| Box::pin(async move {
        if !ctx.is_pg() {
            return;
        }
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "落账", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_running(&client, &run_id, "h").await;

        // 落账前查询：未知 signal_id → -32011
        let err = call_err(
            &client,
            "run.signal_status",
            json!({"run_id": run_id, "signal_id": "never-sent"}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "node_id": "h", "signal_id": "sig-1", "payload": {"ok": true}}),
    )
    .await;
        assert_eq!(ack["delivered"], json!(true));
        let event_seq = ack["event_seq"].as_u64().unwrap();

        let status: Value = call(
            &client,
            "run.signal_status",
            json!({"run_id": run_id, "signal_id": "sig-1"}),
        )
        .await;
        assert_eq!(status["status"], json!("applied"), "{status}");
        assert_eq!(status["delivered"], json!(true));
        assert_eq!(status["event_seq"], json!(event_seq));
        assert!(status["error"].is_null());

        wait_run_terminal(&client, &run_id, SHORT).await;
    })
);

e2e_test!(
    sqlite_signal_status_is_explicitly_unsupported,
    |ctx: &mut Ctx| Box::pin(async move {
        if ctx.is_pg() {
            return;
        }
        let client = ctx.client().await;
        let err = call_err(
            &client,
            "run.signal_status",
            json!({"run_id": "whatever", "signal_id": "sig-1"}),
        )
        .await;
        assert_eq!(
            err.code(),
            -32010,
            "SQLite 无持久 inbox，明确拒绝而不是伪造：{err}"
        );
        assert!(
            err.message().contains("Postgres"),
            "错误信息必须说明只有 Postgres 后端提供：{err}"
        );
    })
);

/// gateway/executor 拆分：信号先 pending 落账，executor 加入后才被消费。
/// 这个用例自己管进程与测试库（不能用 Ctx——它的 all 角色进程会立刻消费信号）。
#[tokio::test]
async fn pg_signal_pending_without_executor_then_delivered() {
    let pg = shared().await;
    let db = TestDb::create(&pg.url(), E2E_DB_PREFIX).await;
    let bin = env!("CARGO_BIN_EXE_flow-server");

    // gateway：只提供元数据与持久输入入口，没有 Driver 消费 inbox
    let mut gateway = spawn_pg_server(bin, &db.url, "gateway", 600).await;
    let client = connect(gateway.addr()).await;
    let (workflow_id, _) = publish_workflow(&client, "拆分", human_def()).await;
    let run_id = start_run(&client, &workflow_id, json!({})).await;

    let ack: Value = call(
        &client,
        "run.signal",
        json!({"run_id": run_id, "node_id": "h", "signal_id": "sig-p", "payload": {"by": "gateway"}}),
    )
    .await;
    assert_eq!(ack["delivered"], json!(false), "{ack}");
    assert_eq!(ack["pending"], json!(true));
    assert_eq!(ack["signal_id"], json!("sig-p"));

    let status: Value = call(
        &client,
        "run.signal_status",
        json!({"run_id": run_id, "signal_id": "sig-p"}),
    )
    .await;
    assert_eq!(status["status"], json!("pending"), "{status}");

    // executor 加入：接管 run、human 节点进入等待、消费 inbox 里的信号
    let mut executor = spawn_pg_server(bin, &db.url, "executor", 5_000).await;
    let run = wait_run_status(&client, &run_id, "succeeded", TIMEOUT).await;
    assert_eq!(run["run"]["output"], json!({ "by": "gateway" }));

    let status: Value = call(
        &client,
        "run.signal_status",
        json!({"run_id": run_id, "signal_id": "sig-p"}),
    )
    .await;
    assert_eq!(status["status"], json!("applied"), "{status}");
    assert_eq!(status["delivered"], json!(true));

    gateway.kill();
    executor.kill();
    db.cleanup().await;
}

/// 轮询直到节点进入 running（human_task / delay 等运行中状态的观测助手）。
async fn wait_node_running(
    client: &backend_e2e::common::Client,
    run_id: &str,
    node_id: &str,
) -> Value {
    let deadline = std::time::Instant::now() + SHORT;
    loop {
        let timeline: Value = call_json(client, "run.timeline", json!({"run_id": run_id})).await;
        if timeline_node(&timeline, node_id)["state"] == json!("running") {
            return timeline;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "节点 {node_id} 未进入 running：{timeline}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
