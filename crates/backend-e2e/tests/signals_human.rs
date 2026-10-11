//! 外部信号：human_task 交付、取消、裁决前置校验。
//!
//! 契约来源：DESIGN.md §6.7（外部信号）。v2 里 signal 的幂等键就是命令
//! request_id（持久 inbox 落账语义已随 pg 臂退役；journal 命令日志即权威账）。

use backend_e2e::common::fixtures::{delay_def, human_def, linear_def, timeline_node};
use backend_e2e::common::{
    call, call_err, call_json, publish_workflow, start_run, wait_run_status, wait_run_terminal,
    Conn, Ctx, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};
use std::time::Instant;

e2e_test!(
    human_task_waits_then_signal_completes_run,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "审批", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({ "amount": 99 })).await;

        // 等待中：run 非终态、节点 running、活着的
        let timeline = wait_node_running(&client, &run_id, "h").await;
        assert_eq!(timeline_node(&timeline, "h")["state"], json!("running"));
        let live = call_json(&client, "run.get.view", json!({"run_id": run_id})).await;
        assert_eq!(live["live"], json!(true));

        // 交付信号：payload 即节点输出（§6.7）；signal_id 回显 = 命令幂等键
        let payload = json!({ "approved_by": "alice", "note": "ok" });
        let ack: Value = call(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "h", "payload": payload}),
        )
        .await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");
        assert!(ack["signal_id"].is_string(), "{ack}");

        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(
            run["run"]["output"], payload,
            "human_task 输出 = 信号 payload"
        );

        // signal_received 事件已落盘
        let events: Value = call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let kinds: Vec<&str> = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["event"]["kind"].as_str().unwrap())
            .collect();
        assert!(kinds.contains(&"signal_received"), "{kinds:?}");
        let signal_event = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["event"]["kind"] == json!("signal_received"))
            .unwrap();
        assert_eq!(signal_event["event"]["node_id"], json!("h"));
        assert_eq!(
            signal_event["event"]["payload"]["payload"]["value"],
            payload
        );
    })
);

e2e_test!(signal_to_non_waiting_node_is_rejected, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "死活", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_running(&client, &run_id, "h").await;

        // 节点不存在 / 节点不在等信号：v2 词汇 = invalid（-32602）
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "ghost", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32602, "{err}");

        // start 节点已经 completed，不在等待
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "start", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32602, "{err}");

        // run 尚未被信号改变：仍在等 h
        let run = call_json(&client, "run.get.view", json!({"run_id": run_id})).await;
        assert_eq!(run["run"]["status"], json!("running"));
    })
});

e2e_test!(
    signal_error_codes_for_unknown_and_finished_runs,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // run 不存在：journal_views 映射 not-found（-32011）
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": "nope", "node_id": "h", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        // run 已终结：-32012（同一组码）
        let (workflow_id, _) = publish_workflow(&client, "终态", linear_def("return 1;")).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_run_terminal(&client, &run_id, SHORT).await;
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "n1", "payload": {}}),
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

        let run = wait_run_status(&client, &run_id, "cancelled", TIMEOUT).await;
        assert_eq!(run["live"], json!(false));
        // run_cancelled 必须落盘；不要求它是末事件——取消提交后，在飞的
        // dispatch 仍可能补写 wait_registered 之类滞留事件（驱动竞态窗口，
        // 状态已定）。事件页以投影 LSN 为上界，状态面可能瞬时超前，轮询收敛。
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let events: Value =
                call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
            let events = events["events"].as_array().unwrap().clone();
            if events
                .iter()
                .any(|e| e["event"]["kind"] == json!("run_cancelled"))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "等待 run_cancelled 落盘超时：{events:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // 终态后 cancel：v2 语义 = 幂等空操作（committed 回执，无新事件）
        let again: Value = call_json(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(again["delivered"], json!(true), "{again}");

        // cancel 未知 run：-32011 not found
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
        assert_eq!(timeline["status"], json!("cancelled"));
    }
));

/// human_task / delay 等运行中状态的观测助手。
async fn wait_node_running(client: &Conn, run_id: &str, node_id: &str) -> Value {
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
