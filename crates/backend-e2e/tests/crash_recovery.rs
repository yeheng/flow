//! 崩溃恢复端到端：SIGKILL 后用同一份存储重启，验证 §7 恢复分类。
//!
//! - delay 中杀掉：重放整段时长后跑完（不续算剩余时间）；
//! - http_call 请求在飞时杀掉：副作用状态不明 → awaiting_resume 人工裁决
//!   （succeeded / failed / retry 三个分支）；
//! - 已终结的 run 重启后不重复执行（事件日志不变，无第二个写者）。
//!
//! 两个后端同一组契约：SQLite 走 recover_unfinished，Postgres 走 executor
//! 接管 + 共享日志。

use backend_e2e::common::fixtures::{
    delay_def, http_def_full, linear_def, timeline_node, StubHttp,
};
use backend_e2e::common::{
    call, call_json, publish_workflow, start_run, wait_run_status, wait_run_terminal, Ctx, SHORT,
    TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};
use std::time::Duration;

/// 等 run 的某个节点出现 node_started（副作用窗口已开）。
async fn wait_node_started(client: &backend_e2e::common::Client, run_id: &str, node_id: &str) {
    let deadline = std::time::Instant::now() + SHORT;
    loop {
        let events: Value = call_json(client, "run.events", json!({"run_id": run_id})).await;
        let started = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == json!("node_started") && e["node_id"] == json!(node_id));
        if started {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "节点 {node_id} 未出现 node_started：{events}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

e2e_test!(
    restart_resumes_run_interrupted_mid_delay,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "中断的等待", delay_def(1_200)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_started(&client, &run_id, "d").await;

        // delay 只过了个零头就 SIGKILL：内存计时器不是权威（§6.5/§7 已知限制）
        ctx.restart().await;
        let client = ctx.client().await;

        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");

        // 重放整段时长：恢复后 elapsed ≥ 1200ms（不续算剩余时间）。
        // journal 无墙钟（ended_at null）：时长语义由输出 slept_ms 钉住。
        if ctx.is_journal() {
            assert!(run["run"]["ended_at"].is_null());
        } else {
            let started_at =
                chrono::DateTime::parse_from_rfc3339(run["run"]["started_at"].as_str().unwrap())
                    .unwrap();
            let ended_at =
                chrono::DateTime::parse_from_rfc3339(run["run"]["ended_at"].as_str().unwrap())
                    .unwrap();
            let elapsed = ended_at
                .signed_duration_since(started_at)
                .num_milliseconds();
            assert!(elapsed >= 1_200, "delay 必须重放整段，实测 {elapsed}ms");
        }

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "d")["state"], json!("completed"));
        assert_eq!(
            timeline_node(&timeline, "d")["output"],
            json!({ "slept_ms": 1200 })
        );
    })
);

/// 等 run 的 http 请求日志行落盘（run.events 可见）。
async fn wait_request_log_written(client: &backend_e2e::common::Client, run_id: &str) {
    let deadline = std::time::Instant::now() + SHORT;
    loop {
        let events: Value = call_json(client, "run.events", json!({"run_id": run_id})).await;
        let written = events["events"].as_array().unwrap().iter().any(|e| {
            e["type"] == json!("node_log")
                && e["message"].as_str().unwrap_or("").starts_with("→ POST")
        });
        if written {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "http 请求日志未落盘：{events}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

e2e_test!(
    restart_asks_for_adjudication_on_side_effect_node,
    |ctx: &mut Ctx| Box::pin(async move {
        // 只接受连接、永不响应：http_call 确定性地挂在请求里
        let stub = StubHttp::hang().await;
        let client = ctx.client().await;
        let definition = http_def_full(json!({
            "method": "POST",
            "url": stub.url("/pay"),
            "body": {"amount": "${input.amount}"}
        }));
        let (workflow_id, _) = publish_workflow(&client, "支付流程", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({ "amount": 99 })).await;
        wait_node_started(&client, &run_id, "call").await;
        // 日志发射即忘：要断言"已落盘的日志跨 SIGKILL 存活"，必须等它出现在
        // 事件日志里再杀（发射到落盘之间的微小窗口内丢尾巴属于分层语义）。
        // journal 的 http 日志在 ObservationStore 不进事件流——等节点进入
        // uncertain 等待（重启后 awaiting_resume）即可杀。
        if !ctx.is_journal() {
            wait_request_log_written(&client, &run_id).await;
        } else {
            // journal：等授权落账再杀（同上，已知问题窗口）
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // 请求已发出但未收到响应时杀进程：副作用是否发生不可知（§7 分类表）
        ctx.restart().await;
        let client = ctx.client().await;

        let run = wait_run_status(&client, &run_id, "awaiting_resume", TIMEOUT).await;
        assert_eq!(run["live"], json!(true), "run 仍在引擎里等待裁决");
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline["status"], json!("awaiting_resume"));
        assert_eq!(
            timeline_node(&timeline, "call")["state"],
            json!("running"),
            "副作用节点接管后保持 Running，等人工裁决"
        );

        // 崩溃前发出的请求日志（进程级持久）在重启后仍可查：日志叙事跨崩溃存活
        // （journal 的请求叙事在审计事实与观测存储里，不在 v1 事件面）
        if !ctx.is_journal() {
            let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
            assert!(
                events["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["type"] == json!("node_log")
                        && e["message"].as_str().unwrap_or("").starts_with("→ POST ")),
                "http 请求日志必须在崩溃后仍可读：{events}"
            );
        }

        // 人确认这次副作用成功了：引擎不替用户猜
        let adjudication = json!({
            "action": "succeeded",
            "output": {"status": 200, "body": {"paid": true}}
        });
        let ack: Value = call(
            &client,
            "run.signal",
            json!({
                "run_id": run_id,
                "node_id": "call",
                "signal_id": "adj-1",
                "payload": adjudication
            }),
        )
        .await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");

        let run = wait_run_status(&client, &run_id, "succeeded", TIMEOUT).await;
        assert_eq!(
            run["run"]["output"],
            json!({"status": 200, "body": {"paid": true}}),
            "裁决输出成为节点输出并透传"
        );
    })
);

e2e_test!(adjudication_failed_marks_run_failed, |ctx: &mut Ctx| {
    Box::pin(async move {
        let stub = StubHttp::hang().await;
        let client = ctx.client().await;
        let definition = http_def_full(json!({
            "method": "POST",
            "url": stub.url("/pay")
        }));
        let (workflow_id, _) = publish_workflow(&client, "判负", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_started(&client, &run_id, "call").await;
        // journal：等授权落账再杀（已知问题：DispatchStarted 与授权之间
        // SIGKILL，重启重派的任务会静默挂起——见迁移文档 §3）
        if ctx.is_journal() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        ctx.restart().await;
        let client = ctx.client().await;
        wait_run_status(&client, &run_id, "awaiting_resume", TIMEOUT).await;

        let ack: Value = call(
            &client,
            "run.signal",
            json!({
                "run_id": run_id,
                "node_id": "call",
                "signal_id": "adj-fail",
                "payload": {"action": "failed", "error": "下游返回重复支付"}
            }),
        )
        .await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");

        let run = wait_run_status(&client, &run_id, "failed", TIMEOUT).await;
        assert!(
            run["run"]["error"].as_str().unwrap().contains("重复支付"),
            "裁决的错误原因必须落终态：{run}"
        );
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "call")["state"], json!("failed"));
    })
});

e2e_test!(
    adjudication_retry_reexecutes_node,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::hang().await;
        let client = ctx.client().await;
        // 短超时：retry 裁决后重放的请求会超时失败（不再无限挂住）
        let definition = http_def_full(json!({
            "method": "GET",
            "url": stub.url("/slow"),
            "timeout_ms": 300
        }));
        let (workflow_id, _) = publish_workflow(&client, "重试裁决", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_node_started(&client, &run_id, "call").await;
        // journal：等授权落账再杀（同上，已知问题窗口）
        if ctx.is_journal() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        ctx.restart().await;
        let client = ctx.client().await;
        wait_run_status(&client, &run_id, "awaiting_resume", TIMEOUT).await;

        let ack: Value = call(
            &client,
            "run.signal",
            json!({
                "run_id": run_id,
                "node_id": "call",
                "signal_id": "adj-retry",
                "payload": {"action": "retry"}
            }),
        )
        .await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");

        // 重放 attempt+1：v1 里再次超时 → retryable → 失败。v2 语义：外部
        // 操作超时 = uncertain（副作用可能已发生；安全立场不因人工授权重试
        // 而放松）→ 再次 awaiting_resume；attempt+1 证明确实重放了。
        if ctx.is_journal() {
            let run = wait_run_status(&client, &run_id, "awaiting_resume", TIMEOUT).await;
            assert_eq!(run["run"]["status"], json!("awaiting_resume"));
        } else {
            let run = wait_run_status(&client, &run_id, "failed", TIMEOUT).await;
            assert_eq!(run["run"]["status"], json!("failed"));
        }
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(
            timeline_node(&timeline, "call")["attempts"],
            json!(2),
            "retry 裁决必须 attempt+1 重放"
        );
    })
);

e2e_test!(
    restart_preserves_terminal_run_without_duplicate_events,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "已终结", linear_def("return 1;")).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let before = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(before["run"]["status"], json!("succeeded"));

        let events_before: Value =
            call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events_before = events_before["events"].as_array().unwrap().clone();
        assert!(!events_before.is_empty());

        ctx.restart().await;
        let client = ctx.client().await;

        // 终态 run 重启后保持终态、事件一字不差（恢复不得产生第二个写者，§12.14）
        let after: Value = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        assert_eq!(after["run"]["status"], json!("succeeded"));
        assert_eq!(after["run"]["output"], json!(1));
        assert_eq!(after["live"], json!(false));

        let events_after: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        assert_eq!(
            events_after["events"].as_array().unwrap(),
            &events_before,
            "重启不得改写已终结 run 的事件日志"
        );
        let seqs: Vec<u64> = events_before
            .iter()
            .map(|e| e["seq"].as_u64().unwrap())
            .collect();
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "seq 严格连续，无重复"
        );
    })
);
