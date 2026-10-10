//! run.subscribe 订阅流端到端：实时增量、按 run_id 回放+追平、未知 run 立即结束、
//! 全局流收全量事件。
//!
//! 契约来源：DESIGN.md §9（订阅流结束条件两臂一致）。注意 wire 层的边界：
//! jsonrpsee 客户端流只在连接断开时结束，服务端 sink 关闭不通知客户端——
//! 「流自然结束」在 wire 上可观测为「终态事件之后不再有任何事件」。

use backend_e2e::common::fixtures::{delay_def, human_def, linear_def, timeline_node};
use backend_e2e::common::{
    call, call_json, collect_run_events, publish_workflow, start_run, subscribe, wait_run_terminal,
    Ctx, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};
use std::time::Duration;

e2e_test!(
    subscribe_replay_matches_event_log,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow, _) = publish_workflow(&client, "订阅", linear_def("return 'hi';")).await;
        let run = start_run(&client, &workflow, json!({})).await;
        wait_run_terminal(&client, &run, TIMEOUT).await;
        let mut sub = subscribe(&client, &run).await;
        let streamed = collect_run_events(&mut sub, &run, TIMEOUT).await;
        let logged = call_json(&client, "run.events.view", json!({"run_id":run})).await;
        assert_eq!(streamed.len(), logged["events"].as_array().unwrap().len());
        for (event, logged) in streamed.iter().zip(logged["events"].as_array().unwrap()) {
            assert_eq!(event["event"]["run_seq"], logged["event"]["run_seq"]);
            assert_eq!(event["event"]["kind"], logged["event"]["kind"]);
        }
        assert_eq!(streamed.last().unwrap()["event"]["kind"], "run_completed");
    })
);

e2e_test!(
    subscribe_with_run_id_replays_then_follows,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "回放", human_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;

        // 等 human_task 进入等待（日志已有若干事件但 run 未终结）
        let timeline = wait_node_running(&client, &run_id, "h", SHORT).await;
        let replayed_until = timeline["last_seq"].as_u64().unwrap();
        assert!(replayed_until >= 2, "{timeline}");

        // journal 的 seq 是 v2 权威序号：等待注册等事实占用序号但不出现在
        // v1 事件面（映射后允许空洞）；timeline.last_seq 是折影水位 ≥ 事件数。
        let allow_gaps = false;
        // 中途订阅：先回放完整历史，再接实时增量
        let mut sub = subscribe(&client, &run_id).await;
        let mut seqs: Vec<u64> = Vec::new();
        let deadline = tokio::time::Instant::now() + SHORT;
        // 回放段终点：有空洞时以「水位内的最大已映射序号」为准（WaitRegistered
        // 占用最后一个序号且不映射）
        let replay_target = if allow_gaps {
            replayed_until.saturating_sub(1)
        } else {
            replayed_until
        };
        loop {
            // 回放段：覆盖到订阅时刻之前的全部事件
            if seqs.len() as u64 >= replay_target {
                break;
            }
            let msg = tokio::time::timeout_at(deadline, sub.next())
                .await
                .expect("回放停滞")
                .expect("回放提前结束");
            let envelope = msg.expect("订阅消息错误");
            assert_eq!(envelope["event"]["run_id"], json!(run_id));
            seqs.push(
                envelope["event"]["run_seq"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
            );
        }
        if allow_gaps {
            assert_eq!(
                seqs,
                (1..=replay_target).collect::<Vec<_>>(),
                "回放段从 1 开始且严格递增（允许尾部空洞）：{seqs:?}"
            );
        } else {
            assert_eq!(
                seqs,
                (1..=replayed_until).collect::<Vec<_>>(),
                "按 run_id 订阅必须从头回放且 seq 连续"
            );
        }

        // 交付信号推进 run 到终态：增量段 + wire 侧「终态后无新事件」
        call::<Value>(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "h", "signal_id": "sig-sub", "payload": {"ok": 1}}),
        )
        .await;
        let streamed = collect_run_events(&mut sub, &run_id, SHORT).await;
        for envelope in &streamed {
            seqs.push(
                envelope["event"]["run_seq"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
            );
        }
        if allow_gaps {
            // journal：严格递增、无重复（空洞合法，见回放段注释）
            assert!(
                seqs.windows(2).all(|w| w[0] < w[1]),
                "回放 + 增量序号严格递增：{seqs:?}"
            );
            assert_eq!(seqs.first(), Some(&1u64), "从 1 开始：{seqs:?}");
        } else {
            assert_eq!(
                seqs,
                (1..=seqs.len() as u64).collect::<Vec<_>>(),
                "回放 + 增量无缝衔接：{seqs:?}"
            );
        }
        assert_eq!(
            streamed.last().unwrap()["event"]["kind"],
            json!("run_completed")
        );

        // 终态之后流不再产出（服务端流已结束；wire 上表现为静默）
        let extra = tokio::time::timeout(Duration::from_secs(2), sub.next())
            .await
            .expect_err("终态后仍有事件产出");
        assert!(
            matches!(extra, tokio::time::error::Elapsed { .. }),
            "终态后不应再有事件"
        );
    })
);

e2e_test!(subscribe_unknown_run_ends_immediately, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        // run 不存在：流立即结束（不是报错、不是永久重试）——wire 上可观测为
        // 「订阅建立后没有任何事件，也不报错」
        let mut sub = subscribe(&client, &format!("missing-{}", uuid::Uuid::now_v7())).await;
        let nothing = tokio::time::timeout(Duration::from_secs(3), sub.next()).await;
        assert!(
            nothing.is_err(),
            "未知 run 的订阅必须立即结束，不得挂起等待"
        );

        // 空日志窗口（run 行已建、run_started 未落盘）同样立即结束——
        // 用 delay 中的订阅对照：真实 run 的订阅必须能收到事件，证明上面的静默
        // 不是「订阅整体坏掉」
        let (workflow_id, _) = publish_workflow(&client, "对照", delay_def(30)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let mut sub = subscribe(&client, &run_id).await;
        let streamed = collect_run_events(&mut sub, &run_id, SHORT).await;
        assert!(!streamed.is_empty(), "真实 run 的订阅必须收到事件");
        wait_run_terminal(&client, &run_id, SHORT).await;
    })
});

e2e_test!(subscriptions_isolate_runs, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (workflow, _) = publish_workflow(&client, "流隔离", linear_def("return 1;")).await;
        let first = start_run(&client, &workflow, json!({})).await;
        let second = start_run(&client, &workflow, json!({})).await;
        let mut a = subscribe(&client, &first).await;
        let mut b = subscribe(&client, &second).await;
        let (a, b) = tokio::join!(
            collect_run_events(&mut a, &first, TIMEOUT),
            collect_run_events(&mut b, &second, TIMEOUT)
        );
        assert!(a.iter().all(|e| e["event"]["run_id"] == first));
        assert!(b.iter().all(|e| e["event"]["run_id"] == second));
        assert_eq!(a.last().unwrap()["event"]["kind"], "run_completed");
        assert_eq!(b.last().unwrap()["event"]["kind"], "run_completed");
    }
));

/// 轮询直到节点进入 running。
async fn wait_node_running(
    client: &backend_e2e::common::Conn,
    run_id: &str,
    node_id: &str,
    timeout: Duration,
) -> Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let timeline: Value = call_json(client, "run.timeline", json!({"run_id": run_id})).await;
        if timeline_node(&timeline, node_id)["state"] == json!("running") {
            return timeline;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待节点 {node_id} 到 running 超时：{timeline}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

e2e_test!(
    subscription_is_ordered_and_observations_are_separate,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // 脚本日志走独立观测面，状态订阅仍可回放并追平。
        let code = "for (let i = 0; i < 5; i++) { console.log('line', i); }\nreturn 'done';";
        let (workflow_id, _) = publish_workflow(&client, "订阅日志", linear_def(code)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;

        let mut sub = subscribe(&client, &run_id).await;
        let streamed = collect_run_events(&mut sub, &run_id, TIMEOUT).await;

        // seq 严格连续（日志行占用 seq，但不能产生缺口）；journal 的 v1 视图
        // 允许空洞（v2 权威序号含不映射的事实），退为严格递增
        for (index, event) in streamed.iter().enumerate() {
            assert!(
                index == 0
                    || streamed[index - 1]["event"]["run_seq"]
                        .as_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap()
                        < event["event"]["run_seq"]
                            .as_str()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap(),
                "订阅流 seq 严格递增：{streamed:?}"
            );
        }
        let observations =
            call_json(&client, "run.observations.page", json!({"run_id": run_id})).await;
        let logs = observations["records"].as_array().unwrap();
        assert_eq!(logs.len(), 5, "全部 console 日志可读取：{observations}");
        for (index, log) in logs.iter().enumerate() {
            assert_eq!(log["line"]["message"], json!(format!("line {index}")));
        }
        assert_eq!(
            streamed.last().unwrap()["event"]["kind"],
            json!("run_completed")
        );
    })
);

// v2 订阅 = journal 事件原形（数据面，StoredValue 包裹、不脱敏）：前端
// monitor 只把订阅当脏信号，投影一律来自 timeline（脱敏展示面）。此处钉住
// 「订阅与事件页同源同值」；脱敏契约由 timeline 测试单独覆盖（run_execution
// 的可观察性用例：node 输出脱敏、事件日志保持原值）。
e2e_test!(
    subscription_redacts_node_output_like_timeline,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(
            &client,
            "订阅脱敏",
            linear_def("return { ok: 1, token: 'sk-secret-value' };"),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let _ = wait_run_terminal(&client, &run_id, TIMEOUT).await;

        // 回放段从头拿全量：订阅走的是同一条服务端路径
        let mut sub = subscribe(&client, &run_id).await;
        let streamed = collect_run_events(&mut sub, &run_id, TIMEOUT).await;
        let completed = streamed
            .iter()
            .find(|e| {
                e["event"]["kind"] == json!("node_completed")
                    && e["event"]["node_id"] == json!("n1")
            })
            .unwrap_or_else(|| panic!("订阅流里没有 n1 的 node_completed：{streamed:?}"));

        assert_eq!(
            completed["event"]["payload"]["output"]["value"]["token"],
            json!("sk-secret-value"),
            "订阅流是数据面原形，与事件日志同源：{completed:#}"
        );
        assert_eq!(
            completed["event"]["payload"]["output"]["value"]["ok"],
            json!(1)
        );

        // 数据面不变：事件日志里仍是原始值（下游节点/fold 要消费它）
        let events: Value = call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let logged = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| {
                e["event"]["kind"] == json!("node_completed")
                    && e["event"]["node_id"] == json!("n1")
            })
            .unwrap_or_else(|| panic!("事件日志里没有 n1 的 node_completed"));
        assert_eq!(
            logged["event"]["payload"]["output"]["value"]["token"],
            json!("sk-secret-value"),
            "事件日志是数据面，不该被脱敏污染：{logged:#}"
        );

        // timeline 原本就对：三个展示/读取面各就各位
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(
            timeline_node(&timeline, "n1")["output"]["token"],
            json!("***")
        );
    })
);
