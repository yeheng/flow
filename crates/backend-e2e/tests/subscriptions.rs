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
    subscribe_global_stream_delivers_events_in_order,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "订阅", linear_def("return 'hi';")).await;

        // 先订阅（全局，无 run_id 过滤）再启动 run：实时增量路径。
        // 订阅建立是异步的（服务端 receiver 注册略晚于 RPC 返回），全局流是
        // 「纯实时增量」——注册前的事件不在流里，客户端按 from_seq 用 run.events
        // 补齐（§9 订阅契约）。这里等一小会让接收端就位，仍按「日志尾部的
        // 连续后缀」断言，不假设必然从 seq 1 开始。
        let mut sub = subscribe(&client, None).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;

        let streamed = collect_run_events(&mut sub, &run_id, TIMEOUT).await;
        assert!(
            streamed.len() >= 5,
            "至少收到 node_*/run_completed：{streamed:?}"
        );
        assert_eq!(streamed.last().unwrap()["type"], json!("run_completed"));

        // 与事件日志对齐：收到的必须是日志尾部的一段连续后缀（不丢中间、不重）
        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        let first_seq = streamed.first().unwrap()["seq"].as_u64().unwrap();
        assert!(
            first_seq >= 1 && first_seq <= events.len() as u64,
            "流必须接在日志上：首条 seq {first_seq}，日志 {} 条",
            events.len()
        );
        for (index, streamed_event) in streamed.iter().enumerate() {
            let logged = &events[first_seq as usize - 1 + index];
            assert_eq!(streamed_event["seq"], logged["seq"]);
            assert_eq!(streamed_event["type"], logged["type"]);
        }
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

        // 中途订阅：先回放完整历史，再接实时增量
        let mut sub = subscribe(&client, Some(run_id.clone())).await;
        let mut seqs: Vec<u64> = Vec::new();
        let deadline = tokio::time::Instant::now() + SHORT;
        loop {
            // 回放段：覆盖到订阅时刻之前的全部事件
            if seqs.len() as u64 >= replayed_until {
                break;
            }
            let msg = tokio::time::timeout_at(deadline, sub.next())
                .await
                .expect("回放停滞")
                .expect("回放提前结束");
            let envelope = msg.expect("订阅消息错误");
            assert_eq!(envelope["run_id"], json!(run_id));
            seqs.push(envelope["seq"].as_u64().unwrap());
        }
        assert_eq!(
            seqs,
            (1..=replayed_until).collect::<Vec<_>>(),
            "按 run_id 订阅必须从头回放且 seq 连续"
        );

        // 交付信号推进 run 到终态：增量段 + wire 侧「终态后无新事件」
        call::<Value>(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "h", "signal_id": "sig-sub", "payload": {"ok": 1}}),
        )
        .await;
        let streamed = collect_run_events(&mut sub, &run_id, SHORT).await;
        for envelope in &streamed {
            seqs.push(envelope["seq"].as_u64().unwrap());
        }
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "回放 + 增量无缝衔接：{seqs:?}"
        );
        assert_eq!(streamed.last().unwrap()["type"], json!("run_completed"));

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
        let mut sub = subscribe(&client, Some(format!("missing-{}", uuid::Uuid::now_v7()))).await;
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
        let mut sub = subscribe(&client, Some(run_id.clone())).await;
        let streamed = collect_run_events(&mut sub, &run_id, SHORT).await;
        assert!(!streamed.is_empty(), "真实 run 的订阅必须收到事件");
        wait_run_terminal(&client, &run_id, SHORT).await;
    })
});

e2e_test!(
    subscribe_global_receives_events_of_all_runs,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (first, _) = publish_workflow(&client, "流甲", linear_def("return 1;")).await;
        let (second, _) = publish_workflow(&client, "流乙", linear_def("return 2;")).await;

        let mut sub = subscribe(&client, None).await;
        // 等订阅处理器完成 broadcast 接收端注册（accept 与注册之间有一个异步
        // 间隙；pg 的 NOTIFY 唤醒会让这个间隙里的事件直接错过——全局流本就是
        // 纯实时增量，间隙里的事件按契约用 run.events 补齐）
        tokio::time::sleep(Duration::from_millis(300)).await;
        let run_a = start_run(&client, &first, json!({})).await;
        let run_b = start_run(&client, &second, json!({})).await;

        // 两个 run 并发跑：必须用同一个收集循环同时等两个终态——分开收会把
        // 先完成的那个 run 的事件（含终态）从流上消费掉，后收的那个永远等不到
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let mut per_run: std::collections::HashMap<String, Vec<Value>> =
            std::collections::HashMap::new();
        while !per_run.get(&run_a).is_some_and(|e| is_terminal(e.last()))
            || !per_run.get(&run_b).is_some_and(|e| is_terminal(e.last()))
        {
            let msg = tokio::time::timeout_at(deadline, sub.next())
                .await
                .expect("订阅在超时前没收齐两个 run 的终态")
                .expect("订阅流结束");
            let envelope = msg.expect("订阅消息错误");
            per_run
                .entry(envelope["run_id"].as_str().unwrap().to_string())
                .or_default()
                .push(envelope);
        }
        let events_a = &per_run[&run_a];
        let events_b = &per_run[&run_b];
        assert!(!events_a.is_empty() && !events_b.is_empty());
        assert_eq!(events_a.last().unwrap()["type"], json!("run_completed"));
        assert_eq!(events_b.last().unwrap()["type"], json!("run_completed"));
        // 每个 run 的流从 seq=1（run_started）起严格连续——两臂一致
        // （sqlite 的 run_started 由 Engine::start_run 显式广播，pg 经共享日志扇出）
        for events in [events_a, events_b] {
            let seqs: Vec<u64> = events.iter().map(|e| e["seq"].as_u64().unwrap()).collect();
            assert_eq!(
                seqs,
                (1..=seqs.len() as u64).collect::<Vec<_>>(),
                "每个 run 的流从 run_started 起连续：{seqs:?}"
            );
        }
        wait_run_terminal(&client, &run_a, SHORT).await;
        wait_run_terminal(&client, &run_b, SHORT).await;
    })
);

fn is_terminal(event: Option<&Value>) -> bool {
    matches!(
        event.and_then(|e| e["type"].as_str()),
        Some("run_completed") | Some("run_failed") | Some("run_cancelled")
    )
}

/// 轮询直到节点进入 running。
async fn wait_node_running(
    client: &backend_e2e::common::Client,
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
