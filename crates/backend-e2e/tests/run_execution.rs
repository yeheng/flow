//! run 执行引擎的端到端语义：script / condition / delay / 输出收集 / 跳过传播 /
//! 事件日志 / run.list 查询。
//!
//! 契约来源：DESIGN.md §3（事件日志）、§4（折叠器）、§6.2-6.4（汇合、输出、真值）。

use backend_e2e::common::fixtures::{
    condition_def, delay_def, failing_def, join_skip_def, linear_def, multi_end_def,
    multi_pred_def, skip_chain_def, timeline_node,
};
use backend_e2e::common::{
    call, call_err, call_json, publish_workflow, start_run, wait_run_terminal, Ctx, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

/// 轮询 run.timeline 直到某节点进入预期状态（用于「运行中等待」这类非终态观测）。
async fn wait_node_state(
    client: &backend_e2e::common::Client,
    run_id: &str,
    node_id: &str,
    state: &str,
    timeout: std::time::Duration,
) -> Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let timeline: Value = call_json(client, "run.timeline", json!({"run_id": run_id})).await;
        if timeline_node(&timeline, node_id)["state"] == json!(state) {
            return timeline;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待节点 {node_id} 到 {state} 超时：{timeline}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

e2e_test!(
    linear_run_succeeds_with_output_passthrough,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, version) = publish_workflow(
            &client,
            "线性",
            linear_def("return { doubled: input.n * 2 };"),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({ "n": 21 })).await;

        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(run["run"]["output"], json!({ "doubled": 42 }));
        assert_eq!(run["run"]["workflow_version"], json!(version));
        assert_eq!(run["run"]["input"], json!({ "n": 21 }));
        assert!(run["run"]["started_at"].is_string());
        assert!(run["run"]["ended_at"].is_string());
        assert!(run["run"]["error"].is_null());
        assert_eq!(run["live"], json!(false));

        // 单 end 单前驱：输出透传（§6.3）
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline["output"], json!({ "doubled": 42 }));
        assert_eq!(timeline["phase"], json!("succeeded"));
    })
);

e2e_test!(
    timeline_reports_nodes_in_definition_order,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) =
            publish_workflow(&client, "时间线", linear_def("return 'ok';")).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_run_terminal(&client, &run_id, TIMEOUT).await;

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let ids: Vec<&str> = timeline["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["start", "n1", "end"], "时间线按定义顺序：{ids:?}");

        for node in timeline["nodes"].as_array().unwrap() {
            assert_eq!(node["state"], json!("completed"));
            assert_eq!(node["attempts"], json!(1));
            assert!(node["started_at"].is_string());
            assert!(node["ended_at"].is_string());
            assert!(node["duration_ms"].is_i64());
            assert!(node.get("child_run_id").is_some(), "字段面齐全");
        }
        assert_eq!(timeline_node(&timeline, "n1")["output"], json!("ok"));
        assert_eq!(timeline["workflow_id"], json!(workflow_id));

        // last_seq 与事件日志条数一致
        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        assert_eq!(timeline["last_seq"].as_u64().unwrap(), events.len() as u64);
    })
);

e2e_test!(multi_end_output_is_mapped_by_node_id, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "多结束", multi_end_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_run_terminal(&client, &run_id, TIMEOUT).await;

        let run = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        // 多 end：{node_id: output} 映射（§6.3）
        assert_eq!(
            run["run"]["output"],
            json!({ "end_a": 1, "end_b": 2 }),
            "{run}"
        );

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "end_a")["output"], json!(1));
        assert_eq!(timeline_node(&timeline, "end_b")["output"], json!(2));
    })
});

e2e_test!(
    multi_pred_end_output_is_mapped_by_predecessor,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "汇合", multi_pred_def()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_run_terminal(&client, &run_id, TIMEOUT).await;

        let run = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        // end 有两个前驱：输出为 {pred_id: output}
        assert_eq!(run["run"]["output"], json!({ "s1": "a", "s2": "b" }));

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(
            timeline_node(&timeline, "end")["output"],
            json!({ "s1": "a", "s2": "b" })
        );
    })
);

e2e_test!(
    condition_selects_branch_and_skips_the_other,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let definition = condition_def("input.ok === true", "return 'T';", "return 'F';");
        let (workflow_id, _) = publish_workflow(&client, "分支", definition).await;

        // 真值分支
        let run_id = start_run(&client, &workflow_id, json!({ "ok": true })).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["output"], json!({ "end_t": "T", "end_f": null }));

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "cond")["output"], json!(true));
        assert_eq!(timeline_node(&timeline, "t")["state"], json!("completed"));
        assert_eq!(
            timeline_node(&timeline, "end_t")["state"],
            json!("completed")
        );
        assert_eq!(
            timeline_node(&timeline, "f")["state"],
            json!("skipped"),
            "未选分支整节点跳过"
        );
        assert_eq!(
            timeline_node(&timeline, "f")["reason"],
            json!("branch_not_taken")
        );
        assert_eq!(
            timeline_node(&timeline, "end_f")["reason"],
            json!("upstream_skipped"),
            "跳过沿下游传播"
        );

        // 假值分支
        let run_id = start_run(&client, &workflow_id, json!({ "ok": false })).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["output"], json!({ "end_t": null, "end_f": "F" }));
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "cond")["output"], json!(false));
        assert_eq!(
            timeline_node(&timeline, "t")["reason"],
            json!("branch_not_taken")
        );
        assert_eq!(timeline_node(&timeline, "f")["state"], json!("completed"));

        // 真值判定与 JS Boolean() 一致：空数组/空对象/"false"/"0" 都为真（§6.4）。
        // 直接用 input 本身当真值来源的独立工作流（上面的 expr 是布尔比较，表达不了这张表）
        let (truthy_wf, _) = publish_workflow(
            &client,
            "真值表",
            condition_def("input", "return 'T';", "return 'F';"),
        )
        .await;
        for truthy_input in [json!([]), json!({}), json!("false"), json!("0"), json!(1)] {
            let run_id = start_run(&client, &truthy_wf, truthy_input.clone()).await;
            wait_run_terminal(&client, &run_id, SHORT).await;
            let timeline: Value =
                call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
            assert_eq!(
                timeline_node(&timeline, "t")["state"],
                json!("completed"),
                "input {truthy_input} 必须走真分支"
            );
        }
        for falsy_input in [json!(null), json!(""), json!(0), json!(false)] {
            let run_id = start_run(&client, &truthy_wf, falsy_input.clone()).await;
            wait_run_terminal(&client, &run_id, SHORT).await;
            let timeline: Value =
                call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
            assert_eq!(
                timeline_node(&timeline, "f")["state"],
                json!("completed"),
                "input {falsy_input} 必须走假分支"
            );
        }
    })
);

e2e_test!(
    skip_propagates_through_multiple_downstream_levels,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // false 分支是 delay → human_task → end_f 的三层下游：跳过必须推进到不动点，
        // 否则 human_task 没被跳过会让 Driver 误判「调度停滞」或挂等信号
        let (workflow_id, _) = publish_workflow(&client, "深层跳过", skip_chain_def("true")).await;
        let run_id = start_run(&client, &workflow_id, json!({ "seed": 1 })).await;
        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        for node in ["d", "h", "end_f"] {
            assert_eq!(
                timeline_node(&timeline, node)["state"],
                json!("skipped"),
                "{node} 必须被跳过"
            );
        }
        assert_eq!(
            timeline_node(&timeline, "h")["reason"],
            json!("upstream_skipped")
        );
        assert_eq!(
            timeline_node(&timeline, "end_t")["state"],
            json!("completed")
        );
        assert_eq!(
            timeline_node(&timeline, "end_f")["attempts"],
            json!(0),
            "跳过无执行"
        );

        // 取另一分支：三层下游要走通——delay 执行、human_task 挂起等信号。
        // 注意：活着的 human_task 等待期 runs.status 仍是 running（awaiting_resume
        // 只属于崩溃恢复里的人工裁决，不是运行中等待外部输入的状态）
        let (workflow_id, _) = publish_workflow(&client, "深层执行", skip_chain_def("false")).await;
        let run_id = start_run(&client, &workflow_id, json!({ "seed": 2 })).await;
        let timeline = wait_node_state(&client, &run_id, "h", "running", SHORT).await;
        let waiting = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        assert_eq!(waiting["run"]["status"], json!("running"), "{waiting}");
        assert_eq!(waiting["live"], json!(true));
        assert_eq!(timeline_node(&timeline, "d")["state"], json!("completed"));
        assert_eq!(timeline_node(&timeline, "h")["state"], json!("running"));
        assert_eq!(timeline_node(&timeline, "end_t")["state"], json!("skipped"));
    })
);

e2e_test!(and_join_skips_when_one_branch_skipped, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "与汇合", join_skip_def("true")).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        // 任一入边 Unsatisfied → 整节点 Skipped（§6.2）：互斥分支不能 AND-join 合流
        assert_eq!(
            timeline_node(&timeline, "join_end")["state"],
            json!("skipped")
        );
        assert_eq!(
            timeline_node(&timeline, "join_end")["reason"],
            json!("upstream_skipped")
        );
        // 单 end 被跳过：输出为 null（与真输出 null 不可区分，§6.3 已知限制）
        assert_eq!(run["run"]["output"], json!(null));
    })
});

e2e_test!(delay_node_waits_full_duration, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "等待", delay_def(400)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        wait_run_terminal(&client, &run_id, SHORT).await;

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let duration = timeline_node(&timeline, "d")["duration_ms"]
            .as_i64()
            .unwrap();
        assert!(duration >= 350, "delay 至少等满时长，实测 {duration}ms");
        assert_eq!(
            timeline_node(&timeline, "d")["output"],
            json!({ "slept_ms": 400 })
        );
        let run = call_json(&client, "run.get", json!({"run_id": run_id})).await;
        assert_eq!(run["run"]["output"], json!({ "slept_ms": 400 }));
    }
));

e2e_test!(
    script_reads_input_and_predecessor_outputs,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let definition = json!({
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "n1", "type": "script", "params": {"code": "return { fromInput: input.seed };"}},
                {"id": "n2", "type": "script", "params": {
                    "code": "return { chained: nodes.n1.fromInput + 1, again: input.seed + 10 };"
                }},
                {"id": "end", "type": "end"}
            ],
            "edges": [
                {"from": "start", "to": "n1"},
                {"from": "n1", "to": "n2"},
                {"from": "n2", "to": "end"}
            ]
        });
        let (workflow_id, _) = publish_workflow(&client, "输入面", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({ "seed": 5 })).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(
            run["run"]["output"],
            json!({ "chained": 6, "again": 15 }),
            "nodes 只暴露直接前驱输出（§10 决定论输入面）"
        );
    })
);

e2e_test!(
    script_error_fails_run_and_skips_downstream,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(
            &client,
            "失败",
            failing_def("throw new Error('boom');", None),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"));
        assert!(
            run["run"]["error"].as_str().unwrap().contains("boom"),
            "{run}"
        );

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline["phase"], json!("failed"));
        assert!(timeline["fatal_error"].as_str().unwrap().contains("boom"));
        assert_eq!(timeline_node(&timeline, "boom")["state"], json!("failed"));
        assert_eq!(
            timeline_node(&timeline, "boom")["attempts"],
            json!(1),
            "JS 抛错是致命失败，不重试（§6.5）"
        );
        assert_eq!(
            timeline_node(&timeline, "end")["state"],
            json!("skipped"),
            "失败节点的下游经 upstream_failed 跳过"
        );
        assert_eq!(
            timeline_node(&timeline, "end")["reason"],
            json!("upstream_failed")
        );

        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let failed = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == json!("node_failed"))
            .unwrap_or_else(|| panic!("缺少 node_failed：{events}"));
        assert_eq!(failed["retryable"], json!(false));
        let last = events["events"].as_array().unwrap().last().unwrap();
        assert_eq!(last["type"], json!("run_failed"));
    })
);

e2e_test!(
    script_error_is_fatal_even_with_retry_policy,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(
            &client,
            "重试策略",
            failing_def(
                "throw new Error('nope');",
                Some(json!({"max_attempts": 3, "backoff_ms": 50})),
            ),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("failed"));

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(
            timeline_node(&timeline, "boom")["attempts"],
            json!(1),
            "NodeFailure.retryable=false 时引擎不重试"
        );
    })
);

e2e_test!(
    js_sandbox_has_no_std_os_or_module_loader,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let code = r#"return {
        std: typeof std, os: typeof os, quickjs: typeof quickjs,
        require: typeof require, process: typeof process,
        fetch: typeof fetch, XMLHttpRequest: typeof XMLHttpRequest
    };"#;
        let (workflow_id, _) = publish_workflow(&client, "沙箱", linear_def(code)).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        assert_eq!(
            run["run"]["output"],
            json!({
                "std": "undefined", "os": "undefined", "quickjs": "undefined",
                "require": "undefined", "process": "undefined",
                "fetch": "undefined", "XMLHttpRequest": "undefined"
            }),
            "沙箱里没有 std/os/模块加载器/网络全局（§10）"
        );
    })
);

e2e_test!(big_int_boundary_contract, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        // 大整数边界（§10）：input 里的大整数经 BigInt 进出沙箱，透传精确无损；
        // 脚本里的 Number 字面量仍是 ECMAScript 语义（9007199254740993 字面量
        // 本身就是 2^53，与沙箱边界无关），需要精确请写 n 后缀或从 input 传入。
        let (workflow_id, _) = publish_workflow(
            &client,
            "大整数",
            linear_def("return { passthrough: input.id, literal: 9007199254740993 };"),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({"id": 9007199254740993i64})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(
            run["run"]["output"],
            json!({
                "passthrough": 9007199254740993i64,
                "literal": 9007199254740992i64
            })
        );
    }
));

e2e_test!(run_list_filters_and_cursor_pagination, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "列表", linear_def("return 1;")).await;
        let (other, _) = publish_workflow(&client, "别的工作流", linear_def("return 1;")).await;

        let mut runs = Vec::new();
        for _ in 0..3 {
            let run_id = start_run(&client, &workflow_id, json!({})).await;
            wait_run_terminal(&client, &run_id, SHORT).await;
            runs.push(run_id);
        }
        let other_run = start_run(&client, &other, json!({})).await;
        wait_run_terminal(&client, &other_run, SHORT).await;

        // 全量：新的在前
        let all: Value = call_json(&client, "run.list", json!({})).await;
        let all = all["runs"].as_array().unwrap();
        assert_eq!(all.len(), 4, "{all:?}");
        assert_eq!(
            all[0]["id"],
            json!(other_run),
            "run.list 按 started_at 倒序"
        );

        // workflow 过滤
        let filtered: Value = call_json(
            &client,
            "run.list",
            json!({"workflow_id": workflow_id, "status": "succeeded"}),
        )
        .await;
        let filtered = filtered["runs"].as_array().unwrap();
        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered[0]["id"], json!(runs[2]), "最新创建的在最前");

        // 另一个 workflow 的 run 不过滤出来
        let none: Value = call_json(
            &client,
            "run.list",
            json!({"workflow_id": workflow_id, "status": "failed"}),
        )
        .await;
        assert_eq!(none["runs"].as_array().unwrap().len(), 0);

        // limit + before_run_id 游标翻页：每页更旧
        let mut cursor = None;
        let mut pages = Vec::new();
        for _ in 0..4 {
            let mut params = json!({"workflow_id": workflow_id, "status": "succeeded", "limit": 1});
            if let Some(cursor) = &cursor {
                params["before_run_id"] = json!(cursor);
            }
            let page: Value = call_json(&client, "run.list", params).await;
            let page = page["runs"].as_array().unwrap().clone();
            if page.is_empty() {
                break;
            }
            cursor = Some(page[0]["id"].as_str().unwrap().to_string());
            pages.push(page[0]["id"].as_str().unwrap().to_string());
        }
        assert_eq!(pages.len(), 3, "必须恰好翻完 3 条：{pages:?}");
        assert_eq!(
            pages,
            vec![runs[2].clone(), runs[1].clone(), runs[0].clone()]
        );

        // limit 夹取：0 → 1，999 → 500
        let clamped: Value = call_json(&client, "run.list", json!({"limit": 0})).await;
        assert_eq!(clamped["runs"].as_array().unwrap().len(), 1);
    })
});

e2e_test!(run_list_rejects_invalid_status_filter, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let err = call_err(&client, "run.list", json!({"status": "bogus"})).await;
        assert_eq!(err.code(), -32010, "status 过滤词必须在状态词汇表内：{err}");
        // 词汇表内的值不被拒
        for status in [
            "initializing",
            "running",
            "awaiting_resume",
            "succeeded",
            "failed",
            "cancelled",
        ] {
            let _: Value = call(&client, "run.list", json!({"status": status})).await;
        }
    })
});

e2e_test!(
    run_events_seq_continuous_and_from_seq_incremental,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "事件", linear_def("return 'x';")).await;
        let run_id = start_run(&client, &workflow_id, json!({ "tag": "t" })).await;
        wait_run_terminal(&client, &run_id, TIMEOUT).await;

        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        assert!(events.len() >= 5, "至少 5 条事件：{events:?}");
        for (index, envelope) in events.iter().enumerate() {
            assert_eq!(
                envelope["seq"],
                json!(index as u64 + 1),
                "seq 从 1 严格连续"
            );
            assert_eq!(envelope["run_id"], json!(run_id));
            assert!(envelope["ts"].is_string());
        }
        assert_eq!(events[0]["type"], json!("run_started"));
        assert_eq!(events[0]["workflow_version"], json!(1));
        assert_eq!(events[0]["input"], json!({ "tag": "t" }));
        assert_eq!(events[0]["depth"], json!(0));
        assert_eq!(events.last().unwrap()["type"], json!("run_completed"));

        // from_seq 增量拉取
        let tail: Value = call_json(
            &client,
            "run.events",
            json!({"run_id": run_id, "from_seq": 2}),
        )
        .await;
        let tail = tail["events"].as_array().unwrap();
        assert_eq!(
            tail.len(),
            events.len() - 1,
            "from_seq 是闭区间（seq >= from_seq）"
        );
        assert_eq!(tail[0]["seq"], json!(2));

        // 越过末尾：空列表
        let beyond: Value = call_json(
            &client,
            "run.events",
            json!({"run_id": run_id, "from_seq": 9999}),
        )
        .await;
        assert_eq!(beyond["events"].as_array().unwrap().len(), 0);

        // run 不存在：-32011
        let err = call_err(&client, "run.events", json!({"run_id": "nope"})).await;
        assert_eq!(err.code(), -32011, "{err}");
    })
);

e2e_test!(
    run_get_unknown_is_not_found_and_timeline_requires_run,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let err = call_err(&client, "run.get", json!({"run_id": "nope"})).await;
        assert_eq!(err.code(), -32011, "{err}");
        let err = call_err(&client, "run.timeline", json!({"run_id": "nope"})).await;
        assert_eq!(err.code(), -32011, "{err}");

        // 未知方法：JSON-RPC 标准 -32601
        let err = call_err(&client, "no.such.method", json!({})).await;
        assert_eq!(err.code(), -32601, "{err}");
    })
);

e2e_test!(
    node_logs_and_input_snapshot_visible_in_observability,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // 脚本 console 输出 + 模板参数（含敏感键）→ 日志与输入面快照全部可查
        let code = "console.log('hi from script', input);\nconsole.error('oops');\nreturn { ok: input.n, token: 'sk-run-level' };";
        let mut def = linear_def(code);
        def["nodes"][1]["params"]["note"] = json!("n=${input.n}");
        def["nodes"][1]["params"]["token"] = json!("sk-should-be-redacted");
        let (workflow_id, _) = publish_workflow(&client, "可观察", def).await;
        let run_id = start_run(&client, &workflow_id, json!({ "n": 7 })).await;

        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));

        // 时间线：输入面快照（展开 + 脱敏）、节点输出展示脱敏
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let node = timeline_node(&timeline, "n1");
        assert_eq!(node["input"]["note"], json!("n=7"), "模板展开后的输入面");
        assert_eq!(node["input"]["token"], json!("***"), "敏感键展示值脱敏");
        assert_eq!(node["output"]["token"], json!("***"), "节点输出展示值脱敏");
        // run 级 output 是数据面：与 run.get / run_completed 逐字一致
        // （同名不同值 = timeline 在骗客户端）
        assert_eq!(timeline["output"]["token"], json!("sk-run-level"));
        assert_eq!(
            run["run"]["output"]["token"],
            json!("sk-run-level"),
            "run.get 与 run.timeline 的 output 必须同值"
        );

        // 事件流里有 node_log：console.log→stdout/info，console.error→stderr/error
        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        let stdout_log = events.iter().find(|e| {
            e["type"] == json!("node_log")
                && e["stream"] == json!("stdout")
                && e["message"]
                    .as_str()
                    .unwrap_or("")
                    .contains("hi from script")
        });
        assert!(stdout_log.is_some(), "console.log 必须进事件流：{events:?}");
        let stderr_log = events.iter().find(|e| {
            e["type"] == json!("node_log")
                && e["stream"] == json!("stderr")
                && e["level"] == json!("error")
        });
        assert!(
            stderr_log.is_some(),
            "console.error 必须以 error 级进事件流"
        );
        let log = stdout_log.unwrap();
        assert_eq!(log["node_id"], json!("n1"));
        assert_eq!(log["attempt"], json!(1));
        // seq 连续性覆盖日志行：全量事件 seq 严格 1..N
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event["seq"], json!(index as u64 + 1));
        }
    })
);
