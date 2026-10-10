//! sub_workflow 端到端语义：子 run 输出透传、child_run_id、失败传导、
//! 无发布版本、取消级联、嵌套深度上限。
//!
//! 契约来源：DESIGN.md §6.8（子工作流）。

use backend_e2e::common::fixtures::{
    deep_chain_defs, human_def, linear_def, set_sub_wf, sub_def, timeline_node,
};
use backend_e2e::common::{
    call, call_json, publish_workflow, start_run, try_call_json, wait_run_status,
    wait_run_terminal, Conn, Ctx, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

e2e_test!(
    child_output_passes_through_with_queryable_id,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (child_wf, child_version) =
            publish_workflow(&client, "子流程", linear_def("return { got: input };")).await;
        let (parent_wf, _) = publish_workflow(&client, "父流程", sub_def(&child_wf)).await;

        let run_id = start_run(&client, &parent_wf, json!({ "amount": 7 })).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        // 子 run 输出透传为父节点输出，再经 end 透传为 run 输出
        assert_eq!(run["run"]["output"], json!({ "got": { "amount": 7 } }));

        // child_run_id：v2 用 uuid v7 + parent 关系（派生式已废弃）——契约是
        // 「可查询、钉版本」
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let child_run_id = timeline_node(&timeline, "sub")["child_run_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!child_run_id.is_empty() && child_run_id != run_id);

        // 子 run 是独立日志：可查询、钉死子工作流的已发布版本、深度 1
        let child = call_json(&client, "run.get.full", json!({"run_id": child_run_id})).await;
        assert_eq!(child["run"]["status"], json!("succeeded"));
        assert_eq!(child["run"]["workflow_id"], json!(child_wf));
        assert_eq!(child["run"]["workflow_version"], json!(child_version));
        assert_eq!(child["run"]["output"], json!({ "got": { "amount": 7 } }));
        let child_events: Value =
            call_json(&client, "run.events.full", json!({"run_id": child_run_id})).await;
        assert_eq!(child_events["events"][0]["depth"], json!(1));
    })
);

e2e_test!(child_failure_is_fatal_to_parent, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (child_wf, _) = publish_workflow(
            &client,
            "会炸的子流程",
            linear_def("throw new Error('child boom');"),
        )
        .await;
        let (parent_wf, _) = publish_workflow(&client, "父流程", sub_def(&child_wf)).await;

        let run_id = start_run(&client, &parent_wf, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        assert!(
            run["run"]["error"].as_str().unwrap().contains("child boom"),
            "子 run 失败必须传导为父节点 fatal：{run}"
        );

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "sub")["state"], json!("failed"));
        assert_eq!(
            timeline_node(&timeline, "end")["state"],
            json!("skipped"),
            "父节点 fatal 后下游跳过"
        );
        assert_eq!(
            timeline_node(&timeline, "end")["reason"],
            json!("upstream_failed")
        );

        // 子 run 自身也是 failed
        let child_run_id = timeline_node(&timeline, "sub")["child_run_id"]
            .as_str()
            .unwrap()
            .to_string();
        let child = call_json(&client, "run.get.full", json!({"run_id": child_run_id})).await;
        assert_eq!(child["run"]["status"], json!("failed"));
    }
));

e2e_test!(
    child_without_published_version_fails_node,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // 子工作流只有 draft：启动子 run 解析不到 published 版本
        let created: Value = call(&client, "workflow.create", json!({"name": "草稿子流程"})).await;
        let child_wf = created["workflow_id"].as_str().unwrap().to_string();
        call::<Value>(
            &client,
            "workflow.update",
            json!({"workflow_id": child_wf, "definition": linear_def("return 1;")}),
        )
        .await;
        let (parent_wf, _) = publish_workflow(&client, "父流程", sub_def(&child_wf)).await;

        let run_id = start_run(&client, &parent_wf, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        // v2 的错误词汇（同一语义：子流程无 published 版本不可执行）
        assert!(
            run["run"]["error"].as_str().unwrap().contains("published"),
            "错误必须指明原因：{run}"
        );
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "sub")["state"], json!("failed"));
    })
);

e2e_test!(parent_cancel_cascades_to_child, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        // 子流程里放 human_task：子 run 永远等待，父节点一直 Running
        let (child_wf, _) = publish_workflow(&client, "等待中的子流程", human_def()).await;
        let (parent_wf, _) = publish_workflow(&client, "父流程", sub_def(&child_wf)).await;

        let run_id = start_run(&client, &parent_wf, json!({})).await;
        let timeline = wait_node_running(&client, &run_id, "sub", SHORT).await;
        let child_run_id = timeline_node(&timeline, "sub")["child_run_id"]
            .as_str()
            .unwrap()
            .to_string();
        // 子 run 已创建并进入运行（human_task 等待中）。父节点 node_started 时
        // child_run_id 已确定，但子 run 行此刻可能还没落库（initializing 窗口）——
        // 轮询时把「不存在」（-32011）当未就绪，直到状态离开 initializing
        let deadline = std::time::Instant::now() + SHORT;
        loop {
            if let Ok(child) =
                try_call_json(&client, "run.get.full", json!({"run_id": child_run_id})).await
            {
                let status = child["run"]["status"].as_str().unwrap_or_default();
                if status == "running" || status == "awaiting_resume" {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "子 run {child_run_id} 未进入运行"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // 取消父 run：先级联取消子 run（best-effort），再写 RunCancelled（§6.8）
        let ack: Value = call_json(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");
        let parent = wait_run_status(&client, &run_id, "cancelled", TIMEOUT).await;
        assert_eq!(parent["run"]["status"], json!("cancelled"));

        // 级联是 best-effort：给子 run 一点时间到达终态
        let deadline = std::time::Instant::now() + SHORT;
        loop {
            let child = call_json(&client, "run.get.full", json!({"run_id": child_run_id})).await;
            if child["run"]["status"].as_str() == Some("cancelled") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "子 run 未被级联取消：{child}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
));

/// 嵌套深度上限：10 层链（根 depth=0 … 最内层 run depth=9），第 8 层的
/// sub_workflow 节点触发上限直接 fatal（§6.8 MAX_SUB_WORKFLOW_DEPTH=8）。
#[tokio::test]
async fn nested_depth_limit_is_fatal() {
    const DEPTH: usize = 10;
    backend_e2e::common::run_case(env!("CARGO_BIN_EXE_flow-journal-server-e2e"), |ctx| {
        Box::pin(depth_chain_body(ctx, DEPTH))
    })
    .await;
}

async fn depth_chain_body(ctx: &Ctx, depth: usize) {
    let client = ctx.client().await;
    let mut defs = deep_chain_defs(depth);
    let mut ids = Vec::new();
    for def in defs.iter_mut() {
        let created: Value = call(&client, "workflow.create", json!({"name": "链"})).await;
        let id = created["workflow_id"].as_str().unwrap().to_string();
        call::<Value>(
            &client,
            "workflow.update",
            json!({"workflow_id": id, "definition": def}),
        )
        .await;
        call::<Value>(
            &client,
            "workflow.publish",
            json!({"workflow_id": id, "version": 1}),
        )
        .await;
        ids.push(id);
    }
    // 回填每层的子工作流指向（占位符换成真实 id 后再发一版）
    for (level, def) in defs.iter_mut().enumerate() {
        if level + 1 < depth {
            set_sub_wf(def, &ids[level + 1]);
            call::<Value>(
                &client,
                "workflow.update",
                json!({"workflow_id": ids[level], "definition": def}),
            )
            .await;
            call::<Value>(
                &client,
                "workflow.publish",
                json!({"workflow_id": ids[level], "version": 2}),
            )
            .await;
        }
    }

    let run_id = start_run(&client, &ids[0], json!({})).await;
    let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
    assert_eq!(run["run"]["status"], json!("failed"), "{run}");
    // v2 的错误词汇（同一语义：深度上限 fatal）
    assert!(
        run["run"]["error"]
            .as_str()
            .unwrap()
            .contains("depth exceeded"),
        "depth>=8 必须直接 fatal：{run}"
    );
}

/// 轮询直到节点进入 running。
async fn wait_node_running(
    client: &Conn,
    run_id: &str,
    node_id: &str,
    timeout: std::time::Duration,
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
