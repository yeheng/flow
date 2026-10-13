//! 输入节点：run 输入已含声明字段（或默认值兜底）时直接放行；
//! 否则挂起等用户填写（run.signal 交付），交付对象成为节点输出。

use backend_e2e::common::fixtures::{input_def, timeline_node};
use backend_e2e::common::{
    call, call_json, publish_workflow, start_run, wait_run_terminal, Conn, Ctx, SHORT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

e2e_test!(
    input_node_passes_through_when_fields_provided,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let fields = json!([{"key": "token", "label": "令牌"}, {"key": "n"}]);
        let (workflow_id, _) = publish_workflow(&client, "输入-直通", input_def(fields)).await;
        let run_id = start_run(&client, &workflow_id, json!({"token": "abc", "n": 5})).await;

        // 字段已齐：不挂起，节点直接成功，输出 = 声明字段的值（额外输入字段不透出）
        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(run["run"]["output"], json!({"token": "abc", "n": 5}));
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let node = timeline_node(&timeline, "i");
        assert_eq!(node["state"], json!("completed"));
        assert!(node["wait"].is_null(), "字段已齐不该登记等待：{node}");
    })
);

e2e_test!(input_node_default_covers_missing_field, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let fields = json!([{"key": "token"}, {"key": "n", "default": 1}]);
        let (workflow_id, _) = publish_workflow(&client, "输入-默认值", input_def(fields)).await;
        let run_id = start_run(&client, &workflow_id, json!({"token": "x"})).await;

        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(run["run"]["output"], json!({"token": "x", "n": 1}));
    })
});

e2e_test!(
    input_node_waits_for_missing_fields_then_delivery_completes,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let fields = json!([{"key": "token"}, {"key": "n"}]);
        let (workflow_id, _) = publish_workflow(&client, "输入-等待", input_def(fields)).await;
        // null 视为未提供；n 压根没给
        let run_id = start_run(&client, &workflow_id, json!({"token": null})).await;

        let timeline = wait_node_waiting(&client, &run_id, "i").await;
        let node = timeline_node(&timeline, "i");
        assert_eq!(node["wait"]["kind"], json!("signal"), "{node}");

        // 用户提交表单：交付对象即节点输出
        let form = json!({"token": "t-1", "n": 2});
        let ack: Value = call(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "i", "payload": form}),
        )
        .await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");

        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(
            run["run"]["output"], form,
            "input 节点输出 = 交付的表单对象"
        );
    })
);

/// 等节点等待登记（wait 结构非空）：signal 的前置是 wait 已提交。
async fn wait_node_waiting(client: &Conn, run_id: &str, node_id: &str) -> Value {
    let deadline = std::time::Instant::now() + SHORT;
    loop {
        let timeline: Value = call_json(client, "run.timeline", json!({"run_id": run_id})).await;
        let node = timeline_node(&timeline, node_id);
        if node["state"] == json!("running") && !node["wait"].is_null() {
            return timeline;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "节点 {node_id} 未登记等待：{timeline}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
