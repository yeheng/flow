//! 冒烟：harness 本身可用（起进程、建流、发 RPC、清理）。
//!
//! 用法：`e2e_test!(名字, |ctx: &mut Ctx| async move { ... })`。

use backend_e2e::common::fixtures::linear_def;
use backend_e2e::common::{
    call_json, publish_workflow, start_run, wait_run_terminal, Ctx, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::json;

e2e_test!(harness_smoke, |ctx: &mut Ctx| Box::pin(async move {
    let client = ctx.client().await;
    let (workflow_id, version) =
        publish_workflow(&client, "冒烟", linear_def("return { hello: 'e2e' };")).await;
    assert_eq!(version, 1);

    let run_id = start_run(&client, &workflow_id, json!({ "seed": 1 })).await;
    let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
    assert_eq!(run["run"]["status"], json!("succeeded"));
    assert_eq!(run["run"]["output"], json!({ "hello": "e2e" }));
    assert_eq!(run["live"], json!(false));

    let timeline: serde_json::Value =
        call_json(&client, "run.timeline", json!({ "run_id": run_id })).await;
    assert_eq!(timeline["status"], json!("succeeded"));
    assert_eq!(timeline["nodes"][1]["state"], json!("completed"));
}));
