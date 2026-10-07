//! 协议与错误码契约：-32010/-32011/-32012/-32601 的映射、params 省略归一、
//! 参数类型校验、limit 夹取。契约来源：DESIGN.md §9（RPC 层）。

use backend_e2e::common::fixtures::linear_def;
use backend_e2e::common::{
    call, call_err, call_json, call_null_params, publish_workflow, start_run, Ctx, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

e2e_test!(
    error_codes_follow_the_documented_mapping,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;

        // -32010 参数非法：缺必填字段 / 类型不对 / 词汇表外取值
        for (method, params) in [
            ("workflow.create", json!({})),
            ("workflow.create", json!({"name": 42})),
            ("workflow.update", json!({"workflow_id": "x"})),
            ("workflow.get", json!({})),
            (
                "workflow.get",
                json!({"workflow_id": "x", "version": "one"}),
            ),
            ("run.start", json!({})),
            ("run.get", json!({})),
            ("run.signal", json!({"run_id": "r", "payload": {}})),
            (
                "schedule.create",
                json!({"workflow_id": "w", "cron": "***"}),
            ),
            ("run.list", json!({"limit": "many"})),
        ] {
            let err = call_err(&client, method, params.clone()).await;
            assert_eq!(err.code(), -32010, "{method} {params} → {err}");
        }

        // -32011 不存在：实体查询类
        for (method, params) in [
            ("workflow.get", json!({"workflow_id": "ghost"})),
            ("workflow.versions", json!({"workflow_id": "ghost"})),
            (
                "workflow.publish",
                json!({"workflow_id": "ghost", "version": 1}),
            ),
            ("run.get", json!({"run_id": "ghost"})),
            ("run.timeline", json!({"run_id": "ghost"})),
            ("run.events", json!({"run_id": "ghost"})),
            (
                "schedule.update",
                json!({"id": "ghost", "cron": "* * * * *"}),
            ),
            ("schedule.delete", json!({"id": "ghost"})),
            ("webhook.create", json!({"workflow_id": "ghost"})),
        ] {
            let err = call_err(&client, method, params.clone()).await;
            assert_eq!(err.code(), -32011, "{method} {params} → {err}");
        }

        // -32012 冲突：显式 draft 版本执行、有 run 的 workflow 删除、终态 run 取消
        let (workflow_id, version) =
            publish_workflow(&client, "契约", linear_def("return 1;")).await;
        let draft_version: Value = call(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": linear_def("return 2;")}),
        )
        .await;
        let draft_version = draft_version["version"].as_i64().unwrap();
        assert!(draft_version > version);

        let err = call_err(
            &client,
            "run.start",
            json!({"workflow_id": workflow_id, "version": draft_version}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");

        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let err = call_err(
            &client,
            "workflow.delete",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");
        let run = backend_e2e::common::wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        let err = call_err(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(err.code(), -32012, "{err}");
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "n1", "signal_id": "s", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");

        // -32601：未知方法（JSON-RPC 标准）
        let err = call_err(&client, "no.such.method", json!({})).await;
        assert_eq!(err.code(), -32601, "{err}");
    })
);

e2e_test!(params_may_be_omitted_entirely, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        // JSON-RPC 允许整体省略 params（到达 null）：解析层归一为 {}，
        // 只依赖缺省参数的方法必须正常工作
        let list: Value = call_null_params(&client, "workflow.list").await;
        assert!(list["workflows"].is_array(), "{list}");
        let types: Value = call_null_params(&client, "nodetypes.list").await;
        assert!(types["node_types"].is_array(), "{types}");
        let runs: Value = call_null_params(&client, "run.list").await;
        assert!(runs["runs"].is_array(), "{runs}");
        let schedules: Value = call_null_params(&client, "schedule.list").await;
        assert!(schedules["schedules"].is_array(), "{schedules}");
        let webhooks: Value = call_null_params(&client, "webhook.list").await;
        assert!(webhooks["webhooks"].is_array(), "{webhooks}");
    }
));

e2e_test!(run_list_limit_is_clamped_to_bounds, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "夹取", linear_def("return 1;")).await;
        for _ in 0..501 {
            start_run(&client, &workflow_id, json!({})).await;
        }

        // 0/负数 → 1；超过 500 → 500（RPC 边缘 clamp）
        let clamped_low: Value = call_json(&client, "run.list", json!({"limit": 0})).await;
        assert_eq!(clamped_low["runs"].as_array().unwrap().len(), 1);
        let clamped_negative: Value = call_json(&client, "run.list", json!({"limit": -5})).await;
        assert_eq!(clamped_negative["runs"].as_array().unwrap().len(), 1);
        let clamped_high: Value = call_json(&client, "run.list", json!({"limit": 9999})).await;
        assert_eq!(clamped_high["runs"].as_array().unwrap().len(), 500);

        // 缺省 limit → 50
        let default: Value = call_json(&client, "run.list", json!({})).await;
        assert_eq!(default["runs"].as_array().unwrap().len(), 50);
    })
});

e2e_test!(
    optional_input_defaults_to_null_and_echoes_back,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        // run.start 省略 input：按 null 执行并如实回显
        let (workflow_id, _) =
            publish_workflow(&client, "缺省输入", linear_def("return { seen: input };")).await;
        let started: Value = call(&client, "run.start", json!({"workflow_id": workflow_id})).await;
        let run_id = started["run_id"].as_str().unwrap().to_string();
        assert!(started["workflow_version"].is_i64());
        let run = backend_e2e::common::wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["input"], json!(null));
        assert_eq!(run["run"]["output"], json!({ "seen": null }));

        // workflow.get 省略 version：取 latest
        let got: Value = call(&client, "workflow.get", json!({"workflow_id": workflow_id})).await;
        assert_eq!(got["version"], json!(1));

        // run.events 省略 from_seq：从 1 开始全量
        let events: Value = call_json(&client, "run.events", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        assert_eq!(events[0]["seq"], json!(1));

        // run.subscribe 省略 run_id：全局流（不报错）
        let _sub = backend_e2e::common::subscribe(&client, None).await;
    })
);
