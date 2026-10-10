//! v2 协议与错误码契约：-32602/-32011/-32012/-32601 的映射、参数类型校验、
//! limit 夹取、写命令幂等语义。契约来源：journal_v2（v2 错误码词汇：
//! `-32001` unauthorized / `-32011` not found / `-32012` conflict /
//! `-32602` invalid params / `-32603` internal）。

use backend_e2e::common::fixtures::linear_def;
use backend_e2e::common::{call, call_err, call_json, publish_workflow, start_run, Ctx, TIMEOUT};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

e2e_test!(
    error_codes_follow_the_documented_mapping,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;

        // -32602 参数非法（v2 词汇）：缺必填字段 / 类型不对 / 词汇表外取值 /
        // 不存在实体的写命令（journal 层按 invalid 报，如 workflow.publish）
        for (method, params) in [
            ("workflow.create", json!({})),
            ("workflow.create", json!({"name": 42})),
            ("workflow.update", json!({"workflow_id": "x"})),
            ("workflow.get.view", json!({})),
            (
                "workflow.get.view",
                json!({"workflow_id": "x", "version": "one"}),
            ),
            ("run.start", json!({})),
            ("run.get.view", json!({})),
            ("run.signal", json!({"run_id": "r", "payload": {}})),
            (
                "schedule.create",
                json!({"workflow_id": "w", "cron": "***"}),
            ),
        ] {
            let err = call_err(&client, method, params.clone()).await;
            assert_eq!(err.code(), -32602, "{method} {params} → {err}");
        }

        // -32011 不存在：实体读面（journal_views 物化层映射 not-found）
        for (method, params) in [
            (
                "workflow.publish",
                json!({"workflow_id": "ghost", "version": 1}),
            ),
            ("workflow.get.view", json!({"workflow_id": "ghost"})),
            ("workflow.versions", json!({"workflow_id": "ghost"})),
            ("run.get.view", json!({"run_id": "ghost"})),
            ("run.timeline", json!({"run_id": "ghost"})),
            ("run.events.view", json!({"run_id": "ghost"})),
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

        // -32012 冲突：有 run 的 workflow 删除
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

        // 显式 draft 版本执行：journal 语义 = invalid（version not published）
        let err = call_err(
            &client,
            "run.start",
            json!({"workflow_id": workflow_id, "version": draft_version}),
        )
        .await;
        assert_eq!(err.code(), -32602, "{err}");

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
        // v2 语义：终态 run 取消是幂等空操作（committed 回执，无事件），不报冲突
        let cancelled: Value = call(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(cancelled["delivered"], json!(true), "{cancelled}");
        // 终态 run 的新信号是状态冲突；原已提交身份仍可查询和重放。
        let err = call_err(
            &client,
            "run.signal",
            json!({"run_id": run_id, "node_id": "n1", "payload": {}}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");

        // -32601：未知方法（JSON-RPC 标准）
        let err = call_err(&client, "no.such.method", json!({})).await;
        assert_eq!(err.code(), -32601, "{err}");

        // -32001：token 错误（v2 每请求认证，常量时间比较后拒绝）
        let bad =
            backend_e2e::common::connect(ctx.addr(), "wrong-token-0123456789abcdef0123456789")
                .await;
        let err = call_err(&bad, "workflow.list.view", json!({})).await;
        assert_eq!(err.code(), -32001, "{err}");
    })
);

e2e_test!(run_list_limit_is_clamped_to_bounds, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "夹取", linear_def("return 1;")).await;
        for _ in 0..501 {
            start_run(&client, &workflow_id, json!({})).await;
        }

        // 0/负数 → 1；超过 500 → 500（RPC 边缘 clamp）
        let clamped_low: Value = call_json(&client, "run.list.view", json!({"limit": 0})).await;
        assert_eq!(clamped_low["runs"].as_array().unwrap().len(), 1);
        let clamped_negative: Value =
            call_json(&client, "run.list.view", json!({"limit": -5})).await;
        assert_eq!(clamped_negative["runs"].as_array().unwrap().len(), 1);
        let clamped_high: Value = call_json(&client, "run.list.view", json!({"limit": 9999})).await;
        assert_eq!(clamped_high["runs"].as_array().unwrap().len(), 500);

        // 缺省 limit → 50
        let default: Value = call_json(&client, "run.list.view", json!({})).await;
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

        // workflow.get.view 省略 version：取 latest
        let got: Value = call(
            &client,
            "workflow.get.view",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        assert_eq!(got["version"], json!(1));

        // run.events.view 省略 cursor：从事件 0 开始全量（v2 事件原形）
        let events: Value = call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        assert_eq!(events[0]["event"]["run_seq"], json!("1"));

        // run.subscribe 省略 from_seq：从 0 起补齐（journal v2 事件原形）
        let mut sub = backend_e2e::common::subscribe(&client, &run_id).await;
        let collected = backend_e2e::common::collect_run_events(&mut sub, &run_id, TIMEOUT).await;
        assert!(!collected.is_empty());
    })
);
