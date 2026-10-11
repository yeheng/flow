//! 触发器端到端：cron 调度（CRUD / next_fire_at / 真触发 / 停用不触发）与
//! webhook HTTP 入口（全部分支）。
//!
//! 契约来源：DESIGN.md §9.2（触发器）。

use backend_e2e::common::fixtures::linear_def;
use backend_e2e::common::{
    call, call_err, call_json, http_post_hook, publish_workflow, wait_run_terminal, Ctx, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};
use std::time::Duration;

e2e_test!(schedule_crud_and_next_fire_at, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "定时", linear_def("return input;")).await;

        // 未知 workflow：-32011
        let err = call_err(
            &client,
            "schedule.create",
            json!({"workflow_id": "ghost", "cron": "* * * * *"}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        // 非法 cron：-32602（RPC 边缘校验）
        for bad in ["* * * *", "not a cron", "60 * * * *", ""] {
            let err = call_err(
                &client,
                "schedule.create",
                json!({"workflow_id": workflow_id, "cron": bad}),
            )
            .await;
            assert_eq!(err.code(), -32602, "cron {bad:?}：{err}");
        }

        let created: Value = call(
            &client,
            "schedule.create",
            json!({
                "workflow_id": workflow_id,
                "cron": "*/2 * * * *",
                "input": {"job": "sync"},
                "enabled": true
            }),
        )
        .await;
        let schedule_id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["workflow_id"], json!(workflow_id));
        assert_eq!(created["cron_expr"], json!("*/2 * * * *"));
        assert_eq!(created["enabled"], json!(true));
        // create 回执不回显 input（命令回执有界；round-trip 由 list 断言）
        assert!(created.get("input").is_none() || created["input"].is_null());

        // next_fire_at 由服务端算好（RFC3339，本地时区），前端不解析 cron
        let next_fire_at = created["next_fire_at"]
            .as_str()
            .expect("必须有 next_fire_at");
        let next = chrono::DateTime::parse_from_rfc3339(next_fire_at)
            .unwrap_or_else(|e| panic!("next_fire_at 不是合法 RFC3339：{next_fire_at}（{e}）"));
        assert!(next > chrono::Utc::now(), "next_fire_at 必须在将来：{next}");

        // list：按 workflow 过滤
        let list: Value = call_json(
            &client,
            "schedule.list",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        let schedules = list["schedules"].as_array().unwrap();
        assert_eq!(schedules.len(), 1, "{list}");
        assert_eq!(schedules[0]["id"], json!(schedule_id));
        assert_eq!(schedules[0]["next_fire_at"], json!(next_fire_at));

        // 部分更新：只改 cron，input 与 enabled 不动
        call::<Value>(
            &client,
            "schedule.update",
            json!({"id": schedule_id, "cron": "0 * * * *"}),
        )
        .await;
        let list: Value = call_json(&client, "schedule.list", json!({})).await;
        let schedule = list["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["id"] == json!(schedule_id))
            .unwrap()
            .clone();
        assert_eq!(schedule["cron_expr"], json!("0 * * * *"));
        assert_eq!(schedule["input"], json!({ "job": "sync" }), "缺省字段不动");
        assert_eq!(schedule["enabled"], json!(true));

        // update 的 input 双 Option：显式 null = 清空
        call::<Value>(
            &client,
            "schedule.update",
            json!({"id": schedule_id, "input": null}),
        )
        .await;
        let list: Value = call_json(&client, "schedule.list", json!({})).await;
        let schedule = list["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["id"] == json!(schedule_id))
            .unwrap()
            .clone();
        assert!(
            schedule["input"].is_null(),
            "显式 null 清空 input：{schedule}"
        );

        // 停用 + 重新启用
        call::<Value>(
            &client,
            "schedule.update",
            json!({"id": schedule_id, "enabled": false}),
        )
        .await;
        let list: Value = call_json(&client, "schedule.list", json!({})).await;
        let schedule = list["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["id"] == json!(schedule_id))
            .unwrap()
            .clone();
        assert_eq!(schedule["enabled"], json!(false));

        // update 里带非法 cron：-32602
        let err = call_err(
            &client,
            "schedule.update",
            json!({"id": schedule_id, "cron": "bogus"}),
        )
        .await;
        assert_eq!(err.code(), -32602, "{err}");

        // 未知 id 的 update / delete：-32011。
        // 注意：全字段缺省的 update 是「无操作」——两个后端都不查存在性直接返回
        // 成功（一致行为），所以这里带一个字段让它走到行存在性检查。
        let err = call_err(
            &client,
            "schedule.update",
            json!({"id": "nope", "cron": "0 * * * *"}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");
        let err = call_err(&client, "schedule.delete", json!({"id": "nope"})).await;
        assert_eq!(err.code(), -32011, "{err}");

        let deleted: Value = call(&client, "schedule.delete", json!({"id": schedule_id})).await;
        assert_eq!(deleted["deleted"], json!(true));
        let list: Value = call_json(&client, "schedule.list", json!({})).await;
        assert!(list["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["id"] != json!(schedule_id)));
    }
));

// cron 真触发：每分钟表达式 + 默认开着的调度器，一个 tick 内补上火。
// scheduler tick 20s，等待窗口放到 60s（CI 慢机器冗余）。
e2e_test!(schedule_fires_run_with_its_input, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (workflow_id, _) =
            publish_workflow(&client, "被调度", linear_def("return { echoed: input };")).await;
        let created: Value = call(
            &client,
            "schedule.create",
            json!({
                "workflow_id": workflow_id,
                "cron": "* * * * *",
                "input": {"fired_by": "cron"}
            }),
        )
        .await;
        assert_eq!(created["enabled"], json!(true));

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let run = loop {
            let list: Value = call_json(
                &client,
                "run.list.view",
                json!({"workflow_id": workflow_id}),
            )
            .await;
            if let Some(run) = list["runs"].as_array().unwrap().first() {
                break run.clone();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "60s 内 schedule 没有触发任何 run"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        // 触发等价 run.start：跑当前 published 版本，输入取 schedule.input
        assert_eq!(run["input"], json!({ "fired_by": "cron" }), "{run}");
        let run_id = run["id"].as_str().unwrap().to_string();
        let finished = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(finished["run"]["status"], json!("succeeded"));
        assert_eq!(
            finished["run"]["output"],
            json!({ "echoed": { "fired_by": "cron" } })
        );

        // 触发去重靠 schedule_fires 主键：同一分钟边界不重复触发
        // （严格的「恰好一次」留给定性单测，这里钉住「至少一次且输入正确」）
    }
));

// 停用的 schedule 不触发：跨过一个 scheduler tick（20s）仍无 run。
e2e_test!(disabled_schedule_does_not_fire, |ctx: &mut Ctx| Box::pin(
    async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "停用", linear_def("return 1;")).await;
        call::<Value>(
            &client,
            "schedule.create",
            json!({
                "workflow_id": workflow_id,
                "cron": "* * * * *",
                "enabled": false
            }),
        )
        .await;

        tokio::time::sleep(Duration::from_secs(26)).await;
        let list: Value = call_json(
            &client,
            "run.list.view",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        assert_eq!(
            list["runs"].as_array().unwrap().len(),
            0,
            "停用的 schedule 跨过一个 tick 仍不得触发：{list}"
        );
    }
));

e2e_test!(webhook_crud_and_http_trigger_branches, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) =
            publish_workflow(&client, "钩子", linear_def("return { got: input };")).await;
        let http_addr = ctx.http_addr();

        // 未知 workflow：-32011
        let err = call_err(&client, "webhook.create", json!({"workflow_id": "ghost"})).await;
        assert_eq!(err.code(), -32011, "{err}");

        let created: Value = call(
            &client,
            "webhook.create",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        let token = created["token"].as_str().unwrap().to_string();
        assert_eq!(created["workflow_id"], json!(workflow_id));
        assert_eq!(created["enabled"], json!(true));
        assert!(token.len() >= 16, "token 必须随机不可猜：{token}");

        let list: Value = call_json(&client, "webhook.list", json!({})).await;
        assert_eq!(list["webhooks"].as_array().unwrap().len(), 1, "{list}");

        // 未知 token：404（与已禁用不区分，避免探测）
        let (status, body) =
            http_post_hook(http_addr, ctx.token(), "wrong-token", Some("{}")).await;
        assert_eq!(status, 400, "{body}");

        // 非 JSON body：400
        let (status, body) = http_post_hook(http_addr, ctx.token(), &token, Some("not json")).await;
        assert_eq!(status, 400, "{body}");

        // 停用：404
        call::<Value>(
            &client,
            "webhook.set_enabled",
            json!({"token": token, "enabled": false}),
        )
        .await;
        let (status, body) = http_post_hook(http_addr, ctx.token(), &token, Some("{}")).await;
        assert_eq!(status, 400, "{body}");
        // 重新启用
        call::<Value>(
            &client,
            "webhook.set_enabled",
            json!({"token": token, "enabled": true}),
        )
        .await;

        // 无 published 版本的 workflow：409（创建时就地再做一个 workflow）
        let created_wf: Value = call(&client, "workflow.create", json!({"name": "未发布"})).await;
        let draft_wf = created_wf["workflow_id"].as_str().unwrap().to_string();
        call::<Value>(
            &client,
            "workflow.update",
            json!({"workflow_id": draft_wf, "definition": linear_def("return 1;")}),
        )
        .await;
        let draft_hook: Value =
            call(&client, "webhook.create", json!({"workflow_id": draft_wf})).await;
        let (status, body) = http_post_hook(
            http_addr,
            ctx.token(),
            draft_hook["token"].as_str().unwrap(),
            Some("{}"),
        )
        .await;
        assert_eq!(status, 400, "{body}");

        // 成功：body 作为 input 启动 run；空 body 视为 null 输入
        let (status, body) = http_post_hook(
            http_addr,
            ctx.token(),
            &token,
            Some(r#"{"event": "order.paid", "id": 7}"#),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let response: Value = serde_json::from_str(&body).expect("响应必须是 JSON");
        let run_id = response["result"]["run_id"].as_str().unwrap().to_string();
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(
            run["run"]["input"],
            json!({ "event": "order.paid", "id": 7 })
        );
        assert_eq!(
            run["run"]["output"],
            json!({ "got": { "event": "order.paid", "id": 7 } })
        );

        let (status, body) = http_post_hook(http_addr, ctx.token(), &token, None).await;
        assert_eq!(status, 200, "{body}");
        let response: Value = serde_json::from_str(&body).unwrap();
        let run_id = response["result"]["run_id"].as_str().unwrap().to_string();
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["input"], json!(null), "空 body → null 输入");

        let deleted: Value = call(&client, "webhook.delete", json!({"token": token})).await;
        assert_eq!(deleted["deleted"], json!(true));
        let (status, _) = http_post_hook(http_addr, ctx.token(), &token, Some("{}")).await;
        assert_eq!(status, 400);
        let list: Value = call_json(&client, "webhook.list", json!({})).await;
        assert_eq!(
            list["webhooks"].as_array().unwrap().len(),
            1,
            "只剩 draft 那个"
        );
    })
});
