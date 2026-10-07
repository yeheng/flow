//! 工作流定义生命周期：create / update / publish / get / list / delete / versions。
//!
//! 契约来源：DESIGN.md §5（定义模型与校验）、§8（存储约束）、§9（RPC 方法表）。

use backend_e2e::common::fixtures::linear_def;
use backend_e2e::common::{
    call, call_err, call_json, call_null_params, publish_workflow, start_run, wait_run_terminal,
    Ctx, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

fn invalid_definitions() -> Vec<(&'static str, Value)> {
    let node = |id: &str, kind: &str, params: Value| json!({"id": id, "type": kind, "name": id, "params": params});
    let start = node("start", "start", json!({}));
    let end = node("end", "end", json!({}));
    let script = node("n1", "script", json!({"code": "return 1;"}));
    vec![
        ("没有任何节点", json!({"nodes": [], "edges": []})),
        (
            "没有 start",
            json!({"nodes": [script.clone(), end.clone()],
                   "edges": [{"from": "n1", "to": "end"}]}),
        ),
        (
            "两个 start",
            json!({"nodes": [start.clone(), start.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "end"}]}),
        ),
        (
            "没有 end",
            json!({"nodes": [start.clone(), script.clone()],
                   "edges": [{"from": "start", "to": "n1"}]}),
        ),
        (
            "节点 id 重复",
            json!({"nodes": [start.clone(), script.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "n1"}, {"from": "n1", "to": "end"}]}),
        ),
        (
            "自环",
            json!({"nodes": [start.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "n1"}, {"from": "n1", "to": "n1"},
                             {"from": "n1", "to": "end"}]}),
        ),
        (
            "成环",
            json!({"nodes": [start.clone(), node("a", "script", json!({"code": "return 1;"})),
                              node("b", "script", json!({"code": "return 2;"})), end.clone()],
                   "edges": [{"from": "start", "to": "a"}, {"from": "a", "to": "b"},
                             {"from": "b", "to": "a"}, {"from": "b", "to": "end"}]}),
        ),
        (
            "未知节点类型",
            json!({"nodes": [start.clone(), node("x", "quantum", json!({})), end.clone()],
                   "edges": [{"from": "start", "to": "x"}, {"from": "x", "to": "end"}]}),
        ),
        (
            "script 缺 code",
            json!({"nodes": [start.clone(), node("n1", "script", json!({})), end.clone()],
                   "edges": [{"from": "start", "to": "n1"}, {"from": "n1", "to": "end"}]}),
        ),
        (
            "delay 缺 ms",
            json!({"nodes": [start.clone(), node("d", "delay", json!({})), end.clone()],
                   "edges": [{"from": "start", "to": "d"}, {"from": "d", "to": "end"}]}),
        ),
        (
            "重复的边",
            json!({"nodes": [start.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "n1"}, {"from": "start", "to": "n1"},
                             {"from": "n1", "to": "end"}]}),
        ),
        (
            "非 condition 节点带端口",
            json!({"nodes": [start.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "n1", "port": "true"},
                             {"from": "n1", "to": "end"}]}),
        ),
        (
            "condition 出边缺端口",
            json!({"nodes": [start.clone(), node("c", "condition", json!({"expr": "true"})), end.clone()],
                   "edges": [{"from": "start", "to": "c"}, {"from": "c", "to": "end"}]}),
        ),
        (
            "边指向不存在的节点",
            json!({"nodes": [start.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "n1"}, {"from": "n1", "to": "ghost"}]}),
        ),
        (
            "孤立节点没有入边",
            json!({"nodes": [start.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "start", "to": "end"}]}),
        ),
        (
            "end 有出边",
            json!({"nodes": [start.clone(), script.clone(), end.clone(), node("e2", "end", json!({}))],
                   "edges": [{"from": "start", "to": "n1"}, {"from": "n1", "to": "end"},
                             {"from": "end", "to": "e2"}]}),
        ),
        (
            "start 有入边",
            json!({"nodes": [start.clone(), script.clone(), end.clone()],
                   "edges": [{"from": "n1", "to": "start"}, {"from": "start", "to": "n1"},
                             {"from": "n1", "to": "end"}]}),
        ),
    ]
}

e2e_test!(create_update_publish_get_list_roundtrip, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;

        let created: Value = call(&client, "workflow.create", json!({"name": "订单流"})).await;
        let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
        assert!(!workflow_id.is_empty());

        // draft：update 后 version=1，未发布前 run.start 不可执行（另测）
        let definition = linear_def("return { total: 42 };");
        let updated: Value = call(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": definition}),
        )
        .await;
        assert_eq!(updated["version"], json!(1));

        let got: Value = call(
            &client,
            "workflow.get",
            json!({"workflow_id": workflow_id, "version": 1}),
        )
        .await;
        assert_eq!(got["status"], json!("draft"));
        assert_eq!(got["version"], json!(1));
        assert_eq!(got["definition"], definition);
        assert!(got["published_version"].is_null());

        let published: Value = call(
            &client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": 1}),
        )
        .await;
        assert_eq!(published["status"], json!("published"));

        let got: Value = call(&client, "workflow.get", json!({"workflow_id": workflow_id})).await;
        assert_eq!(got["status"], json!("published"));
        assert_eq!(got["published_version"], json!(1));

        let list: Value = call_null_params(&client, "workflow.list").await;
        let workflows = list["workflows"].as_array().unwrap();
        let summary = workflows
            .iter()
            .find(|w| w["workflow_id"] == json!(workflow_id))
            .unwrap_or_else(|| panic!("workflow.list 里没有 {workflow_id}：{list}"));
        assert_eq!(summary["name"], json!("订单流"));
        assert_eq!(summary["latest_version"], json!(1));
        assert_eq!(summary["published_version"], json!(1));
        assert!(summary["created_at"].is_string());

        // workflow.get 省略 version 取 latest
        let got: Value = call(&client, "workflow.get", json!({"workflow_id": workflow_id})).await;
        assert_eq!(got["version"], json!(1));

        // 删除
        let deleted: Value = call(
            &client,
            "workflow.delete",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        assert_eq!(deleted["deleted"], json!(true));
        let err = call_err(&client, "workflow.get", json!({"workflow_id": workflow_id})).await;
        assert_eq!(err.code(), -32011, "{err}");
    })
});

e2e_test!(
    same_definition_reuses_version_and_change_bumps,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let created: Value = call(&client, "workflow.create", json!({"name": "同定义"})).await;
        let workflow_id = created["workflow_id"].as_str().unwrap().to_string();

        let definition = linear_def("return 1;");
        let update = |definition: Value| {
            call::<Value>(
                &client,
                "workflow.update",
                json!({"workflow_id": workflow_id, "definition": definition}),
            )
        };

        let v1 = update(definition.clone()).await;
        let v2 = update(definition.clone()).await;
        assert_eq!(v1["version"], json!(1));
        assert_eq!(
            v2["version"],
            json!(1),
            "相同 checksum 的定义必须复用版本号（§8）"
        );

        let changed = linear_def("return 2;");
        let v3 = update(changed.clone()).await;
        assert_eq!(v3["version"], json!(2), "定义变更必须分配新版本");

        // checksum 稳定且随定义变化（§8：checksum = sha256(definition JSON)）
        let versions: Value = call(
            &client,
            "workflow.versions",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        let versions = versions["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0]["version"], json!(2), "版本历史倒序");
        assert_eq!(versions[1]["version"], json!(1));

        // checksum 与 flow-store 的定义 checksum 算法一致：sha256(定义 JSON 字节)
        let digest = |definition: &Value| {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(serde_json::to_vec(definition).unwrap()))
        };
        assert_eq!(versions[0]["checksum"], json!(digest(&changed)));
        assert_eq!(versions[1]["checksum"], json!(digest(&definition)));
        assert!(
            versions[0].get("definition").is_none(),
            "versions 只回元数据列"
        );
    })
);

e2e_test!(
    versions_lists_desc_and_unknown_workflow_not_found,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "历史", linear_def("return 1;")).await;
        let second: Value = call(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": linear_def("return 2;")}),
        )
        .await;
        assert_eq!(second["version"], json!(2));

        let versions: Value = call(
            &client,
            "workflow.versions",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        let versions = versions["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0]["version"], json!(2));
        assert_eq!(versions[0]["status"], json!("draft"));
        assert_eq!(versions[1]["version"], json!(1));
        assert_eq!(versions[1]["status"], json!("published"));

        // 从未保存过版本的 workflow：空列表而非报错
        let empty: Value = call(&client, "workflow.create", json!({"name": "空"})).await;
        let empty_id = empty["workflow_id"].as_str().unwrap();
        let list: Value = call(
            &client,
            "workflow.versions",
            json!({"workflow_id": empty_id}),
        )
        .await;
        assert_eq!(list["versions"].as_array().unwrap().len(), 0);

        // workflow 不存在：与 workflow.get 同语义 -32011
        let err = call_err(&client, "workflow.versions", json!({"workflow_id": "nope"})).await;
        assert_eq!(err.code(), -32011, "{err}");
    })
);

e2e_test!(
    update_and_publish_reject_invalid_definitions,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let created: Value = call(&client, "workflow.create", json!({"name": "校验"})).await;
        let workflow_id = created["workflow_id"].as_str().unwrap().to_string();

        for (label, definition) in invalid_definitions() {
            let err = call_err(
                &client,
                "workflow.update",
                json!({"workflow_id": workflow_id, "definition": definition.clone()}),
            )
            .await;
            assert_eq!(
                err.code(),
                -32010,
                "workflow.update 必须拒收「{label}」：{err}"
            );
            // Seed a real draft, then simulate a legacy/corrupted stored definition.
            // Publication must reject that definition, not merely a missing version.
            let draft: Value = call(
                &client,
                "workflow.update",
                json!({"workflow_id":workflow_id,"definition":linear_def("return 1;")}),
            )
            .await;
            let version = draft["version"].as_i64().unwrap();
            if let Some(url) = ctx.pg_url() {
                let pool = sqlx::PgPool::connect(url).await.unwrap();
                sqlx::query("UPDATE workflow_versions SET definition=$1 WHERE workflow_id=$2 AND version=$3")
                    .bind(&definition).bind(&workflow_id).bind(version).execute(&pool).await.unwrap();
                pool.close().await;
            } else {
                let options =
                    sqlx::sqlite::SqliteConnectOptions::new().filename(ctx.sqlite_path().unwrap());
                let pool = sqlx::SqlitePool::connect_with(options).await.unwrap();
                sqlx::query(
                    "UPDATE workflow_versions SET definition=? WHERE workflow_id=? AND version=?",
                )
                .bind(serde_json::to_string(&definition).unwrap())
                .bind(&workflow_id)
                .bind(version)
                .execute(&pool)
                .await
                .unwrap();
                pool.close().await;
            }
            let err = call_err(
                &client,
                "workflow.publish",
                json!({"workflow_id": workflow_id, "version": version}),
            )
            .await;
            assert_eq!(err.code(), -32010, "「{label}」的 publish 结果：{err}");
        }

        // definition 结构本身不合法（缺 nodes 字段）
        let err = call_err(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": {"edges": []}}),
        )
        .await;
        assert_eq!(err.code(), -32010, "{err}");
    })
);

e2e_test!(publish_unknown_version_is_not_found, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, version) =
            publish_workflow(&client, "版本", linear_def("return 1;")).await;
        assert_eq!(version, 1);
        let err = call_err(
            &client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": 99}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        // 重复 publish 同一版本：published → published 仍成功（幂等发布）
        let again: Value = call(
            &client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": version}),
        )
        .await;
        assert_eq!(again["status"], json!("published"));
    })
});

e2e_test!(delete_workflow_with_runs_is_rejected, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let (workflow_id, _) =
            publish_workflow(&client, "有 run 的流", linear_def("return 1;")).await;
        start_run(&client, &workflow_id, json!({})).await;

        // 有 run 记录时拒删（事件日志不能变孤儿，§8）
        let err = call_err(
            &client,
            "workflow.delete",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");

        // 删除未知 workflow：-32011
        let err = call_err(&client, "workflow.delete", json!({"workflow_id": "ghost"})).await;
        assert_eq!(err.code(), -32011, "{err}");

        // 无 run 的 workflow 可以删
        let (other, _) = publish_workflow(&client, "没 run 的流", linear_def("return 1;")).await;
        let deleted: Value = call(&client, "workflow.delete", json!({"workflow_id": other})).await;
        assert_eq!(deleted["deleted"], json!(true));
        let list: Value = call_null_params(&client, "workflow.list").await;
        assert!(list["workflows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|w| w["workflow_id"] != json!(other)));
    })
});

e2e_test!(
    nodetypes_list_exposes_canvas_capabilities,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let types: Value = call_json(&client, "nodetypes.list", json!({})).await;
        let types = types["node_types"].as_array().unwrap();
        let ids: Vec<&str> = types.iter().map(|t| t["type"].as_str().unwrap()).collect();
        assert_eq!(
            ids,
            vec![
                "start",
                "end",
                "script",
                "condition",
                "delay",
                "http_call",
                "human_task",
                "sub_workflow",
                "llm",
                "email"
            ],
            "node 类型闭集：{ids:?}"
        );

        let find = |kind: &str| {
            types
                .iter()
                .find(|t| t["type"] == json!(kind))
                .unwrap_or_else(|| panic!("缺少 {kind}"))
                .clone()
        };

        // 端口规则（§5 端口约束的对外表达）
        assert_eq!(
            find("start")["ports"],
            json!([{"id": "out", "label": "出"}])
        );
        assert_eq!(
            find("condition")["ports"],
            json!([
                {"id": "in", "label": "入"},
                {"id": "true", "label": "真"},
                {"id": "false", "label": "假"}
            ])
        );

        // params_schema：JSON Schema 子集 + x-* 扩展
        let script = find("script");
        assert_eq!(script["params_schema"]["required"], json!(["code"]));
        assert_eq!(
            script["params_schema"]["properties"]["code"]["x-widget"],
            json!("code")
        );
        assert_eq!(script["supports_retry"], json!(true));

        let http = find("http_call");
        assert_eq!(http["side_effect"], json!(true), "http_call 有外部副作用");
        assert_eq!(
            http["params_schema"]["properties"]["method"]["enum"],
            json!(["GET", "POST", "PUT", "PATCH", "DELETE"])
        );
        assert_eq!(http["supports_retry"], json!(true));

        let sub = find("sub_workflow");
        assert_eq!(
            sub["params_schema"]["properties"]["workflow_id"]["x-widget"],
            json!("workflow-picker")
        );
        assert_eq!(sub["supports_retry"], json!(true));

        // human_task / delay 不声明重试（不在 supports_retry 闭集里）
        assert!(find("human_task").get("supports_retry").is_none());
        assert!(find("delay").get("supports_retry").is_none());
        assert!(find("delay")["params_schema"]["required"] == json!(["ms"]));
    })
);

// ---- 只有 published 可执行 ----

e2e_test!(run_start_requires_published_version, |ctx: &mut Ctx| {
    Box::pin(async move {
        let client = ctx.client().await;
        let created: Value = call(&client, "workflow.create", json!({"name": "草稿流"})).await;
        let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
        call::<Value>(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": linear_def("return 1;")}),
        )
        .await;

        // 一个 published 版本都没有：-32010（invalid）
        let err = call_err(&client, "run.start", json!({"workflow_id": workflow_id})).await;
        assert_eq!(err.code(), -32010, "{err}");

        // 显式指定 draft 版本：-32012（VersionNotPublished → conflict）
        let err = call_err(
            &client,
            "run.start",
            json!({"workflow_id": workflow_id, "version": 1}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");

        // 发布后 v1 可执行，v2（新 draft）显式指定仍拒绝
        call::<Value>(
            &client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": 1}),
        )
        .await;
        call::<Value>(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": linear_def("return 2;")}),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"));
        assert_eq!(
            run["run"]["workflow_version"],
            json!(1),
            "run 钉死 published 版本"
        );

        let err = call_err(
            &client,
            "run.start",
            json!({"workflow_id": workflow_id, "version": 2}),
        )
        .await;
        assert_eq!(err.code(), -32012, "{err}");

        // version 不存在：-32011
        let err = call_err(
            &client,
            "run.start",
            json!({"workflow_id": workflow_id, "version": 77}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        // workflow 不存在且未指定版本：「没有已发布版本」先被解析层拦下（-32010）；
        // 指定版本时才会走到版本查询，报 -32011（resolve_runnable_definition 单点规则）
        let err = call_err(&client, "run.start", json!({"workflow_id": "ghost"})).await;
        assert_eq!(err.code(), -32010, "{err}");
        let err = call_err(
            &client,
            "run.start",
            json!({"workflow_id": "ghost", "version": 1}),
        )
        .await;
        assert_eq!(err.code(), -32011, "{err}");

        // run.start 省略 input：按 null 输入执行
        let no_input: Value = call(&client, "run.start", json!({"workflow_id": workflow_id})).await;
        let run_id = no_input["run_id"].as_str().unwrap().to_string();
        let run = wait_run_terminal(&client, &run_id, SHORT).await;
        assert_eq!(run["run"]["input"], json!(null));
    })
});
