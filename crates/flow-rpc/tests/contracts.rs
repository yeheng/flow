use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flow_backend::journal::JournalBackend;
use flow_backend::{AnyBackend, CreateRun};
use flow_rpc::AppState;
const TOKEN: &str = "flow-rpc-contract-test-token-32-bytes";
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
    backend: Arc<JournalBackend>,
    arm: AnyBackend,
    state: Arc<AppState>,
    workflow: String,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-contracts-{}", uuid::Uuid::now_v7()));
        let backend = JournalBackend::open(&root, Default::default())
            .await
            .unwrap();
        flow_backend::start_execution(&flow_config::ExecutionConfig::default(), &backend)
            .await
            .unwrap();
        let workflow = backend.workflow_create("test", None).await.unwrap();
        let workflow = workflow.result["workflow_id"].as_str().unwrap().to_string();
        backend
            .workflow_update(&workflow, definition(7), None)
            .await
            .unwrap();
        let state = Arc::new(AppState {
            backend: AnyBackend::Journal(backend.clone()),
            config: None,
            secrets: None,
        });
        Self {
            root,
            arm: AnyBackend::Journal(backend.clone()),
            backend,
            state,
            workflow,
        }
    }

    async fn raw_call(&self, method: &str, mut params: Value) -> Value {
        let module = flow_rpc::journal_v2::module_product(
            self.backend.clone(),
            TOKEN.to_owned(),
            Some(self.state.clone()),
        )
        .unwrap();
        params["_token"] = json!(TOKEN);
        let request =
            json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}).to_string();
        let (response, _) = module.raw_json_request(&request, 1).await.unwrap();
        serde_json::from_str(response.get()).unwrap()
    }

    async fn call(&self, method: &str, mut params: Value) -> Value {
        let method = match method {
            "workflow.get" => "workflow.get.full",
            "workflow.list" => "workflow.list.full",
            "run.get" => "run.get.full",
            "run.list" => "run.list.full",
            "run.events" => "run.events.full",
            other => other,
        };
        if flow_rpc::journal_v2::is_write_method(method) && params.get("request_id").is_none() {
            params["request_id"] = json!(uuid::Uuid::now_v7().to_string());
        }
        let mut response = self.raw_call(method, params).await;
        if response["result"]["committed"] == true {
            response["result"] = response["result"]["result"].clone();
        }
        response
    }

    async fn wait_finished(&self, run: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.backend.state().await;
                if let Some(row) = state.runs.get(run).filter(|r| r.terminal()) {
                    return json!({
                        "status": row.status,
                        "error": row.error,
                    });
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("run 未在超时内终结")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn definition(output: i64) -> Value {
    json!({
        "nodes": [
            {"id":"s", "type":"start"},
            {"id":"n", "type":"script", "params":{"code":format!("return {output};")}},
            {"id":"e", "type":"end"}
        ],
        "edges": [{"from":"s", "to":"n"}, {"from":"n", "to":"e"}]
    })
}

fn email_def(api_key: &str) -> Value {
    json!({
        "nodes": [
            {"id":"s", "type":"start"},
            {"id":"n", "type":"email", "params":{"api_key": api_key, "from": "a@b.c", "to": "d@e.f", "subject": "s", "body": "b"}},
            {"id":"e", "type":"end"}
        ],
        "edges": [{"from":"s", "to":"n"}, {"from":"n", "to":"e"}]
    })
}

/// secrets.list：排序后的 {name, source} 清单，永远不含值。
#[tokio::test]
async fn secrets_list_returns_sorted_names_without_values() {
    let f = Fixture::new().await;
    std::env::set_var("FLOW_SECRET_TEST_RPC_B", "value-b");
    std::env::set_var("FLOW_SECRET_TEST_RPC_A", "value-a");

    let resp = f.call("secrets.list", json!({})).await;
    let entries: Vec<(&str, &str)> = resp["result"]["secrets"]
        .as_array()
        .expect("result.secrets 必须是数组")
        .iter()
        .map(|s| {
            (
                s["name"].as_str().expect("name"),
                s["source"].as_str().expect("source"),
            )
        })
        .collect();
    let pos_a = entries
        .iter()
        .position(|(n, _)| *n == "TEST_RPC_A")
        .unwrap();
    let pos_b = entries
        .iter()
        .position(|(n, _)| *n == "TEST_RPC_B")
        .unwrap();
    assert!(pos_a < pos_b, "必须按名称排序：{entries:?}");
    assert!(
        entries.iter().all(|(_, s)| *s == "env"),
        "无持久化存储时全部归 env：{entries:?}"
    );
    assert!(!resp.to_string().contains("value-a"), "响应不得含真值");

    std::env::remove_var("FLOW_SECRET_TEST_RPC_A");
    std::env::remove_var("FLOW_SECRET_TEST_RPC_B");
}

/// workflow.update 的密钥提前校验：名称未配置 → -32602 并点名环境变量；
/// 配置后落库的仍是名称，不是真值。
#[tokio::test]
async fn workflow_update_validates_secret_names_exist() {
    let f = Fixture::new().await;

    let bad = f
        .call(
            "workflow.update",
            json!({"workflow_id": f.workflow, "definition": email_def("TEST_RPC_MISSING")}),
        )
        .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    let message = bad["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("TEST_RPC_MISSING") && message.contains("FLOW_SECRET_TEST_RPC_MISSING"),
        "{message}"
    );

    std::env::set_var("FLOW_SECRET_TEST_RPC_OK", "real-value");
    let ok = f
        .call(
            "workflow.update",
            json!({"workflow_id": f.workflow, "definition": email_def("TEST_RPC_OK")}),
        )
        .await;
    assert!(ok.get("error").is_none(), "{ok}");
    let got = f
        .call("workflow.get", json!({"workflow_id": f.workflow}))
        .await;
    assert_eq!(
        got["result"]["definition"]["nodes"][1]["params"]["api_key"],
        json!("TEST_RPC_OK"),
        "落库的必须是名称而非真值"
    );
    std::env::remove_var("FLOW_SECRET_TEST_RPC_OK");
}

#[tokio::test]
async fn explicit_draft_is_rejected_and_historical_published_version_still_runs() {
    let f = Fixture::new().await;
    let rejected = f
        .call("run.start", json!({"workflow_id":f.workflow, "version":1}))
        .await;
    assert_eq!(rejected["error"]["code"], -32602);
    assert!(f
        .arm
        .list_runs(None, None, None, None, 100)
        .await
        .unwrap()
        .is_empty());
    f.backend
        .workflow_publish(&f.workflow, 1, None)
        .await
        .unwrap();
    f.backend
        .workflow_update(&f.workflow, definition(8), None)
        .await
        .unwrap();
    let rejected = f
        .call("run.start", json!({"workflow_id":f.workflow, "version":2}))
        .await;
    assert_eq!(rejected["error"]["code"], -32602);
    for params in [
        json!({"workflow_id":f.workflow, "version":1}),
        json!({"workflow_id":f.workflow}),
    ] {
        let response = f.call("run.start", params).await;
        assert_eq!(response["result"]["workflow_version"], 1);
        let run = response["result"]["run_id"].as_str().unwrap();
        let row = f.wait_finished(run).await;
        assert_eq!(row["status"], "succeeded");
        assert_eq!(
            f.arm.get_run(run).await.unwrap().output,
            Some(json!(7)),
            "历史 published 版本（v1 定义 output=7）仍可执行"
        );
    }
}

fn human_def() -> Value {
    json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "h", "type": "human_task"},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "h"}, {"from": "h", "to": "e"}]
    })
}

async fn wait_human_waiting(f: &Fixture, run: &str) -> u64 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = f.backend.state().await;
            if let Some(run) = state.runs.get(run) {
                if run.nodes.get("h").is_some_and(|n| n.status == "waiting") {
                    return run.last_run_seq;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("human_task 未进入等待")
}

/// 信号错误码契约（两后端同一组码；PG 侧在 ws_pg.rs 钉住落账语义）：
/// run 不存在 → -32011，run 已终结 → -32012，节点不等待信号 → -32602。
#[tokio::test]
async fn run_signal_error_codes_follow_the_shared_contract() {
    let f = Fixture::new().await;
    f.backend
        .workflow_publish(&f.workflow, 1, None)
        .await
        .unwrap();

    let response = f
        .call(
            "run.signal",
            json!({"run_id": "nope", "node_id": "h", "payload": {}}),
        )
        .await;
    assert_eq!(response["error"]["code"], -32011, "{response}");

    let response = f
        .call("run.start", json!({"workflow_id": f.workflow}))
        .await;
    let run = response["result"]["run_id"].as_str().unwrap().to_string();
    f.wait_finished(&run).await;
    let response = f
        .call(
            "run.signal",
            json!({"run_id": run, "node_id": "h", "payload": {}}),
        )
        .await;
    assert_eq!(response["error"]["code"], -32012, "{response}");

    let wf = f.backend.workflow_create("human", None).await.unwrap();
    let wf = wf.result["workflow_id"].as_str().unwrap().to_string();
    f.backend
        .workflow_update(&wf, human_def(), None)
        .await
        .unwrap();
    f.backend.workflow_publish(&wf, 1, None).await.unwrap();
    let response = f.call("run.start", json!({"workflow_id": wf})).await;
    let run = response["result"]["run_id"].as_str().unwrap().to_string();
    wait_human_waiting(&f, &run).await;
    let response = f
        .call(
            "run.signal",
            json!({"run_id": run, "node_id": "bogus", "payload": {}}),
        )
        .await;
    assert_eq!(response["error"]["code"], -32602, "{response}");
}

/// 指定 run_id 的订阅契约（两后端共享 run_tail）：先回放完整日志、
/// 追实时增量、seq 从 1 严格连续，run 终态追平后流自然结束。
#[tokio::test]
async fn subscribe_with_run_id_replays_follows_and_naturally_ends() {
    use futures::StreamExt;

    let f = Fixture::new().await;
    let wf = f.backend.workflow_create("human", None).await.unwrap();
    let wf = wf.result["workflow_id"].as_str().unwrap().to_string();
    f.backend
        .workflow_update(&wf, human_def(), None)
        .await
        .unwrap();
    f.backend.workflow_publish(&wf, 1, None).await.unwrap();
    let response = f.call("run.start", json!({"workflow_id": wf})).await;
    let run = response["result"]["run_id"].as_str().unwrap().to_string();
    let until = wait_human_waiting(&f, &run).await;

    let stream = f.arm.subscribe(Some(run.clone()));
    futures::pin_mut!(stream);

    // 回放段：覆盖到等待点之前的全部已映射事件（WaitRegistered 占用尾部
    // 序号但不映射到 v1 事件面——journal 的 seq 允许空洞）
    let mut seqs: Vec<u64> = Vec::new();
    while (seqs.len() as u64) < until.saturating_sub(1) {
        let envelope = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("回放停滞")
            .expect("回放提前结束");
        assert_eq!(envelope.run_id, run);
        seqs.push(envelope.seq);
    }

    // 交付信号推进 run 到终态：流必须吐完增量并自然结束
    let response = f
        .call(
            "run.signal",
            json!({"run_id": run, "node_id": "h", "payload": {"ok": true}}),
        )
        .await;
    assert_eq!(response["result"]["delivered"], true, "{response}");

    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("订阅流未在终态后结束")
    {
        assert_eq!(envelope.run_id, run);
        seqs.push(envelope.seq);
    }

    assert_eq!(seqs.first(), Some(&1u64), "从 1 开始：{seqs:?}");
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "journal 序号严格递增（允许空洞）：{seqs:?}"
    );
    let events = f.arm.read_events(&run, None).await.unwrap();
    let last = events.last().unwrap();
    assert_eq!(last.seq, *seqs.last().unwrap());
    assert!(matches!(
        last.event,
        flow_engine::Event::RunCompleted { .. }
    ));
}

#[tokio::test]
async fn workflow_versions_lists_desc_and_unknown_workflow_is_not_found() {
    let f = Fixture::new().await;
    // Fixture 自带 v1 draft（definition(7)）：发布后 v2 是新 draft
    f.backend
        .workflow_publish(&f.workflow, 1, None)
        .await
        .unwrap();
    let v2 = f
        .backend
        .workflow_update(&f.workflow, definition(8), None)
        .await
        .unwrap();
    assert_eq!(v2.result["version"], 2);

    let ok = f
        .call("workflow.versions", json!({"workflow_id": f.workflow}))
        .await;
    let versions = ok["result"]["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2, "{ok}");
    // 按 version 倒序：v2 draft 在前，v1 published 在后
    assert_eq!(versions[0]["version"], 2);
    assert_eq!(versions[0]["status"], "draft");
    assert_eq!(versions[1]["version"], 1);
    assert_eq!(versions[1]["status"], "published");
    // 元数据列齐全；definition 不在版本列表里（按需走 workflow.get）
    assert!(versions[0]["checksum"].is_string());
    assert!(versions[0]["created_at"].is_string());
    assert!(versions[0].get("definition").is_none());

    // 从未保存过版本的 workflow：空列表，不报错
    let empty = f.backend.workflow_create("empty", None).await.unwrap();
    let empty = empty.result["workflow_id"].as_str().unwrap().to_string();
    let resp = f
        .call("workflow.versions", json!({"workflow_id": empty}))
        .await;
    assert_eq!(resp["result"]["versions"].as_array().unwrap().len(), 0);

    // workflow 不存在：与 workflow.get 同一语义 -32011
    let missing = f
        .call("workflow.versions", json!({"workflow_id": "nope"}))
        .await;
    assert_eq!(missing["error"]["code"], -32011);
}

// ---- 触发器：schedule / webhook 契约 ----

#[tokio::test]
async fn schedule_crud_and_next_fire_at() {
    let f = Fixture::new().await;

    // 非法 cron → -32602，不落库
    let bad = f
        .call(
            "schedule.create",
            json!({"workflow_id": f.workflow, "cron": "not a cron"}),
        )
        .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    let bad = f
        .call(
            "schedule.create",
            json!({"workflow_id": f.workflow, "cron": "* * * *"}),
        )
        .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");

    // workflow 不存在 → -32011
    let missing = f
        .call(
            "schedule.create",
            json!({"workflow_id": "nope", "cron": "* * * * *"}),
        )
        .await;
    assert_eq!(missing["error"]["code"], -32011, "{missing}");

    // 创建：默认 enabled，next_fire_at 必须在 future
    let ok = f
        .call(
            "schedule.create",
            json!({"workflow_id": f.workflow, "cron": "*/5 * * * *", "input": {"k": 1}}),
        )
        .await;
    let schedule = &ok["result"];
    let id = schedule["id"].as_str().unwrap().to_string();
    assert_eq!(schedule["workflow_id"], f.workflow, "{ok}");
    assert_eq!(schedule["cron_expr"], "*/5 * * * *");
    assert_eq!(schedule["input"], json!({"k": 1}));
    assert_eq!(schedule["enabled"], true);
    let next_fire_at = schedule["next_fire_at"].as_str().expect("next_fire_at");
    let next = chrono::DateTime::parse_from_rfc3339(next_fire_at).unwrap();
    assert!(next > chrono::Utc::now(), "next_fire_at 必须在未来：{next}");

    // list：带 workflow_id 过滤 + next_fire_at
    let list = f
        .call("schedule.list", json!({"workflow_id": f.workflow}))
        .await;
    let schedules = list["result"]["schedules"].as_array().unwrap();
    assert_eq!(schedules.len(), 1, "{list}");
    assert!(schedules[0]["next_fire_at"].is_string());
    let other = f.backend.workflow_create("other", None).await.unwrap();
    let other = other.result["workflow_id"].as_str().unwrap().to_string();
    let list = f.call("schedule.list", json!({"workflow_id": other})).await;
    assert_eq!(list["result"]["schedules"].as_array().unwrap().len(), 0);

    // update：cron 合法校验；input 缺省=不改
    let bad = f
        .call("schedule.update", json!({"id": id, "cron": "bogus"}))
        .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    let ok = f
        .call(
            "schedule.update",
            json!({"id": id, "cron": "0 9 * * *", "enabled": false}),
        )
        .await;
    assert_eq!(ok["result"]["updated"], true, "{ok}");
    let list = f
        .call("schedule.list", json!({"workflow_id": f.workflow}))
        .await;
    let s = &list["result"]["schedules"][0];
    assert_eq!(s["cron_expr"], "0 9 * * *", "{list}");
    assert_eq!(s["enabled"], false);
    assert_eq!(s["input"], json!({"k": 1}), "input 缺省不动");

    // input 显式 null = 清空
    let ok = f
        .call("schedule.update", json!({"id": id, "input": null}))
        .await;
    assert_eq!(ok["result"]["updated"], true);
    let list = f
        .call("schedule.list", json!({"workflow_id": f.workflow}))
        .await;
    assert!(list["result"]["schedules"][0]["input"].is_null(), "{list}");

    // update/delete 不存在的 id → -32011
    let missing = f
        .call("schedule.update", json!({"id": "nope", "enabled": true}))
        .await;
    assert_eq!(missing["error"]["code"], -32011, "{missing}");
    let missing = f.call("schedule.delete", json!({"id": "nope"})).await;
    assert_eq!(missing["error"]["code"], -32011, "{missing}");

    let ok = f.call("schedule.delete", json!({"id": id})).await;
    assert_eq!(ok["result"]["deleted"], true);
    let list = f
        .call("schedule.list", json!({"workflow_id": f.workflow}))
        .await;
    assert_eq!(list["result"]["schedules"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn webhook_crud() {
    let f = Fixture::new().await;

    // workflow 不存在 → -32011
    let missing = f
        .call("webhook.create", json!({"workflow_id": "nope"}))
        .await;
    assert_eq!(missing["error"]["code"], -32011, "{missing}");

    let ok = f
        .call("webhook.create", json!({"workflow_id": f.workflow}))
        .await;
    let webhook = &ok["result"];
    let token = webhook["token"].as_str().unwrap().to_string();
    assert!(!token.is_empty(), "{ok}");
    assert_eq!(webhook["workflow_id"], f.workflow);
    assert_eq!(webhook["enabled"], true);

    let list = f
        .call("webhook.list", json!({"workflow_id": f.workflow}))
        .await;
    assert_eq!(list["result"]["webhooks"].as_array().unwrap().len(), 1);

    // set_enabled
    let ok = f
        .call(
            "webhook.set_enabled",
            json!({"token": token, "enabled": false}),
        )
        .await;
    assert_eq!(ok["result"]["updated"], true, "{ok}");
    let list = f
        .call("webhook.list", json!({"workflow_id": f.workflow}))
        .await;
    assert_eq!(list["result"]["webhooks"][0]["enabled"], false);
    let missing = f
        .call(
            "webhook.set_enabled",
            json!({"token": "nope", "enabled": true}),
        )
        .await;
    assert_eq!(missing["error"]["code"], -32011, "{missing}");

    // delete
    let missing = f.call("webhook.delete", json!({"token": "nope"})).await;
    assert_eq!(missing["error"]["code"], -32011, "{missing}");
    let ok = f.call("webhook.delete", json!({"token": token})).await;
    assert_eq!(ok["result"]["deleted"], true);
    let list = f
        .call("webhook.list", json!({"workflow_id": f.workflow}))
        .await;
    assert_eq!(list["result"]["webhooks"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn run_stats_counts_exactly_and_groups_by_workflow() {
    let f = Fixture::new().await;
    f.backend
        .workflow_publish(&f.workflow, 1, None)
        .await
        .unwrap();

    // 空库：total 0、by_status 空、by_workflow 空
    let empty = f.call("run.stats", json!({})).await;
    assert_eq!(empty["result"]["total"], 0, "{empty}");
    assert_eq!(empty["result"]["by_status"].as_object().unwrap().len(), 0);
    assert_eq!(empty["result"]["by_workflow"].as_array().unwrap().len(), 0);

    // 造数据：f.workflow 两个 run（succeeded + running），另一个 workflow 一个 succeeded
    let r1 = f
        .call("run.start", json!({"workflow_id": f.workflow}))
        .await;
    let run1 = r1["result"]["run_id"].as_str().unwrap().to_string();
    f.wait_finished(&run1).await; // succeeded
    f.arm
        .create_run(CreateRun {
            workflow_id: f.workflow.clone(),
            version: None,
            input: Value::Null,
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    let wf2 = f.backend.workflow_create("other", None).await.unwrap();
    let wf2 = wf2.result["workflow_id"].as_str().unwrap().to_string();
    f.backend
        .workflow_update(&wf2, definition(9), None)
        .await
        .unwrap();
    f.backend.workflow_publish(&wf2, 1, None).await.unwrap();
    let r2 = f.call("run.start", json!({"workflow_id": wf2})).await;
    let run2 = r2["result"]["run_id"].as_str().unwrap().to_string();
    f.wait_finished(&run2).await;

    // 全局：total/by_status 精确，by_workflow 按 workflow 分组
    let all = f.call("run.stats", json!({})).await;
    let total = all["result"]["total"].as_i64().unwrap();
    assert_eq!(total, 3, "{all}");
    // journal 下第二个 run 真实执行完成（v1 直插 running 行的构造已不可用）
    assert_eq!(all["result"]["by_status"]["succeeded"], 3);
    let by_workflow = all["result"]["by_workflow"].as_array().unwrap();
    assert_eq!(by_workflow.len(), 2, "{all}");
    let group = |wf: &str| {
        by_workflow
            .iter()
            .find(|g| g["workflow_id"] == wf)
            .unwrap_or_else(|| panic!("缺少分组 {wf}：{all}"))
    };
    assert_eq!(group(&f.workflow)["total"], 2);
    assert_eq!(group(&f.workflow)["by_status"]["succeeded"], 2);
    assert_eq!(group(&wf2)["total"], 1);
    assert_eq!(group(&wf2)["by_status"]["succeeded"], 1);

    // 按 workflow 过滤：只回该 workflow 的计数，by_workflow 为空数组
    let one = f
        .call("run.stats", json!({"workflow_id": f.workflow}))
        .await;
    assert_eq!(one["result"]["total"], 2, "{one}");
    assert_eq!(one["result"]["by_status"]["succeeded"], 2);
    assert_eq!(one["result"]["by_workflow"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn run_source_attribution_and_filter() {
    let f = Fixture::new().await;
    f.backend
        .workflow_publish(&f.workflow, 1, None)
        .await
        .unwrap();

    // run.start → manual
    let r = f
        .call("run.start", json!({"workflow_id": f.workflow}))
        .await;
    let manual_run = r["result"]["run_id"].as_str().unwrap().to_string();
    f.wait_finished(&manual_run).await;
    let got = f.call("run.get", json!({"run_id": manual_run})).await;
    assert_eq!(got["result"]["run"]["source"], "manual", "{got}");
    assert!(got["result"]["run"]["source_detail"].is_null());

    // 调度器触发 → schedule + schedule id（直接驱动一轮 fire_due，不等 tick）
    let created = f
        .call(
            "schedule.create",
            json!({"workflow_id": f.workflow, "cron": "* * * * *"}),
        )
        .await;
    let schedule_id = created["result"]["id"].as_str().unwrap().to_string();
    flow_rpc::journal_triggers::fire_due(&f.backend, chrono::Local::now()).await;
    let list = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "source": "schedule"}),
        )
        .await;
    let runs = list["result"]["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1, "{list}");
    assert_eq!(runs[0]["source"], "schedule");
    assert_eq!(runs[0]["source_detail"], json!(schedule_id));

    // source 过滤：manual 只剩 run.start 那条；非法 source → -32602
    let list = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "source": "manual"}),
        )
        .await;
    assert_eq!(list["result"]["runs"].as_array().unwrap().len(), 1);
    let bad = f.call("run.list", json!({"source": "cron"})).await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
}

#[tokio::test]
async fn run_list_filters_by_status_and_paginates_by_cursor() {
    let f = Fixture::new().await;
    f.backend
        .workflow_publish(&f.workflow, 1, None)
        .await
        .unwrap();

    // 起 3 个 run 并等到终态（succeeded）
    let mut runs = Vec::new();
    for _ in 0..3 {
        let resp = f
            .call("run.start", json!({"workflow_id": f.workflow}))
            .await;
        let run_id = resp["result"]["run_id"].as_str().unwrap().to_string();
        f.wait_finished(&run_id).await;
        runs.push(run_id);
    }

    // status 过滤：succeeded 有 3 条，failed 为 0
    let ok = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "status": "succeeded"}),
        )
        .await;
    assert_eq!(ok["result"]["runs"].as_array().unwrap().len(), 3, "{ok}");
    let none = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "status": "failed"}),
        )
        .await;
    assert_eq!(none["result"]["runs"].as_array().unwrap().len(), 0);

    // 非法 status → -32602
    let bad = f.call("run.list", json!({"status": "bogus"})).await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");

    // 游标分页：limit=1 逐页翻，before_run_id 取上一页末尾
    let page1 = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "status": "succeeded", "limit": 1}),
        )
        .await;
    let p1 = page1["result"]["runs"].as_array().unwrap();
    assert_eq!(p1.len(), 1);
    let cursor = p1[0]["id"].as_str().unwrap();
    let page2 = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "status": "succeeded", "limit": 1, "before_run_id": cursor}),
        )
        .await;
    let p2 = page2["result"]["runs"].as_array().unwrap();
    assert_eq!(p2.len(), 1, "{page2}");
    assert_ne!(p2[0]["id"], cursor, "第二页必须更旧");
    let page3 = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "status": "succeeded", "limit": 1, "before_run_id": p2[0]["id"]}),
        )
        .await;
    assert_eq!(page3["result"]["runs"].as_array().unwrap().len(), 1);
    let page4 = f
        .call(
            "run.list",
            json!({"workflow_id": f.workflow, "status": "succeeded", "limit": 1, "before_run_id": page3["result"]["runs"][0]["id"]}),
        )
        .await;
    assert_eq!(
        page4["result"]["runs"].as_array().unwrap().len(),
        0,
        "翻到头应为空"
    );
}

// ---- 可复用节点模板 ----

fn fragment() -> (Value, Value) {
    (
        json!([
            {"id": "h", "type": "http_call", "name": "取数",
             "params": {"url": "https://example.com", "method": "GET"},
             "position": {"x": 10, "y": 20}},
            {"id": "s", "type": "script", "name": "解析",
             "params": {"code": "return input;"}, "position": {"x": 200, "y": 20}}
        ]),
        json!([{"source": "h", "target": "s"}]),
    )
}

#[tokio::test]
async fn template_crud_roundtrip_and_fragment_is_normalized() {
    let f = Fixture::new().await;
    let (nodes, edges) = fragment();
    let created = f
        .call(
            "template.create",
            json!({"name": "HTTP+解析", "category": "集成", "nodes": nodes, "edges": edges}),
        )
        .await;
    let template = &created["result"];
    assert!(!template["id"].as_str().unwrap().is_empty());
    assert_eq!(template["name"], "HTTP+解析");
    assert_eq!(template["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(template["edges"].as_array().unwrap().len(), 1);

    // list 只回信封不回载荷
    let list = f.call("template.list", json!({})).await;
    let entry = &list["result"]["templates"][0];
    assert_eq!(entry["name"], "HTTP+解析");
    assert_eq!(entry["node_count"], 2);
    assert!(entry.get("nodes").is_none(), "list 不回片段载荷");

    // get 回载荷
    let got = f.call("template.get", json!({"id": template["id"]})).await;
    assert_eq!(got["result"]["nodes"][0]["id"], "h");

    // 重名冲突 → -32012
    let conflict = f
        .call(
            "template.create",
            json!({"name": "HTTP+解析", "nodes": nodes, "edges": edges}),
        )
        .await;
    assert_eq!(conflict["error"]["code"], -32012);

    // 改名 + 清空分组
    let updated = f
        .call(
            "template.update",
            json!({"id": template["id"], "name": "标准取数", "category": null}),
        )
        .await;
    assert_eq!(updated["result"]["name"], "标准取数");
    assert_eq!(updated["result"]["category"], Value::Null);

    // 删除后 get → -32011
    let deleted = f
        .call("template.delete", json!({"id": template["id"]}))
        .await;
    assert_eq!(deleted["result"]["deleted"], true);
    let missing = f.call("template.get", json!({"id": template["id"]})).await;
    assert_eq!(missing["error"]["code"], -32011);
}

#[tokio::test]
async fn template_validation_rejects_bad_fragments() {
    let f = Fixture::new().await;
    let (nodes, edges) = fragment();

    // 未知类型
    let bad_type = f
        .call(
            "template.create",
            json!({"name": "t", "nodes": [{"id": "x", "type": "nope", "name": "x", "params": {}}], "edges": []}),
        )
        .await;
    assert_eq!(bad_type["error"]["code"], -32602);

    // 必填参数缺失（http_call 缺 url）
    let bad_params = f
        .call(
            "template.create",
            json!({"name": "t", "nodes": [{"id": "x", "type": "http_call", "name": "x", "params": {}}], "edges": []}),
        )
        .await;
    assert_eq!(bad_params["error"]["code"], -32602);

    // 边端点不在片段内
    let dangling = f
        .call(
            "template.create",
            json!({"name": "t", "nodes": nodes, "edges": [{"source": "ghost", "target": "s"}]}),
        )
        .await;
    assert_eq!(dangling["error"]["code"], -32602);

    // condition 之外的节点不允许 true/false 端口
    let bad_port = f
        .call(
            "template.create",
            json!({"name": "t", "nodes": nodes, "edges": [{"source": "h", "target": "s", "sourceHandle": "true"}]}),
        )
        .await;
    assert_eq!(bad_port["error"]["code"], -32602);

    // 空模板名
    let bad_name = f
        .call(
            "template.create",
            json!({"name": "  ", "nodes": nodes, "edges": edges}),
        )
        .await;
    assert_eq!(bad_name["error"]["code"], -32602);
}

// ---- 统一配置 RPC ----

#[tokio::test]
async fn config_get_update_roundtrip_writes_file_and_redacts_database_url() {
    let root = std::env::temp_dir().join(format!("flow-config-rpc-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let config_path = root.join("flow.toml");
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let state = Arc::new(AppState {
        backend: AnyBackend::Journal(backend.clone()),
        config: Some(flow_rpc::ConfigState {
            config: std::sync::RwLock::new(flow_config::Config::default()),
            path: Some(config_path.clone()),
            env_overrides: vec!["FLOW_ADDR".to_string()],
        }),
        secrets: None,
    });
    let f = Fixture {
        root: root.clone(),
        backend: backend.clone(),
        arm: AnyBackend::Journal(backend),
        state,
        workflow: String::new(),
    };

    // get：默认值 + env 覆盖名单 + 路径
    let resp = f.call("config.get", json!({})).await;
    assert_eq!(
        resp["result"]["config"]["journal"]["addr"],
        "127.0.0.1:9802"
    );
    assert_eq!(resp["result"]["env_overrides"].as_array().unwrap().len(), 1);

    // update：改 scheduler tick + database_url 脱敏
    let resp = f
        .call(
            "config.update",
            json!({"patch": {"server": {"scheduler_enabled": false, "journal_trigger_tick_secs": 30},
                   "storage": {"backend": "postgres", "data_dir": "data",
                               "database_url": "postgres://u:p@db/x"}}}),
        )
        .await;
    assert_eq!(
        resp["result"]["config"]["server"]["journal_trigger_tick_secs"],
        30
    );
    assert_eq!(
        resp["result"]["config"]["storage"]["database_url"], "<set>",
        "连接串必须脱敏"
    );

    // 文件真的写进去了（持久值，非脱敏形状）
    let text = std::fs::read_to_string(&config_path).unwrap();
    assert!(text.contains("postgres://u:p@db/x"));
    assert!(text.contains("journal_trigger_tick_secs = 30"));

    // "<set>" 回传表示保持原值：再改一次别的字段，database_url 不丢
    let resp = f
        .call(
            "config.update",
            json!({"patch": {"storage": {"backend": "postgres", "data_dir": "data2",
                                "database_url": "<set>"}}}),
        )
        .await;
    // database_url 回 "<set>" 说明仍非空
    assert_eq!(resp["result"]["config"]["storage"]["database_url"], "<set>");
    assert_eq!(resp["result"]["config"]["storage"]["data_dir"], "data2");
    let text = std::fs::read_to_string(&config_path).unwrap();
    assert!(
        text.contains("postgres://u:p@db/x"),
        "原连接串应保留：{text}"
    );

    // 非法 patch 被拒且不落盘
    let bad = f
        .call("config.update", json!({"patch": {"bogus": {}}}))
        .await;
    assert_eq!(bad["error"]["code"], -32602);

    let _ = std::fs::remove_dir_all(&root);
}
