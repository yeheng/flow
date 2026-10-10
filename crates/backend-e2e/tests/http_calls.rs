//! http_call 端到端语义：模板展开、成功/4xx 分类、挂起取消。
//!
//! 契约来源：DESIGN.md §11（http_call 语义）、§10（模板）。
//! 全部用本地 TcpListener stub——确定性，不依赖外部网络。
//! （v1 引擎的自动重试语义用例已随 flow-server 退役：v2 里外部操作失败
//! 一律 uncertain 等人工裁决，见 crash_recovery 的裁决用例。）

use backend_e2e::common::fixtures::{http_def, http_def_full, timeline_node, StubHttp};
use backend_e2e::common::{
    call_err, call_json, publish_workflow, start_run, wait_run_status, wait_run_terminal, Conn,
    Ctx, SHORT, TIMEOUT,
};
use backend_e2e::e2e_test;
use serde_json::{json, Value};

/// 从 stub 收到的请求里取请求行与某个头。
fn request_line(request: &str) -> &str {
    request.lines().next().unwrap_or_default()
}

fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request
        .lines()
        .skip(1)
        .find_map(|line| {
            line.split_once(':')
                .filter(|(k, _)| k.trim().eq_ignore_ascii_case(name))
        })
        .map(|(_, v)| v.trim())
}

e2e_test!(
    http_call_success_parses_json_and_expands_templates,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({ "ok": true, "n": 1 })).await;
        let definition = json!({
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "n1", "type": "script",
                 "params": {"code": "return { tag: input.trace };"}},
                {"id": "call", "type": "http_call", "params": {
                    "method": "POST",
                    "url": stub.url("/items/${input.id}"),
                    "headers": {"X-Trace": "${input.trace}"},
                    "body": {"amount": "${input.amount}", "tag": "${nodes.n1.tag}"}
                }},
                {"id": "end", "type": "end"}
            ],
            "edges": [
                {"from": "start", "to": "n1"},
                {"from": "n1", "to": "call"},
                {"from": "call", "to": "end"}
            ]
        });
        let client = ctx.client().await;
        let (workflow_id, _) = publish_workflow(&client, "模板", definition).await;
        let run_id = start_run(
            &client,
            &workflow_id,
            json!({ "id": 42, "trace": "t-1", "amount": 99 }),
        )
        .await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");

        // 节点输出：{status, headers, body}，body 能解析为 JSON 则解析（§11）
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let output = timeline_node(&timeline, "call")["output"].clone();
        assert_eq!(output["status"], json!(200));
        assert_eq!(output["body"], json!({ "ok": true, "n": 1 }));
        assert!(output["headers"].is_object());

        // 服务端真收到了展开后的请求：url / headers / body 三处模板全部生效
        let requests = stub.requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert_eq!(
            request_line(&requests[0]),
            "POST /items/42 HTTP/1.1",
            "url 模板展开"
        );
        assert_eq!(
            header(&requests[0], "X-Trace"),
            Some("t-1"),
            "headers 模板展开"
        );
    })
);

e2e_test!(http_call_4xx_is_fatal, |ctx: &mut Ctx| Box::pin(
    async move {
        let stub = StubHttp::fixed(404, json!({ "error": "not found" })).await;
        let client = ctx.client().await;
        let (workflow_id, _) =
            publish_workflow(&client, "404", http_def("GET", &stub.url("/x"))).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");

        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(timeline_node(&timeline, "call")["state"], json!("failed"));
        assert_eq!(
            timeline_node(&timeline, "call")["attempts"],
            json!(1),
            "4xx 是请求本身的问题：致命，不重试"
        );
        assert_eq!(stub.requests().len(), 1, "4xx 不得重试");

        let events: Value = call_json(&client, "run.events.full", json!({"run_id": run_id})).await;
        let failed = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == json!("node_failed"))
            .unwrap();
        assert_eq!(failed["retryable"], json!(false));
    }
));

e2e_test!(
    http_call_hang_then_cancel_marks_run_cancelled,
    |ctx: &mut Ctx| Box::pin(async move {
        // 只接受连接、永不响应：http_call 确定性地挂在请求里（§13 测试约定）
        let stub = StubHttp::hang().await;
        let client = ctx.client().await;
        let definition = http_def_full(json!({
            "method": "POST",
            "url": stub.url("/pay"),
            "body": {"amount": "${input.amount}"}
        }));
        let (workflow_id, _) = publish_workflow(&client, "挂起", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({ "amount": 7 })).await;

        // 等 node_started 落盘（副作用窗口已开）再取消
        let timeline = wait_node_state(&client, &run_id, "call", "running", SHORT).await;
        assert_eq!(timeline_node(&timeline, "call")["state"], json!("running"));

        let ack: Value = call_json(&client, "run.cancel", json!({"run_id": run_id})).await;
        assert_eq!(ack["delivered"], json!(true), "{ack}");
        let run = wait_run_status(&client, &run_id, "cancelled", TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("cancelled"));

        // 取消是用户主动选择：不判定外部副作用是否发生（§6.6）
        let events: Value = call_json(&client, "run.events.full", json!({"run_id": run_id})).await;
        let events = events["events"].as_array().unwrap();
        assert_eq!(events.last().unwrap()["type"], json!("run_cancelled"));
    })
);

e2e_test!(
    http_call_invalid_method_rejected_at_save,
    |ctx: &mut Ctx| Box::pin(async move {
        let client = ctx.client().await;
        let definition = http_def("FETCH", "http://127.0.0.1:1/x");
        let err = call_err(
            &client,
            "workflow.update",
            json!({"workflow_id": "any", "definition": definition}),
        )
        .await;
        assert_eq!(err.code(), -32602, "方法必须在白名单内：{err}");
    })
);

/// 轮询直到节点进入预期状态。
async fn wait_node_state(
    client: &Conn,
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
