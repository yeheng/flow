use crate::service::{Host, Service};
use serde_json::{json, Value};
use std::sync::Arc;

async fn call(host: &Arc<Host>, service: Service, method: &str, params: Value) -> Value {
    let services = host.services.clone();
    let method = method.to_string();
    host.runtime
        .spawn(async move { services.request(service, method, params).await.unwrap().0 })
        .await
        .unwrap()
}
fn definition() -> Value {
    json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]})
}

#[tokio::test]
async fn embedded_flow_runs_streams_and_persists_without_a_rpc_listener() {
    let root = tempfile::tempdir().unwrap();
    let host = Host::start(root.path().to_path_buf()).unwrap();
    let error = call(&host, Service::Flow, "missing.method", json!({})).await;
    assert_eq!(error["error"]["code"], -32601);
    let created = call(
        &host,
        Service::Flow,
        "workflow.create",
        json!({"name":"desktop"}),
    )
    .await;
    let id = created["result"]["workflow_id"]
        .as_str()
        .unwrap()
        .to_string();
    let updated = call(
        &host,
        Service::Flow,
        "workflow.update",
        json!({"workflow_id":id,"definition":definition()}),
    )
    .await;
    assert_eq!(updated["result"]["version"], 1);
    assert!(call(
        &host,
        Service::Flow,
        "workflow.publish",
        json!({"workflow_id":id,"version":1})
    )
    .await
    .get("error")
    .is_none());
    let service = host.services.clone();
    let (_, mut events) = host
        .runtime
        .spawn(async move {
            service
                .request(Service::Flow, "run.subscribe".into(), json!({}))
                .await
                .unwrap()
        })
        .await
        .unwrap();
    let started = call(
        &host,
        Service::Flow,
        "run.start",
        json!({"workflow_id":id,"input":{"hello":"desktop"}}),
    )
    .await;
    assert!(started["result"]["run_id"].is_string(), "{started}");
    let run = started["result"]["run_id"].clone();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event: Value = serde_json::from_str(events.recv().await.unwrap().get()).unwrap();
            if event.to_string().contains("run_completed") {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(events);
    let record = call(&host, Service::Flow, "run.get", json!({"run_id":run})).await;
    assert_eq!(record["result"]["run"]["status"], "succeeded");
    host.shutdown().unwrap();
    drop(host);
    let reopened = Host::start(root.path().to_path_buf()).unwrap();
    let list = call(&reopened, Service::Flow, "workflow.list", json!({})).await;
    assert_eq!(list["result"]["workflows"][0]["workflow_id"], id);
    reopened.shutdown().unwrap();
}

#[tokio::test]
async fn journal_uses_separate_authority_and_native_token() {
    let root = tempfile::tempdir().unwrap();
    let host = Host::start(root.path().to_path_buf()).unwrap();
    let reply = call(&host, Service::Journal, "workflow.create", json!({"name":"journal desktop", "request_id":"create-once", "_token":"wrong-browser-token"})).await;
    assert_eq!(reply["result"]["committed"], true, "{reply}");
    let replay = call(
        &host,
        Service::Journal,
        "workflow.create",
        json!({"name":"journal desktop", "request_id":"create-once"}),
    )
    .await;
    assert_eq!(reply["result"]["result"], replay["result"]["result"]);
    // M4 后主服务与 journal 工作区同源（v2 唯一权威）：journal 创建的
    // 工作流对 Flow 服务立即可见
    let flow_list = call(&host, Service::Flow, "workflow.list", json!({})).await;
    assert_eq!(
        flow_list["result"]["workflows"].as_array().unwrap().len(),
        1,
        "同一 journal 权威：{flow_list}"
    );
    host.shutdown().unwrap();
    drop(host);
    let reopened = Host::start(root.path().to_path_buf()).unwrap();
    let list = call(
        &reopened,
        Service::Journal,
        "workflow.list",
        json!({"limit":64}),
    )
    .await;
    assert_eq!(
        list["result"]["values"].as_array().unwrap().len(),
        1,
        "{list}"
    );
    reopened.shutdown().unwrap();
}

#[tokio::test]
async fn sessions_cancel_on_close_reload_and_reject_late_subscriptions() {
    let root = tempfile::tempdir().unwrap();
    let host = Host::start(root.path().to_path_buf()).unwrap();
    let session = host.sessions.open("main".into());
    assert!(host.sessions.check(&session, "other").is_err());
    let task = host.runtime.spawn(std::future::pending::<()>());
    host.sessions
        .insert(&session, "main", "sub".into(), task.abort_handle())
        .unwrap();
    host.sessions.close_window("main");
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(host.sessions.check(&session, "main").is_err());
    let late = host.runtime.spawn(std::future::pending::<()>());
    assert!(host
        .sessions
        .insert(&session, "main", "late".into(), late.abort_handle())
        .is_err());
    late.abort();
    // A second desktop instance must never recover/write the same data directory.
    assert!(Host::start(root.path().to_path_buf()).is_err());
    host.shutdown().unwrap();
}
