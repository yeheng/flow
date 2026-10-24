//! 桌面端的集成测试：验证内嵌后端在**没有 RPC 监听器**的情况下
//! 依然能跑完整的工作流生命周期。
//!
//! 这组用例守的是桌面形态的三个特有契约：
//!
//! - 进程内调用与 WebSocket 调用**行为一致**（同一方法模块）；
//! - 会话与窗口绑定，窗口关闭即中止订阅；
//! - 同一数据目录**不允许第二个实例**（目录锁保证单写者）。
use crate::service::{Host, Service};
use serde_json::{json, Value};
use std::sync::Arc;

/// 测试前置：默认执行模式已切 ipc（unix），测试二进制位于 deps/ 目录，
/// locate() 找不到同目录兄弟 flow-executor——注入主工作区构建产物。
/// 编译期拼接路径（env!("CARGO_MANIFEST_DIR") 向上两级 = 主工作区根）。
fn ensure_executor_bin() {
    if std::env::var_os("FLOW_EXECUTOR_BIN").is_some() {
        return;
    }
    let candidate = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join("target/release/flow-executor"));
    if let Some(path) = candidate.filter(|p| p.is_file()) {
        std::env::set_var("FLOW_EXECUTOR_BIN", path);
    }
}

/// 在宿主 runtime 上发起一次调用并取回响应。
///
/// 用 `unwrap` 是有意的：调用失败说明桌面端装配本身坏了，测试应当
/// 直接崩在根因上，而不是继续断言后续步骤。
async fn call(host: &Arc<Host>, service: Service, method: &str, params: Value) -> Value {
    let services = host.services.clone();
    let method = method.to_string();
    host.runtime
        .spawn(async move { services.request(service, method, params).await.unwrap().0 })
        .await
        .unwrap()
}

/// 最小的合法工作流定义（start → end）。
fn definition() -> Value {
    json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]})
}

/// 端到端：创建 → 更新 → 发布 → 启动 → 订阅到完成 → 关闭 → 重开仍在。
///
/// 最后一段（重开后数据仍在）是重点：桌面端的权威在 SQLite，
/// 进程重启不能丢任何已提交的工作流。
#[tokio::test]
async fn embedded_flow_runs_streams_and_persists_without_a_rpc_listener() {
    ensure_executor_bin();
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
    let id = created["result"]["result"]["workflow_id"]
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
    assert_eq!(updated["result"]["result"]["version"], 1);
    assert!(call(
        &host,
        Service::Flow,
        "workflow.publish",
        json!({"workflow_id":id,"version":1})
    )
    .await
    .get("error")
    .is_none());
    let started = call(
        &host,
        Service::Flow,
        "run.start",
        json!({"workflow_id":id,"input":{"hello":"desktop"}}),
    )
    .await;
    assert!(
        started["result"]["result"]["run_id"].is_string(),
        "{started}"
    );
    let run = started["result"]["result"]["run_id"].clone();
    let service = host.services.clone();
    let subscribe_run = run.clone();
    let (_, mut events) = host
        .runtime
        .spawn(async move {
            service
                .request(
                    Service::Flow,
                    "run.subscribe".into(),
                    json!({"run_id": subscribe_run, "event_format": "v2"}),
                )
                .await
                .unwrap()
        })
        .await
        .unwrap();
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
    let record = call(&host, Service::Flow, "run.get.view", json!({"run_id":run})).await;
    assert_eq!(record["result"]["run"]["status"], "succeeded");
    host.shutdown().unwrap();
    drop(host);
    let reopened = Host::start(root.path().to_path_buf()).unwrap();
    let list = call(&reopened, Service::Flow, "workflow.list.view", json!({})).await;
    assert_eq!(list["result"]["workflows"][0]["workflow_id"], id);
    reopened.shutdown().unwrap();
}

/// 幂等与令牌：写命令按 `request_id` 去重，且渲染层自带的 token 被忽略。
///
/// `_token: "wrong-browser-token"` 断言的是「服务端注入的令牌胜出」——
/// 桌面端不接受渲染层提供的网络凭据。
#[tokio::test]
async fn journal_uses_separate_authority_and_native_token() {
    ensure_executor_bin();
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
    // 主服务与 journal 工作区同源（v2 唯一权威）：journal 创建的
    // 工作流对 Flow 服务立即可见
    let flow_list = call(&host, Service::Flow, "workflow.list.view", json!({})).await;
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

/// 会话与窗口绑定：关窗即中止订阅，迟到订阅被拒，单写者锁生效。
#[tokio::test]
async fn sessions_cancel_on_close_reload_and_reject_late_subscriptions() {
    ensure_executor_bin();
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
