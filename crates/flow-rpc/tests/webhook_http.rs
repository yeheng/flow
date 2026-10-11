//! webhook HTTP 入口的端到端契约：
//! 200 触发 + 归因、非法 JSON 400。

use std::path::PathBuf;

use flow_backend::journal::JournalBackend;
use std::sync::Arc;
const TOKEN: &str = "flow-hook-test-token-at-least-32-bytes";
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
    backend: Arc<JournalBackend>,
    base: String,
    client: reqwest::Client,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-webhook-{}", uuid::Uuid::now_v7()));
        let backend = JournalBackend::open(&root, Default::default())
            .await
            .unwrap();
        let http_backend = backend.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                flow_rpc::journal_triggers::router(http_backend, TOKEN.into()),
            )
            .await
            .unwrap();
        });
        Self {
            root,
            backend,
            base: format!("http://{addr}"),
            client: reqwest::Client::new(),
        }
    }

    /// 建 workflow；publish=true 时写入并发布最简定义。
    async fn workflow(&self, publish: bool) -> String {
        let wf = self
            .backend
            .workflow_create("t", None)
            .await
            .unwrap()
            .result["workflow_id"]
            .as_str()
            .unwrap()
            .to_owned();
        if publish {
            self.backend.workflow_update(&wf,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
            self.backend.workflow_publish(&wf, 1, None).await.unwrap();
        }
        wf
    }
    async fn hook(&self, wf: &str) -> flow_dto::Webhook {
        let created = self
            .backend
            .product_command(
                "webhook.create",
                &json!({"workflow_id":wf}),
                &uuid::Uuid::now_v7().to_string(),
            )
            .await
            .unwrap();
        serde_json::from_value(created.result).unwrap()
    }
    async fn enable(&self, token: &str, enabled: bool) {
        self.backend
            .product_command(
                "webhook.set_enabled",
                &json!({"token":token,"enabled":enabled}),
                &uuid::Uuid::now_v7().to_string(),
            )
            .await
            .unwrap();
    }
    async fn post(&self, token: &str, body: &str) -> (u16, Value) {
        let resp = self
            .client
            .post(format!("{}/hooks/{token}", self.base))
            .header("Content-Type", "application/json")
            .bearer_auth(TOKEN)
            .header("Idempotency-Key", uuid::Uuid::now_v7().to_string())
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        (
            status,
            if body["committed"] == true {
                body["result"].clone()
            } else {
                body
            },
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn webhook_post_triggers_run_with_source_attribution() {
    let f = Fixture::new().await;
    let wf = f.workflow(true).await;
    let hook = f.hook(&wf).await;

    let (status, body) = f.post(&hook.token, r#"{"src":"hook"}"#).await;
    assert_eq!(status, 200, "{body}");
    let run_id = body["run_id"].as_str().unwrap();

    // run 归因：source=webhook，detail=token；input 透传 body
    let run = flow_backend::journal_views::get_run(&f.backend, run_id)
        .await
        .unwrap();
    assert_eq!(run.source, "webhook");
    assert_eq!(run.source_detail.as_deref(), Some(hook.token.as_str()));
    assert_eq!(run.input, json!({"src": "hook"}));

    // 未知 token 与禁用 token（不区分，避免探测）
    let (status, _) = f.post("deadbeef", "{}").await;
    assert_eq!(status, 400);
    f.enable(&hook.token, false).await;
    let (status, _) = f.post(&hook.token, "{}").await;
    assert_eq!(status, 400);

    // 无 published 版本
    let wf2 = f.workflow(false).await;
    let hook2 = f.hook(&wf2).await;
    let (status, _) = f.post(&hook2.token, "{}").await;
    assert_eq!(status, 400);

    // body 非法 JSON → 400（先恢复 hook 为启用）
    f.enable(&hook.token, true).await;
    let (status, _) = f.post(&hook.token, "not json").await;
    assert_eq!(status, 400);
}
