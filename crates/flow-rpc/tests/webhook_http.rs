//! webhook HTTP 入口（POST /hook/:token）的端到端契约：
//! 200 触发 + 归因、未知/禁用 token 404、无 published 409、非法 JSON 400。

use std::path::PathBuf;
use std::sync::Arc;

use flow_backend::{AnyBackend, SqliteBackend};
use flow_rpc::{webhook, AppState};
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
    backend: Arc<SqliteBackend>,
    base: String,
    client: reqwest::Client,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-webhook-{}", uuid::Uuid::now_v7()));
        let backend = Arc::new(
            SqliteBackend::open(&root, root.join("flow.db"))
                .await
                .unwrap(),
        );
        let state = Arc::new(AppState::new(AnyBackend::Sqlite(backend.clone())));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, webhook::router(state)).await.unwrap();
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
        let wf = self.backend.create_workflow("t").await.unwrap();
        if publish {
            let v = self
                .backend
                .update_workflow(
                    &wf,
                    &json!({
                        "nodes": [
                            {"id": "s", "type": "start"},
                            {"id": "n", "type": "script", "params": {"code": "return 1;"}},
                            {"id": "e", "type": "end"}
                        ],
                        "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
                    }),
                )
                .await
                .unwrap();
            self.backend.publish(&wf, v).await.unwrap();
        }
        wf
    }

    async fn post(&self, token: &str, body: &str) -> (u16, Value) {
        let resp = self
            .client
            .post(format!("{}/hook/{token}", self.base))
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        (resp.status().as_u16(), resp.json().await.unwrap())
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
    let hook = f.backend.create_webhook(&wf).await.unwrap();

    let (status, body) = f.post(&hook.token, r#"{"src":"hook"}"#).await;
    assert_eq!(status, 200, "{body}");
    let run_id = body["run_id"].as_str().unwrap();

    // run 归因：source=webhook，detail=token；input 透传 body
    let run = f.backend.get_run(run_id).await.unwrap();
    assert_eq!(run.source, "webhook");
    assert_eq!(run.source_detail.as_deref(), Some(hook.token.as_str()));
    assert_eq!(run.input, json!({"src": "hook"}));

    // 未知 token 与禁用 token 同为 404（不区分，避免探测）
    let (status, _) = f.post("deadbeef", "{}").await;
    assert_eq!(status, 404);
    f.backend
        .set_webhook_enabled(&hook.token, false)
        .await
        .unwrap();
    let (status, _) = f.post(&hook.token, "{}").await;
    assert_eq!(status, 404);

    // 无 published 版本 → 409
    let wf2 = f.workflow(false).await;
    let hook2 = f.backend.create_webhook(&wf2).await.unwrap();
    let (status, _) = f.post(&hook2.token, "{}").await;
    assert_eq!(status, 409);

    // body 非法 JSON → 400（先恢复 hook 为启用）
    f.backend
        .set_webhook_enabled(&hook.token, true)
        .await
        .unwrap();
    let (status, _) = f.post(&hook.token, "not json").await;
    assert_eq!(status, 400);
}
