use super::*;
use serde_json::json;
use std::sync::Arc;

use flow_backend::SqliteBackend;

struct Fixture {
    root: std::path::PathBuf,
    backend: AnyBackend,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flow-scheduler-{}", uuid::Uuid::now_v7()));
        let backend = SqliteBackend::open(&root, root.join("flow.db"))
            .await
            .unwrap();
        Self {
            root,
            backend: AnyBackend::Sqlite(Arc::new(backend)),
        }
    }

    /// 建 workflow；publish=true 时写入并发布一个最简定义。
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

    async fn run_count(&self, workflow_id: &str) -> usize {
        self.backend
            .list_runs(Some(workflow_id), None, None, None, 100)
            .await
            .unwrap()
            .len()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn due_schedule_fires_once_and_dedupes_repeated_ticks() {
    let f = Fixture::new().await;
    let wf = f.workflow(true).await;
    f.backend
        .create_schedule(&wf, "* * * * *", None, true)
        .await
        .unwrap();

    let now = Local::now();
    fire_due(&f.backend, now).await;
    assert_eq!(f.run_count(&wf).await, 1, "到期应触发一次");

    // 同一 now 重复 tick：去重不再触发
    fire_due(&f.backend, now).await;
    fire_due(&f.backend, now).await;
    assert_eq!(f.run_count(&wf).await, 1, "重复 tick 不得重复触发");
}

#[tokio::test]
async fn disabled_schedule_never_fires() {
    let f = Fixture::new().await;
    let wf = f.workflow(true).await;
    f.backend
        .create_schedule(&wf, "* * * * *", None, false)
        .await
        .unwrap();

    fire_due(&f.backend, Local::now()).await;
    assert_eq!(f.run_count(&wf).await, 0);
}

#[tokio::test]
async fn workflow_without_published_version_is_skipped() {
    let f = Fixture::new().await;
    let wf = f.workflow(false).await;
    f.backend
        .create_schedule(&wf, "* * * * *", None, true)
        .await
        .unwrap();

    // 不 panic、不报错、不产生 run
    fire_due(&f.backend, Local::now()).await;
    assert_eq!(f.run_count(&wf).await, 0);
}
