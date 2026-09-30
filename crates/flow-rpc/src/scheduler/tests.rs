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

/// 撤销去重行后，同一触发点可被重新取得——这是 `create_run` 遇瞬时故障时
/// 不丢火的机制（`fire_one` 的 Err 分支）。撤销前重复 tick 拿不到触发权，
/// 撤销后拿得到。
#[tokio::test]
async fn revoked_fire_can_be_reclaimed_within_the_same_minute() {
    let f = Fixture::new().await;
    let wf = f.workflow(true).await;
    let schedule = f
        .backend
        .create_schedule(&wf, "* * * * *", None, true)
        .await
        .unwrap();

    // 与 fire_one 相同的触发点计算：最近一次 <= now 的整分点
    let now = Local::now();
    let fire_at = CronSchedule::parse(&schedule.cron_expr)
        .unwrap()
        .previous_before(&(now + chrono::Duration::seconds(1)))
        .unwrap()
        .with_timezone(&Utc);

    // 首次拿到触发权
    assert!(f
        .backend
        .try_insert_fire(&schedule.id, fire_at)
        .await
        .unwrap());
    // 未撤销时拿不到（这正是重复 tick 被去重挡住的机制）
    assert!(!f
        .backend
        .try_insert_fire(&schedule.id, fire_at)
        .await
        .unwrap());

    // 撤销后可再次取得 —— 瞬时故障重试依赖这条
    f.backend.delete_fire(&schedule.id, fire_at).await.unwrap();
    assert!(
        f.backend
            .try_insert_fire(&schedule.id, fire_at)
            .await
            .unwrap(),
        "撤销去重行后应能重新取得触发权，否则该分钟的火永久丢失"
    );

    // 幂等：撤销不存在的行不报错
    f.backend.delete_fire(&schedule.id, fire_at).await.unwrap();
    f.backend.delete_fire(&schedule.id, fire_at).await.unwrap();
}

/// 去重行只影响自己那个触发点：撤销 A 不得让 B 的触发权失效。
#[tokio::test]
async fn revoking_one_fire_leaves_other_minutes_untouched() {
    let f = Fixture::new().await;
    let wf = f.workflow(true).await;
    let schedule = f
        .backend
        .create_schedule(&wf, "* * * * *", None, true)
        .await
        .unwrap();

    let base = Utc::now();
    let first = base;
    let second = base + chrono::Duration::minutes(1);
    assert!(f
        .backend
        .try_insert_fire(&schedule.id, first)
        .await
        .unwrap());
    assert!(f
        .backend
        .try_insert_fire(&schedule.id, second)
        .await
        .unwrap());

    f.backend.delete_fire(&schedule.id, first).await.unwrap();

    assert!(
        f.backend
            .try_insert_fire(&schedule.id, first)
            .await
            .unwrap(),
        "被撤销的那个应可重取"
    );
    assert!(
        !f.backend
            .try_insert_fire(&schedule.id, second)
            .await
            .unwrap(),
        "未撤销的那一分钟不得被重取（否则会重复触发）"
    );
}
