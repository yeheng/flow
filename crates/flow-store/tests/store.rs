use flow_store::{Store, RUN_FAILED, RUN_RUNNING, RUN_SUCCEEDED};
use serde_json::json;
use uuid::Uuid;

async fn store() -> (Store, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("flow-store-test-{}/flow.db", Uuid::now_v7()));
    let store = Store::open(&path).await.unwrap();
    (store, path)
}

fn def(marker: &str) -> serde_json::Value {
    json!({
        "nodes": [{"id": "n1", "type": "start"}, {"id": "n2", "type": "end"}],
        "edges": [{"from": "n1", "to": "n2", "marker": marker}]
    })
}

#[tokio::test]
async fn versions_are_immutable_and_identical_saves_do_not_bump() {
    let (store, path) = store().await;
    let wf = store.create_workflow("订单流程").await.unwrap();

    let v1 = store.update_workflow(&wf, &def("a")).await.unwrap();
    assert_eq!(v1, 1);

    // 编辑器重复保存同一份定义不应刷版本号
    let same = store.update_workflow(&wf, &def("a")).await.unwrap();
    assert_eq!(same, 1);

    let v2 = store.update_workflow(&wf, &def("b")).await.unwrap();
    assert_eq!(v2, 2);

    // v1 内容不被后续保存影响
    let old = store.get_version(&wf, Some(1)).await.unwrap();
    assert_eq!(old.definition["edges"][0]["marker"], json!("a"));
    assert_eq!(old.status, "draft");

    std::fs::remove_dir_all(path.parent().unwrap()).ok();
}

#[tokio::test]
async fn only_published_versions_are_eligible_for_runs() {
    let (store, path) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v1 = store.update_workflow(&wf, &def("a")).await.unwrap();

    assert_eq!(store.latest_published(&wf).await.unwrap(), None);
    store.publish(&wf, v1).await.unwrap();
    assert_eq!(store.latest_published(&wf).await.unwrap(), Some(1));

    let v2 = store.update_workflow(&wf, &def("b")).await.unwrap();
    assert_eq!(store.latest_published(&wf).await.unwrap(), Some(1), "draft 不参与");
    store.publish(&wf, v2).await.unwrap();
    assert_eq!(store.latest_published(&wf).await.unwrap(), Some(2));

    let err = store.publish(&wf, 99).await.unwrap_err();
    assert!(err.to_string().contains("99"), "{err}");

    std::fs::remove_dir_all(path.parent().unwrap()).ok();
}

#[tokio::test]
async fn workflow_with_runs_cannot_be_deleted() {
    let (store, path) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v = store.update_workflow(&wf, &def("a")).await.unwrap();

    store.delete_workflow(&wf).await.unwrap();
    assert!(store.list_workflows().await.unwrap().is_empty());

    let wf2 = store.create_workflow("流程2").await.unwrap();
    let v2 = store.update_workflow(&wf2, &def("a")).await.unwrap();
    store
        .insert_run("run-1", &wf2, v2, &json!({"x": 1}), RUN_RUNNING)
        .await
        .unwrap();

    let err = store.delete_workflow(&wf2).await.unwrap_err();
    assert!(err.to_string().contains("拒绝删除"), "{err}");
    let _ = (v, v2);

    std::fs::remove_dir_all(path.parent().unwrap()).ok();
}

#[tokio::test]
async fn unfinished_runs_drive_crash_recovery() {
    let (store, path) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v = store.update_workflow(&wf, &def("a")).await.unwrap();

    store
        .insert_run("run-live", &wf, v, &json!({"x": 1}), RUN_RUNNING)
        .await
        .unwrap();
    store
        .insert_run("run-done", &wf, v, &json!({"x": 2}), RUN_RUNNING)
        .await
        .unwrap();
    store
        .set_run_status("run-done", RUN_SUCCEEDED, Some(&json!({"ok": true})), None)
        .await
        .unwrap();
    store
        .set_run_status("run-bad", RUN_FAILED, None, Some("boom"))
        .await
        .unwrap_err(); // 未插入的 run 报错

    let unfinished = store.unfinished_runs().await.unwrap();
    let ids: Vec<&str> = unfinished.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["run-live"]);

    let done = store.get_run("run-done").await.unwrap();
    assert_eq!(done.status, RUN_SUCCEEDED);
    assert_eq!(done.output, Some(json!({"ok": true})));
    assert!(done.ended_at.is_some(), "终态必须落结束时间");

    let live = store.get_run("run-live").await.unwrap();
    assert!(live.ended_at.is_none());
    assert_eq!(live.input, json!({"x": 1}));

    std::fs::remove_dir_all(path.parent().unwrap()).ok();
}

#[tokio::test]
async fn workflow_summary_reports_latest_and_published_versions() {
    let (store, path) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v1 = store.update_workflow(&wf, &def("a")).await.unwrap();
    store.publish(&wf, v1).await.unwrap();
    store.update_workflow(&wf, &def("b")).await.unwrap();

    let list = store.list_workflows().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].latest_version, 2);
    assert_eq!(list[0].published_version, Some(1));
    assert_eq!(list[0].name, "流程");

    std::fs::remove_dir_all(path.parent().unwrap()).ok();
}
