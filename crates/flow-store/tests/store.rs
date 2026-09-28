use flow_store::Store;
use flow_test_support::io::TempDir;
use serde_json::json;

async fn store() -> (Store, TempDir) {
    let dir = TempDir::new("flow-store-test");
    let store = Store::open(dir.join("flow.db")).await.unwrap();
    (store, dir)
}

fn def(marker: &str) -> serde_json::Value {
    json!({
        "nodes": [{"id": "n1", "type": "start"}, {"id": "n2", "type": "end"}],
        "edges": [{"from": "n1", "to": "n2", "marker": marker}]
    })
}

#[tokio::test]
async fn versions_are_immutable_and_identical_saves_do_not_bump() {
    let (store, _dir) = store().await;
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
    // TempDir 的 Drop 负责删目录
}

#[tokio::test]
async fn only_published_versions_are_eligible_for_runs() {
    let (store, _dir) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v1 = store.update_workflow(&wf, &def("a")).await.unwrap();

    assert_eq!(store.latest_published(&wf).await.unwrap(), None);
    store.publish(&wf, v1).await.unwrap();
    assert_eq!(store.latest_published(&wf).await.unwrap(), Some(1));

    let v2 = store.update_workflow(&wf, &def("b")).await.unwrap();
    assert_eq!(
        store.latest_published(&wf).await.unwrap(),
        Some(1),
        "draft 不参与"
    );
    store.publish(&wf, v2).await.unwrap();
    assert_eq!(store.latest_published(&wf).await.unwrap(), Some(2));

    let err = store.publish(&wf, 99).await.unwrap_err();
    assert!(err.to_string().contains("99"), "{err}");
    // TempDir 的 Drop 负责删目录
}

#[tokio::test]
async fn workflow_with_runs_cannot_be_deleted() {
    let (store, _dir) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v = store.update_workflow(&wf, &def("a")).await.unwrap();

    store.delete_workflow(&wf).await.unwrap();
    assert!(store.list_workflows().await.unwrap().is_empty());

    let wf2 = store.create_workflow("流程2").await.unwrap();
    let v2 = store.update_workflow(&wf2, &def("a")).await.unwrap();
    store
        .insert_run("run-1", &wf2, v2, &json!({"x": 1}), "running", "manual", None)
        .await
        .unwrap();

    let err = store.delete_workflow(&wf2).await.unwrap_err();
    // 冲突必须是 Conflict 变体（跨后端错误码一致的契约面），
    // 不允许穿 NotFound 的皮把 conflict 撒成 not-found
    assert!(matches!(err, flow_store::StoreError::Conflict(_)), "{err}");
    assert!(err.to_string().contains("拒绝删除"), "{err}");
    let _ = (v, v2);
    // TempDir 的 Drop 负责删目录
}

#[tokio::test]
async fn unfinished_runs_drive_crash_recovery() {
    let (store, _dir) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v = store.update_workflow(&wf, &def("a")).await.unwrap();

    store
        .insert_run("run-live", &wf, v, &json!({"x": 1}), "running", "manual", None)
        .await
        .unwrap();
    store
        .insert_run("run-done", &wf, v, &json!({"x": 2}), "running", "manual", None)
        .await
        .unwrap();
    store
        .set_run_status(
            "run-done",
            "succeeded",
            Some(&json!({"ok": true})),
            None,
            None,
        )
        .await
        .unwrap();
    store
        .set_run_status("run-bad", "failed", None, Some("boom"), None)
        .await
        .unwrap_err(); // 未插入的 run 报错

    let unfinished = store.unfinished_runs().await.unwrap();
    let ids: Vec<&str> = unfinished.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["run-live"]);

    let done = store.get_run("run-done").await.unwrap();
    assert_eq!(done.status, "succeeded");
    assert_eq!(done.output, Some(json!({"ok": true})));
    assert!(done.ended_at.is_some(), "终态必须落结束时间");

    let live = store.get_run("run-live").await.unwrap();
    assert!(live.ended_at.is_none());
    assert_eq!(live.input, json!({"x": 1}));
    // TempDir 的 Drop 负责删目录
}

#[tokio::test]
async fn workflow_summary_reports_latest_and_published_versions() {
    let (store, _dir) = store().await;
    let wf = store.create_workflow("流程").await.unwrap();
    let v1 = store.update_workflow(&wf, &def("a")).await.unwrap();
    store.publish(&wf, v1).await.unwrap();
    store.update_workflow(&wf, &def("b")).await.unwrap();

    let list = store.list_workflows().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].latest_version, 2);
    assert_eq!(list[0].published_version, Some(1));
    assert_eq!(list[0].name, "流程");
    // TempDir 的 Drop 负责删目录
}

#[tokio::test]
async fn concurrent_saves_return_their_own_versions_and_deduplicate_identical_definitions() {
    let (store, _dir) = store().await;
    let store = std::sync::Arc::new(store);
    let workflow = store.create_workflow("concurrent").await.unwrap();
    for identical in [false, true] {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(32));
        let mut workers = Vec::new();
        for marker in 0..32 {
            let store = store.clone();
            let workflow = workflow.clone();
            let barrier = barrier.clone();
            workers.push(tokio::spawn(async move {
                let definition = def(&if identical {
                    "same".to_string()
                } else {
                    marker.to_string()
                });
                barrier.wait().await;
                let version = store.update_workflow(&workflow, &definition).await.unwrap();
                let stored = store.get_version(&workflow, Some(version)).await.unwrap();
                assert_eq!(stored.definition, definition);
                version
            }));
        }
        let mut versions = std::collections::HashSet::new();
        for worker in workers {
            versions.insert(worker.await.unwrap());
        }
        assert_eq!(versions.len(), if identical { 1 } else { 32 });
    }
    assert_eq!(
        store
            .latest_version(&workflow)
            .await
            .unwrap()
            .unwrap()
            .version,
        33
    );
    drop(store);
    // TempDir 的 Drop 负责删目录（assert 失败也不会漏垃圾）
}

#[tokio::test]
async fn status_updates_clear_resolved_errors() {
    let (store, _dir) = store().await;
    let w = store.create_workflow("w").await.unwrap();
    store.update_workflow(&w, &def("a")).await.unwrap();
    store
        .insert_run("r", &w, 1, &json!(null), "initializing", "manual", None)
        .await
        .unwrap();
    assert_eq!(store.unfinished_runs().await.unwrap().len(), 1);
    store
        .set_run_status(
            "r",
            "awaiting_resume",
            None,
            Some("needs adjudication"),
            None,
        )
        .await
        .unwrap();
    store
        .set_run_status("r", "running", None, None, None)
        .await
        .unwrap();
    assert!(store.get_run("r").await.unwrap().error.is_none());
    store
        .set_run_status("r", "succeeded", Some(&json!(7)), None, None)
        .await
        .unwrap();
    let row = store.get_run("r").await.unwrap();
    assert!(row.error.is_none());
    assert_eq!(row.output, Some(json!(7)));
    drop(store);
    // TempDir 的 Drop 负责删目录（assert 失败也不会漏垃圾）
}

/// 删除工作流后版本随级联消失；insert_run 必须在写锁内验证版本存在——
/// 否则 delete 的 COUNT 与 DELETE 之间能插进一个 run.start，造出引用已删
/// 版本的孤儿 run（runs 表无外键，数据库不会拦）。修复前这里插入成功。
#[tokio::test]
async fn run_cannot_reference_deleted_workflow_version() {
    let (store, _dir) = store().await;
    let wf = store.create_workflow("孤儿").await.unwrap();
    store.update_workflow(&wf, &def("a")).await.unwrap();
    store.delete_workflow(&wf).await.unwrap();

    let err = store
        .insert_run("r-orphan", &wf, 1, &json!(null), "initializing", "manual", None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("版本不存在"),
        "必须明确报版本缺失而不是插入孤儿：{err}"
    );
    // run 行确实没插进去
    assert!(store.get_run("r-orphan").await.is_err());

    drop(store);
    // TempDir 的 Drop 负责删目录
}
