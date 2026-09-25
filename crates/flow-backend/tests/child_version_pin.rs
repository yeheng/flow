//! 回归：崩溃重放的子 run 必须沿用 runs 行里已钉的版本与输入，
//! 不得重新解析 latest published——否则重启前发布的新版会让事件日志
//!（权威）与元数据分叉，resume_run 身份校验判 LogCorrupted，run 永久不可恢复。

use std::sync::Arc;

use flow_engine::{ChildRunLauncher, DbRunStatus, Engine, NoopObserver};
use flow_store::Store;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn replayed_child_run_keeps_pinned_version() {
    let root = std::env::temp_dir().join(format!("flow-childpin-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(Store::open(root.join("flow.db")).await.unwrap());
    let engine = Arc::new(Engine::new(&root, Arc::new(NoopObserver)));

    let wf = store.create_workflow("child-wf").await.unwrap();
    let def_v1 = json!({
        "nodes": [{"id": "s", "type": "start"}, {"id": "e", "type": "end"}],
        "edges": [{"from": "s", "to": "e"}]
    });
    let v1 = store.update_workflow(&wf, &def_v1).await.unwrap();
    store.publish(&wf, v1).await.unwrap();

    // 重放窗口内发布的新版：重放路径必须无视它
    let def_v2 = json!({
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "x", "type": "script", "params": {"code": "return 1;"}},
            {"id": "e", "type": "end"}
        ],
        "edges": [{"from": "s", "to": "x"}, {"from": "x", "to": "e"}]
    });
    let v2 = store.update_workflow(&wf, &def_v2).await.unwrap();
    store.publish(&wf, v2).await.unwrap();
    assert_ne!(v1, v2);

    // 崩溃窗口：runs 行已插入（钉 v1 + input），首事件未落盘
    let child_id = format!("child-{}", Uuid::now_v7());
    let input = json!({"amount": 21});
    store
        .insert_run(
            &child_id,
            &wf,
            v1,
            &input,
            DbRunStatus::Initializing.as_str(),
        )
        .await
        .unwrap();

    let launcher = flow_backend::LocalChildLauncher::new(store.clone(), engine.clone());
    launcher
        .start(&child_id, &wf, input.clone(), 1)
        .await
        .unwrap();

    // 日志（权威）与 runs 行（元数据）必须一致地钉在 v1
    let state = engine.snapshot(&child_id).await.unwrap();
    assert_eq!(
        state.workflow_version,
        Some(v1),
        "子 run 日志未沿用钉死版本"
    );
    assert_eq!(state.input, input);
    let row = store.get_run(&child_id).await.unwrap();
    assert_eq!(row.workflow_version, v1);

    let _ = std::fs::remove_dir_all(&root);
}
