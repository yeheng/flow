//! 全局订阅面的 run_started 广播回归（DESIGN.md §9 订阅契约，两臂一致）：
//! run_started 写于 Driver 存在之前，但必须和其他事件一样进进程内广播——
//! Postgres 后端经共享日志 + NOTIFY 扇出，单机后端漏广播会让全局流从
//! node_started 才开始，两条订阅面对同一事件历史给出不同答案。

use std::sync::Arc;
use std::time::Duration;

use flow_engine::{Engine, NoopObserver};
use serde_json::json;

fn linear_definition() -> flow_engine::Definition {
    serde_json::from_value(json!({
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "n1", "type": "script", "params": {"code": "return 1;"}},
            {"id": "end", "type": "end"}
        ],
        "edges": [
            {"from": "start", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    }))
    .unwrap()
}

/// 在 start_run 之前订阅全局流：收到的第一条必须是 seq=1 的 run_started。
#[tokio::test]
async fn global_subscription_sees_run_started() {
    let dir = std::env::temp_dir().join(format!("flow-engine-sub-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let engine = Engine::new(&dir, Arc::new(NoopObserver));

    // 订阅先于 run 创建：run_started 落盘与广播都必须可达已注册的接收端
    let mut events = engine.subscribe();
    let run_id = format!("run-{}", uuid::Uuid::now_v7());
    engine
        .start_run(flow_engine::StartRun {
            run_id: run_id.clone(),
            workflow_id: "wf".into(),
            workflow_version: 1,
            definition: linear_definition(),
            input: json!({ "seed": 1 }),
            depth: 0,
        })
        .await
        .expect("启动 run 失败");

    let first = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("5s 内没收到事件")
        .expect("全局流提前结束");
    assert_eq!(first.seq, 1, "首条事件必须是 seq=1 的 run_started");
    assert_eq!(first.run_id, run_id);
    assert!(
        matches!(first.event, flow_engine::Event::RunStarted { .. }),
        "首条事件必须是 run_started，实际 {:?}",
        first.event.kind()
    );

    // 继续收到后续事件直到终态（广播不断流）
    let mut last_seq = first.seq;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let envelope = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("未收到 run_completed")
            .expect("全局流提前结束");
        assert_eq!(envelope.seq, last_seq + 1, "seq 严格连续");
        last_seq = envelope.seq;
        if matches!(envelope.event, flow_engine::Event::RunCompleted { .. }) {
            break;
        }
    }

    // 与事件日志逐条对齐：广播是日志的如实副本
    let logged = engine.read_events(&run_id, None).await.unwrap();
    assert_eq!(logged.len() as u64, last_seq);

    std::fs::remove_dir_all(&dir).ok();
}
