//! 两代引擎语义对照（parity 安全网）。
//!
//! v1（event.jsonl + fold，DESIGN.md 语义权威）与 v2（JSONL journal +
//! journal_state reducer）必须对同一工作流产出相同的可观察结果。历史上
//! 两代各自演化出 6 处漂移（fail-fast、多 end 输出形状、skip reason、
//! awaiting_resume 词汇、retry 范围、子 run id 派生），且没有任何机制
//! 强制一致——本套件把「切换日不是行为变更日」焊成测试。
//!
//! 钉住的可观察面：
//! 1. 失败语义：节点失败不终结 run，独立分支跑完，失败分支下游跳过
//!    （reason=upstream_failed），全部终态后 run=failed（DESIGN §6.6）。
//! 2. 多 end 输出形状：全部 end 计入；被跳过的 end 记 null；多 end 恒为
//!    map（singular_or_map，§12.7），单 end 透传。
//! 3. skip reason 词汇：upstream_failed / upstream_skipped / branch_not_taken。
//! 4. 等待期 run 状态：正常业务等待（delay/human/retry）run=running；
//!    awaiting_resume 只属于不确定外部结果（平台故障面）。

use flow_backend::journal::JournalBackend;
use flow_backend::{CreateRun, SqliteBackend};
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::time::Duration;

fn temp(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "flow-parity-{tag}-{}",
        uuid::Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 失败分支 + 独立 delay 分支 + 两个 end（一个注定被跳过）。
const FAIL_AND_INDEPENDENT_BRANCH: &str = r#"{
    "nodes": [
        {"id":"s","type":"start"},
        {"id":"boom","type":"script","params":{"code":"throw new Error('kaboom');"}},
        {"id":"d","type":"delay","params":{"ms":50}},
        {"id":"e1","type":"end"},
        {"id":"e2","type":"end"}
    ],
    "edges": [
        {"from":"s","to":"boom"},
        {"from":"boom","to":"e1"},
        {"from":"s","to":"d"},
        {"from":"d","to":"e2"}
    ]
}"#;

/// condition 分支：真出口走通，假出口的 end 被 branch_not_taken 跳过。
const CONDITION_SKIPS_END: &str = r#"{
    "nodes": [
        {"id":"s","type":"start"},
        {"id":"c","type":"condition","params":{"expr":"input.go == 1"}},
        {"id":"e_a","type":"end"},
        {"id":"e_b","type":"end"}
    ],
    "edges": [
        {"from":"s","to":"c"},
        {"from":"c","to":"e_a","port":"true"},
        {"from":"c","to":"e_b","port":"false"}
    ]
}"#;

async fn v1_setup(dir: &std::path::Path, definition: &str) -> (SqliteBackend, String) {
    let backend = SqliteBackend::open(dir, dir.join("flow.db")).await.unwrap();
    backend.start().await.unwrap();
    let workflow_id = backend.create_workflow("parity").await.unwrap();
    backend
        .update_workflow(&workflow_id, &serde_json::from_str(definition).unwrap())
        .await
        .unwrap();
    backend.publish(&workflow_id, 1).await.unwrap();
    (backend, workflow_id)
}

async fn v1_wait_terminal(backend: &SqliteBackend, run_id: &str) -> flow_engine::RunState {
    loop {
        let state = backend.snapshot(run_id).await.unwrap();
        if state.phase.is_terminal() {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn v2_setup(
    dir: &std::path::Path,
    definition: &str,
) -> (std::sync::Arc<JournalBackend>, String) {
    let backend = JournalBackend::open(dir, JournalOptions::default())
        .await
        .unwrap();
    backend.start_execution().await.unwrap();
    let created = backend.workflow_create("parity", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    backend
        .workflow_update(&id, serde_json::from_str(definition).unwrap(), None)
        .await
        .unwrap();
    backend.workflow_publish(&id, 1, None).await.unwrap();
    (backend, id)
}

async fn v2_wait_terminal(
    backend: &std::sync::Arc<JournalBackend>,
    run_id: &str,
) -> flow_engine::journal_state::Run {
    loop {
        let run = backend.state().await.runs[run_id].clone();
        if run.terminal() {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// v2 侧：从 journal 事件流里取某节点 NodeSkipped 的 reason（payload 单一来源）。
fn v2_skip_reason(root: &std::path::Path, node_id: &str) -> Option<String> {
    let mut reason = None;
    flow_journal::scan(root, |tx, _| {
        for event in &tx.events {
            if matches!(event.kind, flow_journal::EventKind::NodeSkipped)
                && event.node_id.as_deref() == Some(node_id)
            {
                reason = event.payload["reason"].as_str().map(str::to_owned);
            }
        }
        Ok(())
    })
    .unwrap();
    reason
}

fn v1_skip_reason(state: &flow_engine::RunState, node_id: &str) -> Option<String> {
    match &state.record(node_id).state {
        flow_engine::NodeState::Skipped { reason } => Some(reason.clone()),
        _ => None,
    }
}

#[tokio::test]
async fn failure_semantics_and_branch_completion_match_v1() {
    let definition: Value = serde_json::from_str(FAIL_AND_INDEPENDENT_BRANCH).unwrap();

    // v1（权威语义）。
    let dir1 = temp("fail-v1");
    let (v1, flow1) = v1_setup(&dir1, FAIL_AND_INDEPENDENT_BRANCH).await;
    let created = v1
        .create_run(CreateRun {
            workflow_id: flow1.clone(),
            version: None,
            input: json!({}),
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    let v1_state = v1_wait_terminal(&v1, &created.run_id).await;
    assert!(matches!(v1_state.phase, flow_engine::RunPhase::Failed));
    assert!(matches!(
        v1_state.record("d").state,
        flow_engine::NodeState::Completed { .. }
    ));
    assert_eq!(
        v1_skip_reason(&v1_state, "e1").as_deref(),
        Some("upstream_failed")
    );

    // v2：同样的失败必须让独立分支跑完、下游跳过、全终态后收口 failed。
    let dir2 = temp("fail-v2");
    let (v2, flow2) = v2_setup(&dir2, FAIL_AND_INDEPENDENT_BRANCH).await;
    let created = v2
        .run_start(&flow2, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let v2_run = v2_wait_terminal(&v2, &run_id).await;
    assert_eq!(v2_run.status, "failed", "error={:?}", v2_run.error);
    assert_eq!(v2_run.nodes["d"].status, "succeeded", "独立分支必须跑完");
    assert_eq!(v2_run.nodes["e2"].status, "succeeded");
    assert_eq!(v2_run.nodes["e1"].status, "skipped");
    assert_eq!(
        v2_skip_reason(&dir2, "e1").as_deref(),
        Some("upstream_failed"),
        "skip reason 必须与 v1 同词"
    );
    let _ = definition;
    v1.shutdown().await.unwrap();
    v2.close().await.unwrap();
}

#[tokio::test]
async fn multi_end_output_shape_matches_v1() {
    // v1：真出口 end 有输出，假出口 end 被 branch_not_taken 跳过 →
    // run 输出 = {"e_a": …, "e_b": null}（多 end 恒为 map，skipped 记 null）。
    let dir1 = temp("shape-v1");
    let (v1, flow1) = v1_setup(&dir1, CONDITION_SKIPS_END).await;
    let created = v1
        .create_run(CreateRun {
            workflow_id: flow1.clone(),
            version: None,
            input: json!({"go": 1}),
            source: "manual".into(),
            source_detail: None,
        })
        .await
        .unwrap();
    let v1_state = v1_wait_terminal(&v1, &created.run_id).await;
    assert!(matches!(v1_state.phase, flow_engine::RunPhase::Succeeded));
    let v1_output = v1_state.output.clone().unwrap_or(serde_json::Value::Null);
    assert_eq!(
        v1_skip_reason(&v1_state, "e_b").as_deref(),
        Some("branch_not_taken")
    );

    // v2：同一工作流必须产出同形状（不允许 filter_map 丢 end 引起的
    // map→scalar 形状翻转）。
    let dir2 = temp("shape-v2");
    let (v2, flow2) = v2_setup(&dir2, CONDITION_SKIPS_END).await;
    let created = v2
        .run_start(&flow2, None, json!({"go": 1}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let v2_run = v2_wait_terminal(&v2, &run_id).await;
    assert_eq!(v2_run.status, "succeeded", "error={:?}", v2_run.error);
    assert_eq!(v2_run.nodes["e_b"].status, "skipped");
    assert_eq!(
        v2_skip_reason(&dir2, "e_b").as_deref(),
        Some("branch_not_taken")
    );

    // 形状对照：两侧 run 输出的键集与 null 位一致（e_a 透传值，e_b=null）。
    let root = v2.journal.root().to_path_buf();
    let upper = v2.journal.durable_lsn();
    let v2_output = match v2_run.output.clone().unwrap() {
        flow_journal::StoredValue::Inline(value) => value,
        flow_journal::StoredValue::Ref(reference) => tokio::task::spawn_blocking(move || {
            flow_journal::value::materialize(
                &root,
                upper,
                &flow_journal::StoredValue::Ref(reference),
                1024 * 1024,
            )
        })
        .await
        .unwrap()
        .unwrap(),
    };
    assert!(v1_output.is_object(), "v1 多 end 输出应为 map：{v1_output}");
    assert!(v2_output.is_object(), "v2 多 end 输出应为 map：{v2_output}");
    assert_eq!(v1_output["e_b"], json!(null), "v1 skipped end 记 null");
    assert_eq!(v2_output["e_b"], json!(null), "v2 skipped end 记 null");
    assert_eq!(
        v1_output["e_a"].to_string(),
        v2_output["e_a"].to_string(),
        "两侧透传值一致：v1={v1_output} v2={v2_output}"
    );
    v1.shutdown().await.unwrap();
    v2.close().await.unwrap();
}

#[tokio::test]
async fn normal_wait_keeps_run_running_in_v2() {
    // v1：delay/human 等待期间 run 状态保持 running（awaiting_resume 只属于
    // 平台故障隔离）。v2 词汇必须同义——曾把全部 WaitRegistered 投影成
    // awaiting_resume，与 v1 的故障语义撞词。
    let dir = temp("wait-v2");
    let (v2, flow) = v2_setup(
        &dir,
        r#"{
            "nodes": [
                {"id":"s","type":"start"},
                {"id":"d","type":"delay","params":{"ms":60000}},
                {"id":"e","type":"end"}
            ],
            "edges": [
                {"from":"s","to":"d"},
                {"from":"d","to":"e"}
            ]
        }"#,
    )
    .await;
    let created = v2
        .run_start(&flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let waiting = loop {
        let run = v2.state().await.runs[&run_id].clone();
        if run.nodes.get("d").is_some_and(|n| n.status == "waiting") {
            break run;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(
        waiting.status, "running",
        "正常 delay 等待期 run 必须是 running（awaiting_resume 是故障词汇）"
    );
    v2.run_cancel(&run_id, None).await.unwrap();
    v2.close().await.unwrap();
}
