//! flow-engine 集成测试基建。
//!
//! 历史上 `engine_recovery.rs`、`recovery_regressions.rs`、`sub_workflow.rs`
//! 各自写了一份几乎同构的 `Harness`（临时目录 + `Engine` + Drop 删目录），
//! `global_subscribe.rs` / `group_commit.rs` 又各写了一个 `temp_root()`——
//! 一份实现漂一次 GitHub issue。现在统一到这里：单点维护，`Drop` 保证失败
//! 路径不留垃圾。
//!
//! 约定（DESIGN.md §13）：每个用例一个独占临时目录，只删自己建的那个，
//! 绝不碰系统临时目录本身。
#![allow(dead_code)]

pub use flow_test_support::io::TempDir;

use std::sync::Arc;
use std::time::{Duration, Instant};

use flow_engine::{Definition, Engine, Event, EventLog, NoopObserver, RunState, StartRun};
use serde_json::{json, Value};
use uuid::Uuid;

/// 一个独占临时目录 + 一个 `Engine`。Drop 时整体删除目录。
pub struct Harness {
    pub dir: TempDir,
    pub engine: Arc<Engine>,
}

impl Harness {
    pub fn new() -> Harness {
        Harness::with_observer(Arc::new(NoopObserver))
    }

    /// 换一个 observer（观测广播面 / 状态上报的用例用）。
    pub fn with_observer(observer: Arc<dyn flow_engine::RunObserver>) -> Harness {
        let dir = TempDir::new("flow-engine-test");
        let engine = Arc::new(Engine::new(dir.path(), observer));
        Harness { dir, engine }
    }

    /// 确定性 run id（`run-<uuid7>`）。
    pub fn run_id() -> String {
        format!("run-{}", Uuid::now_v7())
    }

    /// 轮询到 run 进入终态（Driver 退出后才算：终态 + not live）。
    pub async fn wait_terminal(&self, run_id: &str) -> RunState {
        self.wait_terminal_within(run_id, Duration::from_secs(5))
            .await
    }

    pub async fn wait_terminal_within(&self, run_id: &str, timeout: Duration) -> RunState {
        let deadline = Instant::now() + timeout;
        loop {
            let state = self.engine.snapshot(run_id).await.expect("读取快照失败");
            if state.phase.is_terminal() && !self.engine.is_live(run_id) {
                return state;
            }
            assert!(
                Instant::now() < deadline,
                "run {run_id} 超时未结束：{:?}",
                describe(&state)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// 等到引擎里已经没有这个 run 的 Driver（恢复用的「第二个写者」守卫）。
    pub async fn wait_not_live(&self, run_id: &str) {
        self.wait_live_state(run_id, false).await;
    }

    pub async fn wait_live(&self, run_id: &str) {
        self.wait_live_state(run_id, true).await;
    }

    async fn wait_live_state(&self, run_id: &str, live: bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.engine.is_live(run_id) != live {
            assert!(
                Instant::now() < deadline,
                "run {run_id} 未在 5s 内变为 live={live}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// 手工写出一段「崩溃残留」的事件日志：模拟进程在节点执行中被杀死。
    pub async fn craft_partial_log(&self, run_id: &str, nodes_started: &[&str]) {
        let mut log = EventLog::create(self.dir.path(), run_id)
            .await
            .expect("创建事件日志失败");
        log.append(
            run_id,
            Event::RunStarted {
                workflow_id: "w1".into(),
                workflow_version: 1,
                input: json!({"amount": 21}),
                depth: 0,
            },
        )
        .await
        .expect("追加事件失败");
        for node_id in nodes_started {
            log.append(
                run_id,
                Event::NodeStarted {
                    node_id: (*node_id).to_string(),
                    attempt: 1,
                    child_run_id: None,
                },
            )
            .await
            .expect("追加事件失败");
        }
    }

    /// 在 run_id `r` 的日志前垫一段固定前缀（run_started + 第一个节点完成 +
    /// 第二个节点开始），再缀上调用方给的尾巴。
    ///
    /// 恢复测试的老套路：跳过引擎的写路径，直接摆一个「崩溃现场」让 resume_run
    /// 接手。恢复路径只认 run_id `r`，所以这里也写死成它。
    pub async fn prefix(&self, events: Vec<Event>) {
        let mut log = EventLog::create(self.dir.path(), "r")
            .await
            .expect("创建事件日志失败");
        for event in [
            Event::RunStarted {
                workflow_id: "w".into(),
                workflow_version: 1,
                input: Value::Null,
                depth: 0,
            },
            Event::NodeStarted {
                node_id: "s".into(),
                attempt: 1,
                child_run_id: None,
            },
            Event::NodeCompleted {
                node_id: "s".into(),
                attempt: 1,
                output: Value::Null,
                duration_ms: 0,
            },
            Event::NodeStarted {
                node_id: "n".into(),
                attempt: 1,
                child_run_id: None,
            },
        ]
        .into_iter()
        .chain(events)
        {
            log.append("r", event).await.expect("追加事件失败");
        }
    }

    /// 轮询到某个节点进入 Running（非法信号 / 等待类用例的观测助手）。
    pub async fn wait_node_running(&self, node: &str) {
        wait_node_running(&self.engine, "r", node).await
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

/// 便于 panic 信息里读：把 run 状态压成一行。
pub fn describe(state: &RunState) -> String {
    format!(
        "phase={:?} fatal_error={:?} output={:?}",
        state.phase, state.fatal_error, state.output
    )
}

pub fn def_from(json: Value) -> Definition {
    serde_json::from_value(json).expect("测试定义应可反序列化")
}

/// start → script(×2) → delay → end 的一条直线。
pub fn linear_def() -> Definition {
    def_from(json!({
        "nodes": [
            {"id": "n1", "type": "start", "name": "输入"},
            {"id": "n2", "type": "script", "name": "计算",
             "params": {"code": "return { doubled: input.amount * 2 };"}},
            {"id": "n3", "type": "delay", "name": "等待", "params": {"ms": 10}},
            {"id": "n4", "type": "end", "name": "输出"}
        ],
        "edges": [
            {"from": "n1", "to": "n2"},
            {"from": "n2", "to": "n3"},
            {"from": "n3", "to": "n4"}
        ]
    }))
}

/// start → <给定节点> → end，给「单个节点行为」的用例用。
pub fn node_def(node: Value) -> Definition {
    def_from(json!({
        "nodes": [{"id":"s", "type":"start"}, node, {"id":"e", "type":"end"}],
        "edges": [{"from":"s", "to":"n"}, {"from":"n", "to":"e"}]
    }))
}

/// 一条最小 start → script → end（与 ws_* 那一套同形）。
pub fn line_def(code: &str) -> Definition {
    def_from(json!({
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "n1", "type": "script", "params": {"code": code}},
            {"id": "end", "type": "end"}
        ],
        "edges": [
            {"from": "start", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    }))
}

/// 一个 start 出来的 `StartRun`：run_id / workflow_id 都固定成恢复测试友好的值。
pub fn spec(definition: Definition) -> StartRun {
    StartRun {
        run_id: "r".into(),
        workflow_id: "w".into(),
        workflow_version: 1,
        definition,
        input: Value::Null,
        depth: 0,
    }
}

/// start → script → end 的最简定义（`global_subscribe` 那一款）。
pub fn linear_definition() -> Definition {
    line_def("return 1;")
}

/// 终态轮询（recovery_regressions / sub_workflow 用的短版本）：终态 + not live。
pub async fn terminal(engine: &std::sync::Arc<Engine>, run_id: &str) -> RunState {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = engine.snapshot(run_id).await.expect("读取快照失败");
            if state.phase.is_terminal() && !engine.is_live(run_id) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("run 未在 5s 内结束")
}

/// 等到某个节点进入 Running（非法信号 / 等待类用例的观测助手）。
pub async fn wait_node_running(engine: &Arc<Engine>, run_id: &str, node: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = engine.snapshot(run_id).await.expect("读取快照失败");
            if matches!(
                state.record(node).state,
                flow_engine::NodeState::Running { .. }
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("节点未在 5s 内进入 Running")
}
