//! 压测公共件：定义构造、提交打点、订阅事件到达时刻、信号交付。
//!
//! 与 backend-e2e 的分工：测试助手（连接、call、subscribe、wait_*）直接复用
//! backend-e2e 的 `common`；这里只加「测量」需要的东西——打点必须廉价、
//! 计时口径统一（`Instant`，单机同源时钟），以及测量型场景共用的并发提交。
//!
//! 计时口径约定：
//! - `Mark.call`：run.start 请求发出前；
//! - `Mark.returned`：run.start RPC 响应返回后（提交已确认）；
//! - `Arrivals.first/terminal`：对应事件到达客户端的时刻。
//!
//! 「落账 → 推送到达」的延迟没有外部可观测的 0 点，统一用
//! `arrival - Mark.returned` 代替（事件必然产生于提交之后）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use backend_e2e::common::fixtures::timeline_node;
use backend_e2e::common::{
    call, call_json, subscribe, try_call_json, wait_run_terminal, Conn, TIMEOUT,
};
use futures::StreamExt;

use serde_json::{json, Value};
use uuid::Uuid;

/// 预热 run 数：少量 run 走完全程（JS 沙箱、日志路径都暖起来），不进统计。
pub const WARMUP_RUNS: usize = 4;

/// 订阅建立到接收端就位的等待（全局流是纯实时增量，DESIGN §9：订阅注册略晚于
/// RPC 返回，开跑前先等接收端就位，否则开头的事件不在流里）。

/// Duration → 毫秒（报告里的计数器统一 ms 浮点）。
pub fn ms(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1000.0
}

/// 预热一个 run：提交并等终态，返回 run_id。
pub async fn warm_up_run(client: &Conn, workflow_id: &str) -> String {
    let run_id = start_one(client, workflow_id, &json!({ "x": 1 })).await;
    wait_run_terminal(client, &run_id, TIMEOUT).await;
    run_id
}

/// 提交一个 run（失败即 panic）：预热、非测量路径用。
async fn start_one(client: &Conn, workflow_id: &str, input: &Value) -> String {
    let value: Value = call(
        client,
        "run.start",
        json!({ "workflow_id": workflow_id, "input": input }),
    )
    .await;
    value["run_id"]
        .as_str()
        .expect("run.start 未回 run_id")
        .to_string()
}

// ---- 定义构造 ----

/// 串行链：start → script(1..depth) → end。吃事件写入路径与逐节点调度。
pub fn chain_def(depth: usize) -> Value {
    assert!(depth > 0, "链深必须 > 0");
    let mut nodes = vec![json!({"id": "start", "type": "start", "name": "开始"})];
    let mut edges = Vec::new();
    let mut prev = "start".to_string();
    for index in 0..depth {
        let id = format!("s{}", index + 1);
        nodes.push(json!({
            "id": id, "type": "script", "name": format!("脚本{}", index + 1),
            "params": {"code": format!("return {index};")}
        }));
        edges.push(json!({"from": prev, "to": id}));
        prev = id;
    }
    nodes.push(json!({"id": "end", "type": "end", "name": "结束"}));
    edges.push(json!({"from": prev, "to": "end"}));
    json!({ "nodes": nodes, "edges": edges })
}

/// 并行扇出：start → script(1..width) 全并行 → end（AND-join 汇合）。
/// 吃并发调度与汇合判定。
pub fn fanout_def(width: usize) -> Value {
    assert!(width > 0, "扇出宽度必须 > 0");
    let mut nodes = vec![json!({"id": "start", "type": "start", "name": "开始"})];
    let mut edges = Vec::new();
    for index in 0..width {
        let id = format!("p{}", index + 1);
        nodes.push(json!({
            "id": id, "type": "script", "name": format!("分支{}", index + 1),
            "params": {"code": format!("return {index};")}
        }));
        edges.push(json!({"from": "start", "to": id}));
        edges.push(json!({"from": id, "to": "end"}));
    }
    nodes.push(json!({"id": "end", "type": "end", "name": "结束"}));
    json!({ "nodes": nodes, "edges": edges })
}

/// 唯一名称（工作流名要避开重复，避免版本历史影响测量）。
pub fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::now_v7().simple())
}

// ---- 提交打点 ----

/// 一个 run 的提交打点。
#[derive(Debug, Clone, Copy)]
pub struct Mark {
    pub call: Instant,
    pub returned: Instant,
}

/// 并发提交时各任务写入的打点表（run_id → 打点）+ 提交失败记录。
///
/// 提交失败不 panic：错误率本身就是压测结论（比如 SQLite 后端在高并发
/// run.start 下的 `database is locked`），由场景把它写进报告计数器。
#[derive(Debug, Default, Clone)]
pub struct Marks {
    marks: Arc<Mutex<HashMap<String, Mark>>>,
    errors: Arc<Mutex<Vec<String>>>,
    /// 未结束的提交尝试数（成功或失败都递减）：订阅收集器靠它判断
    /// 「提交全部结束、所有提交成功的 run 都到终态」——提交失败的 run
    /// 永远不会有终态事件，等下去只会白等到超时。
    pending: Arc<AtomicUsize>,
    /// 在飞 run 数（已提交未到终态）。提交侧按 `max_in_flight` 限流：
    /// 「并发 N」的定义是同时在飞 N 个 run（k6 的 VU 模型），不是一次性
    /// 开闸放水——否则延迟分布量到的是排队时间，不是执行延迟。
    in_flight: Arc<AtomicUsize>,
    target: usize,
    max_in_flight: usize,
}

impl Marks {
    /// `count` 次提交尝试的打点表，同时在飞最多 `max_in_flight` 个 run
    /// （与 [`start_runs_measured`] 配套）。
    pub fn starting(count: usize, max_in_flight: usize) -> Marks {
        Marks {
            pending: Arc::new(AtomicUsize::new(count)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            target: count,
            max_in_flight: max_in_flight.max(1),
            ..Marks::default()
        }
    }

    /// 计划提交的 run 数。
    pub fn target(&self) -> usize {
        self.target
    }

    /// 尚未结束的提交尝试数。
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    /// 同时在飞的 run 数。
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// 在飞上限。
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// 成功提交的 run 数。
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// 提交失败次数（每条对应一个没起来的 run）。
    pub fn error_count(&self) -> usize {
        self.errors.lock().expect("错误表锁中毒").len()
    }

    /// 提交失败明细（诊断用）。
    pub fn errors(&self) -> Vec<String> {
        self.errors.lock().expect("错误表锁中毒").clone()
    }

    /// 升序 run_id（读场景按序轮询目标用）。
    pub fn run_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.lock().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// 全量打点快照。
    pub fn snapshot(&self) -> HashMap<String, Mark> {
        self.lock().clone()
    }

    /// 最早的 run.start 调用时刻（吞吐窗口起点）。
    pub fn earliest_call(&self) -> Instant {
        self.lock()
            .values()
            .map(|mark| mark.call)
            .min()
            .expect("打点表为空")
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Mark>> {
        self.marks.lock().expect("打点表锁中毒")
    }

    fn record_error(&self, error: String) {
        self.errors.lock().expect("错误表锁中毒").push(error);
        self.pending.fetch_sub(1, Ordering::SeqCst);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    fn finish_attempt(&self) {
        self.pending.fetch_sub(1, Ordering::SeqCst);
    }

    fn begin_attempt(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
    }

    /// 一个 run 到终态：释放一个在飞额度（订阅收集器调）。
    pub fn complete_run(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 提交失败给报告用的短摘要（只取首条，报告不捾完整错误列表）。
pub fn error_digest(marks: &Marks) -> String {
    let errors = marks.errors();
    errors
        .first()
        .map(|first| format!("共 {} 次，首条：{first}", errors.len()))
        .unwrap_or_else(|| "0 次".to_string())
}

/// 并发发起 `marks.target()` 个 run.start，打点与提交失败记录写进 `marks`。
///
/// 在飞限流：同时最多 `marks.max_in_flight()` 个 run 处于「已提交未到终态」
/// （额度由 [`collect_arrivals`] 在终态事件到达时释放）。提交侧最多并发
/// `max_in_flight` 个请求，避免客户端自己成为压测瓶颈。
///
/// 调用路径就是客户端 `run.start` 的普通用法（具名参数），不给被测端任何
/// 测量专用通道；失败不 panic，由场景决定报错还是计入计数器。限流等待以
/// `timeout` 兜底：收集器停了也只会多提交，不会把提交侧卡死。
pub async fn start_runs_measured(
    client: &Conn,
    workflow_id: &str,
    input: &Value,
    marks: &Marks,
    timeout: Duration,
) {
    let gate_deadline = Instant::now() + timeout;
    futures::stream::iter(0..marks.target())
        .map(|_| {
            let marks = marks.clone();
            async move {
                while marks.in_flight() >= marks.max_in_flight() && Instant::now() < gate_deadline {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                marks.begin_attempt();
                let call_at = Instant::now();
                let started = try_call_json(client, "run.start", json!({
                    "workflow_id": workflow_id, "input": input, "request_id": Uuid::now_v7().to_string()
                })).await;
                let returned = Instant::now();
                match started {
                    Ok(value) => {
                        let run_id = value["result"]["run_id"]
                            .as_str()
                            .expect("run.start 未回 run_id")
                            .to_string();
                        marks.lock().insert(
                            run_id,
                            Mark {
                                call: call_at,
                                returned,
                            },
                        );
                        marks.finish_attempt();
                    }
                    Err(err) => marks.record_error(format!("{err}")),
                }
            }
        })
        .buffer_unordered(marks.max_in_flight())
        .for_each(|()| async {})
        .await;
}

// ---- 订阅事件到达 ----

/// 各 run 订阅上观测到的事件到达时刻（按 run_id 归并）。
#[derive(Debug, Default)]
pub struct Arrivals {
    pub first: HashMap<String, Instant>,
    pub terminal: HashMap<String, Instant>,
    pub completed: usize,
    pub failed: usize,
    pub events: usize,
}

impl Arrivals {
    /// 验收：必须收满 `expected` 个 run 的终态事件，且没有失败收尾。
    pub fn assert_complete(&self, expected: usize, what: &str) {
        assert_eq!(
            self.failed, 0,
            "{what}: 有 run 未成功收尾（failed/cancelled）"
        );
        assert_eq!(
            self.terminal.len(),
            expected,
            "{what}: 期望 {expected} 个 run 的终态事件，实收 {}（事件总数 {}）",
            self.terminal.len(),
            self.events
        );
    }
}

/// 消费全局订阅流，记录每个 run 的首条 / 终态事件到达时刻。收满 `expect`
/// 个终态事件、或 `marks` 里提交成功的 run 全部到终态、或 `timeout` 用尽即
/// 返回（没收到的缺口由调用方 `assert_complete` 报）。
///
/// run.start 返回后为每个 run 建立订阅，从头回放，再接收实时增量。
pub async fn collect_arrivals(
    client: &Conn,
    marks: &Marks,
    expect: usize,
    timeout: Duration,
) -> Arrivals {
    let mut streams = futures::stream::SelectAll::new();
    let mut subscribed = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + timeout;
    // 空闲窗口：没有新事件时也要定期回来看追平条件（提交可能刚结束）
    let idle = Duration::from_millis(5);
    let mut arrivals = Arrivals::default();
    loop {
        // 追平：提交全部结束，且收满 expect 或所有提交成功的 run 都到终态
        if marks.pending() == 0
            && (arrivals.terminal.len() >= expect || arrivals.terminal.len() >= marks.len())
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break; // 超时：缺口交给 assert_complete 报
        }
        for run in marks.run_ids() {
            if subscribed.insert(run.clone()) {
                streams.push(subscribe(client, &run).await);
            }
        }
        let window = deadline.min(tokio::time::Instant::now() + idle);
        let received = tokio::select! {
            item = streams.next(), if !streams.is_empty() => Ok(item),
            _ = tokio::time::sleep_until(window) => Err(()),
        };
        match received {
            Ok(Some(Ok(envelope))) => {
                let Some(run_id) = envelope["run_id"].as_str() else {
                    continue;
                };
                let run_id = run_id.to_string();
                let arrived = Instant::now();
                arrivals.events += 1;
                arrivals.first.entry(run_id.clone()).or_insert(arrived);
                match envelope["type"].as_str() {
                    Some("run_completed") => {
                        arrivals.terminal.insert(run_id, arrived);
                        arrivals.completed += 1;
                        marks.complete_run();
                    }
                    Some("run_failed") | Some("run_cancelled") => {
                        arrivals.terminal.insert(run_id, arrived);
                        arrivals.failed += 1;
                        marks.complete_run();
                    }
                    _ => {}
                }
            }
            Ok(Some(Err(_))) => break,
            Ok(None) => {} // 订阅流出错 / 结束
            Err(_) => {}   // 空闲窗口，回去重查追平条件
        }
    }
    arrivals
}

// ---- 人工节点与信号 ----

/// 轮询 run.timeline 等节点进入 `running`（human_task 停驻点），返回时间线。
pub async fn wait_node_running(
    client: &Conn,
    run_id: &str,
    node_id: &str,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)]
    let mut timeline = Value::Null;
    loop {
        timeline = call_json(client, "run.timeline", json!({ "run_id": run_id })).await;
        if timeline_node(&timeline, node_id)["state"] == json!("running") {
            return timeline;
        }
        assert!(
            Instant::now() < deadline,
            "等待 {run_id}.{node_id} 进入 running 超时：{timeline}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 交付 Journal 信号，返回命令提交与投影可见的耗时。
pub async fn deliver_signal(
    client: &Conn,
    run_id: &str,
    node_id: &str,
    payload: Value,
) -> Duration {
    let started = Instant::now();
    let ack: Value = call(
        client,
        "run.signal",
        json!({ "run_id": run_id, "node_id": node_id, "payload": payload }),
    )
    .await;
    assert_eq!(ack["delivered"], json!(true), "信号提交回执：{ack}");
    started.elapsed()
}
