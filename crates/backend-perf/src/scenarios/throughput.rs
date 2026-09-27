//! run 执行吞吐：并发提交 N 个 run 直到全部终态，量提交延迟、端到端延迟与 runs/s。
//!
//! 两种定义形态各测一遍：
//! - `chain`（串行链 depth=6）：吃事件写入路径与逐节点调度；
//! - `fanout`（8 路并行 + AND-join 汇合）：吃并发调度与汇合判定。
//!
//! 完成检测走全局订阅流（`run.subscribe`），不轮询 run.get——测量本身不给读路径
//! 加压。延迟口径：`run_start_ms` 是 run.start RPC 自身延迟；`e2e_terminal_ms` 是
//! 「run.start 发出 → 终态事件到达客户端」的端到端延迟。吞吐 =
//! runs / (首提交 → 最后一个终态到达)，不含预热。

use std::time::Duration;

use backend_e2e::common::{publish_workflow, subscribe, Client, Ctx};
use serde_json::{json, Value};

use crate::harness::{
    chain_def, collect_arrivals, error_digest, fanout_def, start_runs_measured, unique_name,
    warm_up_run, Marks, SETTLE, WARMUP_RUNS,
};
use crate::opts::Opts;
use crate::report::{Latency, Report};

const CHAIN_DEPTH: usize = 6;
const FANOUT_WIDTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Chain,
    Fanout,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Chain => "chain",
            Shape::Fanout => "fanout",
        }
    }

    fn definition(self) -> Value {
        match self {
            Shape::Chain => chain_def(CHAIN_DEPTH),
            Shape::Fanout => fanout_def(FANOUT_WIDTH),
        }
    }
}

pub async fn run(ctx: &mut Ctx, opts: &Opts) -> Vec<Report> {
    let client = ctx.client().await;
    let mut reports = Vec::new();
    for shape in [Shape::Chain, Shape::Fanout] {
        reports.push(measure(&client, ctx, opts, shape).await);
    }
    reports
}

async fn measure(client: &Client, ctx: &Ctx, opts: &Opts, shape: Shape) -> Report {
    let workflow_name = unique_name(&format!("perf-{}", shape.name()));
    let (workflow_id, _) = publish_workflow(client, &workflow_name, shape.definition()).await;

    // 预热：少量 run 走完全程（JS 沙箱、日志路径都暖起来），不进统计。
    // 全局订阅在预热之后才建立，预热事件不会混进测量流。
    for _ in 0..WARMUP_RUNS {
        warm_up_run(client, &workflow_id).await;
    }

    let sub_client = ctx.client().await;
    let mut sub = subscribe(&sub_client, None).await;
    // 全局流是纯实时增量（DESIGN §9）：订阅注册略晚于 RPC 返回，等接收端就位再开跑
    tokio::time::sleep(SETTLE).await;

    let input = json!({ "x": 1 });
    let timeout = Duration::from_secs(opts.runs as u64 / 2 + 60);
    let marks = Marks::starting(opts.runs, opts.concurrency);
    let (_, arrivals) = tokio::join!(
        start_runs_measured(client, &workflow_id, &input, &marks, timeout),
        collect_arrivals(&mut sub, &marks, opts.runs, timeout),
    );
    let started = marks.len();
    let submit_errors = marks.error_count();
    if submit_errors > 0 {
        eprintln!(
            "⚠ run_throughput/{}：run.start 提交失败（{}）",
            shape.name(),
            error_digest(&marks)
        );
    }
    assert!(started > 0, "全部 run.start 都失败了：{}", error_digest(&marks));
    arrivals.assert_complete(started, &format!("run_throughput/{}", shape.name()));

    let snapshot = marks.snapshot();
    let mut start_latency = Latency::default();
    let mut e2e = Latency::default();
    for (run_id, mark) in snapshot {
        let done = arrivals
            .terminal
            .get(&run_id)
            .unwrap_or_else(|| panic!("run {run_id} 没有终态到达时刻"));
        start_latency.add(mark.returned - mark.call);
        e2e.add(done.saturating_duration_since(mark.call));
    }
    let wall = arrivals
        .terminal
        .values()
        .max()
        .expect("无终态到达时刻")
        .saturating_duration_since(marks.earliest_call());
    let throughput = started as f64 / wall.as_secs_f64();

    Report::new(
        ctx.kind.name(),
        "run_throughput",
        json!({
            "shape": shape.name(),
            "definition": match shape {
                Shape::Chain => format!("chain(depth={CHAIN_DEPTH})"),
                Shape::Fanout => format!("fanout(width={FANOUT_WIDTH})"),
            },
            "runs": opts.runs,
            "concurrency": opts.concurrency,
            "warmup": WARMUP_RUNS,
        }),
        json!({
            "started": started,
            "submit_errors": submit_errors,
            "completed": arrivals.completed,
            "failed": arrivals.failed,
            "events": arrivals.events,
            "wall_ms": crate::harness::ms(wall),
            "throughput_runs_per_s": throughput,
        }),
    )
    .with_metrics(vec![
        start_latency.stats("run_start_ms"),
        e2e.stats("e2e_terminal_ms"),
    ])
}
