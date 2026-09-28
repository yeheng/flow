//! 订阅推送延迟：run.start 返回后，事件多久到达订阅端。
//!
//! 口径：`push_first_event_ms` / `push_terminal_ms` 都以 run.start 的 RPC 响应
//! 返回为 0 点（提交已确认，事件必然产生在此后），量的是「落账 → 推送到达」
//! 在端到端上的净效果；`run_start_ms` 单独报告，便于把提交延迟从推送延迟里剥离。
//! 小并发（≤8）提交，模拟「少量用户盯着 run 看进度」而不是吞吐压力。
//!
//! 两后端的推送机制不同（SQLite 进程内广播 / Postgres 按 last_seq 追共享日志 +
//! LISTEN/NOTIFY），这里的指标就是给这个差异量个数。

use backend_e2e::common::{publish_workflow, subscribe, Ctx};
use serde_json::json;

use crate::harness::{
    chain_def, collect_arrivals, error_digest, start_runs_measured, unique_name, warm_up_run,
    Marks, SETTLE, WARMUP_RUNS,
};
use crate::opts::Opts;
use crate::report::{Latency, Report};

/// 推送延迟用最小定义（单脚本链），事件产生速率贴近「人工盯进度」。
const PUSH_CHAIN_DEPTH: usize = 2;
const MAX_SUBSCRIBE_CONCURRENCY: usize = 8;

pub async fn run(ctx: &mut Ctx, opts: &Opts) -> Vec<Report> {
    let client = ctx.client().await;
    let (workflow_id, _) = publish_workflow(
        &client,
        &unique_name("perf-subscribe"),
        chain_def(PUSH_CHAIN_DEPTH),
    )
    .await;

    // 预热后建立全局订阅（纯实时增量，DESIGN §9），等接收端就位再开跑。
    for _ in 0..WARMUP_RUNS {
        warm_up_run(&client, &workflow_id).await;
    }
    let sub_client = ctx.client().await;
    let mut sub = subscribe(&sub_client, None).await;
    tokio::time::sleep(SETTLE).await;

    let concurrency = opts.concurrency.min(MAX_SUBSCRIBE_CONCURRENCY);
    let input = json!({});
    let timeout = std::time::Duration::from_secs(opts.subscribe_runs as u64 + 60);
    let marks = Marks::starting(opts.subscribe_runs, concurrency);
    let (_, arrivals) = tokio::join!(
        start_runs_measured(&client, &workflow_id, &input, &marks, timeout),
        collect_arrivals(&mut sub, &marks, opts.subscribe_runs, timeout),
    );
    let started = marks.len();
    let submit_errors = marks.error_count();
    if submit_errors > 0 {
        eprintln!(
            "⚠ subscribe_latency：run.start 提交失败（{}）",
            error_digest(&marks)
        );
    }
    assert!(
        started > 0,
        "全部 run.start 都失败了：{}",
        error_digest(&marks)
    );
    arrivals.assert_complete(started, "subscribe_latency");

    let snapshot = marks.snapshot();
    let mut start_latency = Latency::default();
    let mut push_first = Latency::default();
    let mut push_terminal = Latency::default();
    for (run_id, mark) in &snapshot {
        let first = arrivals
            .first
            .get(run_id)
            .unwrap_or_else(|| panic!("run {run_id} 没有任何推送事件"));
        let done = arrivals
            .terminal
            .get(run_id)
            .unwrap_or_else(|| panic!("run {run_id} 没有终态推送事件"));
        start_latency.add(mark.returned - mark.call);
        // 首条事件可能与 run.start 响应几乎同时到达（服务端先推事件再回响应），
        // 饱和减法让它记为 0 而不是 panic
        push_first.add(first.saturating_duration_since(mark.returned));
        push_terminal.add(done.saturating_duration_since(mark.returned));
    }

    vec![Report::new(
        ctx.kind.name(),
        "subscribe_latency",
        json!({
            "runs": opts.subscribe_runs,
            "concurrency": concurrency,
            "warmup": WARMUP_RUNS,
        }),
        json!({
            "started": started,
            "submit_errors": submit_errors,
            "events": arrivals.events,
        }),
    )
    .with_metrics(vec![
        start_latency.stats("run_start_ms"),
        push_first.stats("push_first_event_ms"),
        push_terminal.stats("push_terminal_event_ms"),
    ])]
}
