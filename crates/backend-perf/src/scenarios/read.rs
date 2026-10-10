//! RPC 读路径延迟：预置一批终态 run 后，五种查询方法轮流各调 `iterations` 次，
//! 量单次调用的延迟分位数。
//!
//! 两个防偏差的安排：目标 run 按序轮询（不让某个 run 的日志长度差异偏向统计），
//! 五种方法交错执行（先测的暖、后测的冷不能算进方法本身）。数据量即 `--prefill`，
//! 场景独占进程上下文，读路径压的是「N 个 run 的索引 + 日志」这一档。

use std::time::Instant;

use backend_e2e::common::{call_json, publish_workflow, Ctx};
use serde_json::{json, Value};

use crate::harness::{
    chain_def, collect_arrivals, error_digest, start_runs_measured, unique_name, Marks,
};
use crate::opts::Opts;
use crate::report::{Latency, Report};

/// 读路径压的定义：3 层脚本链（每 run 5 个事件左右）。
const READ_CHAIN_DEPTH: usize = 3;
const METHODS: [&str; 5] = [
    "run.get.full",
    "run.list.full",
    "run.stats",
    "run.timeline",
    "run.events.full",
];

pub async fn run(ctx: &mut Ctx, opts: &Opts) -> Vec<Report> {
    let client = ctx.client().await;
    let (workflow_id, _) = publish_workflow(
        &client,
        &unique_name("perf-read"),
        chain_def(READ_CHAIN_DEPTH),
    )
    .await;

    // 预置：prefill 个 run 全部走到终态，读路径要压在「有数据」的索引上。
    let input = json!({ "x": 1 });
    let timeout = std::time::Duration::from_secs(opts.prefill as u64 / 2 + 60);
    let marks = Marks::starting(opts.prefill, opts.concurrency);
    let (_, arrivals) = tokio::join!(
        start_runs_measured(&client, &workflow_id, &input, &marks, timeout),
        collect_arrivals(&client, &marks, opts.prefill, timeout),
    );
    let started = marks.len();
    let submit_errors = marks.error_count();
    if submit_errors > 0 {
        eprintln!("⚠ rpc_read：预置提交失败（{}）", error_digest(&marks));
    }
    assert!(started > 0, "预置 run 全部失败：{}", error_digest(&marks));
    arrivals.assert_complete(started, "rpc_read/prefill");
    let run_ids = marks.run_ids();

    let mut latencies: Vec<(&str, Latency)> = METHODS
        .iter()
        .map(|method| (*method, Latency::default()))
        .collect();
    for index in 0..opts.iterations {
        let run_id = &run_ids[index % run_ids.len()];
        for (method, latency) in latencies.iter_mut() {
            let params = match *method {
                "run.list.full" => json!({ "limit": 50 }),
                "run.stats" => json!({}),
                _ => json!({ "run_id": run_id }),
            };
            let started = Instant::now();
            let _: Value = call_json(&client, method, params).await;
            latency.add(started.elapsed());
        }
    }

    let metrics = latencies
        .iter()
        .map(|(method, latency)| latency.stats(&metric_name(method)))
        .collect();
    vec![Report::new(
        "journal",
        "rpc_read",
        json!({
            "prefill_runs": opts.prefill,
            "iterations_per_method": opts.iterations,
            "run_list_limit": 50,
        }),
        json!({
            "prefill_started": started,
            "submit_errors": submit_errors,
            "methods": METHODS.len(),
        }),
    )
    .with_metrics(metrics)]
}

/// `run.get` → `run_get_ms`。
fn metric_name(method: &str) -> String {
    format!("{}_ms", method.replace('.', "_"))
}
