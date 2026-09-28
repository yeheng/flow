//! 崩溃恢复耗时：SIGKILL 被测进程后用同一份存储重启，量恢复与恢复后的推进能力。
//!
//! 口径（全部是墙钟）：
//! - `restart_empty_ms`：空存储 SIGKILL → 重启就绪（基线，不含恢复工作）；
//! - `restart_with_runs_ms`：N 个 run 停驻在 human_task 时 SIGKILL → 重启就绪，
//!   含 recover_unfinished 的全部工作（恢复发生在监听之前，就绪即恢复完成）；
//! - `recovery_overhead_ms`：两者之差 = 恢复 N 个未完成 run 的估算代价；
//! - `resume_wall_ms`：重启后把 N 个 run 全部用信号推进到终态的墙钟。
//!
//! 就绪判据是 harness 的 TCP 可连轮询。停驻点选 human_task：run 有完整日志、
//! 有未完成节点、没有自动推进的可能——SIGKILL 后不恢复就永远到不了终态。

use std::time::{Duration, Instant};

use backend_e2e::common::fixtures::human_def;
use backend_e2e::common::{call_json, publish_workflow, subscribe, Client, Ctx, TIMEOUT};
use serde_json::json;

use crate::harness::{
    collect_arrivals, deliver_signal, error_digest, ms, start_runs_measured, unique_name,
    wait_node_running, Marks, SETTLE,
};
use crate::opts::Opts;
use crate::report::{Latency, Report};

/// human_task 节点 id（[`human_def`] 的固定形状）。
const HUMAN_NODE: &str = "h";

pub async fn run(ctx: &mut Ctx, opts: &Opts) -> Vec<Report> {
    let is_pg = ctx.is_pg();

    // 基线：空存储重启。SIGKILL + 同一存储拉起 + 就绪，不含任何恢复工作。
    let empty_started = Instant::now();
    ctx.restart().await;
    let restart_empty = empty_started.elapsed();

    // 停驻 N 个 run：全部走到 human_task 并等待信号。
    let client = ctx.client().await;
    let (workflow_id, _) =
        publish_workflow(&client, &unique_name("perf-recovery"), human_def()).await;
    let marks = Marks::starting(opts.recovery_runs, opts.recovery_runs);
    start_runs_measured(
        &client,
        &workflow_id,
        &json!({}),
        &marks,
        Duration::from_secs(120),
    )
    .await;
    let parked = marks.len();
    let submit_errors = marks.error_count();
    if submit_errors > 0 {
        eprintln!(
            "⚠ crash_recovery：run.start 提交失败（{}）",
            error_digest(&marks)
        );
    }
    assert!(
        parked > 0,
        "全部 run.start 都失败了：{}",
        error_digest(&marks)
    );
    for run_id in marks.run_ids() {
        wait_node_running(&client, &run_id, HUMAN_NODE, TIMEOUT).await;
    }

    // 崩溃：SIGKILL + 同一存储重启（换端口，客户端稍后重连）。
    let with_runs_started = Instant::now();
    ctx.restart().await;
    let restart_with_runs = with_runs_started.elapsed();

    // 恢复验收：每个 run 必须回到 running 且重新活着（有驱动在推进）。
    let client = ctx.client().await;
    for run_id in marks.run_ids() {
        wait_live(&client, &run_id, TIMEOUT).await;
    }

    // 恢复后推进：逐个交付信号直到全部终态（交付耗时含 pending 追账）。
    let sub_client = ctx.client().await;
    let mut sub = subscribe(&sub_client, None).await;
    tokio::time::sleep(SETTLE).await;
    let run_ids = marks.run_ids();
    let resume_started = Instant::now();
    let (signal_latency, arrivals) = tokio::join!(
        signal_all(&client, is_pg, &run_ids),
        collect_arrivals(&mut sub, &marks, parked, Duration::from_secs(60)),
    );
    let resume_wall = resume_started.elapsed();
    arrivals.assert_complete(parked, "crash_recovery/resume");

    vec![Report::new(
        ctx.kind.name(),
        "crash_recovery",
        json!({
            "requested_runs": opts.recovery_runs,
            "concurrency": opts.concurrency,
        }),
        json!({
            "parked_runs": parked,
            "submit_errors": submit_errors,
            "resumed": arrivals.completed,
            "failed": arrivals.failed,
            "restart_empty_ms": ms(restart_empty),
            "restart_with_runs_ms": ms(restart_with_runs),
            "recovery_overhead_ms": ms(restart_with_runs.saturating_sub(restart_empty)),
            "resume_wall_ms": ms(resume_wall),
        }),
    )
    .with_metrics(vec![signal_latency.stats("signal_deliver_ms")])]
}

/// run.get 等到 run 重新活着且仍在 running（重启后恢复成功的验收）。
async fn wait_live(client: &Client, run_id: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)]
    let mut last = serde_json::Value::Null;
    loop {
        last = call_json(client, "run.get", json!({ "run_id": run_id })).await;
        if last["live"] == json!(true) && last["run"]["status"] == json!("running") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "run {run_id} 重启后未恢复为活跃 running：{last}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 串行交付 N 个信号，返回每个信号的交付耗时。
async fn signal_all(client: &Client, is_pg: bool, run_ids: &[String]) -> Latency {
    let mut latency = Latency::default();
    for run_id in run_ids {
        let spent = deliver_signal(
            client,
            is_pg,
            run_id,
            HUMAN_NODE,
            json!({ "approved_by": "perf" }),
        )
        .await;
        latency.add(spent);
    }
    latency
}
