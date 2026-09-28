//! backend-perf：后端性能压测 harness 的入口。
//!
//! 两种模式：
//! - 正常模式：解析参数 → 对每个「后端 × 场景」起一个被测进程上下文测量 →
//!   打印文本报告，可选另存 JSON；
//! - 自举服务模式（`FLOW_PERF_SERVE=1`）：进程被 harness 当作被测 flow-server
//!   拉起，跑 [`flow_rpc::run_from_env`]——与 flow-server 二进制逐字同一份实现。
//!   自举而不是去 target 目录找别人的 bin：跨 package 拿不到 bin，也不该假设
//!   target 目录布局。
//!
//! 存储上下文（SQLite 独占临时目录 / Postgres docker 测试库、SIGKILL、panic
//! 路径清理）复用 backend-e2e 的 harness（[`backend_e2e::common`]）。

mod harness;
mod opts;
mod report;
mod scenarios;

use std::time::Instant;

use backend_e2e::common::{Ctx, Kind};
use futures::FutureExt;
use serde_json::json;

use crate::opts::{BackendSel, Opts, Scenario};
use crate::report::{Report, Suite};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("FLOW_PERF_SERVE").is_some() {
        // 与 flow-server 同形态（sqlite 单线程 runtime / postgres 多线程）：
        // 压测量的就是生产形态，不给 sqlite 模式多线程的开挂值
        let runtime = if flow_rpc::prefer_current_thread_runtime() {
            tokio::runtime::Builder::new_current_thread()
        } else {
            tokio::runtime::Builder::new_multi_thread()
        }
        .enable_all()
        .build()?;
        if let Err(err) = runtime.block_on(flow_rpc::run_from_env()) {
            eprintln!("被测服务进程异常退出：{err}");
            std::process::exit(1);
        }
        return Ok(());
    }
    // 压测 harness 本体：负载生成需要并行，保持多线程 runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(perf_main());
    Ok(())
}

async fn perf_main() {
    if std::env::var_os("FLOW_PERF_SERVE").is_some() {
        if let Err(err) = flow_rpc::run_from_env().await {
            eprintln!("被测服务进程异常退出：{err}");
            std::process::exit(1);
        }
        return;
    }

    let opts = Opts::parse(std::env::args().skip(1));
    let bin = std::env::current_exe()
        .expect("拿不到自身可执行文件路径")
        .to_string_lossy()
        .into_owned();

    let kinds: Vec<Kind> = match opts.backend {
        BackendSel::Both => vec![Kind::Sqlite, Kind::Postgres],
        BackendSel::Sqlite => vec![Kind::Sqlite],
        BackendSel::Postgres => vec![Kind::Postgres],
    };

    let mut reports = Vec::new();
    for kind in kinds {
        for &scenario in &opts.scenarios {
            let started = Instant::now();
            println!(
                "▶ backend={} · {} 开始（{}）",
                kind.name(),
                scenario.name(),
                chrono::Local::now().format("%H:%M:%S")
            );
            let measured = run_one(scenario, kind, &bin, &opts).await;
            for report in &measured {
                report.print();
            }
            println!("   用时 {:.1}s\n", started.elapsed().as_secs_f64());
            reports.extend(measured);
        }
    }

    let suite = Suite {
        meta: json!({
            "tool": "backend-perf",
            "generated_at": chrono::Local::now().to_rfc3339(),
            "opts": {
                "runs": opts.runs,
                "concurrency": opts.concurrency,
                "prefill": opts.prefill,
                "iterations": opts.iterations,
                "subscribe_runs": opts.subscribe_runs,
                "recovery_runs": opts.recovery_runs,
            },
        }),
        reports,
    };
    match opts.json.as_deref() {
        Some("-") => println!("{}", suite.to_json()),
        Some(path) => {
            suite
                .save_json(path)
                .unwrap_or_else(|err| panic!("写报告 {path} 失败：{err}"));
            println!("报告已写入 {path}");
        }
        None => {}
    }
}

/// 跑一个「后端 × 场景」：起被测进程上下文 → 测量 → 保证清理。panic 也先清理
/// 再恢复 unwind（与 backend-e2e 的 run_case 同一条纪律）：绝不漏被测进程、
/// 测试库或临时目录。
async fn run_one(scenario: Scenario, kind: Kind, bin: &str, opts: &Opts) -> Vec<Report> {
    let env = opts.server_env();
    let env_refs: Vec<(&str, &str)> = env
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let mut ctx = Ctx::start_with(kind, bin, &env_refs).await;
    let outcome = std::panic::AssertUnwindSafe(scenarios::run(scenario, &mut ctx, opts))
        .catch_unwind()
        .await;
    let cleanup = std::panic::AssertUnwindSafe(ctx.finish()).catch_unwind().await;
    match outcome {
        Ok(reports) => {
            if let Err(payload) = cleanup {
                std::panic::resume_unwind(payload);
            }
            reports
        }
        Err(payload) => {
            if let Err(cleanup_payload) = cleanup {
                std::panic::resume_unwind(cleanup_payload);
            }
            std::panic::resume_unwind(payload)
        }
    }
}
