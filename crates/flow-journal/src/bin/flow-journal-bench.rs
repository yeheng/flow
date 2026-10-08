//! flow-journal-bench：journal 写入基准（bin: flow-journal-bench）。
//!
//! 三档负载（low/mixed/burst）写入基准：控制/审计双队列 p99 延迟、
//! 组提交比、吞吐与校验。基准数据目录必须不存在（防误写生产数据）。
//!
//! 运行：cargo run -p flow-journal --bin flow-journal-bench -- <root>

use std::path::PathBuf;

use clap::Parser;

/// flow-journal-bench：journal 写入基准（目录必须不存在）。
#[derive(Parser)]
#[command(name = "flow-journal-bench", version, about = "journal 写入基准")]
struct Bench {
    /// 基准数据目录（必须为全新路径）
    root: PathBuf,
}

fn main() -> std::process::ExitCode {
    let bench = match Bench::try_parse() {
        Ok(args) => args,
        Err(error) => {
            let success = matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = error.print();
            std::process::exit(if success { 0 } else { 1 });
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("bench tokio runtime");
    let code = runtime
        .block_on(async move { bench_runs(bench.root).await })
        .unwrap_or_else(|err| {
            eprintln!("error: {err}");
            1
        });
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}

async fn bench_runs(root: PathBuf) -> Result<i32, Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};

    use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass};
    use serde_json::json;

    if root.exists() {
        return Err("benchmark directory must not already exist".into());
    }
    for (name, count, period) in [
        ("low", 20, Some(Duration::from_millis(100))),
        ("mixed", 2000, Some(Duration::from_millis(1))),
        ("burst", 1000, None),
    ] {
        let path = root.join(name);
        let journal = Journal::open(&path, JournalOptions::default()).await?;
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..count {
            if let Some(period) = period {
                tokio::time::sleep_until((start + period * i).into()).await;
            }
            let j = journal.clone();
            tasks.spawn(async move {
                let size = if name != "low" && i % 5 == 0 {
                    16 * 1024
                } else {
                    1024
                };
                let control = i % 5 == 0;
                let event = Event::new(
                    EventKind::Command,
                    json!({"producer":i.to_string(),"data":"x".repeat(size)}),
                );
                let started = Instant::now();
                let result = j
                    .submit(
                        format!("{name}-{i}"),
                        vec![event],
                        if control {
                            QueueClass::Control
                        } else {
                            QueueClass::Audit(format!("dispatch-{}", i % 32))
                        },
                    )
                    .await;
                (control, started.elapsed().as_secs_f64() * 1000.0, result)
            });
        }
        let mut control = Vec::new();
        let mut audit = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let (is_control, ms, commit) = result?;
            commit?;
            if is_control {
                control.push(ms)
            } else {
                audit.push(ms)
            }
        }
        let elapsed = start.elapsed().as_secs_f64();
        control.sort_by(f64::total_cmp);
        audit.sort_by(f64::total_cmp);
        let p99 = |v: &[f64]| v[(v.len() * 99 / 100).min(v.len() - 1)];
        let stats = journal.stats();
        journal.close().await?;
        let mut verified = 0;
        let report = flow_journal::scan(&path, |tx, _| {
            for e in &tx.events {
                if e.kind == EventKind::Command {
                    let i = e.payload["producer"]
                        .as_str()
                        .expect("producer recorded at submit")
                        .parse::<u32>()
                        .expect("producer is u32 decimal");
                    let size = if name != "low" && i % 5 == 0 {
                        16 * 1024
                    } else {
                        1024
                    };
                    assert_eq!(e.payload["data"].as_str().unwrap(), "x".repeat(size));
                    verified += 1;
                }
            }
            Ok(())
        })?;
        assert!(report.fault.is_none());
        assert_eq!(verified, count);
        let pass = p99(&control) <= 100.0
            && p99(&audit) <= 100.0
            && (name != "burst" || elapsed <= 5.0)
            && (name == "low" || stats.transactions as f64 / (stats.data_syncs - 1) as f64 >= 4.0);
        println!(
            "{}",
            json!({"load":name,"pass":pass,"seconds":elapsed,"tx_per_second":count as f64/elapsed,
            "encoded_mib_per_second":stats.encoded_bytes as f64/1048576.0/elapsed,
            "control_p99_ms":p99(&control),"audit_p99_ms":p99(&audit),"verified":verified,"stats":stats})
        );
        if !pass {
            return Err(format!("{name} failed target").into());
        }
    }
    Ok(0)
}
