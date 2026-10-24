//! flow-journal-bench：journal 写入基准（bin: flow-journal-bench）。
//!
//! 三档负载（low/mixed/burst）写入基准：控制/审计双队列 p99 延迟、
//! 组提交比、吞吐与校验。基准数据目录必须不存在（防误写生产数据）。
//!
//! 运行：cargo run -p flow-journal --bin flow-journal-bench -- <root>
//!
//! 本工具是性能护栏：在改动写者之后，用同一组负载确认延迟与组提交比没有
//! 退化。
//! 核心不变量：只在全新目录上跑（绝不碰生产库）；基准结束后必须能把
//! 落盘事实全部扫描回来并逐条校验，不通过即判失败。
//! 需要它的原因：组提交与队列轮转的改动很容易在吞吐上看不出来，却让
//! 尾延迟翻倍；没有基准就无法证明写者还是「一次 fsync 扛一批」。

use std::path::PathBuf;

use clap::Parser;

/// flow-journal-bench：journal 写入基准（目录必须不存在）。
#[derive(Parser)]
#[command(name = "flow-journal-bench", version, about = "journal 写入基准")]
struct Bench {
    /// 基准数据目录（必须为全新路径）
    root: PathBuf,
}

/// 命令行入口。
///
/// 帮助与版本信息算成功，其余参数错误返回失败退出码。
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

/// 依次跑三档负载并输出 JSON 报告。
///
/// # Errors
/// 目标目录已存在、写入失败、扫描校验不通过或指标未达标时返回错误。
async fn bench_runs(root: PathBuf) -> Result<i32, Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};

    use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass};
    use serde_json::json;

    // 目录必须不存在：基准会往里写真实权威库，复用已有目录等于污染生产数据。
    if root.exists() {
        return Err("benchmark directory must not already exist".into());
    }
    // 三档负载：低频（看单笔延迟）、中频（看组提交效果）、突发（看积压与
    // 吞吐上限）。
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
        // 控制面与审计面分别统计 p99：两类的延迟预算不同，混在一起看
        // 会互相掩盖。
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
        // 取第 99 百分位：下标向下取整并夹到末位，避免小样本越界。
        let p99 = |v: &[f64]| v[(v.len() * 99 / 100).min(v.len() - 1)];
        let stats = journal.stats();
        journal.close().await?;
        // 扫描回来逐条校验：基准跑完必须证明事实真的落了盘，而不只是
        // 「提交返回了成功」。
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
        // 验收线：两队列 p99 都在 100 ms 内，突发档总耗时在 5 s 内，且
        // 组提交比至少 4 笔事务共享一次 fsync。
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
