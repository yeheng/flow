use flow_journal::{Event, EventKind, Journal, JournalOptions, QueueClass};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("supply a new benchmark directory")?,
    );
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
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
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
    Ok(())
}
