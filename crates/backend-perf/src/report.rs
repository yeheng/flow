//! 压测报告：延迟样本 → 分位数统计，文本表格与 JSON 两种输出。
//!
//! 分位数用最近秩（nearest-rank）：排序后取第 `ceil(p/100 * n)` 个样本，
//! 不做插值——压测报告要的是「真实观测到的某个样本」，不是平滑出来的数。
//! 样本单位统一毫秒（`Latency` 按 Duration 收，`Stats` 只认 ms）。

use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

/// 一组延迟样本（毫秒）。
#[derive(Debug, Default, Clone)]
pub struct Latency {
    samples: Vec<f64>,
}

impl Latency {
    pub fn add(&mut self, elapsed: Duration) {
        self.samples.push(elapsed.as_secs_f64() * 1000.0);
    }

    /// 汇总成 [`Stats`]；`name` 是报告里的指标名（后缀 `_ms` 标单位）。
    pub fn stats(&self, name: &str) -> Stats {
        Stats::new(name, &self.samples)
    }
}

/// 一个延迟指标的分位数摘要（毫秒）。
#[derive(Debug, Serialize)]
pub struct Stats {
    pub name: String,
    pub unit: String,
    pub count: usize,
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub mean: f64,
}

impl Stats {
    pub fn new(name: &str, samples: &[f64]) -> Stats {
        assert!(!samples.is_empty(), "指标 {name} 没有样本");
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let count = sorted.len();
        let mean = sorted.iter().sum::<f64>() / count as f64;
        Stats {
            name: name.to_string(),
            unit: "ms".to_string(),
            count,
            min: sorted[0],
            p50: percentile(&sorted, 50.0),
            p90: percentile(&sorted, 90.0),
            p95: percentile(&sorted, 95.0),
            p99: percentile(&sorted, 99.0),
            max: sorted[count - 1],
            mean,
        }
    }

    /// 单行文本：`名称 n=.. min=.. p50=.. p90=.. p95=.. p99=.. max=.. mean=..`。
    pub fn line(&self) -> String {
        format!(
            "{:<24} n={:<5} min={:>9.3} p50={:>9.3} p90={:>9.3} p95={:>9.3} p99={:>9.3} max={:>9.3} mean={:>9.3}",
            self.name, self.count, self.min, self.p50, self.p90, self.p95, self.p99, self.max, self.mean
        )
    }
}

/// 最近秩分位数：`sorted` 必须升序。
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// 一次场景测量的结果：参数、计数器（吞吐/耗时等标量）与延迟指标。
#[derive(Debug, Serialize)]
pub struct Report {
    pub backend: String,
    pub scenario: String,
    pub params: Value,
    pub counters: Value,
    pub metrics: Vec<Stats>,
}

impl Report {
    pub fn new(backend: &str, scenario: &str, params: Value, counters: Value) -> Report {
        Report {
            backend: backend.to_string(),
            scenario: scenario.to_string(),
            params,
            counters,
            metrics: Vec::new(),
        }
    }

    pub fn with_metrics(mut self, metrics: Vec<Stats>) -> Report {
        self.metrics = metrics;
        self
    }

    /// 文本报告：标题 + 参数 + 计数器 + 每个指标一行分位数。
    pub fn print(&self) {
        println!(
            "── backend={} · {} {}",
            self.backend,
            self.scenario,
            "─".repeat(44)
        );
        println!("  params     {}", kv_line(&self.params));
        println!("  counters   {}", kv_line(&self.counters));
        for stats in &self.metrics {
            println!("  {}", stats.line());
        }
        println!();
    }
}

/// 整轮压测的汇总（JSON 输出根对象）。
#[derive(Debug, Serialize)]
pub struct Suite {
    pub meta: Value,
    pub reports: Vec<Report>,
}

impl Suite {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("报告序列化失败")
    }

    pub fn save_json(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, format!("{}\n", self.to_json()))
    }
}

/// params/counters 对象 → `k=v k=v` 单行（数值保留 3 位小数，字符串原样）。
fn kv_line(value: &Value) -> String {
    let Some(map) = value.as_object() else {
        return value.to_string();
    };
    map.iter()
        .map(|(key, item)| {
            if let Some(number) = item.as_i64() {
                format!("{key}={number}")
            } else if let Some(number) = item.as_f64() {
                format!("{key}={number:.3}")
            } else {
                format!("{key}={item}")
            }
        })
        .collect::<Vec<_>>()
        .join("  ")
}
