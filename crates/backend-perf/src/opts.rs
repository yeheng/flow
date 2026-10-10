//! 命令行参数：`flow-perf [选项]`。手写解析，不引第三方参数库。
//!
//! 默认规模是「轻量回归」：Journal × 四场景，单次运行数分钟内跑完，适合定期
//! 回归；重负载用 `--runs` / `--concurrency` / `--prefill` / `--iterations`
//! 放大（见 `usage()` 里的口径说明）。

use std::process::exit;

/// 压测场景。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// run 执行吞吐（chain / fanout 两形态）。
    Throughput,
    /// RPC 读路径延迟（run.get / list / stats / timeline / events）。
    Read,
    /// 订阅推送延迟（落账 → 事件到达客户端）。
    Subscribe,
    /// 崩溃恢复耗时（SIGKILL → 重启就绪 + 恢复后推进）。
    Recovery,
}

impl Scenario {
    pub const ALL: [Scenario; 4] = [
        Scenario::Throughput,
        Scenario::Read,
        Scenario::Subscribe,
        Scenario::Recovery,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Scenario::Throughput => "run_throughput",
            Scenario::Read => "rpc_read",
            Scenario::Subscribe => "subscribe_latency",
            Scenario::Recovery => "crash_recovery",
        }
    }
}

/// 产品后端选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendSel {
    Journal,
}

#[derive(Debug, Clone)]
pub struct Opts {
    pub scenarios: Vec<Scenario>,
    pub backend: BackendSel,
    /// run_throughput：总 run 数。
    pub runs: usize,
    /// 并发：同时在飞（已提交未到终态）的 run 数，提交侧限流。
    pub concurrency: usize,
    /// rpc_read：预置的终态 run 数（读路径的数据量）。
    pub prefill: usize,
    /// rpc_read：每种方法的调用次数。
    pub iterations: usize,
    /// subscribe_latency：被盯的 run 数。
    pub subscribe_runs: usize,
    /// crash_recovery：停驻在 human_task 的 run 数。
    pub recovery_runs: usize,
    /// 报告另存 JSON 的路径（`-` 表示打印到 stdout）。
    pub json: Option<String>,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            scenarios: Scenario::ALL.to_vec(),
            backend: BackendSel::Journal,
            runs: 200,
            concurrency: 16,
            prefill: 100,
            iterations: 200,
            subscribe_runs: 50,
            recovery_runs: 40,
            json: None,
        }
    }
}

impl Opts {
    /// 解析失败（含 `--help`）直接退出进程：压测工具没有「带着错参数继续跑」的意义。
    pub fn parse(args: impl Iterator<Item = String>) -> Opts {
        let mut opts = Opts::default();
        let mut args = args.peekable();
        while let Some(flag) = args.next() {
            let mut value = |name: &str| -> String {
                args.next()
                    .unwrap_or_else(|| die(&format!("{name} 缺少参数值")))
            };
            match flag.as_str() {
                "-h" | "--help" => {
                    print!("{}", usage());
                    exit(0);
                }
                "--scenario" => {
                    opts.scenarios = parse_scenarios(&value("--scenario"));
                }
                "--backend" => {
                    opts.backend = match value("--backend").as_str() {
                        "journal" => BackendSel::Journal,
                        other => die(&format!("--backend 不认识：{other}")),
                    };
                }
                "--runs" => opts.runs = parse_count(&flag, &value("--runs")),
                "--concurrency" => opts.concurrency = parse_count(&flag, &value("--concurrency")),
                "--prefill" => opts.prefill = parse_count(&flag, &value("--prefill")),
                "--iterations" => opts.iterations = parse_count(&flag, &value("--iterations")),
                "--subscribe-runs" => {
                    opts.subscribe_runs = parse_count(&flag, &value("--subscribe-runs"))
                }
                "--recovery-runs" => {
                    opts.recovery_runs = parse_count(&flag, &value("--recovery-runs"))
                }
                "--json" => opts.json = Some(value("--json")),
                other => die(&format!("不认识的参数：{other}\n\n{}", usage())),
            }
        }
        opts
    }

    /// 透传给被测进程的环境变量。FLOW_MAX_RUNS 是 Journal executor 的容量许可
    /// （同时驱动的 run 上限）：必须 ≥ 同时在飞的 run 数，否则许可就是个人造瓶颈。
    /// crash_recovery 里停驻在 human_task 的 run 不释放许可，所以按 recovery_runs
    /// 放大再留余量。
    pub fn server_env(&self) -> Vec<(String, String)> {
        vec![
            ("FLOW_PERF_SERVE".to_string(), "1".to_string()),
            (
                "FLOW_MAX_RUNS".to_string(),
                (self.concurrency.max(self.recovery_runs) + 8).to_string(),
            ),
        ]
    }
}

fn parse_scenarios(word: &str) -> Vec<Scenario> {
    let picked: Vec<Scenario> = word
        .split(',')
        .filter(|part| !part.is_empty())
        .filter_map(|part| match part {
            "all" => None,
            "throughput" => Some(Scenario::Throughput),
            "read" => Some(Scenario::Read),
            "subscribe" => Some(Scenario::Subscribe),
            "recovery" => Some(Scenario::Recovery),
            other => die(&format!("--scenario 不认识：{other}")),
        })
        .collect();
    if picked.is_empty() {
        Scenario::ALL.to_vec()
    } else {
        picked
    }
}

fn parse_count(flag: &str, raw: &str) -> usize {
    raw.parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or_else(|| die(&format!("{flag} 必须是正整数：{raw}")))
}

fn die(message: &str) -> ! {
    eprintln!("错误：{message}");
    exit(2);
}

pub fn usage() -> &'static str {
    "flow-perf：flow 后端性能压测 harness（黑盒，真起被测进程，Journal 产品后端）\n\
     \n\
     用法：cargo run -p backend-perf -- [选项]\n\
     \n\
     选项：\n\
       --scenario <列表>      all（缺省）| throughput | read | subscribe | recovery，可逗号组合\n\
       --backend <后端>       journal（缺省，v2 唯一产品后端）\n\
       --runs <N>             run_throughput 的总 run 数（缺省 200）\n\
       --concurrency <N>      同时在飞的 run 数（缺省 16，提交侧限流 + 驱动容量基准）\n\
       --prefill <N>          rpc_read 预置的终态 run 数（缺省 100）\n\
       --iterations <N>       rpc_read 每种方法的调用次数（缺省 200）\n\
       --subscribe-runs <N>   subscribe_latency 的 run 数（缺省 50）\n\
       --recovery-runs <N>    crash_recovery 停驻的 run 数（缺省 40）\n\
       --json <路径>          报告另存 JSON（`-` 表示打印到 stdout）\n\
       -h, --help             本帮助\n\
     \n\
     环境变量：\n\
       RUST_LOG              透传给被测服务进程\n\
     \n\
     默认规模是轻量回归（单次数分钟内）；重负载用 --runs / --concurrency / --prefill\n\
     放大。每个场景独占一个被测进程上下文（临时目录即建即毁，panic 路径\n\
     也清理），场景之间数据量互不污染。\n"
}
