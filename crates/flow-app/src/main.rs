//! flow：统一二进制入口。
//!
//! 一个 bin、多个子命令，替代原先散落的八个可执行文件：
//!
//! | 子命令 | 原二进制 | 职责 |
//! |---|---|---|
//! | `flow server` | flow-server | JSON-RPC 2.0 over WebSocket 服务 + cron + webhook |
//! | `flow cli` | flow-cli | 命令行客户端（纯 RPC，后端无关） |
//! | `flow executor` | flow-executor | 受管理执行子进程（主进程自召唤或独立部署） |
//! | `flow agent` | flow-agent | 受信任远程执行中继（双 TLS 上联） |
//! | `flow journal-tool` | flow-journal-tool | journal 离线维护（verify/index/backup/repair） |
//! | `flow journal-bench` | journal-bench | journal 写入基准 |
//! | `flow journal-server` | flow-journal-server | JSONL v2 开发服务（WS RPC + 下载） |
//! | `flow journal-dev` | flow-journal-dev | JSONL v2 开发运行器（run/resume/import） |
//!
//! 设计约束：
//! - **后端不在构建期区分**：sqlite / postgres 共用同一份二进制，
//!   `FLOW_BACKEND` 在进程入口（`flow_backend::open_from_env`）一次性决定；
//! - **执行器自召唤**：`flow executor` 与主进程是同一个文件——
//!   `locate_executor` 在未显式指定 `FLOW_EXECUTOR_BIN` 时以
//!   `current_exe + ["executor"]` 前缀召唤本子命令（I09 定位规则的
//!   合并二进制形态）；
//! - 测试专用脚手架（backend-e2e 的 `flow-server-e2e`、backend-perf 的
//!   `flow-perf`）不属于产品 bin，保持独立（它们依赖 CARGO_BIN_EXE 的
//!   package 级定位机制）。

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use flow_engine::execution_protocol::contract::ExecutorInvocation;

/// flow：统一二进制入口（server / cli / executor / agent / journal-*）。
#[derive(Parser)]
#[command(
    name = "flow",
    version,
    about = "flow 工作流引擎统一入口",
    long_about = "flow 工作流引擎统一入口。\n\n\
                  后端在启动时用 FLOW_BACKEND 选择（sqlite 缺省 | postgres），\n\
                  不在构建期区分。执行器与主进程是同一个二进制。",
    arg_required_else_help = true
)]
struct Flow {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// JSON-RPC 2.0 over WebSocket 服务（+ cron 调度 + webhook HTTP）
    Server,
    /// 命令行客户端（纯 RPC，对 SQLite / Postgres 后端行为一致）。
    /// 参数整体透传给 CLI 解析器（`flow cli workflow list`、`flow cli --json ...`）。
    Cli {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// 受管理执行子进程（FD 槽位由主进程 pre_exec 固定；未知参数忽略）。
    ///
    /// 原独立 flow-executor 二进制不解析任何参数（`--tag` 等诊断标记由
    /// 主进程追加、执行器静默忽略），合并后仍须保持这条契约：禁用
    /// help/version 拦截，全部参数捕获后丢弃。
    #[command(
        disable_help_flag = true,
        disable_help_subcommand = true,
        disable_version_flag = true
    )]
    Executor {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// 受信任的远程执行中继（双 TLS 上联 + 本机执行器池）
    Agent {
        /// 主进程 control TLS 地址。
        #[arg(long, env = "FLOW_AGENT_CONTROL_ADDR")]
        control_addr: String,
        /// 主进程 data TLS 地址。
        #[arg(long, env = "FLOW_AGENT_DATA_ADDR")]
        data_addr: String,
        /// agent 身份（须与客户端证书 CN 一致）。
        #[arg(long, env = "FLOW_AGENT_ID")]
        agent_id: String,
        #[arg(long, env = "FLOW_AGENT_CA")]
        ca_cert: String,
        #[arg(long, env = "FLOW_AGENT_CERT")]
        cert: String,
        #[arg(long, env = "FLOW_AGENT_KEY")]
        key: String,
        /// 本机执行器二进制（独立部署形态）。缺省自召唤本文件的 executor
        /// 子命令；FLOW_EXECUTOR_BIN 环境变量同样生效。
        #[arg(long)]
        executor_bin: Option<String>,
        /// 本机执行槽位。
        #[arg(long, env = "FLOW_AGENT_SLOTS", default_value = "4")]
        slots: u32,
    },
    /// journal 离线维护：verify / rebuild-index / backup / repair
    JournalTool {
        #[command(subcommand)]
        command: JournalToolCommand,
    },
    /// journal 写入基准（目录必须不存在）
    JournalBench {
        /// 基准数据目录（必须为全新路径）
        root: PathBuf,
    },
    /// JSONL v2 开发服务（WS RPC + 有界下载；仅 loopback）
    JournalServer,
    /// JSONL v2 开发运行器（run / resume / status / import-legacy）
    JournalDev {
        /// journal 数据目录
        #[arg(long)]
        data_dir: PathBuf,
        #[command(subcommand)]
        command: JournalDevCommand,
    },
}

#[derive(Subcommand)]
enum JournalToolCommand {
    /// 全量字节校验 + 已发布值闭环检查
    Verify { data_dir: PathBuf },
    /// 重建位置索引（一次性派生缓存，可随时删除）
    RebuildIndex { data_dir: PathBuf },
    /// 备份可恢复前缀到新目录
    Backup {
        data_dir: PathBuf,
        destination: PathBuf,
    },
    /// 打印可恢复前缀；--confirm 把前缀写入新目录（原文件永不修改）
    Repair {
        data_dir: PathBuf,
        destination: PathBuf,
        #[arg(long)]
        confirm: bool,
    },
}

#[derive(Subcommand)]
enum JournalDevCommand {
    /// Install/publish a definition and run it with a JSON input file.
    Run {
        #[arg(long)]
        definition: PathBuf,
        #[arg(long)]
        input: Option<PathBuf>,
    },
    /// Resume only work that is safe according to committed facts. Unknown operations wait.
    Resume,
    /// Print recovered state without starting any execution.
    Status,
    /// Preserve a stopped legacy dataset as a read-only baseline in a new JSONL directory.
    ImportLegacy {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        database: PathBuf,
    },
    /// Offline: rebuild into a NEW SQLite file, without executing any workflow.
    RebuildProjection {
        #[arg(long)]
        destination: PathBuf,
    },
}

fn main() -> std::process::ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let command = match Flow::try_parse() {
        Ok(args) => args.command,
        Err(error) => {
            let success = matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = error.print();
            std::process::exit(if success { 0 } else { 1 });
        }
    };
    // 每个子命令自带 runtime 形态：executor/agent 是多线程子进程运行时，
    // server 的形态（current_thread/multi_thread）随 FLOW_BACKEND 切换，
    // 其余工具单线程足够。
    let code = match command {
        Command::Server => server_main(),
        Command::Cli { args } => cli_main(args),
        Command::Executor { .. } => executor_main(),
        Command::Agent {
            control_addr,
            data_addr,
            agent_id,
            ca_cert,
            cert,
            key,
            executor_bin,
            slots,
        } => agent_main(
            control_addr,
            data_addr,
            agent_id,
            ca_cert,
            cert,
            key,
            executor_bin,
            slots,
        ),
        Command::JournalTool { command } => journal_tool_main(command),
        Command::JournalBench { root } => journal_bench_main(root),
        Command::JournalServer => journal_server_main(),
        Command::JournalDev { data_dir, command } => journal_dev_main(data_dir, command),
    };
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}

/// `flow server`：与原 flow-server 二进制逐字同一份实现（runtime 形态随
/// FLOW_BACKEND 切换，run_from_env 内部初始化 tracing）。
fn server_main() -> i32 {
    let runtime = if flow_rpc::prefer_current_thread_runtime() {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    }
    .enable_all()
    .build()
    .expect("server tokio runtime");
    match runtime.block_on(flow_rpc::run_from_env()) {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("flow server 异常退出：{err}");
            1
        }
    }
}

/// `flow cli`：把捕获的参数原样交给 CLI 解析器（自带 runtime 与退出码契约）。
fn cli_main(args: Vec<String>) -> i32 {
    flow_cli::run_from_args(&args)
}

/// `flow executor`：执行器子进程入口。FD 槽位由主进程 pre_exec 固定
/// （control=100，data=101），或经环境变量显式覆盖；stdout/stderr 不携带
/// 协议帧，只用于诊断输出。
fn executor_main() -> i32 {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let control_fd = std::env::var("FLOW_EXECUTOR_CONTROL_FD")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(flow_engine::execution_protocol::EXECUTOR_CONTROL_FD_SLOT);
    let data_fd = std::env::var("FLOW_EXECUTOR_DATA_FD")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(flow_engine::execution_protocol::EXECUTOR_DATA_FD_SLOT);
    let config = flow_executor::runtime::ExecutorConfig {
        control_fd,
        data_fd,
        queue_depth: 128,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("executor tokio runtime");
    let code = runtime.block_on(flow_executor::runtime::run_executor(config));
    // 退出前确保 runtime 干净停止（在途阻塞任务随进程终止）。
    runtime.shutdown_timeout(std::time::Duration::from_millis(300));
    code
}

/// `flow agent`：远程中继入口。executor 缺省自召唤本文件的 executor 子命令。
#[allow(clippy::too_many_arguments)]
fn agent_main(
    control_addr: String,
    data_addr: String,
    agent_id: String,
    ca_cert: String,
    cert: String,
    key: String,
    executor_bin: Option<String>,
    slots: u32,
) -> i32 {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let executor = match executor_bin {
        Some(bin) => ExecutorInvocation::explicit(bin),
        None => match ExecutorInvocation::locate() {
            Ok(executor) => executor,
            Err(err) => {
                eprintln!("error: {err}");
                return 1;
            }
        },
    };
    let config = flow_agent::runtime::AgentConfig {
        agent_id,
        control_addr,
        data_addr,
        ca_cert: ca_cert.into(),
        cert: cert.into(),
        key: key.into(),
        executor,
        slots,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("agent tokio runtime");
    let shutdown = tokio_util::sync::CancellationToken::new();
    let reason = runtime.block_on(async move {
        let stop = shutdown.clone();
        let signal = tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            stop.cancel();
        });
        let reason = flow_agent::runtime::run_agent(config, shutdown).await;
        signal.abort();
        reason
    });
    eprintln!("flow agent exiting: {reason}");
    0
}

/// `flow journal-tool`：离线维护（全部动作先取数据目录排他锁）。
fn journal_tool_main(command: JournalToolCommand) -> i32 {
    let report = match command {
        JournalToolCommand::Verify { data_dir } => {
            let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                Ok(lock) => lock,
                Err(err) => return fail(&err),
            };
            match flow_journal::maintenance::verify(&data_dir) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
        JournalToolCommand::RebuildIndex { data_dir } => {
            let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                Ok(lock) => lock,
                Err(err) => return fail(&err),
            };
            match flow_journal::maintenance::rebuild_index(&data_dir, u64::MAX) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
        JournalToolCommand::Backup {
            data_dir,
            destination,
        } => {
            let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                Ok(lock) => lock,
                Err(err) => return fail(&err),
            };
            match flow_journal::maintenance::backup(&data_dir, &destination, u64::MAX) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
        JournalToolCommand::Repair {
            data_dir,
            destination,
            confirm,
        } => {
            if !confirm {
                let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                    Ok(lock) => lock,
                    Err(err) => return fail(&err),
                };
                return match flow_journal::scan(&data_dir, |_, _| Ok(())) {
                    Ok(report) => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&report).unwrap_or_default()
                        );
                        eprintln!(
                            "repair requires --confirm; candidate suffix may include previously \
                             acknowledged data; original evidence will be preserved"
                        );
                        1
                    }
                    Err(err) => fail(&err),
                };
            }
            match flow_journal::maintenance::repair(&data_dir, &destination, true) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_default()
    );
    if report.fault.is_some() {
        eprintln!("journal verification found corruption; source is unchanged");
        return 1;
    }
    0
}

fn fail(err: &dyn std::fmt::Display) -> i32 {
    eprintln!("error: {err}");
    1
}

/// `flow journal-bench`：三档负载（low/mixed/burst）写入基准。
fn journal_bench_main(root: PathBuf) -> i32 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("bench tokio runtime");
    runtime
        .block_on(async move { bench(root).await })
        .unwrap_or_else(|err| {
            eprintln!("error: {err}");
            1
        })
}

async fn bench(root: PathBuf) -> Result<i32, Box<dyn std::error::Error>> {
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

/// `flow journal-server`：JSONL v2 开发服务（原 flow-journal-server bin）。
fn journal_server_main() -> i32 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("journal-server tokio runtime");
    match runtime.block_on(journal_server()) {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("error: {err}");
            1
        }
    }
}

async fn journal_server() -> Result<(), Box<dyn std::error::Error>> {
    use flow_backend::journal::JournalBackend;

    let root = PathBuf::from(std::env::var("FLOW_JOURNAL_DATA_DIR")?);
    let token = std::env::var("FLOW_JOURNAL_TOKEN")?;
    let addr = std::env::var("FLOW_JOURNAL_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9802".into())
        .parse()?;
    let backend = JournalBackend::open(&root, Default::default()).await?;
    let download_addr: std::net::SocketAddr = std::env::var("FLOW_JOURNAL_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9803".into())
        .parse()?;
    if !download_addr.ip().is_loopback() {
        return Err("development downloads require loopback".into());
    }
    let listener = tokio::net::TcpListener::bind(download_addr).await?;
    let router = flow_rpc::journal_download::router(backend.clone(), token.clone())?.merge(
        flow_rpc::journal_triggers::router(backend.clone(), token.clone()),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut downloads = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let (server, addr) = flow_rpc::journal_v2::serve(backend.clone(), token, addr).await?;
    flow_backend::start_execution_from_env(&backend).await?;
    let scheduler = flow_rpc::journal_triggers::start(backend.clone());
    eprintln!("JSONL development RPC listening on {addr}");
    tokio::signal::ctrl_c().await?;
    scheduler.abort();
    let _ = scheduler.await;
    server.stop()?;
    server.stopped().await;
    let _ = stop.send(());
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut downloads)
        .await
        .is_err()
    {
        downloads.abort();
        let _ = downloads.await;
    }
    backend.close().await?;
    Ok(())
}

/// `flow journal-dev`：JSONL v2 开发运行器（原 flow-journal-dev bin）。
fn journal_dev_main(data_dir: PathBuf, command: JournalDevCommand) -> i32 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("journal-dev tokio runtime");
    match runtime.block_on(journal_dev(data_dir, command)) {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("error: {err}");
            1
        }
    }
}

async fn journal_dev(
    data_dir: PathBuf,
    command: JournalDevCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeSet;
    use std::io::Read;

    use flow_backend::journal::JournalBackend;
    use flow_journal::JournalOptions;

    fn read_json(
        path: &std::path::Path,
        max: usize,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(max as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > max {
            return Err("input file exceeds development budget".into());
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    if let JournalDevCommand::ImportLegacy { source, database } = &command {
        let report = flow_backend::journal_import::import(source, database, &data_dir).await?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    if let JournalDevCommand::RebuildProjection { destination } = &command {
        let lsn = JournalBackend::rebuild_projection(&data_dir, destination).await?;
        println!(
            "{}",
            serde_json::json!({"applied_lsn":lsn.to_string(),"destination":destination})
        );
        return Ok(());
    }
    let backend = JournalBackend::open(&data_dir, JournalOptions::default()).await?;
    if let JournalDevCommand::Run { definition, input } = &command {
        let definition = read_json(definition, flow_journal::MAX_LINE_BYTES)?;
        let input = input
            .as_ref()
            .map(|p| read_json(p, 8 * 1024 * 1024))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let created = backend.workflow_create("development", None).await?;
        let workflow = created.result["workflow_id"].as_str().expect("workflow_id");
        backend.workflow_update(workflow, definition, None).await?;
        backend.workflow_publish(workflow, 1, None).await?;
        let receipt = backend
            .run_start(workflow, None, input, "manual", None, None)
            .await?;
        println!("{}", serde_json::to_string(&receipt)?);
    }
    if matches!(command, JournalDevCommand::Status) {
        serde_json::to_writer_pretty(std::io::stdout(), &backend.state().await)?;
        println!();
        backend.close().await?;
        return Ok(());
    }
    let mut pending = backend
        .state()
        .await
        .runs
        .values()
        .filter(|r| !r.terminal())
        .map(|r| r.run_id.clone())
        .collect::<BTreeSet<_>>();
    flow_backend::start_execution_from_env(&backend).await?;
    while !pending.is_empty() {
        tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(std::time::Duration::from_millis(100))=>{}}
        for run in backend.state().await.runs.values().filter(|r| r.terminal()) {
            if pending.remove(&run.run_id) {
                serde_json::to_writer(std::io::stdout(), run)?;
                println!();
            }
        }
    }
    backend.close().await?;
    Ok(())
}
