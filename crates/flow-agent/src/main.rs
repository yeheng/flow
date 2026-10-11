//! flow-agent：受信任的远程执行中继（bin: flow-agent）。
//!
//! 双 TLS 上联（control + data，mTLS，CN 映射 agent_id）+ 本机执行器池。
//! 参数解析：CLI flag > env（clap env 属性已合并）> 配置文件 [agent] > 报错。
//! executor 缺省定位：同目录兄弟 `flow-executor`（ExecutorInvocation::locate），
//! FLOW_EXECUTOR_BIN / [agent].executor_bin / [execution].executor_bin 可覆盖。
//!
//! 运行：cargo run -p flow-agent -- --control-addr ... --data-addr ...

use std::path::PathBuf;

use clap::Parser;
use flow_config::Config;
use flow_engine::execution_protocol::contract::ExecutorInvocation;

/// flow-agent：受信任的远程执行中继（双 TLS 上联 + 本机执行器池）。
#[derive(Parser)]
#[command(
    name = "flow-agent",
    version,
    about = "flow 受信任远程执行中继",
    long_about = "flow 受信任远程执行中继。\n\n\
                  双 TLS 上联主进程（control + data，mTLS，证书 CN 须与 agent_id 一致），\
                  本机维护执行器子进程池。参数缺省回落配置文件 [agent] 分区\
                  （flag > env > 配置文件 > 报错）。",
    arg_required_else_help = true
)]
struct Agent {
    /// 统一配置文件路径（缺省：FLOW_CONFIG → ./flow.toml → 平台配置目录）。
    #[arg(long, global = true, env = "FLOW_CONFIG")]
    config: Option<PathBuf>,
    /// 主进程 control TLS 地址。
    #[arg(long, env = "FLOW_AGENT_CONTROL_ADDR")]
    control_addr: Option<String>,
    /// 主进程 data TLS 地址。
    #[arg(long, env = "FLOW_AGENT_DATA_ADDR")]
    data_addr: Option<String>,
    /// agent 身份（须与客户端证书 CN 一致）。
    #[arg(long, env = "FLOW_AGENT_ID")]
    agent_id: Option<String>,
    #[arg(long, env = "FLOW_AGENT_CA")]
    ca_cert: Option<String>,
    #[arg(long, env = "FLOW_AGENT_CERT")]
    cert: Option<String>,
    #[arg(long, env = "FLOW_AGENT_KEY")]
    key: Option<String>,
    /// 本机执行器二进制。缺省定位同目录兄弟 flow-executor；
    /// FLOW_EXECUTOR_BIN 环境变量同样生效。
    #[arg(long)]
    executor_bin: Option<String>,
    /// 本机执行槽位。
    #[arg(long, env = "FLOW_AGENT_SLOTS")]
    slots: Option<u32>,
}

fn main() -> std::process::ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let agent = match Agent::try_parse() {
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
    // 配置文件在构造 runtime 之前同步加载。
    let loaded = match Config::load(agent.config.as_deref()) {
        Ok(loaded) => loaded,
        Err(err) => return exit_fail(&err),
    };
    let Agent {
        control_addr,
        data_addr,
        agent_id,
        ca_cert,
        cert,
        key,
        executor_bin,
        slots,
        ..
    } = agent;
    let code = agent_main(
        loaded.config,
        control_addr,
        data_addr,
        agent_id,
        ca_cert,
        cert,
        key,
        executor_bin,
        slots,
    );
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}

/// 配置加载失败的统一出口（main 返回 ExitCode）。
fn exit_fail(err: &dyn std::fmt::Display) -> std::process::ExitCode {
    eprintln!("error: {err}");
    std::process::ExitCode::FAILURE
}

#[allow(clippy::too_many_arguments)]
fn agent_main(
    config: Config,
    flag_control_addr: Option<String>,
    flag_data_addr: Option<String>,
    flag_agent_id: Option<String>,
    flag_ca_cert: Option<String>,
    flag_cert: Option<String>,
    flag_key: Option<String>,
    flag_executor_bin: Option<String>,
    flag_slots: Option<u32>,
) -> i32 {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let agent = &config.agent;
    let required =
        |flag: Option<String>, configured: Option<String>, name: &str| -> Result<String, String> {
            flag.or(configured).ok_or_else(|| {
                format!("缺少 {name}：用 CLI 参数、{name} 对应环境变量或配置文件 [agent] 分区提供")
            })
        };
    let control_addr = match required(
        flag_control_addr,
        agent.control_addr.clone(),
        "control_addr",
    ) {
        Ok(v) => v,
        Err(err) => return fail(&err),
    };
    let data_addr = match required(flag_data_addr, agent.data_addr.clone(), "data_addr") {
        Ok(v) => v,
        Err(err) => return fail(&err),
    };
    let agent_id = match required(flag_agent_id, agent.agent_id.clone(), "agent_id") {
        Ok(v) => v,
        Err(err) => return fail(&err),
    };
    let ca_cert = match required(
        flag_ca_cert,
        agent
            .ca_cert
            .clone()
            .map(|p| p.to_string_lossy().into_owned()),
        "ca_cert",
    ) {
        Ok(v) => v,
        Err(err) => return fail(&err),
    };
    let cert = match required(
        flag_cert,
        agent.cert.clone().map(|p| p.to_string_lossy().into_owned()),
        "cert",
    ) {
        Ok(v) => v,
        Err(err) => return fail(&err),
    };
    let key = match required(
        flag_key,
        agent.key.clone().map(|p| p.to_string_lossy().into_owned()),
        "key",
    ) {
        Ok(v) => v,
        Err(err) => return fail(&err),
    };
    let slots = flag_slots.unwrap_or(agent.slots);
    // executor_bin 优先级：CLI flag > [agent].executor_bin > [execution].executor_bin
    // （FLOW_EXECUTOR_BIN env 已在加载时合并进 execution 侧）
    let executor_bin = flag_executor_bin
        .or(agent.executor_bin.clone())
        .or(config.execution.executor_bin.clone());
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
    eprintln!("flow-agent exiting: {reason}");
    0
}

fn fail(err: &dyn std::fmt::Display) -> i32 {
    eprintln!("error: {err}");
    1
}
