//! flow-agent 二进制入口（三期 §1.1/§1.2）。

use clap::Parser;
use flow_agent::runtime::{run_agent, AgentConfig};

#[derive(Parser, Debug)]
#[command(name = "flow-agent", about = "受信任的远程执行中继")]
struct Args {
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
    /// 本机执行器二进制。
    #[arg(long, env = "FLOW_EXECUTOR_BIN")]
    executor_bin: String,
    /// 本机执行槽位。
    #[arg(long, env = "FLOW_AGENT_SLOTS", default_value = "4")]
    slots: u32,
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    let config = AgentConfig {
        agent_id: args.agent_id,
        control_addr: args.control_addr,
        data_addr: args.data_addr,
        ca_cert: args.ca_cert.into(),
        cert: args.cert.into(),
        key: args.key.into(),
        executor_bin: args.executor_bin.into(),
        slots: args.slots,
    };
    let shutdown = tokio_util::sync::CancellationToken::new();
    let reason = run_agent(config, shutdown).await;
    eprintln!("flow-agent exiting: {reason}");
}
