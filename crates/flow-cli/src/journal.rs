//! `flow-cli journal`：v2 协议的低层命令面（回归、比较、原始调用、事件流、下载）。
//!
//! 这一组命令是给**自动化与排障**用的：它们直接暴露 v2 协议的能力，
//! 而不是包装成产品语义。日常操作请用 `flow-cli workflow` / `flow-cli run`。
//!
//! 关键约定：
//!
//! - **写入必须带稳定的 request_id**：写方法的幂等键由 CLI 注入
//!   （`--request-id` > 请求文件里的 `request_id` > 新生成的 UUID）。
//!   回复丢失时用同一把键重发，服务端会认出「这是同一次写」。
//! - **交付状态未知时给出命令身份**：写调用的网络错误不代表服务端没执行，
//!   错误文案会带上 scope 与 request_id，供 `command.status` 查询回执。
//! - **下载写新文件**：目标文件已存在会直接失败（`create_new`），
//!   不会静默覆盖用户的数据。

use crate::error::CliError;
use clap::Subcommand;
use jsonrpsee::{core::client::ClientT, ws_client::WsClient};
use serde_json::{json, Value};
use std::{io::Read, path::PathBuf};

tokio::task_local! {
    /// 本次进程内生效的写命令幂等键。
    ///
    /// 用 task_local 传递是因为它是**命令级**上下文：一个 CLI 进程里
    /// 可能顺序执行多次写，必须共用同一把键才能保证「回复丢失后重发」被识别。
    pub(crate) static REQUEST_ID: Option<String>;
}

#[derive(Subcommand)]
/// journal 子命令组。
pub enum Command {
    /// Manage local plugin bundles on the journal host.
    Plugins {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Execute a candidate against recorded external outcomes; fail CI on differences.
    Regress {
        baseline: String,
        #[arg(long)]
        workflow_id: Option<String>,
        #[arg(long)]
        version: Option<u64>,
        #[arg(long, default_value_t = 60)]
        timeout: u64,
    },
    /// Compare two terminal runs and return exit code 4 on differences.
    Compare { baseline: String, candidate: String },
    /// Call a v2 method. Writes should include a stable request_id in the JSON file.
    Call {
        method: String,
        #[arg(long)]
        params: PathBuf,
    },
    /// Stream fixed-snapshot pages as JSONL; empty filtered pages do not stop traversal.
    Events {
        run_id: String,
        #[arg(long)]
        audit: bool,
    },
    /// 权限审计分页（09 §10.3）：谁在何时批准/拒绝了什么，JSONL 输出。
    PermissionsAudit {
        #[arg(long)]
        run_id: Option<String>,
        /// 只看 allow 或 deny
        #[arg(long)]
        decision: Option<String>,
    },
    /// Stream a complete referenced value into a NEW file.
    Download {
        run_id: String,
        output_id: String,
        output: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:9803")]
        http: String,
    },
}
#[derive(Subcommand)]
pub enum PluginCommand {
    List,
    Recheck,
    Install {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        replace: bool,
    },
    /// Publish an already staged bundle and wait for the selected shared-directory Agents.
    Deploy {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        replace: bool,
        #[arg(long = "agent")]
        agents: Vec<String>,
        #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=600))]
        timeout: u64,
    },
    Uninstall {
        plugin_id: String,
        #[arg(long)]
        stop_first: bool,
    },
}

/// 把任意错误收敛成本地错误。
fn error(e: impl ToString) -> CliError {
    CliError::local(e.to_string())
}
/// 发起一次带令牌的 v2 RPC 调用，并归一化错误。
///
/// 写方法会自动补上 `request_id`；网络层错误（而非服务端明确报错）会被
/// 翻译成「交付状态未知」的提示，并保留命令身份供事后查回执。
pub(crate) async fn call(
    client: &WsClient,
    token: &str,
    method: &str,
    mut params: Value,
) -> Result<Value, CliError> {
    params
        .as_object_mut()
        .ok_or_else(|| error("object parameters required"))?
        .insert("_token".into(), json!(token));
    if crate::client::is_write_method(method) {
        params
            .as_object_mut()
            .unwrap()
            .entry("request_id")
            .or_insert_with(|| {
                Value::String(
                    REQUEST_ID
                        .try_with(Clone::clone)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                )
            });
    }
    match client
        .request::<Value, _>(method, params.as_object().unwrap().clone())
        .await
    {
        Ok(value) => Ok(value),
        Err(jsonrpsee::core::ClientError::Call(e)) => Err(CliError::Rpc {
            code: e.code(),
            message: e.message().into(),
        }),
        Err(e) if crate::client::is_write_method(method) => {
            let identity = json!({"scope":flow_rpc_contract::command_scope(method, &params), "request_id":params["request_id"]});
            let recovery = if matches!(method, "plugins.install" | "plugins.uninstall") {
                "使用同一 request_id 与原参数重试；插件操作回执存于宿主插件目录，不通过 command.status 查询。"
            } else if matches!(method, "config.update" | "secrets.set" | "secrets.delete") {
                "此方法没有持久命令回执；请查询当前配置或密钥状态后再决定是否重试。"
            } else {
                "使用 command.status 查询回执；重试必须使用同一 request_id 与原参数，不能换新键。"
            };
            Err(error(format!(
                "{e}\n写入交付状态未知，请保留命令身份：{identity}\n{recovery}"
            )))
        }
        Err(e) => Err(error(e)),
    }
}
async fn deploy_plugin(
    client: &WsClient,
    token: &str,
    source: PathBuf,
    replace: bool,
    agents: &[String],
    timeout: u64,
) -> Result<Value, CliError> {
    let receipt = call(
        client,
        token,
        "plugins.install",
        json!({"source":source,"replace":replace}),
    )
    .await?;
    let id = receipt["plugin_id"]
        .as_str()
        .ok_or_else(|| error("install receipt missing plugin_id"))?;
    let hash = receipt["content_hash"]
        .as_str()
        .ok_or_else(|| error("install receipt missing content_hash"))?;
    if agents.is_empty() {
        return Ok(json!({"install": receipt, "activation": "published"}));
    }
    if receipt["node_types"].as_array().is_none_or(Vec::is_empty) {
        return Err(error(format!(
            "plugin {id} was published as a service; it has no Worker deployment"
        )));
    }
    let params = json!({"plugin_id": id});
    let mut pending = agents
        .iter()
        .map(|agent| (agent.clone(), "not_connected".to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let waited = tokio::time::timeout(std::time::Duration::from_secs(timeout), async {
        loop {
            let deployment = call(client, token, "plugins.deployment", params.clone())
                .await
                .map_err(|failure| {
                    error(format!(
                        "plugin {id} was published; readiness check failed: {failure}"
                    ))
                })?;
            if deployment["content_hash"].as_str() != Some(hash)
                || receipt["bundle_digest"]
                    .as_str()
                    .is_some_and(|digest| deployment["bundle_digest"].as_str() != Some(digest))
                || deployment["draining"] == true
            {
                return Err(error(format!(
                    "plugin {id} deployment was superseded or is draining"
                )));
            }
            if deployment["mode"] != "remote" {
                return Err(error(format!(
                    "plugin {id} was published; the journal server is using local execution"
                )));
            }
            // Check all targets against the same live snapshot on every poll.
            pending.clear();
            for agent in agents {
                let status = deployment["agents"][agent.as_str()]
                    .as_str()
                    .unwrap_or("not_connected");
                if status != "ready" {
                    pending.insert(agent.clone(), status.to_owned());
                }
            }
            if pending.is_empty() {
                return Ok(deployment);
            }
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        }
    })
    .await;
    match waited {
        Ok(Ok(deployment)) => {
            Ok(json!({"install": receipt, "activation":"ready", "deployment":deployment}))
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(error(format!(
            "plugin {id} was published; Agent readiness timed out: {pending:?}"
        ))),
    }
}

/// 分派 journal 子命令。
pub async fn dispatch(client: &WsClient, command: Command) -> Result<(), CliError> {
    let token =
        std::env::var("FLOW_JOURNAL_TOKEN").map_err(|_| error("FLOW_JOURNAL_TOKEN required"))?;
    match command {
        Command::Plugins { command } => {
            let (method, params) = match command {
                PluginCommand::List => ("plugins.list", json!({})),
                PluginCommand::Recheck => ("plugins.recheck", json!({})),
                PluginCommand::Install { source, replace } => (
                    "plugins.install",
                    json!({"source":source,"replace":replace}),
                ),
                PluginCommand::Deploy {
                    source,
                    replace,
                    agents,
                    timeout,
                } => {
                    let result =
                        deploy_plugin(client, &token, source, replace, &agents, timeout).await?;
                    println!("{}", serde_json::to_string_pretty(&result).map_err(error)?);
                    return Ok(());
                }
                PluginCommand::Uninstall {
                    plugin_id,
                    stop_first,
                } => (
                    "plugins.uninstall",
                    json!({"plugin_id":plugin_id,"stop_first":stop_first}),
                ),
            };
            let result = call(client, &token, method, params).await?;
            println!("{}", serde_json::to_string_pretty(&result).map_err(error)?);
            return Ok(());
        }

        Command::Regress {
            baseline,
            workflow_id,
            version,
            timeout,
        } => {
            let receipt = call(
                client,
                &token,
                "run.regress",
                json!({"baseline":baseline,"workflow_id":workflow_id,"version":version}),
            )
            .await?;
            let candidate = receipt["result"]["run_id"]
                .as_str()
                .or_else(|| receipt["run_id"].as_str())
                .ok_or_else(|| error("missing candidate run_id"))?
                .to_owned();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout);
            loop {
                let report = call(
                    client,
                    &token,
                    "run.compare",
                    json!({"baseline":baseline,"candidate":candidate}),
                )
                .await?;
                if report["complete"] == true {
                    return print_comparison(report, &candidate);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(error(format!(
                        "regression timed out; candidate {candidate}"
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        Command::Compare {
            baseline,
            candidate,
        } => print_comparison(
            call(
                client,
                &token,
                "run.compare",
                json!({"baseline":baseline,"candidate":candidate}),
            )
            .await?,
            &candidate,
        )?,
        Command::Call { method, params } => {
            let mut bytes = Vec::new();
            std::fs::File::open(params)
                .map_err(error)?
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(error)?;
            if bytes.len() > 8 * 1024 * 1024 {
                return Err(error("params exceed 8 MiB"));
            }
            let params = serde_json::from_slice(&bytes).map_err(error)?;
            println!("{}", call(client, &token, &method, params).await?);
        }
        Command::PermissionsAudit { run_id, decision } => {
            let mut params = json!({"limit": 256});
            if let Some(run_id) = &run_id {
                params["run_id"] = json!(run_id);
            }
            if let Some(decision) = &decision {
                params["decision"] = json!(decision);
            }
            let mut cursor = Value::Null;
            loop {
                let mut request = params.clone();
                if !cursor.is_null() {
                    request["cursor"] = cursor.clone();
                }
                let page = call(client, &token, "permissions.audit.page", request).await?;
                let events = page["events"]
                    .as_array()
                    .ok_or_else(|| error("invalid page"))?;
                for event in events {
                    println!("{event}");
                }
                if page["complete"].as_bool().unwrap_or(true) {
                    break;
                }
                let next = match page.get("next_cursor") {
                    Some(next) => next.clone(),
                    None => {
                        // Older servers accept a numeric LSN and resume at the next transaction.
                        let last = events.last().ok_or_else(|| error("missing audit cursor"))?;
                        let lsn = last["lsn"]
                            .as_u64()
                            .or_else(|| last["lsn"].as_str()?.parse().ok())
                            .and_then(|lsn| lsn.checked_add(1))
                            .ok_or_else(|| error("invalid audit event position"))?;
                        json!({"run_id": last["event"]["run_id"], "lsn": lsn})
                    }
                };
                if !next.is_object() || next == cursor {
                    return Err(error("audit cursor did not advance"));
                }
                cursor = next;
            }
        }
        Command::Events { run_id, audit } => {
            let method = if audit {
                "run.audit.page"
            } else {
                "run.events.page"
            };
            let mut cursor = Value::Null;
            loop {
                let page = call(
                    client,
                    &token,
                    method,
                    json!({"run_id":run_id,"cursor":cursor,"limit":100}),
                )
                .await?;
                for event in page["events"]
                    .as_array()
                    .ok_or_else(|| error("invalid page"))?
                {
                    println!("{event}");
                }
                cursor = page["next_cursor"].clone();
                if cursor.is_null() {
                    break;
                }
            }
        }
        Command::Download {
            run_id,
            output_id,
            output,
            http,
        } => {
            use tokio::io::AsyncWriteExt;
            let mut url = reqwest::Url::parse(&http).map_err(error)?;
            url.path_segments_mut()
                .map_err(|_| error("invalid download base URL"))?
                .extend(["runs", &run_id, "values", &output_id]);
            let mut response = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(error)?
                .get(url)
                .bearer_auth(token)
                .send()
                .await
                .map_err(error)?
                .error_for_status()
                .map_err(error)?;
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                options.mode(0o600);
            }
            let temporary =
                output.with_file_name(format!(".flow-download-{}", uuid::Uuid::now_v7()));
            let mut file = options.open(&temporary).await.map_err(error)?;
            while let Some(chunk) = response.chunk().await.map_err(error)? {
                file.write_all(&chunk).await.map_err(error)?;
            }
            file.sync_all().await.map_err(error)?;
            tokio::fs::hard_link(&temporary, &output)
                .await
                .map_err(error)?;
            tokio::fs::remove_file(&temporary).await.map_err(error)?;
            eprintln!("saved {}", output.display());
        }
    }
    Ok(())
}

/// 打印回归比较报告；不通过时以 exit 4 结束。
fn print_comparison(report: Value, candidate: &str) -> Result<(), CliError> {
    println!("{}", serde_json::to_string_pretty(&report).map_err(error)?);
    if report["passed"] == true {
        Ok(())
    } else {
        Err(CliError::RunFailed {
            run_id: candidate.into(),
            status: "regression_difference".into(),
            error: Some("candidate did not match baseline".into()),
        })
    }
}

#[cfg(test)]
mod plugin_command_tests {
    use clap::Parser;
    #[test]
    fn plugin_commands_use_the_existing_global_request_identity() {
        for arguments in [
            vec!["flow", "journal", "plugins", "list"],
            vec!["flow", "journal", "plugins", "recheck"],
            vec![
                "flow",
                "--request-id",
                "stable",
                "journal",
                "plugins",
                "install",
                "--source",
                "/tmp/plugin",
                "--replace",
            ],
            vec![
                "flow",
                "journal",
                "plugins",
                "uninstall",
                "demo",
                "--stop-first",
                "--request-id",
                "stable",
            ],
        ] {
            assert!(crate::cli::Cli::try_parse_from(arguments).is_ok());
        }
        assert!(crate::cli::Cli::try_parse_from([
            "flow",
            "journal",
            "plugins",
            "install",
            "/tmp/plugin"
        ])
        .is_err());
    }
}
