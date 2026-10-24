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
    /// Stream a complete referenced value into a NEW file.
    Download {
        run_id: String,
        output_id: String,
        output: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:9803")]
        http: String,
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
            let identity = json!({"scope":flow_dto::command_scope(method, &params), "request_id":params["request_id"]});
            let recovery = if matches!(method, "config.update" | "secrets.set" | "secrets.delete") {
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
/// 分派 journal 子命令。
pub async fn dispatch(client: &WsClient, command: Command) -> Result<(), CliError> {
    let token =
        std::env::var("FLOW_JOURNAL_TOKEN").map_err(|_| error("FLOW_JOURNAL_TOKEN required"))?;
    match command {
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
