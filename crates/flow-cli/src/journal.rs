use crate::error::CliError;
use clap::Subcommand;
use jsonrpsee::{core::client::ClientT, ws_client::WsClient};
use serde_json::{json, Value};
use std::{io::Read, path::PathBuf};

#[derive(Subcommand)]
pub enum Command {
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
fn error(e: impl ToString) -> CliError {
    CliError::local(e.to_string())
}
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
            .or_insert_with(|| Value::String(uuid::Uuid::now_v7().to_string()));
    }
    match client
        .request::<Value, _>(method, params.as_object().unwrap().clone())
        .await
    {
        Ok(value) => Ok(value),
        Err(jsonrpsee::core::ClientError::Call(e)) if e.code() == -32020 => {
            let original: Value = serde_json::from_str(
                e.data()
                    .ok_or_else(|| error("missing committed receipt"))?
                    .get(),
            )
            .map_err(error)?;
            let scope = match method {
                "run.start" => "run.start:manual:".to_owned(),
                "run.cancel" | "run.signal" | "run.adjudicate" => {
                    format!("{method}:{}", params["run_id"].as_str().unwrap_or(""))
                }
                _ => method.to_owned(),
            };
            let status = json!({"scope":scope,"request_id":original["request_id"],"_token":token});
            for _ in 0..20 {
                let receipt: Value = client
                    .request("command.status", status.as_object().unwrap().clone())
                    .await
                    .map_err(error)?;
                if receipt["visible"] == true {
                    return Ok(receipt);
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            println!("{}", original);
            Err(CliError::Rpc{code:-32020,message:"COMMITTED_NOT_VISIBLE; original receipt printed; query command.status, do not recreate".into()})
        }
        Err(jsonrpsee::core::ClientError::Call(e)) => Err(CliError::Rpc {
            code: e.code(),
            message: e.message().into(),
        }),
        Err(e) => Err(error(e)),
    }
}
pub async fn dispatch(client: &WsClient, command: Command) -> Result<(), CliError> {
    let token =
        std::env::var("FLOW_JOURNAL_TOKEN").map_err(|_| error("FLOW_JOURNAL_TOKEN required"))?;
    match command {
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
