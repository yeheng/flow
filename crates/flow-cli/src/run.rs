//! `flow-cli run`：手动触发 run + 运行记录查询/取消。
//!
//! 触发（`run start`）与服务端 `run.start` 一对一：CLI 只负责收集参数与
//! 轮询终态，「published 才能执行」「创建前校验定义」仍由服务端单点裁决
//! （DESIGN.md §9 的 `resolve_runnable_definition`）。
//!
//! 等待策略是有意的简单：`run.get` 300ms 轮询到终态或超时。订阅流
//! （`run.subscribe`）能把等待降到事件驱动，但那是服务端推送优化，对 CLI
//! 的一次性等待没有收益，反而多一条会断开的通道要兜底。

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use jsonrpsee::ws_client::WsClient;
use serde_json::{json, Value};

use crate::cli::RunCommand;
use crate::client::{call, object};
use crate::error::CliError;
use crate::output::{
    duration_text, load_json_arg, local_time, note, print_json, short_id, table, truncate_chars,
};

pub async fn dispatch(client: &WsClient, json: bool, command: RunCommand) -> Result<(), CliError> {
    match command {
        RunCommand::Start {
            workflow,
            input,
            version,
            detach,
            timeout,
        } => start(client, json, workflow, input, version, detach, timeout).await,
        RunCommand::List {
            workflow_id,
            status,
            source,
            limit,
        } => list(client, json, workflow_id, status, source, limit).await,
        RunCommand::Get { run_id } => get(client, json, &run_id).await,
        RunCommand::Events { run_id, from_seq } => events(client, &run_id, from_seq).await,
        RunCommand::Timeline { run_id } => timeline(client, json, &run_id).await,
        RunCommand::Cancel { run_id } => cancel(client, json, &run_id).await,
    }
}

/// 手动触发：接受 workflow_id 或 name（name 在本地解析成 id），默认等终态。
async fn start(
    client: &WsClient,
    json: bool,
    workflow: String,
    input: Option<String>,
    version: Option<i64>,
    detach: bool,
    timeout: u64,
) -> Result<(), CliError> {
    let workflow_id = crate::workflow::resolve_workflow_id(client, &workflow).await?;
    let input = match input {
        Some(raw) => load_json_arg(&raw, "run 输入")?,
        None => Value::Null,
    };

    let mut params = vec![("workflow_id", json!(workflow_id))];
    if let Some(version) = version {
        params.push(("version", json!(version)));
    }
    params.push(("input", input));
    let created = call(client, "run.start", object(params)).await?;
    let run_id = created["run_id"].as_str().unwrap_or_default().to_string();
    let version = created["workflow_version"].as_i64().unwrap_or_default();
    note(format!(
        "触发 workflow {workflow_id} v{version} → run {run_id}"
    ));

    if detach {
        if json {
            print_json(&created);
        } else {
            // detach 模式 stdout 只留 run_id，方便 `$(flow-cli run start … --detach)` 取用
            println!("{run_id}");
        }
        return Ok(());
    }

    let run = wait_terminal(client, &run_id, Duration::from_secs(timeout)).await?;
    let status = run["status"].as_str().unwrap_or_default().to_string();
    note(format!(
        "run {run_id} {status}（耗时 {}）",
        run_seconds(&run)
            .map(duration_text)
            .unwrap_or_else(|| "未知".to_string())
    ));

    if status == "succeeded" {
        if json {
            print_json(&run);
        } else {
            print_json(run.get("output").unwrap_or(&Value::Null));
        }
        return Ok(());
    }
    if json {
        print_json(&run);
    }
    Err(CliError::RunFailed {
        run_id,
        status,
        error: run["error"].as_str().map(str::to_string),
    })
}

/// 轮询到终态。`awaiting_resume`（human_task 等待 / 待人工裁决）**不是**终态，
/// CLI 会一直等到超时——想在等待期间交付信号或裁决，用别的客户端
/// （web 前端 / run.signal）；CLI 只覆盖触发与查询，不引入第二条写入路径。
async fn wait_terminal(
    client: &WsClient,
    run_id: &str,
    timeout: Duration,
) -> Result<Value, CliError> {
    let deadline = Instant::now() + timeout;
    loop {
        let run = run_record(client, run_id).await?;
        let status = run["status"].as_str().unwrap_or_default();
        if flow_dto::DbRunStatus::is_terminal_str(status) {
            return Ok(run);
        }
        if Instant::now() >= deadline {
            return Err(CliError::local(format!(
                "run {run_id} 在 {} 秒内未终结（当前 {status}）：加大 --timeout，或 --detach 只拿 run_id 再 run get 查询",
                timeout.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn run_record(client: &WsClient, run_id: &str) -> Result<Value, CliError> {
    Ok(call(
        client,
        "run.get.full",
        object(vec![("run_id", json!(run_id))]),
    )
    .await?
    .get("run")
    .cloned()
    .unwrap_or(Value::Null))
}

/// run 耗时（秒）。两端都可能缺（未 started_at 或未 ended_at）时返回 None。
fn run_seconds(run: &Value) -> Option<f64> {
    let started = parse_time(run.get("started_at")?.as_str()?)?;
    let ended = parse_time(run.get("ended_at")?.as_str()?)?;
    Some((ended - started).num_milliseconds() as f64 / 1000.0)
}

fn parse_time(iso: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

async fn list(
    client: &WsClient,
    json: bool,
    workflow_id: Option<String>,
    status: Option<String>,
    source: Option<String>,
    limit: i64,
) -> Result<(), CliError> {
    let mut params: Vec<(&str, Value)> = vec![];
    if let Some(workflow_id) = &workflow_id {
        params.push(("workflow_id", json!(workflow_id)));
    }
    if let Some(status) = &status {
        params.push(("status", json!(status)));
    }
    if let Some(source) = &source {
        params.push(("source", json!(source)));
    }
    params.push(("limit", json!(limit)));
    let result = call(client, "run.list.full", object(params)).await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    let runs = result["runs"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let rows: Vec<Vec<String>> = runs
        .iter()
        .map(|run| {
            vec![
                short_id(run["id"].as_str().unwrap_or_default()),
                short_id(run["workflow_id"].as_str().unwrap_or_default()),
                format!("v{}", run["workflow_version"].as_i64().unwrap_or_default()),
                run["status"].as_str().unwrap_or_default().to_string(),
                run["source"].as_str().unwrap_or_default().to_string(),
                local_time(run["started_at"].as_str().unwrap_or_default()),
            ]
        })
        .collect();
    note(format!(
        "共 {} 条运行记录（id 已截断至 12 字符，全量用 --json）",
        runs.len()
    ));
    println!(
        "{}",
        table(
            &[
                "RUN_ID",
                "WORKFLOW_ID",
                "VERSION",
                "STATUS",
                "SOURCE",
                "STARTED AT"
            ],
            &rows
        )
    );
    Ok(())
}

async fn get(client: &WsClient, json: bool, run_id: &str) -> Result<(), CliError> {
    let result = call(
        client,
        "run.get.full",
        object(vec![("run_id", json!(run_id))]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    let run = result.get("run").cloned().unwrap_or(Value::Null);
    let live = result["live"].as_bool().unwrap_or(false);
    note(format!(
        "# run {} {}｜live={}｜workflow {} v{}",
        run_id,
        run["status"].as_str().unwrap_or_default(),
        live,
        run["workflow_id"].as_str().unwrap_or_default(),
        run["workflow_version"].as_i64().unwrap_or_default()
    ));
    print_json(&result);
    Ok(())
}

/// 原始事件：一行一个 compact JSON（JSONL），可直接 jq / 落盘再分析。
async fn events(client: &WsClient, run_id: &str, from_seq: Option<u64>) -> Result<(), CliError> {
    let mut params = vec![("run_id", json!(run_id))];
    if let Some(from_seq) = from_seq {
        params.push(("from_seq", json!(from_seq)));
    }
    let result = call(client, "run.events.full", object(params)).await?;
    let events = result["events"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    for event in events {
        println!("{}", serde_json::to_string(event).unwrap_or_default());
    }
    note(format!("共 {} 条事件", events.len()));
    Ok(())
}

async fn timeline(client: &WsClient, json: bool, run_id: &str) -> Result<(), CliError> {
    let result = call(
        client,
        "run.timeline",
        object(vec![("run_id", json!(run_id))]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    note(format!(
        "run {} {}（phase {}｜seq {}｜{} → {}）",
        run_id,
        result["status"].as_str().unwrap_or_default(),
        result["phase"].as_str().unwrap_or_default(),
        result["last_seq"].as_i64().unwrap_or_default(),
        local_time(result["started_at"].as_str().unwrap_or_default()),
        result["ended_at"]
            .as_str()
            .map(local_time)
            .unwrap_or_else(|| "未结束".to_string())
    ));
    let nodes = result["nodes"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let rows: Vec<Vec<String>> = nodes
        .iter()
        .map(|node| {
            vec![
                node["id"].as_str().unwrap_or_default().to_string(),
                node["state"].as_str().unwrap_or_default().to_string(),
                node["attempts"].as_i64().unwrap_or_default().to_string(),
                node["duration_ms"]
                    .as_i64()
                    .map(|ms| format!("{ms}ms"))
                    .unwrap_or_else(|| "-".to_string()),
                node_outcome(node),
            ]
        })
        .collect();
    println!(
        "{}",
        table(
            &["NODE", "STATE", "ATTEMPTS", "DURATION", "OUTPUT / ERROR"],
            &rows
        )
    );
    Ok(())
}

/// 时间线最后一列：优先错误，其次输出（都压缩成单行并截断，完整值走 --json）。
fn node_outcome(node: &Value) -> String {
    if let Some(error) = node["error"].as_str() {
        return format!("✘ {}", truncate_chars(error, 60));
    }
    if let Some(reason) = node["reason"].as_str() {
        return format!("跳过：{}", truncate_chars(reason, 60));
    }
    match node.get("output") {
        Some(Value::Null) | None => "-".to_string(),
        Some(output) => truncate_chars(&serde_json::to_string(output).unwrap_or_default(), 60),
    }
}

async fn cancel(client: &WsClient, json: bool, run_id: &str) -> Result<(), CliError> {
    let result = call(
        client,
        "run.cancel",
        object(vec![("run_id", json!(run_id))]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    if result["pending"].as_bool() == Some(true) {
        note(format!(
            "run {run_id} 取消请求已入队（pending，用 run get 确认）"
        ));
    } else {
        note(format!("已取消 run {run_id}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn run_seconds_prefers_logged_timestamps() {
        let run = json!({
            "started_at": "2026-01-01T00:00:00+00:00",
            "ended_at": "2026-01-01T00:00:02.500+00:00",
        });
        assert_eq!(run_seconds(&run), Some(2.5));
    }

    #[test]
    fn run_seconds_is_none_for_unfinished_run() {
        assert_eq!(
            run_seconds(&json!({"started_at": "2026-01-01T00:00:00+00:00"})),
            None
        );
        assert_eq!(run_seconds(&json!({})), None);
    }

    #[test]
    fn node_outcome_prefers_error_then_reason_then_output() {
        assert!(node_outcome(&json!({"error": "boom", "output": {"a": 1}})).contains("boom"));
        assert!(node_outcome(&json!({"reason": "upstream_skipped"})).contains("跳过"));
        assert_eq!(
            node_outcome(&json!({"output": {"ok": true}})),
            "{\"ok\":true}"
        );
        assert_eq!(node_outcome(&json!({"output": null})), "-");
    }
}
