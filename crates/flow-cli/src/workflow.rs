//! `flow-cli workflow`：工作流定义的 CRUD 与导入导出。
//!
//! 与服务端方法的对应关系（DESIGN.md §9，一份实现两种后端通用）：
//!
//! | 子命令 | RPC 方法 |
//! |---|---|
//! | `list` / `get` / `versions` | `workflow.list` / `workflow.get` / `workflow.versions` |
//! | `create` / `update` / `publish` / `delete` | 同名方法（update 与 publish 都强制 validate） |
//! | `import` | `workflow.list`（按 name 反查）+ `create` / `update` / `publish` |
//! | `export` | `workflow.get`（+ `workflow.list` 反查 name） |
//!
//! **导入导出为什么不新增 RPC 方法**：upsert 的三步组合是纯客户端编排，
//! 服务端语义（不可变版本快照、只有 published 可执行、有 run 拒删）一条不变。
//! 把编排逻辑下沉成新方法 = 在服务端养第二份「导入」规则，两边必然漂移。
//!
//! **失败不回滚**：import 的 create 是已发生的副作用，之后的 update / publish
//! 被拒时留下一个没有版本的 workflow 壳（latest 0、published null，不可能被执行）。
//! 重新 import 同一 name 会复用这个壳继续追加版本，不需要用户手工清理。

use std::path::Path;

use jsonrpsee::ws_client::WsClient;
use serde_json::{json, Value};

use crate::cli::WorkflowCommand;
use crate::client::{call, object};
use crate::error::CliError;
use crate::output::{local_time, note, parse_json, print_json, read_text, table, truncate_chars};

pub async fn dispatch(
    client: &WsClient,
    json: bool,
    command: WorkflowCommand,
) -> Result<(), CliError> {
    match command {
        WorkflowCommand::List => list(client, json).await,
        WorkflowCommand::Get {
            workflow_id,
            version,
        } => {
            let id = resolve_workflow_id(client, &workflow_id).await?;
            get(client, json, &id, version).await
        }
        WorkflowCommand::Versions { workflow_id } => {
            let id = resolve_workflow_id(client, &workflow_id).await?;
            versions(client, json, &id).await
        }
        WorkflowCommand::Create { name } => create(client, json, &name).await,
        WorkflowCommand::Update { workflow_id, file } => {
            let id = resolve_workflow_id(client, &workflow_id).await?;
            update(client, json, &id, &file).await
        }
        WorkflowCommand::Publish {
            workflow_id,
            version,
        } => {
            let id = resolve_workflow_id(client, &workflow_id).await?;
            publish(client, json, &id, version).await
        }
        WorkflowCommand::Delete { workflow_id, yes } => {
            let id = resolve_workflow_id(client, &workflow_id).await?;
            delete(client, json, &id, yes).await
        }
        WorkflowCommand::Import {
            file,
            name,
            no_publish,
        } => import(client, json, &file, name.as_deref(), no_publish).await,
        WorkflowCommand::Export {
            workflow_id,
            version,
            output,
        } => {
            let id = resolve_workflow_id(client, &workflow_id).await?;
            export(client, json, &id, version, output.as_deref()).await
        }
    }
}

/// name 或 workflow_id → workflow_id。全部子命令共用：用户不该记 uuid，
/// 但 id 也照常可用。两者都不中时**原样返回**（当 id 用），让服务端的
/// -32011 做最终裁决——CLI 不预判存在性，避免两套「不存在」语义。
pub async fn resolve_workflow_id(client: &WsClient, name_or_id: &str) -> Result<String, CliError> {
    let listed = call(client, "workflow.list.view", object(vec![])).await?;
    let matched = listed["workflows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .find(|workflow| {
            workflow["workflow_id"].as_str() == Some(name_or_id)
                || workflow["name"].as_str() == Some(name_or_id)
        })
        .and_then(|workflow| workflow["workflow_id"].as_str())
        .map(str::to_string);
    Ok(matched.unwrap_or_else(|| name_or_id.to_string()))
}

async fn list(client: &WsClient, json: bool) -> Result<(), CliError> {
    let result = call(client, "workflow.list.view", object(vec![])).await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    let workflows = result["workflows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let rows: Vec<Vec<String>> = workflows
        .iter()
        .map(|workflow| {
            vec![
                workflow["workflow_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                workflow["name"].as_str().unwrap_or_default().to_string(),
                format!(
                    "v{}",
                    workflow["latest_version"].as_i64().unwrap_or_default()
                ),
                workflow["published_version"]
                    .as_i64()
                    .map(|version| format!("v{version}"))
                    .unwrap_or_else(|| "-".to_string()),
                local_time(workflow["created_at"].as_str().unwrap_or_default()),
            ]
        })
        .collect();
    note(format!("共 {} 个工作流", workflows.len()));
    println!(
        "{}",
        table(
            &["WORKFLOW_ID", "NAME", "LATEST", "PUBLISHED", "CREATED AT"],
            &rows
        )
    );
    Ok(())
}

async fn get(
    client: &WsClient,
    json: bool,
    workflow_id: &str,
    version: Option<i64>,
) -> Result<(), CliError> {
    let mut params = vec![("workflow_id", json!(workflow_id))];
    if let Some(version) = version {
        params.push(("version", json!(version)));
    }
    let result = call(client, "workflow.get.view", object(params)).await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    // 说明走 stderr，stdout 只留 definition：`flow-cli workflow get X > def.json` 即纯 JSON
    let published = result["published_version"]
        .as_i64()
        .map(|version| format!("v{version}"))
        .unwrap_or_else(|| "无".to_string());
    note(format!(
        "# workflow {} v{}（{}｜最新已发布 {}）",
        workflow_id,
        result["version"].as_i64().unwrap_or_default(),
        result["status"].as_str().unwrap_or_default(),
        published
    ));
    print_json(result.get("definition").unwrap_or(&Value::Null));
    Ok(())
}

async fn versions(client: &WsClient, json: bool, workflow_id: &str) -> Result<(), CliError> {
    let result = call(
        client,
        "workflow.versions",
        object(vec![("workflow_id", json!(workflow_id))]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    let versions = result["versions"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let rows: Vec<Vec<String>> = versions
        .iter()
        .map(|version| {
            vec![
                format!("v{}", version["version"].as_i64().unwrap_or_default()),
                version["status"].as_str().unwrap_or_default().to_string(),
                truncate_chars(version["checksum"].as_str().unwrap_or_default(), 12),
                local_time(version["created_at"].as_str().unwrap_or_default()),
            ]
        })
        .collect();
    note(format!(
        "workflow {workflow_id} 共 {} 个版本",
        versions.len()
    ));
    println!(
        "{}",
        table(&["VERSION", "STATUS", "CHECKSUM", "CREATED AT"], &rows)
    );
    Ok(())
}

async fn create(client: &WsClient, json: bool, name: &str) -> Result<(), CliError> {
    let result = call(
        client,
        "workflow.create",
        object(vec![("name", json!(name))]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    note(format!(
        "已创建 workflow {}（还没有定义：workflow update --file … 再 publish）",
        result["workflow_id"].as_str().unwrap_or_default()
    ));
    Ok(())
}

async fn update(
    client: &WsClient,
    json: bool,
    workflow_id: &str,
    file: &str,
) -> Result<(), CliError> {
    let definition = load_definition(file)?;
    let result = call(
        client,
        "workflow.update",
        object(vec![
            ("workflow_id", json!(workflow_id)),
            ("definition", definition),
        ]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    note(format!(
        "已保存 workflow {} v{}（还需 publish 才能执行）",
        workflow_id,
        result["version"].as_i64().unwrap_or_default()
    ));
    Ok(())
}

async fn publish(
    client: &WsClient,
    json: bool,
    workflow_id: &str,
    version: i64,
) -> Result<(), CliError> {
    let result = call(
        client,
        "workflow.publish",
        object(vec![
            ("workflow_id", json!(workflow_id)),
            ("version", json!(version)),
        ]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    note(format!("已发布 workflow {workflow_id} v{version}"));
    Ok(())
}

async fn delete(
    client: &WsClient,
    json: bool,
    workflow_id: &str,
    yes: bool,
) -> Result<(), CliError> {
    if !yes && !crate::output::confirm(&format!("删除 workflow {workflow_id}？"))? {
        return Err(CliError::local("已取消"));
    }
    let result = call(
        client,
        "workflow.delete",
        object(vec![("workflow_id", json!(workflow_id))]),
    )
    .await?;
    if json {
        print_json(&result);
        return Ok(());
    }
    note(format!("已删除 workflow {workflow_id}"));
    Ok(())
}

/// 导入：按 name upsert（同名 = 追加新版本，Definition 是不可变快照），默认发布。
async fn import(
    client: &WsClient,
    json: bool,
    file: &str,
    name_override: Option<&str>,
    no_publish: bool,
) -> Result<(), CliError> {
    let text = read_text(file)?;
    let document = parse_json(&text, &format!("定义文件 {file}"))?;
    let (name, definition) = split_document(&document, file, name_override)?;

    // 反查同名工作流：存在则追加版本（run 钉死旧版本，不受影响），否则新建
    let listed = call(client, "workflow.list.view", object(vec![])).await?;
    let existing = listed["workflows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .find(|workflow| workflow["name"].as_str() == Some(name.as_str()))
        .and_then(|workflow| workflow["workflow_id"].as_str())
        .map(str::to_string);
    let (workflow_id, created) = match existing {
        Some(workflow_id) => (workflow_id, false),
        None => {
            let result = call(
                client,
                "workflow.create",
                object(vec![("name", json!(name))]),
            )
            .await?;
            (
                result["workflow_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                true,
            )
        }
    };

    let saved = call(
        client,
        "workflow.update",
        object(vec![
            ("workflow_id", json!(workflow_id)),
            ("definition", definition),
        ]),
    )
    .await?;
    let version = saved["version"].as_i64().unwrap_or_default();

    let status = if no_publish {
        "draft".to_string()
    } else {
        call(
            client,
            "workflow.publish",
            object(vec![
                ("workflow_id", json!(workflow_id)),
                ("version", json!(version)),
            ]),
        )
        .await?;
        "published".to_string()
    };

    let result = json!({
        "workflow_id": workflow_id,
        "name": name,
        "version": version,
        "status": status,
        "created": created,
    });
    if json {
        print_json(&result);
        return Ok(());
    }
    note(format!(
        "导入 {name} → workflow {workflow_id} v{version}（{}{}）",
        if created { "新建" } else { "追加版本" },
        if no_publish {
            "，未发布（draft）".to_string()
        } else {
            "，已发布".to_string()
        }
    ));
    Ok(())
}

/// 导出：`{name, definition}` 信封（外加 version/status 元数据，import 时忽略）。
/// 信封格式与 `examples/*.workflow.json`、`examples/run-workflow.mjs` 一致，
/// 导出的文件可以直接喂给 run-workflow.mjs 或再次 import。
async fn export(
    client: &WsClient,
    json: bool,
    workflow_id: &str,
    version: Option<i64>,
    output: Option<&str>,
) -> Result<(), CliError> {
    let mut params = vec![("workflow_id", json!(workflow_id))];
    if let Some(version) = version {
        params.push(("version", json!(version)));
    }
    let detail = call(client, "workflow.get.view", object(params)).await?;

    // workflow.get 不返回 name；从 list 反查（RPC 面没有 by-id 单查），
    // 查不到（并发删除）时退回用 id 当 name，导出不因此失败。
    let listed = call(client, "workflow.list.view", object(vec![])).await?;
    let name = listed["workflows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .find(|workflow| workflow["workflow_id"].as_str() == Some(workflow_id))
        .and_then(|workflow| workflow["name"].as_str())
        .unwrap_or(workflow_id)
        .to_string();

    let document = if json {
        detail
    } else {
        json!({
            "name": name,
            "workflow_id": detail["workflow_id"],
            "version": detail["version"],
            "status": detail["status"],
            "definition": detail["definition"],
        })
    };
    let text = format!(
        "{}\n",
        serde_json::to_string_pretty(&document).unwrap_or_default()
    );
    crate::output::write_document(output, &text)?;
    if output.is_none() {
        // 写到 stdout 时提示走 stderr，管道里不混进文档
        note(format!(
            "# workflow {} v{}（{}）",
            workflow_id,
            document["version"].as_i64().unwrap_or_default(),
            document["status"].as_str().unwrap_or_default()
        ));
    }
    Ok(())
}

/// 读一份定义文件并做最浅的形状检查（对象 + nodes 数组）。
/// 真正的规则仍在服务端 `Definition::validate`（单一裁决），这里只把
/// 「把字符串当定义传上去」这类明显用法错误挡在本地。
fn load_definition(file: &str) -> Result<Value, CliError> {
    let text = read_text(file)?;
    let definition = parse_json(&text, &format!("定义文件 {file}"))?;
    check_definition_shape(&definition)?;
    Ok(definition)
}

fn check_definition_shape(definition: &Value) -> Result<(), CliError> {
    let Some(object) = definition.as_object() else {
        return Err(CliError::local(
            "定义必须是 JSON 对象：{ \"nodes\": [...], \"edges\": [...] }",
        ));
    };
    if !object.get("nodes").is_some_and(Value::is_array) {
        return Err(CliError::local(
            "定义缺少 nodes 数组：{ \"nodes\": [...], \"edges\": [...] }",
        ));
    }
    if !object.get("edges").is_some_and(Value::is_array) {
        return Err(CliError::local(
            "定义缺少 edges 数组：{ \"nodes\": [...], \"edges\": [...] }",
        ));
    }
    Ok(())
}

/// 拆导入文件：`{name, definition}` 信封（examples 的格式）或裸 definition。
/// name 解析优先级：--name > 信封 name > 文件名去扩展名（`-` 读 stdin 时无名字可用）。
fn split_document(
    document: &Value,
    file: &str,
    name_override: Option<&str>,
) -> Result<(String, Value), CliError> {
    let definition = match document.get("definition") {
        Some(definition) => definition.clone(),
        None => document.clone(),
    };
    check_definition_shape(&definition)?;

    let name = name_override
        .map(str::to_string)
        .or_else(|| {
            document
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| file_stem(file))
        .ok_or_else(|| {
            CliError::local(format!(
                "无法确定工作流名：文件 {file} 没有 name 字段且 stdin 无文件名，请用 --name 指定"
            ))
        })?;
    Ok((name, definition))
}

/// `github-trending.workflow.json` → `github-trending`（与 examples/run-workflow.mjs 同规则）。
fn file_stem(file: &str) -> Option<String> {
    if file == "-" {
        return None;
    }
    let file_name = Path::new(file).file_name()?.to_string_lossy().to_string();
    let stem = file_name
        .strip_suffix(".json")
        .unwrap_or(file_name.as_str());
    let stem = stem.strip_suffix(".workflow").unwrap_or(stem);
    Some(stem.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn definition() -> Value {
        json!({
            "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
            "edges": [{"from": "start", "to": "end"}]
        })
    }

    #[test]
    fn envelope_document_yields_name_and_definition() {
        let doc = json!({"name": "demo", "definition": definition(), "version": 7});
        let (name, definition) = split_document(&doc, "x.json", None).unwrap();
        assert_eq!(name, "demo");
        assert_eq!(definition["nodes"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn bare_definition_falls_back_to_file_stem() {
        let (name, definition) = split_document(&definition(), "demo.workflow.json", None).unwrap();
        assert_eq!(name, "demo");
        assert_eq!(definition["nodes"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn name_override_beats_envelope_name() {
        let doc = json!({"name": "from-file", "definition": definition()});
        let (name, _) = split_document(&doc, "x.json", Some("from-flag")).unwrap();
        assert_eq!(name, "from-flag");
    }

    #[test]
    fn stdin_without_name_is_an_error_not_a_silent_default() {
        let err = split_document(&definition(), "-", None).unwrap_err();
        assert!(err.to_string().contains("--name"), "{err}");
    }

    #[test]
    fn non_object_definition_is_rejected_locally() {
        let err = split_document(&json!("not an object"), "x.json", Some("n")).unwrap_err();
        assert!(err.to_string().contains("nodes"), "{err}");
    }

    #[test]
    fn truncation_is_display_only() {
        // 说明性文字里的截断不参与逻辑，纯展示
        assert_eq!(truncate_chars("0123456789abcdef", 12), "0123456789a…");
    }
}
