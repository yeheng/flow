//! 离线工作流定义校验器：不启动服务就跑完与服务端同一份建图校验。
//!
//! ```sh
//! cargo run -q -p flow-engine --example validate_def -- my-flow.workflow.json
//! cat my-flow.workflow.json | cargo run -q -p flow-engine --example validate_def
//! ```
//!
//! 输入兼容两种文件形态：`{name, definition}` 信封（`workflow export` 与
//! `examples/*.workflow.json` 的格式）与裸 definition。
//!
//! 退出码：0 = 通过，1 = 校验失败（`Definition::validate` 拒绝），2 = 文件
//! 不是合法 JSON 或结构不符。**报错文案与服务端 `workflow.update` 完全一致**
//! ——两边调的是同一个 `Definition::validate`，所以这里红了，导入一定也红。
//!
//! 边界：这里只跑**建图期**校验（`flow-engine/src/model.rs`）。密钥存在性、
//! `input_schema` 的实际取值、子流程依赖闭包都在服务端校验，本工具管不到。

use std::io::Read;

use flow_engine::model::Definition;

fn main() -> std::process::ExitCode {
    let source = match read_source() {
        Ok(source) => source,
        Err(message) => return fail(2, &message),
    };
    let value: serde_json::Value = match serde_json::from_str(&source) {
        Ok(value) => value,
        Err(err) => return fail(2, &format!("JSON 解析失败：{err}")),
    };
    // 信封形态优先；没有 `definition` 键时整体就是 definition。
    let definition = value.get("definition").cloned().unwrap_or(value);
    match serde_json::from_value::<Definition>(definition) {
        Ok(definition) => match definition.validate() {
            Ok(()) => {
                println!(
                    "OK: 定义通过校验（{} 节点 / {} 边）",
                    definition.nodes.len(),
                    definition.edges.len()
                );
                std::process::ExitCode::SUCCESS
            }
            Err(message) => fail(1, &format!("校验失败：{message}")),
        },
        Err(err) => fail(2, &format!("定义结构不符：{err}")),
    }
}

/// 命令行给文件路径则读文件，否则读 stdin（便于管道与heredoc）。
fn read_source() -> Result<String, String> {
    match std::env::args().nth(1) {
        Some(path) => std::fs::read_to_string(&path).map_err(|err| format!("读不到 {path}：{err}")),
        None => {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .map_err(|err| format!("读 stdin 失败：{err}"))?;
            if text.trim().is_empty() {
                return Err("输入为空：给一个文件路径或从stdin 管道喂 JSON".to_string());
            }
            Ok(text)
        }
    }
}

fn fail(code: u8, message: &str) -> std::process::ExitCode {
    eprintln!("{message}");
    std::process::ExitCode::from(code)
}