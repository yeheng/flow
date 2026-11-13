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
//! 语义缺省与服务端一致：缺 `semantics` 时按 **v2** 解释（服务端在校验前
//! 注入 `semantics: 2`；引擎反序列化的缺省是 v1，那是历史定义的解释口径，
//! 不该被离线工具当默认）。
//!
//! 边界：这里只跑**建图期**校验（`flow-engine/src/model.rs`）。以下都在服务端、
//! 本工具管不到：
//! - 密钥存在性、`input_schema` 的实际取值、子流程依赖闭包；
//! - **插件声明的动态节点类型**：离线没有 manifest，插件类型会被当成
//!   「类型未知」拒绝；含插件节点的定义只能在服务端校验；
//! - **权限块的作用域**：权限块本身的形状离线可验，但「含副作用节点却没有
//!   权限块 → run.start 被拒」是运行期门（legacy 迁移），离线查不出来。

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
    let mut definition = value.get("definition").cloned().unwrap_or(value);
    // 与服务端归一化同口径：缺省补 `semantics: 2`（服务端在校验前注入），
    // 否则离线按 v1 判、导入按 v2 判，两边结论会不一致。
    if let Some(object) = definition.as_object_mut() {
        object.entry("semantics").or_insert(serde_json::json!(2));
    }
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
