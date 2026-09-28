//! 终端输出与文档 IO 的小工具集。
//!
//! 两条输出纪律（脚本可依赖，集成测试钉住）：
//! 1. **文档走 stdout，说明走 stderr**：`workflow get` / `export` / `run get` /
//!    `run start`（等待模式）把 JSON 文档写到 stdout，进度与提示写到 stderr，
//!    于是 `flow-cli workflow get X > def.json` 拿到的就是纯 JSON；
//! 2. **人类输出与机器输出分离**：`--json` 时每个命令直接打印服务端返回的
//!    原始结果（pretty JSON），各行各业的解析逻辑交给 jq 而不是 CLI。

use std::io::Write;
use std::path::Path;

use serde_json::Value;

use crate::error::{io_err, CliError};

/// 把任意 JSON 值按 2 空格缩进打到 stdout。
pub fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

/// 说明性输出（stderr）：进度、提示、人类可读摘要。
pub fn note(message: impl AsRef<str>) {
    let mut stderr = std::io::stderr();
    let _ = writeln!(stderr, "{}", message.as_ref());
}

/// 读入一段文本：`-` 表示标准输入，其余按文件路径。
pub fn read_text(source: &str) -> Result<String, CliError> {
    if source == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|err| io_err("读取标准输入失败", err))?;
        Ok(buf)
    } else {
        std::fs::read_to_string(source)
            .map_err(|err| io_err(&format!("读取文件失败 {source}"), err))
    }
}

/// 解析 JSON 文本，失败时带上出处（文件路径 / 参数名）方便定位。
pub fn parse_json(text: &str, what: &str) -> Result<Value, CliError> {
    serde_json::from_str(text)
        .map_err(|err| CliError::local(format!("{what} 不是合法 JSON：{err}")))
}

/// `--input` 类参数的三种写法：内联 JSON、`@文件`、`-`（标准输入）。
pub fn load_json_arg(value: &str, what: &str) -> Result<Value, CliError> {
    if let Some(path) = value.strip_prefix('@') {
        let text = read_text(path)?;
        parse_json(&text, what)
    } else if value == "-" {
        let text = read_text("-")?;
        parse_json(&text, what)
    } else {
        parse_json(value, what)
    }
}

/// 写文档：给 `-o PATH` 时落盘（父目录不存在则创建，覆盖写），缺省打 stdout。
pub fn write_document(target: Option<&str>, content: &str) -> Result<(), CliError> {
    match target {
        Some(path) => {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|err| {
                        io_err(&format!("创建目录失败 {}", parent.display()), err)
                    })?;
                }
            }
            std::fs::write(path, content)
                .map_err(|err| io_err(&format!("写入文件失败 {path}"), err))?;
            note(format!("已写入 {path}"));
            Ok(())
        }
        None => {
            println!("{content}");
            Ok(())
        }
    }
}

/// 渲染一张左对齐表格（列宽按显示宽度算，CJK 不歪）。
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let columns = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| display_width(h)).collect();
    for row in rows {
        for (index, cell) in row.iter().enumerate().take(columns) {
            widths[index] = widths[index].max(display_width(cell));
        }
    }
    let mut out = String::new();
    let header_cells: Vec<String> = headers.iter().map(|h| (*h).to_string()).collect();
    out.push_str(&pad_row(&header_cells, &widths));
    for row in rows {
        out.push('\n');
        out.push_str(&pad_row(row, &widths));
    }
    out
}

fn pad_row(cells: &[String], widths: &[usize]) -> String {
    // 列间两空格；最后一列不补空格（避免拖尾空白进管道）
    let mut out = String::new();
    for (index, cell) in cells.iter().enumerate() {
        let width = widths.get(index).copied().unwrap_or(0);
        let padding = " ".repeat(width.saturating_sub(display_width(cell)));
        out.push_str(cell);
        out.push_str(&padding);
        if index + 1 < cells.len() {
            out.push_str("  ");
        }
    }
    out
}

/// 字符串的终端显示宽度：东亚宽字符（CJK/全角/谚文/假名/常见 emoji）算 2 列。
/// 只为表格对齐服务，不追求覆盖 Unicode 全部组合字符。
pub fn display_width(text: &str) -> usize {
    text.chars().map(|c| if is_wide(c) { 2 } else { 1 }).sum()
}

fn is_wide(c: char) -> bool {
    matches!(c,
        '\u{1100}'..='\u{115F}'
        | '\u{2E80}'..='\u{303E}'
        | '\u{3041}'..='\u{33FF}'
        | '\u{3400}'..='\u{4DBF}'
        | '\u{4E00}'..='\u{9FFF}'
        | '\u{A000}'..='\u{A4CF}'
        | '\u{AC00}'..='\u{D7A3}'
        | '\u{F900}'..='\u{FAFF}'
        | '\u{FE10}'..='\u{FE19}'
        | '\u{FE30}'..='\u{FE6F}'
        | '\u{FF00}'..='\u{FF60}'
        | '\u{FFE0}'..='\u{FFE6}'
        | '\u{1F300}'..='\u{1F64F}'
        | '\u{1F900}'..='\u{1F9FF}'
        | '\u{20000}'..='\u{3FFFD}'
    )
}

/// 按字符数截断（不是字节）：超长补 `…`，CJK 表格里不会被截出半个字。
pub fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// 把 id 截成 12 字符的短形式（`docker ps` 惯例），表格里够认、够贴。
pub fn short_id(id: &str) -> String {
    truncate_chars(id, 12)
}

/// 服务端 RFC3339 时间 → 本地时区可读串；解析失败（旧格式）原样返回。
pub fn local_time(iso: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(iso)
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|_| iso.to_string())
}

/// run 耗时的人类可读形式（秒，保留一位小数）。
pub fn duration_text(seconds: f64) -> String {
    format!("{seconds:.1}s")
}

/// 危险操作前的确认。stdin 不是终端时直接拒绝（CI 里不能挂起等输入），
/// 明确回答 y/yes 才放行；其余（含空行）视为否。
pub fn confirm(prompt: &str) -> Result<bool, CliError> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(CliError::local(format!(
            "{prompt} 非交互环境（stdin 非终端）必须显式传 -y/--yes"
        )));
    }
    eprint!("{prompt} [y/N] ");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|err| io_err("读取确认输入失败", err))?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "YES" | "Yes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_arg_forms_are_documented() {
        assert_eq!(load_json_arg("{\"a\":1}", "input").unwrap(), json!({"a":1}));
        // '@' 前缀走文件分支：错误来自文件读取，不是 JSON 解析
        assert!(load_json_arg("@/nonexistent/x.json", "input")
            .unwrap_err()
            .to_string()
            .contains("读取文件失败"));
    }

    #[test]
    fn table_aligns_cjk_and_ascii() {
        let rendered = table(
            &["名称", "ID"],
            &[
                vec!["脚本".to_string(), "abc".to_string()],
                vec!["wwww".to_string(), "de".to_string()],
            ],
        );
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        // 每行显示宽度一致（CJK 算 2 列后才对齐）
        assert_eq!(display_width(lines[0]), display_width(lines[1]));
        assert_eq!(display_width(lines[1]), display_width(lines[2]));
    }

    #[test]
    fn truncate_counts_chars_not_bytes() {
        assert_eq!(truncate_chars("工作流", 10), "工作流");
        // 截到 3 个字符 = 2 个实字符 + 省略号（不是按字节切出半个字）
        assert_eq!(truncate_chars("工作流引擎", 3), "工作…");
        assert_eq!(truncate_chars("abcdef", 3), "ab…");
    }

    #[test]
    fn short_id_is_stable_prefix() {
        let id = "0197c1f6-2a3b-7c4d-8e9f-0a1b2c3d4e5f";
        // 12 列 = 11 个字符 + 省略号；不足 12 列原样返回
        assert_eq!(short_id(id), "0197c1f6-2a…");
        assert_eq!(short_id("abc"), "abc");
    }
}
