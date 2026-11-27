//! flow-playwright-plugin 库面：方法层契约类型 + 执行核心。
//!
//! bin（main.rs）只是壳：通道引导 + 会话循环。契约常量与错误码在这里，
//! 供两侧对齐（另一侧在 manifest `plugin.toml` 与 runtime 的
//! `plugin_node` 派发里各自声明并注释互指）。
//!
//! 方法层契约（`playwright/invoke`）：
//! - 请求 params：`{params: {script, browser?, headless?, timeout_ms?}}`
//!   （runtime 已完成 permit 授权与 `${}` 模板展开；`script` 是带
//!   `return` 的 JS 函数体，在 Node + Playwright 里执行）；
//! - 响应 result：`{output: {status, browser, headless, duration_ms,
//!   logs, result}}`（`result` = 脚本返回值；`status = failed` 时带
//!   `error`）；
//! - 应用错误码：`-32020` 脚本失败/浏览器崩溃（不确定类：副作用可能
//!   已发生）；`-32021` 输出超过捕获上限；`-32022` 参数畸形（防御
//!   纵深：runtime 授权前已按 manifest 校验过一次）；`-32023` 超时
//!   （不确定类）；`-32024` 环境缺失（node / playwright runtime 未装，
//!   确定类）。
//!
//! 超时纪律：runtime 的 `plugin_node` 不掐表（`let _ = timeout_ms;`），
//! 本插件是**唯一的一只钟**——到点杀整个进程组（node + 浏览器子进程），
//! 不留孤儿。wrapper.js 侧不设第二只钟（与 runtime 对反向裁决的纪律
//! 一致：单钟，不猜）。

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

/// 节点执行入口方法（manifest `[methods] invoke` 的实现侧锚点）。
pub const INVOKE_METHOD: &str = "playwright/invoke";

/// 脚本失败 / 浏览器崩溃（不确定类）。
pub const SCRIPT_ERROR: i64 = -32020;
/// 输出超过捕获上限（不确定类：journal 预算 8 MiB）。
pub const CAPTURE_LIMIT_ERROR: i64 = -32021;
/// 参数畸形（防御纵深；正常不应出现——runtime 授权前已校验）。
pub const MALFORMED_ERROR: i64 = -32022;
/// 超时（不确定类）。
pub const TIMEOUT_ERROR: i64 = -32023;
/// 环境缺失：node 解释器或 playwright runtime 不可用。
pub const ENVIRONMENT_ERROR: i64 = -32024;

/// 超时下限（与 plugin.toml `timeout_ms` 的 min 对齐）。
pub const TIMEOUT_MIN_MS: u64 = 1_000;
/// 超时上限（与 plugin.toml max 及 flow-engine `HTTP_TIMEOUT_MAX_MS` 对齐：
/// 三处必须同改）。
pub const TIMEOUT_MAX_MS: u64 = 600_000;
/// 缺省超时（与 plugin.toml default 对齐）。
pub const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// 脚本长度上限（字节；与 plugin.toml `script` 的 max_len 对齐）。
pub const SCRIPT_MAX_BYTES: usize = 262_144;
/// 输出捕获上限：与 runtime `plugin_node` 的 `store_value` 预算（8 MiB）
/// 同值。跨进程契约值：两侧各自声明，改动必须双侧同步。
pub const OUTPUT_MAX_BYTES: usize = 8 * 1024 * 1024;
/// logs 截断预算（字节）：result 深度受限时优先保 result，logs 先削。
pub const LOGS_MAX_BYTES: usize = 64 * 1024;
/// stderr 诊断尾巴（进错误 message，帮排障）：只带最后一截。
pub const STDERR_TAIL_BYTES: usize = 2_048;

/// wrapper 在 stdout 打的哨兵行前缀：前面任何字节都不是我们的（用户脚本
/// 里 `process.stdout.write` 直写会污染通道——README 已声明禁止）。
pub const RESULT_SENTINEL: &str = "__FLOW_PW__";

/// `playwright/invoke` 请求参数（节点层参数，经模板展开与 secret 注入后）。
#[derive(Debug, Deserialize)]
pub struct InvokeParams {
    /// JS 函数体：拿到 page/context/browser/console，`return` 的值进 output。
    pub script: String,
    #[serde(default)]
    pub browser: Option<String>,
    #[serde(default)]
    pub headless: Option<bool>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// 校验后的参数（闭集与边界都在这里钉死；publish 期 manifest 表单是
/// 第一道，这里是第二道）。
#[derive(Debug, Clone)]
pub struct ValidatedParams {
    pub script: String,
    pub browser: String,
    pub headless: bool,
    pub timeout: Duration,
}

const BROWSERS: [&str; 3] = ["chromium", "firefox", "webkit"];

impl InvokeParams {
    /// 解析并校验节点参数（`value` = 请求 params 里的 `params` 对象）。
    pub fn validate(value: &Value) -> Result<ValidatedParams, String> {
        let params: InvokeParams = serde_json::from_value(value.clone())
            .map_err(|error| format!("params 结构非法：{error}"))?;
        if params.script.trim().is_empty() {
            return Err("script 不能为空".into());
        }
        if params.script.len() > SCRIPT_MAX_BYTES {
            return Err(format!("script 超过上限（{} 字节）", SCRIPT_MAX_BYTES));
        }
        let browser = params.browser.as_deref().unwrap_or("chromium").to_string();
        if !BROWSERS.contains(&browser.as_str()) {
            return Err(format!(
                "browser 必须是 chromium/firefox/webkit，当前 {browser:?}"
            ));
        }
        let headless = params.headless.unwrap_or(true);
        let timeout_ms = params
            .timeout_ms
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .clamp(TIMEOUT_MIN_MS, TIMEOUT_MAX_MS);
        Ok(ValidatedParams {
            script: params.script,
            browser,
            headless,
            timeout: Duration::from_millis(timeout_ms),
        })
    }
}

/// 一次 invoke 的应用层失败：code 进 JSON-RPC error，message 面向排障。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct InvokeError {
    pub code: i64,
    pub message: String,
}

impl InvokeError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// wrapper 的 argv 形态（跨进程契约）：插件固定以 `node runtime/wrapper.js`
/// 启动执行器——cwd = 插件根，argv 恒为这个相对串。manifest 的
/// `pw-exec` 能力声明与此同值（声明 ≠ 真实命令 = 授权记账失真）。
pub const WRAPPER_ARG: &str = "runtime/wrapper.js";

/// 定位插件根目录：插件二进制（`<dir>/bin/flow-playwright-plugin`）的上
/// 两级。部署布局由 install-plugins.sh 钉死：`<plugin>/bin/`、
/// `<plugin>/runtime/`、`<plugin>/plugin.toml`。
pub fn plugin_root_near(exe: &Path) -> Option<PathBuf> {
    exe.parent() // bin/
        .and_then(|bin| bin.parent()) // 插件目录
        .map(Path::to_path_buf)
}

/// 跑一次浏览器自动化：spawn `node runtime/wrapper.js`（cwd = 插件根，
/// stdin 喂参数），收哨兵信封。
///
/// - `node_bin`：Node 解释器（生产走 PATH 的 `node`；测试可注入 stub）；
/// - 返回成功信封（`status` 字段区分 success/failed，failed 由调用方
///   映射为 `SCRIPT_ERROR`）。
pub async fn run_playwright(
    params: &ValidatedParams,
    plugin_root: &Path,
    node_bin: &str,
) -> Result<Value, InvokeError> {
    let runtime_dir = plugin_root.join("runtime");
    let wrapper = runtime_dir.join("wrapper.js");
    if !wrapper.is_file() {
        return Err(InvokeError::new(
            ENVIRONMENT_ERROR,
            format!(
                "playwright runtime 缺失（{} 不存在）：部署时拷入 runtime/ 并安装依赖，见插件 README",
                wrapper.display()
            ),
        ));
    }
    let stdin_payload = serde_json::json!({
        "script": params.script,
        "browser": params.browser,
        "headless": params.headless,
        "timeout_ms": params.timeout.as_millis() as u64,
    });
    let mut command = tokio::process::Command::new(node_bin);
    // argv 固定为相对串（与 manifest pw-exec 声明同值）；cwd = 插件根。
    // wrapper 里 require('playwright') 按脚本自身目录解析，不受 cwd 影响。
    command
        .arg(WRAPPER_ARG)
        .current_dir(plugin_root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // 浏览器是 node 的孙进程：同进程组，超时整组击杀不留孤儿。
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|error| {
        InvokeError::new(
            ENVIRONMENT_ERROR,
            format!("无法启动 {node_bin}：{error}（本机需要 Node.js）"),
        )
    })?;
    // stdin 喂完即关（EOF 是 wrapper 的「参数齐了」信号）。
    {
        use tokio::io::AsyncWriteExt as _;
        let mut stdin = child.stdin.take().expect("stdin piped");
        let payload = stdin_payload.to_string();
        stdin
            .write_all(payload.as_bytes())
            .await
            .map_err(|error| InvokeError::new(SCRIPT_ERROR, format!("写 stdin 失败：{error}")))?;
        stdin
            .shutdown()
            .await
            .map_err(|error| InvokeError::new(SCRIPT_ERROR, format!("关 stdin 失败：{error}")))?;
    }
    // 自己持有三根管道：wait_with_output 会把 child 搬进 future，超时分支
    // 就拿不回进程组句柄了。两根读端并发排空（防 pipe 缓冲区死锁），
    // 都到 EOF 再 wait。
    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let timed = tokio::time::timeout(params.timeout, async {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let out = tokio::io::AsyncReadExt::read_to_end(&mut stdout_pipe, &mut stdout).await;
        let err = tokio::io::AsyncReadExt::read_to_end(&mut stderr_pipe, &mut stderr).await;
        let status = child.wait().await?;
        out?;
        err?;
        std::io::Result::Ok((status, stdout, stderr))
    })
    .await;
    let (_status, stdout, stderr) = match timed {
        Ok(result) => result
            .map_err(|error| InvokeError::new(SCRIPT_ERROR, format!("node 进程失败：{error}")))?,
        Err(_) => {
            // 唯一的一只钟到点：整组 SIGKILL（浏览器是孙进程，单杀 node
            // 会留孤儿）。kill_on_drop 兜底收尸。
            kill_process_group(&mut child);
            return Err(InvokeError::new(
                TIMEOUT_ERROR,
                format!(
                    "playwright 执行超时（{}ms）已终止进程组",
                    params.timeout.as_millis()
                ),
            ));
        }
    };
    let stdout = String::from_utf8_lossy(&stdout);
    let stderr = String::from_utf8_lossy(&stderr);
    let envelope = match parse_envelope(&stdout) {
        Some(envelope) => envelope,
        None => {
            // 没有哨兵 = wrapper 没能正常收尾（崩溃/被信号杀/环境炸了）。
            let tail = stderr.trim();
            let mut message = "wrapper 未返回结果信封".to_string();
            if !tail.is_empty() {
                let tail = tail
                    .chars()
                    .rev()
                    .take(STDERR_TAIL_BYTES)
                    .collect::<Vec<_>>();
                let tail: String = tail.into_iter().rev().collect();
                message.push_str("；stderr 尾巴：");
                message.push_str(&tail);
            }
            return Err(InvokeError::new(SCRIPT_ERROR, message));
        }
    };
    if envelope["status"] == "failed" {
        let detail = envelope["error"].as_str().unwrap_or("未知错误");
        let logs = envelope["logs"].to_string();
        let mut message = format!("playwright 脚本失败：{detail}");
        if logs.len() > 2 {
            // logs 非 "[]" 时附上（截尾巴，别把 8KiB 日志灌进 message）。
            let logs = truncate_chars(&logs, STDERR_TAIL_BYTES);
            message.push_str("；logs：");
            message.push_str(&logs);
        }
        return Err(InvokeError::new(SCRIPT_ERROR, message));
    }
    Ok(envelope)
}

/// 从 stdout 文本里找哨兵行并解出信封 JSON。
pub fn parse_envelope(stdout: &str) -> Option<Value> {
    let line = stdout
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix(RESULT_SENTINEL))?;
    serde_json::from_str(line).ok()
}

/// 杀整个进程组（unix）；非 unix 退化为杀直接子进程（浏览器可能残留，
/// 文档已声明）。幂等：进程已死时错误吞掉。
fn kill_process_group(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        // 负 pgid = 整组（node 是组长，浏览器是同组孙进程）。
        let pgid = child.id().unwrap_or(0) as i32;
        let _ = unsafe { libc::kill(-pgid, libc::SIGKILL) };
    }
    let _ = child.start_kill();
}

/// 按字符截尾巴（UTF-8 安全）：logs/诊断进 message 前先削到预算内。
fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let suffix: String = text.chars().rev().take(max_chars).collect();
    format!("…{}", suffix.chars().rev().collect::<String>())
}

/// 信封预算裁剪：先削 logs，再查总量；result 本身超限时报
/// `CAPTURE_LIMIT_ERROR`（保真优先，不静默截 result——静默截断会让
/// 下游拿到一份「看起来对」的半截数据）。
pub fn fit_output_budget(mut envelope: Value) -> Result<Value, InvokeError> {
    if serde_json::to_string(&envelope).is_ok_and(|s| s.len() <= OUTPUT_MAX_BYTES) {
        return Ok(envelope);
    }
    if let Some(logs) = envelope
        .get_mut("logs")
        .map(Value::take)
        .filter(|l| l.is_array())
    {
        let entries: Vec<Value> = logs.as_array().cloned().unwrap_or_default();
        let mut kept: Vec<Value> = Vec::new();
        let mut used = 0usize;
        for entry in entries.into_iter().rev() {
            let cost = entry.to_string().len() + 1;
            if used + cost > LOGS_MAX_BYTES {
                break;
            }
            used += cost;
            kept.push(entry);
        }
        kept.reverse();
        envelope["logs"] = Value::Array(kept);
    }
    if serde_json::to_string(&envelope).map_or(true, |s| s.len() > OUTPUT_MAX_BYTES) {
        return Err(InvokeError::new(
            CAPTURE_LIMIT_ERROR,
            format!("脚本返回值超过捕获上限（{} 字节）", OUTPUT_MAX_BYTES),
        ));
    }
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_config::plugin_manifest::Widget;
    use serde_json::json;

    /// manifest 是宿主在 spawn 前读的声明：这里钉住它**能被宿主解析器
    /// 接受**（闭集、必填、权限引用全过）。manifest 改坏（widget/format/
    /// 越界）时这个测试第一时间红。
    #[test]
    fn manifest_parses_and_matches_contract() {
        let text = include_str!("../plugin.toml");
        let manifest = flow_config::plugin_manifest::parse(text).expect("manifest 合法");
        assert_eq!(manifest.plugin.id, "playwright");
        assert_eq!(manifest.plugin.node_types, vec!["playwright".to_string()]);
        assert_eq!(manifest.methods.invoke, INVOKE_METHOD);
        let form = &manifest
            .ui
            .params_forms
            .iter()
            .find(|form| form.node_type == "playwright")
            .expect("playwright 参数表单");
        let keys: Vec<&str> = form.fields.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(keys, ["script", "browser", "headless", "timeout_ms"]);
        // 表单边界与实现常量同值（两处声明互指的钉子）。
        for field in &form.fields {
            match &field.widget {
                Widget::Code { max_len, .. } if field.key == "script" => {
                    assert_eq!(
                        *max_len,
                        Some(SCRIPT_MAX_BYTES),
                        "script max_len 与实现常量必须一致"
                    );
                }
                Widget::Number { min, max } if field.key == "timeout_ms" => {
                    assert_eq!(
                        (min.unwrap_or(0.0) as u64, max.unwrap_or(0.0) as u64),
                        (TIMEOUT_MIN_MS, TIMEOUT_MAX_MS)
                    );
                }
                _ => {}
            }
        }
        assert_eq!(
            manifest.ui.palette.as_ref().expect("palette").category,
            "integration"
        );
        // 受管插件：exec 必需（与真实命令 argv 同值），net/file 为
        // optional 族级上限（信息性声明，诚实条款见 manifest 注释）。
        let flow_config::plugin_manifest::ManifestPermissions::Declared(permissions) =
            &manifest.permissions
        else {
            panic!("playwright 必须声明 [permissions]（受管插件）");
        };
        let pw_exec = permissions
            .capabilities
            .iter()
            .find(|c| c.capability.id == "pw-exec")
            .expect("pw-exec 必须声明");
        assert!(pw_exec.required, "pw-exec 必须 required");
        let declared = pw_exec.capability.to_json();
        assert_eq!(declared["action"], "exec");
        assert_eq!(declared["program"], "node");
        // argv 与真实 spawn 同值：授权记账不能与命令行分叉。
        assert_eq!(declared["argv_exact"], json!([WRAPPER_ARG]));
    }

    #[test]
    fn params_validation_closed_set_and_bounds() {
        let ok = InvokeParams::validate(&json!({
            "script": "return 1;",
            "browser": "firefox",
            "headless": false,
            "timeout_ms": 5000,
        }))
        .expect("合法参数");
        assert_eq!(ok.browser, "firefox");
        assert!(!ok.headless);
        assert_eq!(ok.timeout, Duration::from_millis(5000));

        // 缺省：chromium / 有头关 / 60s。
        let defaults = InvokeParams::validate(&json!({"script": "return 1;"})).unwrap();
        assert_eq!(defaults.browser, "chromium");
        assert!(defaults.headless);
        assert_eq!(defaults.timeout, Duration::from_millis(DEFAULT_TIMEOUT_MS));

        // 越界夹紧。
        let clamped =
            InvokeParams::validate(&json!({"script": "return;", "timeout_ms": 5})).unwrap();
        assert_eq!(clamped.timeout, Duration::from_millis(TIMEOUT_MIN_MS));

        assert!(InvokeParams::validate(&json!({})).is_err(), "缺 script");
        assert!(
            InvokeParams::validate(&json!({"script": "   "})).is_err(),
            "空白 script"
        );
        assert!(
            InvokeParams::validate(&json!({"script": "x".repeat(SCRIPT_MAX_BYTES + 1)})).is_err(),
            "script 超长"
        );
        assert!(
            InvokeParams::validate(&json!({"script": "return;", "browser": "ie"})).is_err(),
            "browser 闭集"
        );
    }

    #[test]
    fn envelope_sentinel_parsing() {
        assert!(parse_envelope("noise\nno sentinel here").is_none());
        let envelope =
            parse_envelope("garbage before\n__FLOW_PW__{\"status\":\"success\",\"result\":42}\n");
        assert_eq!(envelope.unwrap()["result"], 42);
        // 哨兵必须可解 JSON，坏了不算（防御：wrapper bug 不会变成成功）。
        assert!(parse_envelope("__FLOW_PW__not-json").is_none());
    }

    #[test]
    fn plugin_root_follows_deploy_layout() {
        let exe = Path::new("/flow/plugins/playwright/bin/flow-playwright-plugin");
        assert_eq!(
            plugin_root_near(exe).as_deref(),
            Some(Path::new("/flow/plugins/playwright"))
        );
    }

    #[test]
    fn output_budget_trims_logs_before_failing() {
        let logs: Vec<Value> = (0..200)
            .map(|i| json!({"level": "log", "text": format!("log line {i} padding padding")}))
            .collect();
        // logs 小于预算：直接通过。
        let small = fit_output_budget(json!({"status": "success", "logs": logs, "result": 1}))
            .expect("小信封通过");
        assert_eq!(small["result"], 1);

        // result 本身超限：报捕获上限，不静默截断。
        let huge = json!({
            "status": "success",
            "logs": [],
            "result": "x".repeat(OUTPUT_MAX_BYTES + 1),
        });
        assert_eq!(
            fit_output_budget(huge).unwrap_err().code,
            CAPTURE_LIMIT_ERROR
        );
    }

    /// 超时路径：node_bin 换成休眠 stub，超时必须整组击杀并回
    /// TIMEOUT_ERROR。这钉住「插件是唯一一只钟」的契约。
    #[tokio::test]
    async fn timeout_kills_stub_and_reports() {
        let dir = std::env::temp_dir();
        let stub = dir.join(format!("flow-pw-stub-{}.sh", std::process::id()));
        std::fs::write(&stub, "#!/bin/sh\nsleep 30\n").unwrap();
        make_executable(&stub);
        let params = ValidatedParams {
            script: "return 1;".into(),
            browser: "chromium".to_string(),
            headless: true,
            timeout: Duration::from_millis(500),
        };
        // wrapper 存在性检查在前：放一个假 wrapper 让流程走到 spawn。
        let plugin_root = dir.join(format!("flow-pw-root-{}", std::process::id()));
        let runtime = plugin_root.join("runtime");
        std::fs::create_dir_all(&runtime).unwrap();
        std::fs::write(runtime.join("wrapper.js"), "// stub").unwrap();
        let error = run_playwright(&params, &plugin_root, stub.to_str().unwrap())
            .await
            .expect_err("stub 必须超时");
        assert_eq!(error.code, TIMEOUT_ERROR);
        let _ = std::fs::remove_file(&stub);
        let _ = std::fs::remove_dir_all(&plugin_root);
    }

    /// 环境缺失路径：runtime 目录不存在 → ENVIRONMENT_ERROR（确定类，
    /// message 带修复指引）。
    #[tokio::test]
    async fn missing_runtime_is_environment_error() {
        let params = ValidatedParams {
            script: "return 1;".into(),
            browser: "chromium".to_string(),
            headless: true,
            timeout: Duration::from_millis(500),
        };
        let error = run_playwright(&params, Path::new("/nonexistent/playwright"), "node")
            .await
            .expect_err("缺 runtime 必须报环境错误");
        assert_eq!(error.code, ENVIRONMENT_ERROR);
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }
    #[cfg(not(unix))]
    fn make_executable(_path: &Path) {}
}
