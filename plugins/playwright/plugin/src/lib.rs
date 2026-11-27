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
//!   已发生）；`-32021` 输出超过捕获上限；`-32022` 参数畸形（spawn 前
//!   拦截；正常由发布期 manifest 表单先拦，运行期不可达）；`-32023`
//!   超时（不确定类）；`-32024` 环境缺失（node 解释器起不来 / runtime
//!   未拷 / wrapper 以退出码 3 收尾，确定类：直接失败，不进 Uncertain）。
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
pub const WRAPPER_EXIT_ENVIRONMENT: i32 = 3;

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
    // 三路管道真并发（stdin 写 + stdout/stderr 读）：
    //
    // - 两路读必须并发：串行读时，wrapper 写满 stderr 管道缓冲而 stdout
    //   尚未关闭，双方互等卡死（旧实现的真实缺陷，测试
    //   heavy_stderr_does_not_deadlock_stdout 钉住）。
    // - 写 stdin 与读端并发：不再依赖「wrapper 一定先读完 stdin」的
    //   顺序巧合；wrapper 不读 stdin 时也不丢它已写出的信封。
    // - 写 stdin 失败（EPIPE = wrapper 已退出）**不是**终态错误：继续把
    //   两路读到 EOF，以信封/退出码为准。
    //
    // 自持三根管道而不是 wait_with_output：后者把 child 搬进 future，
    // 超时分支就拿不回进程组句柄了。
    let mut stdin_pipe = child.stdin.take().expect("stdin piped");
    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let timed = tokio::time::timeout(params.timeout, async {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        // 写端包一层：失败只记日志，不中断读端（永不返回 Err）。
        //
        // stdin_pipe 必须**按值搬进** async 块（`async move`）：`async` 块
        // 默认按引用捕获，块结束时只是借用结束，管道 fd 会活到函数尾，
        // wrapper 就永远等不到 EOF（实测：数据已到、EOF 不来、整个 invoke
        // 超时）。写完显式 drop 关管道——EOF 是 wrapper「参数齐了」的信号。
        let write_stdin = async move {
            use tokio::io::AsyncWriteExt as _;
            let payload = stdin_payload.to_string();
            if let Err(error) = stdin_pipe.write_all(payload.as_bytes()).await {
                tracing::debug!(
                    "playwright wrapper 未读完 stdin 即退出（{error}）；以信封/退出码为准"
                );
            }
            // drop(stdin_pipe) 关闭写端 = EOF；shutdown() 本身不关 fd。
            drop(stdin_pipe);
        };
        let (read_stdout, read_stderr, (), waited) = tokio::join!(
            tokio::io::AsyncReadExt::read_to_end(&mut stdout_pipe, &mut stdout),
            tokio::io::AsyncReadExt::read_to_end(&mut stderr_pipe, &mut stderr),
            write_stdin,
            child.wait(),
        );
        read_stdout?;
        read_stderr?;
        let status = waited?;
        std::io::Result::Ok((status, stdout, stderr))
    })
    .await;
    let (status, stdout, stderr) = match timed {
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
    let exit_code = status.code();
    let envelope = match parse_envelope(&stdout) {
        Some(envelope) => envelope,
        None => {
            // 没有哨兵：按退出码分派。3 = wrapper 自己声明的环境问题
            //（playwright 未安装 / stdin 非法 JSON）——确定类，直接失败，
            // 不该进 Uncertain 让 run 挂起等裁决。其余（崩溃 / 被信号杀 /
            // 未预期退出）仍是不确定类。
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
            return Err(match exit_code {
                Some(WRAPPER_EXIT_ENVIRONMENT) => InvokeError::new(ENVIRONMENT_ERROR, message),
                _ => InvokeError::new(SCRIPT_ERROR, message),
            });
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

    /// 回归（review 问题 1）：wrapper 以退出码 3 收尾（playwright 未安装 /
    /// stdin 非法 JSON）必须映射 ENVIRONMENT_ERROR（确定类，直接失败），
    /// 不能落进 SCRIPT_ERROR（不确定类 → run 挂起等裁决）。
    /// 修前：退出码从不读取，一律 SCRIPT_ERROR。
    #[tokio::test]
    async fn wrapper_exit_3_is_environment_error() {
        let Some(guard) =
            node_wrapper_fixture("process.stderr.write('pw missing\\n'); process.exit(3);")
        else {
            return;
        };
        let error = run_playwright(&guard.params, &guard.root, "node")
            .await
            .expect_err("exit 3 必须是错误");
        assert_eq!(error.code, ENVIRONMENT_ERROR, "message: {}", error.message);
        assert!(
            error.message.contains("pw missing"),
            "stderr 尾巴要带上：{}",
            error.message
        );
    }

    /// 非 3 的非零退出码仍是不确定类（崩溃 / 被信号杀），不因「读退出码」
    /// 而被误降级成确定类。
    #[tokio::test]
    async fn wrapper_crash_stays_script_error() {
        let Some(guard) = node_wrapper_fixture("process.exit(1);") else {
            return;
        };
        let error = run_playwright(&guard.params, &guard.root, "node")
            .await
            .expect_err("exit 1 必须是错误");
        assert_eq!(error.code, SCRIPT_ERROR);
    }

    /// 大脚本（远超管道缓冲）+ wrapper 延迟读 stdin：三路并发下的
    /// 回归保护。如实说明：旧实现在这个形态下**也能过**（对照实验 3/3
    /// 成功——tokio 把 200KB 全塞进了缓冲，没卡到 wrapper 退出）；
    /// 真正在旧实现上必红的是 stderr 风暴（串行读卡死），见
    /// `heavy_stderr_does_not_deadlock_stdout`。本测试钉的是并发改造
    /// 没有把这条真实形态弄坏（v1 改造曾因 stdin 不关而在此超时）。
    #[tokio::test]
    async fn large_script_survives_wrapper_that_reads_stdin_late() {
        // wrapper：先忙 50ms（不碰 stdin），再读 stdin 并回信封——
        // 真 wrapper 的形态（require 冷启动后才 readStdin）。
        let wrapper = r#"
            setTimeout(async () => {
              let raw = '';
              for await (const chunk of process.stdin) raw += chunk;
              const request = JSON.parse(raw);
              process.stdout.write('__FLOW_PW__' + JSON.stringify({
                status: 'success', result: { script_len: request.script.length },
              }) + '\n');
            }, 50);
        "#;
        let Some(guard) = node_wrapper_fixture(wrapper) else {
            return;
        };
        // 远超常见 64KiB 管道缓冲；仍在 manifest 的 256KiB 上限内。
        let big = ValidatedParams {
            script: format!("return 1; //{}", "x".repeat(200 * 1024)),
            ..guard.params.clone()
        };
        let envelope = run_playwright(&big, &guard.root, "node")
            .await
            .expect("大脚本不应丢结果");
        assert_eq!(envelope["status"], "success");
        assert_eq!(envelope["result"]["script_len"], big.script.len());
    }

    /// stderr 大量输出不得卡死读端（两路读必须并发）。**旧实现必红**
    /// （对照实验卡满 20s）：原实现注释说并发排空，实际是先读完 stdout
    /// 再读 stderr；wrapper 写满 stderr 管道缓冲时互等。
    #[tokio::test]
    async fn heavy_stderr_does_not_deadlock_stdout() {
        let wrapper = r#"
            let raw = '';
            for await (const chunk of process.stdin) raw += chunk;
            JSON.parse(raw);
            for (let i = 0; i < 400; i++) process.stderr.write('E'.repeat(2048) + '\n');
            process.stdout.write('__FLOW_PW__{"status":"success","result":"ok"}\n');
        "#;
        let Some(guard) = node_wrapper_fixture(wrapper) else {
            return;
        };
        let envelope = run_playwright(&guard.params, &guard.root, "node")
            .await
            .expect("stderr 风暴不应卡死");
        assert_eq!(envelope["result"], "ok");
    }

    /// 契约钉子（review 问题 3）：`OUTPUT_MAX_BYTES` 是跨进程契约值，
    /// 必须与 runtime `plugin_node` 的 `store_value` 预算同值。runtime 侧
    /// 是裸字面量（无具名常量可引），这里读它的源码断言——两处分叉时
    /// 第一时间红，而不是等到 run 在 journal 写入时才炸。
    #[test]
    fn output_budget_matches_runtime_store_value_budget() {
        let runtime_src =
            include_str!("../../../../crates/flow-plugin-runtime/src/task/plugins.rs");
        // plugin_node 的输出落账行：`store_value(audit, &self.journal_id, &output_value, <budget>)`
        // runtime 侧预算是裸字面量 `8 * 1024 * 1024`（无具名常量可引）。
        // 这里把它按字面写死并断言两处同值：任一侧改了，这个测试就红，
        // 而不是等到 run 在 journal 写入时才炸。
        const RUNTIME_LITERAL: &str = "8 * 1024 * 1024";
        assert_eq!(
            OUTPUT_MAX_BYTES,
            8 * 1024 * 1024,
            "插件侧 OUTPUT_MAX_BYTES 与本测试钉的 runtime 字面量分叉"
        );
        let line = runtime_src
            .lines()
            .find(|line| line.contains("store_value(audit, &self.journal_id, &output_value"))
            .expect("runtime plugin_node 的 store_value 行必须存在");
        let normalized: String = line.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            normalized.contains(&format!(", {RUNTIME_LITERAL})")),
            "plugin_node 输出预算变了（不再是 {RUNTIME_LITERAL}）：插件侧 OUTPUT_MAX_BYTES 必须同改。行：{line}"
        );
    }

    /// 真 node 上跑一个临时 wrapper 的 fixture。本机无 node 时返回 None
    /// （调用方跳过，不算失败）。
    struct WrapperFixture {
        root: PathBuf,
        params: ValidatedParams,
    }

    fn node_wrapper_fixture(wrapper_source: &str) -> Option<WrapperFixture> {
        if !std::process::Command::new("node")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            eprintln!("跳过：本机无 node");
            return None;
        }
        let root = std::env::temp_dir().join(format!(
            "flow-pw-fx-{}-{:p}",
            std::process::id(),
            wrapper_source
        ));
        let runtime = root.join("runtime");
        std::fs::create_dir_all(&runtime).expect("创建 fixture runtime");
        std::fs::write(runtime.join("wrapper.js"), wrapper_source).expect("写 fixture wrapper");
        Some(WrapperFixture {
            root,
            params: ValidatedParams {
                script: "return 1;".into(),
                browser: "chromium".to_string(),
                headless: true,
                timeout: Duration::from_millis(20_000),
            },
        })
    }

    impl Drop for WrapperFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
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
