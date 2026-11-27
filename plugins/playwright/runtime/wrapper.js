// flow playwright 插件的 Node 侧执行器（契约另一半在 Rust lib.rs）。
//
// 输入：stdin 一份 JSON（EOF 即「参数齐了」）：
//   {"script": "<JS 函数体>", "browser": "chromium", "headless": true, "timeout_ms": 60000}
// 输出：stdout 最后一行 `__FLOW_PW__<JSON 信封>`；信封形如
//   {status: "success"|"failed", browser, headless, duration_ms, logs: [{level, text}], result}
//   失败时多一个 error 字段。stdout 其余内容被 Rust 侧忽略（脚本里禁止直写
//   stdout 的哨兵前缀）。
//
// 退出码：0 = 已处理（含脚本失败，见信封 status）；3 = 环境问题
// （playwright 未安装等，Rust 侧读退出码后映射 ENVIRONMENT_ERROR /
// -32024，确定类：直接失败不挂起）；1 = 未预期崩溃（Rust 侧按
// SCRIPT_ERROR + stderr 尾巴收口，不确定类）。改这里的退出码语义 =
// 改 Rust 侧 WRAPPER_EXIT_ENVIRONMENT，两处同改。
//
// 纪律：
// - 不设第二只钟——超时由 Rust 插件整组击杀（单钟原则）；
// - 浏览器在 finally 里关闭，结果信封最后打印（信封 = 脚本结果 + 浏览器干净退出）；
// - console 被捕获进 logs（用户 console.* 与页面 console 事件都收），镜像到 stderr。
'use strict';

const { Readable } = require('stream');

const SENTINEL = '__FLOW_PW__';
const LOGS_MAX_ENTRIES = 200;
const LOGS_MAX_BYTES = 64 * 1024; // 与 Rust 侧 LOGS_MAX_BYTES 同值（两侧各自声明，同改）

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

class LogSink {
  constructor() {
    this.entries = [];
    this.bytes = 0;
  }
  push(level, text) {
    const line = String(text);
    // 镜像到 stderr：开发态直接 node wrapper.js 也能看到日志。
    process.stderr.write(`[pw:${level}] ${line}\n`);
    if (this.entries.length >= LOGS_MAX_ENTRIES) return;
    if (this.bytes + line.length > LOGS_MAX_BYTES) return;
    this.bytes += line.length;
    this.entries.push({ level, text: line });
  }
  captureConsole() {
    for (const level of ['log', 'info', 'warn', 'error', 'debug']) {
      const original = console[level].bind(console);
      console[level] = (...args) => {
        original(...args);
        this.push(level, args.map(fmt).join(' '));
      };
    }
  }
}

function fmt(value) {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

/// 脚本返回值 → JSON 值：undefined → null；BigInt → 字符串（保精度）；
/// 循环引用等不可序列化 = 脚本失败（不静默丢数据）。
function serializeResult(result) {
  if (result === undefined) return null;
  try {
    return JSON.parse(JSON.stringify(result, (_k, v) => (typeof v === 'bigint' ? v.toString() : v)));
  } catch (error) {
    throw new Error(`脚本返回值无法 JSON 序列化：${error.message}`);
  }
}

async function main() {
  const raw = await readStdin();
  let request;
  try {
    request = JSON.parse(raw);
  } catch (error) {
    process.stderr.write(`wrapper: stdin 不是合法 JSON：${error.message}\n`);
    process.exit(3);
  }
  const script = typeof request.script === 'string' ? request.script : '';
  const browserName = ['chromium', 'firefox', 'webkit'].includes(request.browser)
    ? request.browser
    : 'chromium';
  const headless = request.headless !== false;
  const started = Date.now();
  const logs = new LogSink();

  let playwright;
  try {
    playwright = require('playwright');
  } catch (error) {
    process.stderr.write(
      `wrapper: playwright 未安装（${error.message}）：在插件 runtime 目录执行 npm install，见插件 README\n`,
    );
    process.exit(3);
  }

  const envelope = { status: 'failed', browser: browserName, headless, duration_ms: 0, logs: logs.entries, result: null, error: '尚未执行' };
  let browser = null;
  try {
    const engine = playwright[browserName];
    if (!engine) throw new Error(`playwright 不支持浏览器 ${browserName}`);
    browser = await engine.launch({ headless });
    const context = await browser.newContext();
    const page = await context.newPage();
    // 页面侧 console / 未捕获异常也进 logs（浏览器自动化的主排障面）。
    page.on('console', (message) => logs.push(message.type(), message.text()));
    page.on('pageerror', (error) => logs.push('pageerror', error.message));
    logs.captureConsole();

    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    const run = new AsyncFunction('page', 'context', 'browser', 'console', script);
    const result = await run(page, context, browser, console);
    envelope.status = 'success';
    envelope.result = serializeResult(result);
    envelope.error = undefined;
  } catch (error) {
    envelope.status = 'failed';
    envelope.error = error && error.stack ? String(error.stack).split('\n').slice(0, 8).join('\n') : String(error);
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (error) {
        process.stderr.write(`wrapper: 关闭浏览器失败：${error && error.message}\n`);
      }
    }
    envelope.duration_ms = Date.now() - started;
    envelope.logs = logs.entries;
    // 信封最后打：行内单行 JSON（换行会把信封劈成两行，Rust 侧按行解析）。
    process.stdout.write(`${SENTINEL}${JSON.stringify(JSON.parse(JSON.stringify(envelope, (k, v) => (v === undefined ? null : v))))}\n`);
  }
  process.exit(0);
}

process.on('unhandledRejection', (reason) => {
  // 兜底：信封未打印前的意外拒绝 = 未处理失败（exit 1，Rust 侧按 SCRIPT_ERROR + stderr 尾巴收口）。
  process.stderr.write(`wrapper: 未处理的 Promise 拒绝：${reason && reason.stack ? reason.stack : reason}\n`);
  process.exit(1);
});

main().catch((error) => {
  process.stderr.write(`wrapper: ${error && error.stack ? error.stack : error}\n`);
  process.exit(1);
});
