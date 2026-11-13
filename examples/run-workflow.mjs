#!/usr/bin/env node
// 通过 JSON-RPC（WebSocket）把一份工作流定义「导入发布 → 运行 → 落盘」。
//
//   node examples/run-workflow.mjs [定义 JSON] [输出 JSON]
//
// 缺省：定义 examples/github-trending.workflow.json → 输出 data/github-trending.json
// 服务地址用 FLOW_RPC 覆盖（缺省 ws://127.0.0.1:9802，与 flow-cli 一致）。
// 令牌从 FLOW_JOURNAL_TOKEN 取（≥ 32 字节，服务端要求每次调用都带）。
//
// 说明：引擎没有「写文件」节点，run 的结果只是 run 输出（事件日志里的
// run_completed.output）。这个脚本负责把它拉下来写成磁盘上的 JSON。
//
// 三条硬约定（少了就调不通）：
//   1. 每次调用都带 `_token`（凭据，不进 URL、不进日志）；
//   2. **写方法必须带 `request_id`**（幂等键，1..=128 字节）——服务端没有它
//      直接拒（-32602 "request_id required for write commands"）；
//   3. 写方法返回的是**回执信封** `{committed, result, ...}`，业务结果在
//      `result` 里；读方法原样返回结果。

import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const RPC_URL = process.env.FLOW_RPC ?? "ws://127.0.0.1:9802";
const TOKEN = process.env.FLOW_JOURNAL_TOKEN;
const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
const POLL_INTERVAL_MS = 300;
const POLL_TIMEOUT_MS = 120_000;

/** 写方法清单：这些要带 request_id，且返回值要取 `result`。 */
const WRITE_METHODS = new Set([
  "workflow.import",
  "workflow.create",
  "workflow.update",
  "workflow.publish",
  "workflow.delete",
  "run.start",
  "run.cancel",
  "run.signal",
  "run.adjudicate",
  "run.approve",
  "run.deny",
]);

class RpcClient {
  #ws;
  #nextId = 0;
  #pending = new Map();

  async connect(url) {
    this.#ws = new WebSocket(url);
    await new Promise((resolve, reject) => {
      this.#ws.addEventListener("open", resolve, { once: true });
      this.#ws.addEventListener("error", () => reject(new Error(`无法连接 ${url}`)), {
        once: true,
      });
    });
    this.#ws.addEventListener("message", (event) => {
      const msg = JSON.parse(String(event.data));
      const pending = this.#pending.get(msg.id);
      if (!pending) return;
      this.#pending.delete(msg.id);
      if (msg.error) {
        pending.reject(new Error(`${msg.error.message}（code ${msg.error.code}）`));
      } else {
        pending.resolve(msg.result);
      }
    });
  }

  /**
   * 发一次调用。写方法自动补 request_id 并拆出回执里的 `result`。
   * requestId 可显式传入：回复丢了要用**同一个** id 重发，服务端才认成
   * 「同一次写」而不是执行第二次。
   */
  call(method, params = {}, requestId) {
    const payload = { ...params, _token: TOKEN };
    if (WRITE_METHODS.has(method)) {
      payload.request_id = requestId ?? randomUUID();
    }
    const id = ++this.#nextId;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params: payload }));
    }).then((result) =>
      WRITE_METHODS.has(method) && result && typeof result === "object" && "result" in result
        ? result.result
        : result,
    );
  }

  close() {
    this.#ws.close();
  }
}

/**
 * 导入并发布（服务端原子方法 workflow.import）：按 name upsert——同名就是
 * 追加一个新版本，创建/版本/发布三条事实同一事务提交。
 * 返回已发布的 workflow_id 与版本号。
 */
async function importWorkflow(client, name, definition) {
  const receipt = await client.call("workflow.import", { name, definition, publish: true });
  return { workflowId: receipt.workflow_id, version: receipt.version, created: receipt.created };
}

async function waitTerminal(client, runId) {
  const deadline = Date.now() + POLL_TIMEOUT_MS;
  for (;;) {
    // run.get.view 返回 {run: {...}, live}；run.get 是原始投影（values 数组），
    // 视图方法才是给人看的形状。
    const { run } = await client.call("run.get.view", { run_id: runId });
    if (TERMINAL.has(run.status)) return run;
    if (Date.now() > deadline) throw new Error(`run ${runId} 未在 ${POLL_TIMEOUT_MS}ms 内终结（${run.status}）`);
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

async function main() {
  if (!TOKEN) throw new Error("FLOW_JOURNAL_TOKEN 未设置（服务端要求每次调用带令牌）");
  const defPath = process.argv[2] ?? "examples/github-trending.workflow.json";
  const outPath = process.argv[3] ?? "data/github-trending.json";

  const doc = JSON.parse(await readFile(defPath, "utf8"));
  // 兼容两种文件：{name, definition} 信封，或裸 definition
  const name = doc.name ?? path.basename(defPath).replace(/\.(workflow\.)?json$/, "");
  const definition = doc.definition ?? doc;

  const client = new RpcClient();
  await client.connect(RPC_URL);
  try {
    const { workflowId, version, created } = await importWorkflow(client, name, definition);
    console.log(`${created ? "创建" : "复用"}工作流 ${name} (${workflowId}) v${version}，已发布`);

    const { run_id: runId } = await client.call("run.start", { workflow_id: workflowId });
    console.log(`run ${runId} 启动，等待终态…`);
    const run = await waitTerminal(client, runId);

    if (run.status !== "succeeded") {
      throw new Error(`run ${run.status}：${run.error ?? "(无错误信息)"}`);
    }

    await mkdir(path.dirname(path.resolve(outPath)), { recursive: true });
    await writeFile(outPath, JSON.stringify(run.output, null, 2) + "\n", "utf8");

    const counts = run.output?.counts;
    console.log(`✔ run ${runId} 成功 → ${outPath}`);
    // 新流程末尾是 harness agent 策展：输出 { overview, picks[] }；旧流程输出 { counts }。
    const picks = run.output?.picks;
    if (Array.isArray(picks)) {
      console.log(`  推荐 ${picks.length} 个项目：${picks.map((p) => p.repo).join("、")}`);
    } else if (counts) {
      console.log(
        `  chinese ${counts.chinese} / non_chinese ${counts.non_chinese}（全量榜 ${counts.all}）`,
      );
    }
  } finally {
    client.close();
  }
}

main().catch((err) => {
  console.error(`✘ ${err.message}`);
  process.exit(1);
});
