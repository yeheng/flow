#!/usr/bin/env node
// 通过 JSON-RPC（WebSocket）把一份工作流定义「创建 → 发布 → 运行 → 落盘」。
//
//   node examples/run-workflow.mjs [定义 JSON] [输出 JSON]
//
// 缺省：定义 examples/github-trending.workflow.json → 输出 data/github-trending.json
// 服务地址用 FLOW_RPC 覆盖（缺省 ws://127.0.0.1:9800）。
//
// 说明：引擎没有「写文件」节点，run 的结果只是 run 输出（事件日志里的
// run_completed.output）。这个脚本负责把它拉下来写成磁盘上的 JSON。

import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const RPC_URL = process.env.FLOW_RPC ?? "ws://127.0.0.1:9800";
const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
const POLL_INTERVAL_MS = 300;
const POLL_TIMEOUT_MS = 120_000;

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

  call(method, params = {}) {
    const id = ++this.#nextId;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    });
  }

  close() {
    this.#ws.close();
  }
}

/** 定义已存在同名工作流就追加新版本，否则新建。返回已发布的 workflow_id。 */
async function upsert(client, name, definition) {
  const { workflows } = await client.call("workflow.list");
  const existing = workflows.find((w) => w.name === name);
  const workflowId =
    existing?.workflow_id ??
    (await client.call("workflow.create", { name })).workflow_id;
  const { version } = await client.call("workflow.update", {
    workflow_id: workflowId,
    definition,
  });
  await client.call("workflow.publish", { workflow_id: workflowId, version });
  return { workflowId, version, created: !existing };
}

async function waitTerminal(client, runId) {
  const deadline = Date.now() + POLL_TIMEOUT_MS;
  for (;;) {
    const { run } = await client.call("run.get", { run_id: runId });
    if (TERMINAL.has(run.status)) return run;
    if (Date.now() > deadline) throw new Error(`run ${runId} 未在 ${POLL_TIMEOUT_MS}ms 内终结（${run.status}）`);
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

async function main() {
  const defPath = process.argv[2] ?? "examples/github-trending.workflow.json";
  const outPath = process.argv[3] ?? "data/github-trending.json";

  const doc = JSON.parse(await readFile(defPath, "utf8"));
  // 兼容两种文件：{name, definition} 信封，或裸 definition
  const name = doc.name ?? path.basename(defPath).replace(/\.(workflow\.)?json$/, "");
  const definition = doc.definition ?? doc;

  const client = new RpcClient();
  await client.connect(RPC_URL);
  try {
    const { workflowId, version, created } = await upsert(client, name, definition);
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
    if (counts) {
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
