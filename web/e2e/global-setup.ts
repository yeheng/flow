// e2e 全局基建：起一个隔离的 flow-journal-server（临时 data 目录 + 独立端口）。
// 状态落临时文件给 teardown 收尾（pid + 目录）。

import { execSync, spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { E2E_TOKEN } from "./config";

const STATE_FILE = path.join(tmpdir(), "flow-e2e-server.json");
// playwright 从 web/ 目录启动（npm script），仓库根是上一级
const REPO_ROOT = path.resolve(process.cwd(), "..");

interface ServerState {
  pid: number;
  dir: string;
}

async function waitReady(url: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const ws = new WebSocket(url);
      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => {
          ws.close();
          resolve();
        };
        ws.onerror = () => reject(new Error("not ready"));
      });
      return;
    } catch {
      if (Date.now() > deadline) throw new Error(`flow-journal-server 未在 ${timeoutMs}ms 内就绪`);
      await new Promise((r) => setTimeout(r, 200));
    }
  }
}

export default async function globalSetup(): Promise<void> {
  // 确保二进制是最新的（缓存命中时毫秒级）。服务端 bin 是 flow-rpc 的 `flow-journal-server`。
  execSync("cargo build -p flow-rpc --bin flow-journal-server --locked", {
    cwd: REPO_ROOT,
    stdio: "inherit",
  });

  const dir = mkdtempSync(path.join(tmpdir(), "flow-e2e-"));
  writeFileSync(path.join(dir, "flow.toml"), "");
  const bin = path.join(REPO_ROOT, "target/debug/flow-journal-server");
  const child = spawn(bin, [], {
    env: {
      ...process.env,
      FLOW_DATA_DIR: path.join(dir, "secrets"),
      FLOW_JOURNAL_DATA_DIR: path.join(dir, "journal"),
      FLOW_JOURNAL_TOKEN: E2E_TOKEN,
      FLOW_CONFIG: path.join(dir, "flow.toml"),
      FLOW_JOURNAL_ADDR: "127.0.0.1:19311",
      FLOW_JOURNAL_HTTP_ADDR: "127.0.0.1:19312",
      RUST_LOG: "warn",
    },
    stdio: "ignore",
  });
  try {
    await waitReady("ws://127.0.0.1:19311", 15_000);
  } catch (error) {
    child.kill();
    rmSync(dir, { recursive: true, force: true });
    throw error;
  }
  writeFileSync(STATE_FILE, JSON.stringify({ pid: child.pid, dir } satisfies ServerState));
  console.log(`e2e flow-journal-server 已启动 pid=${child.pid} dir=${dir}`);
}
