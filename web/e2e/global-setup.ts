// e2e 全局基建：起一个隔离的 flow-server（临时 data 目录 + 独立端口）。
// 状态落临时文件给 teardown 收尾（pid + 目录）。

import { execSync, spawn } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

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
      if (Date.now() > deadline) throw new Error(`flow-server 未在 ${timeoutMs}ms 内就绪`);
      await new Promise((r) => setTimeout(r, 200));
    }
  }
}

export default async function globalSetup(): Promise<void> {
  // 确保二进制是最新的（缓存命中时毫秒级）。产品 bin 是统一的 `flow`（子命令形态）。
  execSync("cargo build -p flow-app", { cwd: REPO_ROOT, stdio: "inherit" });

  const dir = mkdtempSync(path.join(tmpdir(), "flow-e2e-"));
  const bin = path.join(REPO_ROOT, "target/debug/flow");
  const child = spawn(bin, ["server"], {
    env: {
      ...process.env,
      FLOW_DATA_DIR: dir,
      FLOW_DB: path.join(dir, "flow.db"),
      FLOW_ADDR: "127.0.0.1:19311",
      FLOW_HTTP_ADDR: "127.0.0.1:19312",
      RUST_LOG: "warn",
    },
    stdio: "ignore",
  });
  await waitReady("ws://127.0.0.1:19311", 15_000);
  writeFileSync(STATE_FILE, JSON.stringify({ pid: child.pid, dir } satisfies ServerState));
  console.log(`e2e flow-server 已启动 pid=${child.pid} dir=${dir}`);
}
