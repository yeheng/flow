import { readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const STATE_FILE = path.join(tmpdir(), "flow-e2e-server.json");

export default async function globalTeardown(): Promise<void> {
  try {
    const state = JSON.parse(readFileSync(STATE_FILE, "utf-8")) as { pid: number; dir: string };
    try {
      process.kill(state.pid);
    } catch {
      // 进程已退出
    }
    rmSync(state.dir, { recursive: true, force: true });
    rmSync(STATE_FILE, { force: true });
  } catch {
    // 状态文件不存在（setup 失败）：无事可做
  }
}
