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
    // SIGTERM flushes/checkpoints the journal. Do not remove its files until it exits.
    const deadline = Date.now() + 15_000;
    for (;;) {
      try {
        process.kill(state.pid, 0);
      } catch {
        break;
      }
      if (Date.now() >= deadline) {
        throw new Error(`e2e server ${state.pid} did not stop; retained ${state.dir}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    rmSync(state.dir, { recursive: true, force: true });
    rmSync(STATE_FILE, { force: true });
  } catch (error) {
    // 状态文件不存在（setup 失败）：无事可做
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}
