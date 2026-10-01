import { chromium } from "@playwright/test";
import assert from "node:assert/strict";
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  await page.goto("http://127.0.0.1:19413/journal");
  await page.waitForSelector("input");
  const session = await page.context().newCDPSession(page);
  await session.send("HeapProfiler.collectGarbage");
  const before = await session.send("Runtime.getHeapUsage");
  const result = await page.evaluate(async () => {
    const { EventWindow, JournalClient } = await import("/src/api/journal.ts");
    const { RpcClient } = await import("/src/rpc/client.ts");
    const cache = new EventWindow();
    let received, reconnect;
    RpcClient.prototype.subscribe = async function (_method, _params, callback) {
      received = callback;
      return async () => {};
    };
    RpcClient.prototype.onReconnect = function (callback) {
      reconnect = callback;
      return () => {};
    };
    let seq = 0;
    RpcClient.prototype.call = async function () {
      await new Promise((r) => setTimeout(r, 5));
      return { value: { run_id: "r", status: "running", last_run_seq: String(seq) } };
    };
    const client = new JournalClient("ws://unused", "token", "http://unused");
    let lost = 0,
      snapshots = 0;
    const stop = await client.monitor(
      "r",
      () => snapshots++,
      (e) => cache.push(e),
      (n) => (lost += n),
    );
    for (let batch = 0; batch < 100; batch++) {
      for (let i = 0; i < 2000; i++) {
        seq++;
        received({
          lsn: String(seq),
          event_index: 0,
          event: { run_seq: String(seq), kind: "node", payload: "雪".repeat(400) + seq },
        });
      }
      if (batch % 10 === 0) reconnect();
      await new Promise((r) => setTimeout(r, 10)); // yield to simulate a slow display consumer
    }
    await stop();
    client.close();
    const result = {
      processed: seq,
      cached: cache.events.length,
      bytes: cache.bytes,
      dropped: cache.dropped,
      lost,
      snapshots,
      last: cache.events.at(-1)?.event.run_seq,
    };
    window.acceptanceCache = cache;
    return result;
  });
  await session.send("HeapProfiler.collectGarbage");
  const after = await session.send("Runtime.getHeapUsage");
  assert.equal(result.processed, 200000);
  assert.ok(result.bytes <= 4 * 1024 * 1024);
  assert.ok(result.cached <= 4096);
  assert.ok(result.dropped > 0);
  assert.ok(result.snapshots >= 10);
  assert.equal(result.last, "200000");
  assert.ok(after.usedSize - before.usedSize < 64 * 1024 * 1024);
  console.log(
    JSON.stringify({ result, before, after, heap_growth: after.usedSize - before.usedSize }),
  );
} finally {
  await browser.close();
}
