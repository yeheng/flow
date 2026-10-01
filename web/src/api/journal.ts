import { RpcClient, RpcError } from "../rpc/client";

export interface CommitCursor { journal_id: string; lsn: string }
export interface Receipt { committed: boolean; visible: boolean; request_id: string; commit_cursor: CommitCursor; result: Record<string, unknown> }
export interface JournalEvent { lsn: string; event_index: number; event: { run_seq: string; kind: string; payload: unknown } }
export interface JournalPage { events: JournalEvent[]; next_cursor: Record<string, unknown> | null; snapshot_cursor: CommitCursor }
export interface RunSnapshot { run_id: string; status: string; last_run_seq: string; [key: string]: unknown }

/** A bounded display cache. Historical truth remains in server pages, never this cache. */
export class EventWindow {
  events: JournalEvent[] = [];
  bytes = 0;
  dropped = 0;
  private sizes: number[] = [];
  private keys = new Set<string>();
  push(event: JournalEvent): void {
    const key = `${event.lsn}:${event.event_index}`;
    if (this.keys.has(key)) return;
    const size = new TextEncoder().encode(JSON.stringify(event)).byteLength;
    if (size > 1024 * 1024) throw new Error("事件超过展示上限，请使用审计分页或下载");
    while (this.events.length && (this.events.length >= 4096 || this.bytes + size > 4 * 1024 * 1024)) {
      const old = this.events.shift()!;
      this.keys.delete(`${old.lsn}:${old.event_index}`);
      this.bytes -= this.sizes.shift()!;
      this.dropped++;
    }
    this.events.push(event); this.sizes.push(size); this.keys.add(key); this.bytes += size;
  }
}

export class JournalClient {
  private rpc: RpcClient;
  constructor(url: string, private token: string, private http: string) { this.rpc = new RpcClient(url); }
  close(): void { this.rpc.close(); }
  call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    return this.rpc.call<T>(method, { ...params, _token: this.token });
  }
  async command(method: string, params: Record<string, unknown>, scope = method, requestId: string = crypto.randomUUID()): Promise<Receipt> {
    try { return await this.call<Receipt>(method, { ...params, request_id: requestId }); }
    catch (error) {
      if (!(error instanceof RpcError) || error.code !== -32020) throw error;
      const original = error.data as Receipt;
      if (!original?.committed || original.request_id !== requestId) throw error;
      // Never submit another write merely because the projection is behind.
      for (let i = 0; i < 20; i++) {
        const receipt = await this.call<Receipt | null>("command.status", { scope, request_id: requestId });
        if (receipt?.visible) return receipt;
        await new Promise(resolve => setTimeout(resolve, 250));
      }
      return original;
    }
  }
  page(run: string, audit = false, cursor?: Record<string, unknown> | null): Promise<JournalPage> {
    return this.call(audit ? "run.audit.page" : "run.events.page", { run_id: run, cursor, limit: 100 });
  }
  async monitor(run: string, snapshot: (run: RunSnapshot) => void, event: (event: JournalEvent) => void, loss: (count: number) => void): Promise<() => Promise<void>> {
    let ready = false, stopped = false;
    let upper = 0n, generation = 0;
    let buffer = new EventWindow();
    let syncing: Promise<void> | undefined;
    const deliver = (record: JournalEvent) => {
      const seq = BigInt(record.event.run_seq);
      if (seq > upper) { upper = seq; event(record); }
    };
    const align = (): Promise<void> => {
      ready = false;
      generation++;
      if (syncing) return syncing;
      syncing = (async () => {
        while (!stopped) {
          const current = generation;
          const initial = await this.call<{value: RunSnapshot}>("run.get", {run_id: run});
          if (stopped) return;
          if (current !== generation) continue;
          if (!initial.value) throw new Error("run not found");
          snapshot(initial.value); upper = BigInt(initial.value.last_run_seq);
          if (buffer.dropped) {
            // Discarded events might be newer than this snapshot.
            // Take another snapshot after the loss before resuming delivery.
            loss(buffer.dropped); buffer = new EventWindow(); continue;
          }
          for (const item of buffer.events) deliver(item);
          buffer = new EventWindow(); ready = true; return;
        }
      })().finally(() => { syncing = undefined; });
      return syncing;
    };
    const removeReconnect = this.rpc.onReconnect(() => { void align().catch(() => loss(1)); });
    let unsubscribe: (() => Promise<void>) | undefined;
    try {
      unsubscribe = await this.rpc.subscribe("run.subscribe", { _token: this.token, run_id: run, event_format: "v2", from_seq: "0" }, raw => {
        if (stopped) return;
        const record = raw as JournalEvent;
        if (!record.event) { loss(1); void align().catch(() => loss(1)); return; }
        if (!ready) { buffer.push(record); return; }
        const seq = BigInt(record.event.run_seq);
        if (seq > upper + 1n) {
          buffer.push(record); loss(1); void align().catch(() => loss(1)); return;
        }
        deliver(record);
      });
      await align();
      return async () => { stopped = true; removeReconnect(); await unsubscribe?.(); };
    } catch (error) { stopped = true; removeReconnect(); await unsubscribe?.(); throw error; }
  }
  async download(run: string, output: string): Promise<void> {
    const picker = (window as unknown as {showSaveFilePicker?: (options: unknown) => Promise<{createWritable: () => Promise<WritableStream>}>}).showSaveFilePicker;
    if (!picker) throw new Error("当前浏览器不支持流式保存，请使用 JSONL CLI 下载");
    const file = await picker({suggestedName: `${output}.bin`});
    const response = await fetch(`${this.http}/runs/${encodeURIComponent(run)}/values/${encodeURIComponent(output)}`, {headers: {Authorization: `Bearer ${this.token}`}});
    if (!response.ok || !response.body) throw new Error(`下载失败：${response.status}`);
    await response.body.pipeTo(await file.createWritable());
  }
}
