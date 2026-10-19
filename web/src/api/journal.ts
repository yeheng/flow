import { RpcClient } from "../rpc/client";
import { isDesktop } from "../platform";
import { invoke } from "@tauri-apps/api/core";

export interface CommitCursor { journal_id: string; lsn: string }
export interface Receipt { committed: boolean; visible: boolean; request_id: string; commit_cursor: CommitCursor; result: Record<string, unknown> }
export interface JournalEvent { lsn: string; event_index: number; event: { run_seq: string; kind: string; payload: unknown } }
export interface JournalPage { events: JournalEvent[]; next_cursor: Record<string, unknown> | null; snapshot_cursor: CommitCursor }
export interface RunSnapshot { run_id: string; status: string; last_run_seq: string; [key: string]: unknown }

export class JournalClient {
  private rpc: RpcClient;
  constructor(url: string, private token: string, private http: string) { this.rpc = new RpcClient(url, "journal"); }
  close(): void { this.rpc.close(); }
  call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    return this.rpc.call<T>(method, { ...params, _token: this.token });
  }
  /** 写命令回执。SQLite 单事务提交后 visible 恒为 true；-32020 已无生产者，
   * 这里仍保留完整回执形状供原生协议读面使用。 */
  command(method: string, params: Record<string, unknown>, _scope = method, requestId: string = crypto.randomUUID()): Promise<Receipt> {
    return this.call<Receipt>(method, { ...params, request_id: requestId });
  }
  page(run: string, audit = false, cursor?: Record<string, unknown> | null): Promise<JournalPage> {
    return this.call(audit ? "run.audit.page" : "run.events.page", { run_id: run, cursor, limit: 100 });
  }
  async download(run: string, output: string): Promise<void> {
    if (isDesktop()) {
      await invoke("flow_download", { run, output });
      return;
    }
    const picker = (window as unknown as {showSaveFilePicker?: (options: unknown) => Promise<{createWritable: () => Promise<WritableStream>}>}).showSaveFilePicker;
    if (!picker) throw new Error("当前浏览器不支持流式保存，请使用 JSONL CLI 下载");
    const file = await picker({suggestedName: `${output}.bin`});
    const response = await fetch(`${this.http}/runs/${encodeURIComponent(run)}/values/${encodeURIComponent(output)}`, {headers: {Authorization: `Bearer ${this.token}`}});
    if (!response.ok || !response.body) throw new Error(`下载失败：${response.status}`);
    await response.body.pipeTo(await file.createWritable());
  }
}
