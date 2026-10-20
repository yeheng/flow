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
  /**
   * 写命令：调用方持有显式幂等键（断链重发时同键不产生双写）。
   * `scope` 只服务调用侧的恢复指引（丢失回复时按 scope+request_id 复原），
   * 不上链；返回**完整回执**——journal 侧的 committed/visible/commit_cursor
   * 都是调用方要消费的事实，不解包 result。
   */
  command<T>(method: string, params: Record<string, unknown>, _scope: string, request_id: string): Promise<T> {
    return this.call<T>(method, { ...params, request_id });
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
