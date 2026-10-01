import { afterEach, describe, expect, it, vi } from "vitest";
import { EventWindow, JournalClient } from "./journal";
import { RpcClient, RpcError } from "../rpc/client";
afterEach(()=>vi.restoreAllMocks());
describe("JSONL client",()=>{
  it("deduplicates positions and bounds multibyte display storage",()=>{
    const window=new EventWindow();
    for(let i=0;i<10;i++)window.push({lsn:String(i),event_index:0,event:{run_seq:String(i),kind:"input_prepared",payload:"雪".repeat(200_000)}});
    expect(window.bytes).toBeLessThanOrEqual(4*1024*1024);
    expect(window.dropped).toBeGreaterThan(0);
    const count=window.events.length;window.push(window.events[0]!);expect(window.events).toHaveLength(count);
    expect(()=>window.push({lsn:"100",event_index:0,event:{run_seq:"100",kind:"chunk",payload:"雪".repeat(400_000)}})).toThrow();
  });
  it("queries the original committed command without submitting another write",async()=>{
    const receipt={committed:true,visible:false,request_id:"request",commit_cursor:{journal_id:"dataset",lsn:"7"},result:{run_id:"run"}};
    const call=vi.spyOn(RpcClient.prototype,"call").mockRejectedValueOnce(new RpcError(-32020,"COMMITTED_NOT_VISIBLE",receipt)).mockResolvedValueOnce({...receipt,visible:true});
    const client=new JournalClient("ws://localhost:9802","secret","http://localhost:9803");
    expect((await client.command("run.start",{workflow_id:"w"},"run.start:manual:","request")).visible).toBe(true);
    expect(call.mock.calls.map(v=>v[0])).toEqual(["run.start","command.status"]);
    expect(call.mock.calls[1]![1]).toEqual({scope:"run.start:manual:",request_id:"request",_token:"secret"});
  });
});

it("resnapshots after buffer overflow and stops callbacks on disposal", async () => {
  let receive: (v: unknown) => void = () => {};
  let reconnect: () => void = () => {};
  let resolveSnapshot: (v: unknown) => void = () => {};
  const unsub = vi.fn().mockResolvedValue(undefined);
  vi.spyOn(RpcClient.prototype, "subscribe").mockImplementation(async (_m, _p, cb) => { receive = cb; return unsub; });
  vi.spyOn(RpcClient.prototype, "onReconnect").mockImplementation(cb => { reconnect = cb; return vi.fn(); });
  const call = vi.spyOn(RpcClient.prototype, "call").mockImplementationOnce(() => new Promise(resolve => { resolveSnapshot = resolve; }))
    .mockResolvedValue({value:{run_id:"r",status:"running",last_run_seq:"5000"}});
  const snapshot = vi.fn(), event = vi.fn(), loss = vi.fn();
  const client = new JournalClient("ws://test", "token", "http://test");
  const watching = client.monitor("r",snapshot,event,loss);
  await Promise.resolve(); await Promise.resolve();
  for (let i=1;i<=4200;i++) receive({lsn:String(i),event_index:0,event:{run_seq:String(i),kind:"node",payload:null}});
  resolveSnapshot({value:{run_id:"r",status:"running",last_run_seq:"1"}});
  const stop = await watching;
  expect(call).toHaveBeenCalledTimes(2);
  expect(loss).toHaveBeenCalledWith(104);
  expect(event).not.toHaveBeenCalled();
  receive({lsn:"5001",event_index:0,event:{run_seq:"5001",kind:"node",payload:null}});
  expect(event).toHaveBeenCalledTimes(1);
  await stop();
  receive({lsn:"5002",event_index:0,event:{run_seq:"5002",kind:"node",payload:null}});
  reconnect();
  await Promise.resolve();
  expect(event).toHaveBeenCalledTimes(1);
  expect(unsub).toHaveBeenCalledTimes(1);
});
