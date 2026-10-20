import { afterEach, describe, expect, it, vi } from "vitest";
import { JournalClient } from "./journal";
import { RpcClient, RpcError } from "../rpc/client";
afterEach(()=>vi.restoreAllMocks());
describe("JSONL client",()=>{
  it("propagates COMMITTED_NOT_VISIBLE without recovery polling",async()=>{
    const receipt={committed:true,visible:false,request_id:"request",commit_cursor:{journal_id:"dataset",lsn:"7"},result:{run_id:"run"}};
    const call=vi.spyOn(RpcClient.prototype,"call").mockRejectedValueOnce(new RpcError(-32020,"COMMITTED_NOT_VISIBLE",receipt));
    const client=new JournalClient("ws://localhost:9802","secret","http://localhost:9803");
    await expect(client.call("run.start",{workflow_id:"w",request_id:"request"})).rejects.toMatchObject({code:-32020});
    expect(call.mock.calls.map(v=>v[0])).toEqual(["run.start"]);
  });
});
