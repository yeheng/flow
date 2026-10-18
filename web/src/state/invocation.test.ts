import { describe, expect, it } from "vitest";
import {
  generateInvokeKey,
  invokeCurl,
  invocationResultUrl,
  invokeUrl,
  resultCurl,
  sha256Hex,
} from "./invocation";

const hasSubtle = typeof globalThis.crypto?.subtle?.digest === "function";

describe("invocation key", () => {
  it("生成 32 字节 hex 密钥，两次不重复", () => {
    const a = generateInvokeKey();
    const b = generateInvokeKey();
    expect(a).toMatch(/^[0-9a-f]{64}$/);
    expect(a).not.toBe(b);
  });

  it.skipIf(!hasSubtle)("sha256Hex 与标准向量一致（空串 / abc）", async () => {
    expect(await sha256Hex("")).toBe(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    );
    expect(await sha256Hex("abc")).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });
});

describe("调用与查询示例", () => {
  it("URL 去尾斜杠并编码路径", () => {
    expect(invokeUrl("http://h:1/", "tok")).toBe("http://h:1/invoke/tok");
    expect(invocationResultUrl("http://h:1", "r1")).toBe("http://h:1/invocations/r1");
  });

  it("curl 示例含幂等键与 run_token 说明", () => {
    expect(invokeCurl("http://h:1", "tok")).toContain("Idempotency-Key");
    expect(invokeCurl("http://h:1", "tok")).toContain("X-Correlation-Id");
    expect(resultCurl("http://h:1", "r1")).toContain("/invocations/r1");
    expect(resultCurl("http://h:1", "r1")).toContain("download_url");
  });
});
