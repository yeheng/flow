import { beforeEach, expect, it, vi } from "vitest";
vi.mock("./platform", () => ({ isDesktop: () => false }));
import { rpcUrl, httpUrl, setServerUrls } from "./config";
beforeEach(() => localStorage.clear());
it("persists independent server endpoints and restores defaults", () => {
  const defaults = [rpcUrl(), httpUrl()];
  setServerUrls(" wss://flow.example/rpc ", "https://flow.example");
  expect([rpcUrl(), httpUrl()]).toEqual(["wss://flow.example/rpc", "https://flow.example"]);
  setServerUrls("", "");
  expect([rpcUrl(), httpUrl()]).toEqual(defaults);
});
it("rejects invalid endpoints without partially changing the connection", () => {
  setServerUrls("ws://localhost:9802", "http://localhost:9803");
  for (const invalid of [
    "broken",
    "ws://example",
    "https://user:pass@example",
    "https://example/#fragment",
  ]) {
    expect(() => setServerUrls("wss://new.example", invalid)).toThrow();
    expect(rpcUrl()).toBe("ws://localhost:9802");
  }
});
