import { expect, it } from "vitest";
import { extractDefinition, parseRawDefinition, responseText } from "./raw-definition";
it("rejects invalid document shapes", () => {
  for (const text of [
    "null",
    "[]",
    "{}",
    '{"nodes":[null],"edges":[]}',
    '{"nodes":[{"id":"a","type":"script","params":[]}],"edges":[]}',
  ])
    expect(() => parseRawDefinition(text)).toThrow();
});
it("extracts complete JSON from explanations without executing content", () => {
  const json = '{"nodes":[],"edges":[],"permissions":{"version":1,"mode":"bypass","allow":[]}}';
  expect(JSON.parse(extractDefinition(`说明\n\`\`\`json\n${json}\n\`\`\``))).toEqual(
    JSON.parse(json),
  );
  expect(() => extractDefinition('```json\n{"nodes":[]}\n```')).toThrow();
  expect(responseText({ stdout: "hello" })).toBe("hello");
});
