import type { Definition } from "../types";

export function parseRawDefinition(text: string): Definition {
  const value = JSON.parse(text);
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    !Array.isArray(value.nodes) ||
    !Array.isArray(value.edges)
  ) {
    throw new Error("工作流须为包含 nodes 和 edges 数组的 JSON 对象");
  }
  for (const node of value.nodes) {
    if (
      !node ||
      typeof node.id !== "string" ||
      typeof node.type !== "string" ||
      (node.params !== undefined &&
        (!node.params || typeof node.params !== "object" || Array.isArray(node.params)))
    ) {
      throw new Error("每个节点须包含 id、type，params 须为对象");
    }
  }
  return value as Definition;
}

export function responseText(output: unknown): string {
  if (typeof output === "string") return output;
  if (output && typeof output === "object") {
    const obj = output as Record<string, unknown>;
    for (const key of ["text", "stdout", "response", "answer", "result"]) {
      if (typeof obj[key] === "string") return obj[key];
    }
  }
  return JSON.stringify(output, null, 2) ?? "（空回复）";
}

export function extractDefinition(text: string): string {
  const blocks = [...text.matchAll(/```(?:json)?\s*\n([\s\S]*?)```/g)].map((m) => m[1]!);
  for (const candidate of [...blocks, text]) {
    try {
      return JSON.stringify(parseRawDefinition(candidate), null, 2);
    } catch {
      /* Try next complete JSON block. */
    }
  }
  throw new Error("回复中没有完整的工作流 JSON，请让助手返回包含 nodes 和 edges 的完整定义");
}
