import { beforeEach, describe, expect, it, vi } from "vitest";
import { importWorkflow, nameFromFile, parseImportDocument } from "./workflow-io";
import type { WorkflowSummary } from "../types";

const listWorkflows = vi.fn();
const createWorkflow = vi.fn();
const updateWorkflow = vi.fn();
const publishWorkflow = vi.fn();

vi.mock("../api/flow", () => ({
  listWorkflows: (...a: unknown[]) => listWorkflows(...a),
  createWorkflow: (...a: unknown[]) => createWorkflow(...a),
  updateWorkflow: (...a: unknown[]) => updateWorkflow(...a),
  publishWorkflow: (...a: unknown[]) => publishWorkflow(...a),
}));

beforeEach(() => {
  listWorkflows.mockReset().mockResolvedValue([] as WorkflowSummary[]);
  createWorkflow.mockReset().mockResolvedValue("wf-new");
  updateWorkflow.mockReset().mockResolvedValue(1);
  publishWorkflow.mockReset().mockResolvedValue(undefined);
});

describe("nameFromFile", () => {
  it("与 CLI file_stem 同规则：去路径、剥 .json 与 .workflow", () => {
    expect(nameFromFile("github-trending.workflow.json")).toBe("github-trending");
    expect(nameFromFile("a/b/plain.json")).toBe("plain");
    expect(nameFromFile("no-ext")).toBe("no-ext");
    expect(nameFromFile("windows\\path\\x.workflow.json")).toBe("x");
  });
});

describe("parseImportDocument", () => {
  it("信封格式取 name + definition", () => {
    const parsed = parseImportDocument(
      JSON.stringify({
        name: "github-trending",
        description: "x",
        definition: { nodes: [], edges: [] },
      }),
      "随便.json",
    );
    expect(parsed.name).toBe("github-trending");
    expect(parsed.definition).toEqual({ nodes: [], edges: [] });
  });

  it("裸 definition 回落文件名（剥 .json 与 .workflow，与 CLI 同规则）", () => {
    const parsed = parseImportDocument(
      JSON.stringify({ nodes: [], edges: [] }),
      "github-trending.workflow.json",
    );
    expect(parsed.name).toBe("github-trending");
  });

  it("显式名字覆盖信封 name", () => {
    const parsed = parseImportDocument(
      JSON.stringify({ name: "a", definition: { nodes: [], edges: [] } }),
      "b.json",
      "c",
    );
    expect(parsed.name).toBe("c");
  });

  it("非法 JSON / 缺 nodes / 缺 edges / 非对象 都拒绝", () => {
    expect(() => parseImportDocument("{nope", "a.json")).toThrow(/JSON/);
    expect(() => parseImportDocument('{"edges": []}', "a.json")).toThrow(/nodes/);
    expect(() => parseImportDocument('{"nodes": []}', "a.json")).toThrow(/edges/);
    expect(() => parseImportDocument("[1,2]", "a.json")).toThrow(/对象/);
  });
});

describe("importWorkflow（按 name upsert）", () => {
  const definition = { nodes: [], edges: [] } as never;

  it("同名存在：追加版本不新建，默认发布", async () => {
    listWorkflows.mockResolvedValue([
      { workflow_id: "wf-old", name: "已存在", latest_version: 3, published_version: 2, created_at: "" },
    ] as unknown as WorkflowSummary[]);
    updateWorkflow.mockResolvedValue(4);
    const result = await importWorkflow("已存在", definition);
    expect(createWorkflow).not.toHaveBeenCalled();
    expect(updateWorkflow).toHaveBeenCalledWith("wf-old", definition);
    expect(publishWorkflow).toHaveBeenCalledWith("wf-old", 4);
    expect(result).toEqual({
      workflow_id: "wf-old",
      name: "已存在",
      version: 4,
      created: false,
      status: "published",
    });
  });

  it("不存在：create → update → publish", async () => {
    updateWorkflow.mockResolvedValue(1);
    const result = await importWorkflow("新工作流", definition);
    expect(createWorkflow).toHaveBeenCalledWith("新工作流");
    expect(updateWorkflow).toHaveBeenCalledWith("wf-new", definition);
    expect(result.created).toBe(true);
    expect(result.status).toBe("published");
  });

  it("publish: false 保持 draft", async () => {
    await importWorkflow("x", definition, { publish: false });
    expect(publishWorkflow).not.toHaveBeenCalled();
  });

  it("服务端校验失败（update 抛错）原样上抛，调用方 toast", async () => {
    updateWorkflow.mockRejectedValue(new Error("非法"));
    await expect(importWorkflow("x", definition)).rejects.toThrow("非法");
    // 已建的壳不回滚（与 CLI 语义一致：重新导入同名会复用）
    expect(createWorkflow).toHaveBeenCalledTimes(1);
  });
});
