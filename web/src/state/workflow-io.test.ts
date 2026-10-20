import { beforeEach, describe, expect, it, vi } from "vitest";
import { importWorkflow, nameFromFile, parseImportDocument } from "./workflow-io";

const api = vi.hoisted(() => ({
  listNodeTypes: vi.fn(),
  validateWorkflow: vi.fn(),
  importWorkflowDefinition: vi.fn(),
  setSecret: vi.fn(),
}));
const ask = vi.hoisted(() => vi.fn());
vi.mock("../api/flow", () => api);
vi.mock("./modal", () => ({ formDialog: ask }));

beforeEach(() => {
  vi.resetAllMocks();
  api.listNodeTypes.mockResolvedValue([]);
  api.validateWorkflow.mockResolvedValue({ valid: true, missing_secrets: [] });
  api.importWorkflowDefinition.mockResolvedValue({
    workflow_id: "wf-new",
    name: "new",
    version: 1,
    created: true,
    status: "published",
  });
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

describe("importWorkflow 先校验再原子提交", () => {
  const definition = { nodes: [], edges: [] };
  it("校验通过后只调用原子导入", async () => {
    const result = await importWorkflow("new", definition);
    expect(api.validateWorkflow).toHaveBeenCalledWith(definition);
    expect(api.importWorkflowDefinition).toHaveBeenCalledWith("new", definition, true);
    expect(result?.created).toBe(true);
  });
  it("校验失败和请求失败都不提交", async () => {
    api.validateWorkflow.mockRejectedValueOnce(new Error("非法图"));
    await expect(importWorkflow("bad", definition)).rejects.toThrow("非法图");
    expect(api.importWorkflowDefinition).not.toHaveBeenCalled();
  });
  it("缺少密钥时弹表单，保存后重新校验；重复引用只填一次", async () => {
    api.validateWorkflow.mockResolvedValueOnce({
      valid: false,
      missing_secrets: [
        { node_id: "a", name: "TOKEN" },
        { node_id: "b", name: "TOKEN" },
      ],
    });
    ask.mockResolvedValue({ TOKEN: "secret" });
    await importWorkflow("new", definition);
    expect(ask.mock.calls[0][1]).toHaveLength(1);
    expect(ask.mock.calls[0][1][0].secret).toBe(true);
    expect(api.setSecret).toHaveBeenCalledWith("TOKEN", "secret");
    expect(api.validateWorkflow).toHaveBeenCalledTimes(2);
    expect(api.importWorkflowDefinition).toHaveBeenCalledTimes(1);
  });
  it("取消密钥补填不提交、不保存密钥", async () => {
    api.validateWorkflow.mockResolvedValue({
      valid: false,
      missing_secrets: [{ node_id: "a", name: "TOKEN" }],
    });
    ask.mockResolvedValue(null);
    expect(await importWorkflow("new", definition)).toBeNull();
    expect(api.setSecret).not.toHaveBeenCalled();
    expect(api.importWorkflowDefinition).not.toHaveBeenCalled();
  });
  it("缺少节点必填配置时先补齐，保持原始 JSON 不变", async () => {
    api.listNodeTypes.mockResolvedValue([
      {
        type: "http_call",
        params_schema: {
          required: ["url"],
          properties: { url: { type: "string", "x-label": "地址" } },
        },
      },
    ]);
    const original = { nodes: [{ id: "a", type: "http_call", params: {} }], edges: [] };
    ask.mockResolvedValue({ [JSON.stringify([0, "url"])]: "https://example.test" });
    await importWorkflow("new", original, { publish: false });
    expect(api.validateWorkflow).toHaveBeenCalledWith({
      nodes: [{ id: "a", type: "http_call", params: { url: "https://example.test" } }],
      edges: [],
    });
    expect(original.nodes[0].params).toEqual({});
    expect(api.importWorkflowDefinition.mock.calls[0][2]).toBe(false);
  });
  it("取消配置补填时没有校验或提交请求", async () => {
    api.listNodeTypes.mockResolvedValue([
      { type: "http_call", params_schema: { required: ["url"] } },
    ]);
    ask.mockResolvedValue(null);
    expect(
      await importWorkflow("new", {
        nodes: [{ id: "a", type: "http_call", params: {} }],
        edges: [],
      }),
    ).toBeNull();
    expect(api.validateWorkflow).not.toHaveBeenCalled();
    expect(api.importWorkflowDefinition).not.toHaveBeenCalled();
  });
});
