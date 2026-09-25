import { beforeEach, describe, expect, it } from "vitest";
import { dirty, editor, markSaved, type EditorNode } from "./editor";
import {
  beginDrag,
  commit,
  commitParams,
  endDrag,
  history,
  redo,
  resetHistory,
  undo,
} from "./history";
import type { NodeTypeDesc } from "../types";

const scriptType: NodeTypeDesc = {
  type: "script",
  label: "脚本",
  category: "compute",
  ports: [
    { id: "in", label: "入" },
    { id: "out", label: "出" },
  ],
  params_schema: { type: "object" },
};

function node(id: string, x = 0, y = 0): EditorNode {
  return {
    id,
    type: "flow",
    position: { x, y },
    data: { name: id, nodeType: scriptType, params: {} },
  };
}

beforeEach(() => {
  editor.nodes = [];
  editor.edges = [];
  editor.selectedNodeId = null;
  resetHistory();
});

describe("history：undo/redo", () => {
  it("基本路径：commit → 变更 → undo 还原 → redo 重做", () => {
    editor.nodes = [node("a")];
    commit();
    editor.nodes.push(node("b"));
    expect(editor.nodes.map((n) => n.id)).toEqual(["a", "b"]);

    undo();
    expect(editor.nodes.map((n) => n.id)).toEqual(["a"]);
    expect(history.canRedo).toBe(true);

    redo();
    expect(editor.nodes.map((n) => n.id)).toEqual(["a", "b"]);
    expect(history.canUndo).toBe(true);
  });

  it("新变更清空 redo 分支", () => {
    editor.nodes = [node("a")];
    commit();
    editor.nodes.push(node("b"));
    undo();
    expect(history.canRedo).toBe(true);
    commit();
    editor.nodes.push(node("c"));
    expect(history.canRedo).toBe(false);
    redo(); // 空栈 no-op
    expect(editor.nodes.map((n) => n.id)).toEqual(["a", "c"]);
  });

  it("栈上限 100：超出后最早的历史被丢弃", () => {
    editor.nodes = [node("n0")];
    for (let i = 1; i <= 120; i++) {
      commit();
      editor.nodes.push(node(`n${i}`));
    }
    let steps = 0;
    while (history.canUndo) {
      undo();
      steps++;
    }
    expect(steps).toBe(100);
    // 最早可回到 n20（121 个状态中保留最近 100 步）
    expect(editor.nodes).toHaveLength(21);
  });

  it("节点拖拽合并为一条：dragStart 基线 → 多次移动 → dragStop 一次入栈", () => {
    editor.nodes = [node("a", 0, 0)];
    markSaved();
    beginDrag();
    editor.nodes[0].position = { x: 50, y: 50 };
    editor.nodes[0].position = { x: 120, y: 160 };
    endDrag();
    undo();
    expect(editor.nodes[0].position).toEqual({ x: 0, y: 0 });
    expect(history.canUndo).toBe(false); // 只有一条历史
  });

  it("拖拽未产生位移不入栈", () => {
    editor.nodes = [node("a", 0, 0)];
    beginDrag();
    endDrag();
    expect(history.canUndo).toBe(false);
  });

  it("参数连续输入按 node+field 合并；换字段另起一条", () => {
    editor.nodes = [node("a")];
    markSaved();
    commitParams("a:code");
    editor.nodes[0].data.params.code = "1";
    commitParams("a:code");
    editor.nodes[0].data.params.code = "12";
    commitParams("a:code");
    editor.nodes[0].data.params.code = "123";
    undo();
    expect(editor.nodes[0].data.params.code).toBeUndefined();
    expect(history.canUndo).toBe(false);

    commitParams("a:code");
    editor.nodes[0].data.params.code = "x";
    commitParams("a:other");
    editor.nodes[0].data.params.other = "y";
    expect(history.canUndo).toBe(true);
    undo();
    expect(editor.nodes[0].data.params.other).toBeUndefined();
    expect(editor.nodes[0].data.params.code).toBe("x");
  });

  it("脏标记跟随保存点：undo 回到保存快照即变干净", () => {
    editor.nodes = [node("a")];
    markSaved();
    expect(dirty.value).toBe(false);
    commit();
    editor.nodes.push(node("b"));
    expect(dirty.value).toBe(true);
    undo();
    expect(dirty.value).toBe(false);
    redo();
    expect(dirty.value).toBe(true);
  });
});
