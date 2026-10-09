/**
 * Monaco 集中装配：worker 环境、深色主题（对齐 style.css 设计 token）。
 * CodeEditor 的唯一入口。语言按需装载：json 走完整语言服务（校验/格式化，
 * 需要 json worker），javascript 只装基础语法高亮（不引 ts worker，省体积）。
 * 注意：monaco-editor 0.55+ 的 exports map 省略 esm/vs 前缀（"." 之外按
 * "./*" → "./esm/vs/*.js" 解析），深路径导入不要带 esm/vs。
 */
import * as monaco from "monaco-editor/editor/editor.api.js";
import "monaco-editor/language/json/monaco.contribution.js";
import "monaco-editor/languages/definitions/javascript/register.js";
import EditorWorker from "monaco-editor/editor/editor.worker.js?worker";
import JsonWorker from "monaco-editor/language/json/json.worker.js?worker";

(self as typeof self & { MonacoEnvironment: monaco.Environment }).MonacoEnvironment = {
  getWorker(_workerId: string, label: string): Worker {
    if (label === "json") return new JsonWorker();
    return new EditorWorker();
  },
};

monaco.editor.defineTheme("flow-dark", {
  base: "vs-dark",
  inherit: true,
  rules: [],
  colors: {
    "editor.background": "#0f0f11",
    "editor.foreground": "#e8e8f0",
    "editorLineNumber.foreground": "#55556a",
    "editorLineNumber.activeForeground": "#888899",
    "editor.lineHighlightBackground": "#18181c",
    "editorCursor.foreground": "#e8e8f0",
    "editor.selectionBackground": "#7c6cff44",
    "editorWidget.background": "#18181c",
    "editorWidget.border": "#222228",
    "editorSuggestWidget.background": "#18181c",
    "editorHoverWidget.background": "#18181c",
    "scrollbarSlider.background": "#2a2a3288",
    "scrollbarSlider.hoverBackground": "#2a2a32cc",
  },
});

export { monaco };
