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
    "editor.background": "#141a18",
    "editor.foreground": "#e6eeea",
    "editorLineNumber.foreground": "#7e9089",
    "editorLineNumber.activeForeground": "#b7c6c0",
    "editor.lineHighlightBackground": "#1c2422",
    "editorCursor.foreground": "#e6eeea",
    "editor.selectionBackground": "#0f6f6244",
    "editorWidget.background": "#1c2422",
    "editorWidget.border": "#2c3834",
    "editorSuggestWidget.background": "#1c2422",
    "editorHoverWidget.background": "#1c2422",
    "scrollbarSlider.background": "#2e3b3688",
    "scrollbarSlider.hoverBackground": "#2e3b36cc",
  },
});

export { monaco };
