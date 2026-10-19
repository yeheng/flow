/**
 * 「开发者界面」共享偏好：时间线表格、原始事件、节点日志等调试面统一由这一个
 * 开关控制。此前 RunMonitor 自己存一份（flow.run.devMode）、RunDetailView 永远
 * 显示日志页签，同一个概念两套语义，普通用户被原始事件淹没。
 */
import { ref, watch } from "vue";

/** 沿用旧键，避免已有用户偏好丢失 */
const KEY = "flow.run.devMode";

export const devMode = ref(localStorage.getItem(KEY) === "1");

watch(devMode, (on) => {
  try {
    localStorage.setItem(KEY, on ? "1" : "0");
  } catch {
    // 存储不可用不影响交互
  }
});
