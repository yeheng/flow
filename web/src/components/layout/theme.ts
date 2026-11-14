import { ref } from "vue";

export type ThemeChoice = "light" | "dark" | "system";

const KEY = "flow.theme";

function stored(): ThemeChoice {
  try {
    const value = localStorage.getItem(KEY);
    if (value === "light" || value === "dark" || value === "system") return value;
  } catch {
    /* 隐私模式没有 localStorage 时按系统主题 */
  }
  return "system";
}

export const themeChoice = ref<ThemeChoice>(stored());

export function applyTheme(choice: ThemeChoice): void {
  const systemDark =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-color-scheme: dark)").matches;
  const dark = choice === "dark" || (choice === "system" && systemDark);
  document.documentElement.classList.toggle("dark", dark);
  themeChoice.value = choice;
  try {
    localStorage.setItem(KEY, choice);
  } catch {
    /* 写不进去就只改当前文档 */
  }
}

if (typeof window !== "undefined" && typeof window.matchMedia === "function") {
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
    if (themeChoice.value === "system") applyTheme("system");
  });
}
