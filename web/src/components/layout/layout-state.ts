import { ref } from "vue";

export type SidebarVariant = "inset" | "floating" | "sidebar";
export type SidebarCollapsible = "offcanvas" | "icon" | "none";
export type Direction = "ltr" | "rtl";

const VARIANT_KEY = "flow.layout.variant";
const COLLAPSIBLE_KEY = "flow.layout.collapsible";
const DIR_KEY = "flow.dir";

function read<T extends string>(key: string, allowed: readonly T[], fallback: T): T {
  try {
    const value = localStorage.getItem(key);
    if (value && (allowed as readonly string[]).includes(value)) return value as T;
  } catch {
    /* 隐私模式读不到时用默认值 */
  }
  return fallback;
}

export const sidebarVariant = ref<SidebarVariant>(
  read(VARIANT_KEY, ["inset", "floating", "sidebar"] as const, "inset"),
);
export const sidebarCollapsible = ref<SidebarCollapsible>(
  read(COLLAPSIBLE_KEY, ["offcanvas", "icon", "none"] as const, "icon"),
);
export const direction = ref<Direction>(read(DIR_KEY, ["ltr", "rtl"] as const, "ltr"));

function persist(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* 写不进去只保留当前会话 */
  }
}

export function setSidebarVariant(value: SidebarVariant): void {
  sidebarVariant.value = value;
  persist(VARIANT_KEY, value);
}

export function setSidebarCollapsible(value: SidebarCollapsible): void {
  sidebarCollapsible.value = value;
  persist(COLLAPSIBLE_KEY, value);
}

export function setDirection(value: Direction): void {
  direction.value = value;
  document.documentElement.dir = value;
  persist(DIR_KEY, value);
}

export function resetLayout(): void {
  setSidebarVariant("inset");
  setSidebarCollapsible("icon");
  setDirection("ltr");
}

if (typeof document !== "undefined") {
  document.documentElement.dir = direction.value;
}
