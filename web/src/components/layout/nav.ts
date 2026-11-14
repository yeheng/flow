import type { Component } from "vue";
import { LayoutDashboard, ScrollText, Settings, ListTree, Workflow } from "@lucide/vue";

export interface NavLinkItem {
  title: string;
  url: string;
  icon?: Component;
  badge?: string;
}

export interface NavItem {
  title: string;
  url?: string;
  icon?: Component;
  badge?: string;
  items?: NavLinkItem[];
}

export interface NavGroupData {
  title: string;
  items: NavItem[];
}

/** 分组方式对齐 shadcn-admin 的 sidebarData.navGroups，条目是 Flow 自己的页面。 */
export const navGroups: NavGroupData[] = [
  {
    title: "概览",
    items: [{ title: "仪表盘", url: "/", icon: LayoutDashboard }],
  },
  {
    title: "编排",
    items: [{ title: "工作流", url: "/workflows", icon: Workflow }],
  },
  {
    title: "记录",
    items: [
      { title: "运行记录", url: "/runs", icon: ListTree },
      { title: "事件审计", url: "/journal", icon: ScrollText },
    ],
  },
  {
    title: "其他",
    items: [{ title: "设置", url: "/settings", icon: Settings }],
  },
];

/** `/` 精确命中，其余按路径前缀。 */
export function navActive(path: string, url: string): boolean {
  return url === "/" ? path === "/" : path === url || path.startsWith(`${url}/`);
}
