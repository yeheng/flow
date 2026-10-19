import { describe, expect, it, vi } from "vitest";
import { mount, RouterLinkStub } from "@vue/test-utils";

const state = vi.hoisted(() => ({ path: "/" }));
vi.mock("vue-router", () => ({
  useRoute: () => ({ path: state.path }),
}));

import App from "./App.vue";

function mountApp() {
  return mount(App, {
    global: {
      components: {
        RouterLink: RouterLinkStub,
        RouterView: { template: "<div />" },
      },
      stubs: { AppToasts: true, AppModal: true },
    },
  });
}

/** 高亮的导航项文案：active 由 useRoute 路径前缀判定（nav-active class） */
function activeLabels(path: string): string[] {
  state.path = path;
  const w = mountApp();
  const labels = w
    .findAll("nav.nav a")
    .filter((a) => a.classes("nav-active"))
    .map((a) => a.text().trim());
  w.unmount();
  return labels;
}

describe("App 导航：active 按路径前缀判定", () => {
  it("/ 精确命中，只高亮「仪表盘」", () => {
    expect(activeLabels("/")).toEqual(["仪表盘"]);
  });

  it("/workflows/:id 高亮「工作流」", () => {
    expect(activeLabels("/workflows")).toEqual(["工作流"]);
    expect(activeLabels("/workflows/wf_1")).toEqual(["工作流"]);
  });

  it("/runs/:id 高亮「运行记录」", () => {
    expect(activeLabels("/runs")).toEqual(["运行记录"]);
    expect(activeLabels("/runs/run_1")).toEqual(["运行记录"]);
  });

  it("其余子页按前缀高亮，不误伤相邻路径", () => {
    expect(activeLabels("/journal")).toEqual(["事件审计"]);
    expect(activeLabels("/cluster")).toEqual(["集群节点"]);
    expect(activeLabels("/settings")).toEqual(["设置"]);
    expect(activeLabels("/settings/foo")).toEqual(["设置"]);
    expect(activeLabels("/workflowsx")).toEqual([]);
  });
});

describe("App 顶栏文案与连接状态", () => {
  it("导航用新术语（事件审计/集群节点）；连接状态只留圆点+文案，RPC 地址进 title", () => {
    state.path = "/";
    const w = mountApp();
    expect(w.findAll("nav.nav a").map((a) => a.text().trim())).toEqual([
      "仪表盘",
      "工作流",
      "运行记录",
      "事件审计",
      "集群节点",
      "设置",
    ]);

    const conn = w.find(".conn");
    expect(conn.text()).toMatch(/已连接|未连接/);
    expect(conn.find(".conn-dot").exists()).toBe(true);
    expect(conn.attributes("title")).toBeTruthy();
    expect(conn.text()).not.toContain("://"); // 地址不再常显在正文
    w.unmount();
  });
});
