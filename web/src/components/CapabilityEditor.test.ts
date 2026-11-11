import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { defineComponent, ref } from "vue";
import { mount } from "@vue/test-utils";

vi.mock("../state/editor", () => ({ editor: { secrets: ["PAY_TOKEN", "SMTP_KEY"] } }));

import CapabilityEditor from "./CapabilityEditor.vue";
import { newCapability } from "../state/capabilities";
import type { Capability } from "../types";

const hasSubtle = typeof globalThis.crypto?.subtle?.digest === "function";

const wrappers: ReturnType<typeof mount>[] = [];
/**
 * 用 v-model 宿主挂载：编辑器 emit 的补丁要回写成 prop（与真实父层一致），
 * 后续交互才能基于最新值继续。
 */
function create(capability: Capability) {
  const cap = ref(capability);
  const removed = ref(false);
  const host = defineComponent({
    components: { CapabilityEditor },
    setup: () => ({ cap, removed }),
    template:
      '<CapabilityEditor :capability="cap" @update:capability="cap = $event" @remove="removed = true" />',
  });
  const w = mount(host);
  wrappers.push(w);
  return {
    w,
    cap: () => cap.value,
    removed: () => removed.value,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("CapabilityEditor：动作切换与公共字段", () => {
  it("切换 action 保留 id/approval，闭集字段换新底稿（不带残留）", async () => {
    const { w, cap } = create({
      id: "cap-exec-1",
      action: "exec",
      approval: "ask",
      executable: "/usr/bin/curl",
      argv_exact: ["--version"],
    });
    const select = w.findAll("select").find((s) => s.attributes("aria-label") === "能力动作")!;
    await select.setValue("net");
    expect(cap().id).toBe("cap-exec-1");
    expect(cap().approval).toBe("ask");
    expect(cap().action).toBe("net");
    expect(cap().executable).toBeUndefined(); // 旧动作字段不残留
    expect(cap().methods).toEqual(["GET"]); // 新动作最小形态
    expect(typeof cap().url_prefix).toBe("string");
  });

  it("approval 切换 auto/ask（免审批/需审批）写回能力对象", async () => {
    const { w, cap } = create(newCapability("secret", []));
    const select = w.findAll("select").find((s) => s.attributes("aria-label") === "审批方式")!;
    await select.setValue("auto");
    expect(cap().approval).toBe("auto");
  });

  it("删除按钮 emit remove", async () => {
    const { w, removed } = create(newCapability("net", []));
    await w.findAll("button").find((b) => b.text() === "删除")!.trigger("click");
    expect(removed()).toBe(true);
  });
});

describe("CapabilityEditor：按动作的字段编辑", () => {
  it("net：方法多选按闭集顺序维护，再取消一个不乱序", async () => {
    const { w, cap } = create(newCapability("net", []));
    const boxes = w.findAll('.cap-methods input[type="checkbox"]');
    await boxes[1].setValue(true); // POST
    await boxes[4].setValue(true); // DELETE
    expect(cap().methods).toEqual(["GET", "POST", "DELETE"]);
    await boxes[0].setValue(false); // 取消 GET
    expect(cap().methods).toEqual(["POST", "DELETE"]);
  });

  it("exec：argv 一行一项增删改", async () => {
    const { w, cap } = create({ ...newCapability("exec", []), executable: "/bin/tar", argv_exact: ["czf", "a.tgz"] });
    const inputs = w.findAll(".cap-list-row input");
    expect(inputs).toHaveLength(2);
    await inputs[1].setValue("out.tgz");
    expect(cap().argv_exact).toEqual(["czf", "out.tgz"]);
    await w.findAll(".cap-list-add").find((b) => b.text().includes("参数"))!.trigger("click");
    expect(cap().argv_exact).toEqual(["czf", "out.tgz", ""]);
  });

  it("secret：密钥名 datalist 提供 secrets.list 候选", () => {
    const { w } = create(newCapability("secret", []));
    const options = w.findAll("datalist option").map((o) => o.attributes("value"));
    expect(options).toEqual(["PAY_TOKEN", "SMTP_KEY"]);
  });

  it("file_*：path 与 subtree 单选切换（切到子树清空 path）", async () => {
    const { w, cap } = create({ ...newCapability("file_write", []), path: "out/a.md" });
    const radios = w.findAll('.cap-methods input[type="radio"]');
    expect((radios[0].element as HTMLInputElement).checked).toBe(true);
    await radios[1].setValue(true); // 切到子树：两侧清空，等输入
    expect(cap().path).toBe("");
    expect(cap().subtree).toBe("");
    // 切换后输入子树值
    const input = w.find(".cap-field.grow input");
    await input.setValue("data/");
    expect(cap().subtree).toBe("data/");
  });
});

describe("CapabilityEditor：脚本摘要（WebCrypto）", () => {
  it.skipIf(!hasSubtle)("「计算摘要」用 SHA-256 算 64hex 填入 script_digest 并显示", async () => {
    const { w, cap } = create({ ...newCapability("shell", []), script_digest: "" });
    expect(w.text()).toContain("（未计算）");
    await w.find("textarea").setValue("abc");
    const button = w.findAll("button").find((b) => b.text().includes("计算摘要"))!;
    expect(button.attributes("disabled")).toBeUndefined();
    await button.trigger("click");
    // WebCrypto 摘要在宏任务上落定：flushPromises 不保证等到，用 waitFor 等回写
    await vi.waitFor(() => {
      expect(cap().script_digest).toBe(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
      );
    });
    // 回写后卡片显示摘要（64 位小写 hex）
    expect(w.find(".cap-digest").text()).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });

  it("脚本为空时「计算摘要」禁用", () => {
    const { w } = create({ ...newCapability("shell", []), script_digest: "" });
    const button = w.findAll("button").find((b) => b.text().includes("计算摘要"))!;
    expect(button.attributes("disabled")).toBeDefined();
  });
});

describe("CapabilityEditor：本地校验提示", () => {
  it("exec 非绝对路径实时提示", () => {
    const { w } = create({ ...newCapability("exec", []), executable: "curl" });
    expect(w.find(".cap-errors").text()).toContain("绝对路径");
  });

  it("合法条目不显示错误", () => {
    const { w } = create({
      ...newCapability("exec", []),
      executable: "/usr/bin/curl",
      argv_exact: ["--version"],
      approval: "auto",
    });
    expect(w.find(".cap-errors").exists()).toBe(false);
  });
});
