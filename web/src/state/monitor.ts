import { computed, reactive } from "vue";
import * as api from "../api/flow";
import { client, errText } from "../rpc/client";
import type { RunEvent, TimelineNode } from "../types";
import { editor } from "./editor";
import { alignProjection, applyEvent, drainBuffer, seqAction } from "./monitor-logic";
import { toast } from "./toast";

export const monitor = reactive({
  runId: null as string | null,
  /** 当前查看的 run 所属工作流：画布着色只在仍选中同一工作流时生效 */
  workflowId: null as string | null,
  inputText: "",
  phase: null as string | null,
  /** run 投影状态（run.timeline status）：awaiting_resume 时 fold phase 仍是 running */
  status: null as string | null,
  output: undefined as unknown,
  fatalError: null as string | null,
  lastSeq: 0,
  /** 定义顺序的节点状态，来自 run.timeline 初始对齐 + run.event 增量 */
  nodes: [] as TimelineNode[],
  /** 子 run 钻取栈：栈顶是当前 run 的直接父 run */
  breadcrumb: [] as { runId: string; workflowId: string }[],
  starting: false,
});

export const runActive = computed(() => monitor.runId !== null && monitor.phase === "running");

/** human_task 等待信号中的节点 */
export const waitingHumanTasks = computed(() =>
  monitor.nodes.filter((n) => n.type === "human_task" && n.state === "running"),
);

export function nodeRunState(nodeId: string): string | null {
  if (!monitor.runId || monitor.workflowId !== editor.workflowId) return null;
  return monitor.nodes.find((n) => n.id === nodeId)?.state ?? null;
}

/** sub_workflow 节点已启动的子 run（画布节点上的钻取链接用） */
export function nodeChildRunId(nodeId: string): string | null {
  if (!monitor.runId || monitor.workflowId !== editor.workflowId) return null;
  return monitor.nodes.find((n) => n.id === nodeId)?.child_run_id ?? null;
}

let unsubscribe: (() => Promise<void>) | null = null;
/** attach 世代号：并发 attach（快速切换 run）时旧世代的回调/响应一律丢弃 */
let generation = 0;
/** 订阅建立与 timeline 对齐之间到达的事件先缓冲，对齐后按 seq 补放 */
let buffer: RunEvent[] = [];
let aligned = false;

export async function startRun(): Promise<void> {
  if (!editor.workflowId) {
    toast.error("先选择一个工作流");
    return;
  }
  let input: unknown;
  const raw = monitor.inputText.trim();
  if (raw) {
    try {
      input = JSON.parse(raw);
    } catch {
      toast.error("运行输入不是合法 JSON");
      return;
    }
  }
  monitor.starting = true;
  try {
    const { run_id } = await api.startRun(editor.workflowId, input);
    // breadcrumb/workflowId 只在 attach 成功后切换：失败时 attach 已回滚到旧 run，栈要跟着留
    if (await attach(run_id)) {
      monitor.breadcrumb = [];
      monitor.workflowId = editor.workflowId;
    }
  } catch (e) {
    toast.error(errText(e));
  } finally {
    monitor.starting = false;
  }
}

/** 切换到指定 run：退订旧的、订阅新的、timeline 对齐后补放缓冲事件。
 * 返回 true 表示本次 attach 仍是当前世代且已完成对齐；被并发 attach 取代返回 false；
 * 订阅失败回滚进入前的 monitor 状态后抛出（调用方只报错） */
async function attach(runId: string): Promise<boolean> {
  const gen = ++generation;
  if (unsubscribe) {
    await unsubscribe().catch(() => {});
    unsubscribe = null;
  }
  // 进入前快照：订阅失败时回滚，不留「运行中」的幻影 run
  const prev = {
    runId: monitor.runId,
    workflowId: monitor.workflowId,
    phase: monitor.phase,
    status: monitor.status,
    output: monitor.output,
    fatalError: monitor.fatalError,
    lastSeq: monitor.lastSeq,
    nodes: monitor.nodes,
    breadcrumb: monitor.breadcrumb,
  };
  monitor.runId = runId;
  monitor.phase = "running";
  monitor.status = null;
  monitor.output = undefined;
  monitor.fatalError = null;
  monitor.lastSeq = 0;
  monitor.nodes = [];
  buffer = [];
  aligned = false;
  try {
    const unsub = await api.subscribeRun(runId, (env) => {
      // 陈旧订阅的回调可能在退订前到达：只认本世代、本 run 的事件
      if (!(gen === generation && runId === monitor.runId) || env.run_id !== runId) return;
      if (!aligned) {
        buffer.push(env);
        return;
      }
      onEvent(env);
    });
    if (!(gen === generation && runId === monitor.runId)) {
      // 期间已被新的 attach 取代：立即退订，防止回调泄漏/篡改新时间线
      void unsub().catch(() => {});
      return false;
    }
    unsubscribe = unsub;
    await resync();
    if (!(gen === generation && runId === monitor.runId)) return false;
    aligned = true;
    drainBuffer(buffer, monitor.lastSeq).forEach(onEvent);
    buffer = [];
    return true;
  } catch (e) {
    if (!(gen === generation && runId === monitor.runId)) return false; // 已被新的 attach 取代：状态归它收尾
    Object.assign(monitor, prev);
    buffer = [];
    aligned = false;
    throw e;
  }
}

/** 运行详情页入口：attach 到指定 run 并清空钻取栈 */
export async function attachRun(runId: string): Promise<boolean> {
  const ok = await attach(runId);
  if (ok) monitor.breadcrumb = [];
  return ok;
}

/** 离开运行详情页：退订并清空 monitor，使进行中的 attach 回调失效 */
export async function detachRun(): Promise<void> {
  generation++;
  if (unsubscribe) {
    await unsubscribe().catch(() => {});
    unsubscribe = null;
  }
  monitor.runId = null;
  monitor.workflowId = null;
  monitor.phase = null;
  monitor.status = null;
  monitor.output = undefined;
  monitor.fatalError = null;
  monitor.lastSeq = 0;
  monitor.nodes = [];
  monitor.breadcrumb = [];
  buffer = [];
  aligned = false;
}

/** 钻取 sub_workflow 节点的子 run */
export async function openChildRun(childRunId: string): Promise<void> {
  if (!monitor.runId || !monitor.workflowId || childRunId === monitor.runId) return;
  const parent = { runId: monitor.runId, workflowId: monitor.workflowId };
  try {
    if (await attach(childRunId)) monitor.breadcrumb.push(parent);
  } catch (e) {
    toast.error(errText(e));
  }
}

/** 返回上一级父 run */
export async function backToParentRun(): Promise<void> {
  const parent = monitor.breadcrumb[monitor.breadcrumb.length - 1];
  if (!parent) return;
  try {
    // 只有 attach 成功才弹栈：失败时 monitor 已回滚到原 run，父级要留在栈里
    if (await attach(parent.runId)) monitor.breadcrumb.pop();
  } catch (e) {
    toast.error(errText(e));
  }
}

export async function cancelRun(): Promise<void> {
  if (!monitor.runId) return;
  try {
    await api.runCancel(monitor.runId);
  } catch (e) {
    toast.error(errText(e));
  }
}

export async function deliverSignal(nodeId: string, payloadText: string): Promise<void> {
  if (!monitor.runId) return;
  const raw = payloadText.trim();
  let payload: unknown = null;
  if (raw) {
    try {
      payload = JSON.parse(raw);
    } catch {
      // payload 是任意 JSON；非 JSON 文本按字符串交付
      payload = payloadText;
    }
  }
  try {
    await api.runSignal(monitor.runId, nodeId, payload);
  } catch (e) {
    toast.error(errText(e));
  }
}

/** seq 出现缺口（订阅 Lagged 丢事件）时整体重拉 timeline 对齐 */
async function resync(): Promise<void> {
  // await 前记录世代与 runId：陈旧响应不得写回已切换的 monitor
  const gen = generation;
  const runId = monitor.runId;
  if (!runId) return;
  try {
    const tl = await api.runTimeline(runId);
    if (gen !== generation || runId !== monitor.runId) return;
    alignProjection(monitor, tl);
    // 钻取子 run 后着色守卫按子 run 自己的工作流对齐
    monitor.workflowId = tl.workflow_id;
  } catch (e) {
    if (gen === generation && runId === monitor.runId) toast.error(errText(e));
  }
}

function onEvent(env: RunEvent): void {
  switch (seqAction(monitor.lastSeq, env.seq)) {
    case "skip":
      return;
    case "resync":
      void resync();
      return;
    case "apply":
      applyEvent(monitor, env);
  }
}

// 断线重连后客户端已自动重建订阅，这里补齐断线期间错过的事件
client.onReconnect(() => {
  if (monitor.runId && monitor.phase === "running") void resync();
});
