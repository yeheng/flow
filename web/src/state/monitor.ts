import { computed, reactive } from "vue";
import * as api from "../api/flow";
import { client, errText } from "../rpc/client";
import type { RunEvent, TimelineNode } from "../types";
import { editor, ui } from "./editor";

export const monitor = reactive({
  runId: null as string | null,
  /** 当前查看的 run 所属工作流：画布着色只在仍选中同一工作流时生效 */
  workflowId: null as string | null,
  inputText: "",
  phase: null as string | null,
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
/** 订阅建立与 timeline 对齐之间到达的事件先缓冲，对齐后按 seq 补放 */
let buffer: RunEvent[] = [];
let aligned = false;

export async function startRun(): Promise<void> {
  if (!editor.workflowId) {
    ui.error = "先选择一个工作流";
    return;
  }
  let input: unknown;
  const raw = monitor.inputText.trim();
  if (raw) {
    try {
      input = JSON.parse(raw);
    } catch {
      ui.error = "运行输入不是合法 JSON";
      return;
    }
  }
  monitor.starting = true;
  try {
    const { run_id } = await api.startRun(editor.workflowId, input);
    monitor.breadcrumb = [];
    monitor.workflowId = editor.workflowId;
    await attach(run_id);
    ui.error = null;
  } catch (e) {
    ui.error = errText(e);
  } finally {
    monitor.starting = false;
  }
}

/** 切换到指定 run：退订旧的、订阅新的、timeline 对齐后补放缓冲事件 */
async function attach(runId: string): Promise<void> {
  if (unsubscribe) {
    await unsubscribe().catch(() => {});
    unsubscribe = null;
  }
  monitor.runId = runId;
  monitor.phase = "running";
  monitor.output = undefined;
  monitor.fatalError = null;
  monitor.lastSeq = 0;
  monitor.nodes = [];
  buffer = [];
  aligned = false;
  unsubscribe = await api.subscribeRun(runId, (env) => {
    if (!aligned) {
      buffer.push(env);
      return;
    }
    onEvent(env);
  });
  await resync();
  aligned = true;
  buffer.sort((a, b) => a.seq - b.seq).forEach(onEvent);
  buffer = [];
}

/** 钻取 sub_workflow 节点的子 run */
export async function openChildRun(childRunId: string): Promise<void> {
  if (!monitor.runId || !monitor.workflowId || childRunId === monitor.runId) return;
  const parent = { runId: monitor.runId, workflowId: monitor.workflowId };
  try {
    await attach(childRunId);
    monitor.breadcrumb.push(parent);
  } catch (e) {
    ui.error = errText(e);
  }
}

/** 返回上一级父 run */
export async function backToParentRun(): Promise<void> {
  const parent = monitor.breadcrumb.pop();
  if (!parent) return;
  try {
    await attach(parent.runId);
  } catch (e) {
    ui.error = errText(e);
  }
}

export async function cancelRun(): Promise<void> {
  if (!monitor.runId) return;
  try {
    await api.runCancel(monitor.runId);
  } catch (e) {
    ui.error = errText(e);
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
    ui.error = errText(e);
  }
}

/** seq 出现缺口（订阅 Lagged 丢事件）时整体重拉 timeline 对齐 */
async function resync(): Promise<void> {
  if (!monitor.runId) return;
  try {
    const tl = await api.runTimeline(monitor.runId);
    monitor.nodes = tl.nodes;
    monitor.phase = tl.phase;
    monitor.output = tl.output;
    monitor.fatalError = tl.fatal_error;
    monitor.lastSeq = tl.last_seq;
    // 钻取子 run 后着色守卫按子 run 自己的工作流对齐
    monitor.workflowId = tl.workflow_id;
  } catch (e) {
    ui.error = errText(e);
  }
}

function onEvent(env: RunEvent): void {
  if (env.seq <= monitor.lastSeq) return;
  if (env.seq !== monitor.lastSeq + 1) {
    void resync();
    return;
  }
  applyEvent(env);
}

function applyEvent(env: RunEvent): void {
  monitor.lastSeq = env.seq;
  const rec = env.node_id ? monitor.nodes.find((n) => n.id === env.node_id) : undefined;
  switch (env.type) {
    case "run_started":
      monitor.phase = "running";
      break;
    case "node_started":
      if (rec) {
        rec.state = "running";
        rec.attempts = Math.max(rec.attempts, env.attempt ?? 1);
        rec.started_at = env.ts;
        rec.ended_at = null;
        rec.duration_ms = null;
        rec.output = null;
        rec.error = null;
        // 重试 = 新 attempt = 新的确定性 child_run_id
        rec.child_run_id = env.child_run_id;
      }
      break;
    case "node_completed":
      if (rec) {
        rec.state = "completed";
        rec.attempts = Math.max(rec.attempts, env.attempt ?? 1);
        rec.ended_at = env.ts;
        rec.duration_ms = env.duration_ms ?? null;
        rec.output = env.output ?? null;
        rec.error = null;
      }
      break;
    case "node_failed":
      if (rec) {
        rec.state = env.retryable ? "retrying" : "failed";
        rec.attempts = Math.max(rec.attempts, env.attempt ?? 1);
        rec.ended_at = env.ts;
        rec.error = env.error ?? null;
      }
      break;
    case "node_skipped":
      if (rec) {
        rec.state = "skipped";
        rec.reason = env.reason;
        rec.ended_at = env.ts;
      }
      break;
    case "signal_received":
      break; // 信号落盘本身不改变时间线，节点终态由后续事件推进
    case "run_completed":
      monitor.phase = "succeeded";
      monitor.output = env.output;
      break;
    case "run_failed":
      monitor.phase = "failed";
      monitor.fatalError = env.error ?? null;
      break;
    case "run_cancelled":
      monitor.phase = "cancelled";
      break;
  }
}

// 断线重连后客户端已自动重建订阅，这里补齐断线期间错过的事件
client.onReconnect(() => {
  if (monitor.runId && monitor.phase === "running") void resync();
});
