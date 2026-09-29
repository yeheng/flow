import { computed, reactive } from "vue";
import * as api from "../api/flow";
import { client, errText } from "../rpc/client";
import type { LogLine, RunEvent, TimelineNode } from "../types";
import { editor } from "./editor";
import { TERMINAL_RUN_STATUSES } from "./labels";
import {
  alignProjection,
  applyEvent,
  drainBuffer,
  seqAction,
  type RunProjection,
} from "./monitor-logic";
import { toast } from "./toast";

export const monitor = reactive({
  runId: null as string | null,
  /** 当前查看的 run 所属工作流：画布着色只在仍选中同一工作流时生效 */
  workflowId: null as string | null,
  inputText: "",
  /**
   * run 状态，唯一出处（run.timeline 的 status）。此前还有一份 `phase`
   * （timeline 的 fold 相位），两者对 awaiting_resume 不一致——phase 仍是
   * running，于是 UI 只能用 SPECIAL_STATUS 特判仲裁，结果挂起的 run 渲染成
   * 「运行中配色 + 挂起待恢复文案」。status 是投影、是权威，删掉 phase。
   */
  status: null as string | null,
  output: undefined as unknown,
  fatalError: null as string | null,
  lastSeq: 0,
  /** 定义顺序的节点状态，来自 run.timeline 初始对齐 + run.event 增量 */
  nodes: [] as TimelineNode[],
  /** 节点日志（node_log 事件流，历史段来自订阅回放） */
  logs: [] as LogLine[],
  /** `logs` 按 node_id 的索引（与 logs 同步维护，见 monitor-logic.applyLog） */
  logsByNode: {} as Record<string, LogLine[]>,
  /** 日志去重水位：已收到的最大 node_log seq */
  lastLogSeq: 0,
  /** 子 run 钻取栈：栈顶是当前 run 的直接父 run */
  breadcrumb: [] as { runId: string; workflowId: string }[],
  starting: false,
});

/**
 * run 已确定终结了吗？`status === null` 是**不知道**（timeline 还没回来/
 * 拉失败），既不是终结也不是在跑——所以 `canCancelRun` / `needReattach`
 * 都在 true 一侧：不知道的时候宁可多问一次服务端，也不谎报。
 */
const isTerminal = (): boolean =>
  monitor.status !== null && TERMINAL_RUN_STATUSES.has(monitor.status);

/**
 * 取消按钮：不知道终态时照常可点（点了服务端会给 conflict，不算错）。
 * 判据是「没拿到终态」，不是「确定还在跑」——把 null 当成 running 是撒谎，
 * 把 null 当成终态是漏报，两者都不如承认不知道。
 */
export const canCancelRun = computed(() => monitor.runId !== null && !isTerminal());

/** 断线后要不要整体重建投影。判据同 canCancelRun：run 已确定终结才不重建。 */
export const needReattach = computed(() => monitor.runId !== null && !isTerminal());

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
/**
 * attach 令牌：单调递增，每次 attach/detach 抢占一次。
 * 投影全程在局部变量里构建（订阅缓冲 → timeline 对齐 → 缓冲回放），换入前比对令牌——
 * 不是最新的就退订走人。monitor 从头到尾要么完全是旧 run、要么完全是新 run，
 * 没有中间态，也就不需要快照回滚。
 */
let attachToken = 0;

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
    // breadcrumb/workflowId 只在 attach 成功后切换：失败时 monitor 保持旧 run，栈要跟着留
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

/**
 * 切换到指定 run：退订旧的、订阅新的，投影在局部构建完成后一次性换入 monitor。
 * 返回 true 表示本次 attach 完成换入；被更新的 attach/detach 取代返回 false；
 * 订阅失败抛出（monitor 保持原状，调用方只报错）。
 */
async function attach(runId: string): Promise<boolean> {
  const token = ++attachToken;
  if (unsubscribe) {
    await unsubscribe().catch(() => {});
    unsubscribe = null;
  }

  // 局部投影：订阅建立与 timeline 对齐之间到达的事件先入缓冲。
  // status 缺省 null = 「还不知道」：不撒谎成 running（会对已终结的 run 显示
  // 可用取消按钮），canCancelRun / needReattach 也不依赖它是否为 running。
  const proj: RunProjection = {
    status: null as string | null,
    logsByNode: {} as Record<string, LogLine[]>,
    output: undefined as unknown,
    fatalError: null as string | null,
    lastSeq: 0,
    nodes: [] as TimelineNode[],
    logs: [],
    lastLogSeq: 0,
  };
  const buffer: RunEvent[] = [];
  let aligned = false;

  const onEvent = (env: RunEvent): void => {
    if (env.run_id !== runId) return;
    if (!aligned) {
      buffer.push(env);
      return;
    }
    // 换入后又被新的 attach/detach 取代：事件不再属于当前 monitor
    if (monitor.runId !== runId) return;
    // 日志有自己的水位（历史/实时统一路径），不走状态事件过滤
    if (env.type === "node_log") {
      applyEvent(monitor, env);
      return;
    }
    if (seqAction(monitor.lastSeq, env.seq) === "skip") return;
    applyEvent(monitor, env);
  };

  const unsub = await api.subscribeRun(runId, onEvent);

  const tl = await api.runTimeline(runId).catch((e: unknown) => {
    if (token === attachToken) toast.error(errText(e));
    return null;
  });
  if (tl) alignProjection(proj, tl);

  // 构建期间出现了更新的 attach 或 detach：本次作废，monitor 不动
  if (token !== attachToken) {
    void unsub().catch(() => {});
    return false;
  }

  // 原子换入（同步块，无 await）：monitor 从旧 run 整体切到新 run
  unsubscribe = unsub;
  monitor.runId = runId;
  monitor.status = proj.status;
  monitor.output = proj.output;
  monitor.fatalError = proj.fatalError;
  monitor.lastSeq = proj.lastSeq;
  monitor.nodes = proj.nodes;
  monitor.logs = proj.logs;
  monitor.logsByNode = proj.logsByNode;
  monitor.lastLogSeq = proj.lastLogSeq;
  // 钻取子 run 后着色守卫按子 run 自己的工作流对齐；timeline 拉取失败时退化为不着色
  monitor.workflowId = tl?.workflow_id ?? null;
  aligned = true;
  // 回放段含全部历史日志（订阅从 seq=1 回放），排序后统一补放：
  // 状态事件由 seqAction 去重，日志由 lastLogSeq 水位去重
  drainBuffer(buffer).forEach(onEvent);
  return true;
}

/**
 * 投影重建（原 resyncFor）：断线重连 / 流异常后的唯一恢复路径 = re-attach
 * 同一个 run。attach 从新订阅的回放整体重建投影（状态 + 日志），令牌守卫
 * 保证与进行中的其他 attach 竞争安全；面包屑不动。
 */
async function reattachFor(runId: string): Promise<void> {
  try {
    await attach(runId);
  } catch {
    // 连接尚未就绪等瞬时错误：onReconnect 会再次触发，不弹错
  }
}

/** 运行详情页入口：attach 到指定 run 并清空钻取栈 */
export async function attachRun(runId: string): Promise<boolean> {
  const ok = await attach(runId);
  if (ok) monitor.breadcrumb = [];
  return ok;
}

/** 离开运行详情页：退订并清空 monitor，使进行中的 attach 在换入前自动放弃 */
export async function detachRun(): Promise<void> {
  attachToken++;
  if (unsubscribe) {
    await unsubscribe().catch(() => {});
    unsubscribe = null;
  }
  monitor.runId = null;
  monitor.workflowId = null;
  monitor.status = null;
  monitor.output = undefined;
  monitor.fatalError = null;
  monitor.lastSeq = 0;
  monitor.nodes = [];
  monitor.logs = [];
  monitor.logsByNode = {};
  monitor.lastLogSeq = 0;
  monitor.breadcrumb = [];
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
    // 只有 attach 成功才弹栈：失败时 monitor 仍是原 run，父级要留在栈里
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

// 断线重连后客户端已自动重建订阅，投影整体重建（状态 + 日志从回放重来）
client.onReconnect(() => {
  if (needReattach.value) void reattachFor(monitor.runId!);
});
