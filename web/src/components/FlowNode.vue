<script setup lang="ts">
import { computed } from "vue";
import { Handle, Position } from "@vue-flow/core";
import { editor, nodeValidationError, sourcePortsOf, type FlowNodeData } from "../state/editor";
import { nodeChildRunId, nodeRunState, openChildRun } from "../state/monitor";

const props = defineProps<{
  id: string;
  data: FlowNodeData;
  selected?: boolean;
}>();

// 协议约定：id 为 "in" 的是入端口（target），其余都是出端口（source）
const targetPorts = computed(() => props.data.nodeType.ports.filter((p) => p.id === "in"));
const sourcePorts = computed(() => sourcePortsOf(props.data.nodeType));
// 只读运行画布（RunCanvas）在 data 里显式注入 runState/childRunId（键存在即生效，可为 null）；
// 编辑器画布不含这两个键，走 monitor 查询
const runState = computed(() =>
  "runState" in props.data ? (props.data.runState as string | null) : nodeRunState(props.id),
);
const childRunId = computed(() =>
  "childRunId" in props.data ? (props.data.childRunId as string | null) : nodeChildRunId(props.id),
);

const type = computed(() => props.data.nodeType.type);
const isCapsule = computed(() => type.value === "start" || type.value === "end");
const isSubWorkflow = computed(() => type.value === "sub_workflow");

// 每类型一个内联 SVG 图标（16×16，stroke 风格）；未知类型用圆点兜底
const icons: Record<string, string> = {
  start: '<path d="M5 3.5 12 8 5 12.5Z" fill="currentColor" stroke="none"/>',
  end: '<rect x="4.5" y="4.5" width="7" height="7" rx="1" fill="currentColor" stroke="none"/>',
  script: '<path d="M6 4 3 8l3 4M10 4l3 4-3 4"/>',
  condition: '<path d="M8 2.5 13.5 8 8 13.5 2.5 8Z"/>',
  delay: '<circle cx="8" cy="8" r="5.5"/><path d="M8 5v3l2 1.5"/>',
  http_call:
    '<circle cx="8" cy="8" r="5.5"/><path d="M2.5 8h11M8 2.5c-3.2 3.2-3.2 7.8 0 11M8 2.5c3.2 3.2 3.2 7.8 0 11"/>',
  human_task: '<circle cx="8" cy="5" r="2.2"/><path d="M3.5 13c.6-2.6 2.4-4 4.5-4s3.9 1.4 4.5 4"/>',
  sub_workflow:
    '<rect x="2.5" y="2.5" width="8" height="8" rx="1.5"/><rect x="5.5" y="5.5" width="8" height="8" rx="1.5"/>',
};
const icon = computed(() => icons[type.value] ?? '<circle cx="8" cy="8" r="5"/>');

const subTarget = computed(() => {
  const id = props.data.params.workflow_id;
  return typeof id === "string" && id ? id : null;
});
const subTargetWorkflow = computed(() =>
  editor.workflows.find((w) => w.workflow_id === subTarget.value),
);
const subWarning = computed(() => {
  if (!isSubWorkflow.value) return "";
  if (!subTarget.value) return "未选择目标工作流";
  if (!subTargetWorkflow.value) return "目标工作流不存在或已删除";
  if (!subTargetWorkflow.value.published_version) return "目标工作流未发布";
  return "";
});

/** 前端预校验错误（画布标红 + title 提示） */
const invalidMsg = computed(() => nodeValidationError(props.id));

const nodeClass = computed(() => {
  const cls = [`cat-${props.data.nodeType.category}`];
  if (isCapsule.value) cls.push("capsule");
  if (isSubWorkflow.value) cls.push("sub-workflow");
  if (runState.value) cls.push(`run-${runState.value}`);
  if (editor.highlightNodeId === props.id) cls.push("highlighted");
  if (invalidMsg.value) cls.push("invalid");
  return cls;
});

function sourceStyle(index: number, total: number) {
  // 多个 source handle（condition 的 true/false）在右边均匀分布；单出口用默认居中
  if (total <= 1) return {};
  return { top: `${((index + 1) / (total + 1)) * 100}%` };
}
</script>

<template>
  <div class="flow-node" :class="nodeClass" :title="invalidMsg ?? undefined">
    <Handle
      v-for="p in targetPorts"
      :id="p.id"
      :key="p.id"
      type="target"
      :position="Position.Left"
    />
    <div class="node-head">
      <svg class="node-icon" viewBox="0 0 16 16" v-html="icon"></svg>
      <span class="node-name">{{ data.name || data.nodeType.label }}</span>
      <span v-if="subWarning" class="node-badge" :title="subWarning">!</span>
      <span v-else-if="isSubWorkflow" class="node-drill" title="双击钻取子流程">⤢</span>
    </div>
    <div v-if="!isCapsule" class="node-body">
      <div class="node-type">{{ data.nodeType.label }}</div>
      <div v-if="isSubWorkflow" class="node-sub-target" :class="{ missing: !subTargetWorkflow }">
        {{ subTargetWorkflow?.name ?? subTarget ?? "未选择目标工作流" }}
      </div>
      <button
        v-if="childRunId"
        class="node-child-link"
        title="查看子 run"
        @click.stop="openChildRun(childRunId)"
      >
        子 run →
      </button>
    </div>
    <Handle
      v-for="(p, i) in sourcePorts"
      :id="p.id"
      :key="p.id"
      type="source"
      :position="Position.Right"
      :style="sourceStyle(i, sourcePorts.length)"
      :title="p.label"
    />
  </div>
</template>

<style scoped>
.flow-node {
  min-width: 160px;
  max-width: 220px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 2px 12px var(--shadow);
  transition:
    box-shadow 0.15s,
    border-color 0.15s;
}

.flow-node:hover {
  border-color: var(--hover-border);
  box-shadow: 0 4px 20px var(--shadow-hover);
}

/* 选中态：祖先 .vue-flow__node 在组件外，scoped 只给末级选择器加属性，仍可命中 */
.vue-flow__node.selected .flow-node {
  border-color: var(--accent);
  box-shadow:
    0 0 0 2px rgba(124, 108, 255, 0.35),
    0 4px 20px var(--shadow-hover);
}

.node-head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 12px;
}

.node-icon {
  width: 14px;
  height: 14px;
  flex: none;
  color: var(--cat-color, var(--text2));
  stroke: currentColor;
  stroke-width: 1.5;
  fill: none;
  stroke-linecap: round;
  stroke-linejoin: round;
}

.node-name {
  flex: 1;
  font-size: 12px;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.node-body {
  padding: 0 12px 10px;
}

.node-type {
  color: var(--text3);
  font-size: 9px;
  font-weight: 500;
  font-family: var(--mono);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

/* start/end 胶囊形 */
.flow-node.capsule {
  min-width: 0;
  border-radius: 999px;
}

.flow-node.capsule .node-head {
  padding: 7px 16px;
}

/* sub_workflow 卡片 */
.node-sub-target {
  margin-top: 4px;
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--surface2);
  font-size: 11px;
  color: var(--text2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.node-sub-target.missing {
  color: var(--danger);
}

.node-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: var(--warn);
  color: #0f0f11;
  font-size: 11px;
  font-weight: 700;
  cursor: help;
  flex: none;
}

.node-drill {
  color: var(--text3);
  font-size: 12px;
}

.node-child-link {
  margin-top: 6px;
  font-size: 11px;
  padding: 1px 8px;
}

/* RunPanel 行 hover/click 联动 */
.flow-node.highlighted {
  border-color: #a371f7;
  box-shadow:
    0 0 0 2px rgba(163, 113, 247, 0.35),
    0 4px 20px var(--shadow-hover);
}

/* 运行状态着色 */
.flow-node.run-running {
  border-color: var(--run);
  background: rgba(88, 166, 255, 0.12);
}

.flow-node.run-retrying {
  border-color: var(--warn);
  background: rgba(210, 153, 34, 0.12);
}

.flow-node.run-completed {
  border-color: var(--ok);
  background: rgba(63, 185, 80, 0.12);
}

.flow-node.run-failed {
  border-color: var(--danger);
  background: rgba(248, 81, 73, 0.12);
}

.flow-node.run-skipped {
  opacity: 0.5;
}

/* 前端预校验错误标红 */
.flow-node.invalid {
  border-color: var(--danger);
  box-shadow:
    0 0 0 2px rgba(248, 81, 73, 0.3),
    0 2px 12px var(--shadow);
}
</style>
