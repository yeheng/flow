<script setup lang="ts">
import { computed } from "vue";
import { Handle, Position } from "@vue-flow/core";
import { editor, sourcePortsOf, type FlowNodeData } from "../state/editor";
import { nodeChildRunId, nodeRunState, openChildRun } from "../state/monitor";

const props = defineProps<{
  id: string;
  data: FlowNodeData;
  selected?: boolean;
}>();

// 协议约定：id 为 "in" 的是入端口（target），其余都是出端口（source）
const targetPorts = computed(() => props.data.nodeType.ports.filter((p) => p.id === "in"));
const sourcePorts = computed(() => sourcePortsOf(props.data.nodeType));
const runState = computed(() => nodeRunState(props.id));
const childRunId = computed(() => nodeChildRunId(props.id));

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

const nodeClass = computed(() => {
  const cls = [`cat-${props.data.nodeType.category}`];
  if (isCapsule.value) cls.push("capsule");
  if (isSubWorkflow.value) cls.push("sub-workflow");
  if (runState.value) cls.push(`run-${runState.value}`);
  if (editor.highlightNodeId === props.id) cls.push("highlighted");
  return cls;
});

function sourceStyle(index: number, total: number) {
  // 多个 source handle（condition 的 true/false）在右边均匀分布；单出口用默认居中
  if (total <= 1) return {};
  return { top: `${((index + 1) / (total + 1)) * 100}%` };
}
</script>

<template>
  <div class="flow-node" :class="nodeClass">
    <Handle
      v-for="p in targetPorts"
      :key="p.id"
      :id="p.id"
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
      :key="p.id"
      :id="p.id"
      type="source"
      :position="Position.Right"
      :style="sourceStyle(i, sourcePorts.length)"
      :title="p.label"
    />
  </div>
</template>
