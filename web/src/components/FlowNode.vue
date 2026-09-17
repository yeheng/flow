<script setup lang="ts">
import { computed } from "vue";
import { Handle, Position } from "@vue-flow/core";
import type { FlowNodeData } from "../state/editor";
import { nodeRunState } from "../state/monitor";

const props = defineProps<{
  id: string;
  data: FlowNodeData;
  selected?: boolean;
}>();

// 协议约定：id 为 "in" 的是入端口（target），其余都是出端口（source）
const targetPorts = computed(() => props.data.nodeType.ports.filter((p) => p.id === "in"));
const sourcePorts = computed(() => props.data.nodeType.ports.filter((p) => p.id !== "in"));
const runState = computed(() => nodeRunState(props.id));
const nodeClass = computed(() => {
  const cls = [`cat-${props.data.nodeType.category}`];
  if (runState.value) cls.push(`run-${runState.value}`);
  return cls;
});

function sourceStyle(index: number, total: number) {
  // 多个 source handle（condition 的 true/false）在底边均匀分布
  return { left: `${((index + 1) / (total + 1)) * 100}%` };
}
</script>

<template>
  <div class="flow-node" :class="nodeClass">
    <Handle
      v-for="p in targetPorts"
      :key="p.id"
      :id="p.id"
      type="target"
      :position="Position.Top"
    />
    <div class="node-name">{{ data.name || data.nodeType.label }}</div>
    <div class="node-type">{{ data.nodeType.label }}</div>
    <Handle
      v-for="(p, i) in sourcePorts"
      :key="p.id"
      :id="p.id"
      type="source"
      :position="Position.Bottom"
      :style="sourceStyle(i, sourcePorts.length)"
    />
    <div v-if="sourcePorts.length > 1" class="port-labels">
      <span v-for="p in sourcePorts" :key="p.id">{{ p.label }}</span>
    </div>
  </div>
</template>
