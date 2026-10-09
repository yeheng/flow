<script setup lang="ts">
import { computed } from "vue";
import { MarkerType, VueFlow } from "@vue-flow/core";
import type { NodeMouseEvent } from "@vue-flow/core";
import { Background, BackgroundVariant } from "@vue-flow/background";
import { Controls } from "@vue-flow/controls";
import { MiniMap } from "@vue-flow/minimap";
import { definitionToFlow } from "../state/editor";
import { monitor } from "../state/monitor";
import type { Definition } from "../types";
import FlowNode from "./FlowNode.vue";

/** 只读运行画布：按 run 钉死的版本定义渲染，节点运行态/边动画来自 monitor 投影 */
const props = defineProps<{ definition: Definition }>();
const emit = defineEmits<{ "node-click": [nodeId: string]; "open-child": [childRunId: string] }>();

const defaultEdgeOptions = {
  markerEnd: { type: MarkerType.ArrowClosed, color: "#7c6cff" },
};

const flow = computed(() => definitionToFlow(props.definition));

const nodeStates = computed(() => new Map(monitor.nodes.map((n) => [n.id, n.state])));
/** sub_workflow 节点已启动的子 run（节点上「子 run →」钻取入口用），与 runState 同源投影 */
const childRuns = computed(() => new Map(monitor.nodes.map((n) => [n.id, n.child_run_id ?? null])));

const nodes = computed(() =>
  flow.value.nodes.map((n) => ({
    ...n,
    data: {
      ...n.data,
      runState: nodeStates.value.get(n.id) ?? null,
      childRunId: childRuns.value.get(n.id) ?? null,
    },
  })),
);

function onNodeClick(e: NodeMouseEvent): void {
  emit("node-click", e.node.id);
}

// 运行中节点的下游边做流动动画（与编辑器画布同一规则）
const edges = computed(() => {
  const running = new Set(
    monitor.nodes.filter((n) => n.state === "running" || n.state === "retrying").map((n) => n.id),
  );
  return flow.value.edges.map((e) => ({ ...e, animated: running.has(e.source) }));
});
</script>

<template>
  <div class="canvas-wrap">
    <VueFlow
      :nodes="nodes"
      :edges="edges"
      :nodes-draggable="false"
      :nodes-connectable="false"
      :elements-selectable="false"
      :delete-key-code="null"
      :default-viewport="{ zoom: 1 }"
      :default-edge-options="defaultEdgeOptions"
      :min-zoom="0.2"
      :max-zoom="2"
      color-mode="dark"
      fit-view-on-init
      @node-click="onNodeClick"
    >
      <template #node-flow="nodeProps">
        <FlowNode v-bind="nodeProps" @open-child="emit('open-child', $event)" />
      </template>
      <Background
        :variant="BackgroundVariant.Dots"
        :gap="28"
        :size="1.5"
        color="rgba(255, 255, 255, 0.06)"
      />
      <Controls :show-interactive="false" />
      <MiniMap />
    </VueFlow>
  </div>
</template>
