<script setup lang="ts">
import { computed } from "vue";
import { addNode, editor } from "../state/editor";
import type { NodeTypeDesc } from "../types";

const categoryLabels: Record<string, string> = {
  control: "控制",
  compute: "计算",
  integration: "集成",
  human: "人工",
  composition: "组合",
};

/** 按 category 分组，保持 nodetypes.list 的出现顺序 */
const groups = computed(() => {
  const order: string[] = [];
  const byCat = new Map<string, NodeTypeDesc[]>();
  for (const nt of editor.nodeTypes) {
    if (!byCat.has(nt.category)) {
      byCat.set(nt.category, []);
      order.push(nt.category);
    }
    byCat.get(nt.category)!.push(nt);
  }
  return order.map((cat) => ({
    cat,
    label: categoryLabels[cat] ?? cat,
    items: byCat.get(cat)!,
  }));
});

function onDragStart(event: DragEvent, nt: NodeTypeDesc): void {
  if (!event.dataTransfer) return;
  event.dataTransfer.setData("application/flow-node-type", nt.type);
  event.dataTransfer.effectAllowed = "move";
}

/** 点击即添加：落在现有节点右下方的错位空档处 */
function onClickAdd(nt: NodeTypeDesc): void {
  const n = editor.nodes.length;
  addNode(nt.type, { x: 160 + (n % 5) * 48, y: 120 + (n % 5) * 48 });
}
</script>

<template>
  <div class="palette">
    <h3>节点</h3>
    <div v-for="g in groups" :key="g.cat" class="palette-group">
      <div class="palette-cat">{{ g.label }}</div>
      <div
        v-for="nt in g.items"
        :key="nt.type"
        class="palette-item"
        :class="`cat-${nt.category}`"
        draggable="true"
        :title="`${nt.label}：拖到画布，或点击添加`"
        @dragstart="onDragStart($event, nt)"
        @click="onClickAdd(nt)"
      >
        <span class="palette-dot" />
        {{ nt.label }}
      </div>
    </div>
    <p class="palette-hint">拖拽到画布，或点击添加节点</p>
  </div>
</template>
