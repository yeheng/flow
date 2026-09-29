<script setup lang="ts">
import { computed } from "vue";
import { monitor } from "../state/monitor";
import { nodeStateLabel } from "../state/labels";
import type { LogLine, TimelineNode } from "../types";
import JsonNode from "./JsonNode.vue";

/**
 * 节点检查器：回答"这个节点发生了什么"——
 * 输入面（展开后 params）/ 输出 / 错误 / 按 attempt 分组的节点日志。
 */
const props = defineProps<{ nodeId: string }>();
const emit = defineEmits<{ close: []; "open-child": [childRunId: string] }>();

const node = computed<TimelineNode | null>(
  () => monitor.nodes.find((n) => n.id === props.nodeId) ?? null,
);

const nodeLogs = computed(() => monitor.logs.filter((l) => l.node_id === props.nodeId));

/** 按 attempt 分组：重试的日志不串 */
const logsByAttempt = computed<Array<[number, LogLine[]]>>(() => {
  const groups = new Map<number, LogLine[]>();
  for (const line of nodeLogs.value) {
    const list = groups.get(line.attempt) ?? [];
    list.push(line);
    groups.set(line.attempt, list);
  }
  return [...groups.entries()];
});

const hasInput = computed(() => node.value?.input !== undefined && node.value?.input !== null);
const hasOutput = computed(
  () =>
    node.value?.output !== null &&
    node.value?.output !== undefined &&
    node.value?.state !== "pending",
);
</script>

<template>
  <div v-if="node" class="inspector">
    <div class="inspector-header">
      <span class="inspector-name">{{ node.name || node.id }}</span>
      <span class="inspector-type">{{ node.type }}</span>
      <span class="run-state" :class="`run-${node.state}`">
        {{ nodeStateLabel(node.state) }}
      </span>
      <button class="inspector-close" title="关闭" @click="emit('close')">✕</button>
    </div>

    <div class="inspector-meta">
      <span>尝试 {{ node.attempts }}</span>
      <span v-if="node.duration_ms !== null">耗时 {{ node.duration_ms }}ms</span>
      <button
        v-if="node.child_run_id"
        class="link inspector-child"
        @click="emit('open-child', node.child_run_id)"
      >
        子 run →
      </button>
    </div>

    <div v-if="node.error" class="inspector-error">{{ node.error }}</div>
    <div v-else-if="node.reason" class="inspector-reason">跳过原因：{{ node.reason }}</div>

    <details class="inspector-block" :open="hasInput">
      <summary>输入面（模板展开后）</summary>
      <div v-if="hasInput" class="inspector-json">
        <JsonNode :value="node.input" />
      </div>
      <p v-else class="inspector-empty">无输入记录</p>
    </details>

    <details class="inspector-block" :open="hasOutput">
      <summary>输出</summary>
      <div v-if="hasOutput" class="inspector-json">
        <JsonNode :value="node.output" />
      </div>
      <p v-else class="inspector-empty">尚无输出</p>
    </details>

    <details class="inspector-block" :open="nodeLogs.length > 0">
      <summary>节点日志（{{ nodeLogs.length }}）</summary>
      <p v-if="nodeLogs.length === 0" class="inspector-empty">该节点没有日志</p>
      <div v-for="[attempt, lines] in logsByAttempt" :key="attempt" class="inspector-attempt">
        <p class="inspector-attempt-label">attempt {{ attempt }}</p>
        <div
          v-for="line in lines"
          :key="line.seq"
          class="inspector-log"
          :class="`log-${line.level}`"
        >
          <span class="inspector-log-level">{{ line.level }}</span>
          <span class="inspector-log-msg">{{ line.message }}</span>
        </div>
      </div>
    </details>
  </div>
</template>

<style scoped>
.inspector {
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface2);
  padding: 8px;
  margin: 8px 0;
}

.inspector-header {
  display: flex;
  gap: 8px;
  align-items: center;
}

.inspector-name {
  font-weight: 600;
}

.inspector-type {
  color: var(--text3);
  font-size: 11px;
  font-family: var(--mono);
}

.inspector-close {
  margin-left: auto;
  padding: 0 6px;
  font-size: 11px;
}

.inspector-meta {
  display: flex;
  gap: 10px;
  margin: 6px 0;
  color: var(--text2);
  font-size: 12px;
}

.inspector-child {
  font-size: 12px;
}

.inspector-error {
  color: var(--danger);
  font-size: 12px;
  word-break: break-all;
  margin-bottom: 6px;
}

.inspector-reason {
  color: var(--text3);
  font-size: 12px;
  margin-bottom: 6px;
}

.inspector-block {
  margin: 6px 0;
}

.inspector-block summary {
  cursor: pointer;
  font-size: 12px;
  color: var(--text2);
  user-select: none;
}

.inspector-json {
  max-height: 220px;
  overflow: auto;
  padding: 6px 0 2px;
}

.inspector-empty {
  color: var(--text3);
  font-size: 11px;
  padding: 4px 0;
}

.inspector-attempt {
  margin-top: 6px;
}

.inspector-attempt-label {
  color: var(--text3);
  font-size: 10px;
  font-family: var(--mono);
}

.inspector-log {
  display: flex;
  gap: 6px;
  font-size: 11px;
  font-family: var(--mono);
  padding: 1px 0;
}

.inspector-log-level {
  color: var(--text3);
  flex-shrink: 0;
  width: 34px;
}

.inspector-log.log-warn .inspector-log-level {
  color: var(--warn);
}

.inspector-log.log-error .inspector-log-level,
.inspector-log.log-error .inspector-log-msg {
  color: var(--danger);
}

.inspector-log-msg {
  word-break: break-all;
}
</style>
