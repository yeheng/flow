<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { comparisonRows, comparisonSummary } from "../state/regression";
import type { OverlayEntry } from "../state/overlays";
import type { RunComparison } from "../types";
import AppButton from "./AppButton.vue";

/**
 * 基线 vs 候选的差异面板（全局弹层栈 kind="compare" 的渲染方）。
 * 回归候选自带 source_detail=regression:<baseline>，打开时预填基线并自动比较；
 * 也可手动输入任意基线 run id。Esc 由全局弹层栈关栈顶，本组件不监听键盘。
 */
export interface ComparePayload {
  baseline: string | null;
  candidate: string;
}

const props = defineProps<{ entry: OverlayEntry<ComparePayload> }>();

const baseline = ref(props.entry.payload.baseline ?? "");
const candidateRunId = computed(() => props.entry.payload.candidate);
const report = ref<RunComparison | null>(null);
const loading = ref(false);
const error = ref("");

function close(): void {
  props.entry.close();
}

const summary = computed(() => (report.value ? comparisonSummary(report.value) : null));
const rows = computed(() => (report.value ? comparisonRows(report.value.differences) : []));
const sameRun = computed(() => baseline.value.trim() === candidateRunId.value);

async function compare(): Promise<void> {
  const base = baseline.value.trim();
  if (!base || loading.value) return;
  if (base === candidateRunId.value) {
    error.value = "基线不能与候选是同一个 run";
    report.value = null;
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    report.value = await api.compareRuns(base, candidateRunId.value);
  } catch (e) {
    report.value = null;
    error.value = errText(e);
  } finally {
    loading.value = false;
  }
}

onMounted(async () => {
  if (props.entry.payload.baseline) await compare();
});
</script>

<template>
  <div class="compare-overlay" @click.self="close">
    <div class="compare-modal" role="dialog" aria-modal="true" aria-label="与基线比较">
      <header class="compare-header">
        <h3>与基线比较</h3>
        <span class="muted compare-subtitle mono">候选 {{ candidateRunId.slice(0, 8) }}…</span>
        <button class="compare-close" title="关闭 (Esc)" @click="close">×</button>
      </header>
      <div class="compare-body">
        <div class="compare-form">
          <label for="compare-baseline">基线 run id</label>
          <input
            id="compare-baseline"
            v-model="baseline"
            type="text"
            placeholder="粘贴基线 run id"
            spellcheck="false"
            @keyup.enter="compare"
          />
          <AppButton :disabled="loading || !baseline.trim() || sameRun" @click="compare">
            {{ loading ? "比较中…" : "比较" }}
          </AppButton>
        </div>
        <p v-if="sameRun" class="compare-error">基线不能与候选是同一个 run</p>
        <p v-if="error" class="compare-error" role="alert">{{ error }}</p>

        <template v-if="report && summary">
          <div class="compare-summary">
            <span
              class="badge"
              :class="`run-${summary.tone === 'ok' ? 'succeeded' : summary.tone === 'danger' ? 'failed' : 'awaiting'}`"
            >
              {{ summary.label }}
            </span>
            <span class="muted compare-meta">
              基线
              <RouterLink class="link" :to="`/runs/${report.baseline}`">
                {{ report.baseline.slice(0, 8) }}…
              </RouterLink>
              · 候选
              <RouterLink class="link" :to="`/runs/${report.candidate}`">
                {{ report.candidate.slice(0, 8) }}…
              </RouterLink>
              <template v-if="!report.complete"> · 两侧都终结后才能判定</template>
              <template v-if="report.truncated"> · 差异过多已截断（最多显示 64 条）</template>
            </span>
          </div>

          <table v-if="rows.length" class="compare-table">
            <thead>
              <tr>
                <th>位置</th>
                <th>基线</th>
                <th>候选</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in rows" :key="row.path">
                <td class="compare-path" :title="row.path">{{ row.label }}</td>
                <td>
                  <pre class="compare-value">{{ row.expected }}</pre>
                </td>
                <td>
                  <pre class="compare-value">{{ row.actual }}</pre>
                </td>
              </tr>
            </tbody>
          </table>
          <p v-else-if="report.passed" class="muted compare-meta">
            最终状态、输出与全部节点结果一致（含递归子 run）。
          </p>
          <p v-else class="muted compare-meta">
            暂无差异明细{{ report.complete ? "" : "（两侧尚未都终结）" }}。
          </p>
        </template>
        <p v-else-if="!loading && !error" class="muted compare-empty">
          填入基线 run id 后比较：状态、最终输出与逐节点结果。
        </p>
      </div>
    </div>
  </div>
</template>

<style scoped>
.compare-overlay {
  position: fixed;
  inset: 0;
  z-index: 85;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}

.compare-modal {
  width: 860px;
  max-width: 100%;
  max-height: 82vh;
  display: flex;
  flex-direction: column;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
}

.compare-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}

.compare-header h3 {
  margin: 0;
  font-size: 14px;
}

.compare-subtitle {
  font-size: 11px;
}

.compare-close {
  margin-left: auto;
  padding: 0 8px;
  border: none;
  background: none;
  font-size: 18px;
  line-height: 1.2;
  color: var(--text2);
}

.compare-close:hover {
  color: var(--text);
  background: var(--surface2);
}

.compare-body {
  padding: 14px 16px;
  overflow: auto;
}

.compare-form {
  display: flex;
  align-items: center;
  gap: 8px;
}

.compare-form label {
  color: var(--text2);
  font-size: 12px;
  white-space: nowrap;
}

.compare-form input {
  flex: 1;
  min-width: 0;
  font-family: var(--mono);
  font-size: 12px;
}

.compare-error {
  margin: 8px 0 0;
  color: var(--danger);
  font-size: 12px;
}

.compare-summary {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  margin-top: 14px;
}

.compare-meta {
  font-size: 12px;
}

.compare-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 12px;
  table-layout: fixed;
}

.compare-table th {
  text-align: left;
  font-size: 11px;
  color: var(--text2);
  font-weight: 500;
  padding: 4px 8px;
  border-bottom: 1px solid var(--border);
}

.compare-table td {
  vertical-align: top;
  padding: 6px 8px;
  border-bottom: 1px solid var(--border);
}

.compare-table th:first-child,
.compare-table td:first-child {
  width: 140px;
}

.compare-path {
  font-family: var(--mono);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.compare-value {
  margin: 0;
  font-family: var(--mono);
  font-size: 11px;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  max-height: 180px;
  overflow: auto;
}

.compare-empty {
  margin: 14px 0 0;
  font-size: 12px;
}
</style>
