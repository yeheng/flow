<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { editor, refreshWorkflows } from "../state/editor";
import { normalizeFixtures } from "../state/regression";
import { toast } from "../state/toast";
import type { RunRecord, VersionMeta } from "../types";
import AppButton from "./AppButton.vue";

/**
 * 回归发起弹窗：baseline run 必须已终结且带执行上下文（服务端校验）。
 * 目标工作流固定为基线所属工作流——fixtures 按 node_key 命中，跨工作流回归
 * 基本必然 fixture miss；候选默认取该工作流最新已发布版本，即「同一资产的新版本」。
 */
const props = defineProps<{ baseline: RunRecord }>();
const emit = defineEmits<{ close: []; created: [runId: string] }>();

const workflowId = props.baseline.workflow_id;
const workflowName = computed(
  () =>
    editor.workflows.find((w) => w.workflow_id === workflowId)?.name ?? props.baseline.workflow_id,
);
const version = ref<number | null>(null);
const versions = ref<VersionMeta[]>([]);
const loadingVersions = ref(false);
const submitting = ref(false);
const error = ref("");
/** 高级：覆盖外部操作结果（JSON 表，空=全部用基线录制结果） */
const fixturesText = ref("");
const fixturesError = computed(() => normalizeFixtures(fixturesText.value).error);

const publishedVersions = computed(() =>
  versions.value.filter((v) => v.status === "published").sort((a, b) => b.version - a.version),
);
const canSubmit = computed(() => !submitting.value && version.value !== null);

async function loadVersions(): Promise<void> {
  loadingVersions.value = true;
  error.value = "";
  try {
    versions.value = await api.listVersions(workflowId);
    version.value = publishedVersions.value[0]?.version ?? null;
  } catch (e) {
    versions.value = [];
    version.value = null;
    error.value = errText(e);
  } finally {
    loadingVersions.value = false;
  }
}

function onKeydown(e: KeyboardEvent): void {
  if (e.key === "Escape" && !submitting.value) emit("close");
}

onMounted(async () => {
  window.addEventListener("keydown", onKeydown);
  if (editor.workflows.length === 0) await refreshWorkflows();
  await loadVersions();
});

onBeforeUnmount(() => window.removeEventListener("keydown", onKeydown));

async function submit(): Promise<void> {
  if (!canSubmit.value || version.value === null) return;
  const normalized = normalizeFixtures(fixturesText.value);
  if (normalized.error) {
    error.value = normalized.error;
    return;
  }
  submitting.value = true;
  error.value = "";
  try {
    const created = await api.startRegression(props.baseline.id, {
      workflowId,
      version: version.value,
      ...(normalized.fixtures ? { fixtures: normalized.fixtures } : {}),
    });
    toast.success("已创建回归运行");
    emit("created", created.run_id);
  } catch (e) {
    error.value = errText(e);
  } finally {
    submitting.value = false;
  }
}
</script>

<template>
  <div class="regress-overlay" @click.self="!submitting && emit('close')">
    <div class="regress-modal" role="dialog" aria-modal="true" aria-label="以本运行为基线回归">
      <header class="regress-header">
        <h3>回归运行</h3>
        <button class="regress-close" title="关闭 (Esc)" @click="emit('close')">×</button>
      </header>
      <div class="regress-body">
        <div class="field">
          <label>基线 run</label>
          <code class="regress-baseline" :title="baseline.id">{{ baseline.id }}</code>
          <span class="muted regress-hint">
            v{{ baseline.workflow_version }} · {{ baseline.status }}
          </span>
        </div>
        <div class="field">
          <label>目标工作流</label>
          <div class="regress-workflow">{{ workflowName }}</div>
          <span class="muted regress-hint">
            固定为基线所属工作流（外部操作结果按节点身份匹配）
          </span>
        </div>
        <div class="field">
          <label for="regress-version">候选版本</label>
          <select
            id="regress-version"
            v-model.number="version"
            :disabled="loadingVersions || publishedVersions.length === 0"
          >
            <option v-if="loadingVersions" :value="null">加载中…</option>
            <option v-for="v in publishedVersions" :key="v.version" :value="v.version">
              v{{ v.version }} · 已发布
            </option>
          </select>
          <span
            v-if="!loadingVersions && publishedVersions.length === 0"
            class="muted regress-hint"
          >
            目标工作流没有已发布版本，无法回归
          </span>
        </div>
        <p class="muted regress-note">
          回归不产生外部副作用：HTTP/harness
          复用基线记录的结果与人工输入，请求摘要不一致的节点会显式失败。
        </p>
        <details class="regress-advanced">
          <summary>高级：覆盖外部操作结果</summary>
          <div class="field">
            <label for="regress-fixtures">覆盖表（JSON，可空）</label>
            <textarea
              id="regress-fixtures"
              v-model="fixturesText"
              class="regress-fixtures mono"
              rows="5"
              spellcheck="false"
              placeholder='{"[\"n1\"]": {"fingerprint": "<64位十六进制>", "outcome": {"status": 200}}}'
            ></textarea>
            <span class="muted regress-hint">
              键是节点路径的紧凑 JSON 数组（如 "[\"n1\"]"，子 run 为 ["n2","c1","n1"] 形态）；
              fingerprint 是请求指纹（对比报告的「请求指纹」行可复制）；outcome 写业务值即可。
              覆盖会显式记录，仅用于需要替换录制结果的测试场景。
            </span>
            <p v-if="fixturesError" class="regress-error">{{ fixturesError }}</p>
          </div>
        </details>
        <p v-if="error" class="regress-error" role="alert">{{ error }}</p>
      </div>
      <footer class="regress-footer">
        <AppButton :disabled="submitting" @click="emit('close')">取消</AppButton>
        <AppButton variant="primary" :disabled="!canSubmit" @click="submit">
          {{ submitting ? "创建中…" : "开始回归" }}
        </AppButton>
      </footer>
    </div>
  </div>
</template>

<style scoped>
.regress-overlay {
  position: fixed;
  inset: 0;
  z-index: 85;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding-top: 16vh;
}

.regress-modal {
  width: 460px;
  max-width: calc(100vw - 32px);
  display: flex;
  flex-direction: column;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
}

.regress-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
}

.regress-header h3 {
  margin: 0;
  font-size: 14px;
}

.regress-close {
  margin-left: auto;
  padding: 0 8px;
  border: none;
  background: none;
  font-size: 18px;
  line-height: 1.2;
  color: var(--text2);
}

.regress-close:hover {
  color: var(--text);
  background: var(--surface2);
}

.regress-body {
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.regress-baseline {
  font-family: var(--mono);
  font-size: 12px;
  overflow-wrap: anywhere;
}

.regress-hint {
  font-size: 11px;
}

.regress-workflow {
  font-size: 13px;
}

.regress-note {
  margin: 0;
  font-size: 12px;
  line-height: 1.5;
}

.regress-advanced {
  margin-top: 4px;
}

.regress-advanced summary {
  cursor: pointer;
  color: var(--text2);
  font-size: 12px;
}

.regress-fixtures {
  width: 100%;
  font-family: var(--mono);
  font-size: 11px;
}

.regress-error {
  margin: 0;
  color: var(--danger);
  font-size: 12px;
  overflow-wrap: anywhere;
}

.regress-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 10px 16px;
  border-top: 1px solid var(--border);
}
</style>
