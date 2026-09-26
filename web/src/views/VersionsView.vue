<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { confirmDiscardIfDirty, editor, refreshWorkflows, selectWorkflow } from "../state/editor";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import { diffDefinitions, isEmptyDiff, type DefinitionDiff } from "../state/diff";
import type { Definition, VersionMeta } from "../types";

const props = defineProps<{ workflowId: string }>();
const router = useRouter();

const versions = ref<VersionMeta[]>([]);
const compareA = ref<number | null>(null);
const compareB = ref<number | null>(null);
const publishing = ref(false);
/** version → definition 缓存：diff 与发布共用，避免重复拉取 */
const defCache = new Map<number, Definition>();
const diff = ref<DefinitionDiff | null>(null);
let diffGen = 0;

const workflowName = computed(
  () => editor.workflows.find((w) => w.workflow_id === props.workflowId)?.name ?? "",
);

/** 与后端 latest_published 的 MAX(version) 语义一致 */
const publishedVersion = computed(() => {
  const published = versions.value.filter((v) => v.status === "published");
  return published.length > 0 ? Math.max(...published.map((v) => v.version)) : null;
});

async function load(): Promise<void> {
  try {
    versions.value = await api.listVersions(props.workflowId);
  } catch (e) {
    toast.error(errText(e));
    return;
  }
  // 默认对比最新两版：A 基线（旧）= 次新，B 对比（新）= 最新
  compareB.value = versions.value[0]?.version ?? null;
  compareA.value = versions.value[1]?.version ?? compareB.value;
}

async function definitionOf(version: number): Promise<Definition> {
  const cached = defCache.get(version);
  if (cached) return cached;
  const w = await api.getWorkflow(props.workflowId, version);
  defCache.set(version, w.definition);
  return w.definition;
}

async function recomputeDiff(): Promise<void> {
  const gen = ++diffGen;
  const a = compareA.value;
  const b = compareB.value;
  if (a === null || b === null) {
    diff.value = null;
    return;
  }
  try {
    const [defA, defB] = await Promise.all([definitionOf(a), definitionOf(b)]);
    if (gen !== diffGen) return;
    diff.value = diffDefinitions(defA, defB);
  } catch (e) {
    if (gen === diffGen) toast.error(`加载版本定义失败：${errText(e)}`);
  }
}

watch([compareA, compareB], () => void recomputeDiff());

onMounted(async () => {
  if (editor.workflows.length === 0) void refreshWorkflows();
  await load();
});

/** 把该版本定义载入编辑器作为编辑基础；保存时走正常 workflow.update 生成新版本 */
async function loadToEditor(version: number): Promise<void> {
  if (!(await confirmDiscardIfDirty())) return;
  await selectWorkflow(props.workflowId, version);
  if (editor.workflowId === props.workflowId) {
    void router.push(`/workflows/${props.workflowId}`);
  }
}

/**
 * 回滚语义（已核实后端）：publish 只给目标版本行置 published，
 * latest_published 恒取 MAX(version)，指针无法回拨。
 * 因此「发布旧版」= 复制旧版定义为新草稿 → 发布新版本。
 */
async function publishFromVersion(version: number): Promise<void> {
  if (
    !(await confirmDialog(
      `将以 v${version} 的内容生成一个新版本并发布（不是把已发布指针拨回 v${version}）。继续？`,
    ))
  ) {
    return;
  }
  publishing.value = true;
  try {
    const def = await definitionOf(version);
    const newVersion = await api.updateWorkflow(props.workflowId, def);
    await api.publishWorkflow(props.workflowId, newVersion);
    toast.success(`已发布 v${newVersion}（内容同 v${version}）`);
    defCache.clear();
    await load();
    void refreshWorkflows();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    publishing.value = false;
  }
}

function fmtTime(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function fmtVal(v: unknown): string {
  if (v === undefined) return "（空）";
  const s = JSON.stringify(v);
  return s.length > 80 ? s.slice(0, 80) + "…" : s;
}

function fmtEdge(e: { from: string; to: string; port?: string }): string {
  return `${e.from} → ${e.to}${e.port ? `（${e.port}）` : ""}`;
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>
        版本历史
        <span class="muted">{{ workflowName || workflowId }}</span>
      </h2>
      <RouterLink class="link" :to="`/workflows/${workflowId}`">打开编辑器 →</RouterLink>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>版本</th>
          <th>状态</th>
          <th>校验和</th>
          <th>创建时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="v in versions"
          :key="v.version"
          :class="{ 'row-published': v.version === publishedVersion }"
        >
          <td class="mono">
            v{{ v.version }}
            <span v-if="v.version === publishedVersion" class="published">（当前发布）</span>
          </td>
          <td>
            <span class="badge" :class="v.status === 'published' ? 'run-succeeded' : ''">
              {{ v.status === "published" ? "已发布" : "草稿" }}
            </span>
          </td>
          <td class="mono muted">{{ v.checksum.slice(0, 12) }}</td>
          <td>{{ fmtTime(v.created_at) }}</td>
          <td class="actions">
            <button class="link" @click="loadToEditor(v.version)">加载到编辑器</button>
            <button class="link" :disabled="publishing" @click="publishFromVersion(v.version)">
              以此版本发布
            </button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="versions.length === 0" class="wf-empty">该工作流还没有保存过版本</p>

    <div v-if="versions.length > 0" class="diff-section">
      <h3>版本对比</h3>
      <div class="diff-selectors">
        <label>
          基线（旧）
          <select v-model.number="compareA">
            <option v-for="v in versions" :key="v.version" :value="v.version">
              v{{ v.version }}
            </option>
          </select>
        </label>
        <span class="muted">→</span>
        <label>
          对比（新）
          <select v-model.number="compareB">
            <option v-for="v in versions" :key="v.version" :value="v.version">
              v{{ v.version }}
            </option>
          </select>
        </label>
      </div>

      <template v-if="diff">
        <p v-if="isEmptyDiff(diff)" class="wf-empty">两个版本内容一致（位置变化不算变更）</p>
        <template v-else>
          <div v-if="diff.nodesAdded.length" class="diff-group">
            <h4 class="diff-added">新增节点（{{ diff.nodesAdded.length }}）</h4>
            <div v-for="id in diff.nodesAdded" :key="id" class="diff-item mono">+ {{ id }}</div>
          </div>
          <div v-if="diff.nodesRemoved.length" class="diff-group">
            <h4 class="diff-removed">删除节点（{{ diff.nodesRemoved.length }}）</h4>
            <div v-for="id in diff.nodesRemoved" :key="id" class="diff-item mono">− {{ id }}</div>
          </div>
          <div v-if="diff.nodesChanged.length" class="diff-group">
            <h4>变更节点（{{ diff.nodesChanged.length }}）</h4>
            <div v-for="n in diff.nodesChanged" :key="n.id" class="diff-node">
              <div class="mono">{{ n.id }}</div>
              <div v-for="c in n.changes" :key="c.field" class="diff-item">
                <span class="muted">{{ c.field }}：</span>
                <span class="diff-removed mono">{{ fmtVal(c.from) }}</span>
                →
                <span class="diff-added mono">{{ fmtVal(c.to) }}</span>
              </div>
            </div>
          </div>
          <div v-if="diff.edgesAdded.length" class="diff-group">
            <h4 class="diff-added">新增边（{{ diff.edgesAdded.length }}）</h4>
            <div
              v-for="e in diff.edgesAdded"
              :key="`${e.from}${e.to}${e.port}`"
              class="diff-item mono"
            >
              + {{ fmtEdge(e) }}
            </div>
          </div>
          <div v-if="diff.edgesRemoved.length" class="diff-group">
            <h4 class="diff-removed">删除边（{{ diff.edgesRemoved.length }}）</h4>
            <div
              v-for="e in diff.edgesRemoved"
              :key="`${e.from}${e.to}${e.port}`"
              class="diff-item mono"
            >
              − {{ fmtEdge(e) }}
            </div>
          </div>
        </template>
      </template>
    </div>
  </div>
</template>

<style scoped>
.row-published td {
  background: rgba(63, 185, 80, 0.06);
}

.diff-section {
  margin-top: 20px;
}

.diff-selectors {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
}

.diff-selectors label {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--text2);
  font-size: 12px;
}

.diff-selectors select {
  width: auto;
}

.diff-group {
  margin-bottom: 14px;
}

.diff-group h4 {
  margin: 0 0 6px;
  font-size: 12px;
  font-weight: 600;
}

.diff-added {
  color: var(--ok);
}

.diff-removed {
  color: var(--danger);
}

.diff-node {
  margin-bottom: 8px;
  padding: 6px 8px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 6px;
}

.diff-item {
  padding: 2px 0;
  font-size: 12px;
  word-break: break-all;
}
</style>
