<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import * as api from "../api/flow";
import { createWorkflow, editor, refreshWorkflows, removeWorkflow } from "../state/editor";
import { importWorkflow, parseImportDocument } from "../state/workflow-io";
import { errText } from "../rpc/client";
import { confirmDialog, promptDialog } from "../state/modal";
import { toast } from "../state/toast";
import AppButton from "../components/AppButton.vue";

const router = useRouter();

/** 首次拉取完成前不展示空态，避免「暂无…」闪烁 */
const loaded = ref(false);
/** 拉取失败：展示错误条与重试，而不是把空列表当成「暂无工作流」 */
const error = ref("");
const creating = ref(false);
/** 正在删除的工作流 id：只禁用对应行的删除按钮 */
const deletingId = ref<string | null>(null);

async function refresh(): Promise<void> {
  try {
    // 直接走 api：refreshWorkflows 内部吞掉错误并 toast，本页需要失败态来区分「空」和「拉取失败」
    editor.workflows = await api.listWorkflows();
    error.value = "";
  } catch (e) {
    error.value = errText(e);
  } finally {
    loaded.value = true;
  }
}

onMounted(() => {
  void refresh();
});

async function onCreate(): Promise<void> {
  if (creating.value) return;
  const name = await promptDialog("工作流名称");
  if (!name?.trim()) return;
  creating.value = true;
  try {
    const id = await createWorkflow(name.trim());
    if (id) void router.push(`/workflows/${id}`);
  } finally {
    creating.value = false;
  }
}

async function onDelete(id: string): Promise<void> {
  if (deletingId.value) return;
  if (
    await confirmDialog("确定删除该工作流？删除后将从列表隐藏并移除触发器，已有运行记录会保留。", {
      danger: true,
    })
  ) {
    deletingId.value = id;
    try {
      await removeWorkflow(id);
    } finally {
      deletingId.value = null;
    }
  }
}

// ---- 导入（flow-cli workflow import 的 web 形态：{name, definition} 信封或裸
// definition，按 name upsert，默认发布） ----
const fileInput = ref<HTMLInputElement | null>(null);
const importing = ref(false);

function onPickFile(): void {
  fileInput.value?.click();
}

async function onFileChange(event: Event): Promise<void> {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  // 允许重复选择同一个文件：清空 value，否则 change 不再触发
  input.value = "";
  if (!file || importing.value) return;
  let parsed;
  try {
    parsed = parseImportDocument(await file.text(), file.name);
  } catch (e) {
    toast.error(e instanceof Error ? e.message : String(e));
    return;
  }
  // 名字可改（预填信封 name 或文件名），确认后导入并发布
  const name = await promptDialog(
    `导入「${file.name}」→ 工作流名称（同名将追加新版本，导入后自动发布）`,
    parsed.name,
  );
  if (name === null || !name.trim()) return;
  importing.value = true;
  try {
    const result = await importWorkflow(name.trim(), parsed.definition);
    if (!result) return;
    toast.success(
      `已导入 ${result.name} v${result.version}（${result.created ? "新建" : "追加版本"}，已发布）`,
    );
    await refreshWorkflows();
    void router.push(`/workflows/${result.workflow_id}`);
  } catch (e) {
    toast.error(errText(e));
  } finally {
    importing.value = false;
  }
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>工作流</h2>
      <span class="header-actions">
        <AppButton :disabled="importing" @click="onPickFile">
          {{ importing ? "导入中…" : "导入" }}
        </AppButton>
        <AppButton variant="primary" :disabled="creating" @click="onCreate">新建</AppButton>
      </span>
    </div>
    <!-- 文件选择器藏在按钮后：accept 限定 json；value 清空见 onFileChange -->
    <input
      ref="fileInput"
      type="file"
      accept=".json,application/json"
      class="import-input"
      @change="onFileChange"
    />
    <div v-if="error" role="alert" class="error-banner">
      <span>加载工作流失败：{{ error }}</span>
      <AppButton @click="refresh()">重试</AppButton>
    </div>
    <table class="data-table">
      <thead>
        <tr>
          <th>名称</th>
          <th>最新版本</th>
          <th>已发布版本</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="w in editor.workflows" :key="w.workflow_id">
          <td class="wf-cell">
            <RouterLink class="link" :to="`/workflows/${w.workflow_id}`">{{ w.name }}</RouterLink>
          </td>
          <td>v{{ w.latest_version }}</td>
          <td>
            <span v-if="w.published_version" class="published">v{{ w.published_version }}</span>
            <span v-else class="muted">—</span>
          </td>
          <td class="actions">
            <RouterLink class="link" :to="`/workflows/${w.workflow_id}`">打开</RouterLink>
            <RouterLink class="link" :to="`/workflows/${w.workflow_id}/versions`">版本</RouterLink>
            <RouterLink class="link" :to="`/workflows/${w.workflow_id}/triggers`"
              >触发器</RouterLink
            >
            <RouterLink class="link" :to="`/workflows/${w.workflow_id}/runs`">运行记录</RouterLink>
            <AppButton
              variant="link-danger"
              :disabled="deletingId === w.workflow_id"
              @click="onDelete(w.workflow_id)"
            >
              删除
            </AppButton>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="!loaded" class="wf-empty">加载中…</p>
    <p v-else-if="!error && editor.workflows.length === 0" class="wf-empty">
      暂无工作流，点击「新建」开始
    </p>
  </div>
</template>

<style scoped>
.header-actions {
  display: inline-flex;
  gap: 8px;
}

/* 文件选择器只作按钮背后的通道，不占布局 */
.import-input {
  display: none;
}

/* 长名称可能没有空白/为中文，允许折行避免横向撑破表格 */
.wf-cell {
  word-break: break-all;
}
</style>
