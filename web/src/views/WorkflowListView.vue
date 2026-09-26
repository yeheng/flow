<script setup lang="ts">
import { onMounted } from "vue";
import { useRouter } from "vue-router";
import { createWorkflow, editor, refreshWorkflows, removeWorkflow } from "../state/editor";
import { confirmDialog, promptDialog } from "../state/modal";

const router = useRouter();

onMounted(() => {
  void refreshWorkflows();
});

async function onCreate(): Promise<void> {
  const name = await promptDialog("工作流名称");
  if (!name?.trim()) return;
  const id = await createWorkflow(name.trim());
  if (id) void router.push(`/workflows/${id}`);
}

async function onDelete(id: string): Promise<void> {
  if (await confirmDialog("确定删除该工作流？已有 run 记录的工作流会被服务端拒绝。")) {
    await removeWorkflow(id);
  }
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>工作流</h2>
      <button class="primary" @click="onCreate">新建</button>
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
          <td>
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
            <RouterLink class="link" :to="`/workflows/${w.workflow_id}/runs`">运行记录</RouterLink>
            <button class="link danger" @click="onDelete(w.workflow_id)">删除</button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="editor.workflows.length === 0" class="wf-empty">暂无工作流，点击「新建」开始</p>
  </div>
</template>
