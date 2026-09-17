<script setup lang="ts">
import { createWorkflow, editor, publish, removeWorkflow, save, selectWorkflow } from "../state/editor";
import { monitor, startRun } from "../state/monitor";

function onCreate(): void {
  const name = window.prompt("工作流名称");
  if (name?.trim()) void createWorkflow(name.trim());
}

function onDelete(id: string): void {
  if (window.confirm("确定删除该工作流？已有 run 记录的工作流会被服务端拒绝。")) {
    void removeWorkflow(id);
  }
}
</script>

<template>
  <div class="workflow-list">
    <div class="wf-header">
      <h3>工作流</h3>
      <button @click="onCreate">新建</button>
    </div>
    <div
      v-for="w in editor.workflows"
      :key="w.workflow_id"
      class="wf-item"
      :class="{ active: w.workflow_id === editor.workflowId }"
      @click="selectWorkflow(w.workflow_id)"
    >
      <span class="wf-name">{{ w.name }}</span>
      <span class="wf-meta">
        v{{ w.latest_version }}
        <em v-if="w.published_version" class="wf-pub">已发布 v{{ w.published_version }}</em>
      </span>
      <button class="wf-del" title="删除" @click.stop="onDelete(w.workflow_id)">×</button>
    </div>
    <p v-if="editor.workflows.length === 0" class="wf-empty">暂无工作流</p>

    <div v-if="editor.workflowId" class="wf-toolbar">
      <div class="wf-current">
        {{ editor.workflowName || editor.workflowId }}
        <span v-if="editor.version">v{{ editor.version }}</span>
        <span v-if="editor.dirty" class="wf-dirty">（未保存）</span>
      </div>
      <div class="wf-actions">
        <button @click="save()">保存</button>
        <button @click="publish()">发布</button>
        <button :disabled="monitor.starting" @click="startRun()">运行</button>
      </div>
    </div>
  </div>
</template>
