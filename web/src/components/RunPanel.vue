<script setup lang="ts">
import { monitor } from "../state/monitor";
import RunMonitor from "./RunMonitor.vue";
</script>

<template>
  <div class="run-panel">
    <h3>运行</h3>
    <div class="field">
      <label>输入 JSON（可选）</label>
      <textarea
        v-model="monitor.inputText"
        class="code"
        rows="2"
        spellcheck="false"
        placeholder='{"key": "value"}'
      ></textarea>
    </div>

    <RunMonitor v-if="monitor.runId" />
    <p v-else class="run-hint">在顶部工具栏点击「运行」启动最新已发布版本</p>
  </div>
</template>
