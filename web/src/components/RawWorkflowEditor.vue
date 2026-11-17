<script setup lang="ts">
import { computed, onUnmounted, ref, watch } from "vue";
import * as api from "../api/flow";
import { definitionToFlow, editor, flowToDefinition } from "../state/editor";
import { commit } from "../state/history";
import { errText } from "../rpc/client";
import { extractDefinition, parseRawDefinition, responseText } from "../state/raw-definition";
import CodeEditor from "./CodeEditor.vue";
import AppButton from "./AppButton.vue";

const emit = defineEmits<{ pending: [value: boolean] }>();
const baseline = ref(JSON.stringify(flowToDefinition(), null, 2));
const raw = ref(baseline.value);
const pending = computed(() => raw.value !== baseline.value);
watch(pending, (value) => emit("pending", value), { immediate: true });
const error = ref("");
const applying = ref(false);
const secret = ref("");
const question = ref("");
const messages = ref<{ role: "user" | "assistant"; text: string }[]>([]);
const busy = ref(false);
const runId = ref<string | null>(null);
const helperId = ref<string | null>(null);
let disposed = false;
let generation = 0;
onUnmounted(() => {
  disposed = true;
  generation++;
});
watch(
  () => JSON.stringify(flowToDefinition(), null, 2),
  (value) => {
    if (!pending.value) raw.value = value;
    baseline.value = value;
  },
);

async function apply(): Promise<boolean> {
  error.value = "";
  const source = raw.value;
  const workflowId = editor.workflowId;
  applying.value = true;
  try {
    const definition = parseRawDefinition(source);
    const result = await api.validateWorkflow(definition);
    if (disposed || workflowId !== editor.workflowId || source !== raw.value) return false;
    if (!result.valid) throw new Error("工作流校验未通过");
    const converted = definitionToFlow(definition);
    const { nodes: _nodes, edges: _edges, ...options } = definition;
    commit();
    editor.nodes = converted.nodes;
    editor.edges = converted.edges;
    editor.definitionOptions = options;
    editor.selectedNodeId = null;
    baseline.value = JSON.stringify(flowToDefinition(), null, 2);
    raw.value = baseline.value;
    return true;
  } catch (e) {
    error.value = errText(e);
    return false;
  } finally {
    applying.value = false;
  }
}
function useReply(text: string): void {
  try {
    raw.value = extractDefinition(text);
    error.value = "";
  } catch (e) {
    error.value = errText(e);
  }
}
async function send(): Promise<void> {
  if (busy.value || !question.value.trim() || !secret.value.trim()) return;
  const token = ++generation;
  const current = () => !disposed && token === generation;
  messages.value.push({ role: "user", text: question.value.trim() });
  question.value = "";
  busy.value = true;
  error.value = "";
  try {
    const prompt = `你是工作流 JSON 编辑助手。根据用户问题解释修改方案，需要修改时返回一个 json 代码块，包含完整工作流定义。只讨论和生成 JSON，不调用工具、不执行工作流内的任务。\n节点类型与参数规范：${JSON.stringify(editor.nodeTypes)}\n当前编辑区 JSON：\n${raw.value}\n对话：\n${JSON.stringify(messages.value)}`;
    const definition = {
      permissions: { version: 1 as const, mode: "bypass" as const, allow: [] },
      nodes: [
        { id: "start", type: "start", params: {} },
        {
          id: "assistant",
          type: "harness",
          params: {
            mode: "agent",
            api_key: secret.value.trim(),
            prompt: "${input.prompt}",
            timeout_ms: 300000,
          },
        },
        { id: "end", type: "end", params: {} },
      ],
      edges: [
        { from: "start", to: "assistant" },
        { from: "assistant", to: "end" },
      ],
    };
    if (!helperId.value)
      helperId.value = await api.createWorkflow(
        `JSON 编辑助手 ${editor.workflowName} ${crypto.randomUUID().slice(0, 8)}`,
      );
    if (!current()) return;
    const version = await api.updateWorkflow(helperId.value, definition);
    if (!current()) return;
    await api.publishWorkflow(helperId.value, version);
    if (!current()) return;
    const started = await api.startRun(helperId.value, { prompt }, version);
    if (!current()) {
      await api.runCancel(started.run_id);
      return;
    }
    runId.value = started.run_id;
    const deadline = Date.now() + 330000;
    while (current()) {
      const { run } = await api.getRun(started.run_id);
      if (!current()) return;
      if (run.status === "succeeded") {
        messages.value.push({ role: "assistant", text: responseText(run.output) });
        runId.value = null;
        return;
      }
      if (["failed", "cancelled", "canceled"].includes(run.status)) {
        runId.value = null;
        throw new Error(run.error || `助手运行${run.status}`);
      }
      if (Date.now() > deadline) throw new Error("助手响应超时，可停止运行后重试");
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  } catch (e) {
    if (current()) error.value = errText(e);
  } finally {
    if (current()) busy.value = false;
  }
}
async function cancel(): Promise<void> {
  try {
    if (runId.value) await api.runCancel(runId.value);
    generation++;
    busy.value = false;
    runId.value = null;
  } catch (e) {
    error.value = errText(e);
  }
}
defineExpose({ apply });
</script>

<template>
  <div class="raw-workflow">
    <section class="json-pane">
      <div class="raw-toolbar">
        <strong>工作流 JSON</strong><span>{{ pending ? "草稿未应用" : "已同步画布" }}</span
        ><AppButton :disabled="!pending || applying" @click="apply">{{
          applying ? "校验中…" : "校验并应用"
        }}</AppButton>
      </div>
      <CodeEditor v-model="raw" height="100%" />
      <p v-if="error" role="alert">{{ error }}</p>
    </section>
    <section class="chat-pane" aria-label="Harness Chat">
      <h3>Harness Chat</h3>
      <p>讨论当前 JSON 的修改方案。回复放入编辑区后，校验并应用，再保存工作流。</p>
      <label
        >模型密钥名称
        <input
          v-model="secret"
          list="harness-secrets"
          :disabled="busy"
          placeholder="选择或输入密钥名称"
      /></label>
      <datalist id="harness-secrets">
        <option v-for="name in editor.secrets" :key="name" :value="name" />
      </datalist>
      <p class="help">
        使用设置中的 harness.agent 模型。发送将创建独立的助手工作流，以 bypass
        模式运行并保留记录，关闭面板不会停止已启动的运行。
      </p>
      <div class="messages" aria-live="polite">
        <article v-for="(message, i) in messages" :key="i">
          <strong>{{ message.role === "user" ? "你" : "助手" }}</strong>
          <pre>{{ message.text }}</pre>
          <AppButton v-if="message.role === 'assistant'" @click="useReply(message.text)"
            >将 JSON 放入编辑区</AppButton
          >
        </article>
      </div>
      <RouterLink v-if="runId" :to="`/runs/${runId}`">查看助手运行</RouterLink>
      <textarea
        v-model="question"
        rows="3"
        placeholder="例如：增加一个失败通知节点，并解释连线如何修改"
        :disabled="busy"
      />
      <AppButton :disabled="busy || !!runId || !secret.trim() || !question.trim()" @click="send">{{
        busy ? "助手思考中…" : "发送"
      }}</AppButton>
      <AppButton v-if="busy || runId" @click="cancel">停止</AppButton>
    </section>
  </div>
</template>
<style scoped>
.raw-workflow {
  display: grid;
  grid-template-columns: minmax(0, 3fr) minmax(280px, 2fr);
  height: 100%;
  min-height: 0;
  gap: 12px;
  padding: 12px;
}
.json-pane {
  display: flex;
  flex-direction: column;
  min-height: 0;
  gap: 8px;
}
.json-pane :deep(.code-editor) {
  flex: 1;
  min-height: 200px;
}
.raw-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
}
.chat-pane {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 0;
  overflow: auto;
  border-left: 1px solid var(--border);
  padding-left: 12px;
}
.chat-pane h3,
.chat-pane p {
  margin: 0;
}
.chat-pane .help {
  font-size: 12px;
  color: var(--muted);
}
.messages {
  flex: 1;
  overflow: auto;
  min-height: 80px;
}
article {
  padding: 10px 0;
  border-bottom: 1px solid var(--border);
}
pre {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  font: inherit;
}
input,
textarea {
  width: 100%;
  box-sizing: border-box;
}
[role="alert"] {
  color: var(--danger);
}
@media (max-width: 850px) {
  .raw-workflow {
    grid-template-columns: 1fr;
    overflow: auto;
  }
  .json-pane {
    min-height: 400px;
  }
  .chat-pane {
    min-height: 350px;
    border-left: 0;
  }
}
</style>
