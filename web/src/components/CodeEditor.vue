<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from "vue";
import { monaco } from "../monaco";

/**
 * Monaco 包装：v-model 字符串双向绑定。json 语言自带校验标红与格式化；
 * 外部（undo/redo、版本加载）改写 modelValue 时整段回填。
 */
const props = withDefaults(
  defineProps<{ modelValue: string; language?: string; height?: string }>(),
  { language: "json", height: "200px" },
);
const emit = defineEmits<{ "update:modelValue": [value: string] }>();

const host = ref<HTMLElement>();
let editor: monaco.editor.IStandaloneCodeEditor | undefined;

onMounted(() => {
  editor = monaco.editor.create(host.value!, {
    value: props.modelValue,
    language: props.language,
    theme: "flow-dark",
    automaticLayout: true,
    minimap: { enabled: false },
    fontSize: 12,
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
    lineNumbers: "on",
    scrollBeyondLastLine: false,
    wordWrap: "on",
    tabSize: 2,
    padding: { top: 8, bottom: 8 },
    scrollbar: { verticalScrollbarSize: 8, horizontalScrollbarSize: 8 },
  });
  editor.onDidChangeModelContent(() => {
    const v = editor!.getValue();
    if (v !== props.modelValue) emit("update:modelValue", v);
  });
});

watch(
  () => props.modelValue,
  (v) => {
    if (editor && v !== editor.getValue()) editor.setValue(v ?? "");
  },
);

onBeforeUnmount(() => {
  editor?.dispose();
});
</script>

<template>
  <div ref="host" class="code-editor" :style="{ height }" />
</template>

<style scoped>
.code-editor {
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
  transition: border-color 0.15s;
}

.code-editor:focus-within {
  border-color: var(--accent);
}
</style>
