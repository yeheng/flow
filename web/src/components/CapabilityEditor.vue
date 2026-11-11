<script setup lang="ts">
import { computed, ref, useId } from "vue";
import { editor } from "../state/editor";
import { sha256Hex } from "../state/invocation";
import {
  CAPABILITY_ACTIONS,
  NET_METHODS,
  expiresAtFromInput,
  expiresAtToInput,
  switchCapabilityAction,
  validateCapability,
} from "../state/capabilities";
import { capabilityActionLabel } from "../state/labels";
import type { Capability } from "../types";
import AppButton from "./AppButton.vue";

/**
 * 能力条目结构化编辑器（v-model:capability + 删除按钮）。按 action 闭集切换
 * 字段编辑面；编辑即时 emit 整体补丁，写回与撤销历史由父层（编辑器 state）负责。
 * 本地校验提示（validateCapability）随编辑实时展示，服务端校验仍是最终裁决。
 */
const props = defineProps<{ capability: Capability }>();
const emit = defineEmits<{ "update:capability": [cap: Capability]; remove: [] }>();

const action = computed(() => String(props.capability.action));

/** 浅合并补丁：整体换新对象 emit（父层无需关心哪个字段变了） */
function patch(p: Record<string, unknown>): void {
  emit("update:capability", { ...props.capability, ...p });
}

function onActionChange(ev: Event): void {
  // 切换动作：公共字段保留，闭集字段换新底稿（旧动作的残留字段不带入）
  emit("update:capability", switchCapabilityAction(props.capability, (ev.target as HTMLSelectElement).value));
}

function onId(ev: Event): void {
  patch({ id: (ev.target as HTMLInputElement).value });
}

function onApproval(ev: Event): void {
  patch({ approval: (ev.target as HTMLSelectElement).value === "auto" ? "auto" : "ask" });
}

/** expires_at：datetime-local（本地无秒）↔ ISO（定义 JSON 落库口径） */
const expiresInput = computed(() => expiresAtToInput(props.capability.expires_at as string | null | undefined));

function onExpires(ev: Event): void {
  patch({ expires_at: expiresAtFromInput((ev.target as HTMLInputElement).value) });
}

/** 字符串字段取值（缺失/类型不符 → ""） */
function strOf(key: string): string {
  const v = (props.capability as Record<string, unknown>)[key];
  return typeof v === "string" ? v : "";
}

function onStr(key: string, ev: Event): void {
  patch({ [key]: (ev.target as HTMLInputElement).value });
}

/** 字符串数组字段（argv/names/providers/models/recipients 共用一行一值的编辑模式） */
function strList(key: string): string[] {
  const v = (props.capability as Record<string, unknown>)[key];
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
}

function setListItem(key: string, index: number, ev: Event): void {
  const list = [...strList(key)];
  list[index] = (ev.target as HTMLInputElement).value;
  patch({ [key]: list });
}

function addListItem(key: string): void {
  patch({ [key]: [...strList(key), ""] });
}

function removeListItem(key: string, index: number): void {
  patch({ [key]: strList(key).filter((_, i) => i !== index) });
}

/** net.methods 多选：按 NET_METHODS 闭集顺序维护 */
function toggleMethod(method: string, ev: Event): void {
  const checked = (ev.target as HTMLInputElement).checked;
  const current = new Set(strList("methods"));
  if (checked) current.add(method);
  else current.delete(method);
  patch({ methods: NET_METHODS.filter((m) => current.has(m)) });
}

/** file_*：path 与 subtree 二选一。当前选择是本地态（两侧都空时也保持用户选中的那侧） */
const fileTarget = ref<"path" | "subtree">(strOf("subtree") !== "" ? "subtree" : "path");

function setFileTarget(target: "path" | "subtree"): void {
  if (target === fileTarget.value) return;
  fileTarget.value = target;
  patch({ path: "", subtree: "" });
}

function onFileField(key: "path" | "subtree", ev: Event): void {
  const value = (ev.target as HTMLInputElement).value;
  patch(key === "path" ? { path: value, subtree: "" } : { path: "", subtree: value });
}

// ---- shell：脚本正文只在本地，落库的是 SHA-256 摘要（WebCrypto） ----

const scriptText = ref("");
const digesting = ref(false);

async function computeDigest(): Promise<void> {
  if (digesting.value || scriptText.value === "") return;
  digesting.value = true;
  try {
    patch({ script_digest: await sha256Hex(scriptText.value) });
  } finally {
    digesting.value = false;
  }
}

// ---- 密钥名称选择器（datalist：候选 + 允许手输，同 SchemaField 的 x-secret 模式） ----
const secretListId = useId();

/** 本地校验提示：保存前实时可见（服务端校验权威） */
const errors = computed(() => validateCapability(props.capability));
</script>

<template>
  <div class="cap-editor">
    <div class="cap-row cap-head">
      <label class="cap-field">
        动作
        <select :value="action" aria-label="能力动作" @change="onActionChange">
          <option v-for="a in CAPABILITY_ACTIONS" :key="a" :value="a">
            {{ capabilityActionLabel(a) }}
          </option>
        </select>
      </label>
      <label class="cap-field cap-id">
        id
        <input
          type="text"
          class="mono"
          spellcheck="false"
          :value="capability.id"
          title="能力 id（同层唯一），默认自动生成"
          @input="onId"
        />
      </label>
      <label class="cap-field">
        审批
        <select :value="capability.approval" aria-label="审批方式" @change="onApproval">
          <option value="ask">需审批</option>
          <option value="auto">免审批</option>
        </select>
      </label>
      <label class="cap-field">
        过期（可选）
        <input type="datetime-local" :value="expiresInput" @input="onExpires" />
      </label>
      <AppButton variant="link-danger" title="删除此能力" @click="emit('remove')">删除</AppButton>
    </div>

    <!-- net：方法多选 + URL 前缀（禁止 userinfo/query，见校验提示） -->
    <template v-if="action === 'net'">
      <div class="cap-row">
        <div class="cap-methods" role="group" aria-label="HTTP 方法">
          <label v-for="m in NET_METHODS" :key="m" class="cap-check">
            <input
              type="checkbox"
              :checked="strList('methods').includes(m)"
              @change="toggleMethod(m, $event)"
            />
            {{ m }}
          </label>
        </div>
      </div>
      <div class="cap-row">
        <label class="cap-field grow">
          URL 前缀
          <input
            type="text"
            class="mono"
            spellcheck="false"
            placeholder="https://api.example.com/"
            :value="strOf('url_prefix')"
            @input="onStr('url_prefix', $event)"
          />
        </label>
      </div>
    </template>

    <!-- exec：解释器绝对路径 + argv 逐项（一行一项，不拼 shell 字符串） -->
    <template v-else-if="action === 'exec'">
      <div class="cap-row">
        <label class="cap-field grow">
          可执行文件（绝对路径）
          <input
            type="text"
            class="mono"
            spellcheck="false"
            placeholder="/usr/bin/curl"
            :value="strOf('executable')"
            @input="onStr('executable', $event)"
          />
        </label>
      </div>
      <div class="cap-row cap-list">
        <span class="cap-list-label">参数（argv，一行一项）</span>
        <div v-for="(_, i) in strList('argv_exact')" :key="i" class="cap-list-row">
          <input
            type="text"
            class="mono"
            spellcheck="false"
            :value="strList('argv_exact')[i]"
            @input="setListItem('argv_exact', i, $event)"
          />
          <button type="button" class="cap-list-remove" title="删除此项" @click="removeListItem('argv_exact', i)">
            ×
          </button>
        </div>
        <button type="button" class="cap-list-add" @click="addListItem('argv_exact')">＋ 添加参数</button>
      </div>
    </template>

    <!-- shell：解释器（可选）+ 脚本正文（本地）→「计算摘要」填 script_digest -->
    <template v-else-if="action === 'shell'">
      <div class="cap-row">
        <label class="cap-field grow">
          解释器（可选，绝对路径）
          <input
            type="text"
            class="mono"
            spellcheck="false"
            placeholder="/bin/bash"
            :value="strOf('executable')"
            @input="onStr('executable', $event)"
          />
        </label>
      </div>
      <div class="cap-row cap-list">
        <span class="cap-list-label">脚本正文（只保存摘要）</span>
        <textarea
          v-model="scriptText"
          class="mono"
          rows="4"
          spellcheck="false"
          placeholder="粘贴脚本正文，点「计算摘要」生成 SHA-256"
        ></textarea>
        <div class="cap-row">
          <AppButton :disabled="digesting || scriptText === ''" @click="computeDigest()">
            {{ digesting ? "计算中…" : "计算摘要" }}
          </AppButton>
          <span class="muted cap-digest-label">script_digest</span>
          <code class="mono cap-digest">{{ strOf("script_digest") || "（未计算）" }}</code>
        </div>
      </div>
    </template>

    <!-- secret：密钥名多值（datalist 候选来自 secrets.list，值不出服务端） -->
    <template v-else-if="action === 'secret'">
      <div class="cap-row cap-list">
        <span class="cap-list-label">密钥名（一行一个）</span>
        <div v-for="(_, i) in strList('names')" :key="i" class="cap-list-row">
          <input
            type="text"
            :list="secretListId"
            spellcheck="false"
            placeholder="密钥名称"
            :value="strList('names')[i]"
            @input="setListItem('names', i, $event)"
          />
          <button type="button" class="cap-list-remove" title="删除此项" @click="removeListItem('names', i)">
            ×
          </button>
        </div>
        <datalist :id="secretListId">
          <option v-for="s in editor.secrets" :key="s" :value="s" />
        </datalist>
        <button type="button" class="cap-list-add" @click="addListItem('names')">＋ 添加密钥名</button>
      </div>
    </template>

    <!-- llm_call：providers / models 多值 -->
    <template v-else-if="action === 'llm_call'">
      <div class="cap-row cap-list">
        <span class="cap-list-label">provider（一行一个）</span>
        <div v-for="(_, i) in strList('providers')" :key="i" class="cap-list-row">
          <input
            type="text"
            spellcheck="false"
            :value="strList('providers')[i]"
            @input="setListItem('providers', i, $event)"
          />
          <button type="button" class="cap-list-remove" title="删除此项" @click="removeListItem('providers', i)">
            ×
          </button>
        </div>
        <button type="button" class="cap-list-add" @click="addListItem('providers')">＋ 添加</button>
      </div>
      <div class="cap-row cap-list">
        <span class="cap-list-label">model（一行一个）</span>
        <div v-for="(_, i) in strList('models')" :key="i" class="cap-list-row">
          <input
            type="text"
            class="mono"
            spellcheck="false"
            :value="strList('models')[i]"
            @input="setListItem('models', i, $event)"
          />
          <button type="button" class="cap-list-remove" title="删除此项" @click="removeListItem('models', i)">
            ×
          </button>
        </div>
        <button type="button" class="cap-list-add" @click="addListItem('models')">＋ 添加</button>
      </div>
    </template>

    <!-- send_email：收件人多值 -->
    <template v-else-if="action === 'send_email'">
      <div class="cap-row cap-list">
        <span class="cap-list-label">收件人（一行一个）</span>
        <div v-for="(_, i) in strList('recipients')" :key="i" class="cap-list-row">
          <input
            type="text"
            spellcheck="false"
            placeholder="user@example.com"
            :value="strList('recipients')[i]"
            @input="setListItem('recipients', i, $event)"
          />
          <button
            type="button"
            class="cap-list-remove"
            title="删除此项"
            @click="removeListItem('recipients', i)"
          >
            ×
          </button>
        </div>
        <button type="button" class="cap-list-add" @click="addListItem('recipients')">＋ 添加</button>
      </div>
    </template>

    <!-- file_*：path 或 subtree 二选一（相对路径） -->
    <template v-else-if="action === 'file_read' || action === 'file_write' || action === 'file_delete'">
      <div class="cap-row">
        <div class="cap-methods" role="group" aria-label="文件范围">
          <label class="cap-check">
            <input type="radio" :checked="fileTarget === 'path'" @change="setFileTarget('path')" />
            指定路径
          </label>
          <label class="cap-check">
            <input type="radio" :checked="fileTarget === 'subtree'" @change="setFileTarget('subtree')" />
            子树
          </label>
        </div>
        <label v-if="fileTarget === 'path'" class="cap-field grow">
          路径（相对）
          <input
            type="text"
            class="mono"
            spellcheck="false"
            placeholder="out/report.md"
            :value="strOf('path')"
            @input="onFileField('path', $event)"
          />
        </label>
        <label v-else class="cap-field grow">
          子树（相对）
          <input
            type="text"
            class="mono"
            spellcheck="false"
            placeholder="data/"
            :value="strOf('subtree')"
            @input="onFileField('subtree', $event)"
          />
        </label>
      </div>
    </template>

    <ul v-if="errors.length > 0" class="cap-errors" role="alert">
      <li v-for="e in errors" :key="e">{{ e }}</li>
    </ul>
  </div>
</template>

<style scoped>
.cap-editor {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  background: var(--surface);
}

.cap-row {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}

.cap-head {
  border-bottom: 1px dashed var(--border);
  padding-bottom: 6px;
}

.cap-field {
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: 11px;
  color: var(--text2);
}

.cap-field.grow {
  flex: 1;
  min-width: 220px;
}

.cap-id {
  min-width: 160px;
}

.cap-field input,
.cap-field select,
.cap-list-row input {
  font-size: 12px;
}

.cap-methods {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  align-items: center;
}

.cap-check {
  display: flex;
  gap: 3px;
  align-items: center;
  font-size: 11px;
  color: var(--text2);
  cursor: pointer;
  user-select: none;
}

.cap-list {
  flex-direction: column;
  align-items: stretch;
  gap: 4px;
}

.cap-list-label {
  font-size: 11px;
  color: var(--text2);
}

.cap-list-row {
  display: flex;
  gap: 6px;
  align-items: center;
}

.cap-list-row input {
  flex: 1;
  min-width: 0;
}

.cap-list-remove {
  flex: none;
  padding: 2px 8px;
  font-size: 13px;
  line-height: 1.4;
}

.cap-list-add {
  align-self: flex-start;
  padding: 2px 10px;
  font-size: 12px;
}

.cap-digest-label {
  font-size: 11px;
}

.cap-digest {
  font-size: 11px;
  word-break: break-all;
}

.cap-errors {
  margin: 0;
  padding-left: 16px;
  color: var(--danger);
  font-size: 11px;
  line-height: 1.6;
}
</style>
