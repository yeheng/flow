<script setup lang="ts">
import { computed, onUnmounted, ref } from "vue";
import * as api from "../api/flow";
import { RpcError } from "../rpc/client";
import { approvalBadgeClass, approvalStatusLabel, capabilityActionLabel } from "../state/labels";
import {
  actorLabel,
  approvalSpecSummary,
  isTruncatedPreview,
  remainingText,
  specString,
  specStringList,
} from "../state/approvals";
import { toast } from "../state/toast";
import type { ApprovalItem, ApprovalSpec } from "../types";
import AppButton from "./AppButton.vue";

/**
 * 审批卡片：approvals.list 单条的逐字段渲染 + 人工决定（run.approve / run.deny）。
 * specs 里的 URL/命令/收件人等都是不可信文本，全部纯文本插值（禁止 v-html）；
 * 裁决提交后 emit("resolved")，由容器刷新收件箱与时间线。
 */
const props = defineProps<{ item: ApprovalItem }>();
const emit = defineEmits<{ resolved: [] }>();

// 本地时钟每秒走格：倒计时只是估算，文案标注「以服务端为准」
const now = ref(Date.now());
const ticker = setInterval(() => {
  now.value = Date.now();
}, 1000);
onUnmounted(() => clearInterval(ticker));

const isPending = computed(() => props.item.status === "pending");

const deadlineText = computed(() => {
  const left = remainingText(now.value, props.item.deadline);
  const d = new Date(props.item.deadline);
  const at = Number.isNaN(d.getTime()) ? String(props.item.deadline) : d.toLocaleString();
  // 剩余时长用本地时钟估算；截据时刻以服务端时间为准
  return left === null ? `已到截止（${at}，以服务端为准）` : `剩余 ${left} · ${at} 截止（以服务端为准）`;
});

const decisionText = computed(() => {
  const d = props.item.decision;
  if (!d) return "";
  const who = actorLabel(d.by);
  const reason = d.reason ? `：${d.reason}` : "";
  return `${d.decision === "allow" ? "已批准" : "已拒绝"} · ${who}${reason}`;
});

/** spec 的辅助字段（摘要/环境/正文指纹）：按动作闭集挑已知字段，小字等宽展示 */
function specMeta(spec: ApprovalSpec): { key: string; value: string }[] {
  const keys: Record<string, string[]> = {
    exec: ["cwd", "env_digest", "stdin_digest"],
    shell: ["cwd", "env_digest", "stdin_digest"],
    net: ["headers_digest", "body_digest"],
    llm_call: ["payload_digest"],
    send_email: ["payload_digest"],
    file_read: ["content_digest"],
    file_write: ["content_digest"],
    file_delete: ["content_digest"],
  };
  const out: { key: string; value: string }[] = [];
  for (const key of keys[String(spec.action)] ?? []) {
    const v = specString(spec, key);
    if (v) out.push({ key, value: v });
  }
  return out;
}

const previewJson = computed(() =>
  props.item.request_preview == null || isTruncatedPreview(props.item.request_preview)
    ? ""
    : JSON.stringify(props.item.request_preview, null, 2),
);

/** G4 插件需求摘要（能力模式的可读目标；纯文本，不可信内容不拼接执行）。 */
function needText(need: ApprovalSpec): string {
  const first = (key: string): string => {
    const v = need[key];
    return Array.isArray(v) ? v.join(", ") : typeof v === "string" ? v : "";
  };
  return (
    first("url_prefix") ||
    first("program") ||
    first("executable") ||
    first("names") ||
    first("providers") ||
    first("recipients") ||
    "（族级）"
  );
}

// ---- 人工决定（仅 pending 可操作） ----

const scope = ref<"once" | "run">("once");
const denyReason = ref("");
const submitting = ref(false);

async function onApprove(): Promise<void> {
  if (submitting.value || !isPending.value) return;
  submitting.value = true;
  try {
    await api.runApprove(
      props.item.run_id,
      props.item.node_id,
      props.item.operation_id,
      props.item.request_hash,
      scope.value,
    );
    toast.success(scope.value === "run" ? "已批准（本 Run 内相同操作不再询问）" : "已批准");
    emit("resolved");
  } catch (e) {
    handleDecisionError(e);
  } finally {
    submitting.value = false;
  }
}

async function onDeny(): Promise<void> {
  if (submitting.value || !isPending.value) return;
  const reason = denyReason.value.trim();
  if (!reason) {
    toast.error("拒绝必须填写理由");
    return;
  }
  if (new TextEncoder().encode(reason).length > 4096) {
    toast.error("拒绝理由超过 4096 字节");
    return;
  }
  submitting.value = true;
  try {
    await api.runDeny(
      props.item.run_id,
      props.item.node_id,
      props.item.operation_id,
      props.item.request_hash,
      reason,
    );
    toast.info("已拒绝");
    emit("resolved");
  } catch (e) {
    handleDecisionError(e);
  } finally {
    submitting.value = false;
  }
}

/** -32012：已裁决/实例变化 → 刷新收件箱对齐最新状态；-32020：命令未生效，同样刷新 */
function handleDecisionError(e: unknown): void {
  if (e instanceof RpcError && e.code === -32012) {
    toast.info("该审批已被处理或实例已变化，正在刷新收件箱");
    emit("resolved");
    return;
  }
  if (e instanceof RpcError && e.code === -32020) {
    toast.error(`审批命令未生效：${e.message}`);
    emit("resolved");
    return;
  }
  toast.error(e instanceof Error ? e.message : String(e));
}
</script>

<template>
  <div class="approval-card">
    <div class="approval-head">
      <span class="badge" :class="approvalBadgeClass(item.status)">
        {{ approvalStatusLabel(item.status) }}
      </span>
      <span class="approval-actor" :title="item.node_id">{{ actorLabel(item.actor) }}</span>
      <span class="approval-gate muted">{{ item.gate === "tool" ? "工具门" : "节点门" }}</span>
      <span class="approval-deadline muted">{{ deadlineText }}</span>
    </div>

    <!-- G4 插件节点：specs 为空时展示 manifest 需求（能力模式，纯文本） -->
    <div
      v-if="item.specs.length === 0 && (item.needs?.length ?? 0) > 0"
      class="approval-spec"
    >
      <span class="spec-action">插件需求</span>
      <div class="spec-body">
        <div v-for="(need, i) in item.needs" :key="i" class="spec-mono">
          {{ capabilityActionLabel(String(need.action)) }} {{ needText(need) }}
        </div>
      </div>
    </div>

    <div v-for="(spec, i) in item.specs" :key="i" class="approval-spec">
      <span class="spec-action">{{ capabilityActionLabel(String(spec.action)) }}</span>
      <div class="spec-body">
        <template v-if="spec.action === 'exec'">
          <!-- argv 逐项展示（不拼 shell 字符串），空格分隔 -->
          <code class="spec-mono">{{ specString(spec, "executable") ?? "?" }}</code>
          <code v-for="(arg, j) in specStringList(spec, 'argv')" :key="j" class="spec-mono">{{
            arg
          }}</code>
        </template>
        <template v-else-if="spec.action === 'shell'">
          <code class="spec-mono">{{ specString(spec, "executable") || "shell" }}</code>
          <span class="muted">脚本摘要</span>
          <code class="spec-mono">sha256:{{ (specString(spec, "script_digest") ?? "").slice(0, 16) }}…</code>
        </template>
        <template v-else-if="spec.action === 'net'">
          <code class="spec-mono">{{ specString(spec, "method") ?? "?" }}</code>
          <code class="spec-mono spec-url">{{ specString(spec, "url") ?? "" }}</code>
        </template>
        <template v-else-if="spec.action === 'secret'">
          <!-- 密钥只显示名称，值绝不出现在卡片上 -->
          <code class="spec-mono">{{ specString(spec, "secret_name") ?? "?" }}</code>
        </template>
        <template v-else-if="spec.action === 'llm_call'">
          <code class="spec-mono">{{ specString(spec, "provider") ?? "?" }}</code>
          <span class="muted">/</span>
          <code class="spec-mono">{{ specString(spec, "model") ?? "?" }}</code>
        </template>
        <template v-else-if="spec.action === 'send_email'">
          <span class="muted">收件人</span>
          <code v-for="(r, j) in specStringList(spec, 'recipients')" :key="j" class="spec-mono">{{
            r
          }}</code>
        </template>
        <template v-else-if="spec.action === 'file_read' || spec.action === 'file_write' || spec.action === 'file_delete'">
          <code class="spec-mono spec-url">{{ specString(spec, "path") ?? specString(spec, "subtree") ?? "?" }}</code>
        </template>
        <code v-else class="spec-mono">{{ approvalSpecSummary(spec) }}</code>
      </div>
      <div v-if="specMeta(spec).length > 0" class="spec-meta">
        <span v-for="m in specMeta(spec)" :key="m.key" class="spec-meta-item" :title="m.value">
          {{ m.key }}: {{ m.value.slice(0, 12) }}…
        </span>
      </div>
    </div>

    <div v-if="item.matched.length > 0" class="approval-matched">
      <span v-for="m in item.matched" :key="m" class="matched-chip" :title="m">{{ m }}</span>
    </div>

    <div v-if="decisionText" class="approval-decision">{{ decisionText }}</div>

    <details v-if="item.request_preview != null" class="approval-preview">
      <summary class="muted">请求正文</summary>
      <p v-if="isTruncatedPreview(item.request_preview)" class="muted preview-truncated">
        请求正文过大（{{ item.request_preview.bytes ?? "?" }} 字节），服务端已截断
      </p>
      <!-- 不可信文本：只放 <pre> 纯文本，绝不 v-html -->
      <pre v-else class="spec-mono preview-json">{{ previewJson }}</pre>
    </details>

    <template v-if="isPending">
      <div class="approval-actions">
        <div class="action-row">
          <select v-model="scope" aria-label="批准范围">
            <option value="once">本次（Once）</option>
            <option value="run">本 Run</option>
          </select>
          <AppButton variant="primary" :disabled="submitting" @click="onApprove()">批准</AppButton>
          <span class="muted action-help">Run 仅免除相同操作的再次询问</span>
        </div>
        <div class="action-row">
          <input
            v-model="denyReason"
            type="text"
            placeholder="拒绝理由（必填）"
            @keydown.enter.prevent="onDeny()"
          />
          <AppButton variant="danger" :disabled="submitting || !denyReason.trim()" @click="onDeny()">
            拒绝
          </AppButton>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.approval-card {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 12px;
  min-width: 0;
}

.approval-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.approval-actor {
  font-weight: 600;
  max-width: 40%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.approval-deadline {
  margin-left: auto;
  font-size: 11px;
  white-space: nowrap;
}

.approval-spec {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 4px 0;
  border-top: 1px dashed var(--border);
}

.spec-action {
  color: var(--signal-2);
  font-size: 11px;
  flex-shrink: 0;
}

.spec-body {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 6px;
  align-items: baseline;
  min-width: 0;
}

.spec-mono {
  font-family: var(--mono);
  font-size: 11px;
  word-break: break-all;
}

.spec-url {
  min-width: 0;
}

.spec-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 10px;
}

.spec-meta-item {
  color: var(--text3);
  font-family: var(--mono);
  font-size: 10px;
}

.approval-matched {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.matched-chip {
  padding: 0 6px;
  border: 1px solid var(--signal);
  border-radius: 999px;
  color: var(--signal-2);
  font-size: 10px;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.approval-decision {
  color: var(--text2);
  font-size: 11px;
  word-break: break-all;
}

.approval-preview summary {
  cursor: pointer;
  font-size: 11px;
}

.preview-truncated {
  margin: 4px 0 0;
  font-size: 11px;
}

.preview-json {
  margin: 4px 0 0;
  padding: 6px 8px;
  max-height: 200px;
  overflow: auto;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 6px;
  white-space: pre-wrap;
  user-select: text;
}

.approval-actions {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-top: 2px;
}

.action-row {
  display: flex;
  gap: 6px;
  align-items: center;
}

.action-row input {
  flex: 1;
  min-width: 0;
}

.action-help {
  font-size: 11px;
}
</style>
