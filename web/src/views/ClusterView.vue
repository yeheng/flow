<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import {
  clusterStatus,
  clusterNodes,
  removeClusterNode,
  progressLag,
  type ClusterStatus,
  type ClusterPage,
  type ClusterNode,
} from "../api/cluster";
import { errText, RpcError } from "../rpc/client";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import AppButton from "../components/AppButton.vue";

const status = ref<ClusterStatus>();
const page = ref<ClusterPage>();
const error = ref("");
const busy = ref(false);
const removing = ref("");
const after = ref("");
const pending = new Map<string, string>();
let timer: ReturnType<typeof setInterval> | undefined;
let disposed = false;
const labels: Record<ClusterNode["status"], string> = {
  online: "在线",
  degraded: "异常",
  offline: "租约过期",
  stopped: "已停止",
  removed: "已剔除",
};
const online = computed(() => page.value?.nodes.filter((n) => n.status === "online").length ?? 0);
async function refresh(cursor = after.value) {
  if (busy.value) return;
  busy.value = true;
  try {
    const current = await clusterStatus();
    const result = current.enabled ? await clusterNodes(cursor) : undefined;
    if (disposed) return;
    status.value = current;
    page.value = result;
    after.value = cursor;
    error.value = "";
  } catch (e) {
    if (!disposed) error.value = errText(e);
  } finally {
    if (!disposed) busy.value = false;
  }
}
async function remove(node: ClusterNode) {
  if (removing.value) return;
  const count = Object.entries(node.snapshot.run_counts)
    .filter(([s]) => !["succeeded", "failed", "cancelled"].includes(s))
    .reduce((total, [, n]) => total + n, 0);
  if (
    !(await confirmDialog(
      `永久剔除节点 ${node.node_id}？最近上报有 ${count} 个未终结 run。节点将在下一次心跳拒绝后停止；已有执行可能需要人工核对。本操作不迁移日志或自动重试 run，且该节点与 journal 的绑定无法重新注册。`,
      { danger: true, confirmText: "永久剔除" },
    ))
  )
    return;
  removing.value = node.node_id;
  const key = `${node.node_id}:${node.epoch}`;
  const id = pending.get(key) ?? crypto.randomUUID();
  pending.set(key, id);
  try {
    await removeClusterNode(node, id);
    pending.delete(key);
    toast.success(`节点 ${node.node_id} 已剔除`);
    await refresh();
  } catch (e) {
    if (e instanceof RpcError && [-32012, -32602].includes(e.code)) pending.delete(key);
    toast.error(`${errText(e)}；刷新后可使用相同命令重试`);
  } finally {
    removing.value = "";
  }
}
onMounted(() => {
  void refresh();
  timer = setInterval(() => void refresh(), 5000);
});
onBeforeUnmount(() => {
  disposed = true;
  clearInterval(timer);
});
</script>

<template>
  <main class="cluster-page">
    <header>
      <div>
        <h1>Journal 节点管理</h1>
        <p>各节点独立保存 journal，PG 管理注册、归属、租约和同步进度。</p>
      </div>
      <AppButton :disabled="busy" @click="refresh()">{{ busy ? "刷新中…" : "刷新" }}</AppButton>
    </header>
    <p v-if="error" role="alert" class="error">
      {{ error }}<span v-if="page"> · 当前显示上一次成功查询的数据</span>
    </p>
    <section v-if="status && !status.enabled" class="empty">
      <h2>尚未启用节点注册</h2>
      <p>
        使用启用 journal-cluster feature 的服务端，并在配置中添加
        [journal.cluster]。每个节点需要稳定且唯一的 node_id 和独立数据目录。
      </p>
    </section>
    <template v-else-if="status?.enabled && page">
      <section class="overview">
        <div>
          <small>集群</small><strong>{{ status.cluster_id }}</strong>
        </div>
        <div>
          <small>当前连接节点</small><strong>{{ status.node_id }}</strong>
        </div>
        <div>
          <small>当前页在线 / 节点数</small><strong>{{ online }} / {{ page.nodes.length }}</strong>
        </div>
        <div>
          <small>PG 查询时间</small
          ><strong>{{ new Date(page.server_time).toLocaleString() }}</strong>
        </div>
      </section>
      <p class="hint">
        业务页面显示当前连接节点的工作流与 run。这里的进度是该节点向 PG
        上报的游标，不代表日志已复制到其他节点。
      </p>
      <p v-if="!page.nodes.length" class="empty">当前页没有节点。</p>
      <section v-for="node in page.nodes" :key="node.node_id" class="node">
        <div class="node-title">
          <div>
            <strong>{{ node.node_id }}</strong
            ><span v-if="node.node_id === status.node_id" class="current">当前连接</span>
          </div>
          <span class="badge" :class="node.status">{{ labels[node.status] }}</span>
          <AppButton
            variant="danger"
            :disabled="node.status === 'removed' || !!removing"
            @click="remove(node)"
          >
            {{ removing === node.node_id ? "剔除中…" : "剔除" }}
          </AppButton>
        </div>
        <dl>
          <div>
            <dt>Journal</dt>
            <dd class="mono">{{ node.journal_id }}</dd>
          </div>
          <div>
            <dt>注册 epoch / 修订</dt>
            <dd class="mono">{{ node.epoch }} / {{ node.revision }}</dd>
          </div>
          <div>
            <dt>实例 / 版本</dt>
            <dd class="mono">{{ node.incarnation_id }} / {{ node.build }}</dd>
          </div>
          <div>
            <dt>RPC / HTTP</dt>
            <dd class="mono">{{ node.rpc_url }}<br />{{ node.http_url }}</dd>
          </div>
          <div>
            <dt>持久化 / 内存 / 投影 LSN</dt>
            <dd class="mono">
              {{ node.snapshot.durable_lsn }} / {{ node.snapshot.applied_lsn }} /
              {{ node.snapshot.projected_lsn }}
            </dd>
          </div>
          <div>
            <dt>内存 / 投影落后</dt>
            <dd class="mono">
              {{ progressLag(node.snapshot.durable_lsn, node.snapshot.applied_lsn) }} /
              {{ progressLag(node.snapshot.durable_lsn, node.snapshot.projected_lsn) }}
            </dd>
          </div>
          <div>
            <dt>最近心跳</dt>
            <dd>{{ new Date(node.last_seen_at).toLocaleString() }}</dd>
          </div>
          <div>
            <dt>租约到期（PG 时间）</dt>
            <dd>{{ new Date(node.lease_until).toLocaleString() }}</dd>
          </div>
        </dl>
        <p v-if="node.snapshot.writer_error || node.snapshot.projection_error" class="error">
          {{ node.snapshot.writer_error || node.snapshot.projection_error }}
        </p>
      </section>
      <footer>
        <AppButton :disabled="!after || busy" @click="refresh('')">首页</AppButton
        ><AppButton :disabled="!page.next_after || busy" @click="refresh(page.next_after ?? '')">
          下一页</AppButton
        ><span>每 5 秒刷新当前页</span>
      </footer>
    </template>
    <p v-else-if="busy">正在读取注册状态…</p>
  </main>
</template>

<style scoped>
.cluster-page {
  flex: 1;
  overflow: auto;
  padding: 28px;
  max-width: 1400px;
  width: 100%;
  margin: 0 auto;
}
header,
.node-title,
footer {
  display: flex;
  align-items: center;
  gap: 16px;
}
header {
  justify-content: space-between;
  margin-bottom: 24px;
}
header button {
  flex-shrink: 0;
  white-space: nowrap;
}
h1 {
  font-size: 22px;
  margin: 0 0 8px;
}
p,
dt,
small {
  color: var(--text2);
}
header p {
  margin: 0;
}
.overview {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  background: var(--surface);
  padding: 20px;
  border-radius: var(--radius);
}
.overview strong,
.overview small {
  display: block;
  overflow-wrap: anywhere;
}
.overview strong {
  margin-top: 8px;
}
.hint {
  line-height: 1.7;
  margin: 20px 0;
}
.node {
  padding: 20px;
  margin: 14px 0;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
.node-title > div {
  flex: 1;
  min-width: 0;
  overflow-wrap: anywhere;
}
.node-title strong {
  font-size: 16px;
}
.current {
  margin-left: 12px;
  color: var(--accent2);
  font-size: 12px;
}
.badge {
  padding: 4px 9px;
  background: var(--surface2);
  border-radius: 5px;
}
.online {
  color: var(--ok);
}
.degraded,
.offline {
  color: var(--warn);
}
.removed,
.error {
  color: var(--danger);
}
dl {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 18px 28px;
  margin-bottom: 0;
}
dt {
  font-size: 12px;
  margin-bottom: 5px;
}
dd {
  margin: 0;
  overflow-wrap: anywhere;
  line-height: 1.6;
}
.mono {
  font-family: var(--mono);
  font-size: 12px;
}
.empty {
  background: var(--surface);
  padding: 24px;
  border-radius: var(--radius);
  line-height: 1.8;
}
footer {
  margin-top: 20px;
  color: var(--text2);
}
@media (max-width: 850px) {
  .overview,
  dl {
    grid-template-columns: 1fr 1fr;
  }
  .cluster-page {
    padding: 16px;
  }
}
@media (max-width: 560px) {
  .overview,
  dl {
    grid-template-columns: 1fr;
  }
}
</style>
