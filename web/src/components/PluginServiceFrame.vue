<script setup lang="ts">
import { ref, onUnmounted } from "vue";
import { client, errText } from "../rpc/client";
import { httpUrl } from "../config";
import type { ResidentService } from "../types";
const props = defineProps<{ service: ResidentService }>();
const emit = defineEmits<{ ended: []; error: [message: string] }>();
const frame = ref<HTMLIFrameElement | null>(null);
let port: MessagePort | null = null;
let unsubscribe: (() => Promise<void>) | null = null;
let disposed = false;
let attached = false;
function dispose() {
  port?.close();
  port = null;
  if (unsubscribe) void unsubscribe();
  unsubscribe = null;
}
function close() {
  dispose();
  emit("ended");
}
function attach() {
  if (attached) {
    close();
    return;
  }
  const service = props.service,
    target = frame.value?.contentWindow;
  if (!service || !target) return;
  attached = true;
  const channel = new MessageChannel(),
    bound = channel.port1;
  port = bound;
  const pending = new Set<string>();
  let count = 0,
    epoch = Date.now();
  bound.onmessage = async (event: MessageEvent) => {
    if (port !== bound) return;
    const d = event.data as { id?: unknown; method?: unknown; params?: unknown } | null;
    if (!d || typeof d.id !== "string" || d.id.length > 128 || typeof d.method !== "string") return;
    const id = d.id;
    if (Date.now() - epoch > 1000) {
      epoch = Date.now();
      count = 0;
    }
    if (++count > 32 || pending.size >= 8 || pending.has(id)) {
      bound.postMessage({ id, error: "请求过多" });
      return;
    }
    pending.add(id);
    try {
      if (JSON.stringify(d).length > 65536) throw new Error("请求过大");
      const response = await client.call<{ value: unknown }>("services.call", {
        plugin_id: service.plugin_id,
        instance: service.instance,
        method: d.method,
        params: d.params,
      });
      if (port === bound) bound.postMessage({ id, result: response.value });
    } catch (e) {
      if (port === bound) bound.postMessage({ id, error: errText(e) });
    } finally {
      pending.delete(id);
    }
  };
  void client
    .subscribe(
      "services.subscribe",
      { plugin_id: service.plugin_id, instance: service.instance, after: "0" },
      (event) => {
        if (port !== bound || disposed) return;
        const data = event as Record<string, unknown>;
        if (data.type === "disconnected" || data.type === "resync_required") {
          bound.postMessage({ type: "disconnected", error: String(data.type) });
          close();
        } else if (data.type === "gap") bound.postMessage({ type: "gap", cursor: data.cursor });
        else bound.postMessage({ type: "event", ...data });
      },
    )
    .then((stop) => {
      if (port !== bound || disposed) void stop();
      else unsubscribe = stop;
    })
    .catch((e) => {
      if (port === bound) {
        bound.postMessage({ type: "disconnected", error: errText(e) });
        close();
        emit("error", errText(e));
      }
    });
  target.postMessage({ type: "flow-ui-connect", version: 1, instance: service.instance }, "*", [
    channel.port2,
  ]);
}
onUnmounted(() => {
  disposed = true;
  dispose();
});
</script>
<template>
  <iframe
    ref="frame"
    :src="`${httpUrl().replace(/\/$/, '')}/plugin-ui/${encodeURIComponent(service.ui!)}`"
    sandbox="allow-scripts"
    referrerpolicy="no-referrer"
    :title="`${service.plugin_id} 插件界面`"
    @load="attach"
  />
</template>
<style scoped>
iframe {
  display: block;
  width: 100%;
  height: 480px;
  border: 1px solid var(--border);
  margin-top: 12px;
}
</style>
