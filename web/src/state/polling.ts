import { onMounted, onUnmounted } from "vue";

/**
 * 列表页轮询脚手架：定间隔触发 refresh，页面不可见时跳过本轮、恢复可见时立即
 * 补拉（后台标签不再持续打请求）。refresh 由调用方提供，自带在途守卫与
 * 错误处理（toast-once 等）；手动刷新与重连补拉仍由调用方自行叠加。
 */
export function usePolling(refresh: () => unknown, intervalMs: number): { stop: () => void } {
  let timer: ReturnType<typeof setInterval> | null = null;

  function tick(): void {
    if (document.hidden) return;
    void refresh();
  }

  function onVisibility(): void {
    if (!document.hidden) void refresh();
  }

  onMounted(() => {
    timer = setInterval(tick, intervalMs);
    document.addEventListener("visibilitychange", onVisibility);
  });

  onUnmounted(() => {
    stop();
    document.removeEventListener("visibilitychange", onVisibility);
  });

  /** 停止轮询（降级扫描等切手动刷新的场景）；visibilitychange 监听保留到卸载 */
  function stop(): void {
    if (timer) clearInterval(timer);
    timer = null;
  }

  return { stop };
}
