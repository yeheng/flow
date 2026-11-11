/**
 * vitest 全局 setup：Node 25+ 自带实验性 localStorage（未传 --localstorage-file
 * 时是一个返回 undefined 的 getter），在 jsdom 环境里遮蔽了 jsdom 的实现，导致
 * 模块顶层的 localStorage.getItem 直接抛错（dev.ts / persist.ts / NodePalette）。
 * 这里在取不到实现时补一个内存版 Storage，仅在测试进程内生效。
 */
const existing = (globalThis as { localStorage?: Storage }).localStorage;
if (!existing) {
  const store = new Map<string, string>();
  const shim: Storage = {
    get length(): number {
      return store.size;
    },
    clear(): void {
      store.clear();
    },
    getItem(key: string): string | null {
      return store.has(key) ? (store.get(key) as string) : null;
    },
    key(index: number): string | null {
      return [...store.keys()][index] ?? null;
    },
    removeItem(key: string): void {
      store.delete(key);
    },
    setItem(key: string, value: string): void {
      store.set(key, String(value));
    },
  };
  Object.defineProperty(globalThis, "localStorage", {
    value: shim,
    configurable: true,
    writable: true,
  });
}
