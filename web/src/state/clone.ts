import { isRef, toRaw } from "vue";

/** 递归解包 Vue ref（ref.value 可能还是 ref/Proxy，走同一规则）。
 * 非 ref 层用 toRaw 剥壳；容器重建为普通对象/数组。 */
function unwrapRefs(value: unknown): unknown {
  if (isRef(value)) return unwrapRefs(value.value);
  const raw = toRaw(value);
  if (Array.isArray(raw)) return raw.map((item) => unwrapRefs(item));
  if (raw !== null && typeof raw === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(raw)) out[key] = unwrapRefs(item);
    return out;
  }
  return raw;
}

/** 深拷贝 JSON 面（params/config 等来自 JSON-RPC 的纯数据）。
 *
 * 首选 `structuredClone(toRaw(value))`：快且保留 undefined/Date。但
 * structuredClone 拒绝任何不可克隆成员——reactive Proxy 嵌在 raw 树深处
 * （跨树赋值 / shallowRef / 手写 Object.assign 绕过 setter 都可能造出来）、
 * 误入树的 Vue ref/computed（类实例含函数）——直接抛 DataCloneError，把
 * 保存/发布整条链路炸掉。这些场景对「JSON 面」本就该按 JSON 语义拷贝：
 * fallback 先解包 ref/剥壳再 JSON 往返，永不抛 DataCloneError。干净树
 * （绝大多数调用）仍走快路径，Date 等可克隆对象原样保留。 */
export function deepCloneJson<T>(value: T): T {
  // 解包在主路径之前：structuredClone 对 Proxy 抛错（可 fallback），但对
  // ref 不抛——它把 ref 内部字段（__v_isRef/_rawValue/dep）克隆成垃圾，
  // 那比抛错更糟。JSON 面数据本无 Date/Map（都来自 JSON-RPC 序列化），
  // 整树 walk 语义无损。
  try {
    return structuredClone(unwrapRefs(value)) as T;
  } catch (error) {
    // 浏览器抛 DOMException；Node（vitest/SSR）抛同名普通 Error——按 name 判定
    if ((error as Error)?.name === "DataCloneError") {
      return JSON.parse(JSON.stringify(unwrapRefs(value))) as T;
    }
    throw error;
  }
}
