import { toRaw } from "vue";

/** 深拷贝 JSON 面（params/config 等来自 JSON-RPC 的纯数据）。
 *
 * structuredClone 拒绝克隆 reactive Proxy（DataCloneError），而 JSON 往返
 * 会丢 undefined/Date 等合法值且慢——先 `toRaw` 剥壳再结构化克隆。
 * 依赖 Vue 的不变量：经 reactive setter 写入的值都会落成 raw（嵌套树里
 * 不会藏 Proxy），所以对根做一次 toRaw 即可。
 */
export function deepCloneJson<T>(value: T): T {
  return structuredClone(toRaw(value)) as T;
}
