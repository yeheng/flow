/**
 * 本地持久化的唯一 JSON 读写口：**损坏值回退默认，读取绝不抛异常**。
 * localStorage 是用户可触碰的存储：半截写入/旧格式/手工编辑都会出现，
 * 解析失败绝不能拖垮组件 setup（NodePalette 曾因此崩掉整个编辑器）。
 */
export function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw === null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function writeJson(key: string, value: unknown): void {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // 存储不可用（隐私模式/配额满）不该打断交互，静默放弃
  }
}
