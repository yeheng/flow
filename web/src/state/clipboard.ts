import { toast } from "./toast";

/**
 * 剪贴板 + 全站统一的 toast 反馈；返回是否成功（调用方一般直接忽略）。
 * label 出现在成功提示里（如 "run id"/"输出"/"校验和"）。
 */
export async function copyWithToast(text: string, label: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    toast.success(`已复制 ${label}`);
    return true;
  } catch {
    toast.error("复制失败，请手动复制");
    return false;
  }
}
