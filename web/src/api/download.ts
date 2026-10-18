/**
 * 完整值下载（journal_download.rs 的 /runs/{run_id}/values/{output_id} 路由）。
 * 桌面走 Tauri 原生命令（原生保存对话框 + Rust 流式写入）；浏览器走
 * showSaveFilePicker 流式保存，不支持时回退 <a download> 整块下载。
 */
import { invoke } from "@tauri-apps/api/core";
import { isDesktop } from "../platform";
import { apiToken, httpUrl } from "../config";

export function runValueUrl(runId: string, outputId: string): string {
  return `${httpUrl().replace(/\/$/, "")}/runs/${encodeURIComponent(runId)}/values/${encodeURIComponent(outputId)}`;
}

/** 下载一个 ref 大值；失败抛出可展示的中文错误 */
export async function downloadValue(runId: string, outputId: string): Promise<void> {
  if (isDesktop()) {
    await invoke("flow_download", { run: runId, output: outputId });
    return;
  }
  const response = await fetch(runValueUrl(runId, outputId), {
    headers: { Authorization: `Bearer ${apiToken()}` },
  });
  if (!response.ok || !response.body) {
    throw new Error(`下载失败：HTTP ${response.status}`);
  }
  const picker = (
    window as unknown as {
      showSaveFilePicker?: (
        options: unknown,
      ) => Promise<{ createWritable: () => Promise<WritableStream> }>;
    }
  ).showSaveFilePicker;
  if (picker) {
    const file = await picker({ suggestedName: `${outputId}.bin` });
    await response.body.pipeTo(await file.createWritable());
    return;
  }
  // 无流式保存 API：整块下载（大值会占内存，提示用户优先用桌面/CLI）
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  try {
    const a = document.createElement("a");
    a.href = url;
    a.download = `${outputId}.bin`;
    a.click();
  } finally {
    URL.revokeObjectURL(url);
  }
}
