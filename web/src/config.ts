/**
 * 运行时配置：部署后在 <webroot>/config.json 放置 { "rpcUrl": "wss://…", "httpUrl": "https://…" }
 * 即可切换 flow-server 地址，无需重新构建。缺省时回落构建期 VITE_FLOW_*，再回落本地默认值。
 * 启动时由 main.ts 拉取一次（无配置文件时静默回落）；解析在首次使用时惰性发生。
 */

import { invoke } from "@tauri-apps/api/core";
import { isDesktop } from "./platform";

export interface RuntimeConfig {
  rpcUrl?: string;
  httpUrl?: string;
}

let runtime: RuntimeConfig = {};

export async function loadRuntimeConfig(): Promise<void> {
  if (isDesktop()) {
    runtime = await invoke<RuntimeConfig>("flow_info");
    return;
  }
  try {
    const r = await fetch("/config.json", { cache: "no-store" });
    if (r.ok) runtime = (await r.json()) as RuntimeConfig;
  } catch {
    // 无运行时配置：走构建期/本地默认值
  }
}

export function rpcUrl(): string {
  return (
    runtime.rpcUrl ?? (import.meta.env.VITE_FLOW_RPC as string | undefined) ?? "ws://127.0.0.1:9800"
  );
}

export function httpUrl(): string {
  return (
    runtime.httpUrl ??
    (import.meta.env.VITE_FLOW_HTTP as string | undefined) ??
    "http://127.0.0.1:9801"
  );
}
