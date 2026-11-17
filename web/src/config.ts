/**
 * 运行时配置：部署后在 <webroot>/config.json 放置 { "rpcUrl": "wss://…", "httpUrl": "https://…" }
 * 即可切换 flow-journal-server 地址，无需重新构建。缺省时回落构建期 VITE_FLOW_*，再回落本地默认值。
 * 启动时由 main.ts 拉取一次（无配置文件时静默回落）；解析在首次使用时惰性发生。
 */

import { invoke } from "@tauri-apps/api/core";
import { isDesktop } from "./platform";

export interface RuntimeConfig {
  rpcUrl?: string;
  httpUrl?: string;
  /** v2 API 认证 token（journal-server 部署必配；桌面内嵌服务不需要） */
  token?: string;
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
    (isDesktop() ? null : localStorage.getItem("flow.rpcUrl")) ??
    runtime.rpcUrl ??
    (import.meta.env.VITE_FLOW_RPC as string | undefined) ??
    "ws://127.0.0.1:9802"
  );
}

/** v2 API token：localStorage > config.json > VITE_FLOW_TOKEN（用户在 UI 输入） */
export function apiToken(): string {
  return (
    (typeof localStorage !== "undefined" ? localStorage.getItem("flow.token") : null) ??
    runtime.token ??
    (import.meta.env.VITE_FLOW_TOKEN as string | undefined) ??
    ""
  );
}

export function setApiToken(token: string): void {
  if (token) localStorage.setItem("flow.token", token);
  else localStorage.removeItem("flow.token");
}

export function httpUrl(): string {
  return (
    (isDesktop() ? null : localStorage.getItem("flow.httpUrl")) ??
    runtime.httpUrl ??
    (import.meta.env.VITE_FLOW_HTTP as string | undefined) ??
    "http://127.0.0.1:9803"
  );
}

/** Validate both endpoints before persisting either one. Empty values restore deployment defaults. */
export function setServerUrls(rpc: string, http: string): void {
  const entries = [
    ["flow.rpcUrl", rpc.trim(), ["ws:", "wss:"]],
    ["flow.httpUrl", http.trim(), ["http:", "https:"]],
  ] as const;
  for (const [, value, protocols] of entries) {
    if (!value) continue;
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      throw new Error("请输入完整的服务地址");
    }
    if (
      !(protocols as readonly string[]).includes(url.protocol) ||
      url.username ||
      url.password ||
      url.hash
    ) {
      throw new Error("RPC 地址须使用 ws/wss，HTTP 地址须使用 http/https，且不能包含凭据或片段");
    }
  }
  for (const [key, value] of entries) {
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  }
}
