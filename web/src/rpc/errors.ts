export class RpcError extends Error {
  constructor(
    public code: number,
    message: string,
    public data?: unknown,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

export function errText(e: unknown): string {
  if (e instanceof RpcError) return e.message;
  if (e instanceof Error) return e.message;
  return String(e);
}

/**
 * 物化视图预算超限（服务端 -32021 / "RESPONSE_TOO_LARGE"）：该读面把
 * StoredValue 物化成业务值时超过 8MB 预算。调用方应降级到原始快照
 * （run.get / run.list 返回信封，不物化）再渲染。
 */
export function isResponseTooLarge(e: unknown): boolean {
  return e instanceof RpcError && (e.code === -32021 || e.message.includes("RESPONSE_TOO_LARGE"));
}
