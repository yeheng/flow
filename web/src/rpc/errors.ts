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

