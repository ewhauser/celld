export interface Storage {
  sql: { exec(query: string, ...bindings: any[]): { toArray(): any[] } };
  transactionSync<T>(callback: () => T): T;
  sync(): Promise<void>;
}

export class SandboxError extends Error {
  readonly code: string;
  constructor(code: string, message = code) {
    super(message);
    this.code = code;
  }
}
export function check(
  condition: unknown,
  code = "EINVAL",
  message?: string,
): asserts condition {
  if (!condition) throw new SandboxError(code, message);
}
export function integer(n: unknown, max = Number.MAX_SAFE_INTEGER): number {
  check(typeof n === "number" && Number.isSafeInteger(n) && n >= 0 && n <= max);
  return n;
}
