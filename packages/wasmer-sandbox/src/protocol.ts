import { check, integer } from "./storage.ts";
export interface Command {
  id: string;
  tool: string;
  args?: string[];
  env?: Record<string, string>;
  cwd?: string;
  stdin?: string;
  timeoutMs?: number;
}
export interface NormalCommand {
  id: string;
  tool: string;
  args: string[];
  env: Record<string, string>;
  cwd: string;
  stdin: string;
  timeoutMs: number;
}
export function identifier(value: unknown): string {
  check(typeof value === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(value));
  return value;
}
export function command(value: Command): NormalCommand {
  check(value && typeof value === "object");
  const id = identifier(value.id),
    tool = identifier(value.tool);
  const args = value.args ?? [],
    env = value.env ?? {};
  check(
    Array.isArray(args) &&
      args.length <= 128 &&
      args.every(
        (s) => typeof s === "string" && s.length <= 4096 && !s.includes("\0"),
      ),
  );
  check(
    env &&
      typeof env === "object" &&
      !Array.isArray(env) &&
      Object.keys(env).length <= 64,
  );
  const sorted: Record<string, string> = Object.create(null);
  for (const key of Object.keys(env).sort()) {
    const v = env[key];
    check(
      /^[A-Za-z_][A-Za-z0-9_]*$/.test(key) &&
        typeof v === "string" &&
        v.length <= 4096 &&
        !v.includes("\0"),
    );
    sorted[key] = v;
  }
  const cwd = value.cwd ?? "/workspace",
    stdin = value.stdin ?? "",
    timeoutMs = value.timeoutMs ?? 30000;
  check(
    typeof cwd === "string" &&
      typeof stdin === "string" &&
      new TextEncoder().encode(stdin).length <= 65536,
  );
  check(integer(timeoutMs, 120000) >= 100);
  const result = {
    id,
    tool,
    args: [...args],
    env: sorted,
    cwd,
    stdin,
    timeoutMs,
  };
  check(
    new TextEncoder().encode(JSON.stringify(result)).length <= 131072,
    "E2BIG",
  );
  return result;
}
export async function digest(text: string): Promise<string> {
  return Array.from(
    new Uint8Array(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)),
    ),
  )
    .map((v) => v.toString(16).padStart(2, "0"))
    .join("");
}
export async function boundedJSON(
  request: Request | Response,
  max = 512 * 1024,
): Promise<any> {
  check(request.body, "EINVAL");
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      length += value.length;
      check(length <= max, "E2BIG");
      chunks.push(value);
    }
  } catch (e) {
    await reader.cancel().catch(() => {});
    throw e;
  }
  const bytes = new Uint8Array(length);
  let at = 0;
  for (const c of chunks) {
    bytes.set(c, at);
    at += c.length;
  }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
}
export function serviceURL(value: string): URL {
  const url = new URL(value);
  check(!url.username && !url.password && !url.search && !url.hash);
  check(
    url.protocol === "https:" ||
      (url.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)),
    "EINVAL",
    "use TLS except on loopback",
  );
  return url;
}
