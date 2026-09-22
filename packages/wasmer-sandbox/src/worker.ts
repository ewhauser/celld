// @ts-ignore This module is supplied by celld's Worker runtime.
import { DurableObject } from "cloudflare:workers";
import { Buffer } from "buffer";
import { WasmerSandbox } from "./index.ts";
import { boundedJSON, identifier } from "./protocol.ts";
import { check } from "./storage.ts";

export interface Env {
  SANDBOX_EXPERIMENTAL_NATIVE_STAT?: string;
  WORKSPACES: any;
  SANDBOX_API_TOKEN: string;
  SANDBOX_CALLBACK_TOKEN: string;
  SANDBOX_SUPERVISOR_TOKEN: string;
  SANDBOX_SUPERVISOR_URL: string;
}
function auth(request: Request, expected: string) {
  check(typeof expected === "string" && expected.length >= 32, "ECONFIG");
  // Tokens stay within authenticated TLS or loopback. Do not use this shared
  // service token as end-user authorization: apply your tenant policy upstream.
  check(request.headers.get("authorization") === `Bearer ${expected}`, "EAUTH");
}
export function errorResponse(error: unknown): Response {
  const e = error as { code?: string; message?: string };
  const status =
    e.code === "EAUTH"
      ? 401
      : e.code === "ENOENT"
        ? 404
        : ["EBUSY", "ECONFLICT", "ESTALE"].includes(e.code ?? "")
          ? 409
          : e.code === "ENOSPC"
            ? 507
            : 400;
  return Response.json(
    { code: e.code ?? "EINVAL", error: e.message ?? "invalid request" },
    { status, headers: { "cache-control": "no-store" } },
  );
}
export class SandboxWorkspace extends DurableObject {
  declare ctx: any;
  declare env: Env;
  readonly sandbox: WasmerSandbox;
  constructor(ctx: any, env: Env) {
    super(ctx, env);
    this.sandbox = new WasmerSandbox(ctx, {
      experimentalNativeStat: env.SANDBOX_EXPERIMENTAL_NATIVE_STAT === "1",
      workspace: ctx.id.toString(),
      supervisorURL: env.SANDBOX_SUPERVISOR_URL,
      supervisorToken: env.SANDBOX_SUPERVISOR_TOKEN,
    });
  }
  async fetch(request: Request): Promise<Response> {
    try {
      const url = new URL(request.url),
        parts = url.pathname.split("/");
      check(
        parts.length === 5 && parts[1] === "v1" && parts[2] === "workspaces",
      );
      const workspace = identifier(parts[3]),
        action = parts[4];
      const env = this.env as Env;
      auth(
        request,
        action === "fs" ? env.SANDBOX_CALLBACK_TOKEN : env.SANDBOX_API_TOKEN,
      );
      const id = /^[a-f0-9]{64}$/.test(workspace)
        ? env.WORKSPACES.idFromString(workspace)
        : env.WORKSPACES.idFromName(workspace);
      check(id.toString() === this.ctx.id.toString(), "ECONFLICT");
      if (action === "fs") return this.sandbox.callback(request);
      check(request.method === "POST");
      const body = await boundedJSON(request, 2 * 1024 * 1024);
      const sandbox = this.sandbox;
      let result: any;
      switch (action) {
        case "exec":
          result = await sandbox.exec(body);
          break;
        case "status":
          result = sandbox.status(body.id);
          break;
        case "cancel":
          result = await sandbox.cancel(body.id);
          break;
        case "read":
          result = { data: sandbox.fs.readFile(body.path).toString("base64") };
          break;
        case "write": {
          check(
            typeof body.data === "string" &&
              body.data.length <= 1400000 &&
              /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
                body.data,
              ),
          );
          sandbox.fs.writeFile(body.path, Buffer.from(body.data, "base64"));
          result = { ok: true };
          break;
        }
        case "list":
          result = sandbox.fs.list(body.path);
          break;
        case "stat":
          result = sandbox.fs.stat(body.path);
          break;
        case "mkdir":
          sandbox.fs.mkdir(body.path);
          result = { ok: true };
          break;
        case "rename":
          sandbox.fs.rename(body.path, body.to);
          result = { ok: true };
          break;
        case "unlink":
          sandbox.fs.unlink(body.path);
          result = { ok: true };
          break;
        case "rmdir":
          sandbox.fs.rmdir(body.path);
          result = { ok: true };
          break;
        default:
          throw Error("unknown action");
      }
      return Response.json(result, {
        headers: { "cache-control": "no-store" },
      });
    } catch (e) {
      return errorResponse(e);
    }
  }
}
export async function routeWorkspace(
  request: Request,
  env: Env,
): Promise<Response> {
  try {
    const p = new URL(request.url).pathname.split("/");
    check(p.length === 5 && p[1] === "v1" && p[2] === "workspaces");
    const workspace = identifier(p[3]);
    auth(
      request,
      p[4] === "fs" ? env.SANDBOX_CALLBACK_TOKEN : env.SANDBOX_API_TOKEN,
    );
    const id = /^[a-f0-9]{64}$/.test(workspace)
      ? env.WORKSPACES.idFromString(workspace)
      : env.WORKSPACES.idFromName(workspace);
    return env.WORKSPACES.get(id).fetch(request);
  } catch (e) {
    return errorResponse(e);
  }
}
