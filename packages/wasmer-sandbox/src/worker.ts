// @ts-ignore This module is supplied by celld's Worker runtime.
import { DurableObject } from "cloudflare:workers";
import { Buffer } from "buffer";
import { WasmerSandbox } from "./index.ts";
import { boundedJSON } from "./protocol.ts";
import { check } from "./storage.ts";

import { authorizedWorkspace, type AuthorizationEnv } from "./authorization.ts";

export interface Env extends AuthorizationEnv {
  SANDBOX_SUPERVISOR_TOKEN: string;
  SANDBOX_SUPERVISOR_URL: string;
}
export function errorResponse(error: unknown): Response {
  const e = error as { code?: string; message?: string };
  const status =
    e.code === "ECONFIG"
      ? 503
      : e.code === "EAUTH"
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
      nativeFilesystem: env.SANDBOX_NATIVE_FILESYSTEM !== "0",
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
      const action = parts[4];
      const id = await authorizedWorkspace(request, this.env);
      check(id.toString() === this.ctx.id.toString(), "EAUTH");
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
    const id = await authorizedWorkspace(request, env);
    return env.WORKSPACES.get(id).fetch(request);
  } catch (e) {
    return errorResponse(e);
  }
}
