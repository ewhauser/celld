import { Buffer } from "buffer";
import { WorkspaceFS, type Limits, type OpenFlags } from "./filesystem.ts";
import { check, integer, SandboxError, type Storage } from "./storage.ts";
import {
  boundedJSON,
  command,
  digest,
  identifier,
  serviceURL,
  type Command,
} from "./protocol.ts";
export { WorkspaceFS, SandboxError };
export type { Command, Storage };
export interface State {
  storage: Storage;
  assertCanAwaitCallback(): void;
  experimentalAgentFsStat?(token: string | null, deadline?: number): string;
}
export interface SandboxOptions {
  experimentalNativeStat?: boolean;
  supervisorURL: string;
  supervisorToken: string;
  workspace: string;
  limits?: Partial<Limits>;
  maxCommands?: number;
}
export interface CommandRecord {
  id: string;
  status:
    | "running"
    | "succeeded"
    | "failed"
    | "cancelled"
    | "timed_out"
    | "interrupted";
  result: any;
  startedAt: number;
  finishedAt: number | null;
}
interface Active {
  id: string;
  token: string;
  next: number;
  last?: { request: string; reply: any };
  cancelled: boolean;
  deadline: number;
}

/** One instance per DO activation. Route /fs only through your trusted router.
 * App mutations go through fs and are rejected while a command is running.
 */
export class WasmerSandbox {
  readonly fs: WorkspaceFS;
  private state: State;
  private options: SandboxOptions;
  private active: Active | null = null;
  private guestOperation = false;
  constructor(state: State, options: SandboxOptions) {
    check(
      typeof state.assertCanAwaitCallback === "function",
      "EPROTONOSUPPORT",
      "celld with assertCanAwaitCallback() is required",
    );
    serviceURL(options.supervisorURL);
    identifier(options.workspace);
    check(
      typeof options.supervisorToken === "string" &&
        options.supervisorToken.length >= 32,
      "EINVAL",
      "supervisor token must have at least 32 characters",
    );
    integer(options.maxCommands ?? 10000, 100000);
    this.state = state;
    this.options = options;
    this.fs = new WorkspaceFS(state.storage, options.limits, () =>
      check(
        !this.active || this.guestOperation,
        "EBUSY",
        "workspace has an active command",
      ),
    );
    state.storage.transactionSync(() => {
      this
        .rows(`CREATE TABLE IF NOT EXISTS celld_sandbox_commands(id TEXT PRIMARY KEY, payload TEXT NOT NULL,
        status TEXT NOT NULL, result TEXT, started_at INTEGER NOT NULL, finished_at INTEGER)`);
      this.rows(
        "UPDATE celld_sandbox_commands SET status='interrupted',finished_at=? WHERE status='running'",
        Date.now(),
      );
    });
  }
  private rows(sql: string, ...args: any[]) {
    return this.state.storage.sql.exec(sql, ...args).toArray();
  }
  status(id: string): CommandRecord | null {
    const row = this.rows(
      "SELECT * FROM celld_sandbox_commands WHERE id=?",
      identifier(id),
    )[0];
    return row
      ? {
          id: row.id,
          status: row.status,
          result: row.result ? JSON.parse(row.result) : null,
          startedAt: row.started_at,
          finishedAt: row.finished_at,
        }
      : null;
  }
  private async supervisor(path: string, body: any, timeout: number) {
    const response = await fetch(new URL(path, this.options.supervisorURL), {
      method: "POST",
      redirect: "error",
      headers: {
        authorization: `Bearer ${this.options.supervisorToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeout),
    });
    check(response.ok, "EIO", `supervisor returned HTTP ${response.status}`);
    return boundedJSON(response, 1024 * 1024);
  }
  async exec(input: Command): Promise<CommandRecord> {
    // Must precede all awaits, including hashing or querying the supervisor.
    this.state.assertCanAwaitCallback();
    const cmd = command(input);
    cmd.cwd = this.fs.path(cmd.cwd);
    const payload = await digest(JSON.stringify(cmd));
    this.state.assertCanAwaitCallback();
    const prior = this.rows(
      "SELECT payload FROM celld_sandbox_commands WHERE id=?",
      cmd.id,
    )[0];
    if (prior) {
      check(
        prior.payload === payload,
        "ECONFLICT",
        "command ID has a different payload",
      );
      await this.state.storage.sync();
      return this.status(cmd.id)!;
    }
    check(!this.active, "EBUSY");
    check(this.fs.stat(cmd.cwd).dir, "ENOTDIR");
    check(
      this.rows("SELECT COUNT(*) AS n FROM celld_sandbox_commands")[0].n <
        (this.options.maxCommands ?? 10000),
      "ENOSPC",
      "command journal full; archive IDs explicitly before reusing capacity",
    );
    const active: Active = {
      id: cmd.id,
      token: crypto.randomUUID() + crypto.randomUUID(),
      next: 1,
      cancelled: false,
      deadline: Date.now() + cmd.timeoutMs + 10000,
    };
    this.rows(
      "INSERT INTO celld_sandbox_commands VALUES(?,?,'running',NULL,?,NULL)",
      cmd.id,
      payload,
      Date.now(),
    );
    this.active = active;
    let nativeScope: string | undefined;
    try {
      if (this.options.experimentalNativeStat) {
        check(
          typeof this.state.experimentalAgentFsStat === "function",
          "EPROTONOSUPPORT",
        );
        nativeScope = this.state.experimentalAgentFsStat(
          active.token,
          active.deadline,
        );
      }
      await this.state.storage.sync();
      check(!active.cancelled, "ECANCELLED", "cancelled before launch");
      const result = await this.supervisor(
        "/v1/run",
        {
          ...cmd,
          workspace: this.options.workspace,
          token: active.token,
          nativeStatScope: nativeScope,
        },
        cmd.timeoutMs + 10000,
      );
      check(
        result &&
          ["exited", "timed_out", "cancelled", "failed"].includes(
            result.reason,
          ),
        "EPROTO",
      );
      check(
        Number.isInteger(result.exitCode) &&
          typeof result.stdout === "string" &&
          typeof result.stderr === "string" &&
          result.stdout.length + result.stderr.length <= 512 * 1024,
        "EPROTO",
      );
      const status = active.cancelled
        ? "cancelled"
        : result.reason === "timed_out"
          ? "timed_out"
          : result.reason === "cancelled"
            ? "cancelled"
            : result.reason === "exited" && result.exitCode === 0
              ? "succeeded"
              : "failed";
      this.rows(
        "UPDATE celld_sandbox_commands SET status=?,result=?,finished_at=? WHERE id=?",
        status,
        JSON.stringify(result),
        Date.now(),
        cmd.id,
      );
    } catch (e) {
      this.rows(
        "UPDATE celld_sandbox_commands SET status=?,result=?,finished_at=? WHERE id=?",
        active.cancelled ? "cancelled" : "interrupted",
        JSON.stringify({
          error: e instanceof Error ? e.message : "execution interrupted",
        }),
        Date.now(),
        cmd.id,
      );
    } finally {
      // Revokes callbacks before releasing the execution slot, including on a
      // lost helper response. A fresh activation never adopts old tokens.
      if (nativeScope) this.state.experimentalAgentFsStat!(null);
      this.active = null;
      this.fs.closeAll();
    }
    await this.state.storage.sync();
    return this.status(cmd.id)!;
  }
  async cancel(id: string): Promise<CommandRecord | null> {
    identifier(id);
    const active = this.active;
    if (active?.id === id) {
      active.cancelled = true; // admission closes immediately, before remote I/O
      if (this.options.experimentalNativeStat)
        this.state.experimentalAgentFsStat?.(null);
      try {
        await this.supervisor(
          "/v1/cancel",
          { workspace: this.options.workspace, id, token: active.token },
          5000,
        );
      } catch {}
    }
    await this.state.storage.sync();
    return this.status(id);
  }
  async callback(request: Request): Promise<Response> {
    try {
      check(request.method === "POST", "EINVAL");
      const b = await boundedJSON(request);
      const a = this.active;
      check(
        a && !a.cancelled && Date.now() < a.deadline && b.token === a.token,
        "ESTALE",
      );
      const seq = integer(b.seq, 100000);
      const serial = JSON.stringify(b);
      let reply;
      if (seq === a.next - 1 && a.last) {
        check(serial === a.last.request, "ECONFLICT");
        reply = a.last.reply;
      } else {
        check(seq === a.next, "ESTALE", "out-of-order filesystem request");
        this.guestOperation = true;
        try {
          reply = { value: this.operation(b) };
        } catch (e) {
          reply = {
            error: e instanceof Error ? e.message : "filesystem error",
            code: (e as SandboxError).code ?? "EIO",
          };
        } finally {
          this.guestOperation = false;
        }
        a.last = { request: serial, reply };
        a.next++;
      }
      // Every callback, including read/error/retry, participates in the real
      // cell output gate. No private ungated HTTP shortcut is used.
      if (b.op === "sync") await this.state.storage.sync();
      return Response.json(reply);
    } catch (e) {
      return Response.json(
        {
          error: e instanceof Error ? e.message : "invalid callback",
          code: (e as SandboxError).code ?? "EINVAL",
        },
        { status: 409 },
      );
    }
  }
  private operation(b: any): any {
    switch (b.op) {
      case "stat":
        return this.fs.stat(b.path);
      case "list":
        return this.fs.list(b.path);
      case "mkdir":
        this.fs.mkdir(b.path);
        return {};
      case "rename":
        this.fs.rename(b.path, b.to);
        return {};
      case "unlink":
        this.fs.unlink(b.path);
        return {};
      case "rmdir":
        this.fs.rmdir(b.path);
        return {};
      case "open": {
        const flags: OpenFlags = {};
        for (const k of [
          "read",
          "write",
          "append",
          "create",
          "create_new",
          "truncate",
        ] as const) {
          check(typeof b[k] === "boolean");
          flags[k] = b[k];
        }
        return { handle: this.fs.open(b.path, flags) };
      }
      case "close":
        this.fs.close(b.handle);
        return {};
      case "fstat":
        return this.fs.fstat(b.handle);
      case "read":
        return {
          data: Array.from(
            this.fs.read(b.handle, b.offset, integer(b.size, 65536)),
          ),
        };
      case "write":
        check(
          Array.isArray(b.data) &&
            b.data.length <= 65536 &&
            b.data.every(
              (v: unknown) =>
                typeof v === "number" &&
                Number.isInteger(v) &&
                v >= 0 &&
                v <= 255,
            ),
        );
        return this.fs.write(b.handle, b.offset, Buffer.from(b.data));
      case "truncate":
        this.fs.truncate(b.handle, b.size);
        return {};
      case "sync":
      case "heartbeat":
        return {};
      default:
        throw new SandboxError("ENOTSUP");
    }
  }
}
