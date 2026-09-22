import { DurableObject } from "cloudflare:workers";
import { AgentFS } from "agentfs-sdk/cloudflare";
import { Buffer } from "buffer";

// Loopback-only synthetic probe. This is not an authenticated public API.
export class Workspace extends DurableObject {
  fs: AgentFS;
  active: string | null = null;
  handles = new Map<number, any>();
  nextHandle = 1;
  constructor(ctx: any, env: any) {
    super(ctx, env);
    this.fs = AgentFS.create(ctx.storage);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS probe_runs(id TEXT PRIMARY KEY, payload TEXT, status TEXT, result TEXT)",
    );
    ctx.storage.sql.exec(
      "UPDATE probe_runs SET status='interrupted' WHERE status='running'",
    );
  }
  async fetch(request: Request) {
    try {
      const url = new URL(request.url);
      const body =
        request.method === "POST" ? ((await request.json()) as any) : {};
      if (url.pathname === "/init") {
        if (this.active) throw new Error("EBUSY");
        try {
          await this.fs.mkdir("/workspace");
        } catch (e: any) {
          if (e.code !== "EEXIST") throw e;
        }
        for (const p of [
          "/workspace/partial.txt",
          "/workspace/owner-partial.txt",
        ]) {
          try {
            await this.fs.unlink(p);
          } catch (e: any) {
            if (e.code !== "ENOENT") throw e;
          }
        }
        await this.fs.writeFile(
          "/workspace/input.txt",
          "hello from TypeScript\n",
        );
        await this.fs.writeFile(
          "/workspace/large.bin",
          Buffer.alloc(256 * 1024, 97),
        );
        await this.ctx.storage.sync();
        return Response.json({ ok: true });
      }
      if (url.pathname === "/inspect") {
        const names = await this.fs.readdir("/workspace");
        const files: any = {};
        for (const name of names) {
          const path = "/workspace/" + name;
          const s = await this.fs.stat(path);
          if (!s.isDirectory()) {
            const b = await this.fs.readFile(path);
            files[name] = {
              size: s.size,
              bytes: b.length,
              text: b.length < 100 ? b.toString() : undefined,
            };
          }
        }
        return Response.json({
          files,
          active: !!this.active,
          commands: this.ctx.storage.sql
            .exec("SELECT * FROM probe_runs")
            .toArray(),
        });
      }
      if (url.pathname === "/exec") {
        if (this.active) return new Response("busy", { status: 409 });
        const id = String(body.id);
        const old = this.ctx.storage.sql
          .exec("SELECT * FROM probe_runs WHERE id=?", id)
          .toArray()[0];
        if (old) {
          if (old.payload !== (body.mode || "basic"))
            return new Response("command ID payload conflict", { status: 409 });
          return Response.json({ recovered: true, ...old });
        }
        this.active = crypto.randomUUID();
        this.ctx.storage.sql.exec(
          "INSERT INTO probe_runs VALUES (?, ?, 'running', NULL)",
          id,
          body.mode || "basic",
        );
        await this.ctx.storage.sync();
        try {
          // No transaction or blockConcurrencyWhile may span this await.
          const response = await fetch("http://127.0.0.1:19877/run", {
            method: "POST",
            body: JSON.stringify({
              token: this.active,
              mode: body.mode || "basic",
            }),
          });
          const result = (await response.json()) as any;
          this.ctx.storage.sql.exec(
            "UPDATE probe_runs SET status=?, result=? WHERE id=?",
            result.killed
              ? "cancelled"
              : result.code === 0
                ? "succeeded"
                : "failed",
            JSON.stringify(result),
            id,
          );
          await this.ctx.storage.sync();
          return Response.json(result);
        } catch (e) {
          this.ctx.storage.sql.exec(
            "UPDATE probe_runs SET status='interrupted' WHERE id=?",
            id,
          );
          throw e;
        } finally {
          this.active = null;
          this.handles.clear();
        }
      }
      if (url.pathname === "/fs") {
        if (!this.active || body.token !== this.active)
          return new Response("stale execution", { status: 409 });
        return Response.json(await this.operation(body));
      }
      return new Response("not found", { status: 404 });
    } catch (e: any) {
      return Response.json(
        { error: e.message, code: e.code || "EIO" },
        { status: 400 },
      );
    }
  }
  path(p: string) {
    if (
      !(p === "/workspace" || p.startsWith("/workspace/")) ||
      p.includes("..") ||
      p.includes("\0")
    )
      throw new Error("path denied");
    return p;
  }
  stats(s: any) {
    return { size: s.size, dir: s.isDirectory(), ino: s.ino };
  }
  async operation(b: any): Promise<any> {
    const fs = this.fs;
    const p = b.path === undefined ? undefined : this.path(b.path);
    const h = b.handle === undefined ? undefined : this.handles.get(b.handle);
    if (b.handle !== undefined && !h) throw new Error("stale handle");
    switch (b.op) {
      case "stat":
        return this.stats(await fs.stat(p!));
      case "list":
        return Promise.all(
          (await fs.readdir(p!)).map(async (name) => ({
            name,
            ...this.stats(await fs.stat(p! + "/" + name)),
          })),
        );
      case "mkdir":
        await fs.mkdir(p!);
        return {};
      case "rename":
        await fs.rename(p!, this.path(b.to));
        return {};
      case "unlink":
        await fs.unlink(p!);
        return {};
      case "rmdir":
        await fs.rmdir(p!);
        return {};
      case "open": {
        let exists = true;
        try {
          await fs.stat(p!);
        } catch (e: any) {
          if (e.code !== "ENOENT") throw e;
          exists = false;
        }
        if (exists && b.create_new)
          throw Object.assign(new Error("exists"), { code: "EEXIST" });
        if (!exists) {
          if (!b.create && !b.create_new)
            throw Object.assign(new Error("missing"), { code: "ENOENT" });
          await fs.writeFile(p!, Buffer.alloc(0));
        }
        const handle = await fs.open(p!);
        if (b.truncate) await handle.truncate(0);
        const id = this.nextHandle++;
        this.handles.set(id, {
          file: handle,
          read: b.read,
          write: b.write || b.append,
          append: b.append,
        });
        return { handle: id };
      }
      case "fstat":
        return this.stats(await h.file.fstat());
      case "read": {
        if (!h.read) throw new Error("read denied");
        return {
          data: Array.from(
            await h.file.pread(b.offset, Math.min(b.size, 65536)),
          ),
        };
      }
      case "write": {
        if (!h.write) throw new Error("write denied");
        const offset = h.append ? (await h.file.fstat()).size : b.offset;
        // The first version has one guest writer. Concurrent append is not proven.
        await h.file.pwrite(offset, Buffer.from(b.data));
        return { written: b.data.length };
      }
      case "truncate":
        if (!h.write) throw new Error("write denied");
        await h.file.truncate(b.size);
        return {};
      case "sync":
        await this.ctx.storage.sync();
        return {};
      case "do-read":
        return { text: await fs.readFile("/workspace/output.txt", "utf8") };
      default:
        throw new Error("unsupported operation");
    }
  }
}
export default {
  fetch(request: Request, env: any) {
    return env.WORKSPACE.get(env.WORKSPACE.idFromName("synthetic-agent")).fetch(
      request,
    );
  },
};
