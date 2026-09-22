import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { createHash, timingSafeEqual } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { command, identifier, serviceURL } from "../src/protocol.ts";
import { check } from "../src/storage.ts";

function authorized(actual, token) {
  const a = Buffer.from(actual ?? ""),
    b = Buffer.from(`Bearer ${token}`);
  return a.length === b.length && timingSafeEqual(a, b);
}
async function body(req) {
  let size = 0;
  const chunks = [];
  for await (const c of req) {
    size += c.length;
    check(size <= 256 * 1024, "E2BIG");
    chunks.push(c);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
function kill(child) {
  if (!child.pid) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch (e) {
    if (e.code !== "ESRCH") throw e;
  }
}
export async function createSupervisor(config) {
  check(typeof config.token === "string" && config.token.length >= 32);
  let callback;
  if (config.callbackOrigin !== undefined) {
    check(
      typeof config.callbackToken === "string" &&
        config.callbackToken.length >= 32,
    );
    callback = serviceURL(config.callbackOrigin);
    check(
      callback.pathname === "/",
      "EINVAL",
      "callbackOrigin must be an origin",
    );
  }
  check(
    callback ||
      (typeof config.filesystemSocket === "string" &&
        config.filesystemSocket.startsWith("/")),
    "ECONFIG",
    "configure a local filesystemSocket or an explicit HTTP callback origin",
  );
  check(
    process.platform === "linux" || config.development === true,
    "ENOTSUP",
    "production supervisor requires Linux resource limits",
  );
  const runner = resolve(config.runner),
    tools = new Map();
  for (const [name, tool] of Object.entries(config.tools ?? {})) {
    identifier(name);
    const path = resolve(tool.path);
    check(/^[0-9a-f]{64}$/.test(tool.sha256));
    check((await stat(path)).size <= 256 * 1024 * 1024, "EFBIG");
    const hash = createHash("sha256")
      .update(await readFile(path))
      .digest("hex");
    check(hash === tool.sha256, "EINVAL", `digest mismatch: ${name}`);
    tools.set(name, {
      path,
      sha256: hash,
      entrypoint: tool.entrypoint ?? null,
      package: tool.package ?? null,
    });
  }
  check(tools.size > 0);
  const capacity = config.maxConcurrent ?? 2;
  check(Number.isInteger(capacity) && capacity >= 1 && capacity <= 16);
  const jobs = new Map(),
    running = new Map();
  let stopping = false;
  const server = createServer(async (req, res) => {
    const reply = (status, value) => {
      if (!res.destroyed) {
        res.writeHead(status, {
          "content-type": "application/json",
          "cache-control": "no-store",
        });
        res.end(JSON.stringify(value));
      }
    };
    try {
      if (req.url === "/healthz" && req.method === "GET") {
        reply(stopping ? 503 : 200, {
          ready: !stopping,
          active: running.size,
          capacity,
        });
        return;
      }
      if (!authorized(req.headers.authorization, config.token)) {
        reply(401, { error: "unauthorized" });
        return;
      }
      check(req.method === "POST");
      const b = await body(req),
        workspace = identifier(b.workspace),
        id = identifier(b.id);
      check(
        typeof b.token === "string" &&
          b.token.length >= 64 &&
          b.token.length <= 128,
      );
      const key = workspace + ":" + b.token;
      if (req.url === "/v1/cancel") {
        const job = jobs.get(key);
        if (job && job.id === id && !job.done) {
          job.reason = "cancelled";
          kill(job.child);
        }
        reply(200, { ok: true });
        return;
      }
      check(req.url === "/v1/run");
      const cmd = command(b),
        tool = tools.get(cmd.tool);
      check(tool, "ENOENT", "tool not configured");
      let nativeFilesystem;
      if (b.nativeFilesystemScope !== undefined) {
        check(
          typeof config.filesystemSocket === "string" &&
            config.filesystemSocket.startsWith("/"),
          "ECONFIG",
          "native filesystem socket is not configured",
        );
        check(
          typeof b.nativeFilesystemScope === "string" &&
            b.nativeFilesystemScope.length <= 1024 &&
            b.nativeFilesystemScope.endsWith(":" + workspace),
          "EINVAL",
        );
        nativeFilesystem = {
          socket: config.filesystemSocket,
          scope: b.nativeFilesystemScope,
          command: cmd.id,
        };
      }
      check(
        nativeFilesystem || callback,
        "ENOTSUP",
        "HTTP filesystem is not configured",
      );
      const fingerprint = JSON.stringify({ cmd, nativeFilesystem });
      const old = jobs.get(key);
      if (old) {
        check(old.fingerprint === fingerprint, "ECONFLICT");
        reply(200, await old.promise);
        return;
      }
      if (stopping || running.size >= capacity || running.has(workspace)) {
        reply(503, { error: "executor busy" });
        return;
      }
      // Finished entries cover transport retries during the maximum execution
      // lifetime. Durable command deduplication belongs to the DO journal.
      for (const [k, j] of jobs)
        if (j.done && Date.now() - j.finishedAt > 300000) jobs.delete(k);
      if (jobs.size >= 1024) {
        reply(503, { error: "retry cache full" });
        return;
      }
      const child = spawn(runner, [], {
        detached: true,
        stdio: ["pipe", "pipe", "pipe"],
        cwd: "/",
        env: { RUST_BACKTRACE: "0" },
      });
      const job = {
        id,
        fingerprint,
        child,
        reason: null,
        done: false,
        finishedAt: 0,
        promise: null,
      };
      running.set(workspace, job);
      jobs.set(key, job);
      job.promise = new Promise((resolveResult) => {
        let stdout = [],
          total = 0;
        let diagnosticBytes = 0;
        const timer = setTimeout(() => {
          job.reason = "timed_out";
          kill(child);
        }, cmd.timeoutMs);
        const finish = (value) => {
          if (job.done) return;
          job.done = true;
          job.finishedAt = Date.now();
          clearTimeout(timer);
          running.delete(workspace);
          resolveResult(value);
          // WASIX tasks may outlive _start. Kill any remaining process group.
          if (child.pid) kill(child);
        };
        child.stdout.on("data", (chunk) => {
          total += chunk.length;
          if (total > 1024 * 1024) {
            job.reason = "failed";
            kill(child);
          } else stdout.push(chunk);
        });
        // Compiler/trap diagnostics are never relayed unbounded or exposed as
        // guest output. Actual guest stderr is in the structured result.
        child.stderr.on("data", (chunk) => {
          diagnosticBytes += chunk.length;
          if (diagnosticBytes > 65536) {
            job.reason = "failed";
            kill(child);
          } else
            server.emit("runnerDiagnostic", {
              workspace,
              id,
              text: chunk.toString("utf8"),
            });
        });
        child.on("error", () =>
          finish({
            reason: "failed",
            exitCode: 125,
            stdout: "",
            stderr: "runner launch failed",
          }),
        );
        child.on("close", (code) => {
          if (job.reason) {
            finish({
              reason: job.reason,
              exitCode: 124,
              stdout: "",
              stderr: "execution stopped",
            });
            return;
          }
          try {
            const v = JSON.parse(Buffer.concat(stdout).toString("utf8"));
            check(
              code === 0 &&
                Number.isInteger(v.exitCode) &&
                ["exited", "failed"].includes(v.reason) &&
                typeof v.stdout === "string" &&
                typeof v.stderr === "string" &&
                v.stdout.length + v.stderr.length <= 512 * 1024,
            );
            finish(v);
          } catch {
            finish({
              reason: "failed",
              exitCode: 125,
              stdout: "",
              stderr: "runner terminated without a valid result",
            });
          }
        });
        child.stdin.on("error", () => {});
        child.stdin.end(
          JSON.stringify({
            ...cmd,
            module: tool.path,
            sha256: tool.sha256,
            entrypoint: tool.entrypoint,
            packages: [...tools.values()].filter((t) =>
              t.path.endsWith(".webc"),
            ),
            callback: nativeFilesystem
              ? ""
              : new URL(`/v1/workspaces/${workspace}/fs`, callback).href,
            token: b.token,
            callbackToken: nativeFilesystem ? "" : config.callbackToken,
            nativeFilesystem,
            development: config.development === true,
          }),
        );
      });
      reply(200, await job.promise);
    } catch (e) {
      reply(e.code === "ECONFLICT" ? 409 : 400, {
        error: e.message ?? "invalid request",
        code: e.code ?? "EINVAL",
      });
    }
  });
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  server.maxHeadersCount = 32;
  server.keepAliveTimeout = 5000;
  return {
    server,
    async close() {
      stopping = true;
      for (const j of running.values()) {
        j.reason = "cancelled";
        kill(j.child);
      }
      await Promise.all([...running.values()].map((j) => j.promise));
      server.closeAllConnections();
      await new Promise((r) => server.close(r));
    },
  };
}
