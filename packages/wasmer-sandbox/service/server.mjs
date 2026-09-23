import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { createHash, timingSafeEqual } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { createResources } from "./resources.mjs";
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
  if (!child?.pid) return;
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
    check(
      tool.public === true,
      "ECONFIG",
      "shared tools must explicitly be public",
    );
    const envAllowlist = tool.envAllowlist ?? [];
    check(
      Array.isArray(envAllowlist) &&
        envAllowlist.length <= 64 &&
        envAllowlist.every(
          (key) =>
            typeof key === "string" &&
            /^[A-Za-z_][A-Za-z0-9_]*$/.test(key) &&
            !/^(CELLD_|SANDBOX_|WASMER_|LD_|DYLD_|NODE_)/.test(key) &&
            ![
              "HOME",
              "TMPDIR",
              "TMP",
              "TEMP",
              "XDG_CACHE_HOME",
              "XDG_CONFIG_HOME",
              "XDG_DATA_HOME",
            ].includes(key),
        ),
      "ECONFIG",
      "invalid guest environment allowlist",
    );
    const path = resolve(tool.path);
    check(/^[0-9a-f]{64}$/.test(tool.sha256));
    check((await stat(path)).size <= 256 * 1024 * 1024, "EFBIG");
    const hash = createHash("sha256")
      .update(await readFile(path))
      .digest("hex");
    check(hash === tool.sha256, "EINVAL", `digest mismatch: ${name}`);
    tools.set(name, {
      path,
      envAllowlist,
      sha256: hash,
      entrypoint: tool.entrypoint ?? null,
      package: tool.package ?? null,
    });
  }
  check(tools.size > 0 && tools.size <= 32);
  const capacity = config.maxConcurrent ?? 2;
  check(Number.isInteger(capacity) && capacity >= 1 && capacity <= 16);
  const jobs = new Map(),
    running = new Map();
  const resources = await createResources(config);
  let stopping = false,
    cacheBytes = 0;
  const cacheLimit = 16 * 1024 * 1024;
  const hash = (value) =>
    createHash("sha256").update(JSON.stringify(value)).digest("hex");
  function expire() {
    for (const [key, job] of jobs)
      if (job.done && Date.now() - job.finishedAt >= 300000) {
        cacheBytes -= job.bytes;
        jobs.delete(key);
      }
  }
  const reaper = setInterval(expire, 5000);
  reaper.unref();
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
          perCommandResources: resources.isolated,
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
      const key = hash([workspace, id, b.token]);
      if (req.url === "/v1/cancel") {
        const job = jobs.get(key);
        if (job && job.id === id && !job.done) {
          job.reason = "cancelled";
          job.stop("cancelled");
        }
        reply(200, { ok: true });
        return;
      }
      check(req.url === "/v1/run");
      const cmd = command(b),
        tool = tools.get(cmd.tool);
      check(tool, "ENOENT", "tool not configured");
      check(
        Object.keys(cmd.env).every((key) => tool.envAllowlist.includes(key)),
        "EACCES",
        "guest environment key is not allowed for this tool",
      );
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
      // Only hashes survive in retry metadata; stdin, env and credentials are
      // not retained as a serialized command in the shared cache.
      const fingerprint = hash({ cmd, nativeFilesystem });
      expire();
      const old = jobs.get(key);
      if (old) {
        check(old.fingerprint === fingerprint, "ECONFLICT");
        if (old.expired) {
          reply(410, {
            error: "retry result expired; use the workspace journal",
          });
          return;
        }
        reply(200, await old.promise);
        return;
      }
      if (stopping || running.size >= capacity || running.has(workspace)) {
        reply(503, { error: "executor busy" });
        return;
      }
      if (jobs.size >= 128) {
        reply(503, { error: "retry cache full" });
        return;
      }
      let resolveResult;
      const job = {
        id,
        fingerprint,
        child: null,
        reason: null,
        done: false,
        finishedAt: 0,
        bytes: 0,
        expired: false,
        promise: new Promise((resolve) => {
          resolveResult = resolve;
        }),
        stop(reason) {
          this.reason ??= reason;
          kill(this.child);
        },
      };
      // Reserve admission before the first allocation await. Cancellation and
      // shutdown also see commands that have not spawned yet.
      running.set(workspace, job);
      jobs.set(key, job);
      const timer = setTimeout(() => job.stop("timed_out"), cmd.timeoutMs);
      let allocation,
        finishing = false,
        stdout = [],
        total = 0,
        diagnosticBytes = 0;
      async function finish(value) {
        if (finishing) return;
        finishing = true;
        clearTimeout(timer);
        kill(job.child);
        try {
          await allocation?.cleanup();
        } catch {
          stopping = true; // Uncertain teardown must not admit more commands.
          value = {
            reason: "failed",
            exitCode: 125,
            stdout: "",
            stderr: "runner cleanup failed",
          };
        }
        if (job.reason)
          value = {
            reason: job.reason,
            exitCode: 124,
            stdout: "",
            stderr: "execution stopped",
          };
        job.done = true;
        job.finishedAt = Date.now();
        running.delete(workspace);
        stdout = [];
        job.child = null;
        job.bytes = Buffer.byteLength(JSON.stringify(value));
        cacheBytes += job.bytes;
        resolveResult(value);
        resolveResult = null;
        // Keep tombstones when results are evicted: retries must never launch
        // another command just because private output left the memory cache.
        for (const old of jobs.values()) {
          if (cacheBytes <= cacheLimit) break;
          if (!old.done || old.expired) continue;
          cacheBytes -= old.bytes;
          old.bytes = 0;
          old.expired = true;
          old.promise = null;
        }
        if (diagnosticBytes)
          server.emit("runnerDiagnostic", {
            workspace,
            execution: allocation?.name,
            bytes: diagnosticBytes,
            event: "runner_diagnostic_discarded",
          });
      }
      void (async () => {
        try {
          allocation = await resources.allocate();
          if (job.reason) {
            await finish(null);
            return;
          }
          const child = (job.child = spawn(runner, [], {
            detached: true,
            stdio: ["pipe", "pipe", "pipe"],
            cwd: allocation.directory,
            env: allocation.env,
          }));
          child.stdout.on("data", (chunk) => {
            total += chunk.length;
            if (total > 1024 * 1024) {
              stdout = [];
              job.stop("failed");
            } else stdout.push(chunk);
          });
          child.stderr.on("data", (chunk) => {
            diagnosticBytes += chunk.length;
            if (diagnosticBytes > 65536) job.stop("failed");
            // Raw trap/compiler output can contain user data and never enters
            // shared logs. Guest stderr is available only in its scoped result.
          });
          child.on("error", () => {
            void finish({
              reason: "failed",
              exitCode: 125,
              stdout: "",
              stderr: "runner launch failed",
            });
          });
          child.on("close", (code) => {
            let value = {
              reason: "failed",
              exitCode: 125,
              stdout: "",
              stderr: "runner terminated without a valid result",
            };
            if (!job.reason)
              try {
                const v = JSON.parse(Buffer.concat(stdout).toString("utf8"));
                check(
                  code === 0 &&
                    Number.isInteger(v.exitCode) &&
                    ["exited", "failed"].includes(v.reason) &&
                    typeof v.stdout === "string" &&
                    typeof v.stderr === "string" &&
                    Buffer.byteLength(v.stdout) + Buffer.byteLength(v.stderr) <=
                      512 * 1024,
                );
                // Do not accidentally publish future runner-private fields.
                value = {
                  reason: v.reason,
                  exitCode: v.exitCode,
                  stdout: v.stdout,
                  stderr: v.stderr,
                };
                for (const field of ["fsCalls", "statCalls"])
                  if (
                    v[field] &&
                    ["http", "native"].every(
                      (k) =>
                        Number.isSafeInteger(v[field][k]) && v[field][k] >= 0,
                    )
                  )
                    value[field] = {
                      http: v[field].http,
                      native: v[field].native,
                    };
              } catch {}
            void finish(value);
          });
          child.stdin.on("error", () => {});
          await allocation.attach(child.pid);
          if (finishing || job.reason) {
            kill(child);
            return;
          }
          child.stdin.end(
            JSON.stringify({
              ...cmd,
              supervisorPid: process.pid,
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
        } catch (error) {
          if (error.code === "ECLEANUP") stopping = true;
          if (job.child) job.stop("failed");
          else
            await finish({
              reason: "failed",
              exitCode: 125,
              stdout: "",
              stderr: "runner preparation failed",
            });
        }
      })();
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
  server.maxConnections = 128;
  return {
    server,
    async close() {
      stopping = true;
      clearInterval(reaper);
      for (const j of running.values()) {
        j.stop("cancelled");
      }
      await Promise.all([...running.values()].map((j) => j.promise));
      server.closeAllConnections();
      await new Promise((r) => server.close(r));
      jobs.clear();
      await resources.close();
    },
  };
}
