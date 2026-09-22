import { spawn, execFileSync } from "node:child_process";
import { createServer } from "node:http";
import { once } from "node:events";
import { writeFile, open, rename } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
const dir = dirname(fileURLToPath(import.meta.url));
const url = "http://127.0.0.1:19876";
const results = {
  date: new Date().toISOString(),
  platform: process.platform,
  arch: process.arch,
  runs: [],
};
let celld;
let lastToken;
const runners = new Set();
const helper = createServer(async (req, res) => {
  if (req.url !== "/run" || req.method !== "POST") {
    res.writeHead(404).end();
    return;
  }
  let text = "";
  for await (const chunk of req) text += chunk;
  const { token, mode } = JSON.parse(text);
  lastToken = token;
  const t = performance.now();
  const child = spawn(
    resolve(dir, "native/target/debug/agentfs-wasmer-probe"),
    [url + "/fs", token, resolve(dir, "guest.wasm"), mode],
    { cwd: dir },
  );
  runners.add(child);
  let stdout = "",
    stderr = "";
  child.stdout.on("data", (b) => (stdout += b));
  child.stderr.on("data", (b) => (stderr += b));
  let killed = false;
  const timer = setTimeout(
    () => {
      killed = true;
      child.kill("SIGKILL");
    },
    mode === "cancel" || mode === "owner" ? 5000 : 20000,
  );
  child.on("close", (code, signal) => {
    clearTimeout(timer);
    runners.delete(child);
    res.setHeader("content-type", "application/json");
    res.end(
      JSON.stringify({
        code,
        signal,
        killed,
        stdout,
        stderr,
        elapsedMs: performance.now() - t,
      }),
    );
  });
});
async function call(path, body) {
  const r = await fetch(url + path, {
    method: body ? "POST" : "GET",
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(30000),
  });
  const v = await r.json();
  if (!r.ok) throw Error(JSON.stringify(v));
  return v;
}
async function start() {
  const log = await open(resolve(dir, ".celld-probe.log"), "a");
  const t = performance.now();
  celld = spawn(
    process.env.CELLD_BIN || resolve(dir, "../../target/debug/celld"),
    ["dev", "--port", "19876", "--logs", "--no-watch"],
    {
      cwd: dir,
      env: {
        ...process.env,
        CELLD_ESBUILD:
          process.env.CELLD_ESBUILD ||
          resolve(
            dir,
            `node_modules/@esbuild/${process.platform}-${process.arch}/bin/esbuild`,
          ),
      },
      stdio: ["ignore", log.fd, log.fd],
    },
  );
  await log.close();
  for (let i = 0; i < 200; i++) {
    if (celld.exitCode !== null)
      throw Error("celld exited; see .celld-probe.log");
    try {
      const r = await fetch(url + "/inspect", {
        signal: AbortSignal.timeout(500),
      });
      if (r.status !== 503) {
        results.activationMs ??= performance.now() - t;
        return;
      }
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  throw Error("celld startup timeout");
}
async function stop(signal = "SIGINT") {
  if (celld && celld.exitCode === null) {
    const p = once(celld, "exit");
    if (signal === "SIGKILL") {
      const ids = execFileSync("pgrep", ["-P", String(celld.pid)], {
        encoding: "utf8",
      })
        .trim()
        .split(/\s+/);
      for (const id of ids) process.kill(Number(id), "SIGKILL");
    }
    celld.kill(signal);
    await p;
  }
  celld = undefined;
}
try {
  helper.listen(19877, "127.0.0.1");
  await once(helper, "listening");
  await start();
  await call("/init", {});
  const nonce = Date.now();
  for (let i = 0; i < 3; i++) {
    const v = await call("/exec", { id: `${nonce}-basic-${i}`, mode: "basic" });
    results.runs.push(v);
    assert.equal(v.code, 0, JSON.stringify(v));
    const check = await call("/inspect");
    assert.equal(check.files["output.txt"].text, "HELLO from Wasmer\nappend\n");
    assert.equal(check.files["large-out.bin"].bytes, 256 * 1024);
  }
  results.payloadConflictStatus = (
    await fetch(url + "/exec", {
      method: "POST",
      body: JSON.stringify({ id: `${nonce}-basic-0`, mode: "edge" }),
    })
  ).status;
  assert.equal(results.payloadConflictStatus, 409);
  results.replay = await call("/exec", {
    id: `${nonce}-basic-0`,
    mode: "basic",
  });
  assert.equal(results.replay.recovered, true);
  results.edge = await call("/exec", { id: `${nonce}-edge`, mode: "edge" });
  results.edgeState = await call("/inspect");
  assert.notEqual(
    results.edge.code,
    0,
    "the known truncate defect unexpectedly changed; inspect the result",
  );
  assert.equal(results.edgeState.files["output.txt"].size, 32);
  assert.equal(results.edgeState.files["output.txt"].bytes, 25);
  results.cancel = await call("/exec", {
    id: `${nonce}-cancel`,
    mode: "cancel",
  });
  assert.equal(results.cancel.killed, true);
  const before = await call("/inspect");
  assert.equal(before.files["partial.txt"].text, "committed prefix");
  results.staleStatus = (
    await fetch(url + "/fs", {
      method: "POST",
      body: JSON.stringify({
        token: "old-token",
        op: "stat",
        path: "/workspace/input.txt",
      }),
    })
  ).status;
  assert.equal(results.staleStatus, 409);
  await stop("SIGKILL");
  await rename(
    resolve(dir, ".celld/dev/runtime"),
    resolve(dir, `.celld/runtime-before-crash-${nonce}`),
  );
  await start();
  results.restoredWithoutRuntimeDirectory = true;
  results.afterRestart = await call("/inspect");
  assert.deepEqual(results.afterRestart.files, before.files);
  results.restartVerified = true;
  const ownerId = `${nonce}-owner`;
  const pending = call("/exec", { id: ownerId, mode: "owner" }).catch((e) => ({
    lostReply: e.message,
  }));
  let inflight;
  for (let i = 0; i < 100; i++) {
    inflight = await call("/inspect");
    if (inflight.active && inflight.files["owner-partial.txt"]) break;
    await new Promise((r) => setTimeout(r, 50));
  }
  assert.equal(inflight.files["owner-partial.txt"].text, "committed prefix");
  const oldToken = lastToken;
  await stop("SIGKILL");
  await rename(
    resolve(dir, ".celld/dev/runtime"),
    resolve(dir, `.celld/runtime-mid-command-${nonce}`),
  );
  await start();
  results.ownerLostReply = await pending;
  results.afterOwnerLoss = await call("/inspect");
  assert.equal(
    results.afterOwnerLoss.commands.find((c) => c.id === ownerId).status,
    "interrupted",
  );
  assert.equal(
    results.afterOwnerLoss.files["owner-partial.txt"].text,
    "committed prefix",
  );
  results.oldExecutionStatus = (
    await fetch(url + "/fs", {
      method: "POST",
      body: JSON.stringify({
        token: oldToken,
        op: "stat",
        path: "/workspace/input.txt",
      }),
    })
  ).status;
  assert.equal(results.oldExecutionStatus, 409);
  results.ownerInterruptionVerified = true;
  await writeFile(
    resolve(dir, "results-local.json"),
    JSON.stringify(results, null, 2) + "\n",
  );
  console.log(
    JSON.stringify(
      {
        runs: results.runs,
        edge: results.edgeState.files["output.txt"],
        restartVerified: results.restartVerified,
        restoredWithoutRuntimeDirectory:
          results.restoredWithoutRuntimeDirectory,
      },
      null,
      2,
    ),
  );
} finally {
  for (const c of runners) c.kill("SIGKILL");
  await stop();
  helper.closeAllConnections();
  helper.close();
}
