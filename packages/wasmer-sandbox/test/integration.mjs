import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import { mkdir, writeFile, readFile, rename, open } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import assert from "node:assert/strict";
import { createSupervisor } from "../service/server.mjs";
const dir = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  repo = resolve(dir, "../..");
const artifacts = resolve(dir, "test/artifacts"),
  work = resolve(artifacts, `run-${Date.now()}`);
await mkdir(work, { recursive: true });
const token = randomBytes(32).toString("hex"),
  callbackToken = randomBytes(32).toString("hex");
const port = Number(process.env.SANDBOX_TEST_PORT ?? 19876),
  helperPort = port + 1,
  url = `http://127.0.0.1:${port}`;
const guest = resolve(dir, "test/guest.wasm"),
  runner =
    process.env.CELLD_WASMER_RUNNER ??
    resolve(dir, "runner/target/debug/celld-wasmer-runner");
const config = {
  development: process.platform !== "linux",
  token,
  callbackToken,
  callbackOrigin: url,
  runner,
  tools: {
    guest: {
      path: guest,
      sha256: createHash("sha256")
        .update(await readFile(guest))
        .digest("hex"),
    },
  },
};
if (process.env.SANDBOX_TEST_TOOLS)
  Object.assign(
    config.tools,
    JSON.parse(await readFile(process.env.SANDBOX_TEST_TOOLS, "utf8")),
  );
const service = await createSupervisor(config);
service.server.on("runnerDiagnostic", (event) =>
  console.error("runner:", event.text),
);
let oldToken;
service.server.prependListener("request", (req) => {
  if (req.url === "/v1/run") {
    let s = "";
    req.on("data", (b) => (s += b));
    req.on("end", () => {
      oldToken = JSON.parse(s).token;
    });
  }
});
service.server.listen(helperPort, "127.0.0.1");
await once(service.server, "listening");
await writeFile(
  resolve(work, "wrangler.json"),
  JSON.stringify({
    name: "sandbox-test",
    main: "worker.ts",
    compatibility_date: "2026-09-01",
    compatibility_flags: ["nodejs_compat"],
    durable_objects: {
      bindings: [{ name: "WORKSPACES", class_name: "Workspace" }],
    },
    migrations: [{ tag: "v1", new_sqlite_classes: ["Workspace"] }],
    vars: {
      SANDBOX_API_TOKEN: token,
      SANDBOX_CALLBACK_TOKEN: callbackToken,
      SANDBOX_SUPERVISOR_TOKEN: token,
      SANDBOX_SUPERVISOR_URL: `http://127.0.0.1:${helperPort}`,
    },
  }),
);
await writeFile(
  resolve(work, "worker.ts"),
  `
import { SandboxWorkspace,routeWorkspace } from ${JSON.stringify(resolve(dir, "src/worker.ts"))};
export class Workspace extends SandboxWorkspace {
 async fetch(req) {
  if(new URL(req.url).pathname.endsWith('/guard')) {
   if(req.headers.get('authorization')!==${JSON.stringify(`Bearer ${token}`)})return new Response('',{status:401});
   const capture=async()=>{try{await this.sandbox.exec({id:'guard',tool:'guest'});return 'UNEXPECTED';}catch(e){return e.message;}};
   const a=await this.ctx.storage.transactionSync(capture);
   const b=await this.ctx.storage.transaction(capture);
   const c=await this.ctx.blockConcurrencyWhile(capture);
   return Response.json([a,b,c]);
  }
  return super.fetch(req);
 }
}
export default {fetch:routeWorkspace};`,
);
let celld;
const results = {
  date: new Date().toISOString(),
  platform: process.platform,
  checks: [],
  commands: [],
};
function record(name) {
  results.checks.push(name);
  console.log("PASS", name);
}
async function start() {
  const log = await open(resolve(work, "celld.log"), "a");
  celld = spawn(
    process.env.CELLD_BIN ?? resolve(repo, "target/debug/celld"),
    ["dev", "--port", String(port), "--logs", "--no-watch"],
    {
      cwd: work,
      detached: true,
      env: {
        ...process.env,
        CELLD_ESBUILD:
          process.env.CELLD_ESBUILD ??
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
    if (celld.exitCode !== null) throw Error("celld exited; inspect " + work);
    try {
      const r = await fetch(url + "/v1/workspaces/test/list", {
        method: "POST",
        headers: { authorization: `Bearer ${token}` },
        body: JSON.stringify({ path: "/workspace" }),
        signal: AbortSignal.timeout(500),
      });
      if (r.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  throw Error("startup timeout: " + work);
}
async function stop() {
  if (celld && celld.exitCode === null) {
    const done = once(celld, "exit");
    let children = [];
    try {
      children = execFileSync("pgrep", ["-P", String(celld.pid)], {
        encoding: "utf8",
      })
        .trim()
        .split(/\s+/)
        .filter(Boolean);
    } catch {}
    for (const child of children) {
      try {
        process.kill(Number(child), "SIGKILL");
      } catch {}
    }
    try {
      process.kill(-celld.pid, "SIGKILL");
    } catch {}
    await done;
  }
  celld = null;
}
async function post(action, body = {}, key = token) {
  return fetch(url + "/v1/workspaces/test/" + action, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(140000),
  });
}
async function call(action, body) {
  const r = await post(action, body);
  const v = await r.json();
  assert.ok(r.ok, JSON.stringify(v));
  return v;
}
async function exec(id, args = [], extra = {}) {
  const v = await call("exec", { id, tool: "guest", args, ...extra });
  results.commands.push(v);
  return v;
}
try {
  await start();
  assert.equal((await post("list", { path: "/workspace" }, "bad")).status, 401);
  assert.equal(
    (
      await post(
        "fs",
        { op: "heartbeat", token: "stale", seq: 1 },
        callbackToken,
      )
    ).status,
    409,
  );
  record("unauthorized control and stale callbacks rejected");
  await call("write", {
    path: "/workspace/input.txt",
    data: Buffer.from("from TypeScript").toString("base64"),
  });
  const v = await exec("basic", [], {
    env: { TEST_VALUE: "visible" },
    stdin: "stdin-data",
  });
  assert.equal(v.status, "succeeded", JSON.stringify(v));
  assert.equal(v.result.stdout, "guest-ok\n");
  assert.equal(v.result.stderr, "guest-stderr\n");
  const read = await call("read", { path: "/workspace/renamed.txt" });
  assert.deepEqual(
    Buffer.from(read.data, "base64"),
    Buffer.concat([Buffer.from("he"), Buffer.alloc(10), Buffer.from("tail")]),
  );
  record(
    "real DO/shared Wasmer sparse I/O, shrink/growth, append, rename, stdin/env/stdout/stderr",
  );
  assert.equal(
    (
      await call("exec", {
        id: "basic",
        tool: "guest",
        env: { TEST_VALUE: "visible" },
        stdin: "stdin-data",
      })
    ).status,
    "succeeded",
  );
  assert.equal(
    (await post("exec", { id: "basic", tool: "guest", args: ["conflict"] }))
      .status,
    409,
  );
  record("durable replay and payload conflict");
  const guards = await call("guard", {});
  assert.equal(guards.length, 3);
  for (const value of guards)
    assert.match(value, /Cannot await cell callbacks/);
  record(
    "real celld transactionSync, transaction and blockConcurrencyWhile deadlock guards",
  );
  assert.equal((await exec("exit", ["exit"])).result.exitCode, 42);
  assert.equal((await exec("trap", ["trap"])).status, "failed");
  assert.equal((await exec("isolation", ["isolation"])).status, "succeeded");
  assert.equal(
    (await exec("timeout", ["wait"], { timeoutMs: 1500 })).status,
    "timed_out",
  );
  assert.equal(
    (await exec("output", ["output"], { timeoutMs: 5000 })).status,
    "failed",
  );
  record(
    "exit status, traps, host/network isolation, CPU timeout and bounded output",
  );
  const pending = exec("cancel", ["wait"]);
  for (let i = 0; i < 100; i++) {
    if ((await call("status", { id: "cancel" }))?.status === "running") break;
    await new Promise((r) => setTimeout(r, 20));
  }
  assert.equal(
    (await post("write", { path: "/workspace/blocked", data: "eA==" })).status,
    409,
  );
  await call("cancel", { id: "cancel" });
  assert.equal((await pending).status, "cancelled");
  record("cancellation fences writes and preserves committed prefix");
  if (config.tools.bash) {
    const b = await call("exec", {
      id: "bash",
      tool: "bash",
      args: [
        "-c",
        "printf 'shell-ok' > /workspace/shell.txt; cat /workspace/shell.txt | wc -c",
      ],
      timeoutMs: 120000,
    });
    results.commands.push(b);
    assert.equal(b.status, "succeeded", JSON.stringify(b));
    assert.equal(
      Buffer.from(
        (await call("read", { path: "/workspace/shell.txt" })).data,
        "base64",
      ).toString(),
      "shell-ok",
    );
    record("Bash, pipeline and coreutils on the durable filesystem");
  }
  if (config.tools.python) {
    const p = await call("exec", {
      id: "python",
      tool: "python",
      args: [
        "-c",
        "import json,pathlib; p=pathlib.Path('/workspace/python.json'); p.write_text(json.dumps({'value':42})); print(p.read_text())",
      ],
      timeoutMs: 120000,
    });
    results.commands.push(p);
    assert.equal(p.status, "succeeded", JSON.stringify(p));
    assert.equal(
      JSON.parse(
        Buffer.from(
          (await call("read", { path: "/workspace/python.json" })).data,
          "base64",
        ).toString(),
      ).value,
      42,
    );
    record("Python stdlib on the durable filesystem");
  }
  await stop();
  await rename(
    resolve(work, ".celld/dev/runtime"),
    resolve(work, ".celld/runtime-before-restart"),
  );
  await start();
  assert.equal(
    (await call("read", { path: "/workspace/renamed.txt" })).data,
    read.data,
  );
  assert.equal((await call("status", { id: "basic" })).status, "succeeded");
  record("LTX restore without the prior runtime directory");
  const removed = await post("unlink", { path: "/workspace/partial" });
  assert.ok(removed.ok || removed.status === 404);
  const interrupted = post("exec", {
    id: "owner-loss",
    tool: "guest",
    args: ["wait"],
    timeoutMs: 30000,
  }).catch(() => null);
  for (let i = 0; i < 100; i++) {
    const r = await post("stat", { path: "/workspace/partial" });
    if (r.ok && (await r.json()).size === 9) break;
    await new Promise((r) => setTimeout(r, 50));
  }
  assert.equal(
    Buffer.from(
      (await call("read", { path: "/workspace/partial" })).data,
      "base64",
    ).toString(),
    "committed",
  );
  const staleToken = oldToken;
  await stop();
  await interrupted;
  await rename(
    resolve(work, ".celld/dev/runtime"),
    resolve(work, ".celld/runtime-before-owner-loss"),
  );
  await start();
  assert.equal(
    (await call("status", { id: "owner-loss" })).status,
    "interrupted",
  );
  assert.equal(
    Buffer.from(
      (await call("read", { path: "/workspace/partial" })).data,
      "base64",
    ).toString(),
    "committed",
  );
  assert.equal(
    (
      await post(
        "fs",
        { op: "heartbeat", token: staleToken, seq: 1 },
        callbackToken,
      )
    ).status,
    409,
  );
  record(
    "owner process loss restores committed prefix, interrupts command and fences former execution",
  );
} finally {
  await stop();
  await service.close();
  await writeFile(
    resolve(work, "results.json"),
    JSON.stringify(results, null, 2),
  );
  console.log("Evidence:", resolve(work, "results.json"));
}
