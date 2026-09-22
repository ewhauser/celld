import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import {
  mkdir,
  writeFile,
  readFile,
  rename,
  open,
  mkdtemp,
  rm,
  chmod,
} from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import assert from "node:assert/strict";
import { authFixture } from "./auth-fixture.mjs";
import { createSupervisor } from "../service/server.mjs";
const dir = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  repo = resolve(dir, "../..");
const artifacts = resolve(dir, "test/artifacts"),
  work = resolve(artifacts, `run-${Date.now()}`);
await mkdir(work, { recursive: true });
const nativeFilesystem = process.env.SANDBOX_NATIVE_FILESYSTEM === "1";
const socketDir = nativeFilesystem
  ? await mkdtemp("/tmp/celld-ipc-")
  : undefined;
if (socketDir) await chmod(socketDir, 0o700);
const socketPath = socketDir && resolve(socketDir, "fs.sock");
const auth = await authFixture();
const agentToken = await auth.sign();
let workspaceId;
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
  filesystemSocket: socketPath,
  development: process.platform !== "linux",
  token,
  ...(nativeFilesystem && !process.env.SANDBOX_BENCH
    ? {}
    : { callbackToken, callbackOrigin: url }),
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
      SANDBOX_NATIVE_FILESYSTEM: nativeFilesystem ? "1" : "0",
      ...auth.vars,
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
import { WasmerSandbox } from ${JSON.stringify(resolve(dir, "src/index.ts"))};
import { authorizedWorkspace } from ${JSON.stringify(resolve(dir, "src/authorization.ts"))};
export class Workspace extends SandboxWorkspace {
 async fetch(req) {
  const action=new URL(req.url).pathname.split('/').pop();
  if(['bench','native-register','native-revoke','native-busy','native-app-checks','native-app-close'].includes(action)) {
   if((await authorizedWorkspace(req,this.env)).toString()!==this.ctx.id.toString())return new Response('',{status:401});
   const body=await req.json();
   if(action==='native-app-checks') {
    const fs=this.sandbox.fs;
    fs.writeFile('/workspace/rollback',new Uint8Array([1,2,3]));
    try {this.ctx.storage.transactionSync(()=>{fs.writeFile('/workspace/rollback',new Uint8Array([4]));throw Error('rollback');});}catch(e){if(e.message!=='rollback')throw e;}
    if(fs.readFile('/workspace/rollback').join(',')!=='1,2,3')throw Error('native nested rollback failed');
    const blocked=this.ctx.storage.transactionSync(()=>JSON.parse(this.ctx.agentFsOperation({op:'open',path:'/workspace/rollback',read:true})));
    if(blocked.code!=='EBUSY')throw Error('handle escaped rollback domain');
    return Response.json({handle:fs.open('/workspace/app-handle',{read:true,write:true,create:true})});
   }
   if(action==='native-app-close') {this.sandbox.fs.fstat(body.handle);this.sandbox.fs.close(body.handle);return Response.json({ok:true});}
   if(action==='native-register') return Response.json({scope:this.ctx.agentFsCapability(body.token, Date.now()+(body.ttl ?? 10000))});
   if(action==='native-revoke') {this.ctx.agentFsCapability(null);return Response.json({ok:true});}
   if(action==='native-busy') {await this.ctx.blockConcurrencyWhile(async()=>{await new Promise(r=>setTimeout(r,500));});return Response.json({ok:true});}
   const sandbox=new WasmerSandbox(this.ctx, {workspace:this.ctx.id.toString(),supervisorURL:this.env.SANDBOX_SUPERVISOR_URL,supervisorToken:this.env.SANDBOX_SUPERVISOR_TOKEN,nativeFilesystem:body.native});
   const prior=this.sandbox;this.sandbox=sandbox;
   try {return Response.json(await sandbox.exec({id:body.id,tool:'guest',args:['stat-bench',String(body.count)],timeoutMs:120000}));}
   finally {this.sandbox=prior;}
  }
  if(new URL(req.url).pathname.endsWith('/guard')) {
   if((await authorizedWorkspace(req,this.env)).toString()!==this.ctx.id.toString())return new Response('',{status:401});
   const capture=async()=>{try{await this.sandbox.exec({id:'guard',tool:'guest'});return 'UNEXPECTED';}catch(e){return e.message;}};
   const a=await this.ctx.storage.transactionSync(capture);
   const b=await this.ctx.storage.transaction(capture);
   const c=await this.ctx.blockConcurrencyWhile(capture);
   return Response.json([a,b,c]);
  }
  return super.fetch(req);
 }
}
export default {async fetch(req,env) {
 const u=new URL(req.url),p=u.pathname.split('/');
 if(p[1]==='__auth-id') {try{return Response.json({id:(await authorizedWorkspace(new Request(new URL('/v1/workspaces/test/list',u),req),env)).toString()});}catch{return new Response('',{status:401});}}
 if(p[1]==='__direct') {const target=p[2];u.pathname='/v1/workspaces/'+p[3]+'/'+p[4];return env.WORKSPACES.get(env.WORKSPACES.idFromString(target)).fetch(new Request(u,req));}
 return routeWorkspace(req,env);
}};`,
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
  if (socketPath) await rm(socketPath, { force: true });
  const log = await open(resolve(work, "celld.log"), "a");
  celld = spawn(
    process.env.CELLD_BIN ?? resolve(repo, "target/debug/celld"),
    ["dev", "--port", String(port), "--logs", "--no-watch"],
    {
      cwd: work,
      detached: true,
      env: {
        ...process.env,
        ...(socketPath ? { CELLD_AGENTFS_SOCKET: socketPath } : {}),
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
        headers: { authorization: `Bearer ${await auth.sign()}` },
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
async function post(action, body = {}, key = undefined) {
  return fetch(
    url + `/v1/workspaces/${action === "fs" ? workspaceId : "test"}/` + action,
    {
      method: "POST",
      headers: { authorization: `Bearer ${key ?? (await auth.sign())}` },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(140000),
    },
  );
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
  workspaceId = (
    await (
      await fetch(url + "/__auth-id", {
        headers: { authorization: `Bearer ${await auth.sign()}` },
      })
    ).json()
  ).id;
  const { authorizationChecks } = await import(
    "./authorization-integration.mjs"
  );
  await authorizationChecks({
    url,
    auth,
    agentToken,
    workspaceId,
    token,
    callbackToken,
    record,
  });
  assert.equal((await post("list", { path: "/workspace" }, "bad")).status, 401);
  assert.equal(
    (
      await post(
        "fs",
        { op: "heartbeat", token: "stale", seq: 1 },
        callbackToken,
      )
    ).status,
    nativeFilesystem ? 401 : 409,
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
  assert.ok(v.result.fsCalls[nativeFilesystem ? "native" : "http"] > 0);
  assert.equal(v.result.fsCalls[nativeFilesystem ? "http" : "native"], 0);
  assert.ok(v.result.statCalls[nativeFilesystem ? "native" : "http"] > 0);
  assert.equal(v.result.statCalls[nativeFilesystem ? "http" : "native"], 0);
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
  const quota = await exec("quota", ["quota"]);
  assert.equal(quota.status, "succeeded", JSON.stringify(quota));
  assert.equal(quota.result.stdout, "quota-preserved\n");
  record(
    "temporary quota rejects repeated/huge growth atomically and reclaims capacity",
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
  if (nativeFilesystem) {
    const { ipcChecks, benchmark } = await import("./native-filesystem.mjs");
    await ipcChecks({
      socketPath,
      call,
      post,
      token: randomBytes(32).toString("hex"),
      record,
    });
    if (process.env.SANDBOX_BENCH === "1")
      results.benchmark = await benchmark(call);
  }
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
  if (config.tools.coreutils) {
    const c = await call("exec", {
      id: "coreutils-direct",
      tool: "coreutils",
      args: ["explicit-entrypoint"],
      timeoutMs: 120000,
    });
    results.commands.push(c);
    assert.equal(c.status, "succeeded", JSON.stringify(c));
    assert.equal(c.result.stdout, "explicit-entrypoint\n");
    record("explicit coreutils entrypoint on a multi-command package");
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
  let restartCapability;
  if (nativeFilesystem) {
    restartCapability = { token: randomBytes(32).toString("hex") };
    Object.assign(
      restartCapability,
      await call("native-register", restartCapability),
    );
    const { connect } = await import("./native-filesystem.mjs");
    const c = await connect(socketPath);
    assert.equal(
      (await c.call(restartCapability.scope, restartCapability.token, 1)).code,
      undefined,
    );
    c.socket.destroy();
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
  if (restartCapability) {
    const { connect } = await import("./native-filesystem.mjs");
    const c = await connect(socketPath);
    assert.equal(
      (await c.call(restartCapability.scope, restartCapability.token, 2)).code,
      "ESTALE",
    );
    c.socket.destroy();
    record(
      "native capability is not resurrected by process restart or LTX restore",
    );
  }
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
    nativeFilesystem ? 401 : 409,
  );
  record(
    "owner process loss restores committed prefix, interrupts command and fences former execution",
  );
} finally {
  await stop();
  await service.close();
  if (socketDir) await rm(socketDir, { recursive: true, force: true });
  await writeFile(
    resolve(work, "results.json"),
    JSON.stringify(results, null, 2),
  );
  console.log("Evidence:", resolve(work, "results.json"));
}
