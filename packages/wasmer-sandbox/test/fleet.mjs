// Local qualification: three real celld nodes, an isolated MinIO container,
// native helper, follower-only acknowledgement and former-owner suspension.
import { spawn, execFileSync } from "node:child_process";
import { createServer, request as httpRequest } from "node:http";
import {
  mkdir,
  writeFile,
  readFile,
  open,
  rm,
  mkdtemp,
  chmod,
} from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { once } from "node:events";
import { randomBytes, createHash } from "node:crypto";
import assert from "node:assert/strict";
import { authFixture } from "./auth-fixture.mjs";
import { createSupervisor } from "../service/server.mjs";
const dir = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  repo = resolve(dir, "../..");
const work = resolve(dir, `test/artifacts/fleet-${Date.now()}`);
await mkdir(work, { recursive: true });
const name = `celld-wasmer-fleet-${Date.now()}`,
  port = Number(process.env.SANDBOX_FLEET_PORT ?? 19970),
  s3port = port + 2;
const auth = await authFixture();
const token = randomBytes(32).toString("hex"),
  url = `http://127.0.0.1:${port}`,
  endpoint = `http://127.0.0.1:${s3port}`;
const bin = process.env.CELLD_BIN ?? resolve(repo, "target/debug/celld");
const env = {
  ...process.env,
  AWS_ACCESS_KEY_ID: "sandbox-test",
  AWS_SECRET_ACCESS_KEY: token,
  AWS_REGION: "us-east-1",
  AWS_ALLOW_HTTP: "true",
  CELLD_ESBUILD:
    process.env.CELLD_ESBUILD ??
    resolve(
      dir,
      `node_modules/@esbuild/${process.platform}-${process.arch}/bin/esbuild`,
    ),
  RUST_LOG: "warn,cell_console=info",
  CELLD_READY_FLEET_GATE_MS: "0",
  CELLD_REBALANCE_INTERVAL_MS: "0",
  CELLD_TOKIO_THREADS: "2",
};
const fleetArgs = ["--bucket", "s3://sandbox-test", "--endpoint", endpoint];
const guest = resolve(dir, "test/guest.wasm"),
  runner =
    process.env.CELLD_WASMER_RUNNER ??
    resolve(dir, "runner/target/debug/celld-wasmer-runner");
const nativeFilesystem = process.env.SANDBOX_NATIVE_FILESYSTEM_FLEET === "1";
const socketDir = nativeFilesystem
  ? await mkdtemp("/tmp/celld-fleet-ipc-")
  : undefined;
if (socketDir) await chmod(socketDir, 0o700);
const nodes = [];
let supervisor,
  proxy,
  paused = false,
  oldToken,
  workspaceId;
const evidence = { date: new Date().toISOString(), checks: [], states: [] };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function docker(...args) {
  return execFileSync("docker", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}
async function post(action, body = {}, timeout = 30000) {
  return fetch(
    url + `/v1/workspaces/${action === "fs" ? workspaceId : "test"}/` + action,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${action === "fs" ? token : await auth.sign()}`,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeout),
    },
  );
}
async function call(action, body, timeout) {
  const r = await post(action, body, timeout),
    text = await r.text();
  assert.ok(r.ok, `${r.status}: ${text}`);
  return JSON.parse(text);
}
async function eventually(action, body) {
  const deadline = Date.now() + 60000;
  let last;
  while (Date.now() < deadline) {
    try {
      return await call(action, body, 10000);
    } catch (e) {
      last = e;
      await sleep(250);
    }
  }
  throw last;
}
async function owner() {
  for (const n of nodes.filter((n) => n.route)) {
    const r = await fetch(`http://127.0.0.1:${n.internal}/state`);
    const state = await r.json();
    evidence.states.push({ node: n.index, state });
    if (state.residents.some((s) => s.startsWith("SandboxWorkspace:")))
      return n;
  }
  throw Error("owner missing");
}
function pass(name) {
  evidence.checks.push(name);
  console.log("PASS", name);
}
async function terminate(n) {
  if (n.process.exitCode === null && n.process.signalCode === null) {
    const done = once(n.process, "exit");
    n.process.kill("SIGKILL");
    await done;
  }
  n.route = false;
}
try {
  docker(
    "run",
    "-d",
    "--name",
    name,
    "-p",
    `127.0.0.1:${s3port}:9000`,
    "-e",
    "MINIO_ROOT_USER=sandbox-test",
    "-e",
    `MINIO_ROOT_PASSWORD=${token}`,
    "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z",
    "server",
    "/data",
  );
  for (let i = 0; i < 100; i++) {
    try {
      if ((await fetch(endpoint + "/minio/health/live")).ok) break;
    } catch {}
    await sleep(100);
  }
  docker(
    "run",
    "--rm",
    "--network",
    `container:${name}`,
    "--entrypoint",
    "/bin/sh",
    "quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z",
    "-c",
    `mc alias set test http://127.0.0.1:9000 sandbox-test ${token} >/dev/null && mc mb test/sandbox-test`,
  );
  await writeFile(
    resolve(work, "wrangler.json"),
    JSON.stringify({
      name: "sandbox-test",
      main: "worker.ts",
      compatibility_date: "2026-09-01",
      compatibility_flags: ["nodejs_compat"],
      durable_objects: {
        bindings: [{ name: "WORKSPACES", class_name: "SandboxWorkspace" }],
      },
      migrations: [{ tag: "v1", new_sqlite_classes: ["SandboxWorkspace"] }],
      vars: {
        ...auth.vars,
        SANDBOX_NATIVE_FILESYSTEM: "0", // The shared executor is a remote HTTP reference backend.
        SANDBOX_CALLBACK_TOKEN: token,
        SANDBOX_SUPERVISOR_TOKEN: token,
        SANDBOX_SUPERVISOR_URL: `http://127.0.0.1:${port + 1}`,
      },
    }),
  );
  await writeFile(
    resolve(work, "worker.ts"),
    `import {SandboxWorkspace as Base,routeWorkspace} from ${JSON.stringify(resolve(dir, "src/worker.ts"))};
import {authorizedWorkspace} from ${JSON.stringify(resolve(dir, "src/authorization.ts"))};
export class SandboxWorkspace extends Base {
 async fetch(req) {
  const action=new URL(req.url).pathname.split('/').pop();
  if(['native-register','native-mutate'].includes(action)) {
   if((await authorizedWorkspace(req,this.env)).toString()!==this.ctx.id.toString())return new Response('',{status:401});
   const body=await req.json();
   if(action==='native-register') { this.ctx.agentFsOperation({op:'configure',limits:{maxBytes:67108864,maxFileBytes:16777216,maxInodes:4096,maxHandles:128}}); return Response.json({scope:this.ctx.agentFsCapability(body.token,Date.now()+120000,body.command ?? 'integration-test')}); }
   this.sandbox.fs.writeFile('/workspace/native-gate', new Uint8Array(13));
   console.log('NATIVE_GATE_WRITE_COMPLETE');
   await new Promise(r=>setTimeout(r,2000));
   return Response.json({ok:true});
  }
  return super.fetch(req);
 }
}
export default {fetch:routeWorkspace};`,
  );
  execFileSync(bin, ["deploy", work, ...fleetArgs], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  proxy = createServer((req, res) => {
    const node = nodes.find((n) => n.route);
    if (!node) {
      res.writeHead(503).end();
      return;
    }
    const outgoing = httpRequest(
      {
        hostname: "127.0.0.1",
        port: node.port,
        path: req.url,
        method: req.method,
        headers: { ...req.headers, host: "sandbox-test" },
      },
      (upstream) => {
        res.writeHead(upstream.statusCode, upstream.headers);
        upstream.pipe(res);
      },
    );
    outgoing.on("error", () => {
      if (!res.headersSent) res.writeHead(503);
      res.end();
    });
    req.pipe(outgoing);
    res.on("close", () => outgoing.destroy());
  });
  proxy.listen(port, "127.0.0.1");
  await once(proxy, "listening");
  supervisor = await createSupervisor({
    development: process.platform !== "linux",
    token,
    callbackToken: token,
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
  });
  supervisor.server.prependListener("request", (req) => {
    if (req.url === "/v1/run") {
      let s = "";
      req.on("data", (b) => (s += b));
      req.on("end", () => {
        oldToken = JSON.parse(s).token;
        workspaceId = JSON.parse(s).workspace;
      });
    }
  });
  supervisor.server.listen(port + 1, "127.0.0.1");
  await once(supervisor.server, "listening");
  for (let i = 0; i < 3; i++) {
    const node = {
      index: i,
      port: port + 10 + i * 2,
      internal: port + 11 + i * 2,
      route: true,
      process: null,
      path: resolve(work, `node-${i}`),
    };
    await mkdir(node.path);
    const log = await open(resolve(work, `node-${i}.log`), "a");
    node.process = spawn(
      bin,
      [
        ...fleetArgs,
        "--listen",
        `127.0.0.1:${node.port}`,
        "--internal-listen",
        `127.0.0.1:${node.internal}`,
        "--advertise",
        `127.0.0.1:${node.internal}`,
      ],
      {
        env: {
          ...env,
          ...(socketDir
            ? {
                CELLD_AGENTFS_SOCKET: resolve(socketDir, `${i}.sock`),
              }
            : {}),
          CELLD_WATCH: node.path,
          CELLD_NODE: `sandbox-${Date.now()}-${i}`,
        },
        stdio: ["ignore", log.fd, log.fd],
      },
    );
    await log.close();
    nodes.push(node);
  }
  for (let i = 0; i < 200; i++) {
    try {
      if ((await post("list", { path: "/workspace" }, 1000)).ok) break;
    } catch {}
    await sleep(100);
  }
  await call("write", {
    path: "/workspace/input.txt",
    data: Buffer.from("from TypeScript").toString("base64"),
  });
  const completed = await call("exec", {
    id: "completed",
    tool: "guest",
    env: { TEST_VALUE: "visible" },
    stdin: "stdin-data",
  });
  assert.equal(completed.status, "succeeded", JSON.stringify(completed));
  await sleep(4000); // allow all three node leases to be discovered
  const first = await owner();
  let nativeCapability;
  if (nativeFilesystem) {
    nativeCapability = { token: randomBytes(32).toString("hex") };
    Object.assign(
      nativeCapability,
      await call("native-register", nativeCapability),
    );
    const { connect } = await import("./native-filesystem.mjs");
    const c = await connect(resolve(socketDir, `${first.index}.sock`));
    assert.equal(
      (await c.call(nativeCapability.scope, nativeCapability.token, 1)).code,
      undefined,
    );
    c.socket.destroy();
  }
  let followerPath;
  evidence.followerAttempts = [];
  for (let attempt = 1; attempt <= 3; attempt++) {
    // Warm the existing follower stream immediately before pausing the bucket.
    await call("write", {
      path: "/workspace/warm",
      data: Buffer.from(String(attempt)).toString("base64"),
    });
    await sleep(250);
    docker("pause", name);
    paused = true;
    try {
      const path = `/workspace/follower-only-${attempt}`;
      await call(
        "write",
        {
          path,
          data: Buffer.from("acknowledged with bucket paused").toString(
            "base64",
          ),
        },
        4000,
      );
      followerPath = path;
      evidence.followerAttempts.push({ attempt, acknowledged: true });
      pass("filesystem write acknowledged while object store was paused");
      await terminate(first);
      await rm(first.path, { recursive: true, force: true });
      break;
    } catch (e) {
      evidence.followerAttempts.push({
        attempt,
        acknowledged: false,
        error: e.message,
      });
    } finally {
      docker("unpause", name);
      paused = false;
    }
    await sleep(4000);
  }
  assert.ok(followerPath, "no follower-only acknowledgement was observed");
  const restored = await eventually("read", { path: followerPath });
  assert.equal(
    Buffer.from(restored.data, "base64").toString(),
    "acknowledged with bucket paused",
  );
  assert.equal((await call("status", { id: "completed" })).status, "succeeded");
  pass(
    "another owner restores acknowledged state after prior owner and disk loss",
  );
  if (nativeFilesystem) {
    const current = await owner(),
      { connect } = await import("./native-filesystem.mjs");
    const c = await connect(resolve(socketDir, `${current.index}.sock`));
    assert.equal(
      (await c.call(nativeCapability.scope, nativeCapability.token, 2)).code,
      "ESTALE",
    );
    c.socket.destroy();
    const staleWrite = await connect(
      resolve(socketDir, `${current.index}.sock`),
    );
    assert.equal(
      (
        await staleWrite.call(
          nativeCapability.scope,
          nativeCapability.token,
          2,
          { op: "write", handle: 1, offset: 0 },
          Buffer.from("stale"),
        )
      ).code,
      "ESTALE",
    );
    staleWrite.socket.destroy();
    pass(
      "native IPC capability rejected on the new owner after prior owner and disk loss",
    );
  }
  const pending = post(
    "exec",
    { id: "partition", tool: "guest", args: ["wait"], timeoutMs: 30000 },
    70000,
  ).catch(() => null);
  for (let i = 0; i < 200; i++) {
    const r = await post("stat", { path: "/workspace/partial" });
    if (r.ok && (await r.json()).size === 9) break;
    await sleep(25);
  }
  assert.equal(
    Buffer.from(
      (await call("read", { path: "/workspace/partial" })).data,
      "base64",
    ).toString(),
    "committed",
  );
  const stale = oldToken,
    second = await owner();
  second.route = false;
  second.process.kill("SIGSTOP");
  const status = await eventually("status", { id: "partition" });
  assert.equal(status.status, "interrupted");
  assert.equal(
    (await post("fs", { op: "heartbeat", token: stale, seq: 1 })).status,
    409,
  );
  assert.equal(
    Buffer.from(
      (await call("read", { path: "/workspace/partial" })).data,
      "base64",
    ).toString(),
    "committed",
  );
  second.process.kill("SIGCONT");
  for (
    let i = 0;
    i < 100 &&
    second.process.exitCode === null &&
    second.process.signalCode === null;
    i++
  )
    await sleep(100);
  assert.ok(
    second.process.exitCode !== null || second.process.signalCode !== null,
    "resumed expired owner must self-fence",
  );
  await pending;
  pass(
    "suspended owner expires, takeover fences old execution, resumed owner self-fences",
  );
  if (nativeFilesystem) {
    // Only the takeover owner remains. With the bucket paused there is no
    // follower or bucket proof available for the next committed write.
    const current = await owner(),
      capability = { token: randomBytes(32).toString("hex") };
    Object.assign(capability, await call("native-register", capability));
    const { connect } = await import("./native-filesystem.mjs");
    const client = await connect(resolve(socketDir, `${current.index}.sock`));
    docker("pause", name);
    paused = true;
    const write = post("native-mutate", {}, 15000).catch((e) => e);
    let wrote = false;
    for (let i = 0; i < 100; i++) {
      if (
        (
          await readFile(resolve(work, `node-${current.index}.log`), "utf8")
        ).includes("NATIVE_GATE_WRITE_COMPLETE")
      ) {
        wrote = true;
        break;
      }
      await sleep(25);
    }
    assert.ok(
      wrote,
      "native gate test write must reach managed SQLite before the IPC read",
    );
    let settled = false;
    const reply = client
      .call(capability.scope, capability.token, 1, "/workspace/native-gate")
      .finally(() => {
        settled = true;
      });
    await sleep(500);
    assert.equal(settled, false, "native stat must wait for durability proof");
    docker("unpause", name);
    paused = false;
    const stat = await reply;
    assert.equal(stat.code, undefined);
    assert.equal(stat.value.size, 13);
    assert.equal((await write).status, 200);
    const opened = await client.call(capability.scope, capability.token, 2, {
      op: "open",
      path: "/workspace/native-gate",
      write: true,
    });
    assert.equal(opened.code, undefined);
    docker("pause", name);
    paused = true;
    let writeSettled = false;
    const nativeWrite = client
      .call(
        capability.scope,
        capability.token,
        3,
        { op: "write", handle: opened.value.handle, offset: 13 },
        Buffer.from("durable"),
      )
      .finally(() => {
        writeSettled = true;
      });
    await sleep(500);
    assert.equal(
      writeSettled,
      false,
      "native mutation acknowledgement must wait for durability proof",
    );
    docker("unpause", name);
    paused = false;
    const written = await nativeWrite;
    assert.equal(written.code, undefined);
    assert.equal(written.value.offset, 20);
    assert.equal(
      (
        await client.call(
          capability.scope,
          capability.token,
          4,
          "/workspace/native-gate",
        )
      ).value.size,
      20,
    );
    client.socket.destroy();
    pass(
      "native write acknowledgement is withheld until bucket durability recovers",
    );
    pass(
      "native read output is withheld while proof is unavailable and released after bucket recovery",
    );
  }
} finally {
  if (paused) docker("unpause", name);
  for (const n of nodes) await terminate(n);
  if (supervisor) await supervisor.close();
  if (proxy) {
    proxy.closeAllConnections();
    await new Promise((r) => proxy.close(r));
  }
  docker("rm", "-f", name);
  if (socketDir) await rm(socketDir, { recursive: true, force: true });
  await writeFile(
    resolve(work, "results.json"),
    JSON.stringify(evidence, null, 2),
  );
  console.log("Evidence:", resolve(work, "results.json"));
}
