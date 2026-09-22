import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
// @ts-ignore Node service deliberately ships as native ESM.
import { createSupervisor } from "../service/server.mjs";
test("supervisor authenticates, pins modules, bounds admission, cancels and deduplicates", async () => {
  const dir = await mkdtemp(tmpdir() + "/celld-supervisor-");
  await writeFile(dir + "/tool", "synthetic module");
  await writeFile(
    dir + "/runner",
    `#!${process.execPath}\nlet text='';process.stdin.on('data',b=>text+=b);process.stdin.on('end',()=>{const c=JSON.parse(text);setTimeout(()=>console.log(JSON.stringify({reason:'exited',exitCode:0,stdout:c.args.join(' '),stderr:''})),c.args[0]==='wait'?10000:10);});\n`,
    { mode: 0o700 },
  );
  const token = "x".repeat(64),
    execToken = "y".repeat(64);
  const config = {
    development: true,
    token,
    callbackToken: token,
    callbackOrigin: "http://127.0.0.1:19876",
    runner: dir + "/runner",
    maxConcurrent: 1,
    tools: {
      guest: {
        path: dir + "/tool",
        sha256: createHash("sha256").update("synthetic module").digest("hex"),
      },
    },
  };
  const svc = await createSupervisor(config);
  svc.server.listen(0, "127.0.0.1");
  await new Promise((r) => svc.server.once("listening", r));
  const url = `http://127.0.0.1:${svc.server.address().port}`;
  const run = {
    workspace: "w",
    id: "c",
    tool: "guest",
    token: execToken,
    args: ["hello"],
  };
  const post = (path: string, value: any, auth = token) =>
    fetch(url + path, {
      method: "POST",
      headers: { authorization: `Bearer ${auth}` },
      body: JSON.stringify(value),
    });
  try {
    assert.equal((await post("/v1/run", run, "wrong")).status, 401);
    assert.equal(
      (await post("/v1/run", { ...run, tool: "missing" })).status,
      400,
    );
    assert.equal(
      ((await (await post("/v1/run", run)).json()) as any).stdout,
      "hello",
    );
    assert.equal(
      ((await (await post("/v1/run", run)).json()) as any).stdout,
      "hello",
    );
    assert.equal(
      (await post("/v1/run", { ...run, args: ["conflict"] })).status,
      409,
    );
    const waiting = {
      ...run,
      id: "wait",
      token: "z".repeat(64),
      args: ["wait"],
    };
    const pending = post("/v1/run", waiting);
    for (let i = 0; i < 50; i++) {
      if (((await (await fetch(url + "/healthz")).json()) as any).active) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    assert.equal(
      (await post("/v1/run", { ...run, id: "busy", token: "b".repeat(64) }))
        .status,
      503,
    );
    await post("/v1/cancel", waiting);
    assert.equal(((await (await pending).json()) as any).reason, "cancelled");
    const timed = await post("/v1/run", {
      ...waiting,
      id: "timeout",
      token: "t".repeat(64),
      timeoutMs: 100,
    });
    assert.equal(((await timed.json()) as any).reason, "timed_out");
    await assert.rejects(
      createSupervisor({
        ...config,
        tools: { guest: { ...config.tools.guest, sha256: "0".repeat(64) } },
      }),
      /digest mismatch/,
    );
  } finally {
    await svc.close();
    await rm(dir, { recursive: true, force: true });
  }
});
