import { test } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtemp,
  writeFile,
  readFile,
  readdir,
  rm,
  stat,
} from "node:fs/promises";
import { createHash } from "node:crypto";
import { once } from "node:events";
// @ts-ignore Native service.
import { createSupervisor } from "../service/server.mjs";
// @ts-ignore Native service.
import { createResources } from "../service/resources.mjs";

test("private scratch lease, orphan cleanup and production resource admission", async () => {
  const root = await mkdtemp("/tmp/celld-resource-test-");
  try {
    const config = { development: true, runtimeDirectory: root };
    const first = await createResources(config);
    await assert.rejects(createResources(config), /locked/);
    const old = await first.allocate();
    await writeFile(old.directory + "/private", "agent-secret");
    assert.equal((await stat(old.directory)).mode & 0o777, 0o700);
    await first.close(); // Simulate loss of supervisor lease before cleanup.
    const restarted = await createResources(config);
    assert.equal(
      (await readdir(root)).some((n) => n.startsWith("job-")),
      false,
    );
    const current = await restarted.allocate();
    assert.notEqual(current.directory, old.directory);
    await current.cleanup();
    await restarted.close();
    await assert.rejects(
      createResources({ runtimeDirectory: root }),
      /Linux|cgroupParent/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("concurrent commands isolate scratch, environments, retry results and diagnostics", async () => {
  const root = await mkdtemp("/tmp/celld-private-test-");
  const runtime = root + "/runtime";
  await writeFile(root + "/module", "public test module");
  await writeFile(
    root + "/runner",
    `#!${process.execPath}
import fs from 'node:fs';
let text='';process.stdin.on('data', b=>text+=b);process.stdin.on('end',()=>{
const c=JSON.parse(text), home=process.env.HOME;
const previous=fs.existsSync(home+'/private');
fs.writeFileSync(home+'/private',c.env.AGENT_SECRET||'none');
process.stderr.write(c.token+' '+c.env.AGENT_SECRET+' private diagnostic');
setTimeout(()=>console.log(JSON.stringify({reason:'exited',exitCode:0,
stdout: c.args[0]==='large' ? 'x'.repeat(510*1024) : JSON.stringify({home,previous,secret:c.env.AGENT_SECRET,hostEnv:process.env,cwdMatches:fs.realpathSync(home)===process.cwd()}),
stderr:'',credential:c.token})),c.args[0]==='wait'?10000:75);
});`,
    { mode: 0o700 },
  );
  const token = "service-token-".repeat(5),
    execution = "execution-".repeat(8);
  const config = {
    development: true,
    token,
    runtimeDirectory: runtime,
    filesystemSocket: "/tmp/test-fs.sock",
    runner: root + "/runner",
    maxConcurrent: 2,
    tools: {
      guest: {
        public: true,
        envAllowlist: ["AGENT_SECRET"],
        path: root + "/module",
        sha256: createHash("sha256").update("public test module").digest("hex"),
      },
    },
  };
  const previous = process.env.PRIVATE_PARENT_SECRET;
  process.env.PRIVATE_PARENT_SECRET = "must-not-inherit";
  const service = await createSupervisor(config);
  const diagnostics: any[] = [];
  service.server.on("runnerDiagnostic", (e: any) => diagnostics.push(e));
  service.server.listen(0, "127.0.0.1");
  await once(service.server, "listening");
  const url = `http://127.0.0.1:${service.server.address().port}`;
  const run = (workspace: string, extra = {}) => ({
    workspace,
    id: "same",
    token: execution,
    nativeFilesystemScope: `Workspace:${workspace}`,
    tool: "guest",
    env: { AGENT_SECRET: `secret-${workspace}` },
    ...extra,
  });
  const post = (path: string, body: any) =>
    fetch(url + path, {
      method: "POST",
      headers: { authorization: `Bearer ${token}` },
      body: JSON.stringify(body),
    });
  try {
    const a = run("a"),
      b = run("b");
    const [ra, rb]: any[] = await Promise.all([
      post("/v1/run", a).then((r) => r.json()),
      post("/v1/run", b).then((r) => r.json()),
    ]);
    const va = JSON.parse(ra.stdout),
      vb = JSON.parse(rb.stdout);
    assert.notEqual(va.home, vb.home);
    for (const [value, secret] of [
      [va, "secret-a"],
      [vb, "secret-b"],
    ]) {
      assert.equal(value.secret, secret);
      assert.equal(value.previous, false);
      assert.equal(value.cwdMatches, true);
      assert.deepEqual(
        Object.keys(value.hostEnv)
          .filter(
            (k) =>
              process.platform !== "darwin" || k !== "__CF_USER_TEXT_ENCODING",
          )
          .sort(),
        [
          "HOME",
          "RUST_BACKTRACE",
          "TMPDIR",
          "XDG_CACHE_HOME",
          "XDG_CONFIG_HOME",
          "XDG_DATA_HOME",
        ].sort(),
      );
      await assert.rejects(readFile(value.home + "/private"), {
        code: "ENOENT",
      });
    }
    assert.equal(ra.credential, undefined);
    assert.deepEqual(await (await post("/v1/run", a)).json(), ra);
    assert.deepEqual(await (await post("/v1/run", b)).json(), rb);
    assert.equal(
      (await post("/v1/run", { ...a, nativeFilesystemScope: "Other:a" }))
        .status,
      409,
    );
    const text = JSON.stringify(diagnostics);
    for (const secret of [
      "secret-a",
      "secret-b",
      execution,
      token,
      "private diagnostic",
      "must-not-inherit",
    ])
      assert.equal(text.includes(secret), false);
    assert.ok(
      diagnostics.some((e) => e.workspace === "a") &&
        diagnostics.some((e) => e.workspace === "b"),
    );
    assert.equal(
      (
        await post(
          "/v1/run",
          run("a", { id: "bad-env", env: { UNAPPROVED: "secret" } }),
        )
      ).status,
      400,
    );
    assert.equal(
      (await readdir(runtime)).filter((n) => n.startsWith("job-")).length,
      0,
    );
    // Hold one agent while the other completes with the same command ID/token.
    const pending = post("/v1/run", run("a", { id: "cancel", args: ["wait"] }));
    for (let i = 0; i < 100; i++) {
      if (((await (await fetch(url + "/healthz")).json()) as any).active) break;
      await new Promise((r) => setTimeout(r, 5));
    }
    await post("/v1/cancel", run("b", { id: "cancel" }));
    const other: any = await (
      await post("/v1/run", run("b", { id: "cancel" }))
    ).json();
    assert.equal(JSON.parse(other.stdout).secret, "secret-b");
    assert.equal(
      ((await (await fetch(url + "/healthz")).json()) as any).active,
      1,
    );
    await post("/v1/cancel", run("a", { id: "cancel" }));
    assert.equal(((await (await pending).json()) as any).reason, "cancelled");
    assert.equal(
      (await readdir(runtime)).filter((n) => n.startsWith("job-")).length,
      0,
    );
    // Pressure the bounded private result cache; an evicted retry must not run.
    for (let i = 0; i < 34; i++) {
      const r: any = await (
        await post("/v1/run", run("a", { id: `large-${i}`, args: ["large"] }))
      ).json();
      assert.equal(r.stdout.length, 510 * 1024);
    }
    assert.equal(
      (await post("/v1/run", run("a", { id: "large-0", args: ["large"] })))
        .status,
      410,
    );
    assert.equal(
      (await readdir(runtime)).filter((n) => n.startsWith("job-")).length,
      0,
    );
    await assert.rejects(
      createSupervisor({
        ...config,
        tools: { guest: { ...config.tools.guest, public: false } },
      }),
      /explicitly be public/,
    );
    await assert.rejects(
      createSupervisor({
        ...config,
        tools: {
          guest: {
            ...config.tools.guest,
            envAllowlist: ["CELLD_SANDBOX_TOKEN"],
          },
        },
      }),
      /allowlist/,
    );
  } finally {
    if (previous === undefined) delete process.env.PRIVATE_PARENT_SECRET;
    else process.env.PRIVATE_PARENT_SECRET = previous;
    await service.close();
    await rm(root, { recursive: true, force: true });
  }
});
