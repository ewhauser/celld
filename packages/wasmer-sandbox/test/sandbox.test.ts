import { test } from "node:test";
import assert from "node:assert/strict";
import { WasmerSandbox } from "../src/index.ts";
import { memoryStorage } from "./storage.ts";
const options = {
  workspace: "test",
  supervisorURL: "http://127.0.0.1:19877",
  supervisorToken: "x".repeat(64),
};
test("command replay/conflict, single writer, sequenced callbacks, stale tokens and durable terminal result", async () => {
  const storage = memoryStorage();
  let syncs = 0;
  storage.sync = async () => {
    syncs++;
  };
  const sandbox = new WasmerSandbox(
    { storage, assertCanAwaitCallback() {} },
    options,
  );
  const original = globalThis.fetch;
  let launches = 0;
  globalThis.fetch = async (_url, init) => {
    launches++;
    const config = JSON.parse(init!.body as string);
    assert.throws(
      () => sandbox.fs.writeFile("/workspace/x", Buffer.from("x")),
      { code: "EBUSY" },
    );
    await assert.rejects(sandbox.exec({ id: "second", tool: "guest" }), {
      code: "EBUSY",
    });
    let seq = 0;
    const callback = async (body: any) => {
      const r = await sandbox.callback(
        new Request("http://test/fs", {
          method: "POST",
          body: JSON.stringify({ token: config.token, seq: ++seq, ...body }),
        }),
      );
      return { status: r.status, value: (await r.json()) as any };
    };
    const open = await callback({
      op: "open",
      path: "/workspace/file",
      read: true,
      write: true,
      append: true,
      create: true,
      create_new: false,
      truncate: false,
    });
    const handle = open.value.value.handle;
    const op = { op: "write", handle, offset: 0, data: [65] };
    assert.equal((await callback(op)).value.value.written, 1);
    const replay = await callback({ ...op, seq: 2 });
    assert.equal(replay.value.value.written, 1);
    assert.equal(sandbox.fs.readFile("/workspace/file").toString(), "A");
    assert.equal((await callback({ ...op, data: [66], seq: 2 })).status, 409);
    assert.equal(
      (await callback({ op: "stat", path: "/workspace/file", seq: 6 })).status,
      409,
    );
    assert.equal(
      (await callback({ op: "stat", path: "/workspace/file", seq: 3 })).value
        .value.size,
      1,
    );
    return Response.json({
      reason: "exited",
      exitCode: 0,
      stdout: "ok",
      stderr: "",
    });
  };
  try {
    const input = { id: "first", tool: "guest" };
    assert.equal((await sandbox.exec(input)).status, "succeeded");
    assert.equal((await sandbox.exec(input)).result.stdout, "ok");
    assert.equal(launches, 1);
    await assert.rejects(sandbox.exec({ ...input, args: ["changed"] }), {
      code: "ECONFLICT",
    });
    assert.equal(
      (
        await sandbox.callback(
          new Request("http://test/fs", {
            method: "POST",
            body: JSON.stringify({ seq: 1, token: "old", op: "heartbeat" }),
          }),
        )
      ).status,
      409,
    );
    assert.ok(syncs >= 3);
  } finally {
    globalThis.fetch = original;
  }
});
test("unknown completion remains interrupted across activation and is never automatically rerun", async () => {
  const storage = memoryStorage(),
    state = { storage, assertCanAwaitCallback() {} };
  let sandbox = new WasmerSandbox(state, options);
  const original = globalThis.fetch;
  let launches = 0;
  globalThis.fetch = async () => {
    launches++;
    throw Error("lost acknowledgement");
  };
  try {
    assert.equal(
      (await sandbox.exec({ id: "lost", tool: "guest" })).status,
      "interrupted",
    );
    sandbox = new WasmerSandbox(state, options);
    assert.equal(
      (await sandbox.exec({ id: "lost", tool: "guest" })).status,
      "interrupted",
    );
    assert.equal(launches, 1);
    storage.sql
      .exec(
        "INSERT INTO celld_sandbox_commands VALUES('running','hash','running',NULL,1,NULL)",
      )
      .toArray();
    assert.equal(
      new WasmerSandbox(state, options).status("running")!.status,
      "interrupted",
    );
  } finally {
    globalThis.fetch = original;
  }
});
test("deadlock guard runs before any hash, mutation or launch", async () => {
  const storage = memoryStorage();
  const sandbox = new WasmerSandbox(
    {
      storage,
      assertCanAwaitCallback() {
        throw Error("transaction");
      },
    },
    options,
  );
  await assert.rejects(sandbox.exec({ id: "x", tool: "guest" }), /transaction/);
  assert.equal(sandbox.status("x"), null);
});
test("invalid configuration and oversized/invalid input fail closed", async () => {
  const state = { storage: memoryStorage(), assertCanAwaitCallback() {} };
  assert.throws(
    () => new WasmerSandbox(state, { ...options, supervisorToken: "short" }),
  );
  assert.throws(
    () =>
      new WasmerSandbox(state, {
        ...options,
        supervisorURL: "http://public.example",
      }),
  );
  const sandbox = new WasmerSandbox(state, options);
  await assert.rejects(sandbox.exec({ id: "x", tool: "guest", timeoutMs: -1 }));
  await assert.rejects(sandbox.exec({ id: "x", tool: "guest", args: ["\0"] }));
  await assert.rejects(
    sandbox.exec({ id: "x", tool: "guest", cwd: "/workspace/../secret" }),
  );
});

test("replay survives removal of its former working directory", async () => {
  const storage = memoryStorage();
  const sandbox = new WasmerSandbox(
    { storage, assertCanAwaitCallback() {} },
    options,
  );
  sandbox.fs.mkdir("/workspace/old");
  const original = globalThis.fetch;
  let launches = 0;
  globalThis.fetch = async () => {
    launches++;
    return Response.json({
      reason: "exited",
      exitCode: 0,
      stdout: "done",
      stderr: "",
    });
  };
  try {
    const cmd = { id: "removed-cwd", tool: "guest", cwd: "/workspace/old" };
    assert.equal((await sandbox.exec(cmd)).status, "succeeded");
    sandbox.fs.rmdir(cmd.cwd);
    assert.equal((await sandbox.exec(cmd)).status, "succeeded");
    assert.equal(launches, 1);
  } finally {
    globalThis.fetch = original;
  }
});

test("command transport bounds count UTF-8 bytes and preserve special environment keys", async () => {
  const { command } = await import("../src/protocol.ts");
  assert.throws(
    () =>
      command({
        id: "large",
        tool: "guest",
        args: Array(16).fill("界".repeat(4096)),
      }),
    { code: "E2BIG" },
  );
  const env = JSON.parse('{"__proto__":"literal"}');
  assert.equal(
    JSON.stringify(command({ id: "env", tool: "guest", env }).env),
    '{"__proto__":"literal"}',
  );
});

test("cancellation during the pre-launch durability barrier never starts a guest", async () => {
  const storage = memoryStorage();
  const sandbox = new WasmerSandbox(
    { storage, assertCanAwaitCallback() {} },
    options,
  );
  let entered!: () => void, release!: () => void;
  const atBarrier = new Promise<void>((r) => (entered = r));
  const barrier = new Promise<void>((r) => (release = r));
  let syncs = 0;
  storage.sync = async () => {
    if (++syncs === 1) {
      entered();
      await barrier;
    }
  };
  const original = globalThis.fetch;
  let runs = 0;
  globalThis.fetch = async (url) => {
    if (String(url).endsWith("/v1/run")) runs++;
    return Response.json({ ok: true });
  };
  try {
    const pending = sandbox.exec({ id: "cancel-before-launch", tool: "guest" });
    await atBarrier;
    await sandbox.cancel("cancel-before-launch");
    release();
    assert.equal((await pending).status, "cancelled");
    assert.equal(runs, 0);
  } finally {
    release();
    globalThis.fetch = original;
  }
});
