import { createConnection } from "node:net";
import { once } from "node:events";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { randomBytes } from "node:crypto";
function request(
  scope,
  token,
  sequence,
  operation,
  data = Buffer.alloc(0),
  command = "integration-test",
) {
  const metadata = Buffer.from(
    JSON.stringify({ scope, token, command, sequence, operation }),
  );
  const header = Buffer.alloc(5);
  header[0] = 3;
  header.writeUInt32LE(metadata.length, 1);
  return Buffer.concat([header, metadata, data]);
}
function frame(bytes) {
  const size = Buffer.alloc(4);
  size.writeUInt32LE(bytes.length);
  return Buffer.concat([size, bytes]);
}
export async function connect(socketPath) {
  const socket = createConnection(socketPath);
  await once(socket, "connect");
  let pending = null,
    bytes = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    bytes = Buffer.concat([bytes, chunk]);
    if (bytes.length < 4) return;
    const size = bytes.readUInt32LE(0);
    if (size > 2 * 1024 * 1024) {
      socket.destroy(new Error("unbounded reply"));
      return;
    }
    if (bytes.length < size + 4) return;
    const body = bytes.subarray(4, size + 4);
    bytes = bytes.subarray(size + 4);
    const p = pending;
    pending = null;
    assert.equal(body[0], 3);
    const n = body.readUInt32LE(1);
    p?.resolve({
      ...JSON.parse(body.subarray(5, 5 + n)),
      data: body.subarray(5 + n),
    });
  });
  socket.on("error", (e) => {
    pending?.reject(e);
    pending = null;
  });
  socket.on("close", () => {
    pending?.reject(new Error("IPC closed"));
    pending = null;
  });
  socket.setTimeout(6000, () => socket.destroy(new Error("IPC timeout")));
  return {
    socket,
    call(
      scope,
      token,
      sequence,
      operation = { op: "stat", path: "/workspace/input.txt" },
      data,
      command,
    ) {
      if (typeof operation === "string")
        operation = { op: "stat", path: operation };
      assert.equal(pending, null);
      return new Promise((resolve, reject) => {
        pending = { resolve, reject };
        socket.write(
          frame(request(scope, token, sequence, operation, data, command)),
        );
      });
    },
  };
}
export async function ipcChecks({ socketPath, call, post, token, record }) {
  const app = await call("native-app-checks", {});
  const { scope } = await call("native-register", { token });
  const client = await connect(socketPath);
  const valid = await client.call(scope, token, 1);
  assert.equal(valid.value.size, 15);
  assert.equal(
    (await client.call(scope, token, 2, "/workspace/missing")).code,
    "ENOENT",
  );
  assert.equal(
    (await client.call(scope, token, 3, "/workspace/../outside")).code,
    "EACCES",
  );
  const forged = await connect(socketPath);
  assert.equal((await forged.call(scope, "wrong", 1)).code, "ESTALE");
  forged.socket.destroy();
  const competing = await connect(socketPath);
  assert.equal((await competing.call(scope, token, 4)).code, "ESTALE");
  competing.socket.destroy();
  assert.equal((await client.call(scope, token, 4)).code, undefined);
  // A closed input gate must not be bypassed by the native path.
  const busy = post("native-busy", {});
  await new Promise((r) => setTimeout(r, 100));
  assert.equal((await client.call(scope, token, 5)).code, "EBUSY");
  assert.equal((await busy).status, 200);
  assert.equal((await client.call(scope, token, 5)).code, undefined);
  assert.equal(
    (await client.call(scope, token, 6, { op: "fstat", handle: app.handle }))
      .code,
    "EBADF",
  );
  assert.equal(
    (
      await client.call(scope, token, 7, {
        op: "unlink",
        path: "/workspace/app-handle",
      })
    ).code,
    "EBUSY",
  );
  let seq = 8;
  const fs = async (op, data) => {
    const r = await client.call(scope, token, seq++, op, data);
    assert.equal(r.code, undefined, JSON.stringify(r));
    return r;
  };
  await fs({ op: "mkdir", path: "/workspace/ipc" });
  const handle = (
    await fs({
      op: "open",
      path: "/workspace/ipc/raw",
      read: true,
      write: true,
      create: true,
    })
  ).value.handle;
  const binary = Buffer.from(Array.from({ length: 65536 }, (_, i) => i % 256));
  await fs({ op: "write", handle, offset: 513 }, binary);
  assert.deepEqual(
    (await fs({ op: "read", handle, offset: 513, size: 65536 })).data,
    binary,
  );
  await fs({ op: "truncate", handle, size: 514 });
  await fs({
    op: "rename",
    path: "/workspace/ipc/raw",
    to: "/workspace/ipc/renamed",
  });
  assert.equal((await fs({ op: "fstat", handle })).value.size, 514);
  assert.equal(
    (
      await client.call(scope, token, seq++, {
        op: "unlink",
        path: "/workspace/ipc/renamed",
      })
    ).code,
    "EBUSY",
  );
  await fs({ op: "close", handle });
  assert.equal(
    (await fs({ op: "list", path: "/workspace/ipc" })).value[0].name,
    "renamed",
  );
  await fs({ op: "unlink", path: "/workspace/ipc/renamed" });
  await fs({ op: "rmdir", path: "/workspace/ipc" });
  await fs({ op: "heartbeat" });
  await fs({ op: "sync" });
  record(
    "all native IPC filesystem operations: raw 64 KiB binary I/O, handles, sparse offsets, truncate, rename, directories, sync and heartbeat",
  );
  const staleHandle = (
    await fs({ op: "open", path: "/workspace/input.txt", read: true })
  ).value.handle;
  const switched = client.call(scope + "-other", token, seq);
  await assert.rejects(switched, /IPC closed/);
  const reconnect = await connect(socketPath);
  assert.equal((await reconnect.call(scope, token, seq)).code, "ESTALE");
  reconnect.socket.destroy();
  record(
    "native IPC binds one socket to one grant; disconnect revokes and reconnect fails",
  );
  const secondToken = randomBytes(32).toString("hex");
  const second = await call("native-register", {
    token: secondToken,
    command: "second-command",
  });
  const wrongCommand = await connect(socketPath);
  assert.equal(
    (await wrongCommand.call(second.scope, secondToken, 1)).code,
    "ESTALE",
  );
  wrongCommand.socket.destroy();
  const secondClient = await connect(socketPath);
  assert.equal(
    (
      await secondClient.call(
        second.scope,
        secondToken,
        1,
        { op: "fstat", handle: staleHandle },
        undefined,
        "second-command",
      )
    ).code,
    "EBADF",
  );
  assert.equal(
    (
      await secondClient.call(
        second.scope,
        secondToken,
        3,
        { op: "heartbeat" },
        undefined,
        "second-command",
      )
    ).code,
    "ESTALE",
  );
  secondClient.socket.destroy();
  record(
    "native IPC rejects cross-command handles, forged command IDs and out-of-order requests",
  );
  client.socket.destroy();
  await call("native-app-close", app);
  record(
    "native TypeScript rollback, transaction handle guard and app/guest handle isolation",
  );
  const expiring = await call("native-register", { token, ttl: 300 });
  const expiryClient = await connect(socketPath);
  assert.equal(
    (await expiryClient.call(expiring.scope, token, 1)).code,
    undefined,
  );
  await new Promise((r) => setTimeout(r, 350));
  assert.equal(
    (await expiryClient.call(expiring.scope, token, 2)).code,
    "ESTALE",
  );
  expiryClient.socket.destroy();
  await call("native-revoke", {});
  record(
    "native filesystem: persistent binary connection, metadata, path confinement, auth, ordering, input gate, expiry and revocation",
  );
  for (const bytes of [
    Buffer.from([255, 255, 255, 255]),
    Buffer.from([0, 0, 0, 0]),
    frame(Buffer.from([3, 1])),
    frame(Buffer.from([1])),
  ]) {
    const c = await connect(socketPath);
    const closed = once(c.socket, "close");
    c.socket.write(bytes);
    await closed;
  }
  record(
    "native IPC rejects oversized, empty, unknown-version and truncated requests",
  );
}
export async function benchmark(call) {
  const samples = [],
    count = 1000;
  for (let pair = 0; pair < 8; pair++) {
    for (const native of pair % 2 ? [true, false] : [false, true]) {
      const start = performance.now();
      const result = await call("bench", {
        id: `bench-${pair}-${native}`,
        native,
        count,
      });
      assert.equal(result.status, "succeeded", JSON.stringify(result));
      assert.ok(
        result.result.statCalls[native ? "native" : "http"] >= count,
        JSON.stringify(result),
      );
      assert.equal(result.result.statCalls[native ? "http" : "native"], 0);
      samples.push({
        pair,
        warmup: pair === 0,
        native,
        count,
        guestMicroseconds: Number(result.result.stdout.trim()),
        commandMilliseconds: performance.now() - start,
        statCalls: result.result.statCalls,
      });
    }
  }
  const median = (values) =>
    values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)];
  const http = median(
    samples
      .filter((s) => !s.warmup && !s.native)
      .map((s) => s.guestMicroseconds),
  );
  const ipc = median(
    samples
      .filter((s) => !s.warmup && s.native)
      .map((s) => s.guestMicroseconds),
  );
  const result = {
    description:
      "Same guest, cell, file and output gates; alternating HTTP/native order; pair 0 excluded as warmup. Guest interval excludes compilation and command launch. Debug builds; local macOS only.",
    count,
    httpMedianMicroseconds: http,
    ipcMedianMicroseconds: ipc,
    ratio: http / ipc,
    samples,
  };
  console.log("BENCH", JSON.stringify(result));
  return result;
}
