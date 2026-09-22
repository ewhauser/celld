import { createConnection } from "node:net";
import { once } from "node:events";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
function field(value) {
  const bytes = Buffer.from(value),
    size = Buffer.alloc(2);
  size.writeUInt16LE(bytes.length);
  return Buffer.concat([size, bytes]);
}
function request(scope, token, sequence, path) {
  const seq = Buffer.alloc(8);
  seq.writeBigUInt64LE(BigInt(sequence));
  return Buffer.concat([
    Buffer.from([1, 1]),
    field(scope),
    field(token),
    seq,
    field(path),
  ]);
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
    if (size > 8192) {
      socket.destroy(new Error("unbounded reply"));
      return;
    }
    if (bytes.length < size + 4) return;
    const body = bytes.subarray(4, size + 4);
    bytes = bytes.subarray(size + 4);
    const p = pending;
    pending = null;
    p?.resolve(body);
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
    call(scope, token, sequence, path = "/workspace/input.txt") {
      assert.equal(pending, null);
      return new Promise((resolve, reject) => {
        pending = { resolve, reject };
        socket.write(frame(request(scope, token, sequence, path)));
      });
    },
  };
}
export async function ipcChecks({ socketPath, call, post, token, record }) {
  const { scope } = await call("native-register", { token });
  const client = await connect(socketPath);
  const valid = await client.call(scope, token, 1);
  assert.equal(valid.length, 49);
  assert.equal(valid[0], 0);
  assert.equal(Number(valid.readBigUInt64LE(9)), 15);
  assert.equal(
    (await client.call(scope, token, 2, "/workspace/missing"))[0],
    4,
  );
  assert.equal(
    (await client.call(scope, token, 3, "/workspace/../outside"))[0],
    3,
  );
  assert.equal((await client.call(scope, "wrong", 4))[0], 1);
  assert.equal((await client.call(scope, token, 4))[0], 0);
  assert.equal((await client.call(scope, token, 4))[0], 1);
  // A closed input gate must not be bypassed by the native path.
  const busy = post("native-busy", {});
  await new Promise((r) => setTimeout(r, 100));
  assert.equal((await client.call(scope, "wrong", 5))[0], 1);
  assert.equal((await client.call(scope, token, 5))[0], 9);
  assert.equal((await busy).status, 200);
  assert.equal((await client.call(scope, token, 5))[0], 0);
  await call("native-revoke", {});
  assert.equal((await client.call(scope, token, 6))[0], 1);
  client.socket.destroy();
  const expiring = await call("native-register", { token, ttl: 300 });
  const expiryClient = await connect(socketPath);
  assert.equal((await expiryClient.call(expiring.scope, token, 1))[0], 0);
  await new Promise((r) => setTimeout(r, 350));
  assert.equal((await expiryClient.call(expiring.scope, token, 2))[0], 1);
  expiryClient.socket.destroy();
  await call("native-revoke", {});
  record(
    "native stat: persistent binary connection, metadata, path confinement, auth, ordering, input gate, expiry and revocation",
  );
  for (const bytes of [
    Buffer.from([255, 255, 255, 255]),
    Buffer.from([0, 0, 0, 0]),
    frame(Buffer.from([2, 1])),
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
