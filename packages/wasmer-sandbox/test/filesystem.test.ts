import { test } from "node:test";
import assert from "node:assert/strict";
import { memoryStorage } from "./storage.ts";
import { WorkspaceFS } from "../src/filesystem.ts";

test("sparse writes, truncate growth, shrink/regrowth and EOF preserve byte positions", () => {
  const fs = new WorkspaceFS(memoryStorage());
  const h = fs.open("/workspace/a", { read: true, write: true, create: true });
  fs.write(h, 0, new Uint8Array([1, 2, 3]));
  fs.truncate(h, 9000);
  assert.deepEqual(
    fs.read(h, 0, 9001),
    Buffer.concat([Buffer.from([1, 2, 3]), Buffer.alloc(8997)]),
  );
  fs.write(h, 8193, new Uint8Array([4]));
  assert.deepEqual(fs.read(h, 8190, 7), Buffer.from([0, 0, 0, 4, 0, 0, 0]));
  fs.truncate(h, 2);
  fs.truncate(h, 9000);
  assert.deepEqual(
    fs.read(h, 0, 9000),
    Buffer.concat([Buffer.from([1, 2]), Buffer.alloc(8998)]),
  );
  assert.equal(fs.read(h, 9000, 4).length, 0);
  assert.equal(fs.read(h, 0, 0).length, 0);
});
test("append is atomic, handles survive rename, open unlink/replacement fails safely", () => {
  const fs = new WorkspaceFS(memoryStorage());
  fs.writeFile("/workspace/a", Buffer.from("a"));
  const h = fs.open("/workspace/a", { read: true, append: true });
  fs.write(h, 0, Buffer.from("b"));
  fs.write(h, 0, Buffer.from("c"));
  fs.rename("/workspace/a", "/workspace/b");
  assert.equal(fs.read(h, 0, 10).toString(), "abc");
  assert.throws(() => fs.unlink("/workspace/b"), { code: "EBUSY" });
  fs.writeFile("/workspace/c", Buffer.from("c"));
  assert.throws(() => fs.rename("/workspace/c", "/workspace/b"), {
    code: "EBUSY",
  });
  fs.close(h);
  fs.unlink("/workspace/b");
  assert.throws(() => fs.read(h, 0, 1), { code: "EBADF" });
});
test("path confinement, type checks, directory cycles, exclusivity and handle rights", () => {
  const fs = new WorkspaceFS(memoryStorage());
  for (const p of [
    "/workspace/../secret",
    "/etc/passwd",
    "relative",
    "/workspace/a\0b",
  ]) {
    assert.throws(() => fs.stat(p));
  }
  fs.mkdir("/workspace/a");
  fs.mkdir("/workspace/a/b");
  assert.throws(() => fs.rename("/workspace/a", "/workspace/a/b/c"), {
    code: "EINVAL",
  });
  assert.throws(() => fs.rmdir("/workspace/a"), { code: "ENOTEMPTY" });
  fs.writeFile("/workspace/x", Buffer.from("x"));
  assert.throws(() => fs.stat("/workspace/x/child"), { code: "ENOTDIR" });
  assert.throws(
    () => fs.open("/workspace/x", { write: true, create_new: true }),
    { code: "EEXIST" },
  );
  const h = fs.open("/workspace/x", { read: true });
  assert.throws(() => fs.write(h, 0, Buffer.from("z")), { code: "EBADF" });
  assert.throws(() => fs.truncate(h, 0), { code: "EBADF" });
  assert.throws(() => fs.open("/workspace/x", { read: true, truncate: true }), {
    code: "EINVAL",
  });
});
test("quota failures roll back an entire operation; independent instances preserve AgentFS schema", () => {
  const storage = memoryStorage();
  const fs = new WorkspaceFS(storage, {
    maxBytes: 10,
    maxFileBytes: 10,
    maxInodes: 4,
  });
  fs.writeFile("/workspace/a", Buffer.from("12345"));
  assert.throws(() => fs.writeFile("/workspace/b", Buffer.alloc(6)), {
    code: "ENOSPC",
  });
  assert.throws(() => fs.stat("/workspace/b"), { code: "ENOENT" });
  assert.equal(
    new WorkspaceFS(storage).readFile("/workspace/a").toString(),
    "12345",
  );
  storage.sql
    .exec("UPDATE fs_config SET value='future' WHERE key='schema_version'")
    .toArray();
  assert.throws(() => new WorkspaceFS(storage), { code: "EPROTONOSUPPORT" });
});
test("random offset operations match an independent dense byte model", () => {
  const fs = new WorkspaceFS(memoryStorage());
  const h = fs.open("/workspace/f", { read: true, write: true, create: true });
  let model = Buffer.alloc(0),
    seed = 12345;
  const rand = (n: number) => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return seed % n;
  };
  for (let i = 0; i < 150; i++) {
    if (rand(3) === 0) {
      const size = rand(14000),
        next = Buffer.alloc(size);
      model.copy(next);
      model = next;
      fs.truncate(h, size);
    } else {
      const offset = rand(14000),
        data = Buffer.alloc(rand(1000), rand(256));
      if (data.length) {
        const next = Buffer.alloc(Math.max(model.length, offset + data.length));
        model.copy(next);
        data.copy(next, offset);
        model = next;
      }
      fs.write(h, offset, data);
    }
    assert.deepEqual(fs.read(h, 0, 16000), model, `iteration ${i}`);
  }
});
