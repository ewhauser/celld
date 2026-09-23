import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  lstat,
  readFile,
  writeFile,
  readdir,
  rm,
  rmdir,
} from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { check } from "../src/storage.ts";

const jobName = /^job-[0-9a-f-]{36}$/;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function privateDirectory(path) {
  await mkdir(path, { mode: 0o700, recursive: true });
  const info = await lstat(path);
  check(
    info.isDirectory() &&
      info.uid === process.getuid() &&
      (info.mode & 0o777) === 0o700,
    "ECONFIG",
    "runtime directory must be private and owned by the supervisor",
  );
}

// A separate cgroup is configured before the runner receives any stdin. The
// native runner blocks on its configuration, so no guest code runs before attach.
export async function createResources(config) {
  const development = config.development === true;
  check(
    development || process.platform === "linux",
    "ENOTSUP",
    "production requires Linux cgroup v2",
  );
  const parent = config.cgroupParent && resolve(config.cgroupParent);
  check(
    development || parent,
    "ECONFIG",
    "production requires a delegated cgroupParent",
  );
  const limits = {
    memoryBytes: config.resources?.memoryBytes ?? 1024 * 1024 * 1024,
    cpuMillis: config.resources?.cpuMillis ?? 1000,
    pids: config.resources?.pids ?? 64,
  };
  check(
    Number.isSafeInteger(limits.memoryBytes) &&
      limits.memoryBytes >= 64 * 1024 * 1024 &&
      limits.memoryBytes <= 16 * 1024 ** 3,
  );
  check(
    Number.isSafeInteger(limits.cpuMillis) &&
      limits.cpuMillis >= 100 &&
      limits.cpuMillis <= 16000,
  );
  check(
    Number.isSafeInteger(limits.pids) && limits.pids >= 8 && limits.pids <= 512,
  );
  if (parent) {
    check(
      process.platform === "linux" && (await lstat(parent)).isDirectory(),
      "ECONFIG",
    );
    const enabled = (
      await readFile(join(parent, "cgroup.subtree_control"), "utf8")
    )
      .trim()
      .split(/\s+/);
    check(
      ["cpu", "memory", "pids"].every((v) => enabled.includes(v)),
      "ECONFIG",
      "cgroup controllers are not delegated",
    );
    check(
      (await readFile(join(parent, "cgroup.procs"), "utf8")).trim() === "",
      "ECONFIG",
      "cgroupParent must contain no processes",
    );
  }
  const ephemeral = !config.runtimeDirectory;
  check(
    development || !ephemeral,
    "ECONFIG",
    "production requires a stable private runtimeDirectory",
  );
  const root = config.runtimeDirectory
    ? resolve(config.runtimeDirectory)
    : await mkdtemp(join(tmpdir(), "celld-executor-"));
  await privateDirectory(root);
  // SQLite's OS-backed exclusive lock survives neither crash nor SIGKILL. It
  // avoids stale PID locks and prevents two supervisors reaping one another.
  let lease;
  try {
    const path = join(root, "lease.sqlite");
    try {
      check((await lstat(path)).isFile(), "ECONFIG", "invalid runtime lease");
    } catch (e) {
      if (e.code !== "ENOENT") throw e;
    }
    lease = new DatabaseSync(path);
    lease.exec(
      "PRAGMA journal_mode=DELETE; CREATE TABLE IF NOT EXISTS lease(id INTEGER); BEGIN EXCLUSIVE",
    );
  } catch (error) {
    lease?.close();
    throw error;
  }
  async function killGroup(group) {
    try {
      await writeFile(join(group, "cgroup.kill"), "1");
    } catch (error) {
      if (error.code === "ENOENT") {
        // A missing group is already gone; a missing kill interface on an
        // existing group is a failed teardown, not permission to delete scratch.
        try {
          await lstat(group);
        } catch (missing) {
          if (missing.code === "ENOENT") return;
          throw missing;
        }
      }
      throw error;
    }
    const deadline = Date.now() + 5000;
    while (
      (await readFile(join(group, "cgroup.events"), "utf8")).includes(
        "populated 1",
      )
    ) {
      check(Date.now() < deadline, "EIO", "command cgroup did not empty");
      await sleep(10);
    }
    await rmdir(group);
  }
  try {
    // The parent and runtime directory are dedicated to this supervisor; never
    // touch unrecognized entries or follow symlinks during orphan cleanup.
    if (parent)
      for (const name of await readdir(parent)) {
        if (!jobName.test(name)) continue;
        check((await lstat(join(parent, name))).isDirectory(), "ECONFIG");
        await killGroup(join(parent, name));
      }
    for (const name of await readdir(root)) {
      if (!jobName.test(name)) continue;
      check((await lstat(join(root, name))).isDirectory(), "ECONFIG");
      await rm(join(root, name), { recursive: true });
    }
  } catch (error) {
    lease.close();
    throw error;
  }
  return {
    isolated: Boolean(parent),
    root,
    limits,
    async allocate() {
      const name = "job-" + randomUUID();
      const directory = join(root, name);
      const group = parent && join(parent, name);
      await privateDirectory(directory);
      try {
        if (group) {
          await mkdir(group);
          for (const [file, value] of Object.entries({
            "memory.max": limits.memoryBytes,
            "memory.swap.max": 0,
            "memory.oom.group": 1,
            "pids.max": limits.pids,
            "cpu.max": `${limits.cpuMillis * 100} 100000`,
          }))
            await writeFile(join(group, file), String(value));
          // Require whole-group kill support before admitting a command.
          await lstat(join(group, "cgroup.kill"));
        }
      } catch (error) {
        try {
          if (group) await killGroup(group);
          await rm(directory, { recursive: true });
        } catch {
          throw Object.assign(
            new Error("resource preparation cleanup failed"),
            { code: "ECLEANUP" },
          );
        }
        throw error;
      }
      return {
        name,
        directory,
        env: Object.freeze({
          RUST_BACKTRACE: "0",
          HOME: directory,
          TMPDIR: directory,
          XDG_CACHE_HOME: directory,
          XDG_CONFIG_HOME: directory,
          XDG_DATA_HOME: directory,
        }),
        async attach(pid) {
          check(
            Number.isSafeInteger(pid) && pid > 1,
            "EIO",
            "runner did not start",
          );
          if (group) await writeFile(join(group, "cgroup.procs"), String(pid));
        },
        async cleanup() {
          if (group) await killGroup(group);
          await rm(directory, { recursive: true, force: true });
        },
      };
    },
    async close() {
      lease.close();
      if (ephemeral) await rm(root, { recursive: true });
    },
  };
}
