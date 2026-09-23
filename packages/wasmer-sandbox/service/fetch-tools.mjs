import {
  readFile,
  mkdir,
  rename,
  unlink,
  writeFile,
  chmod,
} from "node:fs/promises";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
const cli = process.env.WASMER_BIN;
if (!cli)
  throw Error("Set WASMER_BIN to a trusted Wasmer CLI (qualified: 7.4.2)");
const directory = resolve(process.argv[2] ?? "tools");
await mkdir(directory, { recursive: true });
const lock = JSON.parse(
  await readFile(new URL("./tools.lock.json", import.meta.url), "utf8"),
);
const hash = (b) => createHash("sha256").update(b).digest("hex");
const tools = {};
for (const [name, entry] of Object.entries(lock)) {
  const path = resolve(directory, name + ".webc");
  let existing;
  try {
    existing = await readFile(path);
  } catch {}
  if (!existing || hash(existing) !== entry.sha256) {
    const temporary = path + ".partial";
    try {
      execFileSync(
        cli,
        [
          "package",
          "download",
          entry.package,
          "--out-path",
          temporary,
          "--validate",
          "--quiet",
        ],
        { stdio: "inherit", timeout: 120000 },
      );
      if (hash(await readFile(temporary)) !== entry.sha256)
        throw Error(
          `SHA-256 mismatch for ${entry.package}; review the catalog before accepting an update`,
        );
      await rename(temporary, path);
    } finally {
      await unlink(temporary).catch(() => {});
    }
  }
  await chmod(path, 0o644); // Public, hash-verified artifacts must be readable by the unprivileged container UID.
  tools[name] = { ...entry, path, public: true, envAllowlist: [] };
  console.log("Verified", entry.package);
}
await writeFile(
  resolve(directory, "tools.json"),
  JSON.stringify(tools, null, 2) + "\n",
);
