import { Effect, FileSystem } from "effect";
import { createRequire } from "node:module";
import { Processes } from "./Processes.js";
import { sha256 } from "./Build.js";

export const provenance = Effect.gen(function* () {
  const processes = yield* Processes;
  const fs = yield* FileSystem.FileSystem;
  const require = createRequire(import.meta.url);
  const revision = yield* processes.run("git", ["rev-parse", "HEAD"]).pipe(
    Effect.map((result) => result.stdout.trim()),
    Effect.catch(() => Effect.succeed("unavailable")),
  );
  const dirty = yield* processes.run("git", ["status", "--porcelain"]).pipe(
    Effect.map((result) => result.stdout.trim().length > 0),
    Effect.catch(() => Effect.succeed(null)),
  );
  return {
    sourceRevision: revision,
    dirty,
    hostNode: process.version,
    platform: process.platform,
    architecture: process.arch,
    effect: require("effect/package.json").version,
    platformNode: require("@effect/platform-node/package.json").version,
    esbuild: require("esbuild/package.json").version,
    lockfileSha256: sha256(
      yield* fs.readFileString(
        new URL("../pnpm-lock.yaml", import.meta.url).pathname,
      ),
    ),
    // infra/compose.yaml's pinned default is already in the evidence bundle;
    // an override is recorded because nothing else would show it.
    ...(process.env.TCK_CELLD_IMAGE
      ? { celldImage: process.env.TCK_CELLD_IMAGE }
      : {}),
  };
});
