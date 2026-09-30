import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { artifactsLayer } from "../src/Artifacts.js";
import { buildFixture } from "../src/Build.js";
import { acquireReference } from "../src/Reference.js";

it.live("keeps Miniflare's process-exit signal hooks out of the driver", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped();
    const before = {
      SIGINT: process.listeners("SIGINT"),
      SIGTERM: process.listeners("SIGTERM"),
    };
    yield* Effect.gen(function* () {
      const bundle = yield* buildFixture;
      const runtime = yield* acquireReference("reference", bundle, (error) =>
        Effect.die(error),
      );
      expect(runtime.metadata.fixtureSha256).toBe(bundle.sha256);
      expect(new URL(runtime.target.baseUrl).hostname).toBe("127.0.0.1");
      expect(process.listeners("SIGINT")).toEqual(before.SIGINT);
      expect(process.listeners("SIGTERM")).toEqual(before.SIGTERM);
    }).pipe(
      Effect.provide(artifactsLayer(directory)),
      Effect.tapCause(() =>
        fs
          .readFileString(`${directory}/reference.log`)
          .pipe(Effect.tap(Effect.logError)),
      ),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);
