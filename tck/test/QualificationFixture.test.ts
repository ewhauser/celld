import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { build } from "esbuild";
import { artifactsLayer } from "../src/Artifacts.js";
import { buildFixtureFor, sha256 } from "../src/Build.js";
import { acquireReference } from "../src/Reference.js";

it.live(
  "allocates pressure memory in the receiving Worker without routing to a Durable Object",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      yield* Effect.gen(function* () {
        const bundle = yield* buildFixtureFor("qualification");
        const built = yield* Effect.tryPromise(() =>
          build({
            stdin: {
              contents: `import worker from "./worker.ts";
            export * from "./worker.ts";
            export default { fetch(request, env) {
              if (new URL(request.url).pathname === "/pressure") {
                env = { ...env, PROBE: { getByName() { throw new Error("pressure routed to DO"); } } };
              }
              return worker.fetch(request, env);
            }};`,
              resolveDir: new URL("../fixtures/qualification", import.meta.url)
                .pathname,
            },
            bundle: true,
            write: false,
            format: "esm",
            platform: "neutral",
            external: ["cloudflare:*"],
          }),
        );
        const source = built.outputFiles![0]!.text;
        yield* fs.writeFileString(`${bundle.directory}/worker.js`, source);
        const hash = sha256(source);
        const runtime = yield* acquireReference(
          "reference",
          {
            ...bundle,
            sha256: hash,
            modules: { ...bundle.modules, "worker.js": hash },
          },
          Effect.die,
        );
        const response = yield* Effect.tryPromise((signal) =>
          fetch(`${runtime.target.baseUrl}/pressure?mb=1`, { signal }),
        );
        expect(response.status).toBe(200);
        expect(yield* Effect.tryPromise(() => response.json())).toEqual({
          mb: 1,
          checksum: 0,
        });
      }).pipe(Effect.provide(artifactsLayer(directory)));
    }).pipe(Effect.provide(NodeServices.layer)),
);
