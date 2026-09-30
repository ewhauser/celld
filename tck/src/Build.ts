import { Effect, FileSystem } from "effect";
import { build } from "esbuild";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { Artifacts } from "./Artifacts.js";
import { decodeJsonc } from "./Jsonc.js";
import { TckError, type Bundle } from "./Domain.js";

import { FixtureConfig } from "./FixtureConfig.js";
export const sha256 = (value: string | Uint8Array) =>
  createHash("sha256").update(value).digest("hex");
const effectVersion: string = createRequire(import.meta.url)(
  "effect/package.json",
).version;

export const buildFixtureFor = (
  fixture:
    | "core"
    | "node"
    | "flags"
    | "extensions"
    | "repro"
    | "recovery"
    | "qualification",
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const artifacts = yield* Artifacts;
    const directory = resolve(artifacts.directory, "fixture");
    yield* fs.makeDirectory(directory, { recursive: true });
    let config = yield* decodeJsonc(
      FixtureConfig,
      yield* fs.readFileString(
        new URL(
          `../fixtures/${fixture === "node" || fixture === "flags" ? "core" : fixture}/wrangler.jsonc`,
          import.meta.url,
        ).pathname,
      ),
    );
    if (fixture === "node")
      config = { ...config, compatibility_flags: ["nodejs_compat"] };
    // The same core worker, rebuilt with the disable counterparts of two
    // switches that are on by default at the pinned compatibility date.
    if (fixture === "flags")
      config = {
        ...config,
        compatibility_flags: [
          "delete_all_preserves_alarm",
          "no_websocket_standard_binary_type",
        ],
      };
    const binding = config.durable_objects.bindings[0];
    if (!binding)
      return yield* Effect.fail(
        new TckError({ phase: "build", message: "Missing fixture DO binding" }),
      );
    let dynamicCode = "";
    // Both the extensions fixture and the recovery fixture load a child bundle
    // through a Worker Loader binding; only the former ships static assets.
    if (fixture === "extensions" || fixture === "recovery") {
      const child = yield* Effect.tryPromise({
        try: () =>
          build({
            entryPoints: [
              new URL(`../fixtures/${fixture}/child.ts`, import.meta.url)
                .pathname,
            ],
            bundle: true,
            write: false,
            format: "esm",
            platform: "neutral",
            target: "es2023",
            external: ["cloudflare:*"],
            legalComments: "none",
          }),
        catch: (error) =>
          new TckError({ phase: "build", message: String(error) }),
      });
      dynamicCode = child.outputFiles?.[0]?.text ?? "";
      if (!dynamicCode)
        return yield* Effect.fail(
          new TckError({ phase: "build", message: "Empty dynamic fixture" }),
        );
      if (fixture === "extensions")
        yield* fs.copy(
          new URL("../fixtures/extensions/assets", import.meta.url).pathname,
          resolve(directory, "assets"),
        );
    }
    yield* Effect.tryPromise({
      try: () =>
        build({
          entryPoints: [
            new URL(
              `../fixtures/${fixture === "flags" ? "core" : fixture}/worker.ts`,
              import.meta.url,
            ).pathname,
          ],
          outfile: resolve(directory, "worker.js"),
          bundle: true,
          format: "esm",
          platform: "neutral",
          target: "es2023",
          external: ["cloudflare:*", "node:*"],
          sourcemap: false,
          legalComments: "none",
          metafile: true,
          loader: { ".wasm": "copy" },
          assetNames: "[name]",
          define: { __DYNAMIC_CODE__: JSON.stringify(dynamicCode) },
        }),
      catch: (error) =>
        new TckError({ phase: "build", message: String(error) }),
    }).pipe(
      Effect.flatMap((result) =>
        artifacts.json("fixture/build-meta.json", result.metafile),
      ),
    );
    yield* artifacts.json("fixture/wrangler.jsonc", {
      ...config,
      main: "worker.js",
      no_bundle: true,
    });
    const source = yield* fs.readFileString(resolve(directory, "worker.js"));
    const modules: Record<string, string> = {};
    for (const file of yield* fs.readDirectory(directory)) {
      if (file === "worker.js" || file.endsWith(".wasm"))
        modules[file] = sha256(yield* fs.readFile(resolve(directory, file)));
    }
    const bundle: Bundle = {
      modules,
      directory,
      config,
      source,
      sha256: sha256(source),
      compatibilityDate: config.compatibility_date,
      binding: { name: binding.name, className: binding.class_name },
    };
    yield* artifacts.json("fixture/manifest.json", {
      ...bundle,
      source: "worker.js",
      effect: effectVersion,
    });
    return bundle;
  });

export const buildFixture = buildFixtureFor("core");
