import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, Schema } from "effect";
import { Log, LogLevel, Miniflare } from "miniflare";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { decodeJson } from "./Artifacts.js";
import { sha256 } from "./Build.js";
import { TckError } from "./Domain.js";
import { ReferenceConfig } from "./ReferenceConfig.js";

// Wrangler's own translation of `assets.run_worker_first`: a boolean sets the
// unconditional order, while an array becomes static routing where a `!/`
// pattern keeps the default asset-first order for the paths it matches.
// https://developers.cloudflare.com/workers/static-assets/binding/#run_worker_first
const staticRouting = (runWorkerFirst: boolean | ReadonlyArray<string>) =>
  typeof runWorkerFirst === "boolean"
    ? { invoke_user_worker_ahead_of_assets: runWorkerFirst }
    : {
        static_routing: {
          user_worker: runWorkerFirst.filter((rule) => !rule.startsWith("!")),
          asset_worker: runWorkerFirst
            .filter((rule) => rule.startsWith("!/"))
            .map((rule) => rule.slice(1)),
        },
      };

// The parent owns this entire process and captures stdout/stderr. Miniflare's
// own signal hook can safely stop workerd here without terminating the driver.
const program = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Schema.decodeUnknownEffect(Schema.String)(
      process.argv[2],
    );
    const config = yield* decodeJson(
      ReferenceConfig,
      yield* fs.readFileString(path),
    );
    for (const [name, hash] of Object.entries(config.modules)) {
      if (
        sha256(
          yield* fs.readFile(resolve(dirname(config.scriptPath), name)),
        ) !== hash
      )
        return yield* Effect.fail(
          new TckError({
            phase: "reference",
            message: `Module hash mismatch: ${name}`,
          }),
        );
    }
    const source = yield* fs.readFileString(config.scriptPath);
    if (sha256(source) !== config.sha256)
      return yield* Effect.fail(
        new TckError({
          phase: "reference",
          message: "Fixture changed before reference startup",
        }),
      );
    const runtime = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          new Miniflare({
            name: "celld-tck-core",
            modules: true,
            modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"] }],
            ...(config.config.assets
              ? {
                  assets: {
                    directory: resolve(
                      dirname(config.scriptPath),
                      config.config.assets.directory,
                    ),
                    binding: config.config.assets.binding,
                    routerConfig: {
                      has_user_worker: true,
                      ...staticRouting(config.config.assets.run_worker_first),
                    },
                    ...(config.config.assets.html_handling === undefined &&
                    config.config.assets.not_found_handling === undefined
                      ? {}
                      : {
                          assetConfig: {
                            ...(config.config.assets.html_handling === undefined
                              ? {}
                              : {
                                  html_handling:
                                    config.config.assets.html_handling,
                                }),
                            ...(config.config.assets.not_found_handling ===
                            undefined
                              ? {}
                              : {
                                  not_found_handling:
                                    config.config.assets.not_found_handling,
                                }),
                          },
                        }),
                  },
                }
              : {}),
            modulesRoot: dirname(config.scriptPath),
            script: source,
            scriptPath: config.scriptPath,
            compatibilityDate: config.compatibilityDate,
            compatibilityFlags: [...config.config.compatibility_flags],
            kvNamespaces: Object.fromEntries(
              (config.config.kv_namespaces ?? []).map((b) => [b.binding, b.id]),
            ),
            d1Databases: Object.fromEntries(
              (config.config.d1_databases ?? []).map((b) => [
                b.binding,
                b.database_id,
              ]),
            ),
            r2Buckets: Object.fromEntries(
              (config.config.r2_buckets ?? []).map((b) => [
                b.binding,
                b.bucket_name,
              ]),
            ),
            serviceBindings: Object.fromEntries(
              (config.config.services ?? []).map((b) => [
                b.binding,
                { name: b.service, entrypoint: b.entrypoint },
              ]),
            ),
            queueProducers: Object.fromEntries(
              (config.config.queues?.producers ?? []).map((b) => [
                b.binding,
                b.queue,
              ]),
            ),
            queueConsumers: Object.fromEntries(
              (config.config.queues?.consumers ?? []).map((b) => [
                b.queue,
                {
                  maxBatchSize: b.max_batch_size,
                  maxBatchTimeout: b.max_batch_timeout,
                  maxRetries: b.max_retries,
                  retryDelay: b.retry_delay,
                },
              ]),
            ),
            workflows: Object.fromEntries(
              (config.config.workflows ?? []).map((b) => [
                b.binding,
                { name: b.name, className: b.class_name },
              ]),
            ),
            workerLoaders: Object.fromEntries(
              (config.config.worker_loaders ?? []).map((b) => [b.binding, {}]),
            ),
            kvPersist: resolve(config.directory, "kv"),
            d1Persist: resolve(config.directory, "d1"),
            r2Persist: resolve(config.directory, "r2"),
            durableObjects: {
              [config.binding.name]: {
                className: config.binding.className,
                useSQLite: true,
              },
            },
            durableObjectsPersist: resolve(config.directory, "state"),
            host: "127.0.0.1",
            port: 0,
            cf: false,
            log: new Log(LogLevel.INFO),
          }),
        catch: (error) =>
          new TckError({ phase: "reference", message: String(error) }),
      }),
      (runtime) => Effect.promise(() => runtime.dispose()),
    );
    const url = yield* Effect.tryPromise({
      try: () => runtime.ready,
      catch: (error) =>
        new TckError({ phase: "reference", message: String(error) }),
    });
    const require = createRequire(import.meta.url);
    const fromMiniflare = createRequire(
      require.resolve("miniflare/package.json"),
    );
    const ready = {
      target: { name: config.name, baseUrl: url.origin },
      metadata: {
        engine: "workerd",
        miniflare: require("miniflare/package.json").version,
        workerd: fromMiniflare("workerd/package.json").version,
        fixtureSha256: config.sha256,
      },
    };
    // Atomic publish: the parent must never parse a half-written JSON document.
    const pending = resolve(config.directory, "ready.tmp");
    yield* fs.writeFileString(pending, JSON.stringify(ready));
    yield* fs.rename(pending, resolve(config.directory, "ready.json"));
    yield* Effect.never;
  }),
);

program.pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain);
