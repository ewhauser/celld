import { Clock, Effect, Exit, FileSystem, Schema } from "effect";
import { resolve } from "node:path";
import { Artifacts, decodeJson } from "./Artifacts.js";
import { TckError, type Bundle, type CaseResult } from "./Domain.js";
import { toolDeploy } from "./Compose.js";

export const rejectionConfigs = [
  {
    id: "deployment.routes",
    patch: { routes: ["example.test/*"] },
    diagnostic: "does not support these config keys: routes",
  },
  {
    id: "deployment.unsupported-binding",
    patch: { vectorize: [{ binding: "VECTOR", index_name: "test" }] },
    diagnostic: "does not support these config keys: vectorize",
  },
  // A compatibility switch paired with its own disable counterpart is not
  // registered here: celld v0.5.0 accepts it at deploy time. See
  // RUNTIME-BREADTH.md for the reproduction and the workerd contrast.
  {
    id: "deployment.invalid-name",
    patch: { name: "INVALID_NAME" },
    diagnostic: "config `name` must contain",
  },
  {
    id: "deployment.legacy-migration",
    patch: { migrations: [{ tag: "v2", new_classes: ["Probe"] }] },
    diagnostic: "new_classes",
  },
  {
    id: "deployment.missing-class",
    patch: { durable_objects: { bindings: [{ name: "PROBE" }] } },
    diagnostic: "class_name",
  },
] as const;

export const deploymentIds = [
  "deployment.valid-config",
  ...rejectionConfigs.map((test) => test.id),
];

export const checkDeployment = (
  bundle: Bundle,
  compose: (
    args: ReadonlyArray<string>,
  ) => Effect.Effect<{ stdout: string; stderr: string }, TckError>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const artifacts = yield* Artifacts;
    // Successful control proves an unavailable Docker daemon or broken CLI cannot
    // masquerade as an expected configuration rejection.
    const start = yield* Clock.currentTimeMillis;
    const control = yield* toolDeploy(compose, "/fixture", { dryRun: true });
    yield* decodeJson(
      Schema.Struct({
        dry_run: Schema.Literal(true),
        worker: Schema.Literal(bundle.config.name),
      }),
      control.stdout,
    );
    const observations: CaseResult[] = [
      {
        id: "deployment.valid-config",
        status: "pass",
        durationMs: (yield* Clock.currentTimeMillis) - start,
      },
    ];
    for (const test of rejectionConfigs) {
      const started = yield* Clock.currentTimeMillis;
      const file = `${test.id}.json`;
      yield* fs.writeFileString(
        resolve(bundle.directory, file),
        JSON.stringify({
          ...bundle.config,
          main: "worker.js",
          no_bundle: true,
          ...test.patch,
        }),
      );
      const result = yield* Effect.exit(
        toolDeploy(compose, `/fixture/${file}`, { dryRun: true }),
      );
      // Failures carry structured stderr in the typed error; Cause.pretty includes it
      // only when rendered, so inspect the failure with Effect.catch below instead.
      const detail = yield* Exit.isFailure(result)
        ? Effect.failCause(result.cause).pipe(
            Effect.catch((e) => Effect.succeed(e.detail ?? e.message)),
          )
        : Effect.succeed("accepted");
      const passed = Exit.isFailure(result) && detail.includes(test.diagnostic);
      observations.push({
        id: test.id,
        status: passed ? "pass" : "fail",
        durationMs: (yield* Clock.currentTimeMillis) - started,
        candidate: { rejected: Exit.isFailure(result), diagnostic: detail },
        ...(!passed
          ? { error: `Expected rejection containing ${test.diagnostic}` }
          : {}),
      });
      yield* artifacts.json("deployment-checks.json", observations);
    }
    return observations;
  });
