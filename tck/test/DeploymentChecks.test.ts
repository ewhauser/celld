import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { artifactsLayer } from "../src/Artifacts.js";
import { checkDeployment, rejectionConfigs } from "../src/DeploymentChecks.js";
import { TckError, type Bundle } from "../src/Domain.js";
const run = (mode: "correct" | "outage" | "accepted") =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped();
    const bundle: Bundle = {
      directory,
      source: "",
      sha256: "",
      modules: {},
      compatibilityDate: "2026-07-30",
      binding: { name: "PROBE", className: "Probe" },
      config: {
        name: "test",
        main: "worker.js",
        compatibility_date: "2026-07-30",
        compatibility_flags: [],
        durable_objects: { bindings: [] },
        migrations: [],
      },
    };
    return yield* checkDeployment(bundle, (args) => {
      const test = rejectionConfigs.find((test) =>
        args.includes(`/fixture/${test.id}.json`),
      );
      if (!test || mode === "accepted")
        return Effect.succeed({
          stdout: JSON.stringify({ dry_run: true, worker: "test" }),
          stderr: "",
        });
      return Effect.fail(
        new TckError({
          phase: "process",
          message: "command failed",
          detail:
            mode === "correct" ? test.diagnostic : "Docker daemon unavailable",
        }),
      );
    }).pipe(Effect.provide(artifactsLayer(directory)));
  }).pipe(Effect.provide(NodeServices.layer));
it.effect("requires the specific rejection, not just a failed process", () =>
  Effect.gen(function* () {
    expect(
      (yield* run("correct")).every((test) => test.status === "pass"),
    ).toBe(true);
    expect(
      (yield* run("outage")).filter((test) => test.status === "fail"),
    ).toHaveLength(rejectionConfigs.length);
    expect(
      (yield* run("accepted")).filter((test) => test.status === "fail"),
    ).toHaveLength(rejectionConfigs.length);
  }),
);
