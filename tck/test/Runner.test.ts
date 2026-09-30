import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { vi } from "vitest";
import { artifactsLayer } from "../src/Artifacts.js";
import { Transport, type Report } from "../src/Domain.js";
import { Processes } from "../src/Processes.js";
import { runSuite } from "../src/Runner.js";

vi.mock("../src/Reference.js", () => ({
  acquireReference: () =>
    Effect.succeed({
      target: { name: "reference", baseUrl: "unused" },
      metadata: { engine: "mock" },
    }),
}));

it.effect(
  "full API run retains host and all fixture provenance separately",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      // Real selection, builds, loop and aggregate reporting; transport is deliberately invalid.
      yield* Effect.exit(
        runSuite({
          runId: "provenance",
          profile: "reference",
          seed: 42,
          caseId: "",
        }).pipe(
          Effect.provide(artifactsLayer(directory)),
          Effect.provideService(Processes, {
            run: () => Effect.succeed({ stdout: "test", stderr: "" }),
          }),
          Effect.provideService(Transport, {
            request: () =>
              Effect.succeed({
                status: 200,
                headers: { "content-type": "application/json" },
                body: { value: "ok" },
              }),
            websocket: () => Effect.succeed({}),
          }),
        ),
      );
      const report = JSON.parse(
        yield* fs.readFileString(directory + "/report.json"),
      ) as Report;
      expect(report.environment.hostNode).toBe(process.version);
      const fixtures = report.environment.fixtures as Record<
        string,
        Record<string, unknown>
      >;
      expect(Object.keys(fixtures)).toEqual([
        "core",
        "node",
        "flags",
        "extensions",
      ]);
      expect(fixtures.node?.compatibilityFlags).toEqual(["nodejs_compat"]);
      expect(fixtures.flags?.compatibilityFlags).toEqual([
        "delete_all_preserves_alarm",
        "no_websocket_standard_binary_type",
      ]);
      expect(fixtures.core?.compatibilityFlags).toEqual([]);
      for (const fixture of Object.values(fixtures)) {
        expect(fixture.fixtureSha256).toMatch(/^[a-f0-9]{64}$/);
        expect(fixture.compatibilityDate).toBe("2026-07-30");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);
