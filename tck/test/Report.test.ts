import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { Report } from "../src/Domain.js";
import { junit } from "../src/Report.js";
import { selectCases } from "../src/Runner.js";

it.effect("counts cleanup errors and escapes XML diagnostics", () =>
  Effect.sync(() => {
    const report: Report = {
      schemaVersion: 1,
      runId: "test",
      profile: "reference",
      seed: 0,
      startedAt: "now",
      completedAt: "now",
      environment: {},
      cases: [{ id: "x", status: "pass", durationMs: 1 }],
      errors: ['<cleanup> & "error"'],
      success: false,
    };
    const output = junit(report);
    expect(output).toContain('tests="2" failures="0" errors="1"');
    expect(output).toContain("&lt;cleanup&gt; &amp; &quot;error&quot;");
  }),
);
it.effect(
  "rejects malformed report status rather than serializing it as a pass",
  () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        Schema.decodeUnknownEffect(Report)({ cases: [{ status: "skipped" }] }),
      );
      expect(exit._tag).toBe("Failure");
    }),
);
it.effect(
  "rejects an empty case selection before starting infrastructure",
  () =>
    Effect.gen(function* () {
      expect((yield* Effect.exit(selectCases("does-not-exist")))._tag).toBe(
        "Failure",
      );
    }),
);

it.effect(
  "reports known bugs as skipped with identifiers rather than passes",
  () =>
    Effect.sync(() => {
      const output = junit({
        schemaVersion: 1,
        runId: "test",
        profile: "local",
        seed: 0,
        startedAt: "now",
        completedAt: "now",
        environment: {},
        errors: [],
        success: true,
        cases: [
          {
            id: "x",
            status: "known-bug",
            durationMs: 1,
            knownBugs: ["CELL-001", "CELL-002"],
          },
        ],
      });
      expect(output).toContain('failures="0" errors="0" skipped="1"');
      expect(output).toContain(
        '<skipped message="Known bugs: CELL-001, CELL-002"/>',
      );
    }),
);
