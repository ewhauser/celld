import { expect, it } from "@effect/vitest";
import { Effect, Exit, FileSystem } from "effect";
import { cases } from "../src/Catalog.js";
import { validateCoverage, Coverage } from "../src/Coverage.js";
import { decodeJson } from "../src/Artifacts.js";
import { NodeServices } from "@effect/platform-node";
import { selectCases } from "../src/Runner.js";
it.effect("requires every declared case exactly once", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const manifest = yield* decodeJson(
      Coverage,
      yield* fs.readFileString("docs/coverage.json"),
    );
    yield* validateCoverage(cases, manifest);
    expect(
      Exit.isFailure(
        yield* Effect.exit(validateCoverage(cases.slice(1), manifest)),
      ),
    ).toBe(true);
    expect(
      Exit.isFailure(
        yield* Effect.exit(validateCoverage([...cases, cases[0]!], manifest)),
      ),
    ).toBe(true);
  }).pipe(Effect.provide(NodeServices.layer)),
);
it.effect(
  "rejects missing cases and selection outside the selected suite",
  () =>
    Effect.gen(function* () {
      expect(Exit.isFailure(yield* Effect.exit(selectCases("not-real")))).toBe(
        true,
      );
      expect(
        Exit.isFailure(yield* Effect.exit(selectCases("node.buffer", "core"))),
      ).toBe(true);
      expect(
        (yield* selectCases("", "node")).every(
          (test) => test.fixture === "node",
        ),
      ).toBe(true);
      // The flag variant suite must only select cases built with the variant's
      // own compatibility flags; its sibling default case stays on core.
      expect(
        (yield* selectCases("", "flags")).map((test) => [
          test.id,
          test.fixture ?? "core",
        ]),
      ).toEqual([
        ["flags.enabled-defaults", "core"],
        ["flags.disabled-counterparts", "flags"],
      ]);
    }),
);
it.effect("rejects lost updates even if the final counter is correct", () =>
  Effect.gen(function* () {
    const test = cases.find((test) => test.id === "concurrency.input-gates")!;
    const history = Array.from({ length: 12 }, (_, i) => ({
      status: 200,
      body: { before: i, after: i + 1, initialized: true },
    }));
    yield* test.check(history, { namespace: "test", seed: 42 });
    const corrupt = history.map((row) => ({ ...row, body: { ...row.body } }));
    corrupt[1]!.body.before = 0;
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          test.check(corrupt, { namespace: "test", seed: 42 }),
        ),
      ),
    ).toBe(true);
  }),
);

it.effect("keeps diagnostic reproductions outside the default corpus", () =>
  Effect.gen(function* () {
    const defaults = yield* selectCases("");
    const repros = yield* selectCases("", "repros");
    expect(defaults.map((test) => test.id).sort()).toEqual(
      cases
        .filter((test) => test.fixture !== "repro")
        .map((test) => test.id)
        .sort(),
    );
    expect(defaults.every((test) => test.fixture !== "repro")).toBe(true);
    expect(repros.map((test) => test.id)).toEqual([
      "repro.body-readers",
      "repro.storage-errors",
    ]);
    expect(repros.every((test) => test.fixture === "repro")).toBe(true);
    expect(cases).toHaveLength(defaults.length + repros.length);
  }),
);
