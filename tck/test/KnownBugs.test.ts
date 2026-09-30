import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { NodeServices } from "@effect/platform-node";
import { BugRegistry, validateBugRegistry } from "../src/KnownBugs.js";
import { decodeJson } from "../src/Artifacts.js";
import { cases } from "../src/Catalog.js";
import { equal, evaluate } from "../src/Oracle.js";
import { TckError, Transport, type TestCase } from "../src/Domain.js";

const reference = { name: "reference", baseUrl: "unused" };
const celld = {
  name: "candidate",
  baseUrl: "unused",
  engine: "celld" as const,
  version: "0.5.0",
};
const input = {
  compatibilityDate: "2026-07-30",
  compatibilityFlags: [] as string[],
  namespace: "test",
  seed: 42,
};
const expectation = {
  caseId: "test",
  bugIds: ["CELL-001"],
  celldVersion: "0.5.0",
  compatibilityDate: "2026-07-30",
  compatibilityFlags: [],
  evidenceRun: "test",
  candidate: 0,
};
const makeTest = (a = 42, b = 0): TestCase => ({
  id: "test",
  contract: "test",
  run: (target) => Effect.succeed(target.name === "reference" ? a : b),
  check: (value) => equal(value, 42),
  compare: equal,
});
const transport = {
  request: () => Effect.die("unused"),
  websocket: () => Effect.die("unused"),
};
it.effect("waives only exact version-scoped known failures", () =>
  Effect.gen(function* () {
    const run = (
      test = makeTest(),
      target = celld,
      policy: "allow" | "error" = "allow",
    ) => evaluate(test, reference, target, input, expectation, policy);
    const known = yield* run();
    expect(known.status).toBe("known-bug");
    expect(known.knownBugs).toEqual(["CELL-001"]);
    expect(known.candidate).toBe(0);
    expect((yield* run(makeTest(42, -1))).status).toBe("fail");
    expect(
      (yield* run(makeTest(), { ...celld, version: "0.6.0" })).status,
    ).toBe("fail");
    expect((yield* run(makeTest(), celld, "error")).status).toBe("fail");
    expect((yield* run(makeTest(42, 42))).error).toContain(
      "Unexpected compatibility pass",
    );
    expect((yield* run(makeTest(0, 0))).status).toBe("reference-error");
    expect(
      (yield* evaluate(
        makeTest(42, 42),
        reference,
        { ...celld, engine: "workerd" },
        input,
        expectation,
      )).status,
    ).toBe("pass");
    expect(
      (yield* run({
        ...makeTest(),
        run: (target) =>
          target.name === "reference"
            ? Effect.succeed(42)
            : Effect.fail(new TckError({ phase: "http", message: "reset" })),
      })).status,
    ).toBe("fail");
    expect(
      (yield* run({
        ...makeTest(),
        check: (value) =>
          value === 42 ? Effect.void : Effect.die("broken assertion"),
      })).status,
    ).toBe("fail");
  }).pipe(Effect.provideService(Transport, transport)),
);

it.effect(
  "validates bug references and prevents broad or stale registrations",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const registry = yield* decodeJson(
        BugRegistry,
        yield* fs.readFileString("docs/bugs.json"),
      );
      yield* validateBugRegistry(registry, cases);
      for (const invalid of [
        { ...registry, bugs: [...registry.bugs, registry.bugs[0]!] },
        {
          ...registry,
          expectations: [...registry.expectations, registry.expectations[0]!],
        },
        {
          ...registry,
          expectations: [{ ...registry.expectations[0]!, caseId: "missing" }],
        },
        {
          ...registry,
          expectations: [{ ...registry.expectations[0]!, bugIds: ["missing"] }],
        },
        {
          ...registry,
          bugs: registry.bugs.map((bug) => ({
            ...bug,
            status: "fixed" as const,
          })),
        },
      ])
        expect(
          (yield* Effect.exit(validateBugRegistry(invalid, cases)))._tag,
        ).toBe("Failure");
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("known bugs reject changed dates and flags", () =>
  Effect.gen(function* () {
    for (const profile of [
      { compatibilityDate: "2026-08-01" },
      { compatibilityFlags: ["nodejs_compat"] },
    ])
      expect(
        (yield* evaluate(
          makeTest(),
          reference,
          celld,
          { ...input, ...profile },
          expectation,
        )).status,
      ).toBe("fail");
  }).pipe(Effect.provideService(Transport, transport)),
);
