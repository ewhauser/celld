import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Fiber } from "effect";
import { TckError, Transport, type TestCase } from "../src/Domain.js";
import { equal, evaluate, matchesCelldVersion } from "../src/Oracle.js";

const reference = { name: "reference", baseUrl: "http://unused.invalid" };
const candidate = { name: "candidate", baseUrl: "http://unused.invalid" };
const input = {
  compatibilityDate: "2026-07-30",
  compatibilityFlags: [] as string[],
  namespace: "test",
  seed: 42,
};
const testCase = (a: unknown, b: unknown): TestCase => ({
  id: "oracle.self-check",
  contract: "test",
  run: (target) => Effect.succeed(target.name === "reference" ? a : b),
  check: (value) => equal(value, { value: 42 }),
  compare: equal,
});
const run = (test: TestCase) =>
  evaluate(test, reference, candidate, input).pipe(
    Effect.provideService(Transport, {
      websocket: () => Effect.die("No sockets in unit tests"),
      request: () => Effect.die("Oracle unit tests must not perform HTTP"),
    }),
  );

it.effect("accepts two independently correct observations", () =>
  Effect.gen(function* () {
    expect((yield* run(testCase({ value: 42 }, { value: 42 }))).status).toBe(
      "pass",
    );
  }),
);
it.effect("never accepts matching wrong observations", () =>
  Effect.gen(function* () {
    expect((yield* run(testCase({ value: 0 }, { value: 0 }))).status).toBe(
      "reference-error",
    );
  }),
);
it.effect("preserves the deliberately corrupted candidate in a failure", () =>
  Effect.gen(function* () {
    const result = yield* run(testCase({ value: 42 }, { value: 43 }));
    expect(result.status).toBe("fail");
    expect(result.candidate).toEqual({ value: 43 });
    expect(result.reference).toEqual({ value: 42 });
  }),
);
it.effect("uses the differential comparator even with a weak invariant", () =>
  Effect.gen(function* () {
    const result = yield* run({ ...testCase(1, 2), check: () => Effect.void });
    expect(result.status).toBe("fail");
  }),
);
it.effect(
  "keeps transport failure separate from a successful empty response",
  () =>
    Effect.gen(function* () {
      const result = yield* run({
        ...testCase(1, 2),
        run: () =>
          Effect.fail(
            new TckError({ phase: "http", message: "connection reset" }),
          ),
      });
      expect(result.status).toBe("reference-error");
      expect(result.error).toContain("connection reset");
    }),
);

const divergent = {
  ...testCase({ value: 42 }, { value: 0 }),
  divergence: {
    celldVersion: "0.5.0",
    compatibilityDate: "2026-07-30",
    compatibilityFlags: [],
    source: "https://example.test/contract",
    reason: "test divergence",
    reviewDate: "2026-09-18",
    owner: "celld-tck maintainers",
    check: (value: unknown) => equal(value, { value: 0 }),
  },
};
const celld = { ...candidate, engine: "celld" as const, version: "0.5.0" };
it.effect("accepts only a version-scoped exact documented divergence", () =>
  Effect.gen(function* () {
    expect((yield* evaluate(divergent, reference, celld, input)).status).toBe(
      "divergence",
    );
    expect(
      (yield* evaluate(
        divergent,
        reference,
        { ...celld, version: "0.6.0" },
        input,
      )).status,
    ).toBe("fail");
    expect(
      (yield* evaluate(
        {
          ...divergent,
          run: (target) =>
            Effect.succeed(
              target.name === "reference" ? { value: 42 } : { value: -1 },
            ),
        },
        reference,
        celld,
        input,
      )).status,
    ).toBe("fail");
  }).pipe(
    Effect.provideService(Transport, {
      request: () => Effect.die("unused"),
      websocket: () => Effect.die("unused"),
    }),
  ),
);
it.effect(
  "requires review when a documented divergence unexpectedly conforms",
  () =>
    Effect.gen(function* () {
      const result = yield* evaluate(
        { ...divergent, run: () => Effect.succeed({ value: 42 }) },
        reference,
        celld,
        input,
      );
      expect(result.status).toBe("fail");
      expect(result.error).toContain("Unexpected compatibility pass");
    }).pipe(
      Effect.provideService(Transport, {
        request: () => Effect.die("unused"),
        websocket: () => Effect.die("unused"),
      }),
    ),
);

it.effect("never waives divergent checker defects or interruptions", () =>
  Effect.gen(function* () {
    for (const phase of ["check", "compare"] as const) {
      const broken = {
        ...divergent,
        check: (value: unknown) =>
          value && (value as { value: number }).value === 42
            ? Effect.void
            : Effect.die("broken checker"),
        ...(phase === "compare"
          ? {
              check: () => Effect.void,
              compare: () => Effect.die("broken checker"),
            }
          : {}),
      };
      const result = yield* evaluate(broken, reference, celld, input);
      expect(result.status).toBe("fail");
      expect(result.error).toContain("broken checker");
    }
  }).pipe(
    Effect.provideService(Transport, {
      request: () => Effect.die("unused"),
      websocket: () => Effect.die("unused"),
    }),
  ),
);

it.effect("divergences reject changed or missing compatibility profiles", () =>
  Effect.gen(function* () {
    for (const profile of [
      { compatibilityDate: "2026-08-01" },
      { compatibilityFlags: ["nodejs_compat"] },
    ]) {
      const result = yield* evaluate(divergent, reference, celld, {
        ...input,
        ...profile,
      });
      expect(result.status).toBe("fail");
    }
  }).pipe(
    Effect.provideService(Transport, {
      request: () => Effect.die("unused"),
      websocket: () => Effect.die("unused"),
    }),
  ),
);

it.effect("divergence check and compare interruptions propagate", () =>
  Effect.gen(function* () {
    for (const phase of ["check", "compare"] as const) {
      let waived = false;
      const broken: TestCase = {
        ...divergent,
        check: (value) =>
          phase === "check" && (value as { value: number }).value !== 42
            ? Effect.interrupt
            : Effect.void,
        compare: () => (phase === "compare" ? Effect.interrupt : Effect.void),
        divergence: {
          ...divergent.divergence,
          check: () =>
            Effect.sync(() => {
              waived = true;
            }),
        },
      };
      const fiber = yield* evaluate(broken, reference, celld, input).pipe(
        Effect.forkChild,
      );
      const exit = yield* Fiber.await(fiber);
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(
        true,
      );
      expect(waived).toBe(false);
    }
  }).pipe(
    Effect.provideService(Transport, {
      request: () => Effect.die("unused"),
      websocket: () => Effect.die("unused"),
    }),
  ),
);
it("scopes fork builds to the upstream release they carry", () => {
  expect(matchesCelldVersion("0.6.0", "0.6.0")).toBe(true);
  expect(matchesCelldVersion("0.6.0-ewhauser.2", "0.6.0")).toBe(true);
  expect(matchesCelldVersion("0.6.1-ewhauser.1", "0.6.0")).toBe(false);
  expect(matchesCelldVersion("0.6.0-rc.1", "0.6.0")).toBe(false);
  expect(matchesCelldVersion("0.6.0-ewhauser", "0.6.0")).toBe(false);
  expect(matchesCelldVersion(undefined, "0.6.0")).toBe(false);
});
it.effect(
  "accepts a documented divergence on a fork build of its release",
  () =>
    Effect.gen(function* () {
      expect(
        (yield* evaluate(
          divergent,
          reference,
          { ...celld, version: "0.5.0-ewhauser.3" },
          input,
        )).status,
      ).toBe("divergence");
    }).pipe(
      Effect.provideService(Transport, {
        request: () => Effect.die("unused"),
        websocket: () => Effect.die("unused"),
      }),
    ),
);
