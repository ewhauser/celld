import { expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { Artifacts } from "../src/Artifacts.js";
import { TckError, type Report } from "../src/Domain.js";
import { makeSuiteExecutor } from "../src/SuiteExecutor.js";
import { owned } from "../src/Resources.js";

const harness = () => {
  const files = new Map<string, unknown>();
  const artifacts = {
    directory: "evidence",
    json: (name: string, value: unknown) =>
      Effect.sync(() => {
        files.set(name, value);
      }),
    text: (name: string, value: string) =>
      Effect.sync(() => {
        files.set(name, value);
      }),
  };
  return { files, artifacts, report: () => files.get("report.json") as Report };
};
const options = {
  runId: "test",
  profile: "local" as const,
  seed: 42,
  ids: ["a", "b"],
  environment: {},
};
const failure = new TckError({
  phase: "assertion",
  message: "bad observation",
});

it.effect("records all selected cases and rejects missing results", () =>
  Effect.gen(function* () {
    const h = harness();
    yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor(options);
      const exit = yield* Effect.exit(
        suite.execute(suite.runCase("a", () => Effect.succeed({ value: 1 }))),
      );
      expect(exit._tag).toBe("Failure");
      expect(h.report().cases.map((c) => c.status)).toEqual([
        "pass",
        "infrastructure-error",
      ]);
      expect(h.report().counts?.pass).toBe(1);
      expect(h.report().success).toBe(false);
      expect(h.files.get("case-a.json")).toEqual(h.report().cases[0]);
    }).pipe(Effect.provideService(Artifacts, h.artifacts));
  }),
);

for (const policy of ["strict", "compatibility"] as const)
  it.effect(`${policy} policy handles preclassified API results`, () =>
    Effect.gen(function* () {
      const h = harness();
      yield* Effect.gen(function* () {
        const suite = yield* makeSuiteExecutor({ ...options, policy });
        const exit = yield* Effect.exit(
          suite.execute(
            Effect.gen(function* () {
              yield* suite.record({
                id: "a",
                status: "divergence",
                durationMs: 1,
              });
              yield* suite.record({
                id: "b",
                status: "known-bug",
                durationMs: 1,
                knownBugs: ["bug"],
              });
            }),
          ),
        );
        expect(Exit.isSuccess(exit)).toBe(policy === "compatibility");
        expect(h.report().success).toBe(policy === "compatibility");
        expect(h.files.get("junit.xml")).toContain('skipped="2"');
      }).pipe(Effect.provideService(Artifacts, h.artifacts));
    }),
  );

it.effect("classifies setup separately and continues independent cases", () =>
  Effect.gen(function* () {
    const h = harness();
    yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor(options);
      yield* Effect.exit(
        suite.execute(
          Effect.gen(function* () {
            yield* suite.runCase("a", () => Effect.fail(failure), {
              includesSetup: true,
            });
            yield* suite.runCase(
              "b",
              (test) => test.ready.pipe(Effect.andThen(Effect.fail(failure))),
              { includesSetup: true },
            );
          }),
        ),
      );
      expect(h.report().cases.map((c) => c.status)).toEqual([
        "infrastructure-error",
        "fail",
      ]);
      expect(h.report().errors).toEqual([]);
    }).pipe(Effect.provideService(Artifacts, h.artifacts));
  }),
);

it.effect("stops dependent stages and preserves the original cause", () =>
  Effect.gen(function* () {
    const h = harness();
    yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor(options);
      const exit = yield* Effect.exit(
        suite.execute(
          Effect.gen(function* () {
            yield* suite.runCase("a", () => Effect.fail(failure), {
              onFailure: "stop",
            });
            yield* suite.runCase("b", () => Effect.void);
          }),
        ),
      );
      expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
        "bad observation",
      );
      expect(h.report().cases.map((c) => c.status)).toEqual([
        "fail",
        "infrastructure-error",
      ]);
    }).pipe(Effect.provideService(Artifacts, h.artifacts));
  }),
);

for (const phase of ["setup", "execution"] as const)
  it.effect(`reports cancellation during ${phase} after cleanup`, () =>
    Effect.gen(function* () {
      const h = harness();
      const entered = yield* Deferred.make<void>();
      let cleaned = false;
      const fiber = yield* Effect.gen(function* () {
        const suite = yield* makeSuiteExecutor(options);
        yield* suite.execute(
          suite.runCase(
            "a",
            (test) =>
              Effect.gen(function* () {
                yield* owned(
                  Effect.void,
                  Effect.sync(() => {
                    cleaned = true;
                  }),
                  suite.cleanupError,
                );
                if (phase === "execution") yield* test.ready;
                yield* Deferred.succeed(entered, undefined);
                yield* Effect.never;
              }),
            { includesSetup: true },
          ),
        );
      }).pipe(
        Effect.provideService(Artifacts, {
          ...h.artifacts,
          json: (name, value) => {
            if (name === "report.json") expect(cleaned).toBe(true);
            return h.artifacts.json(name, value);
          },
        }),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(
        true,
      );
      expect(h.report().success).toBe(false);
      expect(h.report().cases[0]?.status).toBe(
        phase === "setup" ? "infrastructure-error" : "fail",
      );
      expect(h.report().cases[1]?.error).toBe("Case not reached");
      expect(h.report().errors.join(" ")).toContain("interrupt");
      expect(h.files.get("junit.xml")).toContain("testcase");
    }),
  );

it.effect("retains setup failure plus cleanup failure", () =>
  Effect.gen(function* () {
    const h = harness();
    yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor(options);
      const exit = yield* Effect.exit(
        suite.execute(
          owned(
            Effect.fail(failure),
            Effect.fail("cleanup broke"),
            suite.cleanupError,
          ),
        ),
      );
      expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
        "bad observation",
      );
      expect(h.report().errors.join(" ")).toContain("cleanup broke");
      expect(h.report().errors.join(" ")).toContain("bad observation");
      expect(h.report().success).toBe(false);
    }).pipe(Effect.provideService(Artifacts, h.artifacts));
  }),
);

it.effect("cleanup failure fails an otherwise passing suite", () =>
  Effect.gen(function* () {
    const h = harness();
    yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor({ ...options, ids: ["a"] });
      const exit = yield* Effect.exit(
        suite.execute(
          Effect.gen(function* () {
            yield* owned(
              Effect.void,
              Effect.fail("cleanup broke"),
              suite.cleanupError,
            );
            yield* suite.runCase("a", () => Effect.void);
          }),
        ),
      );
      expect(exit._tag).toBe("Failure");
      expect(h.report().cases[0]?.status).toBe("pass");
      expect(h.report().success).toBe(false);
    }).pipe(Effect.provideService(Artifacts, h.artifacts));
  }),
);

for (const level of ["case", "suite"] as const)
  it.effect(`${level} deadline reports timeout and cleans resources`, () =>
    Effect.gen(function* () {
      const h = harness();
      const entered = yield* Deferred.make<void>();
      let cleaned = false;
      const fiber = yield* Effect.gen(function* () {
        const suite = yield* makeSuiteExecutor({
          ...options,
          ...(level === "suite" ? { timeout: "1 second" as const } : {}),
        });
        yield* suite.execute(
          suite.runCase(
            "a",
            () =>
              Effect.gen(function* () {
                yield* owned(
                  Effect.void,
                  Effect.sync(() => {
                    cleaned = true;
                  }),
                  suite.cleanupError,
                );
                yield* Deferred.succeed(entered, undefined);
                yield* Effect.never;
              }),
            level === "case" ? { timeout: "1 second" } : {},
          ),
        );
      }).pipe(Effect.provideService(Artifacts, h.artifacts), Effect.forkChild);
      yield* Deferred.await(entered);
      yield* TestClock.adjust("2 seconds");
      const exit = yield* Fiber.await(fiber);
      expect(exit._tag).toBe("Failure");
      expect(cleaned).toBe(true);
      expect(h.report().success).toBe(false);
      expect(h.files.has("junit.xml")).toBe(true);
    }),
  );

it.effect("preserves defects and rejects duplicate or unknown results", () =>
  Effect.gen(function* () {
    const h = harness();
    yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor(options);
      yield* Effect.exit(
        suite.execute(
          Effect.gen(function* () {
            yield* suite.runCase("a", () => Effect.die("checker broke"));
            yield* suite.record({ id: "b", status: "pass", durationMs: 0 });
            expect(
              (yield* Effect.exit(
                suite.record({ id: "b", status: "pass", durationMs: 0 }),
              ))._tag,
            ).toBe("Failure");
            expect(
              (yield* Effect.exit(
                suite.record({ id: "unknown", status: "pass", durationMs: 0 }),
              ))._tag,
            ).toBe("Failure");
          }),
        ),
      );
      expect(h.report().cases[0]?.error).toContain("checker broke");
      expect(h.report().success).toBe(false);
    }).pipe(Effect.provideService(Artifacts, h.artifacts));
  }),
);

it.effect(
  "attempts both reports when one writer fails, preserving the original failure",
  () =>
    Effect.gen(function* () {
      const h = harness();
      const exit = yield* Effect.exit(
        Effect.gen(function* () {
          const suite = yield* makeSuiteExecutor(options);
          yield* suite.execute(Effect.fail(failure));
        }).pipe(
          Effect.provideService(Artifacts, {
            ...h.artifacts,
            json: (name, value) =>
              name === "report.json"
                ? Effect.fail(
                    new TckError({ phase: "artifact", message: "disk full" }),
                  )
                : h.artifacts.json(name, value),
          }),
        ),
      );
      expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
        "bad observation",
      );
      expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
        "disk full",
      );
      expect(h.files.has("junit.xml")).toBe(true);
    }),
);

it.effect(
  "writes case evidence to its fixture and aggregate evidence to the suite root",
  () =>
    Effect.gen(function* () {
      const root = harness();
      const fixture = harness();
      yield* Effect.gen(function* () {
        const suite = yield* makeSuiteExecutor({ ...options, ids: ["a"] });
        yield* suite.execute(
          suite
            .record({ id: "a", status: "pass", durationMs: 3 })
            .pipe(Effect.provideService(Artifacts, fixture.artifacts)),
        );
        expect(root.report().success).toBe(true);
        expect(root.report().counts?.pass).toBe(1);
        expect(root.files.has("case-a.json")).toBe(false);
        expect(fixture.files.get("case-a.json")).toEqual(
          root.report().cases[0],
        );
        expect(fixture.files.has("report.json")).toBe(false);
      }).pipe(Effect.provideService(Artifacts, root.artifacts));
    }),
);

it.effect(
  "case artifact failure preserves scenario failure and prevents continuing",
  () =>
    Effect.gen(function* () {
      const h = harness();
      let continued = false;
      const exit = yield* Effect.exit(
        Effect.gen(function* () {
          const suite = yield* makeSuiteExecutor(options);
          yield* suite.execute(
            Effect.gen(function* () {
              yield* suite.runCase("a", () => Effect.fail(failure));
              continued = true;
            }),
          );
        }).pipe(
          Effect.provideService(Artifacts, {
            ...h.artifacts,
            json: (name, value) =>
              name === "case-a.json"
                ? Effect.fail(
                    new TckError({
                      phase: "artifact",
                      message: "case disk full",
                    }),
                  )
                : h.artifacts.json(name, value),
          }),
        ),
      );
      expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
        "bad observation",
      );
      expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
        "case disk full",
      );
      expect(h.report().success).toBe(false);
      expect(continued).toBe(false);
    }),
);

it.effect("cancellation before any case preserves every placeholder", () =>
  Effect.gen(function* () {
    const h = harness();
    const entered = yield* Deferred.make<void>();
    const fiber = yield* Effect.gen(function* () {
      const suite = yield* makeSuiteExecutor(options);
      yield* suite.execute(
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      );
    }).pipe(Effect.provideService(Artifacts, h.artifacts), Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(fiber);
    expect(h.report().cases.map((c) => c.error)).toEqual([
      "Case not reached",
      "Case not reached",
    ]);
    expect(h.report().success).toBe(false);
  }),
);

it.effect(
  "invalid selection or repeated execution never runs scenario work",
  () =>
    Effect.gen(function* () {
      const h = harness();
      yield* Effect.gen(function* () {
        for (const ids of [[], ["a", "a"]])
          expect(
            (yield* Effect.exit(makeSuiteExecutor({ ...options, ids })))._tag,
          ).toBe("Failure");
        const suite = yield* makeSuiteExecutor({ ...options, ids: ["a"] });
        let calls = 0;
        const work = () =>
          Effect.sync(() => {
            calls++;
          });
        yield* suite.execute(
          Effect.gen(function* () {
            expect(
              (yield* Effect.exit(suite.runCase("unknown", work)))._tag,
            ).toBe("Failure");
            yield* suite.runCase("a", work);
            expect((yield* Effect.exit(suite.runCase("a", work)))._tag).toBe(
              "Failure",
            );
          }),
        );
        expect(calls).toBe(1);
      }).pipe(Effect.provideService(Artifacts, h.artifacts));
    }),
);

it.effect(
  "compatibility policy still rejects ordinary failures and reference errors",
  () =>
    Effect.gen(function* () {
      const h = harness();
      yield* Effect.gen(function* () {
        const suite = yield* makeSuiteExecutor({
          ...options,
          policy: "compatibility",
        });
        const exit = yield* Effect.exit(
          suite.execute(
            Effect.gen(function* () {
              yield* suite.record({
                id: "a",
                status: "fail",
                durationMs: 1,
                error: "candidate",
              });
              yield* suite.record({
                id: "b",
                status: "reference-error",
                durationMs: 1,
                error: "reference",
              });
            }),
          ),
        );
        expect(exit._tag).toBe("Failure");
        expect(h.report().success).toBe(false);
        expect(h.files.get("junit.xml")).toContain('failures="1" errors="1"');
      }).pipe(Effect.provideService(Artifacts, h.artifacts));
    }),
);
