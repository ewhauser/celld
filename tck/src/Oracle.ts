import { deepStrictEqual } from "node:assert";
import { Cause, Clock, Effect, Exit } from "effect";
import {
  TckError,
  type CaseInput,
  type CaseResult,
  type Target,
  type TestCase,
} from "./Domain.js";

import type { KnownBugExpectation } from "./KnownBugs.js";

export const equal = (actual: unknown, expected: unknown) =>
  Effect.try({
    try: () => deepStrictEqual(actual, expected),
    catch: (error) =>
      new TckError({ phase: "assertion", message: String(error) }),
  });

/**
 * ewhauser/celld fork builds are versioned `<upstream>-ewhauser.<n>` and
 * inherit the upstream release's reviewed divergences and known bugs. Any other
 * version difference still requires review.
 */
export const matchesCelldVersion = (
  actual: string | undefined,
  expected: string,
) => actual?.replace(/-ewhauser\.\d+$/, "") === expected;

export const evaluate = (
  test: TestCase,
  reference: Target,
  candidate: Target,
  input: CaseInput,
  knownBug?: KnownBugExpectation,
  knownBugs: "allow" | "error" = "allow",
) =>
  Effect.gen(function* () {
    const start = yield* Clock.currentTimeMillis;
    let referenceValue: unknown;
    let candidateValue: unknown;
    let status: CaseResult["status"] = "reference-error";
    const exit = yield* Effect.exit(
      Effect.gen(function* () {
        referenceValue = yield* test
          .run(reference, input)
          .pipe(Effect.timeout("30 seconds"));
        yield* test.check(referenceValue, input);
        status = "fail";
        candidateValue = yield* test
          .run(candidate, input)
          .pipe(Effect.timeout("30 seconds"));
        // Both non-conformance paths (documented divergence and known bug) share
        // the same shape: guard the scope, prove the candidate still fails the
        // compatibility oracle, then assert the expectation-specific evidence.
        const expectNonConformance = (expectation: {
          readonly phase: string;
          readonly guard: boolean;
          readonly guardMessage: string;
          readonly unexpectedPassMessage: string;
          readonly finalCheck: Effect.Effect<void, TckError>;
        }) =>
          Effect.gen(function* () {
            if (!expectation.guard)
              return yield* Effect.fail(
                new TckError({
                  phase: expectation.phase,
                  message: expectation.guardMessage,
                }),
              );
            const conforms = yield* Effect.exit(
              test
                .check(candidateValue, input)
                .pipe(
                  Effect.andThen(test.compare(referenceValue, candidateValue)),
                ),
            );
            if (Exit.isSuccess(conforms))
              return yield* Effect.fail(
                new TckError({
                  phase: expectation.phase,
                  message: expectation.unexpectedPassMessage,
                }),
              );
            if (
              Cause.hasInterrupts(conforms.cause) ||
              Cause.hasDies(conforms.cause)
            )
              return yield* Effect.failCause(conforms.cause);
            yield* expectation.finalCheck;
          });
        if (test.divergence && candidate.engine === "celld") {
          yield* expectNonConformance({
            phase: "divergence",
            guard:
              matchesCelldVersion(
                candidate.version,
                test.divergence.celldVersion,
              ) && matchesProfile(input, test.divergence),
            guardMessage:
              "Divergence requires review for this celld version/compatibility profile",
            unexpectedPassMessage:
              "Unexpected compatibility pass; review the documented divergence",
            finalCheck: test.divergence.check(candidateValue),
          });
          status = "divergence";
          return;
        }
        if (knownBug && candidate.engine === "celld" && knownBugs === "allow") {
          yield* expectNonConformance({
            phase: "known-bugs",
            guard:
              knownBug.caseId === test.id &&
              matchesCelldVersion(candidate.version, knownBug.celldVersion) &&
              matchesProfile(input, knownBug),
            guardMessage:
              "Known bug requires review for this case/version/compatibility profile",
            unexpectedPassMessage:
              "Unexpected compatibility pass; retire the known-bug expectation",
            finalCheck: equal(candidateValue, knownBug.candidate),
          });
          status = "known-bug";
          return;
        }
        yield* test.check(candidateValue, input);
        yield* test.compare(referenceValue, candidateValue);
        status = "pass";
      }),
    );
    if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause))
      return yield* Effect.failCause(exit.cause);
    return {
      id: test.id,
      status,
      ...((status as CaseResult["status"]) === "divergence"
        ? {
            divergence: `${test.divergence?.reason} (${test.divergence?.source})`,
          }
        : {}),
      ...((status as CaseResult["status"]) === "known-bug" && knownBug
        ? { knownBugs: knownBug.bugIds }
        : {}),
      durationMs: (yield* Clock.currentTimeMillis) - start,
      ...(referenceValue === undefined ? {} : { reference: referenceValue }),
      ...(candidateValue === undefined ? {} : { candidate: candidateValue }),
      ...(Exit.isFailure(exit) ? { error: Cause.pretty(exit.cause) } : {}),
    } satisfies CaseResult;
  });

const matchesProfile = (
  input: CaseInput,
  expected: {
    readonly compatibilityDate: string;
    readonly compatibilityFlags: readonly string[];
  },
) =>
  input.compatibilityFlags !== undefined &&
  input.compatibilityDate === expected.compatibilityDate &&
  JSON.stringify([...(input.compatibilityFlags ?? [])].sort()) ===
    JSON.stringify([...expected.compatibilityFlags].sort());
