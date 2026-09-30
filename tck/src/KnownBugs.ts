import { Effect, Schema } from "effect";
import { TckError, type TestCase } from "./Domain.js";

export const KnownBugExpectation = Schema.Struct({
  caseId: Schema.String,
  bugIds: Schema.Array(Schema.String),
  celldVersion: Schema.String,
  compatibilityDate: Schema.String,
  compatibilityFlags: Schema.Array(Schema.String),
  evidenceRun: Schema.String,
  candidate: Schema.Unknown,
});
export type KnownBugExpectation = typeof KnownBugExpectation.Type;
export const BugRegistry = Schema.Struct({
  bugs: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      title: Schema.String,
      status: Schema.Literals(["open", "fixed"]),
      report: Schema.String,
      owner: Schema.String,
      reviewedAt: Schema.String,
      upstreamIssue: Schema.NullOr(Schema.String),
    }),
  ),
  expectations: Schema.Array(KnownBugExpectation),
});
export const validateBugRegistry = (
  registry: typeof BugRegistry.Type,
  cases: readonly TestCase[],
) =>
  Effect.gen(function* () {
    const bugs = new Map(registry.bugs.map((bug) => [bug.id, bug]));
    const seen = new Set<string>();
    const invalid = (message: string) =>
      Effect.fail(new TckError({ phase: "known-bugs", message }));
    if (bugs.size !== registry.bugs.length)
      return yield* invalid("Duplicate bug IDs");
    for (const entry of registry.expectations) {
      const test = cases.find((test) => test.id === entry.caseId);
      if (!test || test.divergence || seen.has(entry.caseId))
        return yield* invalid(
          `Invalid or duplicate known-bug case: ${entry.caseId}`,
        );
      seen.add(entry.caseId);
      if (
        !entry.celldVersion ||
        !entry.evidenceRun ||
        !entry.bugIds.length ||
        new Set(entry.bugIds).size !== entry.bugIds.length ||
        entry.bugIds.some((id) => bugs.get(id)?.status !== "open")
      )
        return yield* invalid(`Invalid known-bug expectation: ${entry.caseId}`);
    }
  });
