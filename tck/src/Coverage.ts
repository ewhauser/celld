import { Effect, Schema } from "effect";
import { TckError, type TestCase } from "./Domain.js";
import { equal } from "./Oracle.js";
export const Coverage = Schema.Struct({
  version: Schema.Literal(1),
  scope: Schema.String,
  families: Schema.Array(
    Schema.Struct({
      family: Schema.String,
      cases: Schema.Array(Schema.String),
    }),
  ),
  excluded: Schema.Array(
    Schema.Struct({ family: Schema.String, reason: Schema.String }),
  ),
});
export const validateCoverage = (
  cases: ReadonlyArray<TestCase>,
  manifest: typeof Coverage.Type,
) =>
  Effect.gen(function* () {
    const actual = cases.map((test) => test.id);
    const expected = manifest.families.flatMap((family) => family.cases);
    yield* equal(new Set(actual).size, actual.length);
    yield* equal(new Set(expected).size, expected.length);
    yield* equal([...actual].sort(), [...expected].sort());
    for (const test of cases) {
      if (!test.contract.startsWith("https://"))
        return yield* Effect.fail(
          new TckError({
            phase: "coverage",
            message: `Missing contract for ${test.id}`,
          }),
        );
      if (
        test.divergence &&
        (!test.divergence.source ||
          !test.divergence.reviewDate ||
          !test.divergence.owner ||
          !test.divergence.celldVersion)
      )
        return yield* Effect.fail(
          new TckError({
            phase: "coverage",
            message: `Unscoped divergence for ${test.id}`,
          }),
        );
    }
  });
