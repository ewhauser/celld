# Case oracle mutation tests

`test/CaseOracles.test.ts` imports the real API case registry and runs each case's `check` and `compare` through `evaluate`. Only the acquisition of observations is replaced. It tests oracle behavior; it does not run modified Workers or replace the integration suites.

The checked-in observations are independent controls captured from successful workerd reference runs with seed 42. Their source run IDs and revisions are recorded in `observations.json`. The two documented divergence controls came from the corresponding successful local run. These small observation records are committed so contributors can run the tests without Docker, celld, AWS, or an ignored artifact directory. They are not generated from the current checker or its expected-value function.

## Adding a case

1. Implement the fixture operation and case oracle, then register its ID in the normal case catalog and coverage manifest.
2. Run the case against the reference profile, for example `pnpm tck --profile reference --suite core --case storage.new-case --seed 42`. Review the observation against the intended contract and add it under `cases` in `observations.json`. Record the new control's source run ID and revision. Preserve existing controls unless the contract or observation format intentionally changes.
3. Add at least one entry under the same ID in `Mutations.ts`. Name the broken behavior, choose an existing observation field, and replace it with a plausible wrong result. Rollback tests should retain an uncommitted write; isolation tests should leak state; retry tests should lose a retry. Cases covering several distinct behaviors should have separate mutations for those behaviors.
4. If the case permits a documented divergence, add its reviewed positive control under `divergences` and a mutation of that control under `divergenceMutations`. Known-bug registrations also need an additional-corruption mutation in `CaseOracles.test.ts`.
5. Run `pnpm check` and the affected runtime suite. If a mutation survives, strengthen the actual oracle or observation. Do not weaken the negative example to manufacture a green test.

The coverage gate compares the complete catalog to both observation and mutation registries. Adding a case without either fails CI. It also rejects empty registrations, unnamed or duplicate mutations, and mutations that only change HTTP envelope fields. Mutation application rejects stale paths and unchanged values.

For every semantic mutation, the registered checker must fail with an assertion error; a checker defect or interruption is not a passing negative test. A good reference plus a mutated candidate must produce `fail`, and matching mutated observations must produce `reference-error`. Assertions also retain the corrupted observation as failure evidence. Separate tests preserve valid concurrency ordering variations and valid alternative compression encodings.

This corpus covers all API fixture families, including diagnostic repros. Recovery and qualification scenarios have different execution contracts and keep their targeted negative tests alongside their invariant helpers, such as `History.test.ts`, `Outage.test.ts`, and `QualificationOracles.test.ts`. New cases in those suites need corresponding deliberate bad observations as required by `AGENTS.md`.
