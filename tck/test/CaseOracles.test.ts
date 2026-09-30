import { gzipSync, deflateSync } from "node:zlib";
import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, FileSystem } from "effect";
import { cases } from "../src/Catalog.js";
import {
  TckError,
  Transport,
  type CaseInput,
  type TestCase,
} from "../src/Domain.js";
import { evaluate } from "../src/Oracle.js";
import { decodeJson } from "../src/Artifacts.js";
import { BugRegistry } from "../src/KnownBugs.js";
import {
  applyMutation,
  assertCoverage,
  loadCorpus,
} from "./case-oracles/Corpus.js";
import {
  mutations,
  divergenceMutations,
  type Mutation,
} from "./case-oracles/Mutations.js";

const reference = {
  name: "reference",
  baseUrl: "http://unused.invalid",
  engine: "workerd" as const,
};
const candidate = { ...reference, name: "candidate" };
const celld = { ...candidate, engine: "celld" as const, version: "0.6.0" };
const transport = {
  request: () => Effect.die("Observation replay must not perform HTTP"),
  websocket: () => Effect.die("Observation replay must not open sockets"),
};
const inputFor = (
  test: TestCase,
  corpus: Effect.Success<typeof loadCorpus>,
): CaseInput => ({
  seed: corpus.seed,
  namespace: "oracle-mutations",
  compatibilityDate: corpus.compatibilityDate,
  compatibilityFlags:
    test.fixture === "node"
      ? ["nodejs_compat"]
      : test.fixture === "flags"
        ? ["delete_all_preserves_alarm", "no_websocket_standard_binary_type"]
        : [],
});
// Replace only acquisition of observations. These are the actual registered
// check/compare/divergence functions, including independent semantic invariants.
const replay = (test: TestCase, a: unknown, b: unknown): TestCase => ({
  ...test,
  run: (target) =>
    Effect.sync(() => structuredClone(target.name === reference.name ? a : b)),
});
const rejects = (test: Effect.Effect<void, TckError>) =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(test);
    expect(
      Exit.isFailure(exit),
      "The actual case checker accepted the semantic mutation",
    ).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(
        Cause.hasDies(exit.cause),
        "A broken checker is not a successful negative test",
      ).toBe(false);
      expect(Cause.hasInterrupts(exit.cause)).toBe(false);
      expect(Cause.squash(exit.cause)).toBeInstanceOf(TckError);
      expect((Cause.squash(exit.cause) as TckError).phase).toBe("assertion");
    }
  });

const negatives: Readonly<Record<string, readonly Mutation[]>> = mutations;
const divergentNegatives: Readonly<Record<string, readonly Mutation[]>> =
  divergenceMutations;
it.effect(
  "requires independent observations and meaningful negative examples for every registered case",
  () =>
    Effect.gen(function* () {
      const corpus = yield* loadCorpus;
      assertCoverage(
        cases.map((test) => test.id),
        corpus.cases,
        negatives,
      );
      assertCoverage(
        cases.filter((test) => test.divergence).map((test) => test.id),
        corpus.divergences,
        divergentNegatives,
      );
    }).pipe(Effect.provide(NodeServices.layer)),
);

for (const test of cases) {
  it.effect(`${test.id}: accepts the independent workerd observation`, () =>
    Effect.gen(function* () {
      const corpus = yield* loadCorpus;
      const good = corpus.cases[test.id];
      const input = inputFor(test, corpus);
      yield* test.check(structuredClone(good), input);
      expect(
        (yield* evaluate(replay(test, good, good), reference, candidate, input))
          .status,
      ).toBe("pass");
    }).pipe(
      Effect.provideService(Transport, transport),
      Effect.provide(NodeServices.layer),
    ),
  );
  for (const mutation of negatives[test.id] ?? [])
    it.effect(`${test.id}: rejects ${mutation.name}`, () =>
      Effect.gen(function* () {
        const corpus = yield* loadCorpus;
        const good = corpus.cases[test.id];
        const input = inputFor(test, corpus);
        const bad = applyMutation(good, mutation);
        yield* rejects(test.check(structuredClone(bad), input));
        const result = yield* evaluate(
          replay(test, good, bad),
          reference,
          candidate,
          input,
        );
        expect(result.status).toBe("fail");
        expect(result.reference).toEqual(good);
        expect(result.candidate).toEqual(bad);
        // Differential equality must not turn two identically broken runtimes green.
        expect(
          (yield* evaluate(replay(test, bad, bad), reference, candidate, input))
            .status,
        ).toBe("reference-error");
      }).pipe(
        Effect.provideService(Transport, transport),
        Effect.provide(NodeServices.layer),
      ),
    );

  for (const mutation of divergentNegatives[test.id] ?? [])
    it.effect(
      `${test.id}: documented divergence rejects ${mutation.name}`,
      () =>
        Effect.gen(function* () {
          const corpus = yield* loadCorpus;
          const input = inputFor(test, corpus);
          const good = corpus.cases[test.id];
          const allowed = corpus.divergences[test.id];
          yield* test.divergence!.check(structuredClone(allowed));
          expect(
            (yield* evaluate(
              replay(test, good, allowed),
              reference,
              celld,
              input,
            )).status,
          ).toBe("divergence");
          const bad = applyMutation(allowed, mutation);
          yield* rejects(test.divergence!.check(bad));
          expect(
            (yield* evaluate(replay(test, good, bad), reference, celld, input))
              .status,
          ).toBe("fail");
        }).pipe(
          Effect.provideService(Transport, transport),
          Effect.provide(NodeServices.layer),
        ),
    );
}

const knownBugMutations: Record<string, Mutation> = {
  "crypto.x25519": {
    name: "derived key changes alongside the error class bug",
    changes: [{ path: ["body", "shared"], value: "00" }],
  },
  "r2.key-identity": {
    name: "empty segment aliases another key alongside the listing bug",
    changes: [{ path: ["body", "values", 1], value: "value-0" }],
  },
  "http.body-consumption": {
    name: "consumed text is corrupted in addition to the known bug",
    changes: [{ path: ["body", "text"], value: "corrupt" }],
  },
  "storage.invalid-input": {
    name: "an unrelated error replaces the known error",
    changes: [{ path: ["body", "limit"], value: "RangeError" }],
  },
  "assets.html-routing": {
    name: "the canonical redirect is also lost",
    changes: [{ path: ["body", "folder", "location"], value: null }],
  },
  "crypto.ecdsa-p256": {
    name: "a valid signature is rejected in addition to the known bug",
    changes: [{ path: ["body", "verified"], value: false }],
  },
  "crypto.invalid-input": {
    name: "a still-enforced rejection changes class",
    changes: [{ path: ["body", "emptyIv"], value: "accepted" }],
  },
  "node.stream-timers": {
    name: "a conforming timer result breaks alongside the known bug",
    changes: [{ path: ["body", "race"], value: "slow" }],
  },
  "flags.enabled-defaults": {
    name: "the still-honored deleteAll switch breaks alongside the known bug",
    changes: [{ path: ["body", "deleteAll", "alarmPresent"], value: true }],
  },
  "repro.body-readers": {
    name: "fresh body is already marked used",
    changes: [{ path: ["body", 0, "before"], value: true }],
  },
  "repro.storage-errors": {
    name: "failed writes leave data behind",
    changes: [{ path: ["body", "absent"], value: false }],
  },
};
it.effect("actual known-bug cases reject additional semantic corruption", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const registry = yield* decodeJson(
      BugRegistry,
      yield* fs.readFileString("docs/bugs.json"),
    );
    const corpus = yield* loadCorpus;
    expect(Object.keys(knownBugMutations).sort()).toEqual(
      registry.expectations.map((bug) => bug.caseId).sort(),
    );
    for (const bug of registry.expectations) {
      const test = cases.find((test) => test.id === bug.caseId)!;
      const input = inputFor(test, corpus);
      const good = corpus.cases[test.id];
      expect(
        (yield* evaluate(
          replay(test, good, bug.candidate),
          reference,
          celld,
          input,
          bug,
        )).status,
      ).toBe("known-bug");
      const bad = applyMutation(bug.candidate, knownBugMutations[test.id]!);
      expect(
        (yield* evaluate(replay(test, good, bad), reference, celld, input, bug))
          .status,
      ).toBe("fail");
    }
  }).pipe(
    Effect.provideService(Transport, transport),
    Effect.provide(NodeServices.layer),
  ),
);

it.effect(
  "the coverage gate rejects missing, empty, stale, and envelope-only mutation registrations",
  () =>
    Effect.sync(() => {
      const sample = mutations["alarms.fire"];
      const invalidRegistrations: ReadonlyArray<
        Readonly<Record<string, readonly Mutation[]>>
      > = [
        {},
        { example: [] },
        { example: sample, stale: sample },
        {
          example: [
            {
              name: "only a server error",
              changes: [{ path: ["status"], value: 500 }],
            },
          ],
        },
      ];
      for (const invalid of invalidRegistrations)
        expect(() =>
          assertCoverage(["example"], { example: {} }, invalid),
        ).toThrow();
      expect(() =>
        assertCoverage(
          ["example", "new.case"],
          { example: {} },
          { example: sample },
        ),
      ).toThrow();
      expect(() =>
        applyMutation(
          { body: { fires: 1 } },
          { name: "no-op", changes: [{ path: ["body", "fires"], value: 1 }] },
        ),
      ).toThrow();
      expect(() =>
        applyMutation(
          { body: { fires: 1 } },
          {
            name: "stale path",
            changes: [{ path: ["body", "removedField"], value: 0 }],
          },
        ),
      ).toThrow();
    }),
);

// Valid nondeterminism is a control too: mutation tests must not force exact
// request arrival order or a particular compressor's byte representation.
for (const id of ["concurrency.input-gates", "concurrency.explicit-gate"])
  it.effect(`${id}: accepts reordered legal histories`, () =>
    Effect.gen(function* () {
      const test = cases.find((test) => test.id === id)!;
      const corpus = yield* loadCorpus;
      const good = corpus.cases[id];
      expect(Array.isArray(good)).toBe(true);
      const reordered = [...(good as unknown[])].reverse();
      expect(
        (yield* evaluate(
          replay(test, good, reordered),
          reference,
          candidate,
          inputFor(test, corpus),
        )).status,
      ).toBe("pass");
    }).pipe(
      Effect.provideService(Transport, transport),
      Effect.provide(NodeServices.layer),
    ),
  );

it.effect(
  "node.hash-compression: accepts independent valid compression encodings",
  () =>
    Effect.gen(function* () {
      const test = cases.find((test) => test.id === "node.hash-compression")!;
      const corpus = yield* loadCorpus;
      const good = corpus.cases[test.id];
      const alternative = applyMutation(good, {
        name: "valid alternative compressor output",
        changes: [
          {
            path: ["body", "gzipBytes"],
            value: gzipSync("hello λ", { level: 9 }).toString("base64"),
          },
          {
            path: ["body", "deflateBytes"],
            value: deflateSync("hello λ", { level: 9 }).toString("base64"),
          },
        ],
      });
      expect(
        (yield* evaluate(
          replay(test, good, alternative),
          reference,
          candidate,
          inputFor(test, corpus),
        )).status,
      ).toBe("pass");
    }).pipe(
      Effect.provideService(Transport, transport),
      Effect.provide(NodeServices.layer),
    ),
);
