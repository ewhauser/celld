import { deepStrictEqual, notDeepStrictEqual, ok } from "node:assert";
import { Effect, FileSystem, Schema } from "effect";
import { decodeJson } from "../../src/Artifacts.js";
import type { Mutation } from "./Mutations.js";

export const Corpus = Schema.Struct({
  seed: Schema.Int,
  compatibilityDate: Schema.String,
  sources: Schema.Array(
    Schema.Struct({ runId: Schema.String, sourceRevision: Schema.String }),
  ),
  cases: Schema.Record(Schema.String, Schema.Unknown),
  divergences: Schema.Record(Schema.String, Schema.Unknown),
});
export const loadCorpus = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  return yield* decodeJson(
    Corpus,
    yield* fs.readFileString(
      new URL("./observations.json", import.meta.url).pathname,
    ),
  );
});

// Work on an isolated copy, reject stale paths, and ensure a mutation changes data.
export const applyMutation = (value: unknown, mutation: Mutation): unknown => {
  const result: unknown = structuredClone(value);
  for (const { path, value: replacement } of mutation.changes) {
    ok(path.length > 0, `${mutation.name}: empty mutation path`);
    let parent: unknown = result;
    for (const key of path.slice(0, -1)) {
      ok(
        typeof parent === "object" &&
          parent !== null &&
          Object.hasOwn(parent, key),
        `${mutation.name}: missing path ${path.join(".")}`,
      );
      parent = (parent as Record<string | number, unknown>)[key];
    }
    const key = path.at(-1)!;
    ok(
      typeof parent === "object" &&
        parent !== null &&
        Object.hasOwn(parent, key),
      `${mutation.name}: missing field ${path.join(".")}`,
    );
    const record = parent as Record<string | number, unknown>;
    notDeepStrictEqual(
      record[key],
      replacement,
      `${mutation.name}: mutation does not change ${path.join(".")}`,
    );
    record[key] = structuredClone(replacement);
  }
  notDeepStrictEqual(result, value, `${mutation.name}: mutation is a no-op`);
  return result;
};

export const assertCoverage = (
  ids: readonly string[],
  observations: Readonly<Record<string, unknown>>,
  negatives: Readonly<Record<string, readonly Mutation[]>>,
) => {
  deepStrictEqual(
    Object.keys(observations).sort(),
    [...ids].sort(),
    "Every registered case needs an independent positive observation",
  );
  deepStrictEqual(
    Object.keys(negatives).sort(),
    [...ids].sort(),
    "Every registered case needs named semantic mutations",
  );
  for (const id of ids) {
    const examples = negatives[id]!;
    ok(examples.length > 0, `${id}: missing negative examples`);
    const names = examples.map((example) => example.name);
    ok(
      names.every((name) => name.trim().length > 0),
      `${id}: unnamed mutation`,
    );
    deepStrictEqual(
      new Set(names).size,
      names.length,
      `${id}: duplicate mutation names`,
    );
    for (const mutation of examples) {
      ok(mutation.changes.length > 0, `${id}: empty mutation`);
      // Generic envelope damage is not a semantic negative example. HTTP cases
      // mutate response bodies; websocket traces/concurrency histories use indices.
      ok(
        mutation.changes.some(
          ({ path }) => path[0] === "body" || typeof path[0] === "number",
        ),
        `${id}: mutate the promised behavior, not only HTTP status or headers`,
      );
    }
  }
};
