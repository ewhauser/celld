import { Effect, Schema } from "effect";
import { gunzipSync, inflateSync } from "node:zlib";
import { TckError } from "./Domain.js";
import { decodeAs } from "./Artifacts.js";
import { endpoint } from "./CoreCases.js";
import { equal } from "./Oracle.js";

export const checkCompression = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(
      Schema.Struct({
        status: Schema.Literal(200),
        headers: Schema.Record(Schema.String, Schema.String),
        body: Schema.Struct({
          sha256: Schema.String,
          gzip: Schema.String,
          deflate: Schema.String,
          gzipBytes: Schema.String,
          deflateBytes: Schema.String,
        }),
      }),
      "assertion",
    )(value);
    yield* equal(observation.headers, { "content-type": "application/json" });
    const body = observation.body;
    yield* equal(
      { sha256: body.sha256, gzip: body.gzip, deflate: body.deflate },
      {
        sha256:
          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        gzip: "hello λ",
        deflate: "hello λ",
      },
    );
    // The host decoder independently validates emitted streams; encoder bytes may vary.
    for (const [encoded, decode] of [
      [body.gzipBytes, gunzipSync],
      [body.deflateBytes, inflateSync],
    ] as const) {
      const decoded = yield* Effect.try({
        try: () => decode(Buffer.from(encoded, "base64")).toString("utf8"),
        catch: (error) =>
          new TckError({ phase: "assertion", message: String(error) }),
      });
      yield* equal(decoded, "hello λ");
    }
  });
export const nodeCases = [
  endpoint(
    "node.buffer",
    "/node/buffer",
    {
      hex: "cebbf09f8c8d",
      base64: "zrvwn4yN",
      roundTrip: "λ🌍",
      slice: [206, 187],
    },
    "https://developers.cloudflare.com/workers/runtime-apis/nodejs/buffer/",
  ),
  endpoint(
    "node.path-events",
    "/node/path-events",
    {
      path: "/b/c",
      basename: "b",
      trace: ["once:a", "on:a", "on:b"],
      remaining: 0,
    },
    "https://nodejs.org/api/events.html",
  ),
  endpoint(
    "node.async-context",
    "/node/async-context",
    { inside: "request", outsideMissing: true },
    "https://developers.cloudflare.com/workers/runtime-apis/nodejs/asynclocalstorage/",
  ),
  endpoint(
    "node.hash-compression",
    "/node/hash-compression",
    {
      sha256:
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
      gzip: "hello λ",
      deflate: "hello λ",
    },
    "https://celld.dev/docs/cloudflare-compat/#nodejs-compatibility",
  ),
  endpoint(
    "node.util",
    "/node/util",
    {
      promisified: "value:x",
      promisifiedError: "TypeError",
      inspect: "{ a: 1, b: [ 1, 2 ], c: 'λ' }",
      inspectDepth: "{ a: { b: { c: [Object] } } }",
      format: 'x:2:{"a":1}',
      types: {
        date: true,
        view: true,
        promise: true,
        map: true,
        notDate: false,
      },
      deepEqual: [true, false],
      sharedEncoder: [2, "λ"],
    },
    "https://nodejs.org/api/util.html",
  ),
  endpoint(
    "node.assert",
    "/node/assert",
    {
      satisfied: "accepted",
      strictEqual: {
        name: "AssertionError",
        code: "ERR_ASSERTION",
        operator: "strictEqual",
        actual: 1,
        expected: 2,
      },
      deepStrictEqual: {
        name: "AssertionError",
        code: "ERR_ASSERTION",
        operator: "deepStrictEqual",
        actual: { a: [1] },
        expected: { a: [2] },
      },
      typeSensitive: {
        name: "AssertionError",
        code: "ERR_ASSERTION",
        operator: "deepStrictEqual",
        actual: { a: 1 },
        expected: { a: "1" },
      },
      throws: "accepted",
      throwsMissing: {
        name: "AssertionError",
        code: "ERR_ASSERTION",
        operator: "throws",
      },
      rejects: "accepted",
      resolvedNotRejected: "AssertionError",
    },
    "https://nodejs.org/api/assert.html",
  ),
  endpoint(
    "node.stream-timers",
    "/node/stream-timers",
    {
      chunks: ["a", "λ"],
      chunkType: "string",
      binary: [0, 128, 255],
      fromWeb: "aλ",
      race: "fast",
      immediate: "soon",
      aborted: "AbortError",
    },
    "https://nodejs.org/api/stream.html",
  ),
].map((test) => ({
  ...test,
  fixture: "node" as const,
  ...(test.id === "node.hash-compression"
    ? {
        check: checkCompression,
        compare: (reference: unknown, candidate: unknown) =>
          checkCompression(reference).pipe(
            Effect.andThen(checkCompression(candidate)),
          ),
      }
    : {}),
}));
