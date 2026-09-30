import { Effect, Schema } from "effect";
import { pollUntil } from "./Polling.js";
import { encode, richValue } from "../fixtures/shared/Codec.js";
import { channelMessages } from "../fixtures/shared/Messaging.js";
import { call, define, response, initialCases } from "./Cases.js";
import { Transport, TckError, toTckError, type TestCase } from "./Domain.js";
import { decodeAs } from "./Artifacts.js";
import { equal } from "./Oracle.js";

export const endpoint = (
  id: string,
  path: string,
  expected: unknown,
  contract: string,
): TestCase => ({
  ...define(
    id,
    (target, input) => call(target, input, path),
    () => response(expected),
  ),
  contract,
});
const webDoc = "https://developers.cloudflare.com/workers/runtime-apis/";
const cryptoDoc = webDoc + "web-crypto/";
const storageDoc =
  "https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/";
const ep = (id: string, path: string, expected: unknown) =>
  endpoint(
    id,
    path,
    expected,
    id.startsWith("storage.") || id.startsWith("sql.") ? storageDoc : webDoc,
  );
const concurrency = (id: string, path: string): TestCase => ({
  id,
  contract:
    "https://developers.cloudflare.com/durable-objects/api/state/#blockconcurrencywhile",
  run: (target, input) =>
    Effect.all(
      Array.from({ length: 12 }, () => call(target, input, path)),
      { concurrency: "unbounded" },
    ),
  check: (value) =>
    Effect.gen(function* () {
      const rows = yield* decodeAs(
        Schema.Array(
          Schema.Struct({
            status: Schema.Literal(200),
            body: Schema.Struct({
              before: Schema.Int,
              after: Schema.Int,
              initialized: Schema.Literal(true),
            }),
          }),
        ),
        "assertion",
      )(value);
      yield* equal(rows.length, 12);
      // Arrival order is intentionally not asserted; the observed transitions must
      // form a legal serialized history, including every intermediate value.
      yield* equal(
        rows.map((r) => r.body.before).sort((a, b) => a - b),
        Array.from({ length: 12 }, (_, i) => i),
      );
      for (const row of rows) yield* equal(row.body.after, row.body.before + 1);
    }),
  compare: () => Effect.void,
});
export const poll = <A>(
  read: Effect.Effect<A, TckError, Transport>,
  done: (value: A) => boolean,
): Effect.Effect<A, TckError, Transport> =>
  pollUntil(read, done, {
    interval: "100 millis",
    attempts: 160,
    timeout: "30 seconds",
    message: "Eventual state not reached before deadline",
  }).pipe(Effect.mapError(toTckError("assertion")));
const alarm = (retry: boolean): TestCase => ({
  ...define(
    retry ? "alarms.retry" : "alarms.fire",
    (target, input) =>
      Effect.gen(function* () {
        yield* equal(
          yield* call(target, input, `/alarms/start${retry ? "?retry=1" : ""}`),
          response({ scheduled: true }),
        );
        return yield* poll(
          call(target, input, "/alarms/state"),
          (result) =>
            typeof result.body === "object" &&
            result.body !== null &&
            "fires" in result.body &&
            Number(result.body.fires) >= (retry ? 2 : 1),
        );
      }),
    () => response({ fires: retry ? 2 : 1, retry, retryCount: retry ? 1 : 0 }),
  ),
  contract: "https://developers.cloudflare.com/durable-objects/api/alarms/",
});
export const coreCases: ReadonlyArray<TestCase> = [
  {
    ...define(
      "alarms.rearm-pending-timer",
      (target, input) =>
        Effect.gen(function* () {
          yield* equal(
            yield* call(target, input, "/alarms/rearm-start"),
            response({ scheduled: true }),
          );
          return yield* poll(
            call(target, input, "/alarms/rearm-state"),
            (value) => (value.body as { fires: number }).fires >= 2,
          );
        }),
      () => response({ fires: 2, beforeTimer: true }),
    ),
    contract: "https://developers.cloudflare.com/durable-objects/api/alarms/",
  },
  {
    id: "websocket.handler-streaming",
    run: (target, input) =>
      Effect.gen(function* () {
        return yield* (yield* Transport).websocket(
          target,
          `/websocket?name=${input.namespace}`,
          "stream",
        );
      }),
    check: (value) =>
      Effect.gen(function* () {
        const samples = yield* decodeAs(
          Schema.Array(
            Schema.Struct({
              body: Schema.Struct({
                messages: Schema.Array(Schema.String),
                receivedAt: Schema.Array(Schema.Number),
              }),
            }),
          ),
          "assertion",
        )(value);
        yield* equal(samples.length, 1);
        const { messages, receivedAt } = samples[0]!.body;
        yield* equal(messages, ["first", "second", "finished"]);
        yield* equal(receivedAt.length, 3);
        // The handler waits 500 ms twice. Allow scheduling margin, but reject
        // delivery in a batch after it returns. Use one client's monotonic clock.
        yield* equal(receivedAt[2]! - receivedAt[0]! >= 750, true);
        yield* equal(receivedAt[2]! - receivedAt[1]! >= 250, true);
        yield* equal(receivedAt[1]! >= receivedAt[0]!, true);
      }),
    compare: () => Effect.void,
    contract: webDoc + "websockets/",
  },
  {
    ...define(
      "websocket.peer-close",
      (target, input) =>
        Effect.gen(function* () {
          yield* equal(
            yield* call(target, input, "/websocket/peer-close"),
            response({ sent: true }),
          );
          return yield* poll(
            call(target, input, "/websocket/close-state"),
            (value) => value.body !== null,
          );
        }),
      () => response({ code: 4002, reason: "peer λ", wasClean: true }),
    ),
    contract: webDoc + "websockets/",
  },
  endpoint(
    "crypto.ed25519",
    "/crypto/ed25519",
    {
      publicKeys: Array(2).fill(
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
      ),
      signatures: Array(2).fill(
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
      ),
      verified: [
        [true, true],
        [true, true],
      ],
      tampered: false,
    },
    cryptoDoc,
  ),
  endpoint(
    "crypto.x25519",
    "/crypto/x25519",
    {
      publicKey:
        "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f",
      shared:
        "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742",
      lowOrder: "OperationError",
    },
    cryptoDoc,
  ),
  ep("sql.nested-sync", "/sql/nested-sync", {
    arities: [0, 0],
    returned: 42,
    rows: [{ n: 1 }, { n: 2 }, { n: 4 }],
  }),
  ep("sql.invalid-utf8", "/sql/invalid-utf8", {
    value: "a�b",
    bytes: "61FF62",
  }),
  ep("http.unicode-headers", "/web/unicode-headers", {
    constructed: "λ🌍, 雪",
    response: "λ🌍, 雪",
    fetched: "λ🌍, 雪",
  }),

  ...initialCases,
  ep("storage.sync-committed", "/storage/sync-committed", {
    retained: "durable λ",
    removed: null,
    rows: [{ id: 1, value: "committed" }],
  }),
  ep("sql.consumed-write-cursor", "/sql/consumed-write-cursor", {
    returned: [{ n: 1 }, { n: 2 }, { n: 3 }],
    rows: [{ n: 1 }, { n: 2 }, { n: 3 }],
  }),
  ep("sql.open-read-cursor", "/sql/open-read-cursor", {
    first: { n: 1 },
    remaining: [{ n: 2 }, { n: 3 }],
  }),
  ep("http.fetch-abort", "/web/fetch-abort", { error: "AbortError" }),
  ep("storage.synchronous-kv", "/storage/synchronous-kv", {
    a: { value: 1 },
    list: [
      ["b", "λ"],
      ["a", { value: 1 }],
    ],
    removed: true,
    missing: true,
  }),
  ep("sql.mixed-rollback", "/sql/mixed-rollback", {
    error: "Error",
    balance: 1,
    rows: [],
  }),
  {
    ...define(
      "context.wait-until",
      (target, input) =>
        Effect.gen(function* () {
          yield* equal(
            yield* call(target, input, "/context/background-start"),
            response({ scheduled: true }),
          );
          return yield* poll(
            call(target, input, "/storage/get?key=background"),
            (result) =>
              (result.body as { present?: boolean } | null)?.present === true,
          );
        }),
      () => response({ present: true, value: "complete" }),
    ),
    contract:
      "https://developers.cloudflare.com/durable-objects/api/state/#waituntil",
  },
  {
    ...ep("cache.documented-miss", "/web/cache", {
      value: "cached",
      deleted: true,
    }),
    divergence: {
      celldVersion: "0.6.0",
      compatibilityDate: "2026-07-30",
      compatibilityFlags: [],
      source: "https://celld.dev/docs/cloudflare-compat/#cache",
      reason: "celld deliberately implements an always-miss cache",
      reviewDate: "2026-09-26",
      owner: "celld-tck maintainers",
      check: (value) => equal(value, response({ value: null, deleted: false })),
    },
  },
  ep("http.body-consumption", "/web/body", {
    before: false,
    after: true,
    text: "λ🌍",
    cloned: "λ🌍",
    twice: "TypeError",
    lateClone: "TypeError",
  }),
  ep("http.headers", "/web/headers", {
    joined: "first, second",
    replaced: "replacement",
    deleted: true,
    invalid: "TypeError",
  }),
  ep("http.url-formdata", "/web/url-form", {
    pathname: "/a%20b",
    query: ["1", "2"],
    values: ["first", "λ"],
    file: {
      name: "bytes.bin",
      type: "application/octet-stream",
      bytes: [0, 128, 255],
    },
  }),
  ep("http.encoding", "/web/encoding", {
    bytes: [206, 187, 240, 159, 140, 141],
    decoded: "λ🌍",
    base64: "AP8=",
    decodedBase64: [0, 255],
    invalid: "TypeError",
  }),
  ep("http.abort", "/web/abort", {
    aborted: true,
    calls: 1,
    reason: "test abort",
    thrown: "Error",
  }),
  ep("http.redirect", "/web/redirect", {
    status: 307,
    location: "https://example.test/next",
    invalid: "RangeError",
  }),
  ep("crypto.sha256-hmac", "/web/crypto", {
    digest: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    hmac: "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
    verified: true,
    rejected: false,
    uuid: true,
  }),
  ep("crypto.aes-gcm", "/web/aes", {
    ciphertext: "58e2fccefa7e3061367f1d57a4e7455a",
    plaintext: [],
    tampered: "OperationError",
  }),
  endpoint(
    "crypto.ecdsa-p256",
    "/web/ecdsa",
    {
      signatureBytes: 64,
      verified: true,
      tampered: false,
      jwkVerified: true,
      rawVerified: true,
      jwk: {
        kty: "EC",
        crv: "P-256",
        ext: true,
        keyOps: ["verify"],
        xBytes: 32,
        yBytes: 32,
        privateOmitted: true,
      },
      raw: { bytes: 65, uncompressed: true },
      usages: { private: ["sign"], public: ["verify"] },
      algorithm: { name: "ECDSA", namedCurve: "P-256" },
      types: ["private", "public"],
    },
    cryptoDoc,
  ),
  endpoint(
    "crypto.key-derivation",
    "/web/derive",
    {
      // PBKDF2-HMAC-SHA-256("password", "salt", 4096, 256 bits) and RFC 5869
      // HKDF-SHA-256 test case 1, both independently reproduced with node:crypto.
      pbkdf2:
        "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a",
      hkdf: "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865",
      derivedMac:
        "08d3277c85e4850fcc2d3599090b3e13814fdb36925068cb91273b336fe3f5b0",
      derivedAlgorithm: {
        name: "HMAC",
        hash: { name: "SHA-256" },
        length: 256,
      },
      unalignedLength: "OperationError",
      zeroIterations: "OperationError",
    },
    cryptoDoc,
  ),
  {
    ...endpoint(
      "crypto.key-export",
      "/web/key-export",
      {
        hmacRaw: [107, 101, 121],
        aesRaw: Array.from({ length: 16 }, (_, i) => i),
        hmacJwk: {
          kty: "oct",
          alg: "HS256",
          keyOps: ["sign", "verify"],
          ext: true,
          k: "a2V5",
        },
        aesJwk: {
          kty: "oct",
          alg: "A128GCM",
          keyOps: ["encrypt", "decrypt"],
          ext: true,
          k: "AAECAwQFBgcICQoLDA0ODw",
        },
        algorithms: [
          { name: "HMAC", hash: { name: "SHA-256" }, length: 24 },
          { name: "AES-GCM", length: 128 },
        ],
      },
      cryptoDoc,
    ),
    divergence: {
      celldVersion: "0.6.0",
      compatibilityDate: "2026-07-30",
      compatibilityFlags: [],
      source: "https://celld.dev/docs/cloudflare-compat/#web-crypto",
      reason:
        "celld documents that a secret key cannot use jwk with exportKey() or wrapKey()",
      reviewDate: "2026-09-26",
      owner: "celld-tck maintainers",
      check: (value) =>
        equal(
          value,
          response({
            hmacRaw: [107, 101, 121],
            aesRaw: Array.from({ length: 16 }, (_, i) => i),
            hmacJwk: "NotSupportedError",
            aesJwk: "NotSupportedError",
            algorithms: [
              { name: "HMAC", hash: { name: "SHA-256" }, length: 24 },
              { name: "AES-GCM", length: 128 },
            ],
          }),
        ),
    },
  },
  endpoint(
    "crypto.invalid-input",
    "/web/crypto-invalid",
    {
      wrongUsage: "InvalidAccessError",
      unknownAlgorithm: "NotSupportedError",
      unknownHash: "NotSupportedError",
      badKeyLength: "DataError",
      malformedJwk: "DataError",
      emptyUsages: "SyntaxError",
      mismatchedUsage: "SyntaxError",
      nonExtractable: "InvalidAccessError",
      emptyIv: "OperationError",
    },
    cryptoDoc,
  ),
  endpoint(
    "messaging.message-channel",
    "/messaging/channel",
    {
      synchronous: 0,
      messages: encode(channelMessages()),
      unserializable: "DataCloneError",
      afterClose: "accepted",
      droppedAfterClose: true,
    },
    "https://developer.mozilla.org/en-US/docs/Web/API/MessageChannel",
  ),
  endpoint(
    "messaging.event-source",
    "/messaging/event-source",
    {
      events: [
        { type: "greeting", data: "hello λ", lastEventId: "1" },
        { type: "message", data: "line one\nline two", lastEventId: "1" },
      ],
      trace: ["error"],
      exhausted: 2,
      closed: 2,
      states: [0, 1, 2],
      withCredentials: false,
    },
    "https://developers.cloudflare.com/workers/runtime-apis/eventsource/",
  ),
  ep("web.html-rewriter", "/web/html", {
    html: '<p data-test="yes">λ &amp; text</p>',
  }),
  ep("storage.rich-values", "/storage/rich", encode(richValue())),
  ep("storage.batch-ranges", "/storage/batch", {
    got: [
      ["k:a", 1],
      ["k:c", 3],
    ],
    forward: [["k:b", 2]],
    reverse: [
      ["k:c", 3],
      ["k:b", 2],
    ],
    after: [["k:b", 2]],
    deleted: 2,
    remaining: [
      ["k:b", 2],
      ["other", 9],
    ],
  }),
  ep("storage.delete-all", "/storage/delete-all", { values: [], alarm: null }),
  ep("storage.transaction-commit", "/storage/commit", {
    result: 7,
    entries: [
      ["a", 7],
      ["b", 8],
    ],
  }),
  ep("storage.explicit-rollback", "/storage/explicit-rollback", { value: 1 }),
  ep("storage.invalid-input", "/storage/invalid", {
    limit: "TypeError",
    unserializable: "DataCloneError",
  }),
  ep("sql.bindings-cursors", "/sql/types", {
    columns: ["id", "text", "n", "empty", "nil", "bytes"],
    inserted: [
      {
        id: 1,
        text: "λ'",
        n: 1.25,
        empty: "",
        nil: null,
        bytes: { type: "ArrayBuffer", value: [0, 128, 255] },
      },
    ],
    raw: [[1, "λ'", null]],
    count: { n: 1 },
  }),
  ep("sql.constraints", "/sql/constraints", {
    duplicate: true,
    nil: true,
    multiple: true,
    empty: true,
    rows: [{ id: 1, v: "a" }],
  }),
  ep("sql.transaction-commit", "/sql/commit", { returned: 42, row: { n: 3 } }),
  ep("identity.namespace", "/identity/namespace", {
    same: true,
    different: true,
    roundTrip: true,
    unique: true,
    invalid: "TypeError",
  }),
  concurrency("concurrency.input-gates", "/concurrency/increment"),
  concurrency("concurrency.explicit-gate", "/concurrency/block"),
  ep("rpc.structured-clone", "/rpc/echo", encode(richValue())),
  ep("rpc.thrown-error", "/rpc/error", { error: "TypeError" }),
  {
    ...ep("rpc.returned-target", "/rpc/target", { sum: 42 }),
    divergence: {
      celldVersion: "0.6.0",
      compatibilityDate: "2026-07-30",
      compatibilityFlags: [],
      source: "https://celld.dev/docs/cloudflare-compat/#rpc",
      reason: "celld cannot transfer an RPC stub across an isolate boundary",
      reviewDate: "2026-09-26",
      owner: "celld-tck maintainers",
      check: (value) =>
        equal(
          value,
          response({
            error: {
              name: "Error",
              message: "RPC stubs cannot cross isolate boundaries yet.",
            },
          }),
        ),
    },
  },
  ep("alarms.set-replace-delete", "/alarms/manage", {
    set: true,
    replaced: true,
    deleted: null,
  }),
  alarm(false),
  alarm(true),
  ep("streams.tee", "/streams/tee", {
    a: [0, 128, 255, 10],
    b: [0, 128, 255, 10],
  }),
  ep("streams.cancel", "/streams/cancel", {
    first: { value: "first", done: false },
    reason: "stop",
    pulls: 1,
    after: { done: true },
  }),
  ep("streams.error", "/streams/error", { error: "TypeError" }),
  ep("streams.transform", "/streams/transform", { text: "AΛ" }),
  ep("streams.backpressure", "/streams/backpressure", {
    initial: 1,
    blocked: 0,
    resumed: 1,
    trace: ["one"],
  }),
  {
    ...define(
      "streams.http-binary",
      (target, input) => call(target, input, "/streams/response"),
      () =>
        response([0, 1, 128, 255, 10], 200, {
          "content-type": "application/octet-stream",
        }),
    ),
    contract: webDoc + "streams/",
  },
  {
    ...define(
      "websocket.protocol-attachment",
      (target, input) =>
        Effect.gen(function* () {
          return yield* (yield* Transport).websocket(
            target,
            `/websocket?name=${input.namespace}`,
          );
        }),
      () => [
        { type: "open" },
        { type: "message", value: "hello λ" },
        { type: "message", value: [0, 128, 255] },
        { type: "message", value: '{"label":"durable","count":7}' },
        { type: "close", code: 1000, reason: "done", clean: true },
      ],
    ),
    contract:
      "https://developers.cloudflare.com/durable-objects/api/websockets/",
  },
  endpoint(
    "websocket.outbound-close",
    "/websocket/outbound",
    {
      echo: { type: "message", data: "echo:ping λ" },
      closed: { type: "close", code: 4001, reason: "server λ", clean: true },
      readyState: 3,
      // The reserved code 1005 is outside the ranges close() accepts.
      invalidCode: "InvalidAccessError",
    },
    webDoc + "websockets/",
  ),
  endpoint(
    "websocket.concurrent-sockets",
    "/websocket/concurrent",
    {
      echoes: Array.from({ length: 16 }, (_, i) => `echo:n${i}`).sort(),
      open: 16,
    },
    webDoc + "websockets/",
  ),
];
