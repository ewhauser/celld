import { gzipSync, deflateSync } from "node:zlib";

// Each mutation models a specific broken behavior promised by the case. Do not
// generate these from expected results or substitute generic malformed envelopes.
export interface Change {
  readonly path: readonly (string | number)[];
  readonly value: unknown;
}
export interface Mutation {
  readonly name: string;
  readonly changes: readonly [Change, ...Change[]];
}
const change = (path: Change["path"], value: unknown): Change => ({
  path,
  value,
});
const mutate = (name: string, ...changes: [Change, ...Change[]]): Mutation => ({
  name,
  changes,
});
const body = (name: string, field: string, value: unknown) =>
  mutate(name, change(["body", field], value));
export const mutations = {
  "storage.sync-committed": [
    body("sync loses an unconfirmed KV write", "retained", null),
    body("sync loses an unconfirmed deletion", "removed", "old"),
    body("sync loses committed SQL rows", "rows", []),
  ],
  "sql.consumed-write-cursor": [
    body("consuming RETURNING loses a write", "rows", [{ n: 1 }, { n: 2 }]),
    body("RETURNING skips an inserted row", "returned", [{ n: 1 }, { n: 3 }]),
  ],
  "sql.open-read-cursor": [
    body("sync invalidates an open read cursor", "remaining", []),
    body("read cursor repeats its first row", "remaining", [
      { n: 1 },
      { n: 2 },
      { n: 3 },
    ]),
  ],
  "http.request-response": [
    body("POST is incorrectly forwarded as GET", "method", "GET"),
    body("duplicate query parameters are collapsed", "query", ["second"]),
    body("UTF-8 request body is corrupted", "body", "hello ? ?\n"),
  ],
  "storage.round-trip": [
    mutate("stored value is lost", change([3, "body"], { present: false })),
    mutate(
      "another object can read the stored value",
      change([5, "body"], { present: true, value: "leaked" }),
    ),
    mutate(
      "deleted key remains readable",
      change([7, "body"], { present: true, value: "stale" }),
    ),
    mutate(
      "listing reverses key order",
      change([4, "body", 0, 0], "item:b"),
      change([4, "body", 1, 0], "item:a"),
    ),
  ],
  "storage.transaction-rollback": [
    mutate(
      "rollback retains the modified balance",
      change([2, "body", "value"], 99),
    ),
    mutate(
      "rollback retains a newly inserted key",
      change([3, "body"], { present: true, value: "uncommitted" }),
    ),
  ],
  "sql.transaction-rollback": [
    mutate(
      "rollback leaks an uncommitted SQL row",
      change(
        [2, "body"],
        [
          { id: 1, value: "committed" },
          { id: 2, value: "uncommitted" },
        ],
      ),
    ),
  ],
  "http.fetch-abort": [
    body("aborted fetch resolves instead of rejecting", "error", "accepted"),
  ],
  "storage.synchronous-kv": [
    body("synchronous get returns a stale value", "a", { value: 0 }),
    body("reverse listing is returned forwards", "list", [
      ["a", { value: 1 }],
      ["b", "λ"],
    ]),
    body("deleted key remains present", "missing", false),
  ],
  "sql.mixed-rollback": [
    body("SQL rollback leaves KV writes committed", "balance", 2),
    body("KV rollback leaves SQL writes committed", "rows", [{ n: 2 }]),
  ],
  "context.wait-until": [
    body("background write is never completed", "value", "pending"),
  ],
  "cache.documented-miss": [
    body("cache put is lost on the conforming runtime", "value", null),
    body("cached entry cannot be deleted", "deleted", false),
  ],
  "http.body-consumption": [
    body("a consumed body can be read twice", "twice", "accepted"),
    body("a consumed body can be cloned", "lateClone", "accepted"),
    body("consumption does not set bodyUsed", "after", false),
  ],
  "http.headers": [
    body("append replaces the existing value", "joined", "second"),
    body("delete leaves the header in place", "deleted", false),
    body("invalid header name is accepted", "invalid", "accepted"),
  ],
  "http.url-formdata": [
    body("repeated form fields lose their first value", "values", ["λ"]),
    mutate(
      "file bytes are corrupted",
      change(["body", "file", "bytes"], [0, 127, 255]),
    ),
    body("duplicate URL parameters lose ordering", "query", ["2", "1"]),
  ],
  "http.encoding": [
    body("UTF-8 decoder replaces valid non-ASCII text", "decoded", "��"),
    body("base64 decoding truncates a high byte", "decodedBase64", [0, 127]),
    body("invalid encoded input is accepted", "invalid", "accepted"),
  ],
  "http.abort": [
    body("abort listener is never invoked", "calls", 0),
    body("throwIfAborted does not throw", "thrown", "accepted"),
    body("abort reason is discarded", "reason", null),
  ],
  "http.redirect": [
    body("redirect changes its method-preserving status", "status", 302),
    body(
      "redirect loses its destination",
      "location",
      "https://example.test/wrong",
    ),
    body("invalid redirect status is accepted", "invalid", "accepted"),
  ],
  "crypto.sha256-hmac": [
    body("SHA-256 digest is incorrect", "digest", "00".repeat(32)),
    body("HMAC digest is incorrect", "hmac", "00".repeat(32)),
    body("invalid signature is accepted", "rejected", true),
    body("valid signature is rejected", "verified", false),
  ],
  "crypto.aes-gcm": [
    body(
      "authenticated encryption returns the wrong ciphertext",
      "ciphertext",
      "00".repeat(16),
    ),
    body("tampered ciphertext decrypts without error", "tampered", "accepted"),
  ],
  "web.html-rewriter": [
    body(
      "element handler fails to add its attribute",
      "html",
      "<p>λ &amp; text</p>",
    ),
  ],
  "storage.rich-values": [
    mutate(
      "bigint loses integer precision",
      change(["body", "value", 0, 1, "value"], "9007199254740992"),
    ),
    mutate(
      "typed array loses its type",
      change(["body", "value", 2, 1, "type"], "Array"),
    ),
    mutate(
      "negative zero becomes positive zero",
      change(["body", "value", 8, 1, "value"], 0),
    ),
  ],
  "storage.batch-ranges": [
    body("exclusive range end is incorrectly included", "forward", [
      ["k:b", 2],
      ["k:c", 3],
    ]),
    body("reverse range returns ascending keys", "reverse", [
      ["k:b", 2],
      ["k:c", 3],
    ]),
    body("delete count includes missing keys", "deleted", 3),
  ],
  "storage.delete-all": [
    body("deleteAll retains a key", "values", [["leftover", 1]]),
    body("deleteAll retains an alarm", "alarm", 123),
  ],
  "storage.transaction-commit": [
    body("commit drops one of its writes", "entries", [["a", 7]]),
    body("transaction callback result is discarded", "result", null),
  ],
  "storage.explicit-rollback": [
    body("explicit rollback commits the new value", "value", 2),
  ],
  "storage.invalid-input": [
    body("negative limit throws the wrong error class", "limit", "Error"),
    body(
      "uncloneable value throws the wrong error class",
      "unserializable",
      "TypeError",
    ),
  ],
  "sql.bindings-cursors": [
    mutate(
      "SQL blob loses its high byte",
      change(["body", "inserted", 0, "bytes", "value"], [0, 127, 255]),
    ),
    body("raw cursor reorders columns", "raw", [["λ'", 1, null]]),
    mutate(
      "bound SQL text is truncated at the quote",
      change(["body", "inserted", 0, "text"], "λ"),
    ),
  ],
  "sql.constraints": [
    body("duplicate primary key is accepted", "duplicate", false),
    body("NOT NULL constraint is not enforced", "nil", false),
    body("failed insert changes committed rows", "rows", [
      { id: 1, v: "overwritten" },
    ]),
  ],
  "sql.transaction-commit": [
    body("committed SQL update is lost", "row", { n: 1 }),
    body("SQL transaction return value is lost", "returned", null),
  ],
  "identity.namespace": [
    body("distinct names resolve to the same object", "different", false),
    body("unique IDs collide", "unique", false),
    body("invalid ID text is accepted", "invalid", "accepted"),
  ],
  "concurrency.input-gates": [
    mutate(
      "two requests read the same pre-increment value",
      change([1, "body", "before"], 0),
      change([1, "body", "after"], 1),
    ),
    mutate("one increment is lost", change([0, "body", "after"], 0)),
    mutate(
      "request observes uninitialized state",
      change([0, "body", "initialized"], false),
    ),
  ],
  "concurrency.explicit-gate": [
    mutate(
      "two gated requests occupy the same transition",
      change([0, "body", "before"], 10),
      change([0, "body", "after"], 11),
    ),
    mutate("gated increment skips a value", change([0, "body", "after"], 3)),
    mutate(
      "request enters before initialization completes",
      change([0, "body", "initialized"], false),
    ),
  ],
  "rpc.structured-clone": [
    mutate(
      "RPC bigint is rounded",
      change(["body", "value", 0, 1, "value"], "9007199254740992"),
    ),
    mutate(
      "RPC Map becomes an ordinary object",
      change(["body", "value", 4, 1, "type"], "Object"),
    ),
  ],
  "rpc.thrown-error": [
    body("RPC loses the thrown error class", "error", "Error"),
  ],
  "rpc.returned-target": [
    body("returned RPC target drops an argument", "sum", 40),
  ],
  "alarms.set-replace-delete": [
    body("alarm replacement keeps the old deadline", "replaced", false),
    body("deleted alarm retains its deadline", "deleted", 123),
  ],
  "alarms.fire": [
    body("scheduled alarm never fires", "fires", 0),
    body("successful alarm fires twice", "fires", 2),
  ],
  "alarms.retry": [
    body("failed alarm is never retried", "fires", 1),
    body("retry metadata does not advance", "retryCount", 0),
  ],
  "streams.tee": [
    body("tee loses bytes on its second branch", "b", [0, 128, 10]),
  ],
  "streams.cancel": [
    body("cancellation reason does not reach the source", "reason", null),
    body("cancelled reader continues yielding chunks", "after", {
      done: false,
      value: "extra",
    }),
  ],
  "streams.error": [
    body("stream source failure is swallowed", "error", "accepted"),
  ],
  "streams.transform": [
    body("transform passes through unmodified input", "text", "aλ"),
  ],
  "streams.backpressure": [
    body("writer proceeds while the queue is full", "blocked", 1),
    body("writer never resumes after the queue drains", "resumed", 0),
  ],
  "streams.http-binary": [
    mutate(
      "streamed binary response drops its high byte",
      change(["body"], [0, 1, 128, 10]),
    ),
  ],
  "websocket.protocol-attachment": [
    mutate(
      "binary websocket frame becomes text",
      change([2, "value"], "0,128,255"),
    ),
    mutate(
      "serialized attachment loses its counter",
      change([3, "value"], '{"label":"durable","count":0}'),
    ),
    mutate(
      "websocket closes abnormally",
      change([4, "code"], 1006),
      change([4, "clean"], false),
    ),
  ],
  "workflows.retry": [
    mutate(
      "workflow step succeeds without its required retry",
      change(["body", "output", "attempts"], 1),
    ),
  ],
  "workflows.event": [
    mutate(
      "workflow event payload is lost",
      change(["body", "output", "payload", "value"], ""),
    ),
  ],
  "rpc.named-service": [
    body(
      "named service receives the wrong argument",
      "greeting",
      "hello undefined",
    ),
  ],
  "kv.metadata-list": [
    mutate("KV metadata is lost", change(["body", "value", "metadata"], null)),
    body("deleted KV key remains readable", "missing", "stale"),
    body("KV prefix listing includes another prefix", "names", [
      "/a",
      "other/b",
    ]),
  ],
  "kv.binary-pagination": [
    body("pagination repeats the first page", "second", ["/a"]),
    body("binary KV value loses its high byte", "bytes", [0, 127, 255]),
  ],
  "kv.stream-put": [
    body("streamed KV write loses a chunk", "bytes", [0, 128, 255, 65]),
    body("streamed KV write drops metadata", "metadata", null),
    body("deleted streamed value remains readable", "deleted", "stale"),
  ],
  "kv.stream-limit": [
    body("oversized streamed KV value is accepted", "rejected", false),
    body("rejected streamed value is partly stored", "missing", false),
  ],
  "d1.bindings-results": [
    mutate("D1 binds the wrong integer", change(["body", "first", "n"], 0)),
    body("D1 raw results lose column order", "raw", [["λ'", 7]]),
  ],
  "d1.batch-rollback": [
    body("failed D1 batch commits its first statement", "value", 2),
  ],
  "r2.metadata-range-delete": [
    body("R2 range includes the wrong bytes", "range", [0, 128]),
    body("R2 custom metadata is lost", "metadata", {}),
    body("deleted R2 object remains readable", "missing", { size: 4 }),
  ],
  "r2.conditional-write": [
    body("mismatched ETag is allowed to overwrite", "rejected", false),
    body("matching ETag write does not persist", "value", "first"),
  ],
  "r2.multipart": [
    body("multipart completion loses its final part", "value", ""),
    body("aborted multipart upload leaves an object", "aborted", { size: 17 }),
  ],
  "queues.batch-ack-retry": [
    mutate(
      "retry request never produces redelivery",
      change(["body", "single", "attempts"], 1),
    ),
    mutate(
      "acknowledged batch message is delivered twice",
      change(["body", "batch-a", "attempts"], 2),
    ),
  ],
  "workflows.steps-sleep-result": [
    mutate(
      "workflow loses the persisted step result",
      change(["body", "output", "result"], 0),
    ),
  ],
  "crypto.ecdsa-p256": [
    body("ECDSA verifies a tampered signature", "tampered", true),
    body("ECDSA rejects its own valid signature", "verified", false),
    mutate(
      "a JWK-imported public key cannot verify the signature",
      change(["body", "jwkVerified"], "NotSupportedError"),
    ),
    mutate(
      "a raw-imported public key cannot verify the signature",
      change(["body", "rawVerified"], false),
    ),
    mutate(
      "the raw public key is exported in a different encoding",
      change(["body", "raw", "bytes"], 91),
      change(["body", "raw", "uncompressed"], false),
    ),
    mutate(
      "exported public JWK omits its extractability metadata",
      change(["body", "jwk", "ext"], false),
      change(["body", "jwk", "keyOps"], []),
    ),
    mutate(
      "exported public JWK leaks the private scalar",
      change(["body", "jwk", "privateOmitted"], false),
    ),
    mutate(
      "P-256 coordinates are truncated",
      change(["body", "jwk", "xBytes"], 31),
      change(["body", "jwk", "yBytes"], 31),
    ),
    mutate(
      "exported public key carries the sign usage",
      change(["body", "usages", "public"], ["sign", "verify"]),
    ),
    body("signature is not fixed-width P-1363", "signatureBytes", 70),
  ],
  "crypto.key-derivation": [
    body(
      "PBKDF2 derives the wrong key material",
      "pbkdf2",
      "0000000000000000000000000000000000000000000000000000000000000000",
    ),
    body(
      "HKDF ignores the info parameter",
      "hkdf",
      "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8",
    ),
    body(
      "deriveKey and deriveBits disagree for the same inputs",
      "derivedMac",
      "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
    ),
    mutate(
      "derived HMAC key reports the wrong length",
      change(["body", "derivedAlgorithm", "length"], 128),
    ),
    body(
      "an unaligned HKDF length is silently accepted",
      "unalignedLength",
      "accepted",
    ),
    body("PBKDF2 accepts zero iterations", "zeroIterations", "accepted"),
  ],
  "crypto.key-export": [
    mutate(
      "raw secret export returns the wrong key bytes",
      change(["body", "hmacRaw"], [0, 0, 0]),
    ),
    mutate(
      "JWK export reports the wrong HMAC hash algorithm",
      change(["body", "hmacJwk", "alg"], "HS512"),
    ),
    mutate(
      "JWK export drops a granted key usage",
      change(["body", "aesJwk", "keyOps"], ["encrypt"]),
    ),
    mutate(
      "JWK export marks an extractable key as non-extractable",
      change(["body", "hmacJwk", "ext"], false),
    ),
    mutate(
      "imported key reports the wrong bit length",
      change(["body", "algorithms", 1, "length"], 256),
    ),
  ],
  "crypto.invalid-input": [
    body(
      "signing with a verify-only key is accepted",
      "wrongUsage",
      "accepted",
    ),
    body(
      "an unsupported algorithm name is accepted",
      "unknownAlgorithm",
      "accepted",
    ),
    body("an unsupported digest name is accepted", "unknownHash", "accepted"),
    body("an invalid AES key length is accepted", "badKeyLength", "accepted"),
    body("a malformed JWK is accepted", "malformedJwk", "DataCloneError"),
    body("an empty usage list is accepted", "emptyUsages", "accepted"),
    body(
      "usages inconsistent with the algorithm are accepted",
      "mismatchedUsage",
      "accepted",
    ),
    body("a non-extractable key can be exported", "nonExtractable", "accepted"),
    body("AES-GCM accepts an empty IV", "emptyIv", "accepted"),
  ],
  "messaging.message-channel": [
    mutate(
      "MessagePort delivers messages out of order",
      change(["body", "messages", "value", 0], {
        type: "Array",
        value: [{ type: "number", value: 3 }, { type: "null" }],
      }),
      change(["body", "messages", "value", 2], {
        type: "string",
        value: "first",
      }),
    ),
    mutate(
      "structured clone downgrades a Set to an array",
      change(["body", "messages", "value", 1, "value", 2, 1], {
        type: "Array",
        value: [{ type: "string", value: "a" }],
      }),
    ),
    mutate(
      "structured clone corrupts typed-array bytes",
      change(["body", "messages", "value", 1, "value", 0, 1], {
        type: "Uint8Array",
        value: [0, 0],
      }),
    ),
    body("a message is delivered synchronously", "synchronous", 1),
    body("an unserializable message is accepted", "unserializable", "accepted"),
    body(
      "a message arrives after the receiving port closed",
      "droppedAfterClose",
      false,
    ),
  ],
  "messaging.event-source": [
    mutate(
      "a named server-sent event is delivered as a default message",
      change(["body", "events", 0, "type"], "message"),
    ),
    mutate(
      "multi-line data fields are not joined with newlines",
      change(["body", "events", 1, "data"], "line one line two"),
    ),
    mutate(
      "the last event id does not persist across events",
      change(["body", "events", 1, "lastEventId"], ""),
    ),
    mutate(
      "UTF-8 event data is corrupted",
      change(["body", "events", 0, "data"], "hello ?"),
    ),
    body("an exhausted stream stays open", "exhausted", 1),
    body("the stream ends without an error event", "trace", []),
  ],
  "node.util": [
    body("promisify drops the callback result", "promisified", "value:"),
    body("promisify swallows a callback error", "promisifiedError", "accepted"),
    body(
      "inspect renders nested arrays as [Object]",
      "inspect",
      "{ a: 1, b: [Object], c: 'λ' }",
    ),
    body(
      "inspect ignores the default depth limit",
      "inspectDepth",
      "{ a: { b: { c: { d: 1 } } } }",
    ),
    body("format ignores the %j specifier", "format", "x:2:%j"),
    mutate(
      "util.types misidentifies a plain object as a Date",
      change(["body", "types", "notDate"], true),
    ),
    mutate(
      "isDeepStrictEqual ignores value types",
      change(["body", "deepEqual", 1], true),
    ),
  ],
  "node.assert": [
    body("a satisfied assertion still throws", "satisfied", {
      name: "AssertionError",
    }),
    mutate(
      "AssertionError loses its error code",
      change(["body", "strictEqual", "code"], "ERR_INVALID_ARG_TYPE"),
    ),
    mutate(
      "AssertionError reports the wrong operator",
      change(["body", "deepStrictEqual", "operator"], "notDeepStrictEqual"),
    ),
    mutate(
      "AssertionError loses the compared values",
      change(["body", "strictEqual", "actual"], null),
      change(["body", "strictEqual", "expected"], null),
    ),
    mutate(
      "deepStrictEqual stops distinguishing value types",
      change(["body", "typeSensitive"], "accepted"),
    ),
    body(
      "assert.throws accepts a body that never throws",
      "throwsMissing",
      "accepted",
    ),
    body(
      "assert.rejects accepts a fulfilled promise",
      "resolvedNotRejected",
      "accepted",
    ),
  ],
  "node.stream-timers": [
    body("Readable.toWeb corrupts object-mode chunks", "chunks", ["a", "?"]),
    body("Readable.toWeb concatenates object-mode chunks", "chunks", ["aλ"]),
    body("Readable.toWeb drops binary chunks", "binary", [0, 128]),
    body("Readable.fromWeb corrupts UTF-8 text", "fromWeb", "a?"),
    body("timers/promises resolves the longer sleep first", "race", "slow"),
    body("an aborted sleep resolves instead of rejecting", "aborted", "never"),
  ],
  "node.buffer": [
    body("Buffer encoding truncates non-ASCII bytes", "hex", "3f3f"),
    body("Buffer slice uses the wrong bounds", "slice", [187, 240]),
  ],
  "node.path-events": [
    body("once listener fires more than once", "trace", [
      "once:a",
      "on:a",
      "once:b",
      "on:b",
    ]),
    body(
      "path normalization fails to resolve parent segments",
      "path",
      "/a/../b/c",
    ),
    body("removed listeners remain registered", "remaining", 1),
  ],
  "node.async-context": [
    body("async-local state escapes its scope", "outsideMissing", false),
  ],
  "node.hash-compression": [
    body(
      "valid gzip stream contains the wrong plaintext",
      "gzipBytes",
      gzipSync("wrong λ").toString("base64"),
    ),
    body(
      "valid deflate stream contains the wrong plaintext",
      "deflateBytes",
      deflateSync("wrong λ").toString("base64"),
    ),
    body("gzip encoder is an identity function", "gzipBytes", "aGVsbG8gzrs="),
    body(
      "deflate encoder is an identity function",
      "deflateBytes",
      "aGVsbG8gzrs=",
    ),
    body("gzip decoder returns compressed input", "gzip", "aGVsbG8gzrs="),
    body("deflate decoder drops UTF-8 text", "deflate", "hello ?"),
  ],
  "wasm.module": [
    body(
      "WebAssembly addition does not wrap i32 overflow",
      "overflow",
      2147483648,
    ),
    body(
      "WebAssembly treats signed operands as unsigned",
      "negative",
      4294967291,
    ),
  ],
  "assets.binding": [
    body(
      "asset binding returns fallback content for a missing file",
      "missing",
      200,
    ),
    body("asset binding loses file contents", "text", ""),
  ],
  "assets.html-routing": [
    mutate(
      "extensionless path does not serve its HTML asset",
      change(["body", "page", "status"], 404),
      change(["body", "page", "body"], ""),
    ),
    mutate(
      "the .html path serves the asset instead of redirecting",
      change(["body", "pageHtml", "status"], 200),
      change(["body", "pageHtml", "location"], null),
    ),
    mutate(
      "a directory serves its index without the trailing-slash redirect",
      change(["body", "folder", "status"], 200),
      change(["body", "folder", "location"], null),
    ),
    mutate(
      "a missing path falls back to an existing page",
      change(["body", "missing", "status"], 200),
      change(
        ["body", "missing", "body"],
        "<!doctype html>\n<title>tck page</title>",
      ),
    ),
    mutate(
      "a _headers rule does not reach the HTML asset",
      change(["body", "page", "page"], null),
    ),
  ],
  "assets.redirects": [
    mutate(
      "a _redirects rule is ignored",
      change(["body", "permanent", "status"], 404),
      change(["body", "permanent", "location"], null),
    ),
    mutate(
      "a temporary redirect is served as permanent",
      change(["body", "temporary", "status"], 301),
    ),
    mutate(
      "an unmatched path picks up a redirect target",
      change(["body", "unmatched", "status"], 302),
      change(["body", "unmatched", "location"], "/page"),
    ),
  ],
  "assets.worker-first": [
    mutate(
      "a worker-first path serves the shadowed asset instead of the Worker",
      change([0, "body"], "shadowed asset\n"),
      change([0, "headers", "content-type"], "text/plain; charset=utf-8"),
    ),
    mutate(
      "a negative rule does not restore asset-first order",
      change([1, "body"], { served: "worker", path: "/asset-first/note.txt" }),
      change([1, "headers", "content-type"], "application/json"),
    ),
    mutate(
      "the asset-first path loses the asset contents",
      change([1, "body"], ""),
    ),
  ],
  "dynamic.limits": [
    body(
      "declared custom limits drop a structured-clone env value",
      "token",
      null,
    ),
    body(
      "a subrequest inside the declared budget is refused",
      "upstream",
      "no binding",
    ),
  ],
  "facets.outbound-transaction": [
    mutate(
      "the former celld restriction refuses outbound during a root transaction",
      change(["body", "inTransaction", "outbound"], "rejected"),
    ),
    mutate(
      "an outbound effect from a facet is refused with no root transaction",
      change(["body", "control", "outbound"], "rejected"),
    ),
    mutate(
      "the facet write made during the root transaction is lost",
      change(["body", "after", "balance"], 10),
    ),
    mutate(
      "the outbound call during the root transaction reaches the wrong target",
      change(["body", "inTransaction", "outbound"], "gateway /binding"),
    ),
  ],
  "dynamic.fetch": [
    body("dynamic worker is not invoked", "text", "static fallback"),
  ],
  "dynamic.props": [
    mutate("per-call props are lost", change(["body", "tenant", "props"], {})),
    mutate(
      "a reused loaded Worker keeps the first call's props",
      change(["body", "other", "props"], { tenant: "alpha", seed: 7 }),
    ),
  ],
  "dynamic.bindings": [
    body("a structured-clone env value is lost", "token", null),
    body(
      "a service capability in env does not reach its entrypoint",
      "upstream",
      "no binding",
    ),
  ],
  "dynamic.outbound": [
    mutate(
      "globalOutbound null does not block a global fetch",
      change(["body", "blocked", "global"], "gateway /global"),
    ),
    mutate(
      "a blocked outbound also disables an env service binding",
      change(["body", "blocked", "binding"], "blocked"),
    ),
    mutate(
      "a globalOutbound gateway does not intercept a global fetch",
      change(["body", "gateway", "global"], "blocked"),
    ),
  ],
  "facets.isolation": [
    body("one facet's counter leaks into a different facet", "c", { n: 3 }),
  ],
  "facets.transaction": [
    mutate(
      "a committed facet transaction is discarded",
      change(["body", "committed", "balance"], 10),
    ),
    mutate(
      "a rolled back facet transaction keeps its writes",
      change(["body", "restored", "balance"], 30),
    ),
    mutate(
      "a facet transaction does not read its own uncommitted write",
      change(["body", "rollback", "inside"], 20),
    ),
  ],
  "repro.body-readers": [
    mutate(
      "Request text reader accepts repeat consumption",
      change(["body", 0, "second"], { outcome: "fulfilled" }),
    ),
    mutate(
      "Response formData reader accepts repeat consumption",
      change(["body", 10, "second"], { outcome: "fulfilled" }),
    ),
    mutate(
      "null Request body is incorrectly marked used",
      change(["body", 5, "used"], true),
    ),
    mutate(
      "null Response body is incorrectly marked used",
      change(["body", 11, "used"], true),
    ),
  ],
  "repro.storage-errors": [
    mutate(
      "async storage accepts a negative list limit",
      change(["body", "asyncLimit", "outcome"], "fulfilled"),
    ),
    mutate(
      "sync storage throws the wrong list error",
      change(["body", "syncLimit", "name"], "Error"),
    ),
    mutate(
      "clone error loses DOMException identity",
      change(["body", "asyncPut", "domException"], false),
    ),
    body("failed batch stores a partial result", "absent", false),
  ],
  "websocket.outbound-close": [
    mutate(
      "the server's close code is replaced by a generic normal closure",
      change(["body", "closed", "code"], 1000),
    ),
    mutate(
      "the close reason is dropped from the client's close event",
      change(["body", "closed", "reason"], ""),
    ),
    mutate(
      "an outbound socket stays open after its peer closed it",
      change(["body", "readyState"], 1),
    ),
    mutate(
      "close() accepts the reserved code 1005",
      change(["body", "invalidCode"], "accepted"),
    ),
    body("the outbound frame is not echoed back", "echo", {
      type: "message",
      data: "echo:",
    }),
  ],
  "websocket.concurrent-sockets": [
    mutate(
      "one of sixteen concurrent sockets is silently dropped",
      change(["body", "open"], 15),
    ),
    mutate(
      "concurrent sockets cross-deliver one another's frames",
      change(["body", "echoes", 0], "echo:n1"),
    ),
  ],
  "flags.enabled-defaults": [
    mutate(
      "deleteAll() preserves the alarm even with the default flag on",
      change(["body", "deleteAll", "alarmPresent"], true),
    ),
    mutate(
      "the default binaryType keeps the legacy value",
      change(["body", "binaryType"], "arraybuffer"),
    ),
  ],
  "flags.disabled-counterparts": [
    mutate(
      "delete_all_preserves_alarm is ignored and the alarm is deleted",
      change(["body", "deleteAll", "alarmPresent"], false),
    ),
    mutate(
      "no_websocket_standard_binary_type is ignored",
      change(["body", "binaryType"], "blob"),
    ),
    mutate(
      "deleteAll() leaves stored values behind under the disable flag",
      change(["body", "deleteAll", "values"], [["a", 1]]),
    ),
  ],
  "kv.expiration": [
    mutate(
      "expirationTtl is ignored and the key is listed without an expiration",
      change(["body", "expiringInWindow"], false),
    ),
    mutate(
      "a key stored without a TTL is given one",
      change(["body", "keepExpiration"], 1789786428),
    ),
    mutate(
      "a TTL below the documented minimum is accepted",
      change(["body", "tooShort"], "accepted"),
    ),
    body("metadata is lost when an expiration is set", "metadata", null),
  ],
  "d1.exec-batch": [
    mutate(
      "exec() reports a single statement for a multi-statement script",
      change(["body", "execCount"], 1),
    ),
    mutate(
      "a batched insert is lost before the batch's own read",
      change(["body", "results", 2], [{ n: 2 }]),
    ),
    mutate(
      "an insert reports no changed rows",
      change(["body", "changes", 0], 0),
    ),
    mutate(
      "a statement in the batch reports failure",
      change(["body", "success", 1], false),
    ),
  ],
  "r2.list-options": [
    mutate(
      "the delimiter does not roll up nested keys",
      change(["body", "objects"], ["/dir/a", "/dir/b", "/top"]),
    ),
    mutate(
      "no common prefix is reported for the delimiter",
      change(["body", "prefixes"], []),
    ),
    mutate(
      "a deeper prefix does not descend into the rolled-up keys",
      change(["body", "nested"], []),
    ),
    mutate(
      "include is ignored and stored HTTP metadata is not returned",
      change(["body", "included", "http"], null),
    ),
    mutate(
      "stored metadata leaks into a listing that did not request it",
      change(["body", "bare", "custom"], { label: "λ" }),
    ),
    mutate(
      "writeHttpMetadata loses the stored content disposition",
      change(["body", "written", "disposition"], null),
    ),
  ],
  "queues.retry-delay": [
    mutate(
      "the explicit retry delay is ignored and redelivery is immediate",
      change(["body", "delayed:2", "at"], 1789785828970),
    ),
    mutate(
      "redelivery arrives as a new message instead of a retry",
      change(["body", "delayed:2", "id"], "0000000000000000000000000000000a"),
    ),
    mutate(
      "the retried delivery reports the first attempt number again",
      change(["body", "delayed:2", "attempts"], 1),
    ),
  ],
  "workflows.timeout": [
    mutate(
      "waitForEvent resolves instead of timing out",
      change(["body", "output", "timedOut"], "accepted"),
    ),
    mutate(
      "the instance does not continue past the timed-out step",
      change(["body", "output", "after"], 20),
    ),
  ],
} satisfies Record<string, readonly [Mutation, ...Mutation[]]>;

export const divergenceMutations = {
  "crypto.key-export": [
    mutate(
      "the documented jwk restriction is extended to raw secret export",
      change(["body", "hmacRaw"], "NotSupportedError"),
    ),
    mutate(
      "raw secret export returns the wrong key bytes under the waiver",
      change(
        ["body", "aesRaw"],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
      ),
    ),
  ],
  "cache.documented-miss": [
    body("always-miss cache invents a hit", "value", "invented"),
  ],
  "rpc.returned-target": [
    mutate(
      "unrelated RPC failure is incorrectly waived",
      change(["body", "error", "message"], "network unavailable"),
    ),
  ],
} satisfies Record<string, readonly [Mutation, ...Mutation[]]>;
