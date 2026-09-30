import { Effect, Schema } from "effect";
import { endpoint, poll } from "./CoreCases.js";
import { call, define, response } from "./Cases.js";
import { decodeAs } from "./Artifacts.js";
import { equal } from "./Oracle.js";
import type { TestCase } from "./Domain.js";
const bindingCase = (
  id: string,
  path: string,
  expected: unknown,
  family: string,
) =>
  endpoint(id, path, expected, `https://developers.cloudflare.com/${family}/`);
const workflowMode = (
  mode: "retry" | "event" | "timeout",
  expected: unknown,
): TestCase => ({
  ...define(
    `workflows.${mode}`,
    (target, input) =>
      Effect.gen(function* () {
        yield* equal(
          yield* call(target, input, `/workflows/start?mode=${mode}`),
          response({ sameId: true }),
        );
        if (mode === "event")
          yield* equal(
            yield* call(target, input, "/workflows/send"),
            response({ sent: true }),
          );
        return yield* poll(
          call(target, input, "/workflows/state"),
          (result) =>
            (result.body as { status?: string } | null)?.status === "complete",
        );
      }),
    () => response({ status: "complete", output: expected }),
  ),
  contract: "https://developers.cloudflare.com/workflows/build/workers-api/",
});
const Stamp = Schema.Struct({
  attempts: Schema.Int,
  at: Schema.Int,
  id: Schema.String,
});
/**
 * An explicit `retry({ delaySeconds })` must postpone redelivery beyond the
 * consumer's configured `retry_delay` of 0 and keep the message identity.
 * Arrival times are runtime-specific, so the oracle bounds the interval instead
 * of comparing the two observations for equality.
 */
const queueRetryDelay: TestCase = {
  id: "queues.retry-delay",
  contract:
    "https://developers.cloudflare.com/queues/configuration/javascript-apis/",
  run: (target, input) =>
    Effect.gen(function* () {
      yield* equal(
        yield* call(target, input, "/queues/retry-delay"),
        response({ sent: true }),
      );
      return yield* poll(
        call(target, input, "/queues/state"),
        (result) =>
          typeof result.body === "object" &&
          result.body !== null &&
          "delayed:2" in result.body,
      );
    }),
  check: (value) =>
    Effect.gen(function* () {
      const observation = yield* decodeAs(
        Schema.Struct({
          status: Schema.Literal(200),
          headers: Schema.Record(Schema.String, Schema.String),
          body: Schema.Struct({
            "delayed:1": Stamp,
            "delayed:2": Stamp,
          }),
        }),
        "assertion",
      )(value);
      const first = observation.body["delayed:1"];
      const second = observation.body["delayed:2"];
      yield* equal([first.attempts, second.attempts], [1, 2]);
      yield* equal(first.id, second.id);
      yield* equal(first.id.length > 0, true);
      const elapsed = second.at - first.at;
      yield* equal(elapsed >= 2500 && elapsed < 60000, true);
    }),
  compare: () => Effect.void,
};
export const serviceCases: ReadonlyArray<TestCase> = [
  bindingCase(
    "r2.key-identity",
    "/r2/key-identity",
    {
      values: [
        "value-0",
        "value-1",
        "value-2",
        "value-3",
        "value-4",
        "value-5",
      ],
      listed: [
        "/case/a/b",
        "case/a/%2F",
        "case/a//b",
        "case/a/b",
        "case/a/b/",
        "case/λ/%",
      ],
      deleted: true,
      sibling: "value-1",
    },
    "r2/api/workers/workers-api-reference",
  ),

  bindingCase(
    "kv.stream-put",
    "/kv/stream-put",
    {
      bytes: [0, 128, 255, 65, 206, 187],
      metadata: { source: "stream" },
      deleted: null,
    },
    "kv/api/write-key-value-pairs",
  ),
  bindingCase(
    "kv.stream-limit",
    "/kv/stream-limit",
    { rejected: true, missing: true },
    "kv/api/write-key-value-pairs",
  ),
  workflowMode("retry", { attempts: 2 }),
  workflowMode("event", { payload: { value: "λ" }, type: "continue" }),
  workflowMode("timeout", { timedOut: "Error", after: 25 }),
  queueRetryDelay,
  bindingCase(
    "kv.expiration",
    "/kv/expiration",
    {
      value: "λ",
      metadata: { tag: 1 },
      names: ["/keep", "/ttl"],
      keepExpiration: null,
      expiringInWindow: true,
      tooShort: "Error",
    },
    "kv",
  ),
  bindingCase(
    "d1.exec-batch",
    "/d1/exec-batch",
    {
      execCount: 2,
      success: [true, true, true],
      results: [[], [], [{ n: 1 }, { n: 2 }]],
      changes: [1, 1, 0],
    },
    "d1",
  ),
  bindingCase(
    "r2.list-options",
    "/r2/list-options",
    {
      objects: ["/top"],
      prefixes: ["/dir/"],
      truncated: false,
      nested: ["/dir/a", "/dir/b"],
      nestedPrefixes: [],
      // Without `include`, a listed object carries no stored metadata.
      bare: { http: {}, custom: {} },
      included: {
        http: "text/plain; charset=utf-8",
        custom: { label: "λ" },
      },
      written: {
        type: "text/plain; charset=utf-8",
        disposition: 'attachment; filename="report.txt"',
        cache: "max-age=42",
      },
    },
    "r2",
  ),
  bindingCase(
    "rpc.named-service",
    "/services/rpc",
    { greeting: "hello λ" },
    "workers/runtime-apis/bindings/service-bindings/rpc",
  ),
  bindingCase(
    "kv.metadata-list",
    "/kv/round-trip",
    {
      value: { value: { value: "λ" }, metadata: { tag: 7 } },
      names: ["/a"],
      complete: true,
      missing: null,
    },
    "kv",
  ),
  bindingCase(
    "kv.binary-pagination",
    "/kv/binary-pagination",
    { bytes: [0, 128, 255], first: ["/a"], second: ["/b"], complete: true },
    "kv",
  ),
  bindingCase(
    "d1.bindings-results",
    "/d1/query",
    {
      success: true,
      inserted: [{ n: 7, text: "λ'" }],
      first: { n: 7, text: "λ'", bytes: [0, 255] },
      raw: [[7, "λ'"]],
    },
    "d1",
  ),
  bindingCase(
    "d1.batch-rollback",
    "/d1/batch-rollback",
    { rejected: true, value: 1 },
    "d1",
  ),
  bindingCase(
    "r2.metadata-range-delete",
    "/r2/round-trip",
    {
      bytes: [0, 128, 255, 10],
      range: [128, 255],
      size: 4,
      type: "application/test",
      metadata: { label: "λ" },
      sameEtag: true,
      names: ["/a"],
      missing: null,
    },
    "r2",
  ),
  bindingCase(
    "r2.conditional-write",
    "/r2/conditional",
    { rejected: true, accepted: true, value: "second" },
    "r2",
  ),
  bindingCase(
    "r2.multipart",
    "/r2/multipart",
    { part: 1, value: "single final part", aborted: null },
    "r2",
  ),
  {
    ...define(
      "queues.batch-ack-retry",
      (target, input) =>
        Effect.gen(function* () {
          yield* equal(
            yield* call(target, input, "/queues/start"),
            response({ sent: true }),
          );
          return yield* poll(call(target, input, "/queues/state"), (result) => {
            const body = result.body as Record<
              string,
              { attempts: number }
            > | null;
            return (
              body?.single?.attempts === 2 &&
              body["batch-a"] !== undefined &&
              body["batch-b"] !== undefined
            );
          });
        }),
      () =>
        response({
          single: { attempts: 2, retry: true },
          "batch-a": { attempts: 1, retry: false },
          "batch-b": { attempts: 1, retry: false },
        }),
    ),
    contract:
      "https://developers.cloudflare.com/queues/configuration/javascript-apis/",
  },
  {
    ...define(
      "workflows.steps-sleep-result",
      (target, input) =>
        Effect.gen(function* () {
          yield* equal(
            yield* call(target, input, "/workflows/start"),
            response({ sameId: true }),
          );
          return yield* poll(
            call(target, input, "/workflows/state"),
            (result) =>
              (result.body as { status?: string } | null)?.status ===
              "complete",
          );
        }),
      () => response({ status: "complete", output: { result: 41 } }),
    ),
    contract: "https://developers.cloudflare.com/workflows/build/workers-api/",
  },
];
