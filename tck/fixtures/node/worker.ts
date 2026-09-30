import { Effect } from "effect";
import assert, { AssertionError } from "node:assert";
import { Buffer } from "node:buffer";
import { EventEmitter } from "node:events";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { posix } from "node:path";
import { Readable } from "node:stream";
import { setImmediate, setTimeout as delay } from "node:timers/promises";
import {
  format,
  inspect,
  isDeepStrictEqual,
  promisify,
  types,
} from "node:util";
import { gzipSync, gunzipSync, deflateSync, inflateSync } from "node:zlib";
import { operation, outcome, platform, rejection } from "../core/Platform.js";
import core from "../core/worker.js";
export { Probe, Service, TestWorkflow } from "../core/worker.js";
export default {
  ...core,
  fetch(request: Request, env: Env) {
    return Effect.runPromise(
      Effect.gen(function* () {
        switch (new URL(request.url).pathname) {
          case "/node/buffer": {
            const buffer = Buffer.from("λ🌍", "utf8");
            return Response.json({
              hex: buffer.toString("hex"),
              base64: buffer.toString("base64"),
              roundTrip: Buffer.from(
                buffer.toString("base64"),
                "base64",
              ).toString("utf8"),
              slice: [...buffer.subarray(0, 2)],
            });
          }
          case "/node/path-events": {
            const emitter = new EventEmitter();
            const trace: string[] = [];
            emitter.once("event", (value) => trace.push("once:" + value));
            const listener = (value: string) => {
              trace.push("on:" + value);
            };
            emitter.on("event", listener);
            emitter.emit("event", "a");
            emitter.emit("event", "b");
            emitter.off("event", listener);
            emitter.emit("event", "c");
            return Response.json({
              path: posix.normalize("/a/../b/./c"),
              basename: posix.basename("/a/b.txt", ".txt"),
              trace,
              remaining: emitter.listenerCount("event"),
            });
          }
          case "/node/async-context": {
            const storage = new AsyncLocalStorage<string>();
            const inside = yield* platform(() =>
              storage.run("request", () =>
                Effect.runPromise(
                  Effect.gen(function* () {
                    yield* platform(() => Promise.resolve());
                    return storage.getStore();
                  }),
                ),
              ),
            );
            return Response.json({
              inside,
              outsideMissing: storage.getStore() === undefined,
            });
          }
          case "/node/hash-compression": {
            const bytes = Buffer.from("hello λ");
            return Response.json({
              sha256: createHash("sha256").update("abc").digest("hex"),
              gzip: gunzipSync(
                Buffer.from(
                  "1f8b08000000000002ffcb48cdc9c95738b71b00396394ce08000000",
                  "hex",
                ),
              ).toString(),
              gzipBytes: gzipSync(bytes).toString("base64"),
              deflate: inflateSync(
                Buffer.from("789ccb48cdc9c95738b71b000f2203be", "hex"),
              ).toString(),
              deflateBytes: deflateSync(bytes).toString("base64"),
            });
          }
          case "/node/util": {
            const join = (
              value: string,
              done: (error: Error | null, result: string) => void,
            ) => {
              done(null, `value:${value}`);
            };
            const broken = (done: (error: Error) => void) => {
              done(new TypeError("promisified failure"));
            };
            return Response.json({
              promisified: yield* platform(() => promisify(join)("x")),
              promisifiedError: yield* rejection(
                platform(() => promisify(broken)()),
              ),
              inspect: inspect({ a: 1, b: [1, 2], c: "λ" }),
              inspectDepth: inspect({ a: { b: { c: { d: 1 } } } }),
              format: format("%s:%d:%j", "x", 2, { a: 1 }),
              types: {
                date: types.isDate(new Date()),
                view: types.isArrayBufferView(new TextEncoder().encode("λ")),
                promise: types.isPromise(Promise.resolve()),
                map: types.isMap(new Map()),
                notDate: types.isDate({}),
              },
              deepEqual: [
                isDeepStrictEqual({ a: [1] }, { a: [1] }),
                isDeepStrictEqual({ a: 1 }, { a: "1" }),
              ],
              // node:util re-exports the same WHATWG encoders as the global scope.
              sharedEncoder: [
                new TextEncoder().encode("λ").length,
                new TextDecoder().decode(new Uint8Array([206, 187])),
              ],
            });
          }
          case "/node/assert": {
            const failure = (body: () => void) =>
              operation(body).pipe(
                Effect.as("accepted" as unknown),
                Effect.catch((cause) =>
                  Effect.succeed(
                    cause instanceof AssertionError
                      ? {
                          name: cause.name,
                          code: cause.code,
                          operator: cause.operator,
                          actual: cause.actual,
                          expected: cause.expected,
                        }
                      : {
                          name: cause instanceof Error ? cause.name : "Unknown",
                        },
                  ),
                ),
              );
            return Response.json({
              satisfied: yield* failure(() => {
                assert.ok(true);
                assert.strictEqual("λ", "λ");
                assert.deepStrictEqual({ a: [1] }, { a: [1] });
              }),
              strictEqual: yield* failure(() => {
                assert.strictEqual(1, 2);
              }),
              deepStrictEqual: yield* failure(() => {
                assert.deepStrictEqual({ a: [1] }, { a: [2] });
              }),
              // deepStrictEqual is type sensitive where deepEqual is not.
              typeSensitive: yield* failure(() => {
                assert.deepStrictEqual({ a: 1 }, { a: "1" });
              }),
              throws: yield* failure(() => {
                assert.throws(() => {
                  // oxlint-disable-next-line effect/throw-in-effect-gen -- assert.throws is specified in terms of a native throw; the failure adapter captures it.
                  throw new TypeError("expected");
                }, TypeError);
              }),
              // A body that never throws is the specified "missing expected
              // exception" failure, independent of error-class matching rules.
              throwsMissing: yield* failure(() => {
                assert.throws(() => undefined, TypeError);
              }),
              rejects: yield* rejection(
                platform(() =>
                  assert.rejects(
                    Promise.reject(new TypeError("expected")),
                    TypeError,
                  ),
                ),
              ),
              resolvedNotRejected: yield* rejection(
                platform(() => assert.rejects(Promise.resolve())),
              ),
            });
          }
          case "/node/stream-timers": {
            // Readable.toWeb throws synchronously on a runtime that does not
            // implement it, so each conversion reports its own outcome.
            const toWeb = (source: Readable) =>
              operation(
                () => Readable.toWeb(source) as unknown as ReadableStream,
              );
            const chunks = yield* outcome(
              toWeb(Readable.from(["a", "λ"])).pipe(
                Effect.flatMap((stream) =>
                  platform(async () => {
                    const reader = stream.getReader();
                    const read: unknown[] = [];
                    for (;;) {
                      const next = await reader.read();
                      if (next.done) break;
                      read.push(next.value);
                    }
                    return read;
                  }),
                ),
              ),
            );
            const binary = yield* outcome(
              toWeb(
                Readable.from([
                  new Uint8Array([0, 128]),
                  new Uint8Array([255]),
                ]),
              ).pipe(
                Effect.flatMap((stream) =>
                  platform(() => new Response(stream).arrayBuffer()),
                ),
                Effect.map((bytes) => [...new Uint8Array(bytes)]),
              ),
            );
            const fromWeb = yield* outcome(
              operation(() =>
                Readable.fromWeb(new Response("aλ").body as never),
              ).pipe(
                Effect.flatMap((node) =>
                  platform(async () => {
                    const parts: string[] = [];
                    for await (const chunk of node)
                      parts.push(
                        Buffer.from(chunk as Uint8Array).toString("utf8"),
                      );
                    return parts.join("");
                  }),
                ),
              ),
            );
            return Response.json({
              chunks,
              chunkType: Array.isArray(chunks) ? typeof chunks[0] : null,
              binary,
              fromWeb,
              // The shorter sleep must settle first; both are real timers.
              race: yield* platform(() =>
                Promise.race([delay(1, "fast"), delay(60, "slow")]),
              ),
              immediate: yield* platform(() => setImmediate("soon")),
              aborted: yield* rejection(
                platform(() =>
                  delay(60_000, "never", { signal: AbortSignal.abort() }),
                ),
              ),
            });
          }
          default:
            return yield* platform(() => core.fetch(request, env));
        }
      }),
    );
  },
} satisfies ExportedHandler<Env>;
