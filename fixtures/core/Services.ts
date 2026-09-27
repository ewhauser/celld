import {
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
} from "cloudflare:workers";
import { Effect, Schema } from "effect";
import { platform, rejection } from "./Platform.js";
export class TestWorkflow extends WorkflowEntrypoint<
  Env,
  { value: number; mode?: string }
> {
  run(
    event: WorkflowEvent<{ value: number; mode?: string }>,
    step: WorkflowStep,
  ) {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        if (event.payload.mode === "retry") {
          const attempts = yield* platform(() =>
            step.do(
              "retry-once",
              { retries: { limit: 2, delay: "1 second", backoff: "constant" } },
              () =>
                Effect.runPromise(
                  Effect.gen({ self: this }, function* () {
                    const attempt = yield* platform(() =>
                      this.env.PROBE.getByName(
                        event.instanceId,
                      ).workflowAttempt(),
                    );
                    if (attempt === 1)
                      return yield* Effect.fail(new Error("retry step"));
                    return attempt;
                  }),
                ),
            ),
          );
          return { attempts };
        }
        if (event.payload.mode === "timeout") {
          // A waitForEvent deadline must expire as a step failure the workflow
          // can observe, and the instance must keep running afterwards.
          const timedOut = yield* rejection(
            platform(() =>
              step.waitForEvent("absent", {
                type: "never",
                timeout: "3 seconds",
              }),
            ),
          );
          yield* platform(() =>
            step.sleepUntil("resume", new Date(Date.now() + 1000)),
          );
          return {
            timedOut,
            after: yield* platform(() =>
              step.do("after", () =>
                Effect.runPromise(Effect.succeed(event.payload.value + 5)),
              ),
            ),
          };
        }
        if (event.payload.mode === "event") {
          const received = yield* platform(() =>
            step.waitForEvent<{ value: string }>("receive", {
              type: "continue",
              timeout: "10 seconds",
            }),
          );
          return { payload: received.payload, type: received.type };
        }
        const doubled = yield* platform(() =>
          step.do("double", () =>
            Effect.runPromise(Effect.succeed(event.payload.value * 2)),
          ),
        );
        yield* platform(() => step.sleep("pause", "1 second"));
        return yield* platform(() =>
          step.do("finish", () =>
            Effect.runPromise(Effect.succeed({ result: doubled + 1 })),
          ),
        );
      }),
    );
  }
}
export const QueueBody = Schema.Struct({
  namespace: Schema.String,
  key: Schema.String,
  retry: Schema.Boolean,
  /** Record one entry per attempt with its arrival time and message identity. */
  stamp: Schema.optionalKey(Schema.Boolean),
  delaySeconds: Schema.optionalKey(Schema.Int),
});
export const consume = (batch: MessageBatch<unknown>, env: Env) =>
  Effect.runPromise(
    Effect.gen(function* () {
      for (const message of batch.messages) {
        const body = yield* Schema.decodeUnknownEffect(QueueBody)(message.body);
        const stub = env.PROBE.getByName(body.namespace);
        const key = body.stamp ? `${body.key}:${message.attempts}` : body.key;
        yield* platform(() =>
          stub.fetch(
            new Request(`https://fixture.test/storage/put?key=${key}`, {
              method: "POST",
              body: JSON.stringify(
                body.stamp
                  ? {
                      attempts: message.attempts,
                      at: Date.now(),
                      id: message.id,
                    }
                  : { attempts: message.attempts, retry: body.retry },
              ),
            }),
          ),
        );
        if (body.retry && message.attempts === 1)
          message.retry({ delaySeconds: body.delaySeconds ?? 0 });
        else message.ack();
      }
    }),
  );
export const services = (request: Request, env: Env, name: string) =>
  Effect.gen(function* () {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/r2/key-identity": {
        const suffixes = ["a/b", "a//b", "a/b/", "λ/%", "a/%2F"];
        const keys = [
          ...suffixes.map((key) => `${name}/${key}`),
          `/${name}/a/b`,
        ];
        return yield* Effect.gen(function* () {
          yield* Effect.forEach(keys, (key, i) =>
            platform(() => env.BUCKET.put(key, `value-${i}`)),
          );
          const values = yield* Effect.forEach(keys, (key) =>
            platform(() => env.BUCKET.get(key)).pipe(
              Effect.flatMap((object) =>
                object ? platform(() => object.text()) : Effect.succeed(null),
              ),
            ),
          );
          const lists = yield* Effect.forEach(
            [name + "/", "/" + name + "/"],
            (prefix) => platform(() => env.BUCKET.list({ prefix })),
          );
          const listed = lists
            .flatMap((list) =>
              list.objects.map((object) => object.key.replace(name, "case")),
            )
            .sort();
          yield* platform(() => env.BUCKET.delete(keys[0]!));
          const sibling = yield* platform(() => env.BUCKET.get(keys[1]!));
          return {
            values,
            listed,
            deleted: (yield* platform(() => env.BUCKET.get(keys[0]!))) === null,
            sibling: sibling ? yield* platform(() => sibling.text()) : null,
          };
        }).pipe(
          Effect.ensuring(
            platform(() => env.BUCKET.delete(keys)).pipe(Effect.orDie),
          ),
        );
      }
      case "/services/rpc":
        return { greeting: yield* platform(() => env.SERVICE.greet("λ")) };
      case "/kv/round-trip": {
        const key = name + "/a";
        yield* platform(() =>
          env.KV.put(key, JSON.stringify({ value: "λ" }), {
            metadata: { tag: 7 },
          }),
        );
        const value = yield* platform(() =>
          env.KV.getWithMetadata(key, "json"),
        );
        const list = yield* platform(() =>
          env.KV.list({ prefix: name + "/", limit: 1 }),
        );
        yield* platform(() => env.KV.delete(key));
        return {
          value: { value: value.value, metadata: value.metadata },
          names: list.keys.map((k) => k.name.slice(name.length)),
          complete: list.list_complete,
          missing: yield* platform(() => env.KV.get(name + "/never")),
        };
      }
      case "/kv/binary-pagination": {
        yield* platform(() =>
          env.KV.put(name + "/a", new Uint8Array([0, 128, 255])),
        );
        yield* platform(() => env.KV.put(name + "/b", "b"));
        const bytes = yield* platform(() =>
          env.KV.get(name + "/a", "arrayBuffer"),
        );
        const first = yield* platform(() =>
          env.KV.list({ prefix: name + "/", limit: 1 }),
        );
        if (first.list_complete)
          return yield* Effect.fail(
            new Error("First page unexpectedly complete"),
          );
        const second = yield* platform(() =>
          env.KV.list({ prefix: name + "/", cursor: first.cursor, limit: 1 }),
        );
        return {
          bytes: bytes ? [...new Uint8Array(bytes)] : null,
          first: first.keys.map((k) => k.name.slice(name.length)),
          second: second.keys.map((k) => k.name.slice(name.length)),
          complete: second.list_complete,
        };
      }
      case "/kv/stream-put": {
        const key = name + "/stream";
        const chunks = [
          [0, 128],
          [255, 65],
          [206, 187],
        ];
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            for (const chunk of chunks)
              controller.enqueue(new Uint8Array(chunk));
            controller.close();
          },
        });
        yield* platform(() =>
          env.KV.put(key, stream, { metadata: { source: "stream" } }),
        );
        const stored = yield* platform(() =>
          env.KV.getWithMetadata(key, "arrayBuffer"),
        );
        yield* platform(() => env.KV.delete(key));
        return {
          bytes: stored.value ? [...new Uint8Array(stored.value)] : null,
          metadata: stored.metadata,
          deleted: yield* platform(() => env.KV.get(key)),
        };
      }
      case "/kv/stream-limit": {
        const key = name + "/oversized-stream";
        const chunk = new Uint8Array(1024 * 1024);
        let remaining = 26;
        const stream = new ReadableStream<Uint8Array>({
          pull(controller) {
            if (remaining-- > 0) controller.enqueue(chunk);
            else controller.close();
          },
        });
        const rejected = yield* platform(() => env.KV.put(key, stream)).pipe(
          Effect.as(false),
          Effect.catch(() => Effect.succeed(true)),
        );
        return {
          rejected,
          missing: (yield* platform(() => env.KV.get(key))) === null,
        };
      }
      case "/d1/query": {
        yield* platform(() =>
          env.DB.prepare(
            "CREATE TABLE IF NOT EXISTS values_table (namespace TEXT, n INTEGER, text TEXT, bytes BLOB, PRIMARY KEY(namespace, n))",
          ).run(),
        );
        const inserted = yield* platform(() =>
          env.DB.prepare(
            "INSERT INTO values_table VALUES (?, ?, ?, ?) RETURNING n, text",
          )
            .bind(name, 7, "λ'", new Uint8Array([0, 255]))
            .all(),
        );
        const first = yield* platform(() =>
          env.DB.prepare(
            "SELECT n, text, bytes FROM values_table WHERE namespace = ?",
          )
            .bind(name)
            .first(),
        );
        const raw = yield* platform(() =>
          env.DB.prepare("SELECT n, text FROM values_table WHERE namespace = ?")
            .bind(name)
            .raw(),
        );
        return {
          success: inserted.success,
          inserted: inserted.results,
          first,
          raw,
        };
      }
      case "/d1/batch-rollback": {
        yield* platform(() =>
          env.DB.prepare(
            "CREATE TABLE IF NOT EXISTS batch_table (namespace TEXT PRIMARY KEY, value INTEGER)",
          ).run(),
        );
        yield* platform(() =>
          env.DB.prepare("INSERT INTO batch_table VALUES (?, 1)")
            .bind(name)
            .run(),
        );
        const rejected = yield* rejection(
          platform(() =>
            env.DB.batch([
              env.DB.prepare(
                "UPDATE batch_table SET value = 2 WHERE namespace = ?",
              ).bind(name),
              env.DB.prepare("INSERT INTO batch_table VALUES (?, 3)").bind(
                name,
              ),
            ]),
          ),
        );
        return {
          rejected: rejected !== "accepted",
          value: yield* platform(() =>
            env.DB.prepare("SELECT value FROM batch_table WHERE namespace = ?")
              .bind(name)
              .first("value"),
          ),
        };
      }
      case "/r2/round-trip": {
        const key = name + "/a";
        const put = yield* platform(() =>
          env.BUCKET.put(key, new Uint8Array([0, 128, 255, 10]), {
            httpMetadata: { contentType: "application/test" },
            customMetadata: { label: "λ" },
          }),
        );
        const head = yield* platform(() => env.BUCKET.head(key));
        const get = yield* platform(() => env.BUCKET.get(key));
        if (!put || !head || !get)
          return yield* Effect.fail(new Error("Missing R2 object"));
        const ranged = yield* platform(() =>
          env.BUCKET.get(key, { range: { offset: 1, length: 2 } }),
        );
        const bytes = [
          ...new Uint8Array(yield* platform(() => get.arrayBuffer())),
        ];
        const range =
          ranged && "arrayBuffer" in ranged
            ? [...new Uint8Array(yield* platform(() => ranged.arrayBuffer()))]
            : null;
        const listed = yield* platform(() =>
          env.BUCKET.list({ prefix: name + "/" }),
        );
        yield* platform(() => env.BUCKET.delete(key));
        return {
          bytes,
          range,
          size: head.size,
          type: head.httpMetadata?.contentType,
          metadata: head.customMetadata,
          sameEtag: put.etag === head.etag && head.etag === get.etag,
          names: listed.objects.map((o) => o.key.slice(name.length)),
          missing: yield* platform(() => env.BUCKET.get(key)),
        };
      }
      case "/r2/conditional": {
        const put = yield* platform(() => env.BUCKET.put(name, "first"));
        if (!put) return yield* Effect.fail(new Error("Missing put result"));
        const rejected = yield* platform(() =>
          env.BUCKET.put(name, "wrong", {
            onlyIf: { etagMatches: "does-not-match" },
          }),
        );
        const accepted = yield* platform(() =>
          env.BUCKET.put(name, "second", { onlyIf: { etagMatches: put.etag } }),
        );
        const get = yield* platform(() => env.BUCKET.get(name));
        return {
          rejected: rejected === null,
          accepted: accepted !== null,
          value: get ? yield* platform(() => get.text()) : null,
        };
      }
      case "/r2/multipart": {
        const upload = yield* platform(() =>
          env.BUCKET.createMultipartUpload(name),
        );
        const part = yield* platform(() =>
          upload.uploadPart(1, "single final part"),
        );
        yield* platform(() => upload.complete([part]));
        const get = yield* platform(() => env.BUCKET.get(name));
        const aborted = yield* platform(() =>
          env.BUCKET.createMultipartUpload(name + "-abort"),
        );
        yield* platform(() => aborted.abort());
        return {
          part: part.partNumber,
          value: get ? yield* platform(() => get.text()) : null,
          aborted: yield* platform(() => env.BUCKET.head(name + "-abort")),
        };
      }
      case "/queues/retry-delay": {
        yield* platform(() =>
          env.QUEUE.send({
            namespace: name,
            key: "delayed",
            retry: true,
            stamp: true,
            delaySeconds: 3,
          }),
        );
        return { sent: true };
      }
      case "/queues/start": {
        yield* platform(() =>
          env.QUEUE.send({ namespace: name, key: "single", retry: true }),
        );
        yield* platform(() =>
          env.QUEUE.sendBatch([
            { body: { namespace: name, key: "batch-a", retry: false } },
            { body: { namespace: name, key: "batch-b", retry: false } },
          ]),
        );
        return { sent: true };
      }
      case "/queues/state": {
        const result = yield* platform(() =>
          env.PROBE.getByName(name).fetch(
            "https://fixture.test/queues/received",
          ),
        );
        return yield* platform(() => result.json());
      }
      case "/workflows/start": {
        const instance = yield* platform(() =>
          env.FLOW.create({
            id: name,
            params: {
              value: 20,
              mode: url.searchParams.get("mode") ?? "basic",
            },
          }),
        );
        return { sameId: instance.id === name };
      }
      case "/workflows/send": {
        const instance = yield* platform(() => env.FLOW.get(name));
        yield* platform(() =>
          instance.sendEvent({ type: "continue", payload: { value: "λ" } }),
        );
        return { sent: true };
      }
      case "/workflows/state": {
        const instance = yield* platform(() => env.FLOW.get(name));
        const state = yield* platform(() => instance.status());
        return { status: state.status, output: state.output ?? null };
      }
      default:
        return undefined;
    }
  });
