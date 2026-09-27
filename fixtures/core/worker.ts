import { DurableObject, RpcTarget, WorkerEntrypoint } from "cloudflare:workers";
import { Effect, Schema } from "effect";

import { services, consume } from "./Services.js";
import { boundaries } from "./Boundaries.js";
export { TestWorkflow } from "./Services.js";
import { platform, operation, rejection } from "./Platform.js";
import { web } from "./Web.js";
import { storageOperation } from "./Storage.js";
import { encode, richValue } from "../shared/Codec.js";

export class Calculator extends RpcTarget {
  add(a: number, b: number) {
    return Effect.runSync(Effect.succeed(a + b));
  }
}
export class Service extends WorkerEntrypoint<Env> {
  greet(name: string) {
    return Effect.runSync(Effect.succeed(`hello ${name}`));
  }
}

export class Probe extends DurableObject<Env> {
  private initialized = false;
  private openSockets = 0;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(() =>
      Effect.runPromise(
        Effect.gen(function* () {
          yield* platform(() => ctx.storage.get("constructor-check"));
        }).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              this.initialized = true;
            }),
          ),
        ),
      ),
    );
  }
  workflowAttempt() {
    return Effect.runPromise(
      Effect.sync(() =>
        this.ctx.storage.transactionSync(() => {
          const next =
            (this.ctx.storage.kv.get<number>("workflow-attempt") ?? 0) + 1;
          this.ctx.storage.kv.put("workflow-attempt", next);
          return next;
        }),
      ),
    );
  }
  echo(value: unknown) {
    return Effect.runSync(Effect.succeed(value));
  }
  fail() {
    return Effect.runPromise(Effect.fail(new TypeError("tck rpc error")));
  }
  calculator() {
    return Effect.runSync(Effect.sync(() => new Calculator()));
  }
  alarm(info: AlarmInvocationInfo): Promise<void> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        if (this.ctx.storage.kv.get("rearm")) {
          const fires =
            (this.ctx.storage.kv.get<number>("rearm-fires") ?? 0) + 1;
          this.ctx.storage.kv.put("rearm-fires", fires);
          if (fires === 1) {
            yield* platform(() => this.ctx.storage.setAlarm(Date.now() + 100));
            this.ctx.waitUntil(
              Effect.runPromise(
                Effect.sleep("5 seconds").pipe(
                  Effect.andThen(
                    Effect.sync(() =>
                      this.ctx.storage.kv.put("timer-finished", true),
                    ),
                  ),
                ),
              ),
            );
          } else
            this.ctx.storage.kv.put(
              "before-timer",
              !this.ctx.storage.kv.get("timer-finished"),
            );
          return;
        }
        const count =
          (yield* platform(() => this.ctx.storage.get<number>("fires"))) ?? 0;
        yield* platform(() =>
          this.ctx.storage.put({
            fires: count + 1,
            retry: info.isRetry,
            retryCount: info.retryCount,
          }),
        );
        if (
          (yield* platform(() => this.ctx.storage.get("retry-once"))) &&
          count === 0
        )
          return yield* Effect.fail(new Error("retry this alarm"));
      }),
    );
  }
  webSocketMessage(socket: WebSocket, message: string | ArrayBuffer) {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        if (message === "stream") {
          socket.send("first");
          yield* Effect.sleep("500 millis");
          socket.send("second");
          yield* Effect.sleep("500 millis");
          socket.send("finished");
        } else if (message === "attachment")
          socket.send(JSON.stringify(socket.deserializeAttachment()));
        else if (message === "close") socket.close(1000, "done");
        else socket.send(message);
      }),
    );
  }
  webSocketClose(
    socket: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean,
  ) {
    return Effect.runPromise(
      Effect.sync(() => {
        this.ctx.storage.kv.put("last-close", { code, reason, wasClean });
        socket.close(code, reason);
      }),
    );
  }
  fetch(request: Request): Promise<Response> {
    return Effect.runPromise(this.handle(request));
  }
  private handle(request: Request) {
    const storage = this.ctx.storage;
    // oxlint-disable-next-line typescript/no-this-alias -- Capture the Durable Object for the generator below.
    const self = this;
    return Effect.gen(function* () {
      const url = new URL(request.url);
      const key = url.searchParams.get("key") ?? "value";
      const extended = yield* storageOperation(storage, url.pathname);
      if (extended !== undefined) return Response.json(extended);
      switch (url.pathname) {
        case "/queues/received":
          return Response.json(
            Object.fromEntries(yield* platform(() => storage.list())),
          );
        case "/context/background-start": {
          self.ctx.waitUntil(
            Effect.runPromise(
              Effect.gen(function* () {
                yield* platform(
                  () => new Promise<void>((resolve) => setTimeout(resolve, 20)),
                );
                yield* platform(() => storage.put("background", "complete"));
              }),
            ),
          );
          return Response.json({ scheduled: true });
        }
        case "/identity":
          return Response.json({
            id: self.ctx.id.toString(),
            initialized: self.initialized,
          });
        case "/concurrency/increment": {
          const before =
            (yield* platform(() => storage.get<number>("counter"))) ?? 0;
          yield* platform(() => storage.put("counter", before + 1));
          return Response.json({
            before,
            after: before + 1,
            initialized: self.initialized,
          });
        }
        case "/concurrency/block": {
          return Response.json(
            yield* platform(() =>
              self.ctx.blockConcurrencyWhile(() =>
                Effect.runPromise(
                  Effect.gen(function* () {
                    const before =
                      (yield* platform(() => storage.get<number>("counter"))) ??
                      0;
                    yield* platform(
                      () =>
                        new Promise<void>((resolve) => setTimeout(resolve, 5)),
                    );
                    yield* platform(() => storage.put("counter", before + 1));
                    return {
                      before,
                      after: before + 1,
                      initialized: self.initialized,
                    };
                  }),
                ),
              ),
            ),
          );
        }
        case "/headers/unicode":
          return new Response("ok", { headers: { "x-unicode": "λ🌍, 雪" } });
        case "/alarms/rearm-start":
          storage.kv.put("rearm", true);
          yield* platform(() => storage.setAlarm(Date.now() + 100));
          return Response.json({ scheduled: true });
        case "/alarms/rearm-state":
          return Response.json({
            fires: storage.kv.get("rearm-fires") ?? 0,
            beforeTimer: storage.kv.get("before-timer") ?? null,
          });
        case "/websocket/close-state":
          return Response.json(storage.kv.get("last-close") ?? null);
        case "/alarms/manage": {
          const first = Date.now() + 3600000;
          yield* platform(() => storage.setAlarm(first));
          const set = (yield* platform(() => storage.getAlarm())) === first;
          yield* platform(() => storage.setAlarm(first + 1000));
          const replaced =
            (yield* platform(() => storage.getAlarm())) === first + 1000;
          yield* platform(() => storage.deleteAlarm());
          return Response.json({
            set,
            replaced,
            deleted: yield* platform(() => storage.getAlarm()),
          });
        }
        case "/alarms/start": {
          yield* platform(() =>
            storage.put("retry-once", url.searchParams.has("retry")),
          );
          yield* platform(() => storage.setAlarm(Date.now() + 100));
          return Response.json({ scheduled: true });
        }
        case "/alarms/state":
          return Response.json({
            fires: (yield* platform(() => storage.get("fires"))) ?? 0,
            retry: (yield* platform(() => storage.get("retry"))) ?? false,
            retryCount: (yield* platform(() => storage.get("retryCount"))) ?? 0,
          });
        case "/websocket/echo": {
          // Non-hibernatable peer for a Worker-initiated Upgrade: the object
          // counts live sockets and owns every close it initiates.
          const pair = new WebSocketPair();
          const server = pair[1];
          server.accept();
          self.openSockets++;
          server.addEventListener("message", (event) =>
            Effect.runSync(
              Effect.sync(() => {
                if (event.data === "close") server.close(4001, "server λ");
                else server.send(`echo:${String(event.data)}`);
              }),
            ),
          );
          server.addEventListener("close", () =>
            Effect.runSync(
              Effect.sync(() => {
                self.openSockets--;
              }),
            ),
          );
          return new Response(null, { status: 101, webSocket: pair[0] });
        }
        case "/websocket/count":
          return Response.json({ open: self.openSockets });
        case "/websocket": {
          const pair = new WebSocketPair();
          pair[1].serializeAttachment({ label: "durable", count: 7 });
          self.ctx.acceptWebSocket(pair[1], ["test"]);
          return new Response(null, { status: 101, webSocket: pair[0] });
        }

        case "/ready": {
          yield* platform(() => storage.put("ready", "ok"));
          return Response.json({
            value: yield* platform(() => storage.get("ready")),
          });
        }
        case "/storage/put": {
          const body = yield* platform(() => request.json());
          yield* platform(() => storage.put(key, body));
          return Response.json({ stored: true });
        }
        case "/storage/get": {
          const value = yield* platform(() => storage.get(key));
          return Response.json(
            value === undefined ? { present: false } : { present: true, value },
          );
        }
        case "/storage/list":
          return Response.json([
            ...(yield* platform(() => storage.list({ prefix: "item:" }))),
          ]);
        case "/storage/delete":
          return Response.json({
            deleted: yield* platform(() => storage.delete(key)),
          });
        case "/storage/rollback": {
          // This platform callback requires a Promise; the transaction itself is Effect-based.
          const error = yield* platform(() =>
            storage.transaction((tx) =>
              Effect.runPromise(
                Effect.gen(function* () {
                  yield* platform(() => tx.put("balance", 999));
                  yield* platform(() => tx.put("uncommitted", true));
                  return yield* Effect.fail(
                    new Error("tck intentional rollback"),
                  );
                }),
              ),
            ),
          ).pipe(
            Effect.as(null as string | null),
            Effect.catch((cause) =>
              Effect.succeed(
                cause instanceof Error ? cause.message : String(cause),
              ),
            ),
          );
          return Response.json({ error });
        }
        case "/sql/prepare":
          yield* operation(() => {
            storage.sql.exec(
              "CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            );
            storage.sql.exec(
              "INSERT INTO ledger VALUES (?, ?)",
              1,
              "committed",
            );
          });
          return Response.json({ prepared: true });
        case "/sql/rollback": {
          // SQLite's synchronous callback cannot yield or run Promise-based effects.
          const error = yield* operation(() =>
            storage.transactionSync(() => {
              storage.sql.exec(
                "UPDATE ledger SET value = ? WHERE id = ?",
                "overwritten",
                1,
              );
              storage.sql.exec(
                "INSERT INTO ledger VALUES (?, ?)",
                2,
                "uncommitted",
              );
              // oxlint-disable-next-line effect/throw-in-effect-gen -- The synchronous transaction must throw to roll back; operation captures the error.
              throw new Error("tck intentional rollback");
            }),
          ).pipe(
            Effect.as(null as string | null),
            Effect.catch((cause) =>
              Effect.succeed(
                cause instanceof Error ? cause.message : String(cause),
              ),
            ),
          );
          return Response.json({ error });
        }
        case "/sql/read":
          return Response.json(
            yield* operation(() =>
              storage.sql
                .exec("SELECT id, value FROM ledger ORDER BY id")
                .toArray(),
            ),
          );
        default:
          return new Response("Unknown operation", { status: 404 });
      }
    });
  }
}

export default {
  queue: consume,
  fetch(request: Request, env: Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        const url = new URL(request.url);
        if (url.pathname === "/http/echo") {
          return Response.json(
            {
              method: request.method,
              query: url.searchParams.getAll("q"),
              header: request.headers.get("x-tck-value"),
              body: yield* platform(() => request.text()),
            },
            { status: 201, headers: { "x-tck-response": "echo" } },
          );
        }
        const webResult = yield* web(request);
        if (webResult !== undefined) return Response.json(webResult);
        if (url.pathname === "/streams/response") {
          return new Response(
            new ReadableStream<Uint8Array>({
              start(c) {
                for (const bytes of [
                  [0, 1, 128],
                  [255, 10],
                ])
                  c.enqueue(new Uint8Array(bytes));
                c.close();
              },
            }),
            { headers: { "content-type": "application/octet-stream" } },
          );
        }
        const name = yield* Schema.decodeUnknownEffect(Schema.String)(
          url.searchParams.get("name"),
        );
        if (name.length === 0 || name.length > 200)
          return new Response("Invalid object name", { status: 400 });
        const serviceResult = yield* services(request, env, name);
        if (serviceResult !== undefined) return Response.json(serviceResult);
        const boundaryResult = yield* boundaries(request, env, name);
        if (boundaryResult !== undefined) return Response.json(boundaryResult);
        const stub = env.PROBE.getByName(name);
        if (url.pathname === "/rpc/echo")
          return Response.json(
            encode(yield* platform(() => stub.echo(richValue()))),
          );
        if (url.pathname === "/rpc/error")
          return Response.json({
            error: yield* rejection(platform(() => stub.fail())),
          });
        if (url.pathname === "/rpc/target") {
          return yield* Effect.gen(function* () {
            const target = yield* platform(() => stub.calculator());
            const sum = yield* platform(() => target.add(20, 22)).pipe(
              Effect.ensuring(Effect.sync(() => target[Symbol.dispose]())),
            );
            return Response.json({ sum });
          }).pipe(
            Effect.catch((cause) =>
              Effect.succeed(
                Response.json({
                  error:
                    cause instanceof Error
                      ? { name: cause.name, message: cause.message }
                      : { name: "Unknown", message: String(cause) },
                }),
              ),
            ),
          );
        }
        if (url.pathname === "/identity/namespace") {
          const a = env.PROBE.idFromName(name),
            b = env.PROBE.idFromName(name),
            c = env.PROBE.idFromName(name + "other");
          return Response.json({
            same: a.equals(b),
            different: !a.equals(c),
            roundTrip: env.PROBE.idFromString(a.toString()).equals(a),
            unique: !env.PROBE.newUniqueId().equals(env.PROBE.newUniqueId()),
            invalid: yield* rejection(
              operation(() => env.PROBE.idFromString("invalid")),
            ),
          });
        }
        return yield* platform(() => stub.fetch(request));
      }),
    );
  },
} satisfies ExportedHandler<Env>;
