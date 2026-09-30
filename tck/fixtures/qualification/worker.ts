import {
  DurableObject,
  WorkerEntrypoint,
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
} from "cloudflare:workers";
import { Duration, Effect, Schema } from "effect";
import { durabilityOperation } from "./Durability.js";
import { platform, ready } from "../shared/Platform.js";
const blob = (id: number) => {
  const bytes = new Uint8Array(65536);
  let state = id + 1;
  for (let i = 0; i < bytes.length; i++) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    bytes[i] = state & 255;
  }
  return bytes;
};
const Write = Schema.Struct({ id: Schema.String, payload: Schema.String });
/**
 * The one outbound target the telemetry fixture will call. The driver supplies
 * the address because the fixture must not know the test topology, but the
 * fixture pins the scheme and the path so the route cannot be used as a proxy.
 */
const traceSink = (value: string | null) => {
  if (!value) return undefined;
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    return undefined;
  }
  return parsed.protocol === "http:" && parsed.pathname === "/trace/sink"
    ? parsed.toString()
    : undefined;
};
export class Recovery extends DurableObject<QualificationEnv> {
  private readonly activation = crypto.randomUUID();
  private durabilityCursor: ReturnType<SqlStorage["exec"]> | undefined;
  record(key: string) {
    return Effect.runPromise(
      Effect.sync(() => {
        const storage = this.ctx.storage;
        return storage.transactionSync(() => {
          const count = (storage.kv.get<number>(key) ?? 0) + 1;
          storage.kv.put(key, count);
          return count;
        });
      }),
    );
  }
  webSocketMessage(socket: WebSocket, message: string | ArrayBuffer) {
    return Effect.runPromise(
      Effect.sync(() => {
        const attachment = socket.deserializeAttachment() as {
          counter: number;
        };
        attachment.counter++;
        socket.serializeAttachment(attachment);
        socket.send(
          JSON.stringify({
            counter: attachment.counter,
            activation: this.activation,
            revision: "qualification-v1",
            message: String(message),
          }),
        );
      }),
    );
  }
  webSocketClose(socket: WebSocket, code: number, reason: string) {
    socket.close(code, reason);
  }
  // Observation only: the driver decides which deployment may run an alarm.
  alarm() {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const storage = this.ctx.storage;
        const holdMs = storage.kv.get<number>("alarm:hold") ?? 0;
        if (holdMs > 0) yield* Effect.sleep(Duration.millis(holdMs));
        storage.transactionSync(() => {
          storage.kv.put(
            "alarm:count",
            (storage.kv.get<number>("alarm:count") ?? 0) + 1,
          );
          storage.kv.put("alarm:revision", "qualification-v1");
          storage.kv.put("alarm:activation", this.activation);
          storage.kv.put("alarm:at", Date.now());
        });
      }),
    );
  }
  fetch(request: Request) {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const url = new URL(request.url);
        const storage = this.ctx.storage;
        const durability = yield* durabilityOperation(
          this.ctx,
          url,
          (cursor) => {
            this.durabilityCursor = cursor;
          },
          () =>
            this.env.PROBE.getByName(
              `${url.searchParams.get("name")}-witness`,
            ).fetch("https://fixture.test/durability/witness-write"),
        );
        if (durability) {
          void this.durabilityCursor;
          return durability;
        }
        if (url.pathname === "/ready") return ready();
        if (url.pathname === "/fleet/id")
          return Response.json({
            cell: this.ctx.id.toString(),
            activation: this.activation,
            revision: "qualification-v1",
          });
        if (url.pathname === "/socket") {
          const [client, server] = Object.values(new WebSocketPair());
          this.ctx.acceptWebSocket(server!);
          server!.serializeAttachment({ counter: 0 });
          return new Response(null, { status: 101, webSocket: client! });
        }
        // A regular (non-hibernatable) socket: the documented safe-point blocker.
        if (url.pathname === "/socket/regular") {
          const [client, server] = Object.values(new WebSocketPair());
          server!.accept();
          let counter = 0;
          server!.addEventListener("message", (event) => {
            counter++;
            server!.send(
              JSON.stringify({
                counter,
                activation: this.activation,
                revision: "qualification-v1",
                message: String(event.data),
              }),
            );
          });
          return new Response(null, { status: 101, webSocket: client! });
        }
        // Holds one request open in the object for a bounded time.
        if (url.pathname === "/hold") {
          const ms = Number(url.searchParams.get("ms") ?? 0);
          if (!Number.isInteger(ms) || ms < 0 || ms > 120000)
            return new Response("invalid hold", { status: 400 });
          const started = Date.now();
          yield* Effect.sleep(Duration.millis(ms));
          return Response.json({
            revision: "qualification-v1",
            activation: this.activation,
            requestedMs: ms,
            heldMs: Date.now() - started,
          });
        }
        if (url.pathname === "/alarm/arm") {
          const ms = Number(url.searchParams.get("ms") ?? 0);
          const hold = Number(url.searchParams.get("hold") ?? 0);
          if (
            !Number.isInteger(ms) ||
            ms < 0 ||
            ms > 120000 ||
            !Number.isInteger(hold) ||
            hold < 0 ||
            hold > 60000
          )
            return new Response("invalid alarm", { status: 400 });
          storage.kv.put("alarm:hold", hold);
          const at = Date.now() + ms;
          yield* platform(() => storage.setAlarm(at));
          return Response.json({
            armed: true,
            at,
            revision: "qualification-v1",
          });
        }
        if (url.pathname === "/alarm/state")
          return Response.json({
            count: storage.kv.get<number>("alarm:count") ?? 0,
            revision: storage.kv.get<string>("alarm:revision") ?? null,
            activation: storage.kv.get<string>("alarm:activation") ?? null,
            at: storage.kv.get<number>("alarm:at") ?? null,
            pending: yield* platform(() => storage.getAlarm()),
          });
        if (url.pathname.startsWith("/history/") || url.pathname === "/stream")
          storage.sql.exec(
            "CREATE TABLE IF NOT EXISTS history (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL, payload TEXT NOT NULL)",
          );
        if (url.pathname === "/history/write") {
          const input = yield* platform(() => request.json()).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Write)),
          );
          if (input.payload.length > 4096 || input.id.length > 128)
            return new Response("too large", { status: 400 });
          // Holds the request open inside the owning object before it commits,
          // so a graceful drain can be started while this write is in flight.
          const delayMs = Number(url.searchParams.get("delay") ?? 0);
          if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > 30000)
            return new Response("invalid delay", { status: 400 });
          if (delayMs > 0) {
            // Observation only: records that the object accepted this request
            // before it began waiting, so a drain can be started afterwards
            // with evidence rather than with a guess about timing.
            storage.kv.put(`event:accepted:${input.id}`, Date.now());
            yield* Effect.sleep(Duration.millis(delayMs));
          }
          const result = storage.transactionSync(() => {
            storage.sql
              .exec(
                "INSERT INTO history(id,payload) VALUES (?,?)",
                input.id,
                input.payload,
              )
              .toArray();
            storage.kv.put(`history:${input.id}`, input.payload);
            return storage.sql
              .exec<{
                seq: number;
                id: string;
                payload: string;
              }>("SELECT seq,id,payload FROM history WHERE id=?", input.id)
              .one();
          });
          return Response.json({ ...result, activation: this.activation });
        }
        if (url.pathname === "/history/kv")
          return Response.json([...storage.kv.list({ prefix: "history:" })]);
        if (url.pathname === "/history/state") {
          const after = Number(url.searchParams.get("after") ?? 0);
          const rows = storage.sql
            .exec<{
              seq: number;
              id: string;
              payload: string;
            }>(
              "SELECT seq,id,payload FROM history WHERE seq>? ORDER BY seq LIMIT 128",
              after,
            )
            .toArray();
          return Response.json(
            rows.map((row) => ({
              ...row,
              kv: storage.kv.get(`history:${row.id}`) ?? null,
            })),
          );
        }
        if (url.pathname === "/stream") {
          const after = Number(url.searchParams.get("after") ?? 0);
          const rows = storage.sql
            .exec<{
              seq: number;
              id: string;
              payload: string;
            }>(
              "SELECT seq,id,payload FROM history WHERE seq>? ORDER BY seq",
              after,
            )
            .toArray();
          let index = 0;
          const paddingBytes = Number(url.searchParams.get("padding") ?? 0);
          if (![0, 65536].includes(paddingBytes))
            return new Response("invalid padding", { status: 400 });
          const padding = "x".repeat(paddingBytes);
          return new Response(
            new ReadableStream({
              pull(controller) {
                return Effect.runPromise(
                  Effect.gen(function* () {
                    const row = rows[index++];
                    if (!row) {
                      controller.close();
                      return;
                    }
                    yield* Effect.sleep("10 millis");
                    controller.enqueue(
                      new TextEncoder().encode(
                        JSON.stringify({ ...row, padding }) + "\n",
                      ),
                    );
                  }),
                );
              },
            }),
            { headers: { "content-type": "application/x-ndjson" } },
          );
        }
        if (url.pathname === "/blob/write") {
          const id = Number(url.searchParams.get("id"));
          const bytes = blob(id);
          storage.kv.put(`blob:${id}`, bytes);
          return Response.json({ id, bytes: bytes.length });
        }
        if (url.pathname === "/blob/check") {
          const count = Number(url.searchParams.get("count"));
          let bytes = 0;
          for (let id = 0; id < count; id++) {
            const value = storage.kv.get<Uint8Array>(`blob:${id}`);
            const expected = blob(id);
            if (
              !value ||
              value.length !== 65536 ||
              value.some((byte, index) => byte !== expected[index])
            )
              return new Response(`corrupt ${id}`, { status: 500 });
            bytes += value.length;
          }
          return Response.json({ count, bytes });
        }
        // Observation only. Emits two log lines around an await so the driver
        // can check that celld keeps the handler's trace context across
        // asynchronous work, then makes one outbound call and reports the
        // `traceparent` the downstream actually received.
        if (url.pathname === "/trace/chain") {
          const marker = url.searchParams.get("marker") ?? "";
          console.log(`${marker}:cell`);
          yield* Effect.sleep("25 millis");
          console.log(`${marker}:cell-await`);
          const sink = traceSink(url.searchParams.get("sink"));
          if (!sink) return new Response("invalid sink", { status: 400 });
          const response = yield* platform(() => fetch(sink));
          const echoed = yield* platform(() => response.json());
          return Response.json({ downstream: echoed, status: response.status });
        }
        if (url.pathname === "/events")
          return Response.json([...storage.kv.list({ prefix: "event:" })]);
        return new Response("not found", { status: 404 });
      }),
    );
  }
}
export class ServiceProbe extends WorkerEntrypoint<QualificationEnv> {
  echo(value: string) {
    return Effect.runPromise(
      Effect.succeed({ value, revision: "qualification-v1" }),
    );
  }
}
export class RecoveryWorkflow extends WorkflowEntrypoint<
  QualificationEnv,
  { name: string }
> {
  run(event: WorkflowEvent<{ name: string }>, step: WorkflowStep) {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const first = yield* platform(() =>
          step.do("first", () =>
            this.env.PROBE.getByName(event.payload.name).record(
              "event:flow-first",
            ),
          ),
        );
        yield* platform(() =>
          step.waitForEvent("continue", {
            type: "continue",
            timeout: "10 minutes",
          }),
        );
        const last = yield* platform(() =>
          step.do("last", () =>
            this.env.PROBE.getByName(event.payload.name).record(
              "event:flow-last",
            ),
          ),
        );
        return { first, last };
      }),
    );
  }
}
const Message = Schema.Struct({
  name: Schema.String,
  id: Schema.String,
  retry: Schema.Boolean,
});
export default {
  fetch(request: Request, env: QualificationEnv) {
    return Effect.runPromise(
      Effect.gen(function* () {
        const url = new URL(request.url);
        if (url.pathname === "/deployment/revision")
          return Response.json({ revision: "qualification-v1" });
        // Observation only: reports how the runtime built request.url so a
        // forwarded-header policy can be read without the fixture trusting it.
        if (url.pathname === "/echo/url")
          return Response.json({
            url: request.url,
            host: request.headers.get("host"),
          });
        // Worker-local, observation only: reports the trace context celld
        // propagated onto an outbound call, without interpreting it.
        if (url.pathname === "/trace/sink")
          return Response.json({
            traceparent: request.headers.get("traceparent"),
          });
        // Worker-local: emits one log line so the driver can resolve which
        // trace this request ran in. It never reads the incoming header, so
        // the observation stays independent of what is being asserted.
        if (url.pathname === "/trace/emit") {
          console.log(url.searchParams.get("marker") ?? "");
          return Response.json({ emitted: true });
        }
        if (url.pathname === "/trace/chain")
          console.log(`${url.searchParams.get("marker") ?? ""}:worker`);
        // Worker-local: a Durable Object request may execute on another node.
        if (url.pathname === "/pressure") {
          const mb = Number(url.searchParams.get("mb") ?? 16);
          if (!Number.isInteger(mb) || mb < 1 || mb > 256)
            return new Response("invalid size", { status: 400 });
          const buffers = Array.from({ length: mb }, (_, i) =>
            new Uint8Array(1024 * 1024).fill(i % 251),
          );
          yield* Effect.sleep("2 seconds");
          return Response.json({
            mb,
            checksum: buffers.reduce((sum, item) => sum + item[0]!, 0),
          });
        }
        const name = url.searchParams.get("name") ?? "ready";
        if (url.pathname === "/queue/send") {
          const count = Number(url.searchParams.get("count") ?? 1);
          const delaySeconds = Number(url.searchParams.get("delay") ?? 0);
          const offset = Number(url.searchParams.get("offset") ?? 0);
          if (!Number.isInteger(count) || count < 1 || count > 100)
            return new Response("invalid count", { status: 400 });
          yield* platform(() =>
            env.QUEUE.sendBatch(
              Array.from({ length: count }, (_, i) => ({
                body: {
                  name,
                  id: String(i + offset),
                  retry: url.searchParams.get("retry") === "1",
                },
                delaySeconds,
              })),
            ),
          );
          return Response.json({ accepted: count });
        }
        if (url.pathname === "/flow/start") {
          const instance = yield* platform(() =>
            env.FLOW.create({ id: name, params: { name } }),
          );
          return Response.json({ id: instance.id });
        }
        if (url.pathname === "/flow/continue") {
          const instance = yield* platform(() => env.FLOW.get(name));
          yield* platform(() =>
            instance.sendEvent({ type: "continue", payload: { value: 1 } }),
          );
          return Response.json({ sent: true });
        }
        if (url.pathname === "/flow/status")
          return Response.json(
            yield* platform(() =>
              env.FLOW.get(name).then((instance) => instance.status()),
            ),
          );
        if (url.pathname === "/service")
          return Response.json(yield* platform(() => env.SERVICE.echo(name)));
        return yield* platform(() =>
          env.PROBE.getByName(name).fetch(request),
        ).pipe(
          Effect.catch((error) =>
            url.pathname.startsWith("/durability/")
              ? Effect.succeed(
                  Response.json(
                    {
                      rejected: true,
                      message:
                        error instanceof Error ? error.message : String(error),
                    },
                    { status: 500 },
                  ),
                )
              : Effect.fail(error),
          ),
        );
      }),
    );
  },
  queue(batch: MessageBatch<unknown>, env: QualificationEnv) {
    return Effect.runPromise(
      Effect.gen(function* () {
        for (const message of batch.messages) {
          const body = yield* Schema.decodeUnknownEffect(Message)(message.body);
          const attempt = yield* platform(() =>
            env.PROBE.getByName(body.name).record(`event:queue:${body.id}`),
          );
          if (body.retry && attempt === 1) message.retry({ delaySeconds: 15 });
          else message.ack();
        }
      }),
    );
  },
} satisfies ExportedHandler<QualificationEnv>;
