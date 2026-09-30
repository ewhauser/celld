import { Effect } from "effect";
import { operation, platform, rejection } from "./Platform.js";

type SocketEvent =
  | { readonly type: "message"; readonly data: string }
  | {
      readonly type: "close";
      readonly code: number;
      readonly reason: string;
      readonly clean: boolean;
    };

/** One bounded wait for the next frame or close on an owned outbound socket. */
const nextEvent = (socket: WebSocket) =>
  Effect.callback<SocketEvent, unknown>((resume) => {
    const onMessage = (event: MessageEvent) =>
      resume(
        Effect.succeed({ type: "message" as const, data: String(event.data) }),
      );
    const onClose = (event: CloseEvent) =>
      resume(
        Effect.succeed({
          type: "close" as const,
          code: event.code,
          reason: event.reason,
          clean: event.wasClean,
        }),
      );
    const onError = () =>
      resume(Effect.fail(new Error("Outbound WebSocket error")));
    socket.addEventListener("message", onMessage);
    socket.addEventListener("close", onClose);
    socket.addEventListener("error", onError);
    return Effect.sync(() => {
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
      socket.removeEventListener("error", onError);
    });
  }).pipe(Effect.timeout("10 seconds"));

/** A Worker-initiated Upgrade to the Durable Object's own echo endpoint. */
const openSocket = (stub: DurableObjectStub, path: string) =>
  Effect.gen(function* () {
    const response = yield* platform(() =>
      stub.fetch(`https://fixture.test${path}`, {
        headers: { Upgrade: "websocket" },
      }),
    );
    if (response.status !== 101 || !response.webSocket)
      return yield* Effect.fail(
        new Error(`Upgrade refused with status ${response.status}`),
      );
    const socket = response.webSocket;
    socket.accept();
    return socket;
  });

const exchange = (socket: WebSocket, message: string) =>
  Effect.gen(function* () {
    yield* Effect.sync(() => socket.send(message));
    return yield* nextEvent(socket);
  });

export const boundaries = (request: Request, env: Env, name: string) =>
  Effect.gen(function* () {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/web/unicode-headers": {
        const headers = new Headers({ "x-unicode": "λ🌍" });
        headers.append("x-unicode", "雪");
        const response = new Response("ok", { headers });
        const fetched = yield* platform(() =>
          env.PROBE.getByName(name).fetch(
            "https://fixture.test/headers/unicode",
          ),
        );
        yield* platform(() => fetched.text());
        return {
          constructed: headers.get("x-unicode"),
          response: response.headers.get("x-unicode"),
          fetched: fetched.headers.get("x-unicode"),
        };
      }
      case "/websocket/peer-close": {
        const socket = yield* openSocket(
          env.PROBE.getByName(name),
          "/websocket",
        );
        socket.close(4002, "peer λ");
        return { sent: true };
      }
      case "/kv/expiration": {
        const ttl = 600;
        const before = Math.floor(Date.now() / 1000);
        yield* platform(() =>
          env.KV.put(name + "/ttl", "λ", {
            expirationTtl: ttl,
            metadata: { tag: 1 },
          }),
        );
        yield* platform(() => env.KV.put(name + "/keep", "keep"));
        const list = yield* platform(() => env.KV.list({ prefix: name + "/" }));
        const expirations = new Map(
          list.keys.map((key) => [
            key.name.slice(name.length),
            key.expiration ?? null,
          ]),
        );
        const expiring = expirations.get("/ttl") ?? null;
        const stored = yield* platform(() =>
          env.KV.getWithMetadata(name + "/ttl"),
        );
        // A published minimum: expirationTtl below 60 seconds is rejected.
        const tooShort = yield* rejection(
          platform(() =>
            env.KV.put(name + "/short", "x", { expirationTtl: 30 }),
          ),
        );
        for (const suffix of ["/ttl", "/keep", "/short"])
          yield* platform(() => env.KV.delete(name + suffix));
        return {
          value: stored.value,
          metadata: stored.metadata,
          names: [...expirations.keys()].sort(),
          keepExpiration: expirations.get("/keep") ?? null,
          expiringInWindow:
            typeof expiring === "number" &&
            expiring >= before + ttl - 5 &&
            expiring <= before + ttl + 5,
          tooShort,
        };
      }
      case "/d1/exec-batch": {
        const exec = yield* platform(() =>
          env.DB.exec(
            "CREATE TABLE IF NOT EXISTS exec_table (namespace TEXT, n INTEGER);\nCREATE INDEX IF NOT EXISTS exec_index ON exec_table (namespace);",
          ),
        );
        const insert = env.DB.prepare("INSERT INTO exec_table VALUES (?, ?)");
        const results = yield* platform(() =>
          env.DB.batch<{ n: number }>([
            insert.bind(name, 1),
            insert.bind(name, 2),
            env.DB.prepare(
              "SELECT n FROM exec_table WHERE namespace = ? ORDER BY n",
            ).bind(name),
          ]),
        );
        return {
          execCount: exec.count,
          success: results.map((result) => result.success),
          results: results.map((result) => result.results),
          changes: results.map((result) => result.meta.changes),
        };
      }
      case "/r2/list-options": {
        const body = new Uint8Array([1, 2, 3]);
        for (const suffix of ["/dir/a", "/dir/b", "/top"])
          yield* platform(() =>
            env.BUCKET.put(name + suffix, body, {
              httpMetadata: {
                contentType: "text/plain; charset=utf-8",
                // HTTP metadata stays ASCII: it is echoed into response
                // headers, and the non-ASCII case belongs in customMetadata.
                contentDisposition: 'attachment; filename="report.txt"',
                cacheControl: "max-age=42",
              },
              customMetadata: { label: "λ" },
            }),
          );
        const top = yield* platform(() =>
          env.BUCKET.list({ prefix: name + "/", delimiter: "/" }),
        );
        const nested = yield* platform(() =>
          env.BUCKET.list({ prefix: name + "/dir/", delimiter: "/" }),
        );
        const detailed = yield* platform(() =>
          env.BUCKET.list({
            prefix: name + "/top",
            include: ["httpMetadata", "customMetadata"],
          }),
        );
        const object = yield* platform(() => env.BUCKET.get(name + "/top"));
        if (!object) return yield* Effect.fail(new Error("Missing R2 object"));
        const headers = new Headers();
        object.writeHttpMetadata(headers);
        for (const suffix of ["/dir/a", "/dir/b", "/top"])
          yield* platform(() => env.BUCKET.delete(name + suffix));
        return {
          objects: top.objects.map((o) => o.key.slice(name.length)),
          prefixes: top.delimitedPrefixes.map((p) => p.slice(name.length)),
          truncated: top.truncated,
          nested: nested.objects.map((o) => o.key.slice(name.length)),
          nestedPrefixes: nested.delimitedPrefixes,
          bare: {
            http: top.objects[0]?.httpMetadata ?? null,
            custom: top.objects[0]?.customMetadata ?? null,
          },
          included: {
            http: detailed.objects[0]?.httpMetadata?.contentType ?? null,
            custom: detailed.objects[0]?.customMetadata ?? null,
          },
          written: {
            type: headers.get("content-type"),
            disposition: headers.get("content-disposition"),
            cache: headers.get("cache-control"),
          },
        };
      }
      case "/websocket/outbound": {
        const stub = env.PROBE.getByName(name);
        const socket = yield* openSocket(stub, "/websocket/echo");
        const echo = yield* exchange(socket, "ping λ");
        const closed = yield* exchange(socket, "close");
        const readyState = socket.readyState;
        // A second socket proves the invalid-code rejection on a live socket.
        const other = yield* openSocket(stub, "/websocket/echo");
        const invalidCode = yield* rejection(
          operation(() => other.close(1005, "reserved")),
        );
        yield* Effect.sync(() => other.close(1000, "done"));
        return { echo, closed, readyState, invalidCode };
      }
      case "/websocket/concurrent": {
        const stub = env.PROBE.getByName(name);
        const count = 16;
        const sockets = yield* Effect.forEach(
          Array.from({ length: count }, (_, i) => i),
          () => openSocket(stub, "/websocket/echo"),
          { concurrency: "unbounded" },
        );
        const echoes = yield* Effect.forEach(
          sockets,
          (socket, index) => exchange(socket, `n${index}`),
          { concurrency: "unbounded" },
        );
        const counted = yield* platform(() =>
          stub.fetch("https://fixture.test/websocket/count"),
        );
        const open = yield* platform(() => counted.json<{ open: number }>());
        for (const socket of sockets)
          yield* Effect.sync(() => socket.close(1000, "done"));
        return {
          echoes: echoes
            .map((event) =>
              event.type === "message" ? event.data : event.type,
            )
            .sort(),
          open: open.open,
        };
      }
      case "/flags/report": {
        const stub = env.PROBE.getByName(name);
        const socket = yield* openSocket(stub, "/websocket/echo");
        const binaryType = socket.binaryType ?? null;
        yield* Effect.sync(() => socket.close(1000, "done"));
        const response = yield* platform(() =>
          stub.fetch("https://fixture.test/flags/delete-all-alarm"),
        );
        return {
          binaryType,
          deleteAll: yield* platform(() => response.json()),
        };
      }
      default:
        return undefined;
    }
  });
