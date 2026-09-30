// Local fault-injection sidecar. The control listener is published only on loopback.
import { Effect, Schema } from "effect";
// oxlint-disable-next-line effect/use-http-client-service -- Fault injection needs direct control of Node sockets and response streams.
import { createServer, request as upstreamRequest } from "node:http";
const Control = Schema.Struct({
  mode: Schema.Literals([
    "normal",
    "latency",
    "throttle",
    "timeout",
    "drop-response",
  ]),
  durationMs: Schema.Int,
  pathContains: Schema.optional(Schema.String),
});
export const acquireStorageProxy = (
  config = {
    upstreamHostname: "minio",
    upstreamPort: 9000,
    dataPort: 8082,
    controlPort: 9091,
  },
) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const run = <A, E>(work: Effect.Effect<A, E>) =>
      // oxlint-disable-next-line effect/effect-run-in-body -- Bridge Node HTTP callbacks into the owning scope.
      Effect.runFork(work.pipe(Effect.forkIn(scope)));
    let control: typeof Control.Type = { mode: "normal", durationMs: 0 };
    let until = 0;
    const events: Array<{
      mode: string;
      method: string;
      path: string;
      upstreamStatus?: number;
      dropped?: boolean;
    }> = [];

    const data = createServer((request, response) => {
      run(
        Effect.gen(function* () {
          const mode =
            Date.now() < until &&
            (control.pathContains === undefined ||
              (request.url ?? "").includes(control.pathContains))
              ? control.mode
              : "normal";
          const event: (typeof events)[number] = {
            mode,
            method: request.method ?? "GET",
            path: request.url ?? "/",
          };
          if (events.length >= 2048) events.shift();
          events.push(event);
          if (mode === "throttle") {
            response.writeHead(503, {
              "content-type": "application/xml",
              "retry-after": "1",
            });
            response.end(
              "<Error><Code>SlowDown</Code><Message>Injected throttling</Message></Error>",
            );
            request.resume();
            return;
          }
          if (mode === "timeout") {
            request.resume();
            yield* Effect.sleep("12 seconds");
            response.destroy();
            return;
          }
          if (mode === "latency") yield* Effect.sleep("750 millis");
          yield* Effect.callback<void>((resume) => {
            const upstream = upstreamRequest(
              {
                hostname: config.upstreamHostname,
                port: config.upstreamPort,
                path: request.url,
                method: request.method,
                // MinIO can close a connection after rejecting a conditional PUT.
                // Never reuse that socket or retry an ambiguously completed write.
                agent: false,
                headers: { ...request.headers, connection: "close" },
              },
              (incoming) => {
                event.upstreamStatus = incoming.statusCode ?? 0;
                if (
                  mode === "drop-response" &&
                  ["PUT", "POST", "DELETE"].includes(request.method ?? "")
                ) {
                  incoming.resume();
                  incoming.on("end", () => {
                    event.dropped = true;
                    response.destroy();
                    resume(Effect.void);
                  });
                } else {
                  response.writeHead(
                    incoming.statusCode ?? 502,
                    incoming.headers,
                  );
                  incoming.pipe(response);
                  incoming.on("end", () => resume(Effect.void));
                }
                incoming.on("error", () => {
                  response.destroy();
                  resume(Effect.void);
                });
              },
            );
            upstream.setTimeout(15000, () => upstream.destroy());
            upstream.on("error", () => {
              response.destroy();
              resume(Effect.void);
            });
            request.pipe(upstream);
            return Effect.sync(() => upstream.destroy());
          });
        }),
      );
    });
    const admin = createServer((request, response) => {
      run(
        Effect.gen(function* () {
          const url = new URL(request.url ?? "/", "http://proxy");
          if (url.pathname === "/mode") {
            const next = yield* Schema.decodeUnknownEffect(Control)({
              mode: url.searchParams.get("mode"),
              durationMs: Number(url.searchParams.get("ms")),
              pathContains: url.searchParams.get("pathContains") ?? undefined,
            });
            if (next.durationMs < 0 || next.durationMs > 15000) {
              response.writeHead(400);
              response.end();
              return;
            }
            control = next;
            until = Date.now() + next.durationMs;
          }
          response.writeHead(200, { "content-type": "application/json" });
          response.end(JSON.stringify({ control, until, events }));
        }).pipe(
          Effect.catchCause(() =>
            Effect.sync(() => {
              response.writeHead(400);
              response.end();
            }),
          ),
        ),
      );
    });
    for (const [server, port] of [
      [data, config.dataPort],
      [admin, config.controlPort],
    ] as const)
      yield* Effect.acquireRelease(
        Effect.callback<void, Error>((resume) => {
          server.once("error", (error) => resume(Effect.fail(error)));
          server.listen(port, "0.0.0.0", () => resume(Effect.void));
        }),
        () =>
          Effect.sync(() => {
            server.closeAllConnections();
            server.close();
          }),
      );
    return { data, admin };
  });
if (process.env.TCK_PROXY_CHILD === "1")
  // oxlint-disable-next-line effect/effect-run-in-body -- Standalone sidecar entrypoint.
  Effect.runFork(
    Effect.scoped(acquireStorageProxy().pipe(Effect.andThen(Effect.never))),
  );
