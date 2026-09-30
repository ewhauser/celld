// Local OTLP collector sidecar.
//
// The roadmap is explicit that "finding a log line alone is insufficient
// evidence of telemetry correctness", so this sidecar terminates celld's real
// OTLP/HTTP export, decodes every payload, and keeps it. The driver then
// asserts on recorded spans and log records rather than on celld's own output.
//
// Two listeners, mirroring StorageProxy.ts:
//   * the data listener speaks OTLP/HTTP on /v1/traces and /v1/logs;
//   * the control listener returns what was received and injects collector
//     faults, and is the only one published to the host loopback.
//
// Every raw body is digested before decoding. celld retries a failed batch, so
// a repeated digest is how the driver proves a retry happened rather than
// inferring it from timing.
import { Effect, Schema } from "effect";
import { createHash } from "node:crypto";
// oxlint-disable-next-line effect/use-http-client-service -- The collector terminates raw OTLP requests and withholds responses.
import { createServer } from "node:http";
import {
  decodeExport,
  type DecodedLog,
  type DecodedSpan,
} from "./TelemetryProtobuf.js";

/**
 * Collector behaviour the driver can install for a bounded window.
 *   normal     – accept and record.
 *   transient  – 503 with Retry-After; celld must retry a batch in this state.
 *   permanent  – 400; celld must drop the batch immediately.
 */
const Control = Schema.Struct({
  mode: Schema.Literals(["normal", "transient", "permanent"]),
  durationMs: Schema.Int,
});
export type Control = typeof Control.Type;

export interface Delivery {
  readonly sequence: number;
  readonly signal: "traces" | "logs";
  readonly contentType: string;
  readonly bytes: number;
  /** Identifies one batch across retries: celld resends the same encoded body. */
  readonly digest: string;
  readonly mode: Control["mode"];
  readonly status: number;
  readonly receivedAtMs: number;
  readonly spanCount: number;
  readonly logCount: number;
  readonly decodeError?: string;
}

/** Bounds every buffer so a long run cannot exhaust the sidecar. */
const LIMIT = 8192;

export const acquireTelemetryCollector = (
  config = { dataPort: 4318, controlPort: 9092 },
) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const run = <A, E>(work: Effect.Effect<A, E>) =>
      // oxlint-disable-next-line effect/effect-run-in-body -- Bridge Node HTTP callbacks into the owning scope.
      Effect.runFork(work.pipe(Effect.forkIn(scope)));
    let control: Control = { mode: "normal", durationMs: 0 };
    let until = 0;
    let sequence = 0;
    const deliveries: Delivery[] = [];
    const spans: DecodedSpan[] = [];
    const logs: DecodedLog[] = [];
    /** Resource attributes of the most recent accepted batch, per signal. */
    const resources: Record<string, Record<string, unknown>> = {};
    const push = <A>(buffer: A[], value: A) => {
      if (buffer.length >= LIMIT) buffer.shift();
      buffer.push(value);
    };

    const data = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () =>
        run(
          Effect.sync(() => {
            const path = (request.url ?? "/").split("?")[0];
            const signal =
              path === "/v1/traces"
                ? ("traces" as const)
                : path === "/v1/logs"
                  ? ("logs" as const)
                  : undefined;
            if (request.method !== "POST" || signal === undefined) {
              response.writeHead(404);
              response.end();
              return;
            }
            const body = Buffer.concat(chunks);
            const mode = Date.now() < until ? control.mode : "normal";
            const contentType = String(request.headers["content-type"] ?? "");
            let decodeError: string | undefined;
            let batchSpans: readonly DecodedSpan[] = [];
            let batchLogs: readonly DecodedLog[] = [];
            let resource: Record<string, unknown> = {};
            // A payload celld answered with a rejection is still recorded as a
            // delivery, but its records are not counted as received.
            try {
              const decoded = decodeExport(signal, body);
              batchSpans = decoded.spans;
              batchLogs = decoded.logs;
              resource = decoded.resource;
            } catch (error) {
              decodeError = String(error);
            }
            const status =
              mode === "transient" ? 503 : mode === "permanent" ? 400 : 200;
            push(deliveries, {
              sequence: ++sequence,
              signal,
              contentType,
              bytes: body.length,
              digest: createHash("sha256").update(body).digest("hex"),
              mode,
              status,
              receivedAtMs: Date.now(),
              spanCount: batchSpans.length,
              logCount: batchLogs.length,
              ...(decodeError === undefined ? {} : { decodeError }),
            });
            if (status === 200 && decodeError === undefined) {
              for (const span of batchSpans) push(spans, span);
              for (const log of batchLogs) push(logs, log);
              resources[signal] = resource;
            }
            response.writeHead(status, {
              "content-type": "application/x-protobuf",
              ...(status === 503 ? { "retry-after": "1" } : {}),
            });
            response.end();
          }).pipe(
            Effect.catchCause(() =>
              Effect.sync(() => {
                response.writeHead(500);
                response.end();
              }),
            ),
          ),
        ),
      );
    });

    const admin = createServer((request, response) => {
      run(
        Effect.gen(function* () {
          const url = new URL(request.url ?? "/", "http://collector");
          if (url.pathname === "/mode") {
            const next = yield* Schema.decodeUnknownEffect(Control)({
              mode: url.searchParams.get("mode"),
              durationMs: Number(url.searchParams.get("ms")),
            });
            if (next.durationMs < 0 || next.durationMs > 120000) {
              response.writeHead(400);
              response.end();
              return;
            }
            control = next;
            until = Date.now() + next.durationMs;
          }
          if (url.pathname === "/reset") {
            deliveries.length = 0;
            spans.length = 0;
            logs.length = 0;
            control = { mode: "normal", durationMs: 0 };
            until = 0;
          }
          response.writeHead(200, { "content-type": "application/json" });
          response.end(
            JSON.stringify({
              control,
              until,
              resources,
              deliveries,
              spans,
              logs,
            }),
          );
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

if (process.env.TCK_COLLECTOR_CHILD === "1")
  // oxlint-disable-next-line effect/effect-run-in-body -- Standalone sidecar entrypoint.
  Effect.runFork(
    Effect.scoped(
      acquireTelemetryCollector().pipe(Effect.andThen(Effect.never)),
    ),
  );
