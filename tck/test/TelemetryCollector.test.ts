import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import type { AddressInfo } from "node:net";
import { acquireTelemetryCollector } from "../src/TelemetryCollector.js";

/** One ExportLogsServiceRequest carrying a single record, built by hand. */
const logExport = (body: string, traceId: string, spanId: string) => {
  const varint = (value: number) => {
    const out: number[] = [];
    let remaining = value;
    do {
      let byte = remaining & 0x7f;
      remaining = Math.floor(remaining / 128);
      if (remaining > 0) byte |= 0x80;
      out.push(byte);
    } while (remaining > 0);
    return out;
  };
  const bytes = (field: number, payload: readonly number[]): number[] => [
    ...varint((field << 3) | 2),
    ...varint(payload.length),
    ...payload,
  ];
  const text = (field: number, value: string) =>
    bytes(field, [...new TextEncoder().encode(value)]);
  const hex = (value: string) =>
    (value.match(/../g) ?? []).map((pair) => Number.parseInt(pair, 16));
  return Buffer.from(
    bytes(1, [
      ...bytes(
        1,
        bytes(1, [...text(1, "service.name"), ...bytes(2, text(1, "celld"))]),
      ),
      ...bytes(2, [
        ...bytes(2, [
          ...bytes(5, text(1, body)),
          ...bytes(9, hex(traceId)),
          ...bytes(10, hex(spanId)),
        ]),
      ]),
    ]),
  );
};

const TRACE = "11112222333344445555666677778888";
const SPAN = "aaaabbbbccccdddd";

const post = (port: number, path: string, body: Buffer) =>
  Effect.tryPromise({
    try: (signal) =>
      fetch(`http://127.0.0.1:${port}${path}`, {
        method: "POST",
        headers: { "content-type": "application/x-protobuf" },
        body: new Uint8Array(body),
        signal,
      }),
    catch: (error) => error,
  });

const control = (port: number, path: string) =>
  Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        signal,
      });
      return (await response.json()) as {
        deliveries: readonly {
          signal: string;
          status: number;
          mode: string;
          digest: string;
          decodeError?: string;
        }[];
        logs: readonly { body: string; traceId: string; spanId: string }[];
        resources: Record<string, Record<string, unknown>>;
      };
    },
    catch: (error) => error,
  });

const collector = Effect.gen(function* () {
  const started = yield* acquireTelemetryCollector({
    dataPort: 0,
    controlPort: 0,
  });
  return {
    data: (started.data.address() as AddressInfo).port,
    admin: (started.admin.address() as AddressInfo).port,
  };
});

it.live(
  "records and decodes an OTLP payload rather than storing raw bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ports = yield* collector;
        const payload = logExport("marker:cell-await", TRACE, SPAN);
        const response = yield* post(ports.data, "/v1/logs", payload);
        expect(response.status).toBe(200);
        const state = yield* control(ports.admin, "/");
        expect(state.deliveries).toHaveLength(1);
        expect(state.deliveries[0]).toMatchObject({
          signal: "logs",
          status: 200,
          mode: "normal",
        });
        expect(state.deliveries[0]!.decodeError).toBeUndefined();
        expect(state.logs).toHaveLength(1);
        expect(state.logs[0]).toMatchObject({
          body: "marker:cell-await",
          traceId: TRACE,
          spanId: SPAN,
        });
        expect(state.resources["logs"]!["service.name"]).toBe("celld");
        // An unknown path is not an export and must not be recorded as one.
        const stray = yield* post(ports.data, "/v1/metrics", payload);
        expect(stray.status).toBe(404);
        expect((yield* control(ports.admin, "/")).deliveries).toHaveLength(1);
      }),
    ),
);

it.live("reports a payload that is not decodable instead of accepting it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const ports = yield* collector;
      yield* post(ports.data, "/v1/traces", Buffer.from([0x0b, 0x00]));
      const state = yield* control(ports.admin, "/");
      expect(state.deliveries[0]!.decodeError).toBeDefined();
    }),
  ),
);

it.live("injects transient and permanent collector failures on request", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const ports = yield* collector;
      const payload = logExport("marker", TRACE, SPAN);

      yield* control(ports.admin, "/mode?mode=transient&ms=5000");
      const transient = yield* post(ports.data, "/v1/logs", payload);
      expect(transient.status).toBe(503);
      expect(transient.headers.get("retry-after")).toBe("1");

      yield* control(ports.admin, "/mode?mode=permanent&ms=5000");
      expect((yield* post(ports.data, "/v1/logs", payload)).status).toBe(400);

      // A rejected batch is recorded as a delivery, so a retry is observable,
      // but its records are not counted as received.
      const rejected = yield* control(ports.admin, "/");
      expect(rejected.deliveries).toHaveLength(2);
      expect(rejected.deliveries.map((value) => value.status)).toEqual([
        503, 400,
      ]);
      expect(rejected.logs).toHaveLength(0);
      // The same batch resent carries the same digest, which is what proves a
      // retry rather than a fresh batch.
      expect(rejected.deliveries[0]!.digest).toBe(
        rejected.deliveries[1]!.digest,
      );

      yield* control(ports.admin, "/mode?mode=normal&ms=0");
      expect((yield* post(ports.data, "/v1/logs", payload)).status).toBe(200);
      expect((yield* control(ports.admin, "/")).logs).toHaveLength(1);

      // Reset clears the recording so a case can start from a clean state.
      const cleared = yield* control(ports.admin, "/reset");
      expect(cleared.deliveries).toHaveLength(0);
      expect(cleared.logs).toHaveLength(0);
    }),
  ),
);

it.live("refuses an unusable control request without changing the mode", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const ports = yield* collector;
      for (const path of [
        "/mode?mode=explode&ms=100",
        "/mode?mode=transient&ms=-1",
        "/mode?mode=transient&ms=999999",
      ]) {
        const response = yield* Effect.tryPromise({
          try: (signal) =>
            fetch(`http://127.0.0.1:${ports.admin}${path}`, { signal }),
          catch: (error) => error,
        });
        expect(response.status).toBe(400);
      }
      const state = yield* control(ports.admin, "/");
      expect(state.deliveries).toHaveLength(0);
      expect(
        (yield* post(ports.data, "/v1/logs", logExport("marker", TRACE, SPAN)))
          .status,
      ).toBe(200);
    }),
  ),
);
