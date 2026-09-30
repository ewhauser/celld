import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { decodeExport, ProtobufError } from "../src/TelemetryProtobuf.js";

// Captured from celld v0.5.0 exporting over OTLP/HTTP protobuf. A request
// carrying `traceparent: 00-11112222333344445555666677778888-aaaabbbbccccdddd-01`
// reached a Worker that called a Durable Object; the object logged either side
// of an await and then made one outbound fetch. These bytes keep the decoder
// honest against real output rather than only against the builders below.
const TRACES =
  "CoIKCokBCiMKDHNlcnZpY2UubmFtZRITChF0Y2stc3Bpa2Utc2VydmljZQoaCg9zZXJ2aWNlLnZl" +
  "cnNpb24SBwoFMC41LjAKKQoTc2VydmljZS5pbnN0YW5jZS5pZBISChBkZXYtMGM5YTQ1M2ZhZDYx" +
  "ChsKDGNlbGxkLnJlZ2lvbhILCgl1cy1lYXN0LTES8wgKDgoFY2VsbGQSBTAuNS4wEqUBChAgYhfV" +
  "WU60mSxXPV1bpyW8EgjgzUKcMPlAMyoLY2VsbGQuZmV0Y2gwAjmYfPjsK5XWGEFAkAXtK5XWGEo2" +
  "ChBjZWxsZC5yZXF1ZXN0X2lkEiIKIGJjNzVjM2E3YTJkODNhZDEwMDAwMDAwMDAwMDAwMDAxShMK" +
  "DWNlbGxkLmlzb2xhdGUSAhgAShkKE2NlbGxkLnF1ZXVlX3dhaXRfdXMSAhhOErEBChATOwSvaBCL" +
  "6SY3i46SHwP7EgiBB62AqHlMyioSY2VsbGQuY2VsbF9zdGFydHVwMAE5iK0B8CuV1hhBmIJc8SuV" +
  "1hhKWAoKY2VsbGQuY2VsbBJKCkhDb3VudGVyOjYxZTY3YzRmODRkMmYxN2VkZmU1NGY0NzlhYmZi" +
  "OTQzNDQ0N2NiYmYxZmIwYzkyMWM0YzVlMmU1MDY3NzNhNWZKEQoLY2VsbGQuZXBvY2gSAhgBEsoB" +
  "ChARESIiMzNERFVVZmZ3d4iIEgh7lWWCDgibpCIIer67cRhMNWUqC2NlbGxkLmZldGNoMAI58Eyc" +
  "8yuV1hhB8KWi8yuV1hhKNgoQY2VsbGQucmVxdWVzdF9pZBIiCiBiYzc1YzNhN2EyZDgzYWQxMDAw" +
  "MDAwMDAwMDAwMDAwNEoTCg1jZWxsZC5pc29sYXRlEgIYAEoZChNjZWxsZC5xdWV1ZV93YWl0X3Vz" +
  "EgIYBkoZChNjZWxsZC5wYXJlbnRfcmVtb3RlEgIQARKmAQoQEREiIjMzRERVVWZmd3eIiBIIer67" +
  "cRhMNWUiCOz51rBVB4fmKgVmZXRjaDADOaByivMrldYYQRhApPMrldYYSiYKCHVybC5mdWxsEhoK" +
  "GGh0dHA6Ly9kZXZub2RlOjk4NzYvc2lua0ogChlodHRwLnJlc3BvbnNlLnN0YXR1c19jb2RlEgMY" +
  "yAFKGQoTY2VsbGQucGFyZW50X3JlbW90ZRICEAASwQEKEBERIiIzM0REVVVmZnd3iIgSCOz51rBV" +
  "B4fmIgiL8HMJDbB5+SoQY2VsbGQuY2VsbF9mZXRjaDACOWgBXvErldYYQbgZrfMrldYYSlgKCmNl" +
  "bGxkLmNlbGwSSgpIQ291bnRlcjo2MWU2N2M0Zjg0ZDJmMTdlZGZlNTRmNDc5YWJmYjk0MzQ0NDdj" +
  "YmJmMWZiMGM5MjFjNGM1ZTJlNTA2NzczYTVmShkKE2NlbGxkLnBhcmVudF9yZW1vdGUSAhAAEsoB" +
  "ChARESIiMzNERFVVZmZ3d4iIEgiL8HMJDbB5+SIIqqq7u8zM3d0qC2NlbGxkLmZldGNoMAI5OG/i" +
  "7SuV1hhBYMi18yuV1hhKNgoQY2VsbGQucmVxdWVzdF9pZBIiCiBiYzc1YzNhN2EyZDgzYWQxMDAw" +
  "MDAwMDAwMDAwMDAwMkoTCg1jZWxsZC5pc29sYXRlEgIYAEoZChNjZWxsZC5xdWV1ZV93YWl0X3Vz" +
  "EgIYIUoZChNjZWxsZC5wYXJlbnRfcmVtb3RlEgIQAQ==";

const LOGS =
  "CvoCCokBCiMKDHNlcnZpY2UubmFtZRITChF0Y2stc3Bpa2Utc2VydmljZQoaCg9zZXJ2aWNlLnZl" +
  "cnNpb24SBwoFMC41LjAKKQoTc2VydmljZS5pbnN0YW5jZS5pZBISChBkZXYtMGM5YTQ1M2ZhZDYx" +
  "ChsKDGNlbGxkLnJlZ2lvbhILCgl1cy1lYXN0LTES6wEKDgoFY2VsbGQSBTAuNS4wEkgJMP/m7SuV" +
  "1hgQCSoWChR0Y2std29ya2VyLWJlZm9yZS1kb0oQEREiIjMzRERVVWZmd3eIiFIIi/BzCQ2weflZ" +
  "MP/m7SuV1hgSRwkg9HbxK5XWGBAJKhUKE3Rjay1kby1iZWZvcmUtYXdhaXRKEBERIiIzM0REVVVm" +
  "Znd3iIhSCOz51rBVB4fmWSD0dvErldYYEkYJINsG8yuV1hgQCSoUChJ0Y2stZG8tYWZ0ZXItYXdh" +
  "aXRKEBERIiIzM0REVVVmZnd3iIhSCOz51rBVB4fmWSDbBvMrldYY";

// A protobuf encoder, written out field by field, so a decoder change cannot
// be "verified" by the same mistake in the fixture that produced the payload.
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
const tag = (field: number, wire: number) => varint((field << 3) | wire);
const bytes = (field: number, payload: readonly number[]): number[] => [
  ...tag(field, 2),
  ...varint(payload.length),
  ...payload,
];
const text = (field: number, value: string) =>
  bytes(field, [...new TextEncoder().encode(value)]);
const fixed64 = (field: number, value: number) => {
  const out = [...tag(field, 1)];
  let remaining = BigInt(value);
  for (let index = 0; index < 8; index++) {
    out.push(Number(remaining & 0xffn));
    remaining >>= 8n;
  }
  return out;
};
const hexBytes = (value: string) =>
  (value.match(/../g) ?? []).map((pair) => Number.parseInt(pair, 16));

/** KeyValue{1 key, 2 AnyValue}, at whichever field the container uses. */
const stringAttribute = (field: number, key: string, value: string) =>
  bytes(field, [...text(1, key), ...bytes(2, text(1, value))]);
const intAttribute = (field: number, key: string, value: number) =>
  bytes(field, [
    ...text(1, key),
    ...bytes(2, [...tag(3, 0), ...varint(value)]),
  ]);
const boolAttribute = (field: number, key: string, value: boolean) =>
  bytes(field, [
    ...text(1, key),
    ...bytes(2, [...tag(2, 0), ...varint(value ? 1 : 0)]),
  ]);

const span = (options: {
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly attributes?: readonly number[];
}) =>
  bytes(2, [
    ...bytes(1, hexBytes(options.traceId)),
    ...bytes(2, hexBytes(options.spanId)),
    ...(options.parentSpanId === undefined
      ? []
      : bytes(4, hexBytes(options.parentSpanId))),
    ...text(5, options.name),
    ...tag(6, 0),
    ...varint(2),
    ...fixed64(7, 1000),
    ...fixed64(8, 2000),
    ...(options.attributes ?? []),
  ]);

const logRecord = (options: {
  readonly traceId: string;
  readonly spanId: string;
  readonly body: string;
}) =>
  bytes(2, [
    ...fixed64(1, 4242),
    ...tag(2, 0),
    ...varint(9),
    ...bytes(5, text(1, options.body)),
    ...bytes(9, hexBytes(options.traceId)),
    ...bytes(10, hexBytes(options.spanId)),
  ]);

/** ExportXServiceRequest{1: Resource*{1: Resource, 2: Scope*{2: record*}}}. */
const exportRequest = (
  serviceName: string,
  records: readonly (readonly number[])[],
) =>
  new Uint8Array(
    bytes(1, [
      ...bytes(1, stringAttribute(1, "service.name", serviceName)),
      ...bytes(
        2,
        records.flatMap((record) => [...record]),
      ),
    ]),
  );

it.effect("decodes a hand-built OTLP trace export field by field", () =>
  Effect.sync(() => {
    const payload = exportRequest("celld", [
      span({
        traceId: "11112222333344445555666677778888",
        spanId: "aaaabbbbccccdddd",
        name: "celld.fetch",
        attributes: [
          ...stringAttribute(9, "url.full", "http://sink/trace/sink"),
          ...intAttribute(9, "http.response.status_code", 200),
          ...boolAttribute(9, "celld.parent_remote", true),
        ],
      }),
      span({
        traceId: "11112222333344445555666677778888",
        spanId: "1111222233334444",
        parentSpanId: "aaaabbbbccccdddd",
        name: "celld.cell_fetch",
      }),
    ]);
    const decoded = decodeExport("traces", payload);
    expect(decoded.resource["service.name"]).toBe("celld");
    expect(decoded.spans).toHaveLength(2);
    expect(decoded.spans[0]).toMatchObject({
      traceId: "11112222333344445555666677778888",
      spanId: "aaaabbbbccccdddd",
      parentSpanId: "",
      name: "celld.fetch",
      kind: 2,
      startUnixNano: "1000",
      endUnixNano: "2000",
      attributes: {
        "url.full": "http://sink/trace/sink",
        "http.response.status_code": 200,
        "celld.parent_remote": true,
      },
    });
    expect(decoded.spans[1]!.parentSpanId).toBe("aaaabbbbccccdddd");
  }),
);

it.effect("decodes a hand-built OTLP log export field by field", () =>
  Effect.sync(() => {
    const decoded = decodeExport(
      "logs",
      exportRequest("celld", [
        logRecord({
          traceId: "11112222333344445555666677778888",
          spanId: "1111222233334444",
          body: "marker:cell-await",
        }),
      ]),
    );
    expect(decoded.logs).toEqual([
      {
        traceId: "11112222333344445555666677778888",
        spanId: "1111222233334444",
        body: "marker:cell-await",
        severityNumber: 9,
        timeUnixNano: "4242",
        attributes: {},
      },
    ]);
  }),
);

it.effect("decodes the trace payload celld v0.5.0 actually produced", () =>
  Effect.sync(() => {
    const decoded = decodeExport("traces", Buffer.from(TRACES, "base64"));
    expect(decoded.resource).toMatchObject({
      "service.name": "tck-spike-service",
      "service.version": "0.5.0",
      "celld.region": "us-east-1",
    });
    // The request arrived on this trace, and the whole chain stayed on it.
    const chain = decoded.spans.filter(
      (value) => value.traceId === "11112222333344445555666677778888",
    );
    expect(chain.map((value) => value.name).sort()).toEqual([
      "celld.cell_fetch",
      "celld.fetch",
      "celld.fetch",
      "fetch",
    ]);
    const worker = chain.find(
      (value) => value.parentSpanId === "aaaabbbbccccdddd",
    )!;
    const cell = chain.find((value) => value.name === "celld.cell_fetch")!;
    const outbound = chain.find((value) => value.name === "fetch")!;
    expect(cell.parentSpanId).toBe(worker.spanId);
    expect(outbound.parentSpanId).toBe(cell.spanId);
    expect(outbound.attributes).toMatchObject({
      "url.full": "http://devnode:9876/sink",
      "http.response.status_code": 200,
    });
    // A cell start is its own root trace, not a child of the request.
    const startup = decoded.spans.find(
      (value) => value.name === "celld.cell_startup",
    )!;
    expect(startup.parentSpanId).toBe("");
    expect(startup.traceId).not.toBe("11112222333344445555666677778888");
  }),
);

it.effect("decodes the log payload celld v0.5.0 actually produced", () =>
  Effect.sync(() => {
    const decoded = decodeExport("logs", Buffer.from(LOGS, "base64"));
    expect(decoded.logs.map((log) => log.body)).toEqual([
      "tck-worker-before-do",
      "tck-do-before-await",
      "tck-do-after-await",
    ]);
    for (const log of decoded.logs)
      expect(log.traceId).toBe("11112222333344445555666677778888");
    // The correlation survives the await: both object lines carry one span.
    expect(decoded.logs[1]!.spanId).toBe(decoded.logs[2]!.spanId);
    expect(decoded.logs[0]!.spanId).not.toBe(decoded.logs[1]!.spanId);
  }),
);

it.effect("refuses malformed wire data instead of inventing records", () =>
  Effect.sync(() => {
    const good = exportRequest("celld", [
      span({
        traceId: "11112222333344445555666677778888",
        spanId: "aaaabbbbccccdddd",
        name: "celld.fetch",
      }),
    ]);
    // A length prefix that runs past the end of the buffer.
    expect(() =>
      decodeExport("traces", good.subarray(0, good.length - 3)),
    ).toThrow(ProtobufError);
    // The removed group wire types, 3 and 4.
    expect(() => decodeExport("traces", new Uint8Array([0x0b, 0x00]))).toThrow(
      ProtobufError,
    );
    // A varint with no terminating byte.
    expect(() => decodeExport("traces", new Uint8Array([0x80]))).toThrow(
      ProtobufError,
    );
    // Field number zero is never valid.
    expect(() => decodeExport("traces", new Uint8Array([0x02, 0x00]))).toThrow(
      ProtobufError,
    );
  }),
);

it.effect("keeps the two signals apart rather than guessing", () =>
  Effect.sync(() => {
    // Reading one signal's payload as the other must never yield a usable
    // record of the wrong kind. The decoder may also simply refuse, because a
    // Span's name field walked as an AnyValue is arbitrary bytes; what it must
    // not do is hand back records that look real.
    const usable = (signal: "traces" | "logs", base64: string) => {
      let decoded;
      try {
        decoded = decodeExport(signal, Buffer.from(base64, "base64"));
      } catch (error) {
        expect(error).toBeInstanceOf(ProtobufError);
        return 0;
      }
      return signal === "traces"
        ? decoded.spans.filter((value) => value.name !== "").length
        : decoded.logs.filter((log) => log.body !== "").length;
    };
    expect(usable("logs", TRACES)).toBe(0);
    expect(usable("traces", LOGS)).toBe(0);
    // The matching signal does produce usable records, so the check above is
    // not passing merely because the decoder returns nothing for everything.
    expect(
      decodeExport("traces", Buffer.from(TRACES, "base64")).spans.filter(
        (value) => value.name !== "",
      ).length,
    ).toBeGreaterThan(0);
    expect(
      decodeExport("logs", Buffer.from(LOGS, "base64")).logs.filter(
        (log) => log.body !== "",
      ).length,
    ).toBe(3);
  }),
);
