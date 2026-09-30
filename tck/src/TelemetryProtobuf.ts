// Minimal OTLP/HTTP protobuf reader.
//
// celld v0.5.0 selects OTLP/HTTP **protobuf** whenever CELLD_OTEL names a
// collector base URL, so a collector that only stored raw bytes could not tell
// a correct export from a malformed one. This module decodes exactly the
// fields the telemetry suite asserts on, straight from the wire format, so the
// harness needs no protobuf runtime dependency and the decoder itself is
// covered by unit tests against captured celld payloads.
//
// Field numbers are from the OTLP v1 protos (opentelemetry-proto):
//   ExportTraceServiceRequest{1: repeated ResourceSpans}
//   ResourceSpans{1: Resource, 2: repeated ScopeSpans}
//   ScopeSpans{2: repeated Span}
//   Span{1 trace_id, 2 span_id, 4 parent_span_id, 5 name, 6 kind,
//        7 start_time_unix_nano, 8 end_time_unix_nano, 9 attributes}
//   ExportLogsServiceRequest{1: repeated ResourceLogs}
//   ResourceLogs{1: Resource, 2: repeated ScopeLogs}
//   ScopeLogs{2: repeated LogRecord}
//   LogRecord{1 time_unix_nano, 2 severity_number, 3 severity_text, 5 body,
//             6 attributes, 9 trace_id, 10 span_id}
//   Resource{1: repeated KeyValue}; KeyValue{1 key, 2 AnyValue}
//   AnyValue{1 string, 2 bool, 3 int, 4 double, 7 bytes}

export type AttributeValue = string | number | boolean;
export type Attributes = Record<string, AttributeValue>;

export interface DecodedSpan {
  readonly traceId: string;
  readonly spanId: string;
  /** Empty when the span has no parent; celld omits the field for a root span. */
  readonly parentSpanId: string;
  readonly name: string;
  readonly kind: number;
  readonly startUnixNano: string;
  readonly endUnixNano: string;
  readonly attributes: Attributes;
}

export interface DecodedLog {
  readonly traceId: string;
  readonly spanId: string;
  readonly body: string;
  readonly severityNumber: number;
  readonly timeUnixNano: string;
  readonly attributes: Attributes;
}

export interface DecodedBatch {
  readonly resource: Attributes;
  readonly spans: readonly DecodedSpan[];
  readonly logs: readonly DecodedLog[];
}

/** A protobuf wire-format field: length-delimited payloads arrive as slices. */
interface Field {
  readonly number: number;
  readonly wire: number;
  readonly varint: bigint;
  readonly bytes: Uint8Array;
}

export class ProtobufError extends Error {}

const EMPTY = new Uint8Array(0);

/** Walks one protobuf message. Unknown fields are skipped, as the format requires. */
function* walk(buffer: Uint8Array): Generator<Field> {
  let offset = 0;
  const varint = () => {
    let value = 0n;
    let shift = 0n;
    for (;;) {
      if (offset >= buffer.length) throw new ProtobufError("truncated varint");
      // A varint wider than 10 bytes cannot be a valid 64-bit value.
      if (shift > 63n) throw new ProtobufError("oversized varint");
      const byte = buffer[offset++]!;
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
      shift += 7n;
    }
  };
  const take = (length: number) => {
    if (length < 0 || offset + length > buffer.length)
      throw new ProtobufError("truncated field");
    const slice = buffer.subarray(offset, offset + length);
    offset += length;
    return slice;
  };
  while (offset < buffer.length) {
    const key = varint();
    const number = Number(key >> 3n);
    const wire = Number(key & 7n);
    if (number === 0) throw new ProtobufError("field number 0");
    if (wire === 0) yield { number, wire, varint: varint(), bytes: EMPTY };
    else if (wire === 1) yield { number, wire, varint: 0n, bytes: take(8) };
    else if (wire === 2)
      yield { number, wire, varint: 0n, bytes: take(Number(varint())) };
    else if (wire === 5) yield { number, wire, varint: 0n, bytes: take(4) };
    else throw new ProtobufError(`unsupported wire type ${wire}`);
  }
}

const utf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const fixed64 = (bytes: Uint8Array) => {
  let value = 0n;
  for (let index = 7; index >= 0; index--)
    value = (value << 8n) | BigInt(bytes[index]!);
  return value.toString();
};

const float64 = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, 8).getFloat64(0, true);

/** Signed 64-bit varint, as OTLP encodes AnyValue.int_value. */
const signed = (value: bigint) => Number(BigInt.asIntN(64, value));

const anyValue = (buffer: Uint8Array): AttributeValue | undefined => {
  for (const field of walk(buffer)) {
    if (field.number === 1) return utf8(field.bytes);
    if (field.number === 2) return field.varint !== 0n;
    if (field.number === 3) return signed(field.varint);
    if (field.number === 4) return float64(field.bytes);
    if (field.number === 7) return hex(field.bytes);
  }
  return undefined;
};

/** KeyValue pairs; a key with no decodable value is dropped rather than guessed. */
const keyValues = (entries: readonly Uint8Array[]): Attributes => {
  const attributes: Attributes = {};
  for (const entry of entries) {
    let key: string | undefined;
    let value: AttributeValue | undefined;
    for (const field of walk(entry)) {
      if (field.number === 1) key = utf8(field.bytes);
      if (field.number === 2) value = anyValue(field.bytes);
    }
    if (key !== undefined && value !== undefined) attributes[key] = value;
  }
  return attributes;
};

/** Resource{1: repeated KeyValue}. */
const resourceAttributes = (buffer: Uint8Array) => {
  const entries: Uint8Array[] = [];
  for (const field of walk(buffer))
    if (field.number === 1) entries.push(field.bytes);
  return keyValues(entries);
};

const decodeSpan = (buffer: Uint8Array): DecodedSpan => {
  let traceId = "";
  let spanId = "";
  let parentSpanId = "";
  let name = "";
  let kind = 0;
  let startUnixNano = "0";
  let endUnixNano = "0";
  const entries: Uint8Array[] = [];
  for (const field of walk(buffer)) {
    if (field.number === 1) traceId = hex(field.bytes);
    else if (field.number === 2) spanId = hex(field.bytes);
    else if (field.number === 4) parentSpanId = hex(field.bytes);
    else if (field.number === 5) name = utf8(field.bytes);
    else if (field.number === 6) kind = Number(field.varint);
    else if (field.number === 7) startUnixNano = fixed64(field.bytes);
    else if (field.number === 8) endUnixNano = fixed64(field.bytes);
    else if (field.number === 9) entries.push(field.bytes);
  }
  return {
    traceId,
    spanId,
    parentSpanId,
    name,
    kind,
    startUnixNano,
    endUnixNano,
    attributes: keyValues(entries),
  };
};

const decodeLog = (buffer: Uint8Array): DecodedLog => {
  let traceId = "";
  let spanId = "";
  let body = "";
  let severityNumber = 0;
  let timeUnixNano = "0";
  const entries: Uint8Array[] = [];
  for (const field of walk(buffer)) {
    if (field.number === 1) timeUnixNano = fixed64(field.bytes);
    else if (field.number === 2) severityNumber = Number(field.varint);
    else if (field.number === 5) {
      const value = anyValue(field.bytes);
      body = value === undefined ? "" : String(value);
    } else if (field.number === 6) entries.push(field.bytes);
    else if (field.number === 9) traceId = hex(field.bytes);
    else if (field.number === 10) spanId = hex(field.bytes);
  }
  return {
    traceId,
    spanId,
    body,
    severityNumber,
    timeUnixNano,
    attributes: keyValues(entries),
  };
};

/**
 * Decodes an ExportTraceServiceRequest or ExportLogsServiceRequest. Both share
 * the Resource{...}/Scope{...}/record nesting, so one walk handles either; the
 * caller knows which signal it asked for from the request path.
 */
export const decodeExport = (
  signal: "traces" | "logs",
  buffer: Uint8Array,
): DecodedBatch => {
  let resource: Attributes = {};
  const spans: DecodedSpan[] = [];
  const logs: DecodedLog[] = [];
  for (const top of walk(buffer)) {
    if (top.number !== 1) continue;
    for (const inner of walk(top.bytes)) {
      if (inner.number === 1)
        resource = { ...resource, ...resourceAttributes(inner.bytes) };
      if (inner.number !== 2) continue;
      for (const record of walk(inner.bytes)) {
        if (record.number !== 2) continue;
        if (signal === "traces") spans.push(decodeSpan(record.bytes));
        else logs.push(decodeLog(record.bytes));
      }
    }
  }
  return { resource, spans, logs };
};
