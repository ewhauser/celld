// Pure oracles for the CLI and telemetry qualification suite.
//
// Everything these classify is an observation: a span or log record the local
// collector decoded, an object listing from the bucket sink, or the captured
// streams and exit status of a `celld` invocation. Keeping the classification
// here, away from the orchestration, is what makes a deliberately wrong
// observation testable without a fleet: a span whose parent link is broken, a
// trace id that changed across the Durable Object hop, a payload the collector
// should have received and did not, or an application result that moved while
// the collector was down.
import { Effect, Schema } from "effect";
import { TckError } from "./Domain.js";
import { equal } from "./Oracle.js";
import type {
  AttributeValue,
  DecodedLog,
  DecodedSpan,
} from "./TelemetryProtobuf.js";
import type { ParquetFooter } from "./TelemetryParquet.js";

const refuse = (message: string, detail?: string) =>
  Effect.fail(
    new TckError({
      phase: "telemetry",
      message,
      ...(detail === undefined ? {} : { detail }),
    }),
  );

// ---------------------------------------------------------------------------
// Collector payloads
// ---------------------------------------------------------------------------

const Attribute = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
const Attributes = Schema.Record(Schema.String, Attribute);

export const Span = Schema.Struct({
  traceId: Schema.String,
  spanId: Schema.String,
  parentSpanId: Schema.String,
  name: Schema.String,
  kind: Schema.Int,
  startUnixNano: Schema.String,
  endUnixNano: Schema.String,
  attributes: Attributes,
});
export const Log = Schema.Struct({
  traceId: Schema.String,
  spanId: Schema.String,
  body: Schema.String,
  severityNumber: Schema.Int,
  timeUnixNano: Schema.String,
  attributes: Attributes,
});
export const Delivery = Schema.Struct({
  sequence: Schema.Int,
  signal: Schema.Literals(["traces", "logs"]),
  contentType: Schema.String,
  bytes: Schema.Int,
  digest: Schema.String,
  mode: Schema.String,
  status: Schema.Int,
  receivedAtMs: Schema.Number,
  spanCount: Schema.Int,
  logCount: Schema.Int,
  decodeError: Schema.optionalKey(Schema.String),
});
export const CollectorState = Schema.Struct({
  control: Schema.Struct({ mode: Schema.String, durationMs: Schema.Int }),
  until: Schema.Number,
  resources: Schema.Record(Schema.String, Attributes),
  deliveries: Schema.Array(Delivery),
  spans: Schema.Array(Span),
  logs: Schema.Array(Log),
});
export type CollectorState = typeof CollectorState.Type;
export type Delivery = typeof Delivery.Type;

// ---------------------------------------------------------------------------
// Identifier shapes
// ---------------------------------------------------------------------------

const TRACE_ID = /^[0-9a-f]{32}$/;
const SPAN_ID = /^[0-9a-f]{16}$/;

export const isTraceId = (value: string) =>
  TRACE_ID.test(value) && value !== "0".repeat(32);
export const isSpanId = (value: string) =>
  SPAN_ID.test(value) && value !== "0".repeat(16);

/** A W3C traceparent for a sampled remote parent, as the driver sends it. */
export const traceparent = (traceId: string, spanId: string) =>
  `00-${traceId}-${spanId}-01`;

// ---------------------------------------------------------------------------
// Log-driven request identification
// ---------------------------------------------------------------------------

/**
 * Resolves the trace a request ran in from a marker the fixture logged.
 *
 * celld's Worker spans carry no request URL, so a recorded span cannot be
 * attributed to one probe by inspection. A `console.log` marker can: celld
 * stamps each log record with the trace and span of the handler that wrote it,
 * which is exactly the correlation the suite is also testing. Requiring
 * *exactly* one record keeps a duplicated or missing marker from resolving.
 */
export const traceOfMarker = (
  logs: readonly DecodedLog[],
  marker: string,
): Effect.Effect<{ traceId: string; spanId: string }, TckError> => {
  const found = logs.filter((log) => log.body === marker);
  if (found.length !== 1)
    return refuse(
      `Expected exactly one log record for marker ${marker}`,
      `found ${found.length}`,
    );
  const record = found[0]!;
  if (!isTraceId(record.traceId) || !isSpanId(record.spanId))
    return refuse(
      `Log record ${marker} carries no usable trace context`,
      `${record.traceId}/${record.spanId}`,
    );
  return Effect.succeed({
    traceId: record.traceId,
    spanId: record.spanId,
  });
};

/**
 * An unsampled request must record nothing at all.
 *
 * The marker is the only handle on a request that produced no telemetry, so
 * this checks that nothing carrying it reached the collector: no log record,
 * and no span in whatever trace such a record would have named. The case pairs
 * it with the same probe against an always-on node, which must resolve; alone,
 * an absence proves nothing about whether the collector was even reachable.
 */
export const checkNotRecorded = (
  state: {
    readonly logs: readonly DecodedLog[];
    readonly spans: readonly DecodedSpan[];
  },
  marker: string,
): Effect.Effect<void, TckError> => {
  const logs = state.logs.filter((log) => log.body === marker);
  if (logs.length > 0) {
    const traces = new Set(logs.map((log) => log.traceId));
    const spans = state.spans.filter((span) => traces.has(span.traceId));
    return refuse(
      `An unsampled request recorded ${logs.length} log record(s) and ${spans.length} span(s) for ${marker}`,
      logs.map((log) => `${log.traceId}/${log.spanId}`).join(","),
    );
  }
  return Effect.void;
};

// ---------------------------------------------------------------------------
// Trace context
// ---------------------------------------------------------------------------

export interface ChainStep {
  readonly name: string;
  /** Attributes the span must carry, compared by value. */
  readonly attributes?: Readonly<Record<string, AttributeValue>>;
}

/**
 * Walks an expected parent chain through the recorded spans.
 *
 * Each step must be the unique span carrying the expected trace id, the
 * expected name, and the previous step's span id as its parent. That single
 * requirement rejects both failure modes the roadmap names: a span whose
 * parent link is broken has no match at its step, and a trace id that changed
 * across the Durable Object hop has no match either, because the whole walk is
 * anchored to one trace id.
 */
export const checkSpanChain = (
  spans: readonly DecodedSpan[],
  options: {
    readonly traceId: string;
    /** The remote parent the first step must attach to. */
    readonly rootParentSpanId: string;
    readonly steps: readonly ChainStep[];
  },
): Effect.Effect<readonly DecodedSpan[], TckError> =>
  Effect.gen(function* () {
    if (!isTraceId(options.traceId))
      return yield* refuse("Chain anchor is not a trace id", options.traceId);
    if (options.steps.length === 0)
      return yield* refuse("A span chain needs at least one step");
    const chain: DecodedSpan[] = [];
    let parent = options.rootParentSpanId;
    for (const step of options.steps) {
      const matches = spans.filter(
        (span) =>
          span.traceId === options.traceId &&
          span.name === step.name &&
          span.parentSpanId === parent,
      );
      if (matches.length !== 1)
        return yield* refuse(
          `Expected exactly one ${step.name} span under ${parent || "(root)"} in trace ${options.traceId}`,
          `found ${matches.length}`,
        );
      const span = matches[0]!;
      if (!isSpanId(span.spanId))
        return yield* refuse(
          `Span ${step.name} has no usable span id`,
          span.spanId,
        );
      for (const [key, expected] of Object.entries(step.attributes ?? {}))
        if (span.attributes[key] !== expected)
          return yield* refuse(
            `Span ${step.name} attribute ${key} is ${String(span.attributes[key])}, expected ${String(expected)}`,
          );
      chain.push(span);
      parent = span.spanId;
    }
    const ids = new Set(chain.map((span) => span.spanId));
    return ids.size === chain.length
      ? (chain as readonly DecodedSpan[])
      : yield* refuse("A span chain reused a span id");
  });

/**
 * A malformed incoming traceparent must start a *new* trace: a well-formed
 * trace id, no remote parent, and nothing carried over from the rejected
 * input or from any other request in the run.
 */
export const checkIndependentTrace = (
  resolved: { readonly traceId: string; readonly spanId: string },
  spans: readonly DecodedSpan[],
  rejected: readonly string[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (!isTraceId(resolved.traceId))
      return yield* refuse(
        "A rejected traceparent did not start a well-formed trace",
        resolved.traceId,
      );
    for (const value of rejected)
      if (resolved.traceId === value || resolved.traceId.includes(value))
        return yield* refuse(
          "A malformed traceparent was carried into the new trace",
          value,
        );
    const root = spans.filter((span) => span.spanId === resolved.spanId);
    if (root.length !== 1)
      return yield* refuse(
        "Expected exactly one span for the rejected-traceparent request",
        `found ${root.length}`,
      );
    return root[0]!.parentSpanId === ""
      ? undefined
      : yield* refuse(
          "A rejected traceparent left a remote parent on the new trace",
          root[0]!.parentSpanId,
        );
  });

/**
 * Log correlation, including across `await`: each marker must appear exactly
 * once and carry the trace and span of the handler that wrote it.
 */
export const checkLogCorrelation = (
  logs: readonly DecodedLog[],
  expected: readonly {
    readonly body: string;
    readonly traceId: string;
    readonly spanId: string;
  }[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    for (const want of expected) {
      const found = logs.filter((log) => log.body === want.body);
      if (found.length !== 1)
        return yield* refuse(
          `Expected exactly one log record for ${want.body}`,
          `found ${found.length}`,
        );
      const record = found[0]!;
      if (record.traceId !== want.traceId)
        return yield* refuse(
          `Log ${want.body} is in trace ${record.traceId}, expected ${want.traceId}`,
        );
      if (record.spanId !== want.spanId)
        return yield* refuse(
          `Log ${want.body} is attributed to span ${record.spanId}, expected ${want.spanId}`,
        );
    }
  });

/**
 * The outbound `traceparent` celld sent, as the downstream fixture saw it: the
 * trace must continue and the span must be the recorded outbound span, never
 * the caller's own.
 */
export const checkOutboundHeader = (
  header: string | null,
  options: {
    readonly traceId: string;
    readonly outboundSpanId: string;
    readonly callerSpanId: string;
  },
): Effect.Effect<void, TckError> => {
  if (header === null) return refuse("No traceparent reached the downstream");
  const parts = header.split("-");
  if (parts.length !== 4 || parts[0] !== "00")
    return refuse("Downstream traceparent is not a v0 W3C header", header);
  const [, traceId, spanId, flags] = parts as [string, string, string, string];
  if (traceId !== options.traceId)
    return refuse(
      `Outbound traceparent changed the trace id to ${traceId}`,
      options.traceId,
    );
  if (spanId === options.callerSpanId)
    return refuse("Outbound traceparent reused the caller's span id", spanId);
  if (spanId !== options.outboundSpanId)
    return refuse(
      `Outbound traceparent carries ${spanId}, not the recorded outbound span ${options.outboundSpanId}`,
    );
  return /^[0-9a-f]{2}$/.test(flags)
    ? Effect.void
    : refuse("Outbound traceparent has malformed flags", flags);
};

// ---------------------------------------------------------------------------
// Export payloads, retry, and failure isolation
// ---------------------------------------------------------------------------

/** celld selects OTLP/HTTP protobuf for a collector URL and names the service. */
export const checkExportEnvelope = (
  state: CollectorState,
  options: {
    readonly serviceName: string;
    readonly signals: readonly ("traces" | "logs")[];
  },
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    for (const signal of options.signals) {
      const deliveries = state.deliveries.filter(
        (delivery) => delivery.signal === signal,
      );
      if (deliveries.length === 0)
        return yield* refuse(`The collector received no ${signal} payload`);
      for (const delivery of deliveries) {
        if (delivery.decodeError !== undefined)
          return yield* refuse(
            `A ${signal} payload did not decode as OTLP`,
            delivery.decodeError,
          );
        if (!delivery.contentType.includes("protobuf"))
          return yield* refuse(
            `A ${signal} payload was not OTLP/HTTP protobuf`,
            delivery.contentType,
          );
      }
      const resource = state.resources[signal];
      if (resource === undefined)
        return yield* refuse(`No ${signal} resource was recorded`);
      if (resource["service.name"] !== options.serviceName)
        return yield* refuse(
          `The ${signal} resource names ${String(resource["service.name"])}, expected ${options.serviceName}`,
        );
    }
  });

/** The documented ceiling on delivery attempts for one batch. */
export const MAX_ATTEMPTS = 5;

const byDigest = (deliveries: readonly Delivery[]) => {
  const groups = new Map<string, Delivery[]>();
  for (const delivery of deliveries) {
    const existing = groups.get(delivery.digest);
    if (existing) existing.push(delivery);
    else groups.set(delivery.digest, [delivery]);
  }
  return groups;
};

/**
 * A transient collector failure must produce a retry of the same batch, and no
 * batch may exceed the documented five attempts.
 */
export const checkRetryEvidence = (
  deliveries: readonly Delivery[],
): Effect.Effect<{ digest: string; attempts: number }, TckError> =>
  Effect.gen(function* () {
    const groups = byDigest(deliveries);
    for (const [digest, group] of groups)
      if (group.length > MAX_ATTEMPTS)
        return yield* refuse(
          `Batch ${digest.slice(0, 12)} was delivered ${group.length} times, above the documented ${MAX_ATTEMPTS}`,
        );
    const retried = [...groups.entries()]
      .filter(
        ([, group]) =>
          group.length > 1 && group.some((delivery) => delivery.status === 503),
      )
      .sort((left, right) => right[1].length - left[1].length)[0];
    return retried === undefined
      ? yield* refuse(
          "No batch was redelivered after a transient collector failure",
          `${groups.size} distinct batch(es)`,
        )
      : { digest: retried[0], attempts: retried[1].length };
  });

/** Application observations must be identical across a telemetry outage. */
export const checkResultsUnchanged = (
  label: string,
  before: unknown,
  after: unknown,
): Effect.Effect<void, TckError> =>
  equal(after, before).pipe(
    Effect.mapError(
      (error) =>
        new TckError({
          phase: "telemetry",
          message: `Application results changed under a telemetry fault: ${label}`,
          detail: error.message,
        }),
    ),
  );

/**
 * The collector must have been genuinely impaired. Without this, an
 * "application unchanged" result would also pass on a run where the injected
 * fault never reached the exporter at all.
 */
export const checkOutageObserved = (
  deliveries: readonly Delivery[],
  mode: string,
): Effect.Effect<void, TckError> =>
  deliveries.some((delivery) => delivery.mode === mode)
    ? Effect.void
    : refuse(
        `The collector never served a delivery in ${mode} mode`,
        `${deliveries.length} delivery(ies)`,
      );

// ---------------------------------------------------------------------------
// Bucket sink
// ---------------------------------------------------------------------------

export interface TelemetryObject {
  readonly signal: "traces" | "logs";
  readonly node: string;
  readonly year: string;
  readonly month: string;
  readonly day: string;
  readonly hour: string;
  readonly file: string;
}

// telemetry/<signal>/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet
const KEY =
  /(?:^|\/)telemetry\/(traces|logs)\/([^/]+)\/(\d{4})\/(\d{2})\/(\d{2})\/(\d{2})\/([^/]+)\.parquet$/;

/** Parses a documented telemetry object key; undefined when it does not match. */
export const parseTelemetryKey = (key: string): TelemetryObject | undefined => {
  const match = KEY.exec(key);
  if (!match) return undefined;
  const [, signal, node, year, month, day, hour, file] = match as unknown as [
    string,
    "traces" | "logs",
    string,
    string,
    string,
    string,
    string,
    string,
  ];
  const inRange = (value: string, low: number, high: number) => {
    const parsed = Number(value);
    return Number.isInteger(parsed) && parsed >= low && parsed <= high;
  };
  if (
    !inRange(month, 1, 12) ||
    !inRange(day, 1, 31) ||
    !inRange(hour, 0, 23) ||
    node.length === 0 ||
    file.length === 0
  )
    return undefined;
  return { signal, node, year, month, day, hour, file };
};

export const checkTelemetryLayout = (
  keys: readonly string[],
  signals: readonly ("traces" | "logs")[],
): Effect.Effect<readonly TelemetryObject[], TckError> =>
  Effect.gen(function* () {
    if (keys.length === 0)
      return yield* refuse("The bucket sink wrote no telemetry objects");
    const parsed: TelemetryObject[] = [];
    for (const key of keys) {
      const object = parseTelemetryKey(key);
      if (object === undefined)
        return yield* refuse(
          "A telemetry object is not under the documented partition layout",
          key,
        );
      parsed.push(object);
    }
    for (const signal of signals)
      if (!parsed.some((object) => object.signal === signal))
        return yield* refuse(`The bucket sink wrote no ${signal} objects`);
    return parsed as readonly TelemetryObject[];
  });

/** The declared schema version celld stamps on every telemetry object. */
export const SCHEMA_VERSION = "v0-unstable";

export const checkTelemetryMetadata = (
  metadata: Readonly<Record<string, string>>,
): Effect.Effect<void, TckError> => {
  const entry = Object.entries(metadata).find(
    ([name]) => name.toLowerCase() === "x-amz-meta-celld-schema",
  );
  if (entry === undefined)
    return refuse(
      "A telemetry object carries no schema version in its metadata",
      JSON.stringify(metadata),
    );
  return entry[1] === SCHEMA_VERSION
    ? Effect.void
    : refuse(
        `A telemetry object declares schema ${entry[1]}, expected ${SCHEMA_VERSION}`,
      );
};

export const checkParquetSchema = (
  footer: ParquetFooter,
  required: readonly string[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (footer.rows <= 0)
      return yield* refuse("A telemetry Parquet file declares no rows");
    const columns = new Set(footer.columns);
    const missing = required.filter((column) => !columns.has(column));
    return missing.length === 0
      ? undefined
      : yield* refuse(
          "A telemetry Parquet file is missing documented columns",
          missing.join(","),
        );
  });

// ---------------------------------------------------------------------------
// CLI invocations
// ---------------------------------------------------------------------------

export interface CommandResult {
  readonly label: string;
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * The wrapper the suite runs inside the container. `celld` is invoked with the
 * caller's arguments, both streams are captured separately, and one line
 * carries the exit status and the base64 of each stream, so the harness sees
 * the real exit code instead of a process failure.
 */
export const COMMAND_MARKER = "__tck_cli__";
export const COMMAND_WRAPPER =
  `celld "$@" >/tmp/tck-out 2>/tmp/tck-err; code=$?; ` +
  `echo "${COMMAND_MARKER} $code $(base64 -w0 </tmp/tck-out) $(base64 -w0 </tmp/tck-err)"`;

export const parseCommandOutput = (
  label: string,
  raw: string,
): Effect.Effect<CommandResult, TckError> => {
  const lines = raw
    .split("\n")
    .filter((line) => line.startsWith(COMMAND_MARKER + " "));
  if (lines.length !== 1)
    return refuse(
      `Expected exactly one ${COMMAND_MARKER} line for ${label}`,
      `found ${lines.length}`,
    );
  const parts = lines[0]!.split(" ");
  if (parts.length !== 4)
    return refuse(`Malformed ${COMMAND_MARKER} line for ${label}`, lines[0]);
  const exitCode = Number(parts[1]);
  if (!Number.isInteger(exitCode) || exitCode < 0)
    return refuse(`Unusable exit status for ${label}`, parts[1]);
  const decode = (value: string) => {
    try {
      return Buffer.from(value, "base64").toString("utf8");
    } catch {
      return undefined;
    }
  };
  const stdout = decode(parts[2]!);
  const stderr = decode(parts[3]!);
  return stdout === undefined || stderr === undefined
    ? refuse(`Unusable captured streams for ${label}`)
    : Effect.succeed({ label, exitCode, stdout, stderr });
};

export interface CommandExpectation {
  readonly exitCode: number;
  readonly stdout?: string;
  readonly stdoutIncludes?: readonly string[];
  readonly stdoutEmpty?: boolean;
  readonly stderrIncludes?: readonly string[];
  readonly stderrEmpty?: boolean;
}

const checkCommand = (
  result: CommandResult,
  expectation: CommandExpectation,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (result.exitCode !== expectation.exitCode)
      return yield* refuse(
        `${result.label} exited ${result.exitCode}, expected ${expectation.exitCode}`,
        result.stderr || result.stdout,
      );
    if (
      expectation.stdout !== undefined &&
      result.stdout.trim() !== expectation.stdout
    )
      return yield* refuse(
        `${result.label} printed unexpected data`,
        result.stdout,
      );
    for (const fragment of expectation.stdoutIncludes ?? [])
      if (!result.stdout.includes(fragment))
        return yield* refuse(
          `${result.label} stdout is missing ${fragment}`,
          result.stdout,
        );
    if (expectation.stdoutEmpty === true && result.stdout.trim() !== "")
      return yield* refuse(
        `${result.label} wrote data to stdout`,
        result.stdout,
      );
    for (const fragment of expectation.stderrIncludes ?? [])
      if (!result.stderr.includes(fragment))
        return yield* refuse(
          `${result.label} stderr is missing ${fragment}`,
          result.stderr,
        );
    if (expectation.stderrEmpty === true && result.stderr.trim() !== "")
      return yield* refuse(
        `${result.label} wrote messages to stderr`,
        result.stderr,
      );
  });

/** Guards the oracle itself: a rejection expectation may never describe success. */
export const checkCommandRejected = (
  result: CommandResult,
  expectation: CommandExpectation,
): Effect.Effect<void, TckError> =>
  expectation.exitCode === 0
    ? refuse("A rejection oracle requires a non-zero exit status")
    : checkCommand(result, expectation);

/**
 * For an error path whose exact status v0.5.0 does not document: the command
 * must fail, and it must keep to the documented stream split by reporting on
 * stderr without emitting a partial result on its data stream.
 */
export const checkCommandFailed = (
  result: CommandResult,
  expectation: Omit<CommandExpectation, "exitCode"> = {},
): Effect.Effect<void, TckError> =>
  result.exitCode === 0
    ? refuse(
        `${result.label} succeeded where a rejection was required`,
        result.stdout,
      )
    : result.stderr.trim() === ""
      ? refuse(`${result.label} failed without reporting anything on stderr`)
      : checkCommand(result, {
          ...expectation,
          exitCode: result.exitCode,
          stdoutEmpty: expectation.stdoutEmpty ?? true,
        });

export const checkCommandAccepted = (
  result: CommandResult,
  expectation: Omit<CommandExpectation, "exitCode"> = {},
): Effect.Effect<void, TckError> =>
  checkCommand(result, { ...expectation, exitCode: 0 });

/**
 * The documented stream contract: data on stdout, messages on stderr, so a
 * redirect or a pipe carries only data. Every stdout line must parse as JSON.
 */
export const checkJsonDataStream = (
  result: CommandResult,
  expectedKeys: readonly string[],
): Effect.Effect<readonly unknown[], TckError> =>
  Effect.gen(function* () {
    if (result.exitCode !== 0)
      return yield* refuse(
        `${result.label} exited ${result.exitCode}`,
        result.stderr,
      );
    const lines = result.stdout.split("\n").filter((line) => line.trim());
    if (lines.length === 0)
      return yield* refuse(`${result.label} produced no data on stdout`);
    const rows: unknown[] = [];
    for (const line of lines) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        return yield* refuse(
          `${result.label} wrote a non-JSON line to its data stream`,
          line,
        );
      }
      if (typeof parsed !== "object" || parsed === null)
        return yield* refuse(
          `${result.label} wrote a non-object data row`,
          line,
        );
      for (const key of expectedKeys)
        if (!(key in parsed))
          return yield* refuse(
            `${result.label} data row is missing ${key}`,
            line,
          );
      rows.push(parsed);
    }
    return rows as readonly unknown[];
  });
