// CLI and telemetry qualification.
//
// Two groups of cases share this file because they share one constraint from
// the roadmap: "Use a local collector that records received payloads. Finding a
// log line alone is insufficient evidence of telemetry correctness." The
// telemetry cases therefore assert on spans and log records that a recording
// OTLP collector decoded, or on Parquet files read back out of the bucket, and
// the CLI cases assert on exit status, the documented stream split, and
// resulting on-disk or namespace state rather than on console output alone.
//
// The v0.5.0 CLI and telemetry inventory these rest on, and the bullets that
// are not testable on this release, are in docs/CLI-TELEMETRY.md.
import { Effect, Schema } from "effect";
import { HttpClient } from "effect/unstable/http";
import { decodeJson } from "./Artifacts.js";
import { publishedPort } from "./Compose.js";
import { TckError, type Target } from "./Domain.js";
import type { Node } from "./FleetControls.js";
import { equal } from "./Oracle.js";
import { Processes } from "./Processes.js";
import {
  acknowledgedBatch,
  type QualificationContext,
} from "./QualificationContext.js";
import { readParquetFooter } from "./TelemetryParquet.js";
import {
  CollectorState,
  COMMAND_WRAPPER,
  SCHEMA_VERSION,
  checkCommandAccepted,
  checkCommandFailed,
  checkCommandRejected,
  checkExportEnvelope,
  checkIndependentTrace,
  checkJsonDataStream,
  checkLogCorrelation,
  checkNotRecorded,
  checkOutageObserved,
  checkOutboundHeader,
  checkParquetSchema,
  checkResultsUnchanged,
  checkRetryEvidence,
  checkSpanChain,
  checkTelemetryLayout,
  checkTelemetryMetadata,
  parseCommandOutput,
  traceOfMarker,
  traceparent,
  type CommandResult,
} from "./TelemetryOracles.js";

/** OTEL_SERVICE_NAME in infra/telemetry.yaml. */
const SERVICE_NAME = "tck-telemetry";
/** The node infra/telemetry.yaml gives a zero-ratio parent-based sampler. */
const UNSAMPLED: Node = "celld3";
const SAMPLED: Node = "celld";
/** The fixture's KV namespace id and D1 database name, from its wrangler config. */
const KV_NAMESPACE = "tck-kv";
const D1_DATABASE = "tck-db";
/** The Worker route the fixture is allowed to call outbound. */
const SINK = "http://celld:8080/trace/sink";

/**
 * Runs a case with the harness's own trace-context propagation turned off.
 *
 * Effect's HTTP client stamps a `traceparent` for the ambient span onto every
 * outgoing request, which would overwrite the headers these cases send and
 * would give every probe a remote parent — making a sampling decision for a
 * *root* request impossible to observe. Disabling it for this suite only means
 * the trace context celld sees is exactly the one the case put there, or none.
 */
const untraced = <A, E, R>(work: Effect.Effect<A, E, R>) =>
  Effect.provideService(work, HttpClient.TracerPropagationEnabled, false);

// ---------------------------------------------------------------------------
// Running celld inside the fleet
// ---------------------------------------------------------------------------

/**
 * Runs one `celld` invocation in the tool container and returns its real exit
 * status and both streams.
 *
 * `compose run` fails the harness process on a non-zero exit and keeps only
 * stderr, which is unusable for testing documented error paths. The wrapper
 * captures each stream separately and reports the status on a single line, so
 * the container always exits 0 and the classification happens in the oracle.
 */
const celld = (
  ctx: QualificationContext,
  label: string,
  args: readonly string[],
  options: { readonly env?: Readonly<Record<string, string>> } = {},
) =>
  Effect.gen(function* () {
    const output = yield* ctx.runtime.controls.compose([
      "run",
      "--rm",
      "-T",
      ...Object.entries(options.env ?? {}).flatMap(([name, value]) => [
        "--env",
        `${name}=${value}`,
      ]),
      "--entrypoint",
      "/bin/sh",
      "tool",
      "-c",
      COMMAND_WRAPPER,
      label,
      ...args,
    ]);
    const result = yield* parseCommandOutput(label, output.stdout);
    yield* ctx.artifacts.text(
      "cli.jsonl",
      JSON.stringify({ args, ...result }) + "\n",
      true,
    );
    return result;
  });

// ---------------------------------------------------------------------------
// Collector control
// ---------------------------------------------------------------------------

const collector = (ctx: QualificationContext) =>
  ctx.runtime.controls.collector();

const collectorState = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const endpoint = yield* collector(ctx);
    const response = yield* ctx.transport.request(endpoint, { path: "/" });
    yield* equal(response.status, 200);
    return yield* Schema.decodeUnknownEffect(CollectorState)(
      response.body,
    ).pipe(
      Effect.mapError(
        (error) =>
          new TckError({
            phase: "telemetry",
            message: "The collector returned an undecodable state",
            detail: String(error),
          }),
      ),
    );
  });

const collectorControl = (ctx: QualificationContext, path: string) =>
  Effect.gen(function* () {
    const endpoint = yield* collector(ctx);
    const response = yield* ctx.transport.request(endpoint, { path });
    yield* equal(response.status, 200);
  });

/** Installs a collector fault for a bounded window; it expires on its own. */
const collectorMode = (ctx: QualificationContext, mode: string, ms: number) =>
  collectorControl(ctx, `/mode?mode=${mode}&ms=${ms}`);

const collectorReset = (ctx: QualificationContext) =>
  collectorControl(ctx, "/reset");

/** Waits for the exporter to flush; the overlay sets a two-second interval. */
const settled = (
  ctx: QualificationContext,
  done: (state: CollectorState) => boolean,
) => ctx.poll(collectorState(ctx), done);

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

const unique = (ctx: QualificationContext, label: string) =>
  `tck-${label}-${ctx.name}`;

/** A run-unique, well-formed trace id derived from the case name. */
const traceIdFor = (ctx: QualificationContext, salt: number) => {
  let hash = 0x811c9dc5 ^ salt;
  for (const character of ctx.name) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return Array.from({ length: 4 }, (_, index) => {
    const mixed = Math.imul(hash + index * 0x9e3779b9, 0x85ebca6b) >>> 0;
    return mixed.toString(16).padStart(8, "0");
  }).join("");
};

const probe = (
  ctx: QualificationContext,
  node: Node,
  path: string,
  headers: Readonly<Record<string, string>> = {},
) =>
  ctx.transport
    .request(ctx.targets[node], { path, headers })
    .pipe(Effect.tap((response) => equal(response.status, 200)));

const Chain = Schema.Struct({
  downstream: Schema.Struct({
    traceparent: Schema.NullOr(Schema.String),
  }),
  status: Schema.Int,
});

/** Drives the Worker → Durable Object → outbound chain and reports the echo. */
const chain = (
  ctx: QualificationContext,
  node: Node,
  marker: string,
  headers: Readonly<Record<string, string>>,
) =>
  probe(
    ctx,
    node,
    `/trace/chain?name=${ctx.name}&marker=${marker}&sink=${encodeURIComponent(SINK)}`,
    headers,
  ).pipe(
    Effect.flatMap((response) =>
      Schema.decodeUnknownEffect(Chain)(response.body).pipe(
        Effect.mapError(
          (error) =>
            new TckError({
              phase: "telemetry",
              message: "The trace chain route returned an unexpected body",
              detail: String(error),
            }),
        ),
      ),
    ),
  );

// ---------------------------------------------------------------------------
// telemetry.trace-context
// ---------------------------------------------------------------------------

const traceContext = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 3);
    yield* collectorReset(ctx);
    const traceId = traceIdFor(ctx, 1);
    const remoteParent = "a1b2c3d4e5f60718";
    const marker = unique(ctx, "chain");
    const malformedMarker = unique(ctx, "malformed");
    const sampledMarker = unique(ctx, "sampled");
    const unsampledMarker = unique(ctx, "unsampled");
    const MALFORMED = "00-not-a-traceparent";

    const chained = yield* chain(ctx, SAMPLED, marker, {
      traceparent: traceparent(traceId, remoteParent),
    });
    // A malformed header must start a new trace rather than fail the request.
    yield* probe(ctx, SAMPLED, `/trace/emit?marker=${malformedMarker}`, {
      traceparent: MALFORMED,
    });
    // Sampling control and its negative, both Worker-local so the decision is
    // made on the node the request was sent to.
    yield* probe(ctx, SAMPLED, `/trace/emit?marker=${sampledMarker}`);
    yield* probe(ctx, UNSAMPLED, `/trace/emit?marker=${unsampledMarker}`);

    const state = yield* settled(ctx, (observed) =>
      [
        `${marker}:worker`,
        `${marker}:cell`,
        `${marker}:cell-await`,
        malformedMarker,
        sampledMarker,
      ].every((body) => observed.logs.some((log) => log.body === body)),
    );
    yield* ctx.artifacts.json("collector-state.json", state);
    yield* ctx.artifacts.json("chain-response.json", chained);

    // The whole chain must sit on the incoming trace, each span parented by the
    // previous one, and the outbound span must carry the call it made.
    const spans = yield* checkSpanChain(state.spans, {
      traceId,
      rootParentSpanId: remoteParent,
      steps: [
        { name: "celld.fetch" },
        { name: "celld.cell_fetch" },
        {
          name: "fetch",
          attributes: { "url.full": SINK, "http.response.status_code": 200 },
        },
      ],
    });
    const [worker, cell, outbound] = spans as unknown as [
      (typeof spans)[number],
      (typeof spans)[number],
      (typeof spans)[number],
    ];
    // Log correlation, including across the fixture's await: every marker must
    // carry the trace and the span of the handler that wrote it.
    yield* checkLogCorrelation(state.logs, [
      { body: `${marker}:worker`, traceId, spanId: worker.spanId },
      { body: `${marker}:cell`, traceId, spanId: cell.spanId },
      { body: `${marker}:cell-await`, traceId, spanId: cell.spanId },
    ]);
    // Independent evidence of the outbound propagation: what the downstream
    // fixture actually received, checked against the recorded outbound span.
    yield* checkOutboundHeader(chained.downstream.traceparent, {
      traceId,
      outboundSpanId: outbound.spanId,
      callerSpanId: cell.spanId,
    });
    const malformed = yield* traceOfMarker(state.logs, malformedMarker);
    yield* checkIndependentTrace(malformed, state.spans, [traceId, MALFORMED]);
    // Sampling: the always-on node records the control, the zero-ratio node
    // records nothing at all for a request that starts on it.
    yield* traceOfMarker(state.logs, sampledMarker);
    yield* checkNotRecorded(state, unsampledMarker);
    yield* ctx.verify();
    return {
      traceId,
      chain: spans.map((span) => ({ name: span.name, spanId: span.spanId })),
      malformedTraceId: malformed.traceId,
    };
  });

// ---------------------------------------------------------------------------
// telemetry.export-isolation
// ---------------------------------------------------------------------------

/** The window the collector spends rejecting batches, in milliseconds. */
const OUTAGE_MS = 25000;

const exportIsolation = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 3);
    yield* collectorReset(ctx);
    const marker = unique(ctx, "envelope");
    yield* probe(ctx, SAMPLED, `/trace/emit?marker=${marker}`);
    const before = yield* settled(ctx, (observed) =>
      observed.logs.some((log) => log.body === marker),
    );
    yield* ctx.artifacts.json("envelope-state.json", before);
    // celld selects OTLP/HTTP protobuf for a collector URL and names the
    // service in the exported resource.
    yield* checkExportEnvelope(before, {
      serviceName: SERVICE_NAME,
      signals: ["traces", "logs"],
    });

    // Failure isolation: application results must not move while the collector
    // is failing, and the exporter must retry rather than give up silently.
    const baseline = yield* ctx.verify();
    yield* collectorMode(ctx, "transient", OUTAGE_MS);
    yield* acknowledgedBatch(ctx, 6);
    const outageMarker = unique(ctx, "outage");
    yield* probe(ctx, SAMPLED, `/trace/emit?marker=${outageMarker}`);
    const during = yield* ctx.state();
    // Wait for the injected window to expire before reading retry evidence.
    yield* settled(ctx, (observed) =>
      observed.deliveries.some(
        (delivery) => delivery.mode === "transient" && delivery.status === 503,
      ),
    );
    const after = yield* settled(
      ctx,
      (observed) =>
        Date.now() > observed.until &&
        observed.deliveries.filter((delivery) => delivery.mode === "transient")
          .length > 0,
    );
    yield* ctx.artifacts.json("outage-state.json", after);
    yield* checkOutageObserved(after.deliveries, "transient");
    const retry = yield* checkRetryEvidence(after.deliveries);
    // The application is the thing that must not have changed.
    const recovered = yield* ctx.verify();
    yield* checkResultsUnchanged(
      "collector outage",
      during,
      yield* ctx.state(),
    );
    if (recovered.acknowledged < baseline.acknowledged)
      return yield* Effect.fail(
        new TckError({
          phase: "telemetry",
          message: "Acknowledged writes were lost across a collector outage",
          detail: `${baseline.acknowledged} -> ${recovered.acknowledged}`,
        }),
      );
    return { retry, acknowledged: recovered.acknowledged };
  });

// ---------------------------------------------------------------------------
// telemetry.parquet-export
// ---------------------------------------------------------------------------

/** Columns the published DuckDB queries read, so they are part of the contract. */
const TRACE_COLUMNS = [
  "trace_id",
  "span_id",
  "parent_span_id",
  "name",
  "start_unix_us",
  "duration_us",
] as const;
const LOG_COLUMNS = ["trace_id", "span_id", "body"] as const;

const McStat = Schema.Struct({
  status: Schema.String,
  size: Schema.Int,
  metadata: Schema.Record(Schema.String, Schema.String),
});

const mc = (ctx: QualificationContext, args: readonly string[]) =>
  ctx.runtime.controls.compose([
    "run",
    "--no-deps",
    "--rm",
    "-T",
    "--entrypoint",
    "mc",
    "storage",
    ...args,
  ]);

const parquetExport = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 6);
    const marker = unique(ctx, "parquet");
    yield* probe(ctx, SAMPLED, `/trace/emit?marker=${marker}`);
    // The overlay flushes every five seconds; poll the listing rather than
    // sleeping for a fixed interval.
    // Searched from the bucket root, not the telemetry prefix: mc fails
    // outright on a prefix that does not exist yet, and before the first flush
    // it does not. Anything Parquet that is *not* under the documented layout
    // then reaches the oracle and fails there, rather than being filtered away.
    const listing = yield* ctx.poll(
      mc(ctx, ["find", "local/tck", "--name", "*.parquet"]).pipe(
        Effect.map((output) =>
          output.stdout
            .split("\n")
            .map((line) => line.trim())
            .filter(Boolean),
        ),
      ),
      (keys) =>
        keys.some((key) => key.includes("/traces/")) &&
        keys.some((key) => key.includes("/logs/")),
    );
    yield* ctx.artifacts.json("telemetry-objects.json", listing);
    const objects = yield* checkTelemetryLayout(listing, ["traces", "logs"]);

    const inspected: unknown[] = [];
    for (const signal of ["traces", "logs"] as const) {
      const index = objects.findIndex((object) => object.signal === signal);
      const key = listing[index]!;
      const stat = yield* decodeJson(
        McStat,
        (yield* mc(ctx, ["stat", "--json", key])).stdout,
      );
      // The schema version travels in the object's own metadata.
      yield* checkTelemetryMetadata(stat.metadata);
      // Read the file itself: the declared columns and the row count come from
      // the Parquet footer, not from the listing.
      const base64 = (yield* ctx.runtime.controls.compose([
        "run",
        "--no-deps",
        "--rm",
        "-T",
        "--entrypoint",
        "/bin/sh",
        "storage",
        "-ec",
        'mc cat "$1" | base64 -w0',
        "read-parquet",
        key,
      ])).stdout.trim();
      const bytes = Buffer.from(base64, "base64");
      const footer = yield* Effect.try({
        try: () => readParquetFooter(bytes),
        catch: (error) =>
          new TckError({
            phase: "telemetry",
            message: `Could not read the ${signal} Parquet footer`,
            detail: String(error),
          }),
      });
      yield* checkParquetSchema(
        footer,
        signal === "traces" ? TRACE_COLUMNS : LOG_COLUMNS,
      );
      inspected.push({ signal, key, size: stat.size, footer });
    }
    yield* ctx.artifacts.json("parquet-footers.json", inspected);
    yield* ctx.verify();
    return { objects: objects.length, schema: SCHEMA_VERSION, inspected };
  });

// ---------------------------------------------------------------------------
// telemetry.cli-dev
// ---------------------------------------------------------------------------

const Receipt = Schema.Struct({ seq: Schema.Int, id: Schema.String });
const Rows = Schema.Array(
  Schema.Struct({ seq: Schema.Int, id: Schema.String, payload: Schema.String }),
);

const devTarget = (ctx: QualificationContext) =>
  publishedPort(
    ctx.runtime.controls.compose,
    "dev",
    "9876",
    "telemetry",
    (value) => `Unexpected celld dev address: ${value}`,
  ).pipe(
    Effect.map((address): Target => ({
      name: "celld-dev",
      baseUrl: `http://${address}`,
    })),
  );

const devReady = (ctx: QualificationContext, target: Target) =>
  ctx.poll(
    Effect.exit(
      ctx.transport.request(target, { path: "/deployment/revision" }),
    ),
    (exit) => exit._tag === "Success" && exit.value.status === 200,
  );

const cliDev = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const compose = ctx.runtime.controls.compose;
    const processes = yield* Processes;
    // A recorded baseline on the fleet, so the closing ctx.verify() proves the
    // fleet the dev node ran beside was untouched by any of it.
    yield* acknowledgedBatch(ctx, 3);

    // Invalid configuration, before anything is started. Each of these is a
    // documented rejection, and each must fail without emitting a result.
    const rejections = {
      // `celld dev --port` takes a port number.
      port: yield* celld(ctx, "dev-invalid-port", [
        "dev",
        "/work",
        "--port",
        "not-a-port",
      ]),
      // A project directory with no Wrangler configuration.
      project: yield* celld(ctx, "dev-missing-project", [
        "dev",
        "/tck-no-such-project",
      ]),
      // "CELLD_OTEL_SINK is removed. Remove this setting before starting the node."
      removed: yield* celld(ctx, "removed-setting", ["--bucket", "s3://tck"], {
        env: { CELLD_OTEL_SINK: "bucket" },
      }),
      // "Boolean variables accept only `0` or `1`; invalid values stop startup."
      boolean: yield* celld(ctx, "invalid-boolean", ["--bucket", "s3://tck"], {
        env: { CELLD_TRUST_FORWARDED_HEADERS: "yes" },
      }),
      // "a non-loopback address requires --internal-listen"
      listener: yield* celld(ctx, "non-loopback-listen", [
        "--bucket",
        "s3://tck",
        "--listen",
        "0.0.0.0:8099",
      ]),
      unknown: yield* celld(ctx, "unknown-command", ["tck-not-a-command"]),
    } satisfies Record<string, CommandResult>;
    yield* checkCommandRejected(rejections.unknown, {
      exitCode: 1,
      stderrIncludes: ["unknown command or option: tck-not-a-command"],
      stdoutEmpty: true,
    });
    yield* checkCommandFailed(rejections.port);
    yield* checkCommandFailed(rejections.project);
    yield* checkCommandFailed(rejections.removed, {
      stderrIncludes: ["CELLD_OTEL_SINK"],
    });
    yield* checkCommandFailed(rejections.boolean);
    yield* checkCommandFailed(rejections.listener, {
      stderrIncludes: ["--internal-listen"],
    });

    // Startup on the documented default listener: nothing passes --port, so
    // the published container port 9876 is the default being exercised.
    yield* compose(["up", "-d", "dev"]);
    const target = yield* devTarget(ctx);
    yield* devReady(ctx, target);
    const revision = yield* ctx.transport.request(target, {
      path: "/deployment/revision",
    });
    yield* equal(revision.status, 200);

    // The operator listener stays on loopback, so it is not reachable through
    // the Worker listener even though --host exposed that one.
    const operator = yield* ctx.transport.request(target, { path: "/state" });
    yield* equal(operator.status, 404);

    // Local persistence: write through the dev node's own storage.
    const id = `${ctx.name}-dev`;
    const written = yield* ctx.transport.request(target, {
      path: `/history/write?name=${ctx.name}-dev`,
      method: "POST",
      body: JSON.stringify({ id, payload: "celld-dev-persistence" }),
      headers: { "content-type": "application/json" },
    });
    yield* equal(written.status, 200);
    const receipt = yield* Schema.decodeUnknownEffect(Receipt)(written.body);

    // Shutdown: a normal stop must exit cleanly and keep the local state.
    yield* compose(["stop", "--timeout", "40", "dev"]);
    const stoppedId = (yield* compose([
      "ps",
      "--all",
      "-q",
      "dev",
    ])).stdout.trim();
    const stopped = yield* decodeJson(
      Schema.Array(
        Schema.Struct({
          State: Schema.Struct({
            Running: Schema.Boolean,
            ExitCode: Schema.Number,
          }),
        }),
      ),
      (yield* processes.run("docker", ["inspect", stoppedId])).stdout,
    );
    yield* ctx.artifacts.json("dev-shutdown.json", stopped);
    yield* equal(
      {
        running: stopped[0]?.State.Running,
        exitCode: stopped[0]?.State.ExitCode,
      },
      { running: false, exitCode: 0 },
    );

    // The next invocation uses the same durable state.
    yield* compose(["start", "dev"]);
    const restarted = yield* devTarget(ctx);
    yield* devReady(ctx, restarted);
    const rows = yield* Schema.decodeUnknownEffect(Rows)(
      (yield* ctx.transport.request(restarted, {
        path: `/history/state?name=${ctx.name}-dev`,
      })).body,
    );
    yield* ctx.artifacts.json("dev-persistence.json", { receipt, rows });
    yield* equal(
      rows.map((row) => ({ seq: row.seq, id: row.id, payload: row.payload })),
      [{ seq: receipt.seq, id, payload: "celld-dev-persistence" }],
    );

    // The fleet the case ran beside is untouched by any of it.
    yield* ctx.verify();
    return { receipt, rejections: Object.keys(rejections).length };
  });

// ---------------------------------------------------------------------------
// telemetry.cli-operator
// ---------------------------------------------------------------------------

const cliOperator = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 3);
    const key = `${ctx.name}-operator`;
    const value = `operator-value-${ctx.seed}`;
    const second = `${key}-b`;

    // Supported operations, in an order that leaves the namespace as it began.
    yield* checkCommandAccepted(
      yield* celld(ctx, "kv-put", ["kv", "put", KV_NAMESPACE, key, value]),
    );
    yield* checkCommandAccepted(
      yield* celld(ctx, "kv-put-second", [
        "kv",
        "put",
        KV_NAMESPACE,
        second,
        value,
      ]),
    );
    // `get` writes the value to stdout as bytes, byte for byte.
    yield* checkCommandAccepted(
      yield* celld(ctx, "kv-get", ["kv", "get", KV_NAMESPACE, key]),
      { stdout: value },
    );
    yield* checkCommandAccepted(
      yield* celld(ctx, "kv-info", ["kv", "info", KV_NAMESPACE]),
      { stdoutIncludes: ["keys:", "bytes:", "stored:"] },
    );
    // `info` reports live keys, their bytes, and stored rows.
    // One JSON object per key on the data stream, and nothing else on it.
    const listed = yield* celld(ctx, "kv-list-json", [
      "kv",
      "list",
      KV_NAMESPACE,
      "--prefix",
      ctx.name,
      "--json",
    ]);
    const rows = yield* checkJsonDataStream(listed, ["name"]);
    if (rows.length < 2)
      return yield* Effect.fail(
        new TckError({
          phase: "telemetry",
          message: "celld kv list did not return both written keys",
          detail: listed.stdout,
        }),
      );
    // A truncated listing reports the continuation on stderr, not on stdout.
    const truncated = yield* celld(ctx, "kv-list-truncated", [
      "kv",
      "list",
      KV_NAMESPACE,
      "--prefix",
      ctx.name,
      "--json",
      "--limit",
      "1",
    ]);
    yield* checkCommandAccepted(truncated, { stderrIncludes: ["--after"] });
    yield* checkJsonDataStream(truncated, ["name"]);

    yield* checkCommandAccepted(
      yield* celld(ctx, "d1-execute", [
        "d1",
        "execute",
        D1_DATABASE,
        "--command",
        "SELECT 1 AS probe",
        "/fixture",
      ]),
    );

    // Error paths. v0.5.0 documents the first message verbatim; the rest are
    // required to fail and to keep the documented stream split.
    yield* checkCommandRejected(
      yield* celld(ctx, "kv-missing-namespace", ["kv", "get"]),
      {
        exitCode: 1,
        stderrIncludes: ["celld kv needs a namespace id"],
        stdoutEmpty: true,
      },
    );
    yield* checkCommandFailed(
      yield* celld(ctx, "kv-unknown-namespace", [
        "kv",
        "get",
        "tck-no-such-namespace",
        key,
      ]),
      // v0.5.0 reports an unknown namespace as an absent key rather than as an
      // unknown namespace; docs/CLI-TELEMETRY.md records that wording.
      { stderrIncludes: ['no key "', 'in namespace "tck-no-such-namespace"'] },
    );
    yield* checkCommandFailed(
      yield* celld(ctx, "kv-missing-key", [
        "kv",
        "get",
        KV_NAMESPACE,
        `${key}-absent`,
      ]),
      {
        stderrIncludes: [
          `no key "${key}-absent" in namespace "${KV_NAMESPACE}"`,
        ],
      },
    );
    yield* checkCommandFailed(
      yield* celld(ctx, "d1-unknown-database", [
        "d1",
        "execute",
        "tck-no-such-database",
        "--command",
        "SELECT 1",
        "/fixture",
      ]),
      {
        stderrIncludes: [
          'the project declares no D1 database named "tck-no-such-database"',
        ],
      },
    );
    yield* checkCommandFailed(
      yield* celld(ctx, "d1-missing-statement", [
        "d1",
        "execute",
        D1_DATABASE,
        "/fixture",
      ]),
      {
        stderrIncludes: [
          "celld d1 execute requires exactly one of --command or --file",
        ],
      },
    );

    // A delete is observable through the same read path that saw the value.
    yield* checkCommandAccepted(
      yield* celld(ctx, "kv-delete", [
        "kv",
        "delete",
        KV_NAMESPACE,
        key,
        second,
      ]),
    );
    yield* checkCommandFailed(
      yield* celld(ctx, "kv-get-after-delete", [
        "kv",
        "get",
        KV_NAMESPACE,
        key,
      ]),
      { stderrIncludes: [`no key "${key}" in namespace "${KV_NAMESPACE}"`] },
    );
    // Operator traffic must not have disturbed the application's own history.
    yield* ctx.verify();
    return { key, rows: rows.length };
  });

export const telemetryCases = [
  {
    id: "telemetry.cli-dev" as const,
    telemetry: "cli" as const,
    run: (ctx: QualificationContext) => untraced(cliDev(ctx)),
  },
  {
    id: "telemetry.cli-operator" as const,
    telemetry: "cli" as const,
    run: (ctx: QualificationContext) => untraced(cliOperator(ctx)),
  },
  {
    id: "telemetry.trace-context" as const,
    telemetry: "otlp" as const,
    run: (ctx: QualificationContext) => untraced(traceContext(ctx)),
  },
  {
    id: "telemetry.export-isolation" as const,
    telemetry: "otlp" as const,
    run: (ctx: QualificationContext) => untraced(exportIsolation(ctx)),
  },
  {
    id: "telemetry.parquet-export" as const,
    telemetry: "bucket" as const,
    run: (ctx: QualificationContext) => untraced(parquetExport(ctx)),
  },
];
