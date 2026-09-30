import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import type { DecodedLog, DecodedSpan } from "../src/TelemetryProtobuf.js";
import {
  COMMAND_MARKER,
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
  parseTelemetryKey,
  traceOfMarker,
  traceparent,
  type CollectorState,
  type Delivery,
} from "../src/TelemetryOracles.js";

const fails = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.map(Effect.exit(effect), (exit) => exit._tag === "Failure");

const TRACE = "11112222333344445555666677778888";
const REMOTE = "a1b2c3d4e5f60718";
const WORKER = "1111111111111111";
const CELL = "2222222222222222";
const OUTBOUND = "3333333333333333";
const SINK = "http://celld:8080/trace/sink";

const span = (overrides: Partial<DecodedSpan> = {}): DecodedSpan => ({
  traceId: TRACE,
  spanId: WORKER,
  parentSpanId: REMOTE,
  name: "celld.fetch",
  kind: 2,
  startUnixNano: "1000",
  endUnixNano: "2000",
  attributes: {},
  ...overrides,
});

/** The chain a correct Worker → Durable Object → outbound hop produces. */
const chain: DecodedSpan[] = [
  span(),
  span({ spanId: CELL, parentSpanId: WORKER, name: "celld.cell_fetch" }),
  span({
    spanId: OUTBOUND,
    parentSpanId: CELL,
    name: "fetch",
    kind: 3,
    attributes: { "url.full": SINK, "http.response.status_code": 200 },
  }),
];

const STEPS = [
  { name: "celld.fetch" },
  { name: "celld.cell_fetch" },
  {
    name: "fetch",
    attributes: { "url.full": SINK, "http.response.status_code": 200 },
  },
] as const;

const walk = (spans: readonly DecodedSpan[], traceId = TRACE) =>
  checkSpanChain(spans, {
    traceId,
    rootParentSpanId: REMOTE,
    steps: [...STEPS],
  });

it.effect(
  "a span chain requires a real parent link and one unbroken trace id",
  () =>
    Effect.gen(function* () {
      expect((yield* walk(chain)).map((value) => value.spanId)).toEqual([
        WORKER,
        CELL,
        OUTBOUND,
      ]);
      // A broken parent link: the object span no longer hangs off the Worker.
      expect(
        yield* fails(
          walk([
            chain[0]!,
            { ...chain[1]!, parentSpanId: "9999999999999999" },
            chain[2]!,
          ]),
        ),
      ).toBe(true);
      // A trace id that changed across the Durable Object hop.
      expect(
        yield* fails(
          walk([
            chain[0]!,
            { ...chain[1]!, traceId: "99998888777766665555444433332222" },
            chain[2]!,
          ]),
        ),
      ).toBe(true);
      // The outbound hop left the trace, even though its parent still matches.
      expect(
        yield* fails(
          walk([
            chain[0]!,
            chain[1]!,
            { ...chain[2]!, traceId: "99998888777766665555444433332222" },
          ]),
        ),
      ).toBe(true);
      // The Worker span attached to no remote parent at all.
      expect(
        yield* fails(
          walk([{ ...chain[0]!, parentSpanId: "" }, ...chain.slice(1)]),
        ),
      ).toBe(true);
      // A missing step is not a pass.
      expect(yield* fails(walk([chain[0]!, chain[2]!]))).toBe(true);
      expect(yield* fails(walk([]))).toBe(true);
      // An ambiguous step must not resolve to whichever span came first.
      expect(
        yield* fails(
          walk([
            chain[0]!,
            chain[1]!,
            { ...chain[1]!, spanId: "4444444444444444" },
            chain[2]!,
          ]),
        ),
      ).toBe(true);
      // The outbound span recorded a different call, or a failed one.
      expect(
        yield* fails(
          walk([
            ...chain.slice(0, 2),
            {
              ...chain[2]!,
              attributes: {
                "url.full": "http://elsewhere/trace/sink",
                "http.response.status_code": 200,
              },
            },
          ]),
        ),
      ).toBe(true);
      expect(
        yield* fails(
          walk([
            ...chain.slice(0, 2),
            {
              ...chain[2]!,
              attributes: {
                "url.full": SINK,
                "http.response.status_code": 502,
              },
            },
          ]),
        ),
      ).toBe(true);
      // The anchor itself has to be a trace id.
      expect(yield* fails(walk(chain, "not-a-trace-id"))).toBe(true);
      expect(yield* fails(walk(chain, "0".repeat(32)))).toBe(true);
    }),
);

const log = (overrides: Partial<DecodedLog> = {}): DecodedLog => ({
  traceId: TRACE,
  spanId: WORKER,
  body: "marker:worker",
  severityNumber: 9,
  timeUnixNano: "1500",
  attributes: {},
  ...overrides,
});

const logs = [
  log(),
  log({ spanId: CELL, body: "marker:cell" }),
  log({ spanId: CELL, body: "marker:cell-await" }),
];

it.effect("log correlation must hold across the await, not merely exist", () =>
  Effect.gen(function* () {
    const expected = [
      { body: "marker:worker", traceId: TRACE, spanId: WORKER },
      { body: "marker:cell", traceId: TRACE, spanId: CELL },
      { body: "marker:cell-await", traceId: TRACE, spanId: CELL },
    ];
    yield* checkLogCorrelation(logs, expected);
    // The line after the await lost its span: this is the failure the case
    // exists to catch, and it must not pass merely because the line is there.
    expect(
      yield* fails(
        checkLogCorrelation(
          [logs[0]!, logs[1]!, { ...logs[2]!, spanId: "0000000000000001" }],
          expected,
        ),
      ),
    ).toBe(true);
    // The line after the await landed in another trace.
    expect(
      yield* fails(
        checkLogCorrelation(
          [
            logs[0]!,
            logs[1]!,
            { ...logs[2]!, traceId: "99998888777766665555444433332222" },
          ],
          expected,
        ),
      ),
    ).toBe(true);
    // A missing or duplicated record never resolves.
    expect(yield* fails(checkLogCorrelation(logs.slice(0, 2), expected))).toBe(
      true,
    );
    expect(
      yield* fails(checkLogCorrelation([...logs, logs[2]!], expected)),
    ).toBe(true);
  }),
);

it.effect(
  "a marker resolves a request's trace only when it is unambiguous",
  () =>
    Effect.gen(function* () {
      expect(yield* traceOfMarker(logs, "marker:cell")).toEqual({
        traceId: TRACE,
        spanId: CELL,
      });
      expect(yield* fails(traceOfMarker(logs, "marker:absent"))).toBe(true);
      expect(
        yield* fails(traceOfMarker([...logs, logs[1]!], "marker:cell")),
      ).toBe(true);
      // A record with no usable context must not resolve to a zeroed trace.
      expect(
        yield* fails(
          traceOfMarker(
            [
              log({
                body: "marker:bare",
                traceId: "0".repeat(32),
                spanId: "0".repeat(16),
              }),
            ],
            "marker:bare",
          ),
        ),
      ).toBe(true);
    }),
);

it.effect("a malformed traceparent must start a genuinely new trace", () =>
  Effect.gen(function* () {
    const fresh = "abcdabcdabcdabcdabcdabcdabcdabcd";
    const root = span({ traceId: fresh, spanId: WORKER, parentSpanId: "" });
    yield* checkIndependentTrace(
      { traceId: fresh, spanId: WORKER },
      [root],
      [TRACE, "00-not-a-traceparent"],
    );
    // The rejected trace id was carried in anyway.
    expect(
      yield* fails(
        checkIndependentTrace(
          { traceId: TRACE, spanId: WORKER },
          [span({ parentSpanId: "" })],
          [TRACE],
        ),
      ),
    ).toBe(true);
    // A remote parent survived the rejection.
    expect(
      yield* fails(
        checkIndependentTrace(
          { traceId: fresh, spanId: WORKER },
          [span({ traceId: fresh, parentSpanId: REMOTE })],
          [TRACE],
        ),
      ),
    ).toBe(true);
    // The new trace id is not well formed.
    expect(
      yield* fails(
        checkIndependentTrace(
          { traceId: "0".repeat(32), spanId: WORKER },
          [root],
          [],
        ),
      ),
    ).toBe(true);
    // No span at all was recorded for the request.
    expect(
      yield* fails(
        checkIndependentTrace({ traceId: fresh, spanId: WORKER }, [], []),
      ),
    ).toBe(true);
  }),
);

it.effect("an unsampled request must record nothing at all", () =>
  Effect.gen(function* () {
    yield* checkNotRecorded({ logs, spans: chain }, "marker:absent");
    // A log the sampler should have dropped.
    expect(
      yield* fails(checkNotRecorded({ logs, spans: chain }, "marker:cell")),
    ).toBe(true);
  }),
);

it.effect("the outbound header must match the recorded outbound span", () =>
  Effect.gen(function* () {
    const options = {
      traceId: TRACE,
      outboundSpanId: OUTBOUND,
      callerSpanId: CELL,
    };
    yield* checkOutboundHeader(traceparent(TRACE, OUTBOUND), options);
    // Nothing propagated.
    expect(yield* fails(checkOutboundHeader(null, options))).toBe(true);
    // The trace id changed on the way out.
    expect(
      yield* fails(
        checkOutboundHeader(
          traceparent("99998888777766665555444433332222", OUTBOUND),
          options,
        ),
      ),
    ).toBe(true);
    // The caller's own span id was reused instead of a new one.
    expect(
      yield* fails(checkOutboundHeader(traceparent(TRACE, CELL), options)),
    ).toBe(true);
    // A span id that is not the one the collector recorded.
    expect(
      yield* fails(
        checkOutboundHeader(traceparent(TRACE, "9999999999999999"), options),
      ),
    ).toBe(true);
    // Not a W3C header at all.
    expect(yield* fails(checkOutboundHeader("garbage", options))).toBe(true);
    expect(
      yield* fails(checkOutboundHeader(`01-${TRACE}-${OUTBOUND}-01`, options)),
    ).toBe(true);
  }),
);

const delivery = (overrides: Partial<Delivery> = {}): Delivery => ({
  sequence: 1,
  signal: "traces",
  contentType: "application/x-protobuf",
  bytes: 512,
  digest: "a".repeat(64),
  mode: "normal",
  status: 200,
  receivedAtMs: 0,
  spanCount: 3,
  logCount: 0,
  ...overrides,
});

const state = (overrides: Partial<CollectorState> = {}): CollectorState => ({
  control: { mode: "normal", durationMs: 0 },
  until: 0,
  resources: {
    traces: { "service.name": "tck-telemetry" },
    logs: { "service.name": "tck-telemetry" },
  },
  deliveries: [
    delivery(),
    delivery({ sequence: 2, signal: "logs", logCount: 3 }),
  ],
  spans: chain,
  logs,
  ...overrides,
});

it.effect(
  "the export envelope must actually be OTLP protobuf for this service",
  () =>
    Effect.gen(function* () {
      const signals = ["traces", "logs"] as const;
      yield* checkExportEnvelope(state(), {
        serviceName: "tck-telemetry",
        signals: [...signals],
      });
      // A signal that never arrived.
      expect(
        yield* fails(
          checkExportEnvelope(state({ deliveries: [delivery()] }), {
            serviceName: "tck-telemetry",
            signals: [...signals],
          }),
        ),
      ).toBe(true);
      // A payload that was not protobuf.
      expect(
        yield* fails(
          checkExportEnvelope(
            state({
              deliveries: [
                delivery({ contentType: "application/json" }),
                delivery({ sequence: 2, signal: "logs" }),
              ],
            }),
            { serviceName: "tck-telemetry", signals: [...signals] },
          ),
        ),
      ).toBe(true);
      // A payload that did not decode.
      expect(
        yield* fails(
          checkExportEnvelope(
            state({
              deliveries: [
                delivery({ decodeError: "truncated field" }),
                delivery({ sequence: 2, signal: "logs" }),
              ],
            }),
            { serviceName: "tck-telemetry", signals: [...signals] },
          ),
        ),
      ).toBe(true);
      // The resource named a different service.
      expect(
        yield* fails(
          checkExportEnvelope(
            state({
              resources: {
                traces: { "service.name": "celld" },
                logs: { "service.name": "tck-telemetry" },
              },
            }),
            { serviceName: "tck-telemetry", signals: [...signals] },
          ),
        ),
      ).toBe(true);
    }),
);

it.effect(
  "retry evidence needs a real redelivery within the documented cap",
  () =>
    Effect.gen(function* () {
      const retried = [
        delivery({ sequence: 1, mode: "transient", status: 503 }),
        delivery({ sequence: 2, mode: "transient", status: 503 }),
        delivery({ sequence: 3, mode: "normal", status: 200 }),
      ];
      expect(yield* checkRetryEvidence(retried)).toEqual({
        digest: "a".repeat(64),
        attempts: 3,
      });
      // One delivery per batch is not a retry, however many batches there were.
      expect(
        yield* fails(
          checkRetryEvidence([
            delivery({ sequence: 1, digest: "a".repeat(64) }),
            delivery({ sequence: 2, digest: "b".repeat(64) }),
          ]),
        ),
      ).toBe(true);
      // Repeated deliveries that were never rejected are not retries either.
      expect(
        yield* fails(
          checkRetryEvidence([
            delivery({ sequence: 1 }),
            delivery({ sequence: 2 }),
          ]),
        ),
      ).toBe(true);
      // More attempts than celld documents is a failure, not stronger evidence.
      expect(
        yield* fails(
          checkRetryEvidence(
            Array.from({ length: 6 }, (_, index) =>
              delivery({ sequence: index + 1, mode: "transient", status: 503 }),
            ),
          ),
        ),
      ).toBe(true);
      expect(yield* fails(checkRetryEvidence([]))).toBe(true);
    }),
);

it.effect(
  "failure isolation needs both an observed outage and stable results",
  () =>
    Effect.gen(function* () {
      yield* checkOutageObserved(
        [delivery({ mode: "transient" })],
        "transient",
      );
      // No delivery ever saw the fault, so "results unchanged" proves nothing.
      expect(yield* fails(checkOutageObserved([delivery()], "transient"))).toBe(
        true,
      );
      expect(yield* fails(checkOutageObserved([], "transient"))).toBe(true);

      const rows = [{ seq: 1, id: "a", payload: "x" }];
      yield* checkResultsUnchanged("outage", rows, [
        { seq: 1, id: "a", payload: "x" },
      ]);
      // An application result that moved under a telemetry fault.
      expect(
        yield* fails(
          checkResultsUnchanged("outage", rows, [
            { seq: 1, id: "a", payload: "y" },
          ]),
        ),
      ).toBe(true);
      expect(yield* fails(checkResultsUnchanged("outage", rows, []))).toBe(
        true,
      );
    }),
);

it.effect(
  "telemetry object keys must match the documented partition layout",
  () =>
    Effect.gen(function* () {
      const good =
        "local/tck/telemetry/traces/node_105c42d3/2026/09/19/01/1789782036-77baf33e.parquet";
      expect(parseTelemetryKey(good)).toMatchObject({
        signal: "traces",
        node: "node_105c42d3",
        year: "2026",
        month: "09",
        day: "19",
        hour: "01",
      });
      for (const bad of [
        // A partition level is missing.
        "local/tck/telemetry/traces/node_1/2026/09/19/x.parquet",
        // Not under the telemetry prefix.
        "local/tck/deploy/traces/node_1/2026/09/19/01/x.parquet",
        // An unknown signal.
        "local/tck/telemetry/metrics/node_1/2026/09/19/01/x.parquet",
        // Not a Parquet file.
        "local/tck/telemetry/traces/node_1/2026/09/19/01/x.json",
        // An hour that cannot exist, and a month that cannot either.
        "local/tck/telemetry/traces/node_1/2026/09/19/24/x.parquet",
        "local/tck/telemetry/traces/node_1/2026/13/19/01/x.parquet",
        // A non-numeric partition.
        "local/tck/telemetry/traces/node_1/2026/09/19/ab/x.parquet",
      ])
        expect(parseTelemetryKey(bad)).toBeUndefined();

      yield* checkTelemetryLayout(
        [good, good.replace("/traces/", "/logs/")],
        ["traces", "logs"],
      );
      // A signal the sink never wrote.
      expect(
        yield* fails(checkTelemetryLayout([good], ["traces", "logs"])),
      ).toBe(true);
      // Nothing written at all.
      expect(yield* fails(checkTelemetryLayout([], ["traces"]))).toBe(true);
      // One stray object off the documented layout fails the whole listing.
      expect(
        yield* fails(
          checkTelemetryLayout(
            [good, "local/tck/telemetry/traces/loose.parquet"],
            ["traces"],
          ),
        ),
      ).toBe(true);
    }),
);

it.effect(
  "the schema version and the Parquet footer both have to check out",
  () =>
    Effect.gen(function* () {
      yield* checkTelemetryMetadata({
        "X-Amz-Meta-Celld-Schema": "v0-unstable",
      });
      expect(
        yield* fails(
          checkTelemetryMetadata({ "X-Amz-Meta-Celld-Schema": "v1" }),
        ),
      ).toBe(true);
      expect(
        yield* fails(
          checkTelemetryMetadata({ "Content-Type": "binary/octet-stream" }),
        ),
      ).toBe(true);

      const footer = {
        version: 1,
        rows: 21,
        columns: ["trace_id", "span_id", "name", "duration_us"],
      };
      yield* checkParquetSchema(footer, ["trace_id", "span_id"]);
      // A file with the right columns and no records is not evidence.
      expect(
        yield* fails(checkParquetSchema({ ...footer, rows: 0 }, ["trace_id"])),
      ).toBe(true);
      // A documented column that is not in the file.
      expect(
        yield* fails(checkParquetSchema(footer, ["trace_id", "start_unix_us"])),
      ).toBe(true);
    }),
);

const wrapped = (exitCode: number, stdout: string, stderr: string) =>
  `${COMMAND_MARKER} ${exitCode} ${Buffer.from(stdout).toString("base64")} ${Buffer.from(stderr).toString("base64")}`;

it.effect("a celld invocation's real exit status survives the wrapper", () =>
  Effect.gen(function* () {
    expect(
      yield* parseCommandOutput("probe", wrapped(1, "", "Error: nope\n")),
    ).toEqual({
      label: "probe",
      exitCode: 1,
      stdout: "",
      stderr: "Error: nope\n",
    });
    // Both streams empty is the shape an empty successful command produces.
    expect(
      yield* parseCommandOutput("probe", wrapped(0, "", "")),
    ).toMatchObject({
      exitCode: 0,
    });
    // Without exactly one marker line there is no observation to classify.
    expect(yield* fails(parseCommandOutput("probe", "no marker here"))).toBe(
      true,
    );
    expect(
      yield* fails(
        parseCommandOutput(
          "probe",
          `${wrapped(0, "a", "")}\n${wrapped(1, "b", "")}`,
        ),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        parseCommandOutput("probe", `${COMMAND_MARKER} 0 onlythree`),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        parseCommandOutput("probe", `${COMMAND_MARKER} nan aGk= aGk=`),
      ),
    ).toBe(true);
  }),
);

it.effect("CLI oracles cannot describe a success as a rejection", () =>
  Effect.gen(function* () {
    const failed = {
      label: "kv-get",
      exitCode: 1,
      stdout: "",
      stderr: "Error: celld kv needs a namespace id\n",
    };
    yield* checkCommandRejected(failed, {
      exitCode: 1,
      stderrIncludes: ["celld kv needs a namespace id"],
      stdoutEmpty: true,
    });
    // The guard: a rejection expectation may never name a success status.
    expect(yield* fails(checkCommandRejected(failed, { exitCode: 0 }))).toBe(
      true,
    );
    // A command that succeeded where a rejection was required.
    expect(
      yield* fails(
        checkCommandFailed({
          label: "kv-get",
          exitCode: 0,
          stdout: "value",
          stderr: "",
        }),
      ),
    ).toBe(true);
    // A failure that emitted a partial result on the data stream, or said
    // nothing on the message stream, breaks the documented split.
    expect(
      yield* fails(
        checkCommandFailed({
          label: "kv-list",
          exitCode: 1,
          stdout: "{}\n",
          stderr: "Error\n",
        }),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        checkCommandFailed({
          label: "kv-list",
          exitCode: 1,
          stdout: "",
          stderr: "",
        }),
      ),
    ).toBe(true);
    // A success oracle checks the data, not just the status.
    yield* checkCommandAccepted(
      { label: "kv-get", exitCode: 0, stdout: "value\n", stderr: "" },
      { stdout: "value" },
    );
    expect(
      yield* fails(
        checkCommandAccepted(
          { label: "kv-get", exitCode: 0, stdout: "other\n", stderr: "" },
          { stdout: "value" },
        ),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        checkCommandAccepted(
          { label: "kv-get", exitCode: 1, stdout: "", stderr: "Error\n" },
          {},
        ),
      ),
    ).toBe(true);
  }),
);

it.effect("the data stream must carry only data", () =>
  Effect.gen(function* () {
    const listed = {
      label: "kv-list",
      exitCode: 0,
      stdout: '{"name":"a"}\n{"name":"b"}\n',
      stderr: "2 keys listed\n",
    };
    expect(yield* checkJsonDataStream(listed, ["name"])).toHaveLength(2);
    // A human-readable message leaked onto the data stream.
    expect(
      yield* fails(
        checkJsonDataStream(
          { ...listed, stdout: '{"name":"a"}\nmore keys exist\n' },
          ["name"],
        ),
      ),
    ).toBe(true);
    // A row that is not an object, or is missing the expected field.
    expect(
      yield* fails(
        checkJsonDataStream({ ...listed, stdout: '"a"\n' }, ["name"]),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        checkJsonDataStream({ ...listed, stdout: '{"key":"a"}\n' }, ["name"]),
      ),
    ).toBe(true);
    // No data at all, and a command that failed, are both unusable.
    expect(
      yield* fails(checkJsonDataStream({ ...listed, stdout: "" }, [])),
    ).toBe(true);
    expect(
      yield* fails(checkJsonDataStream({ ...listed, exitCode: 1 }, [])),
    ).toBe(true);
  }),
);
