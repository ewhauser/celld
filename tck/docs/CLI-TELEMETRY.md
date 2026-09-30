# CLI and telemetry

This records what celld v0.5.0 actually provides for the command line and for
telemetry, what the `telemetry` qualification suite checks against it, and what
this release leaves untestable. Everything below was established against the
pinned image
`ghcr.io/denoland/celld:v0.5.0@sha256:df8e74bb9a059df5779644368984933eba76acd6a2d196672732f4368f760fc8`,
by probing the binary and by running the suite, not from the published
documentation alone.

## v0.5.0 inventory

### Commands

`celld --help` lists one long-running form and six subcommands. Each subcommand
prints its own help when invoked with no arguments or with `--help`.

| Command                                                            | Purpose                                                   |
| ------------------------------------------------------------------ | --------------------------------------------------------- |
| `celld --bucket …`                                                 | Run a node.                                               |
| `celld deploy [PROJECT] --bucket …`                                | Upload an application revision.                           |
| `celld dev [PROJECT]`                                              | Run an application locally with persistent local storage. |
| `celld cell list [CLASS] --bucket …`                               | List cells.                                               |
| `celld d1 execute\|migrations apply\|migrations list --bucket …`   | Operate a deployed D1 database.                           |
| `celld kv get\|put\|delete\|list\|info\|bulk … --bucket …`         | Operate a deployed KV namespace.                          |
| `celld queue info\|peek\|purge\|pause\|resume\|redrive --bucket …` | Operate a deployed Queue.                                 |
| `celld diagnose --bucket … [--peer NODE_ID]`                       | Check bucket and peer reachability.                       |

Every command writes data to stdout and messages to stderr, so a redirect or a
pipe carries only data. The suite asserts that split rather than restating it:
`celld kv list --json` must put one JSON object per key on stdout and its
"more keys exist" continuation hint on stderr, and every rejection must leave
stdout empty.

### `celld dev`

- Default Worker listener `127.0.0.1:9876`; `--host` and `--port` change it.
- The operator listener stays on loopback and is never exposed by `--host`.
- All local state lives in `PROJECT/.celld/dev`, which a normal shutdown keeps.
- `--clean` deletes that directory before the server starts; `--no-watch`
  disables rebuilds; `--watch-ignore PATTERN` adds ignored globs.
- `.dev.vars` beside the configuration supplies Worker variables.
- Worker projects need `esbuild` on `PATH` (or `CELLD_ESBUILD`). The image does
  **not** ship esbuild, so the suite runs `celld dev` against the harness's
  already-bundled fixture, whose emitted Wrangler configuration carries
  `no_bundle: true` — the same mechanism `celld deploy` uses in every other
  suite. Nothing in the suite therefore exercises the build-and-watch path.

### Telemetry configuration

Telemetry is off by default and none of its variables appear in `celld --help`.

| Variable                     | Default                 | Effect                                                                                           |
| ---------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------ |
| `CELLD_OTEL`                 | `0`                     | `0` off; `1` writes Parquet to the fleet bucket; an HTTP(S) base URL selects OTLP/HTTP protobuf. |
| `CELLD_OTEL_BUCKET`          | the fleet bucket        | A different bucket for the Parquet files.                                                        |
| `CELLD_OTEL_RETENTION`       | `30d`                   | Deletes telemetry files older than this; `none` disables deletion.                               |
| `CELLD_OTEL_FLUSH_MS`        | `300000`                | Flush interval.                                                                                  |
| `CELLD_OTEL_FLUSH_BYTES`     | `5242880`               | Buffered-byte flush trigger.                                                                     |
| `OTEL_TRACES_SAMPLER`        | `parentbased_always_on` | Standard sampler name; `OTEL_TRACES_SAMPLER_ARG` carries the ratio.                              |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset                   | Comma-separated `name=value` collector headers.                                                  |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000`                 | Collector request timeout, milliseconds.                                                         |
| `OTEL_SERVICE_NAME`          | `celld`                 | Service name in the exported resource.                                                           |

`CELLD_OTEL` supplies the collector address, so celld does not read
`OTEL_EXPORTER_OTLP_ENDPOINT`. `CELLD_OTEL_SINK` is removed and its presence
stops startup. celld appends `/v1/traces` and `/v1/logs` to the base URL
itself.

`parentbased_traceidratio` is accepted and behaves as the OpenTelemetry
specification describes, which is what makes a per-node sampling asymmetry
usable in a fleet: see [Sampling](#sampling).

### What the export actually contains

Observed on the wire, decoded from OTLP/HTTP protobuf by the suite's collector.

Resource attributes: `service.name`, `service.version`, `service.instance.id`,
`celld.region`.

Span names and the attributes that carry the facts the suite asserts on:

| Span                 | Kind     | Notable attributes                                                                |
| -------------------- | -------- | --------------------------------------------------------------------------------- |
| `celld.fetch`        | server   | `celld.request_id`, `celld.isolate`, `celld.queue_wait_us`, `celld.parent_remote` |
| `celld.cell_fetch`   | server   | `celld.cell`, `celld.parent_remote`                                               |
| `celld.cell_startup` | internal | `celld.cell`, `celld.epoch`                                                       |
| `fetch`              | client   | `url.full`, `http.response.status_code`, `celld.parent_remote`                    |

The outbound span is named `fetch`, not `celld.fetch`. A cell start is its own
root trace rather than a child of the request that caused it.

Each `console.log` line becomes an OTLP log record carrying the trace id and
the span id of the handler that wrote it.

### Parquet bucket sink

Objects land at
`telemetry/<traces|logs>/<node>/<yyyy>/<mm>/<dd>/<hh>/<micros>-<hex>.parquet`
in the fleet bucket, and each carries `X-Amz-Meta-Celld-Schema: v0-unstable`
and `X-Amz-Meta-Celld-Retention` in its object metadata.

Columns observed in a file celld wrote, schema `v0-unstable`:

- traces: `node`, `region`, `trace_id`, `span_id`, `parent_span_id`, `name`,
  `kind`, `start_unix_us`, `duration_us`, `ok`, `error`, `request_id`, `cell`,
  `epoch`, `isolate`, `queue_wait_us`, `url`, `http_status`, `parent_remote`
- logs: `node`, `region`, `trace_id`, `span_id`, `time_unix_us`, `body`

## The local collector

The roadmap requires that "finding a log line alone is insufficient evidence of
telemetry correctness", so the OTLP cases assert on payloads a recording
collector received and decoded.

`src/TelemetryCollector.ts` is a Node sidecar, bundled and mounted the way
`src/StorageProxy.ts` is and started by `infra/telemetry.yaml`. Its OTLP
listener stays inside the Compose network; only its control listener is
published on host loopback. It decodes every payload with
`src/TelemetryProtobuf.ts` — a wire-format reader for exactly the OTLP fields
the suite asserts on, so the harness needs no protobuf runtime dependency — and
returns the recorded spans, log records, and per-delivery metadata as JSON.

Each delivery carries the SHA-256 of its raw body. celld resends the same
encoded batch on a retry, so a repeated digest is what proves a retry happened
rather than an inference from timing. The control endpoint can also make the
collector fail for a bounded window: `transient` answers 503 with `Retry-After`
and `permanent` answers 400.

`src/TelemetryParquet.ts` reads the Thrift `FileMetaData` footer of a Parquet
file, which is where the declared schema and the row count live. Column
_values_ are in compressed pages and are **not** read; see
[Limits](#limits-and-untested-bullets).

## Implemented cases

Each case owns a fresh three-node fleet. `telemetry.cli-dev` and
`telemetry.cli-operator` use `infra/telemetry-cli.yaml`, the OTLP cases use
`infra/telemetry.yaml`, and `telemetry.parquet-export` uses
`infra/telemetry-bucket.yaml`.

### `telemetry.cli-dev`

Six documented rejections, each required to fail with a non-zero status,
report on stderr, and print nothing on its data stream: an unparsable `--port`,
a project directory with no Wrangler configuration, the removed
`CELLD_OTEL_SINK`, a boolean environment variable set to something other than
`0` or `1`, a non-loopback `--listen` without `--internal-listen`, and an
unknown command. Then `celld dev` is started on its default listener, its
operator API is required to be unreachable through that listener, a write is
made through the dev node's own storage, the service is stopped and required to
exit 0, and the same row is required to read back after a restart. The fleet
the dev node ran beside is verified unchanged.

### `telemetry.cli-operator`

`kv put`/`get`/`info`/`list`/`delete` and `d1 execute` against the deployed
fixture, then their error paths. `get` is required to return the stored bytes
exactly; `list --json` to put one JSON object per key on stdout; `list --limit
1` to report the `--after` continuation on stderr and still emit only JSON on
stdout; and a deleted key to become unreadable through the same path that saw
it. The acknowledged application history is verified unchanged afterwards.

### `telemetry.trace-context`

One request carrying a known `traceparent` drives Worker → Durable Object →
outbound `fetch`. The suite requires the recorded spans to form an unbroken
parent chain anchored on the incoming trace id and remote parent span, the
outbound span to carry the call it made and its status, the three `console.log`
markers to carry the trace and span of the handler that wrote them — including
one written _after_ an `await` — and the `traceparent` the downstream fixture
actually received to name that same trace and the recorded outbound span rather
than the caller's. A malformed `traceparent` is required to start a new,
well-formed, unrelated trace with no remote parent rather than to fail the
request. A root request sent to the zero-ratio node is required to record
nothing, paired with the same probe against an always-on node, which must.

The harness's own trace propagation is disabled for this suite. Effect's HTTP
client stamps a `traceparent` for the ambient span onto every outgoing request,
which would overwrite the headers these cases send and would give every probe a
remote parent, making a sampling decision for a _root_ request unobservable.

### `telemetry.export-isolation`

Requires every payload to be OTLP/HTTP protobuf, to decode, and to name the
configured service in its resource. The collector is then made to fail
transiently for 25 seconds while acknowledged application traffic runs. The
suite requires that a batch was actually served in the failing mode, that the
same batch was redelivered, that no batch exceeded the documented five
attempts, and that the application's acknowledged history is unchanged and
complete across the outage.

Observed: celld made exactly five attempts per batch and then dropped it. The
telemetry was lost; the application was not affected.

### `telemetry.parquet-export`

Polls the bucket for written objects, requires every Parquet object to sit
under the documented partition layout, requires the schema version in object
metadata, and reads each file's footer to require a positive row count and the
columns the published DuckDB queries select.

## Sampling

`infra/telemetry.yaml` gives `celld3` `parentbased_traceidratio` with a ratio of
`0` while the other two nodes record every trace. The sampler is parent-based
on purpose: the decision is then pinned to where a trace _starts_, so a root
request arriving on `celld3` records nothing, while a Durable Object hop that
ownership happens to place there still joins a trace another node already
sampled. A plain `traceidratio` would drop that hop instead and make the
propagation assertions depend on cell placement.

Two documented behaviours were confirmed while establishing this and are not
separately asserted in a case: an incoming context whose sampled flag is clear
is kept, propagated onward with a new span id and the flag still clear, and
records nothing; and an unsampled _root_ request sends no `traceparent` at all
on its outbound call.

## Limits and untested bullets

- **Parquet values.** Only the footer is read, so the assertion covers the
  declared schema and the row count, not the content of any column. Reading
  values needs a full Parquet reader with page decompression.
- **Retention.** `CELLD_OTEL_RETENTION` sweeps at startup and then every six
  hours. A case would have to either wait out that interval or restart a node
  with a retention short enough to expire files a sibling node wrote, and
  v0.5.0 does not document whether a node sweeps objects other than its own.
  Untested.
- **`CELLD_OTEL_FLUSH_BYTES`.** The suite shortens `CELLD_OTEL_FLUSH_MS`
  instead, so the size-triggered flush path is not exercised.
- **`CELLD_OTEL_BUCKET`, `OTEL_EXPORTER_OTLP_HEADERS`.** Not exercised.
- **Bounded telemetry queue.** The exporter's input channel holds 8192 events
  and drops beyond that. Filling it needs sustained load well past what a
  bounded local case should generate, so the failure-isolation case exercises
  the _collector_ outage path instead. The drop counter celld keeps is not
  exposed on any endpoint this release documents.
- **`celld dev` build and watch.** The image ships no esbuild, so the suite
  runs a pre-bundled project. `--clean`, `--watch-ignore`, and `.dev.vars` are
  untested.
- **`celld queue` and `celld cell list`.** Inventoried above, not covered by a
  case.
- **Unknown KV namespace.** `celld kv get` on a namespace the project does not
  declare reports `no key "…" in namespace "…"` rather than naming the
  namespace as unknown. v0.5.0 documents no contract for this, so the case
  requires the observed wording rather than treating it as a defect.

No deviation from documented v0.5.0 behaviour was found while implementing this
suite, so nothing here is registered in [BUGS.md](BUGS.md).
