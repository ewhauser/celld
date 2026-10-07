# Telemetry

celld can record traces and logs for the requests it serves, and it can
export metrics on node load and on the CPU and memory its cells use. The
feature is off by default, and the off state costs nothing. Set
`CELLD_OTEL=1` to write telemetry to the fleet bucket.
Set `CELLD_OTEL=http://collector:4318` to send telemetry to an OTLP collector.

The schema is version `v0-unstable`, so column names can change before a
stable release. Each file carries the version in the object metadata name
`celld-schema`, or `celld_schema` on an `az://` bucket.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_OTEL` | `0` | `0` disables telemetry. `1` writes Parquet to the fleet bucket. An HTTP(S) collector base URL selects OTLP/HTTP protobuf. |
| `CELLD_OTEL_BUCKET` | the fleet bucket | A different bucket for the Parquet files, on the same endpoint and credentials. |
| `CELLD_OTEL_RETENTION` | `30d` | celld deletes telemetry files older than this. `none` disables the deletion, so your own lifecycle rules can control the data. |
| `CELLD_OTEL_FLUSH_MS` | `300000` | celld writes a Parquet file after this many milliseconds of buffered events. |
| `CELLD_OTEL_FLUSH_BYTES` | `5242880` | The estimated buffered bytes that trigger a flush before the interval ends. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | A standard sampler name. `traceidratio` with `OTEL_TRACES_SAMPLER_ARG` records a fraction of the traces. |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | A comma-separated list of `name=value` headers for the collector. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | The collector request timeout in milliseconds. |
| `OTEL_SERVICE_NAME` | `celld` | The service name in the exported resource. |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Extra `key=value` resource attributes, comma-separated, with percent-encoded values, such as `celld.fleet=prod`. celld sends them with every OTLP signal. They cannot replace celld's own `service.name`, `service.version`, `service.instance.id`, or `celld.region`. |
| `OTEL_METRICS_EXPORTER` | `otlp` | `none` turns off metrics while keeping traces and logs. |
| `OTEL_METRIC_EXPORT_INTERVAL` | `60000` | The metrics export interval in milliseconds. Each export covers the interval since the previous one. |

The bucket sink requires `CELLD_BUCKET`. The OTLP sink does not.

Use a full HTTP(S) collector base URL without a query or a fragment.
celld adds `/v1/traces`, `/v1/logs`, and `/v1/metrics` to its path for the three signals.
`CELLD_OTEL` supplies the collector address, so celld does not read
`OTEL_EXPORTER_OTLP_ENDPOINT`.

`CELLD_OTEL_SINK` is removed, and a node with it set does not start. Put the
collector base URL in `CELLD_OTEL`, or keep `CELLD_OTEL=1` for the bucket.

## What celld records

celld records a span for each stateless Worker request, each cell event (a
fetch, an alarm, an RPC, a WebSocket message), each outbound `fetch()`, and
each cell start. A span carries the request id, the cell, the isolate, the
queue wait, the outbound URL and status, and the known durability facts.

Each `console.log` line becomes a log record with the trace id and span id of
its handler, across `await`. The record carries the console method's severity
in `severity_number` and `severity_text` (OTLP fields and Parquet columns). The
body holds only the message.

| method | severity number | severity text |
| --- | --- | --- |
| `console.debug` | `5` | `DEBUG` |
| `console.log`, `console.info` | `9` | `INFO` |
| `console.warn` | `13` | `WARN` |
| `console.error` | `17` | `ERROR` |

Log files from earlier celld versions have no severity columns. Read a mix of
versions with `union_by_name = true`.

celld reads the W3C `traceparent` header on incoming requests and sends it on
outbound `fetch()`. A malformed header starts a new trace. A Worker call to a
Durable Object stays in one trace.

The sampler decides at the start of a request. An unsampled request
records nothing and costs almost nothing. Under load, telemetry sheds
before requests do, and celld counts what it sheds.

A sampling ratio of `0` records no traces, and a ratio of `1` records
every trace. An intermediate ratio makes the same trace-id decision on
each node.

celld keeps a valid incoming context when the sampler rejects it. An
outbound `fetch()` or Durable Object call keeps the trace id, uses a new
span id, and keeps the sampled flag clear.

## Metrics

celld exports metrics only to an OTLP collector (`CELLD_OTEL=<url>`). The
bucket sink carries traces and logs, but not metrics. celld exports
one set of metrics per interval (`OTEL_METRIC_EXPORT_INTERVAL`, 60 seconds by
default).

No data point carries a cell, script, or request attribute. Each series
belongs to a node, so the number of series grows with the number of nodes and
not with the number of cells. Use `OTEL_RESOURCE_ATTRIBUTES` to name the
fleet, so a dashboard can group its nodes.

### Node load

These metrics are gauges. celld reads them from the same snapshot that
`/state` serves, so a chart and `/state` agree. celld omits a gauge whose
`/state` field is absent or `null`, because an unknown value is not a zero.

| metric | `/state` source |
| --- | --- |
| `celld.node.owned_cells` | `owned_cells` |
| `celld.node.resident_cells` | `node_load.resident_cells` |
| `celld.node.occupied` | `occupied` |
| `celld.node.restoring` | `restoring` |
| `celld.node.capacity_waiting` | `capacity_waiting` |
| `celld.node.activation_waiting` | `activation_waiting` |
| `celld.node.host_websockets` | `node_load.host_websockets` |
| `celld.node.rss_bytes` | `node_load.rss_bytes` |
| `celld.node.in_use_bytes` | `node_load.in_use_bytes` |
| `celld.node.cgroup_working_set_bytes` | `node_load.cgroup_working_set_bytes` |
| `celld.node.cpu_utilization` | `node_load.cpu_percent_x100 / 10000`, a ratio of one core |
| `celld.node.open_fds`, `celld.node.fd_limit` | `node_load` |
| `celld.node.pressured`, `celld.node.memory_headroom`, `celld.node.draining` | `node_load`, as 0 or 1 |
| `celld.node.shed_cells` | `node_load.shed_cells` |
| `celld.node.isolates`, `celld.node.isolate_cells` | the `live` and `cells` counts of the cell pools in `deployment.isolates.cells`, summed with the cell pools of the generations the node still drains |

### Cell distributions

These metrics are delta exponential histograms. Each point carries its
`min`, `max`, `sum`, and `count`. A backend can merge the points of
all the nodes into fleet-wide percentiles, with no bucket bounds to
choose. Datadog maps them to distributions. celld omits a histogram when an
interval has no observations.

`celld.cell.cpu_time` (seconds) has one observation per cell that ran
JavaScript in the interval. The observation is the thread CPU time of that
cell's turns. It is exact, and a facet's CPU counts toward its root cell.
It is a distribution over active cells, not over requests or turns.
When metrics are on, celld reads the thread CPU clock at each end of every
cell turn. It does this even when no CPU limit is set. Stateless Worker
requests are not in this metric.

`celld.cell.heap_bytes` (bytes) has one observation per resident cell. The
observation is its isolate's V8 heap, physical plus external, divided by the
number of cells that share that isolate. Up to 32 cells can share an isolate,
and V8 cannot cheaply attribute memory to one of them, so this value is an
average:

- It is exact for an isolate that holds one cell. When a heavy cell shares an
  isolate with light cells, the metric understates the maximum.
- It counts only the V8 heap. It does not count memory on the Rust side, such
  as storage buffers and sockets.

The heap sample never waits for an isolate that is running a turn. celld skips
that isolate and counts it in `celld.cell.heap_samples_skipped`.

### Delivery

Metrics use the retry and backoff rules of the other OTLP signals. The exporter
retries one export and holds one more export in a queue. celld drops an export
that arrives while both places are taken. It counts the drop in
`celld.metrics.shed`, and it reports that count in the next export that
goes out.

## Performance counters

Apart from OTLP, every node keeps counters and latency histograms for its
hot paths. These cover bucket requests by operation, key class and outcome;
output gates; durability proofs by source; activations by restore path;
and core and main-loop lag. It serves them as one JSON snapshot on its
internal listener, at `GET /debug/metrics`. They need no collector and
cost one atomic add per observation. The benchmark harness reads them; see
[performance tests](performance-tests.md#what-a-node-counts).

## Query the bucket with DuckDB

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET celld_telemetry (
  TYPE s3, KEY_ID '...', SECRET '...',
  ENDPOINT 's3.example.com', URL_STYLE 'path'
);
CREATE VIEW traces AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/traces/*/*/*/*/*/*.parquet');
CREATE VIEW logs AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/logs/*/*/*/*/*/*.parquet');

-- The slowest requests.
SELECT name, duration_us, trace_id FROM traces
  ORDER BY duration_us DESC LIMIT 20;

-- The error lines.
SELECT time_unix_us, body FROM logs WHERE severity_number >= 17;

-- Every log line, inside the span that wrote it.
SELECT l.body, t.name, t.duration_us FROM logs l
  JOIN traces t ON l.trace_id = t.trace_id AND l.span_id = t.span_id;
```

A non-AWS S3-compatible endpoint needs `URL_STYLE 'path'`. A plain-HTTP
endpoint also needs `USE_SSL false`.

Files are partitioned by node and hour:
`telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`.

## Flushing and delivery

celld writes one Parquet file per flush, at 5 minutes
(`CELLD_OTEL_FLUSH_MS=300000`) or 5 MiB of estimated buffered events
(`CELLD_OTEL_FLUSH_BYTES=5242880`), whichever comes first. The event that
reaches the target can take the batch past it.

Keep the defaults if you run no compaction job. They produce large files, but
data can arrive up to 5 minutes late. `CELLD_OTEL_FLUSH_MS=5000` gives a
five-second interval, plus upload or collector delay. A short interval makes
many small files and slows queries within hours, so start the compaction job
first. For a near-live view, use the OTLP sink with the same short interval.

The OTLP sink makes at most five attempts per batch on a transient failure
(HTTP 408, 429, 502, 503, or 504). It uses exponential backoff with jitter and
honors `Retry-After`, with each delay capped at 30 seconds. A permanent
refusal drops the batch.

The exporter holds one retrying batch, and the input channel holds 8192 new
events. When the channel is full, celld drops and counts new telemetry, so
request handling continues.

The retention sweep runs at startup and six hours after each completed sweep.

## Compaction

celld does not compact its own files. Run a compaction job on a maintenance
node once an hour, for the hour that just ended. Do not compact the current
hour, because a node still writes to it.

```sql
COPY (
  SELECT * FROM
    read_parquet('s3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/*.parquet')
  ORDER BY start_unix_us
) TO 's3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/compacted.parquet'
  (FORMAT parquet, COMPRESSION zstd);
```

Delete the source files after DuckDB writes the compacted file.
