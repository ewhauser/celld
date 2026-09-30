# Runtime and service breadth

Implemented coverage, the inventory it rests on, and the items that remain untestable for [roadmap section 5](ROADMAP.md#5-runtime-and-service-breadth). Everything here was confirmed on celld **v0.5.0** and workerd **1.20260730.1** (through Miniflare 4.20260730.0) at compatibility date **2026-07-30**. The reference results come from local workerd emulation, not from managed Cloudflare.

## Implemented in this workstream

| Case                           | Fixture | Behavior                                                                                                                                                                      |
| ------------------------------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `websocket.outbound-close`     | core    | Worker-initiated `Upgrade` to its own Durable Object: echo, server-initiated close code/reason, terminal `readyState`, reserved close code rejected with `InvalidAccessError` |
| `websocket.concurrent-sockets` | core    | Sixteen concurrent outbound sockets against one object; every exchange is answered and the object reports all sixteen live at once                                            |
| `flags.enabled-defaults`       | core    | `delete_all_deletes_alarm` and `websocket_standard_binary_type` in their default-on state                                                                                     |
| `flags.disabled-counterparts`  | flags   | The same request under `delete_all_preserves_alarm` and `no_websocket_standard_binary_type`                                                                                   |
| `kv.expiration`                | core    | `expirationTtl` reported on `list()`, absent for a key stored without one, and rejected below the documented minimum                                                          |
| `d1.exec-batch`                | core    | Multi-statement `exec()` count, a reused prepared statement in `batch()`, per-statement `success`/`changes`, read-after-write inside one batch                                |
| `r2.list-options`              | core    | `delimiter`/`prefix` roll-up and descent, `include` for HTTP and custom metadata, and `writeHttpMetadata()`                                                                   |
| `queues.retry-delay`           | core    | Explicit `retry({ delaySeconds })` overriding the consumer's `retry_delay: 0`, with a stable message id across attempts                                                       |
| `workflows.timeout`            | core    | `step.waitForEvent()` deadline expiry, then `step.sleepUntil()` and a further step on the same instance                                                                       |
| `dependencies.socket-failover` | qual.   | Sockets held across a `fleet.kill` of the owner: forced close, no frame after the kill, reconnection on a new activation, acknowledged history preserved                      |

The `flags` fixture is the **same** core worker rebuilt with two disable counterparts (see `Build.ts`); it is wired like the `node` variant and runs as `pnpm tck --suite flags`. `dependencies.hibernation`, with its real eviction and surviving socket, is unchanged.

This registered one known bug: [CELL-008](BUGS.md), celld does not apply `websocket_standard_binary_type`.

## Cron: no controllable scheduling environment exists

**Not implemented. The bullet stays open.**

celld documents Cron Triggers as supported, with fleet-wide single execution, only the most recent missed occurrence after downtime, per-script serialization, and retry until the next occurrence unless the handler calls `noRetry()`. Those are exactly the behaviors worth testing. The obstacle is triggering, and it is asymmetric:

- **Miniflare/workerd has no scheduler.** Its options carry no `triggers`/`crons` field at all. A scheduled handler runs only when something dispatches `GET /cdn-cgi/handler/scheduled?cron=…&time=…` (`CorePaths.SCHEDULED` in `miniflare/dist/src/index.d.ts`), which returns the handler's `outcome`. Occurrence timing, missed occurrences, deduplication and retry are properties of a scheduler the reference does not have.
- **celld has no trigger.** The documented operator surface is `/state`, `/reload`, `/shutdown`, `/rebalance/pause`, `/rebalance/resume`, `/.well-known/celld/health`, and the `deploy`/`dev`/`d1`/`kv`/`diagnose`/`cell list` commands. None dispatches a scheduled event, and nothing exposes or advances the clock.

So the two runtimes have disjoint triggering mechanisms, and no API-suite differential case can be written: the reference can only be poked manually, the candidate only fires on wall-clock minutes. The fastest legal schedule is `* * * * *`, which makes even a single occurrence a minute-scale wait, and deduplication, missed occurrences and retry need several occurrences across a fleet with controlled downtime.

Cron therefore belongs in a qualification scenario against celld alone, with a fake clock or an injected schedule, not in the differential API corpus. Prerequisites before scheduling the work: a supported way to fire or fast-forward an occurrence on celld, and a reference position on whether celld-only scheduling evidence is acceptable for this bullet.

## TCP/TLS: no fixture-identical controlled server

**Not implemented. The bullet stays open.**

celld documents `connect()` as supported, with three constraints: a socket cannot outlive its event, TLS servers are verified against a bundled Mozilla root store, and celld does not block Cloudflare's blocked destination ports. Testing any of them needs a controlled server both runtimes can reach.

What was examined:

- **Reachability is solvable but touches shared infrastructure.** In the local profile a Node TCP/TLS sidecar joins the compose project exactly like `proxy` in `infra/storage-proxy.yaml`. The reference profile has no compose project: Miniflare runs as a child process on the host (`src/ReferenceProcess.ts`), so a compose service name is meaningless to it. The workable shape is a driver-hosted echo server on host loopback with its address passed as a request parameter — the fixture stays byte-identical and reads the address the way it already reads `name`. That requires `extra_hosts: ["host.docker.internal:host-gateway"]` on the celld service in the shared `infra/compose.yaml`, which every suite and the hosted CI runners inherit.
- **Certificate validation is not reachable locally at all.** Both runtimes trust only public roots — celld by its bundled Mozilla store, workerd by its own bundle — and neither is documented as accepting an injected trust anchor. A locally generated certificate is therefore rejected by both, so the only observable outcome is a failure, and a failure cannot distinguish correct chain validation from "TLS is not implemented". The positive path needs a publicly trusted certificate, which means a hosted endpoint and an external network dependency the suite does not have.
- **Event lifetime has no reference counterpart.** "A socket cannot outlive its event" is a celld restriction; workerd has no equivalent observable, so it is a qualification assertion rather than a differential case.

The connection lifecycle and read/write-error bullets could be delivered after the loopback plumbing above; certificate validation cannot be delivered locally. Splitting the bullet that way, rather than weakening the identical-fixture rule or accepting a rejection-only certificate test, is the recommended next step.

## Compatibility flags: what was picked and why

Switches with a documented observable effect at the pinned compatibility date, with celld's stance from its compatibility page:

| Switch                           | Disable counterpart                 | Default from | celld   | Covered                                                                                                                                                         |
| -------------------------------- | ----------------------------------- | ------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `delete_all_deletes_alarm`       | `delete_all_preserves_alarm`        | 2026-02-24   | honored | yes, both directions, both runtimes agree                                                                                                                       |
| `websocket_standard_binary_type` | `no_websocket_standard_binary_type` | 2026-03-17   | honored | yes; celld reports the legacy value (CELL-008)                                                                                                                  |
| `fetcher_no_get_put_delete`      | `fetcher_has_get_put_delete`        | 2024-03-26   | honored | no: unobservable, an RPC stub answers `typeof stub.get === "function"` for any property, so the removed helpers cannot be distinguished from RPC method proxies |
| `sqlite_vec`                     | —                                   | opt-in       | honored | no: not documented as available on the pinned workerd, so no reference position                                                                                 |
| `js_rpc`                         | —                                   | on           | honored | no: already exercised throughout the `rpc.*` family                                                                                                             |
| static-assets navigation flags   | `assets_navigation_has_no_effect`   | 2025-04-01   | honored | no: the assets fixture is a separate variant; see [EXTENSIONS.md](EXTENSIONS.md)                                                                                |

Flag names were verified against the pinned workerd binary rather than documentation alone.

**Unsupported combination, not registered as a case.** workerd refuses to start when a switch and its own disable counterpart are both set:

```
service core:user:: Compatibility flags are mutually contradictory:
delete_all_deletes_alarm vs delete_all_preserves_alarm
```

celld v0.5.0 accepts the same pair: `celld deploy --dry-run` returns a successful deployment record (evidence run `tck-9d0a6e43-9ff0-47fa-90fa-4dbbd9e45dd3`, captured while the check was briefly registered). Because the roadmap's deployment-rejection check only applies if celld rejects at deploy time, no entry was added to `rejectionConfigs`. It is recorded here rather than waived: celld's page says it accepts unhonored flags without effect, but both of these are honored switches, so silent acceptance leaves the effective behavior of a contradictory configuration unspecified.

**Also observed.** celld v0.5.0 fails a HEAD for an R2 object whose `contentDisposition` carries a non-ASCII filename (`Error: head s3://…`). The fixture uses an ASCII `filename` because HTTP metadata is echoed into response headers; the non-ASCII case stays in `customMetadata`, which both runtimes handle. No bug was registered: the input is itself non-conformant for a header value.

## Service boundary inventory

Operations and documented limits per binding, against the case corpus. "Covered" names the case; "open" marks supported behavior with no case yet.

### KV

| Operation or limit                               | Status                                                                                  |
| ------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `put`/`get` with text, JSON, `arrayBuffer` types | `kv.metadata-list`, `kv.binary-pagination`                                              |
| `getWithMetadata`, metadata round trip           | `kv.metadata-list`                                                                      |
| `list` prefix, limit, `list_complete`, cursor    | `kv.metadata-list`, `kv.binary-pagination`                                              |
| `expirationTtl`, expiration reported on `list`   | `kv.expiration`                                                                         |
| `expiration` (absolute timestamp)                | open                                                                                    |
| `delete`, missing-key `null`                     | `kv.metadata-list`                                                                      |
| `stream` value type, `cacheTtl`                  | excluded: celld has no edge cache, `cacheTtl` has no effect and `cacheStatus` is `null` |
| Values above 1 MiB                               | excluded: requires a fleet bucket                                                       |
| One writer per namespace                         | excluded: a capacity property, not an API contract                                      |

### D1

| Operation or limit                                 | Status                                              |
| -------------------------------------------------- | --------------------------------------------------- |
| `prepare`/`bind`/`all`/`first`/`raw`/`run`         | `d1.bindings-results`                               |
| Column types including BLOB and `NULL`             | `d1.bindings-results`                               |
| `batch` atomicity on failure                       | `d1.batch-rollback`                                 |
| `batch` success path, per-statement `meta.changes` | `d1.exec-batch`                                     |
| `exec` with multiple statements                    | `d1.exec-batch`                                     |
| `withSession` / read replication                   | open                                                |
| Result limits: 100,000 rows or 32 MiB              | open: a documented limit, needs a bounded generator |
| Invalid UTF-8 in `TEXT` refused                    | open                                                |

### R2

| Operation or limit                                                                                                     | Status                                                              |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `put`/`get`/`head`/`delete`, ranged reads, ETag                                                                        | `r2.metadata-range-delete`                                          |
| `httpMetadata`, `customMetadata`                                                                                       | `r2.metadata-range-delete`, `r2.list-options`                       |
| `onlyIf` conditional write                                                                                             | `r2.conditional-write`                                              |
| Multipart create/upload/complete/abort                                                                                 | `r2.multipart`                                                      |
| `list` with `delimiter`, `include`, `truncated`                                                                        | `r2.list-options`                                                   |
| `writeHttpMetadata`                                                                                                    | `r2.list-options`                                                   |
| `list` cursor pagination beyond one page                                                                               | open                                                                |
| `version` equals content ETag                                                                                          | open                                                                |
| `ssecKey`, `jurisdiction`                                                                                              | excluded: documented as unavailable                                 |
| Conditional write with a streamed body above 8 MiB, multipart checksum, cross-node resume, 256 MiB out-of-order budget | excluded: documented limits needing large payloads or node movement |

### Queues

| Operation or limit                                | Status                                                                                                                   |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `send`, `sendBatch`, batch delivery, `ack`        | `queues.batch-ack-retry`                                                                                                 |
| `retry()` and attempt counting                    | `queues.batch-ack-retry`                                                                                                 |
| `retry({ delaySeconds })`, stable message id      | `queues.retry-delay`                                                                                                     |
| `ackAll`/`retryAll`, `message.timestamp`          | open                                                                                                                     |
| `max_batch_size` / `max_batch_timeout` boundaries | open: batch composition is not deterministic enough for an equality oracle; needs a bounded statistical or timing oracle |
| `max_retries` exhaustion and dead-letter routing  | open                                                                                                                     |
| 256 concurrent producer calls, four-day retention | excluded: capacity and retention, not API shape                                                                          |
| Pull consumers, HTTP API, event subscriptions     | excluded: documented as unavailable                                                                                      |

### Workflows

| Operation or limit                                | Status                                           |
| ------------------------------------------------- | ------------------------------------------------ |
| `create` with id and params, `status`, output     | `workflows.steps-sleep-result`                   |
| `step.do`, `step.sleep`                           | `workflows.steps-sleep-result`                   |
| `step.do` retry policy                            | `workflows.retry`                                |
| `waitForEvent` with `sendEvent`                   | `workflows.event`                                |
| `waitForEvent` timeout, `step.sleepUntil`         | `workflows.timeout`                              |
| Instance recovery across node restarts            | `dependencies.workflow-recovery` (qualification) |
| `terminate`, `pause`, `resume`                    | open                                             |
| `retention` option, 30-day cap                    | open                                             |
| 1 MiB step result / event payload / params limits | open                                             |
| 60-second non-step pending limit                  | open                                             |
| Rollback, sensitive step results, stream results  | excluded: documented as unavailable              |

### WebSockets

| Operation or limit                             | Status                                                       |
| ---------------------------------------------- | ------------------------------------------------------------ |
| Hibernatable accept, subprotocol, attachment   | `websocket.protocol-attachment`                              |
| Real hibernation across eviction               | `dependencies.hibernation` (qualification)                   |
| Outbound socket echo, close code/reason        | `websocket.outbound-close`                                   |
| Invalid close code                             | `websocket.outbound-close`                                   |
| Many concurrent sockets against one object     | `websocket.concurrent-sockets`                               |
| Close and reconnect across an ownership change | `dependencies.socket-failover` (qualification)               |
| Socket lifetime past the end of its event      | open: a celld-only restriction with no reference counterpart |
| 1 MiB input-queue budget, discarded frames     | open: needs a controlled backpressure generator              |
| `acceptWebSocket()` above 90% of the V8 heap   | excluded: a heap-pressure limit, not an API contract         |
