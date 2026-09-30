# Coverage roadmap

This roadmap prioritizes gaps in celld-tck's feature and operational coverage. It describes planned work, not established guarantees. The [qualification backlog](BACKLOG.md) records the existing reliability checklist; the [live results](https://ewhauser.github.io/celld-tck/) show current CI evidence.

The suite currently pins celld v0.6.0. Upstream documentation changes independently: confirm each feature's availability and contract on the pinned release before implementing a case. A feature requiring a newer binary must explicitly declare that requirement or accompany a reviewed version update.

Many inventories and fault observations below were established on v0.5.0. The [v0.6.0 assessment](RELEASE-0.6.0.md) records what has been rerun on the new pin and what still needs requalification.

## Priorities

| Order | Workstream                          | Starting point                                                                                                                | Environment                                                          |
| ----- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| 1     | Storage durability contracts        | Barrier, cursor, deadline, post-abort, and interrupted-replication cases implemented; cloud durability remains open           | Local workerd and celld; fault scenarios on celld                    |
| 2     | In-place deployment                 | Explicit reload and invalid-replacement cases implemented; lifecycle transitions remain open                                  | Local celld fleet                                                    |
| 3     | Security boundaries                 | Listener, reserved-class, forwarded-header, and body-limit cases implemented; peer credential ageing and replay remain open   | Isolated local celld fleet                                           |
| 4     | Assets, dynamic Workers, and facets | Dynamic Workers complete; asset and facet items open where no contract or single deployment can express them                  | Local reference and candidate, with celld recovery scenarios         |
| 5     | Runtime and service breadth         | Representative cases, not comprehensive API coverage                                                                          | Local where possible; controlled network/clock fixtures where needed |
| 6     | Fleet operations and upgrades       | Weighted placement, rebalance controls, drain, readiness, and rolling upgrade cases implemented                               | Local multi-node fleet and explicitly selected binary versions       |
| 7     | CLI and telemetry                   | CLI, trace-context, OTLP export, Parquet, and collector-outage cases implemented; retention and the bounded queue remain open | Local celld, object store, and recording OTLP collector              |
| 8     | Containers and Sandbox              | Explicitly excluded today                                                                                                     | Separate container-runtime test environment                          |
| 9     | Cloud qualification                 | Local MinIO coverage only                                                                                                     | Dedicated provider accounts and managed reference environment        |

These are implementation priorities, not release dates. Keep workstreams independently reviewable.

## 1. Storage durability contracts

- [x] Exercise `storage.sync()` after committed writes and during an open transaction.
- [x] Exercise `storage.sync()` after object abort with an oracle that distinguishes sync rejection from the abort itself.
- [x] Interrupt object storage while synchronization is pending; verify failure classification and recovery of acknowledged writes using independent evidence.
- [x] Interrupt peer replication during a synchronization barrier.
- [x] Leave a SQL write `RETURNING` cursor unfinished and test response, outbound-effect, and synchronization boundaries. Contrast with a fully consumed write cursor and an open read cursor.
- [x] Test transaction and `blockConcurrencyWhile()` deadlines, rollback, and object reset.

Implemented coverage and limits are in [STORAGE-DURABILITY.md](STORAGE-DURABILITY.md).

Completion requires assertions about stored values and externally observable effects, not just successful requests or elapsed time. Keep celld-specific durability scenarios separate from portable API comparisons.

## 2. In-place deployment

- [x] Adopt a new application revision through `/reload` without restarting nodes.
- [x] Verify that invalid replacement code leaves the previous deployment serving.
- [x] Exercise deployment adoption with in-flight requests, alarms, and pending durability work.
- [x] Verify storage and hibernatable WebSocket preservation at a safe-point transition.
- [x] Test forced adoption after the configured deadline, including regular WebSocket closure and client reconnection.
- [x] Reject modified module bytes that do not match the deployment manifest.

The six items are covered by `faults.reload-adoption`, `faults.reload-invalid`, `faults.reload-in-flight`, `faults.reload-socket`, `faults.reload-forced`, and `faults.reload-module-bytes`; see [IN-PLACE-DEPLOYMENT.md](IN-PLACE-DEPLOYMENT.md) for the v0.5.0 inventory, the local evidence, and the limits.

Every contract this section names was documented and observable on the previous v0.5.0 binary, so nothing here was recorded as untestable. Three adjacent behaviors remain open and are not claimed: cross-deployment Durable Object calls during the drain window, rebalancing during an in-progress adoption, and `CELLD_DEPLOY_MAX_AGE_S=0`, which forces every resident object at the adoption instead of after a deadline.

Record the revision that handled each request and validate retained state. This extends the current rolling application-deployment case; it does not establish binary-upgrade compatibility.

## 3. Security boundaries

- [x] Verify that operator and peer endpoints are inaccessible through the public listener and unknown internal paths do not invoke application code.
- [ ] Reject missing, forged, expired, and replayed peer authentication using controlled test credentials. Missing and forged are covered. Expired and replayed remain open: both need a _valid_ credential to age or resend, and v0.5.0 documents neither the canonical signing input for `cells-peer-request-v1` nor any supported way to mint, inject, or capture a test credential.
- [x] Verify reserved runtime classes cannot be reached through unauthenticated ordinary-object routes.
- [x] Test forwarded-header policy and malformed host handling with and without a trusted proxy configuration. Rejection of _noncanonical_ hosts remains unasserted: the published wording does not define the term, and the likeliest forms — an uppercase host and a trailing-dot host — are accepted on v0.5.0.
- [x] Enforce request-body limits for declared and streamed oversized bodies.

Implemented coverage, the v0.5.0 listener/route/credential inventory it rests on, and the untestable items are in [SECURITY-BOUNDARIES.md](SECURITY-BOUNDARIES.md).

Each denial case needs an authorized control and an assertion that no protected state changed. This verifies documented boundaries; it is not a claim of hostile multi-tenant isolation.

## 4. Deepen existing extension coverage

- [ ] **Assets:** asset-only deployments, HTML/not-found routing, redirects, worker-first rules, and header restrictions. `assets.html-routing`, `assets.redirects`, and `assets.worker-first` now cover default HTML handling, the not-found response, `_redirects` rules, a `_headers` rule on an HTML asset, and `run_worker_first` route patterns including the `!/` negation; `FixtureConfig`/`ReferenceProcess` also accept `html_handling` and `not_found_handling`. Asset-only deployments and non-default handling modes remain open because each needs a second, separately configured deployment. `_headers` protocol-header restrictions remain open: Cloudflare still documents no restricted headers, so there is no reference contract.
- [x] **Dynamic Workers:** props, service capabilities in bindings, outbound restrictions, generation lifecycle, and documented limits. `dynamic.props`, `dynamic.bindings`, `dynamic.outbound`, and `dynamic.limits` cover per-call props, structured-clone and Service Binding values in `WorkerCode.env`, both `globalOutbound: null` and a `globalOutbound` gateway, and acceptance of a `WorkerCode.limits` declaration. celld v0.5.1 now passes `dynamic.limits`. Limit _enforcement_ and the generation lifecycle are not assertable on the pinned workerd reference — it enforces neither the concurrent-Dynamic-Worker limit nor a custom `subRequests` budget locally, and the loader callback may be called any number of times. Tail reports and celld-only limit enforcement need qualification cases; see [RELEASE-0.5.1.md](RELEASE-0.5.1.md).
- [ ] **Facets:** explicit transaction commit/rollback, persistence after eviction/restart, replication, and outbound effects during uncommitted root transactions. `facets.transaction` covers explicit commit and rollback, `facets.outbound-transaction` now requires successful outbound calls during an uncommitted root transaction on v0.6.0, and `recovery.facets` covers facet persistence across a celld restart. Eviction while the node stays up, and facet behavior during a multi-node ownership move, remain open.

Use identical fixtures for shared APIs. Validate documented celld differences explicitly rather than weakening the reference expectation.

Implemented coverage and limits are in [EXTENSIONS.md](EXTENSIONS.md).

## 5. Runtime and service breadth

- [ ] **Cron:** occurrence timing, fleet-wide deduplication, missed occurrences, serialization, retry, and `noRetry()`. Investigated and blocked: the two runtimes have disjoint triggering mechanisms. Miniflare has no scheduler and fires a scheduled handler only through `/cdn-cgi/handler/scheduled`; celld v0.5.0 exposes no trigger and no clock control, so it only fires on wall-clock occurrences. No differential API case is possible; this belongs in a celld-only qualification scenario once a supported trigger or fake clock exists. See [RUNTIME-BREADTH.md](RUNTIME-BREADTH.md).
- [ ] **TCP/TLS:** connection lifecycle, read/write errors, certificate validation, and event lifetime with a controlled server. Investigated and partly blocked: a host-loopback echo server with its address passed as a request parameter would keep fixtures identical, but needs a host-gateway entry in the shared compose base. Certificate validation is not reachable locally at all, because both runtimes trust only public roots and a rejection alone cannot distinguish correct validation from an unimplemented feature. Event lifetime is a celld-only restriction with no reference counterpart. See [RUNTIME-BREADTH.md](RUNTIME-BREADTH.md).
- [x] **WebSockets:** outbound lifetime, overload, and close/reconnect behavior during ownership changes. `websocket.outbound-close` and `websocket.concurrent-sockets` cover a Worker-initiated upgrade to its own object, the server-initiated close code and reason, the reserved-close-code rejection, and sixteen concurrent sockets against one object; `dependencies.socket-failover` holds sockets across a kill of the owner and asserts the forced close and a working reconnection. The documented 1 MiB input-queue budget remains open. The existing real-hibernation coverage in `dependencies.hibernation` is unchanged.
- [ ] **Runtime APIs:** covered so far: ECDSA P-256 sign/verify with JWK and raw public-key round trips, PBKDF2/HKDF derivation against published vectors, secret-key raw/JWK export, a SubtleCrypto invalid-input matrix, MessagePort structured-clone ordering and close, an `EventSource.from()` server-sent event stream, and `node:util`, `node:assert`, and `node:stream`/`node:timers/promises` interop. Remaining: RSA-OAEP, Ed25519, X25519/ECDH, AES-CTR/CBC/KW, wrapKey/unwrapKey, `pkcs8`/`spki` formats, `crypto.DigestStream` and `timingSafeEqual`, network-backed `EventSource` with a `fetcher` binding and its reconnection/`Last-Event-ID` behavior, MessagePort transfer lists (rejected by the pinned workerd), and the remaining documented Node.js modules (`node:diagnostics_channel`, `node:fs`, `node:os`, and `node:crypto` beyond digests and compression).
- [x] **Compatibility flags:** exercise relevant behavior with flags enabled and disabled, including documented unsupported combinations. A `flags` fixture variant rebuilds the identical core worker with `delete_all_preserves_alarm` and `no_websocket_standard_binary_type`; `flags.enabled-defaults` and `flags.disabled-counterparts` assert the differing observations. The contradictory-pair combination is recorded rather than registered: workerd refuses to start, celld v0.5.0 accepts it at deploy time, so no `rejectionConfigs` entry applies. `fetcher_no_get_put_delete` is unobservable behind RPC stubs and `sqlite_vec` has no reference position. See [RUNTIME-BREADTH.md](RUNTIME-BREADTH.md).
- [ ] **Service boundaries:** inventory KV, D1, R2, Queues, and Workflows operations and limits against existing cases; add targeted cases for uncovered supported behavior. The inventory is in [RUNTIME-BREADTH.md](RUNTIME-BREADTH.md), and `kv.expiration`, `d1.exec-batch`, `r2.list-options`, `queues.retry-delay`, and `workflows.timeout` close five gaps. Remaining: KV absolute `expiration`, D1 sessions and result limits, R2 list cursors, queue batch-size/timeout boundaries and retry exhaustion, and workflow instance control and payload limits.

Document whether each reference result comes from local workerd emulation or managed Cloudflare. Do not treat their service guarantees as interchangeable.

The Web Crypto and messaging work registered one documented divergence (`crypto.key-export`: celld cannot export a secret key as `jwk`) and three known bugs, CELL-005 to CELL-007, in [BUGS.md](BUGS.md). The compatibility-flag work registered CELL-008: celld does not apply `websocket_standard_binary_type`.

## 6. Fleet operations and binary upgrades

- [x] Verify weighted ownership placement, rebalance pause/resume, and behavior when capacity samples are missing or stale. Placement weights, pause/resume, and a missing sample are covered. A _stale_ sample distinct from a missing one is not reachable on v0.5.0: the only way to stop a node publishing samples is to stop the process, and a process that cannot renew its lease fences itself.
- [x] Assert readiness transitions, completion of accepted requests, and handoff behavior during graceful drain.
- [x] Exercise concurrent drains and bounded shutdown when survivors lack capacity.
- [x] Validate same-node preserve/reload behavior if supported by the selected release. `POST /shutdown?handoff=preserve` is supported and covered.
- [x] Run mixed-version and rolling binary upgrades with two explicitly pinned releases; assert data preservation and documented compatibility or rejection behavior. v0.5.1 and v0.6.0 are both pinned by digest. The v0.6.0 release supports rolling updates only with bucket durability; fleet durability requires a whole-fleet stop. See [RELEASE-0.6.0.md](RELEASE-0.6.0.md). Downgrades are not attempted.

Implemented coverage, the v0.5.0 operational-control inventory it rests on, and the untestable items are in [FLEET-OPERATIONS.md](FLEET-OPERATIONS.md).

Use multiple cells and retain ownership, readiness, and acknowledged-write evidence. Existing failover tests are a foundation, not proof of every operational control.

## 7. CLI and telemetry

- [x] **CLI:** `celld dev` startup, local persistence, shutdown, invalid configuration, and listener defaults; supported D1/KV operator commands and their error paths. Covered by `telemetry.cli-dev` and `telemetry.cli-operator`. `celld dev`'s build-and-watch path is not exercised: the pinned image ships no esbuild, so the suite runs a pre-bundled project, leaving `--clean`, `--watch-ignore`, and `.dev.vars` open. `celld queue` and `celld cell list` are inventoried but uncovered.
- [x] **Trace context:** propagation across Worker, Durable Object, and outbound calls; malformed input; log correlation across asynchronous work. Covered by `telemetry.trace-context`, including a log line written after an `await` and the `traceparent` the downstream fixture actually received.
- [ ] **Export:** Parquet schema and records, OTLP payloads, sampling, retry, and retention behavior. `telemetry.export-isolation` and `telemetry.parquet-export` cover the OTLP envelope, the documented five-attempt retry cap, the bucket partition layout, the `v0-unstable` schema version, and each file's declared columns and row count; `telemetry.trace-context` covers sampling. Parquet column _values_ remain unread, and retention remains open: the sweep runs at startup and then every six hours, and v0.5.0 does not document whether a node sweeps objects other than its own.
- [ ] **Failure isolation:** collector/storage outages and bounded telemetry queues must not silently change application results. `telemetry.export-isolation` covers the collector outage with an independently observed fault and an unchanged acknowledged history. The bounded queue remains open: its 8192-event channel needs sustained load past what a bounded local case should generate, and the drop counter celld keeps is not exposed on any documented endpoint.

Use a local collector that records received payloads. Finding a log line alone is insufficient evidence of telemetry correctness.

Implemented coverage, the v0.5.0 CLI and telemetry inventory it rests on, and the untestable items are in [CLI-TELEMETRY.md](CLI-TELEMETRY.md).

## 8. Containers and Sandbox

- [ ] Establish a dedicated runner with the required Docker/Podman and network-isolation capabilities.
- [ ] Exercise start, stop, execution, ports, inactivity, and resource limits.
- [ ] Verify documented egress restrictions and failure when required isolation cannot be installed.
- [ ] Test owner movement and node restart, including ephemeral container state and SDK recovery behavior.
- [ ] Add separately scoped Sandbox SDK cases for files, processes, sessions, and code execution.

The Docker container currently running celld is not a test of celld's Containers API. Keep this suite opt-in until its infrastructure and reference requirements are reproducible.

## 9. Cloud qualification

- [ ] Qualify actual S3, R2, GCS, and Azure backends separately, including conditional operations, ranged reads, multipart transfers, and recovery.
- [ ] Exercise supported workload identities, credential renewal, and permission failures.
- [ ] Run infrastructure-specific failure scenarios with independently retained acknowledged-write evidence.
- [ ] Add managed Cloudflare reference runs for contracts that local emulation cannot establish.

Provider results must identify the backend, authentication mode, runtime versions, and fault environment. A MinIO pass must never satisfy a cloud qualification gate.

## Investigate before scheduling

- [ ] **PITR:** confirm which restore/bookmark operations celld actually supports on a selected release. Define a supported contract before adding it as a delivery commitment.
- [ ] **Unsupported features:** distinguish features to test from documented rejections to enforce. An unavailable Cloudflare service is not automatically an implementation gap in celld.

## Definition of done

For every new case or scenario:

1. Identify the contract, runtime version, and required environment.
2. Add a meaningful negative example exercising the actual checker; matching wrong observations must fail.
3. Keep fixture operations separate from semantic expectations and preserve identical fixtures across reference and candidate where applicable.
4. Update the case registry, coverage/exclusions, and dashboard expectations as needed. Missing evidence must remain visible.
5. Pass `pnpm check` and the affected runtime suites. Verify cancellation and cleanup when adding resource-owning orchestration.
6. Report local, hosted-CI, managed-reference, and cloud validation separately. Remove an exclusion only when corresponding evidence exists.

See [CONTRIBUTING.md](../CONTRIBUTING.md) and the [oracle corpus guide](../test/case-oracles/README.md).

## Upstream references

These documents informed the gap review; verify their claims against the selected release before implementation:

- [Cloudflare compatibility](https://celld.dev/docs/cloudflare-compat/)
- [Deployment, storage, CLI, and fleet operations](https://celld.dev/docs/)
- [Security boundaries](https://celld.dev/docs/security/)
- [Telemetry](https://celld.dev/docs/telemetry/)
- [Limitations](https://celld.dev/docs/limitations/)
