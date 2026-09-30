# Full local qualification

This extends the API corpus and existing recovery suites to the full P0/P1/P2 checklist. Run `pnpm test:qualification` for all 51 additional scenarios, or run `test:traffic`, `test:dependencies`, `test:faults`, `test:capacity`, and `test:security` separately. Select one scenario with `--suite qualification --case <id>`. Each scenario owns a fresh three-node fleet, independent disks, MinIO, and a storage fault proxy. Cases continue after a failed scenario using new resources; failures are never automatically retried or waived.

All authored TypeScript uses Effect v4 RC.115. New runtime fixtures use the same compatibility date as the API corpus. Fault scenarios assert lifecycle invariants against real celld; they are not labeled as differential workerd tests.

## Traffic and ownership

- `traffic.crash-ledger`: four concurrent clients, three seeded crash schedules, 96 attempted operations per round, acknowledged baseline and final writes, and exact recovered histories through all three nodes.
- `traffic.paused-owner`: pause the observed owner past its lease, write on a successor, verify a higher ownership epoch, then resume the old owner while concurrent clients send writes to all endpoints. Require fencing and preserve every acknowledged operation in a single valid order.
- `traffic.restart-races`: restart and kill the owner again before the normal lease wait while traffic continues; repeat with three seeded delays, then verify recovered histories.

The external driver writes an intent before sending each unique operation and an acknowledgment only after validating the full HTTP receipt. It appends JSONL through a serialized writer and calls filesystem sync for every entry. Verification reopens this file, rather than trusting an in-memory acknowledgment list. Requests with missing or unsuccessful responses remain uncertain: their transaction may be absent or wholly present, never partial. SQL sequence numbers define a total order, and the oracle checks real-time precedence for non-overlapping completed operations using the driver's clock. Payloads must match both SQL and KV. A separate complete KV scan must exactly match the SQL history, rejecting orphaned KV writes as well as missing or corrupt values. This checks append histories; it is not a general-purpose linearizability checker for arbitrary application operations.

For a driver crash that leaves the dedicated test fleet alive, resume the read-only audit:

```sh
pnpm tck --suite audit --ledger /absolute/path/to/history.jsonl \
  --endpoint http://127.0.0.1:PORT --name RECORDED_OBJECT_NAME
```

Use the name, ledger, and current endpoint from the run evidence. This command does not redeploy or write to the application. An intent without a terminal record remains uncertain; malformed/truncated ledger records fail rather than being discarded. A response received just before a driver crash may have only an intent in the durable ledger. Handled interruptions normally clean the owned fleet; audit cannot resurrect resources that were already removed.

## Application dependencies

- `dependencies.queue-acceptance`: acknowledge a delayed batch, stop all nodes before delivery, remain down past the delay, restart together, and require every accepted message.
- `dependencies.queue-redelivery`: observe first delivery and an explicit delayed retry, crash the fleet during the delay, and require redelivery. At-least-once duplicates are permitted; missing messages are not.
- `dependencies.workflow-recovery`: persist a completed step, stop while waiting for an event, recover, deliver the event, and require both steps' effects and returned results exactly once in this schedule.
- `dependencies.stream-reconnect`: retain a client cursor, interrupt an active ordered stream with owner failure, reconnect from that cursor, and require the exact full sequence and payloads without gaps or duplicates.
- `dependencies.hibernation`: keep one WebSocket connected, explicitly evict its owning DO through celld's internal lifecycle endpoint, and require a changed activation token plus continued attachment state on the same socket.
- `dependencies.socket-failover`: hold three WebSockets open, kill the owning node, and require every socket to end without a clean close and without serving another frame, then require a reconnection on a new activation and the acknowledged write history intact. celld documents that a WebSocket transport cannot move to a new cell owner, so this asserts the forced close and the client's reconnection, not transport survival.

Named service RPC is covered by the API corpus and checked again after each node rolls to a new fixture revision. Application streams here are a persisted ordered append feed; adapt that contract if your application uses different cursor or acknowledgment semantics. Hibernation means an eviction with a surviving connection, not a process restart.

## Storage and lifecycle faults

Six [in-place deployment cases](IN-PLACE-DEPLOYMENT.md) verify reload adoption and its lifecycle transitions without process restarts.

- `faults.reload-adoption`: explicit reload adopts a valid second revision on every node.
- `faults.reload-invalid`: a replacement that fails initialization leaves the previous deployment serving.
- `faults.reload-in-flight`: adopt while a six-second request, an armed alarm, and an outstanding `storage.sync()` barrier are open on the resident object, then issue nine more acknowledged writes. Require immediate node-level adoption, the resident object to stay on the previous generation until its safe point, the request, alarm, and barrier to finish on the previous revision, and no lost acknowledged write.
- `faults.reload-socket`: adopt at an unobstructed safe point with an open hibernatable WebSocket. Require the same socket to stay open, continue its serialized attachment, report the new revision with a changed activation token, and keep its durable rows.
- `faults.reload-forced`: hold a safe point open with a regular WebSocket past a twenty-second `CELLD_DEPLOY_MAX_AGE_S` set by a scenario overlay. Require close code 1012 no earlier than the deadline, an untouched hibernatable socket, and a reconnecting client that reaches the new revision.
- `faults.reload-module-bytes`: rewrite a published module in the bucket at its original length and require all three nodes to refuse the reload with a digest mismatch naming the module, while the previous revision keeps serving.

The lifecycle cases verify the documented safe-point, adoption-deadline, preservation, and module-verification contracts of celld v0.5.0. The [linked document](IN-PLACE-DEPLOYMENT.md) records the inventory those assertions rest on.

Ten [storage durability cases](STORAGE-DURABILITY.md) cover explicit sync barriers, cursor output restrictions, transaction/gate deadlines, a barrier armed across an object reset, and a barrier held open while the owner loses every peer. They run in this group alongside the cases below.

- `faults.storage-latency`: delay storage forwarding by 750 ms during active writes.
- `faults.storage-throttle`: return S3 `SlowDown`/503 responses during active writes.
- `faults.storage-timeout`: withhold responses beyond the driver's request deadline and span the lease window.
- `faults.storage-ambiguous`: forward writes to MinIO, observe an upstream successful response, then destroy the downstream connection. Require proof that a successful PUT response for this exact object's LTX data was dropped after completion.
- `faults.peer-partition`: disconnect the owner's peer network while retaining its storage network, prove the advertised peer endpoint is unreachable, apply traffic, restore connectivity, and verify every acknowledgment. Docker's public-port mapping is restored by restarting the reconnected node after checking history through a peer.
- `faults.rolling-deployment`: deploy a second application revision with the same storage schema, gracefully restart nodes one at a time under traffic, require new named-service RPC results, and preserve the complete history. This tests application rolling deployment on the pinned celld binary, not cross-version celld binary upgrades.

Storage cases use bucket durability so a follower cannot bypass the injected storage fault. MinIO stays alive. Each storage case requires fault evidence on the exact test object's LTX data path. The bounded proxy records its mode and upstream status; the lost-response mode also records actual downstream destruction after upstream completion. Modes automatically expire and scoped finalizers clear them. Diagnosis and deployment use this same normal proxy path before faults are enabled. All suites use fresh upstream proxy connections for diagnosis, without write retries. Existing non-qualification suites retain direct-MinIO deployment and runtime traffic. Peer aliases exist only on the peer network, preventing silent routing around that network through storage-network DNS.

## Capacity and performance

- `capacity.large-restore`: write 16 MiB of deterministic pseudorandom payloads, destroy the owner's local disk, and verify every byte after restoration; record the post-restart restore/read verification request duration, excluding process startup and lease wait.
- `capacity.memory-pressure`: impose verified 128 MiB container limits and request 96 MiB allocations in each receiving Worker, without Durable Object routing; require either a verified allocation or a node termination under the verified limit; record overload responses, exit codes, and Docker OOM flags, restore 512 MiB per-node capacity, then require acknowledged data to recover.
- `capacity.slow-consumer`: four clients each consume an 8 MiB padded, 128-record stream slowly with bounded buffers and exact ordering/payload checks.
- `capacity.queue-load`: submit five 100-message batches while consumers run; require all 500 IDs and record drain duration and delivery counts.
- `capacity.insufficient-spare`: restrict the two replacement nodes to 8 MiB, verify they cannot remain running (Docker OOM flags are retained separately from exit code 137), remove the remaining node, attempt traffic, then restore capacity and require every acknowledgment to survive. Unavailability is allowed; false success and lost acknowledged data are not.

These are bounded, reproducible load scenarios, not production capacity claims or latency SLOs. History artifacts report p50/p95/max acknowledged response latency and elapsed duration, including driver ledger overhead. Docker limits are local to the owned containers and are raised to a verified 512 MiB before recovery and the owned containers are removed afterward. The pinned Node sidecar adds a network hop; do not compare its timings directly with the older direct-MinIO suites.

## Security boundaries

Five [security-boundary cases](SECURITY-BOUNDARIES.md) verify the listener split, peer authentication, reserved runtime classes, forwarded-header policy, and ingress body limits documented for celld v0.5.0.

- `security.listener-separation`: prove the operator API answers on the internal listener and the application on the public one, then require nine operator and peer paths on the public listener to produce the application's own 404 and three internal paths — including one the application serves publicly — to produce celld's `{"error":"not_found"}`.
- `security.peer-authentication`: control with a fleet-signed `celld diagnose --peer` probe against every node and an unauthenticated operator cell resolve, then require `401 peer authentication failed` for missing, bearer-token, forged, stale-timestamped, and wrong-target credentials on `/peer/probe`, and for `/runtime/<reserved>` with and without forged credentials on all three nodes.
- `security.reserved-classes`: control with the unauthenticated ordinary-object route reaching application code, then require the documented 403 and class name for the reserved queue and workflow scopes on `/do/`, and for an ordinary class on `/runtime/`.
- `security.forwarded-headers`: run one fleet with two policies — the default on two nodes, a trusted proxy on the third, identical fixture throughout — and require forwarded headers to be ignored, last-value applied, malformed forwarded hosts dropped, valid `Host` forms preserved, and malformed or absent hosts replaced with `celld.local`.
- `security.body-limits`: control with an acknowledged ledger write, require a body exactly at the limit to reach the application and be rejected by it, and require `413 request body too large` for one byte over, four times over, and a chunked body with no declared length.

Each case snapshots ownership, the acknowledged SQL history, its KV mirror, and the durable-object event log around its denials, requires them identical, and then verifies the full ledger. Probes run inside the sidecar container because the peer and operator listener is never published to the host; pure oracles classify every observation and reject transport errors offered as denial evidence. The scenario-specific overlay lowers the ingress body limit, trusts forwarded headers on one node, and joins the deployment tool to the peer network. Peer credential expiry and replay are not testable on this release; see the linked document.

## CLI and telemetry

Five [CLI and telemetry cases](CLI-TELEMETRY.md) cover the `celld` command line and the traces and logs celld v0.5.0 exports.

- `telemetry.cli-dev`: require six documented configuration rejections to fail with a non-zero status, report on stderr, and print nothing on the data stream; then start `celld dev` on its default listener, require its operator API to be unreachable there, write through its own local storage, require a normal stop to exit 0, and require the same row to read back after a restart.
- `telemetry.cli-operator`: exercise `kv put`/`get`/`info`/`list`/`delete` and `d1 execute` against the deployed fixture, require `get` to return the stored bytes exactly and `list --json` to put one JSON object per key on stdout with its continuation hint on stderr, then require each documented error path to fail without emitting a partial result.
- `telemetry.trace-context`: drive Worker → Durable Object → outbound `fetch` under a known `traceparent` and require an unbroken recorded parent chain on that trace, log markers carrying the writing handler's trace and span including one written after an `await`, and the downstream's received `traceparent` to name the recorded outbound span. Require a malformed header to start a new unrelated trace, and a root request on the zero-ratio node to record nothing while the same probe on an always-on node records.
- `telemetry.export-isolation`: require every payload to be OTLP/HTTP protobuf naming the configured service, then fail the collector transiently under acknowledged traffic and require an observed fault, a redelivery of the same batch, no more than the documented five attempts, and an unchanged complete application history.
- `telemetry.parquet-export`: require every Parquet object under the documented `telemetry/<signal>/<node>/<yyyy>/<mm>/<dd>/<hh>/` layout, the `v0-unstable` schema version in object metadata, and each file's footer to declare a positive row count and the columns the published DuckDB queries select.

The OTLP cases assert on payloads a recording collector sidecar decoded, never on celld's own log output. Each delivery is digested, so a repeated digest is what proves a retry. Only the Parquet footer is read, so column values, retention, and the bounded telemetry queue stay unasserted; the linked document records why.

## Fleet operations

Six [fleet-operations cases](FLEET-OPERATIONS.md) exercise the operational
controls documented for celld v0.5.0, plus a rolling binary upgrade from the
previous pinned release, v0.5.1, to v0.6.0 using bucket durability. Fleet
durability requires a whole-fleet stop for this upgrade and is not covered by
the rolling scenario.

- `operations.weighted-placement`: require every node to publish its configured
  `CELLD_PLACEMENT_WEIGHT` through both `/state` and its bucket lease, create
  25 cells through one node, and require balancing to settle with no node two
  whole cells above its weighted target.
- `operations.rebalance-control`: pause balancing on one node, prove the pause
  is republished in its lease, and require no ownership record and no
  `rebalanced` counter to move across ten sample intervals while another node is
  far above its target; resume and require convergence. Then freeze a node past
  its lease lifetime and require the live pair to keep balancing while the node
  without a capacity sample acquires nothing.
- `operations.graceful-drain`: hold a write open inside its owning object,
  drain the node through `POST /shutdown` while it is in flight, and require the
  accepted write to complete with its own receipt, a new public request to be
  refused with 503, readiness to fall once and never rise, an exit 0 inside the
  stop bound, every cell to be owned by a peer, and the node's own log to record
  each handoff.
- `operations.concurrent-drain`: drain two nodes at once into a survivor held at
  a verified 128 MiB; require both to stop inside the bound with disjoint
  drain-token holds and to give every cell away.
- `operations.preserve-reload`: ask one node for a same-node preserve and
  require it to log a prepared clean reload, no handoff at all, and ownership
  records that are byte-identical across the stop and the restart.
- `operations.binary-upgrade`: start the fleet on the pinned v0.5.1 image with bucket durability and
  roll each node to the pinned v0.6.0 image one at a time, requiring a genuinely
  mixed fleet at each step, an acknowledged write on every cell through every
  live node while it is mixed, and every acknowledged write intact at the end.
  Each case holds several cells, each with its own durable ledger, and ends by
  requiring every acknowledged write of every cell through every node. Operator
  calls run from the sidecar container because the operator listener is never
  published to the host. The scenario-specific overlays set distinct placement
  weights and a short stop bound, enable balancing and idle eviction for the two
  balancing cases, and pin a second release per node for the upgrade case.
  Each scenario has an eight-minute bound and separate artifacts under its ID. The aggregate report and JUnit retain unexecuted placeholders and infrastructure failures. CI runs each group independently. Local Docker results do not qualify AWS, managed Cloudflare delivery guarantees, or host/availability-zone failure domains.

References: [Cloudflare queue APIs](https://developers.cloudflare.com/queues/configuration/javascript-apis/), [workflow sleeping and retrying](https://developers.cloudflare.com/workflows/build/sleeping-and-retrying/), and [Durable Object WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).
