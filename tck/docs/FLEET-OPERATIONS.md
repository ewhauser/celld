# Fleet operations

Six local qualification cases exercise the operational controls celld documents
for the pinned [v0.6.0](https://github.com/denoland/celld/releases/tag/v0.6.0)
release, plus a rolling binary upgrade from the previous pinned release,
v0.5.1. They run in a dedicated `operations` CI job and results matrix
column.

These cases qualify documented operator controls on a three-node local fleet.
They are not a capacity model, not a claim about any particular orchestrator,
and not a statement about an upgrade path other than the one they run.

## Operational inventory originally established on v0.5.0

Established by reading `celld --help` from the pinned image, driving a live
three-node fleet through every control below, and reading the fleet bucket.
Everything here is what the pinned binary actually does, not only what the docs
say.

### Controls

| Control                       | Where it lives                                                | Observed                                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Placement weight              | `CELLD_PLACEMENT_WEIGHT` (default: the container's CPU count) | Published as `node_load.placement_weight` on `/state` and as `load.placement_weight` in `nodes/<node>.json`                                        |
| Balancing interval/sample age | `CELLD_REBALANCE_INTERVAL_MS` (default 5000; `0` disables)    | `fleet_sample_refreshed` per interval; `/state` counts `rebalanced`, `handed_off`, `rebalance_failed`                                              |
| Idle eviction                 | `CELLD_IDLE_EVICT_S` (disabled unless set)                    | Required before any move: only a hibernated cell moves, and `/state` `occupied` has to fall to zero first                                          |
| Balancing pause               | `POST /rebalance/pause` on the internal listener              | `200 {"rebalance_paused":true}`; republished as `load.rebalance_paused` in that node's lease; one paused lease stops every move                    |
| Balancing resume              | `POST /rebalance/resume` on the same node                     | `200 {"rebalance_paused":false}`; convergence restarts within about ten seconds                                                                    |
| Graceful drain                | `POST /shutdown`, or SIGTERM/SIGINT                           | `200 {"ok":true}`, then health 503, public 503, handoff, exit 0                                                                                    |
| Same-node preserve            | `POST /shutdown?handoff=preserve`                             | `200 {"ok":true}`, exit 0, ownership records unchanged, `clean_reload_prepared`                                                                    |
| Stop bound                    | `CELLD_SHUTDOWN_TOTAL_MS` (default 40000)                     | Token wait is 3/4 and the handoff no-progress interval 5/8 of this one value                                                                       |
| First-readiness gate          | `CELLD_READY_FLEET_GATE_MS` (default 120000; `0` disables)    | `ready_gate_open` with `readiness_reason="fleet_settled"`; a replacement stays 503 until the fleet settles                                         |
| Deployment adoption           | `POST /reload`                                                | Covered by [IN-PLACE-DEPLOYMENT.md](IN-PLACE-DEPLOYMENT.md), not by this suite                                                                     |
| Removed variables             | `CELLD_SHUTDOWN_DRAIN_MS`, `CELLD_DRAIN_TOKEN_WAIT_MS`        | Rejected at startup: `Error: CELLD_SHUTDOWN_DRAIN_MS is removed; set only CELLD_SHUTDOWN_TOTAL_MS; celld derives the handoff no-progress interval` |

The operator API is on the internal listener only, which the compose fleet never
publishes to the host. Every operator call in this suite therefore runs from the
sidecar container through `src/SecurityProbe.ts`, the same in-network probe the
security suite uses, on a raw socket: `/shutdown` answers and then takes the
process down while the client is still attached, which an ordinary HTTP client
turns into a failed request with the response discarded.

### `/state`

The counters this suite reads: `owned_cells`, `occupied`, `handed_off`,
`rebalanced`, `rebalance_failed`, and the `node_load` block, which is the same
load sample the node lease publishes (`sampled_ms`, `owned_cells`,
`placement_weight`, `rebalance_paused`, `draining`, and the memory values).
`deployment.cells` lists every cell the node knows, so the response grows with
the fleet; the probe's body limit was raised to a mebibyte for it.

### Ownership records

`cells/Recovery:<cell>/own.json` holds `{"node":"...","epoch":N}`. A released
cell keeps its record with an **empty** node until a successor acquires it, so a
snapshot taken during a drain or a balancing move has to tolerate that. The
suite reads every record in one throwaway `mc` container rather than one
container per cell.

### What balancing actually settles on

The documented rule is one target per node: the fleet's owned cells divided in
proportion to the node weights. Two roundings sit on top of it, both downwards
and both in whole cells — the donor hands over the whole cells by which it
exceeds its target, and a receiver fills only to 2% below its own target. So a
settled fleet does not reach its targets exactly. Observed at weights 8/1/1:

| Cells | Targets          | Settled    |
| ----- | ---------------- | ---------- |
| 24    | 19.2 / 2.4 / 2.4 | 20 / 1 / 3 |
| 25    | 20 / 2.5 / 2.5   | 21 / 1 / 3 |
| 48    | 38.4 / 4.8 / 4.8 | 39 / 5 / 4 |

The oracle therefore bounds each node at two whole cells above its target. With
a fixed fleet-wide total, that single upper bound still pins the distribution: a
lightly weighted node cannot absorb the difference, so an even split and a
one-node pile-up both fail it.

One consequence worth recording: at small totals the receivers have no whole
cell of room at all. With ten cells at weights 8/1/1 the light nodes' targets
are 1.0 and their 2% headroom rounds to zero, so a fleet sitting at 10/0/0 never
moves anything. The balancing cases therefore use 25 cells, not a handful.

### A missing capacity sample

A node frozen past its lease lifetime publishes no new sample. Observed: the
live nodes keep balancing among themselves, and no cell's ownership record ever
names the frozen node while it is frozen. The moves aimed at it fail and are
counted in the donor's `rebalance_failed`; v0.5.0 attempts them rather than
excluding the stale peer from the shared sample. The published wording calls
`CELLD_REBALANCE_INTERVAL_MS` "the maximum sample age" without saying what
happens to an over-age sample, so this is recorded as an observation, not as a
deviation.

A stale sample cannot be produced independently of lease loss on this release:
the only way to stop a node publishing samples is to stop the process, and a
process that cannot renew its lease fences itself (exit 3) as soon as it runs
again. The tested condition is therefore a **missing** sample.

### Drain evidence

A drain writes its own structured log, which the suite parses:

- `drain_restoration_baseline` with `nodes=` and `maximum=`
- `drain_token_acquired` with `waited_ms=` and `expires_ms=`
- `cell_handoff_accepted` with `cell=Recovery:<hex>`, `released_epoch=`,
  `successor=`, `successor_epoch=`
- `cell_handoff_timing`, then `drain_token_released`
- `clean_reload_prepared` (with `stale_live_databases_pruned=`) for a preserve,
  or `clean_reload_abandoned` when the preserve falls back to normal recovery

Observed once during the inventory, on a node that had been through a peer
freeze and a fenced restart: `clean_reload_abandoned` with `error=clean reload
inventory mismatch after pruning: expected 0, found 24`, while the ownership
records were still preserved. It did not reproduce on a clean node with or
without resident cells, and the documented fallback for an abandoned clean
reload is normal recovery, so this is recorded here rather than registered as a
bug. `operations.preserve-reload` requires `clean_reload_prepared`; if it ever
reports `abandoned` on a clean fleet, that is the reproduction to file.

### The previous pinned release

The upgrade case pins v0.5.1 at digest
`sha256:9df15352bcbb92a8d73dabadc349383ccd3d7851b2ba4ccffe2c956f2f9fc93f`
and replaces one node at a time with the v0.6.0 digest in
`infra/compose.yaml`. The [v0.6.0 release notes](https://github.com/denoland/celld/releases/tag/v0.6.0)
allow rolling updates only with **bucket durability**, which this case explicitly
selects. Fleet durability requires stopping the whole fleet first and is not
qualified by this rolling scenario. The upgrade overlay retains a 40-second
stop bound. Current validation is recorded in [RELEASE-0.6.0.md](RELEASE-0.6.0.md).

## Cases

All six run on a three-node fleet with MinIO and the storage proxy, using the
standard qualification fixture plus `infra/operations.yaml`. Each one holds
several cells, each cell has its own durable ledger, and every case ends by
requiring every acknowledged write of every cell to be readable through every
node and the full ledger check to pass.

- `operations.weighted-placement`: every node must publish the weight it was
  configured with, through both `/state` and its bucket lease, and at least two
  weights must differ. Twenty-five cells are then created through one node, which
  leaves that node far above its target, and balancing must move ownership until
  no node holds two whole cells more than its weighted target.
- `operations.rebalance-control`: `POST /rebalance/pause` on `celld2` must
  answer `{"rebalance_paused":true}` and republish the pause in that node's
  lease. The overload is then built on `celld`, because one paused lease is
  documented to stop every move in the fleet and not only the pauser's: across
  ten sample intervals no ownership record may change and no node's `rebalanced`
  counter may move. `POST /rebalance/resume` must restore convergence. Finally
  `celld3` is frozen past its lease lifetime, more cells are created on `celld2`,
  and while that node's capacity sample is missing the live pair must keep
  balancing and `celld3` must acquire nothing. It is then unfrozen, required to
  fence itself, and restarted.
- `operations.graceful-drain`: three cells start on `celld2`. A write is held
  open inside its owning object and the object records that it accepted it;
  `POST /shutdown` is issued while that write is in flight. The accepted write
  must complete with its own receipt, a new public request must be refused with
  503, readiness must go healthy then unhealthy and never back, the node must
  exit 0 inside its stop bound, every one of its cells must be owned by a peer
  afterwards, and its own log must record a handoff for each of them to a node
  other than itself.
- `operations.concurrent-drain`: two cells start on each of `celld2` and
  `celld3` and both nodes are drained at the same time, into a survivor held at
  a verified 128 MiB. Both must exit 0 inside the bound, their drain-token holds
  must not overlap, each must log its own handoffs, and both must have given
  every cell away. Capacity is restored by the scoped finalizer before recovery
  is required.
- `operations.preserve-reload`: three cells start on `celld3`, which is then
  asked for a same-node preserve. It must exit 0 inside the bound, log no
  handoff at all and log `clean_reload_prepared`, and every one of its ownership
  records must be byte-identical afterwards — same node and same epoch, neither
  handed to a peer nor released. The records must still name it after the
  restart.
- `operations.binary-upgrade`: the fleet starts on v0.5.1 using bucket durability, with one cell
  on each node, then each node in turn is re-pinned to v0.6.0, drained with
  `POST /shutdown`, recreated on the new image, and waited on until it reports
  healthy. Every intermediate step must be a genuinely mixed fleet running two
  distinct releases, and at each such step every cell must accept and keep an
  acknowledged write through every live node. The fleet must finish on v0.6.0
  with every acknowledged write intact.

## Compose overlays

`infra/operations.yaml` is layered after `infra/qualification.yaml`:

- `CELLD_PLACEMENT_WEIGHT` 8/1/1, so a weighted placement is distinguishable
  from an even one. The default is the container's CPU count, which is identical
  on all three nodes and therefore proves nothing about weighting.
- `CELLD_SHUTDOWN_TOTAL_MS: "20000"`, so a graceful drain fits inside the case
  deadline. celld derives the token wait and the no-progress interval from it.
- `CELLD_READY_FLEET_GATE_MS: "60000"`, so the gate is exercised but bounded.

`infra/balancing.yaml` adds `CELLD_REBALANCE_INTERVAL_MS: "1000"` and
`CELLD_IDLE_EVICT_S: "1"` for the two balancing cases only.
`infra/qualification.yaml` pins the interval to `0`, so the drain, preserve and
upgrade cases take their ownership snapshots without a background move racing
them.

`infra/upgrade.yaml` indirects each node's image through its own variable and
restores the 40-second stop bound, for the binary-upgrade case only. All
defaults are the pinned v0.6.0 digest, so selecting the overlay without setting
the variables changes nothing.

The fixture gains one parameter, `?delay=` on its existing history write. It
holds the request open inside the owning object and records an
`event:accepted:<id>` marker before it waits, which is what makes "the node
finishes the public HTTP requests that it accepted before shutdown" observable
rather than assumed. It is identical on every node and makes no authorization or
placement decision of its own.

## Evidence and oracles

Every case writes its observations to its evidence directory — the ownership
snapshots, the `/state` samples, the readiness series, the parsed drain log, the
per-cell owners before and after — before its oracles run, so a failing
operation still leaves its evidence behind. The classifiers live in
`src/OperationsOracles.ts` and are tested against deliberately wrong
observations in `test/OperationsOracles.test.ts`: a cell that stayed on a
draining node, a request accepted and then lost, a readiness flip in the wrong
order, a placement that ignores the node weights, a lost acknowledged write, an
ownership record that moved while balancing was paused, a same-node preserve
that released a cell, overlapping drain-token holds, and a uniform fleet offered
as mixed-version evidence all fail.

Background "ballast" cells exist only to give balancing something to move. They
carry no ledger, and a cold activation they are refused under contention is
retried. Every acknowledged write under test lives on a ledgered cell, where no
retry is applied.

## Not covered here

- **Stale, as distinct from missing, capacity samples.** See the inventory: a
  process that stops publishing samples cannot keep its lease, so the two
  conditions cannot be separated on this release.
- **`POST /reload`.** Deployment adoption is
  [in-place deployment](IN-PLACE-DEPLOYMENT.md), not a binary operation.
- **Downgrades.** v0.5.0 documents downgrade hazards for v0.3.0 and v0.4.0
  binaries; running a fleet backwards is a data-loss scenario that this suite
  does not attempt.
- **Orchestrator integration.** The stop grace, rollout deadline and health
  gating are properties of systemd or Kubernetes, not behaviours the binary
  enforces; there is nothing to assert.

## Running

```sh
pnpm tck --profile local --suite operations
pnpm tck --profile local --suite operations --case operations.graceful-drain
```

Local validation uses three celld nodes and MinIO on Docker, and the
binary-upgrade case additionally pulls the pinned v0.5.1 image. It does not
qualify AWS, managed Cloudflare, or any fleet whose nodes sit in separate
failure domains.
