# Storage durability

The API suite and fault suite cover explicit durability barriers, unfinished SQL cursors, and runtime deadlines. Expectations follow the [celld v0.5.0 compatibility contract](https://github.com/denoland/celld/blob/v0.5.0/docs/cloudflare-compat.md) and [Cloudflare SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).

## Portable API cases

These fixtures run unchanged on workerd and celld:

| Case                        | Required behavior                                                                                                                         |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `storage.sync-committed`    | `sync()` completes after committed SQL writes and KV writes/deletes using `allowUnconfirmed`; repeated barriers preserve the exact state. |
| `sql.consumed-write-cursor` | Draining an `INSERT … RETURNING` cursor permits `sync()` and preserves every returned and stored row.                                     |
| `sql.open-read-cursor`      | An open read cursor permits `sync()` and retains the remaining rows when no concurrent writer intervenes.                                 |

## Local fault cases

Run `pnpm test:faults`, or select one case:

```sh
pnpm tck --profile local --suite faults --case faults.sync-recovery
```

| Case                            | Required behavior                                                                                                                                                                                                   |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `faults.sync-recovery`          | Acknowledged SQL and KV writes survive a restart of all three nodes using bucket durability.                                                                                                                        |
| `faults.sync-outage`            | `sync()` rejects while the object's storage uploads are throttled. After storage recovery and fleet restart, previously acknowledged state survives. The unacknowledged replacement value may survive or be absent. |
| `faults.sync-transaction`       | `sync()` rejects inside an open transaction; rollback removes its SQL write.                                                                                                                                        |
| `faults.write-cursor-sync`      | `sync()` rejects an unfinished write cursor without consuming its remaining rows.                                                                                                                                   |
| `faults.write-cursor-response`  | An unfinished write cursor prevents a successful response.                                                                                                                                                          |
| `faults.write-cursor-outbound`  | An unfinished write cursor prevents an outbound request from reaching a separate witness object. A consumed cursor first proves that the witness path works.                                                        |
| `faults.transaction-deadline`   | A transaction held open for 40 seconds hits the runtime's 30-second limit, resets the object, and rolls back the pending SQL write.                                                                                 |
| `faults.gate-deadline`          | A gate held open for 40 seconds hits the same limit, resets the object, and preserves committed state.                                                                                                              |
| `faults.sync-abort`             | A barrier armed immediately before `ctx.abort()` never resolves; the reset is classified apart from a barrier rejection, the activation changes, acknowledged state survives, and a new activation syncs again.     |
| `faults.sync-barrier-partition` | Repeated barriers held open across the loss of every peer must reject, block, or resolve with a durability proof for this cell that does not claim fleet replication. No acknowledged barrier write is lost.        |

### Post-abort `sync()` (`faults.sync-abort`)

The fixture arms `storage.sync()` synchronously — a lazily started barrier would never be pending when the reset lands — then calls `ctx.abort("tck-object-abort")` and races the armed promise against a two-second window. Three textual sources stay separable: `resolved`, `unsettled`, and a `tck-sync-rejected:` prefix applied only to the barrier's own rejection. `classifyAbortSync` maps the response to `abort-error`, `sync-rejected`, `sync-unsettled`, `sync-resolved`, or `unclassified`; a rejection whose text carries the abort marker is the reset's own error and is never reported as a barrier rejection. Only the first three classifications pass.

**Observed on celld v0.5.0** (run `tck-ffb1d7fd-fa23-476f-bb4a-86e7b8c2b20a`): the handler does not survive the reset, so the armed barrier has no observable settlement. The request fails with the abort's own error (`{"rejected":true,"message":"tck-object-abort"}`, HTTP 500) — classification `abort-error`. This matches Cloudflare's documented contract that `abort` immediately resets the object and that application code cannot catch the error; neither Cloudflare nor celld v0.5.0 documents how a pending `sync()` settles, so the case asserts only the invariants it can establish: the activation token changes, the acknowledged seed row survives with no uncommitted row added, the unconfirmed replacement may survive or be absent, a barrier on the new activation succeeds, and the independent ledger keeps every acknowledged write across the reset. The case uses bucket durability so the reset cannot be masked by a peer acknowledgment.

### Interrupted peer replication (`faults.sync-barrier-partition`)

This case uses `durability: "fleet"`. It first requires a `proof="fleet"` `durable_wait` log for the tested cell, so the barrier demonstrably depends on a peer before replication is cut. The fixture then holds barriers open for 20 seconds — an `allowUnconfirmed` put followed by an explicit `sync()`, repeated — while the driver disconnects **every peer** of the owner from the peer network with `fleet.partitionPeers`. Partitioning the peers rather than the owner keeps the owner's published port, so the barrier stays observable; a sidecar probe proves each `peer-<node>:8081` endpoint is unreachable, and the fiber is confirmed still in flight at that moment. A barrier that has not completed within 40 seconds is classified `blocked` and joined only after the scoped finalizer reconnects the peers.

**Observed on celld v0.5.0** (run `tck-ffb1d7fd-fa23-476f-bb4a-86e7b8c2b20a`): the barrier neither rejects nor blocks. All 143 rounds resolved. Three rounds took 1577 ms, 1247 ms, and 284 ms — the barriers spanning the network changes — and every other round completed within 42 ms. The owner's log holds 30 `proof="fleet"` waits and no bucket wait before the cut, and 123 `proof="bucket"` waits and no fleet wait inside the partitioned window — the documented single-node fallback to bucket durability, not a false claim of replication. After reconnection and a full fleet restart, all three nodes reported the last acknowledged barrier value (`barrier-143`) and exactly the seeded SQL row. This is documented behaviour, so no bug is registered.

The oracle asserts only what the release documents: the peers were unreachable, the barrier was open across the cut, every round outcome is either `resolved` or a barrier rejection, no `proof="fleet"` wait is logged for this cell while no peer is reachable, the three nodes agree, the committed SQL state holds exactly the seeded row, the value that survives is at least the last resolved barrier's, and a fully resolved run carries a durability proof for this cell. The history ledger is then verified through every node.

Deadline cases require a runtime timeout response between 25 and 45 seconds and a changed activation token. Their driver deadline is 50 seconds; a driver timeout or unrelated handler error cannot pass. The outage case uses `allowUnconfirmed` and identifies rejection from the explicit `sync()` call, so an ordinary output-gate failure cannot substitute for the barrier. Fault evidence must identify the test object's storage path. A scoped finalizer clears the fault even when an assertion fails.

Every new semantic oracle has deliberate negative examples: lost writes, stale values, leaked output, accepted barriers, missing cursor rows, unchanged activations, early failures, and unrolled-back transactions. Portable positive observations are captured from actual workerd runs.

## Remaining coverage

These cases do not establish AWS durability. The restart case retains local disks and MinIO; it does not simulate permanent loss of every replica or an availability zone. Existing disk-loss and partition cases provide separate coverage.

Two limits of the new cases are explicit. Post-abort `sync()` records how the _request_ settles, because celld v0.5.0 destroys the handler before the armed promise can be observed; if a future release lets the handler survive, `classifyAbortSync` already distinguishes a barrier rejection from the reset. The replication case interrupts peer replication at the Docker network level on one host: it establishes that a barrier open across the loss of every peer falls back to bucket durability without losing an acknowledged write, not that every replication failure mode or a genuine object-store outage behaves the same way. `faults.sync-outage` covers the storage side separately.
