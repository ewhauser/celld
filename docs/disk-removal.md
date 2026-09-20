# Strict disk-removal shutdown

Ordinary `POST /shutdown` acknowledges a stop request. Its process can exit after
an incomplete drain, and neither that response nor exit status authorizes disk
deletion. A node's disk also contains follower fragments for other leaders.

A bucket-backed node supports an explicit strict mode on the existing private
control listener. The caller must first require `schema_version: 1` and
`capabilities.strict_disk_removal: true` from **every binary which can later
recover this fleet**, including replacements. Older recovery readers do not
understand the native coverage proof. Do not downgrade recovery binaries after
using it. This is a homogeneous-fleet rollout requirement, not an operator
migration mechanism.

## Caller contract

`GET /state` includes this additive field (other existing fields are unchanged
until the node enters its terminal control-only phase):

```json
{
  "shutdown": {
    "schema_version": 1,
    "runtime_generation": "opaque-process-generation",
    "capabilities": {"strict_disk_removal": true},
    "control_only": false,
    "operation": null
  }
}
```

Submit `POST /shutdown?mode=remove-disk` with JSON:

```json
{"operation_id":"remove-123","expected_generation":"opaque-process-generation"}
```

Operation IDs are 1–128 ASCII letters, digits, dots, underscores or hyphens.
`expected_generation` is the process generation from `/state`, not a deployment
version. The response is HTTP 202 containing the `shutdown` object directly.
Acceptance is not completion. Repeat requests for the same operation/generation
return the current status without restarting work. A wrong generation, different
operation, or an ordinary shutdown already in progress returns 409. Malformed
JSON or an unsupported mode returns 400; unsupported runtime configuration
returns 501. An ordinary `/shutdown` during a strict operation returns 409.

Poll `GET /state`. A completed result looks like:

```json
{
  "shutdown": {
    "schema_version": 1,
    "runtime_generation": "opaque-process-generation",
    "capabilities": {"strict_disk_removal": true},
    "control_only": true,
    "operation": {
      "operation_id": "remove-123",
      "expected_generation": "opaque-process-generation",
      "mode": "remove-disk",
      "phase": "data_safe",
      "blocker": null
    }
  }
}
```

The other phases are `draining` and `failed`; their `blocker` describes outstanding
proof or failure. Terminal results are immutable. Require the exact operation,
generation, mode, schema and `data_safe` phase. Persist that result before
terminating the process. **Data-safe does not mean process-dead.** The process
remains unready and serves control status plus authenticated log recovery and
frozen append replies. The launcher must separately prove exact-child termination
and prevent restart before the caller deletes the disk or changes infrastructure.

Intent/result persistence belongs to the caller. There is no cross-restart job
or result archive. If the process or caller loses completion evidence, retain the
disk and block. A new process has a new generation and rejects the old request.
The runtime does not enroll disks or enforce restart exclusion.

`/state` and `/shutdown` remain on the existing private internal listener with
its existing access boundary; this does not introduce public routes or a new
credential scheme. Existing peer authentication still protects log RPCs.
Ordinary shutdown, preserve mode and signals retain their stop semantics.

## Proof and recovery

Strict shutdown closes follower append admission with a write lock. Readers hold
the lock through admitted append/fsync completion. It inventories both on-disk
session/epoch fragments and current ensembles; missing or unreadable evidence is
a blocker. An append reply advertises `quiesced: true` after this cut, including
idle probes, so a quiet leader can permanently degrade the shipper and release
its last follower obligation without waiting for another application write.

The existing node lease's folded log gains `bucket_complete` (default false for
old records). A leader publishes it for the exact log epoch only after permanent
shipper degradation, zero outstanding batches, and complete bucket coverage of
shipped data. Removed handles retain a conservative uncovered-tail latch.
Maintenance publishes the proof even when no replacement follower exists. A new
epoch resets it; recovery claim updates preserve it.

This proof is not a log seal. Recovery consumes it as the complete witness and
still folds every retained bundle into the per-cell layout before sealing. This
allows a 2-to-1 survivor to crash later without asking the removed follower to
witness recovery. Failed leaders without that proof use ordinary fenced recovery
while the retiring disk still serves its tail. Loss declarations scoped to each
obligated session prevent success; unrelated historical loss does not globally
block the fleet.

Completion also requires the application drain, local durability task joins,
actor termination/join and every own/follower recovery obligation. An own log
that could not publish coverage waits for its now-stopped lease to expire and
uses ordinary recovery. Store errors, missing records, drain/join failures and
the absolute `CELLD_SHUTDOWN_TOTAL_MS` deadline produce no successful result.
A timeout can leave work unresolved, but never grants disk deletion.

## Validation

`cargo test -p celld-logic --test disk_removal` exercises operation state,
completion prerequisites, epoch specificity and recovery proof propagation.
`cargo test -p celld --lib disk_removal::tests` exercises the real follower store,
append/freeze racing, absent evidence, and session-scoped loss declarations.

`examples/disk-removal/demo.py` runs disposable native processes against local
Docker MinIO, deletes disks only after polling the exact result, and checks an
acknowledged-write ledger. Its normal run covers shrink/grow, 2-to-1, and a later
survivor crash with the removed disks gone. `--failed-leader` kills a leader after
a peer-only acknowledgment while MinIO is frozen, then kills/restarts MinIO to
discard buffered PUTs. `--outage` holds MinIO unavailable across the strict
shutdown deadline and requires either an immutable failed result or the existing
lease self-fence with no completion and a retained disk. `--deadline` forces an
immutable failed result with a one-millisecond shutdown budget. `--ordinary`
checks preserve mode, ordinary shutdown and SIGTERM followed by restart and
ledger verification.

These checks do not qualify AWS, Kubernetes termination, launcher restart
exclusion, or mixed recovery binary versions. The private upstream simulation
and TLA+ suites are not included in this checkout.
