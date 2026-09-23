# Application deployment observations

`GET /state?view=application` on the private internal listener returns a compact,
read-only deployment snapshot with schema version 1. Unlike ordinary `/state`,
it does not contain per-cell identifiers or isolate details. It never reads S3
on demand, reloads code or modifies lifecycle state. Non-GET requests to this
view return 405. The endpoint uses the existing private-listener network boundary.

```json
{
  "schema_version": 1,
  "runtime_generation": "process-incarnation",
  "sampled_at_ms": 1790130200000,
  "snapshot_valid": true,
  "loaded": {"version": "v2", "prefix": "deploy/app/v2"},
  "local_generation": 2,
  "target": {"version": "v2", "prefix": "deploy/app/v2", "observed_at_ms": 1790130199000},
  "pointer_status": "observed",
  "adoption_status": "adopted",
  "resident_cells": 3,
  "pending_cells": 1,
  "swapping_cells": 0
}
```

`loaded` is the runtime's current application artifact identity. `target` is the
last successfully read deployment pointer, not an assertion about the bucket's
current contents. Until the first watcher poll, it is null and both statuses are
`unknown`. A failed pointer read sets `pointer_status=unavailable` while retaining
the last target for diagnostics. Successful reads refresh the target timestamp,
including reads of a previously failed deployment. Adoption is `adopting`,
`adopted`, `unchanged` or `failed`; failures keep the existing loaded generation.
Raw errors, credentials and application manifests are not exposed.

`runtime_generation` identifies the process incarnation. `local_generation` is
process-local, starts at one and must never be compared across nodes. Compare the
version and prefix across nodes; rollback legitimately changes back to an older
version while local generations continue to increase.

`pending_cells` counts resident cells not yet on the decision core's current
application generation. `swapping_cells` counts ongoing cell swaps. These are
one decision-core census, independent of successful node adoption. They do not
assert that every old stateless request or background activity has ended.

`snapshot_valid=false` means a loaded runtime was unavailable, the actor failed
to answer within one second, or pointer/runtime/core generations changed or
disagreed across the snapshot. Counts in an invalid snapshot must not be treated
as proof of completion. The core's pre-reload generation zero corresponds to the
runtime's first boot generation; later generations must match exactly. The view
remains available during control-only shutdown, with an invalid census.

An observer may report convergence only with fresh, consistent snapshots from
its complete current node membership, a fresh successful pointer observation on
every node, agreement on both target fields, successful adoption of that target
and zero pending/swapping cells. A deployment published after a poll may not yet
be discovered. A release pipeline must also check its expected version. A long
poll interval or interrupted pointer reads can therefore produce Unknown while
previous application code continues serving successfully.

Validation:

```sh
cargo test -p celld --lib deployment_status --locked
cargo check -p celld --bin celld --locked
cargo build -p celld --bin celld --locked
CELLD_TEST_BINARY="$PWD/target/debug/celld" CELLD_ESBUILD=/absolute/path/to/esbuild \
  python3 scripts/test-application-status.py
```

The opt-in live test requires Docker and the AWS CLI. It uses only disposable
MinIO with fixture credentials and an explicit loopback endpoint. It exercises
boot, application adoption while a resident cell is busy, cell convergence,
failed adoption, rollback and pointer disappearance, then removes its container
and local data. This does not qualify Kubernetes, AWS or recovery durability.
