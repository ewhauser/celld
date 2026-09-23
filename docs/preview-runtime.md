# Preview snapshot bootstrap

`celld::preview_seed` supplies the storage part of the operator's
`celld-snapshot-v1` executor. The Kubernetes executor is a separate CLI change.

A caller holding the destination's exclusive initialization claim:

1. Checks `ensure_unopened` before starting. The destination must have no fleet
   or application objects, and the operator must prevent runtime startup.
2. Calls `capture` for every selected class/canonical-ID pair. Capture restores a
   frozen LTX epoch chain, sanitizes its SQLite image, and creates an immutable
   snapshot under `preview-snapshots/<reservation-UID>/`. Source access is read-only.
3. Pins the complete returned manifest in the operator reservation **before**
   calling `import`. Each entry contains `class`, `id`, `snapshotID`,
   `sourceVersion` (`ltx:eN:txid:N`) and the payload's SHA-256 digest.
4. Imports only the pinned snapshots, then relinquishes all writes before
   acknowledging completion. Partial, byte-identical imports are idempotent.

The API does not acquire Kubernetes authority or fence other workers. Never call
it on a serving fleet, reset an uncertain operation, or allow concurrent retries.
A failed capture may have persisted its snapshot; it cannot be recaptured under
the same operation identity. Snapshot data and partial imports are retained.

## What is copied

Each object is a consistent **persisted checkpoint**, independently selected.
This is not an atomic multi-object cut, nor a promise to include writes still in
an active node or recovery log. Missing checkpoints fail rather than create empty
objects. Do not use this workflow for production backup or disaster recovery.

Application SQLite tables, KV, actor names and embedded facet images are retained.
Replication control tables and wake epochs are removed. `Clear` removes alarms,
including facet alarms; `Preserve` retains their persisted values. The new fleet's
ordinary wake-index initialization inventories seeded cells before serving.
Leases, deployment configuration, credentials and node logs are never copied.
Application data can itself contain sensitive values; source authorization must
cover those values.

Selections accept 1–100 unique objects, subject to the runtime's canonical scope
length limit. Checkpoints and expanded images are capped at 256 MiB per object.

## Runtime bootstrap

Imports create a complete LTX snapshot at reserved epoch zero, TXID one. Epoch
zero is never an ownership/writer epoch. A fresh activation checks for this exact
bootstrap key and restores through the normal epoch-chain path if it exists;
its first writer still acquires epoch one. Subsequent ownership/recovery uses
normal replication. Storage errors fail activation instead of serving empty
state. This adds one object-store HEAD to a fresh activation.

The preview runtime image must include this change. Older binaries skip restore
for fresh ownership and therefore cannot safely serve a seeded preview.

## Validation

`cargo test -p celld --lib preview_seed` covers multi-object checkpoint round trips,
KV and SQL retention, both alarm policies, digest/operation rejection, uncertain
recapture refusal, and an actual fresh runtime activation restoring the seed.
These are local tests over an in-memory object store; they do not qualify cloud
credentials, Kubernetes policy, routing or TLS.
