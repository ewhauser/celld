# Fleet durability and follower recovery

Run `pnpm test:fleet` or `pnpm tck --profile local --suite fleet`. This uses the same two-node topology as [the bucket-durable suite](MULTINODE.md), overlaid with `CELLD_DURABILITY=fleet` and durability-proof debug logging. Both node disks and the MinIO bucket are retained.

Fleet mode may safely fall back to bucket proofs. The suite therefore requires evidence of actual fleet replication, rather than accepting the configuration flag as proof.

The first four stages repeat routing, owner failure, old-owner rejoin, and storage-partition fencing in fleet mode. A fifth stage, `fleet.follower-recovery`, then:

1. Finds the current owner from its bucket record and issues up to eight distinct warm-up transactions until a new `durable_wait` log reports a fleet proof for the tested cell. Missing proof fails; an ensemble-open message alone is insufficient.
2. Disconnects that owner from the storage network while keeping peer networking intact. Before the lease expires, a new transaction must be acknowledged and produce a new fleet-proof log for this cell. The write is issued once; failure is not retried or waived.
3. Kills the owner, leaves it disconnected, and waits beyond the lease lifetime. The survivor must recover every acknowledged transaction in both SQL and KV, acquire a newer ownership epoch, and log nonempty node-log recovery and sealing for the dead owner.
4. Writes new state on the survivor, restores the old owner's network connection, restarts it, and checks the full acknowledged history through both nodes.

The key distinction is the acknowledged write while the owner cannot reach MinIO: it needs its peer for durability. This occurs within the still-valid lease window, not after fencing. A slow host that misses that window fails visibly. The independent bucket suite continues to require fencing after lease loss.

Evidence includes new proof logs, follower recovery logs, ownership records, exact state snapshots, HTTP write results, and Docker network/lifecycle inspections. The parsers are pinned to celld v0.5.0 log formats. They reject bucket-only proofs, proofs for another cell, empty recoveries, and recoveries for a different node. Each stage has a 150-second deadline and the run has an eight-minute deadline. No known-bug waiver applies.

This qualifies one-follower recovery in a two-node local fleet. Three-node ensembles, simultaneous loss of follower disks, and paused-owner fencing are separate scenarios in [RESILIENCE.md](RESILIENCE.md). Neither suite covers all possible failure schedules. MinIO results are not AWS qualification. The follower may upload recovery data before the final read; the suite establishes recovery of a peer-replicated acknowledgment, not that the data remains absent from the bucket until that read.
