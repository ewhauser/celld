# Qualification backlog

This tracks the full P0/P1/P2 checklist, replacing the earlier, narrower local reliability backlog. Implementation details and limits are in [QUALIFICATION.md](QUALIFICATION.md); retained results and failures are in [FINDINGS.md](FINDINGS.md).

| Priority | Suite                         | Implementation                                                                                                                                    | Validation                                                             |
| -------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| P0       | API compatibility             | 66 identical-fixture differential cases plus 6 deployment checks; known bugs remain visible                                                       | Validated: 66 reference; 68 local passes, 2 divergences, 2 known bugs  |
| P0       | Acknowledged-write durability | Fsynced external ledger, concurrent traffic, seeded repeated crashes, complete payload/history checks, read-only audit after driver interruption  | Validated: 3 traffic cases with persisted-ledger audits                |
| P0       | Ownership and failover        | Pause/takeover/resume under traffic, higher ownership epochs, fencing, real-time history ordering                                                 | Validated: traffic fencing plus existing failover suites               |
| P0       | Application dependencies      | Queue acceptance/redelivery over downtime, workflow checkpoints/recovery, named RPC after deploy, stream reconnect cursors                        | Validated: 5 dependency cases and service RPC after rolling deployment |
| P1       | Storage and lifecycle faults  | Live-store latency/503/timeout/lost-response injection, peer partition, rolling application deployment, real WebSocket hibernation, restart races | Validated: 6 fault cases plus hibernation and restart races            |
| P2       | Capacity and performance      | 16 MiB restore, verified memory limits, four slow 8 MiB readers, 500-message queue load, failure without spare memory; timing metrics             | Validated: all 5 bounded capacity scenarios                            |

Existing recovery, bucket/fleet failover, and three-node resilience suites remain separate regression gates. Known-bug API waivers never apply to qualification scenarios. A passing all-replica-disk-loss declaration check remains distinct from preservation of acknowledged data.

The original local checklist was validated with 19 qualification scenarios and all 24 existing recovery/failover scenarios passing. The API results above retain known bugs and divergences. These are finite schedules and workloads, not exhaustive correctness proofs or production capacity certification. Rolling deployment covers two application revisions on the pinned celld binary; cross-version celld binary upgrades need a second explicitly selected version.

External qualification remains unavailable without dedicated AWS and managed Cloudflare environments. Local MinIO and Miniflare results do not substitute for those environments. Hosted CI has run and exposed diagnostic transport and memory-pressure routing failures; see [FINDINGS.md](FINDINGS.md) for the fixes and validation. Upstream reports remain local drafts. PITR, cron, TCP/TLS, and Containers/Sandbox are outside this checklist and remain separate API expansion work.

The MinIO conditional-write diagnostic reset remains tracked as INFRA-001 in [BUGS.md](BUGS.md), without a waiver. All suites now diagnose through a proxy that opens a fresh upstream connection per request. Qualification uses the same transport for runtime storage; other suites retain direct-MinIO runtime traffic.

Ten additional [storage durability cases](STORAGE-DURABILITY.md) extend the fault suite with sync barriers, cursor restrictions, runtime deadlines, a barrier armed across an object reset, and a barrier held open while the owner loses every peer. Cloud durability remains a follow-up; the linked coverage document states the precise limits.
